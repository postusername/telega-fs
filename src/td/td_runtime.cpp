#include "td/td_runtime.h"

#include <spdlog/spdlog.h>

#include <exception>
#include <utility>
#include <vector>

namespace telega::td {

TdRuntime::TdRuntime(std::shared_ptr<TdTransport> transport, std::chrono::milliseconds poll)
    : transport_(std::move(transport)), poll_(poll) {}

TdRuntime::~TdRuntime() { stop(); }

std::shared_ptr<TdClient> TdRuntime::create_client() {
    const std::lock_guard<std::mutex> lock(mu_);
    if (stopped_) {
        throw TdException(TdError::make(TdError::kStopped, "runtime stopped"));
    }
    // Клиент регистрируется до первого send: апдейты TDLib начинает слать только после него.
    auto client = std::make_shared<TdClient>(transport_, transport_->create_client_id());
    clients_.emplace(client->id(), client);
    if (!thread_.joinable()) {
        thread_ = std::thread([this] { run(); });
    }
    return client;
}

void TdRuntime::stop() {
    std::vector<std::shared_ptr<TdClient>> clients;
    {
        const std::lock_guard<std::mutex> lock(mu_);
        if (stopped_) {
            return;
        }
        stopped_ = true;
        stop_.store(true);
    }
    if (thread_.joinable()) {
        thread_.join();
    }
    {
        const std::lock_guard<std::mutex> lock(mu_);
        for (auto& entry : clients_) {
            clients.push_back(std::move(entry.second));
        }
        clients_.clear();
    }
    for (const auto& client : clients) {
        client->shutdown(TdError::make(TdError::kStopped, "runtime stopped"));
    }
}

TdRuntimeStats TdRuntime::stats() const {
    return TdRuntimeStats{messages_.load(), malformed_.load(), unknown_client_.load()};
}

void TdRuntime::run() {
    using Clock = std::chrono::steady_clock;
    constexpr std::chrono::milliseconds kSweepPeriod{10};
    auto last_sweep = Clock::now();
    while (!stop_.load()) {
        try {
            if (const auto raw = transport_->receive(poll_)) {
                handle(*raw);
            }
        } catch (const std::exception& e) {
            spdlog::error("td: receive loop error: {}", e.what());
        }
        // Под потоком сообщений проверяем таймауты не чаще раза в 10 мс.
        if (const auto now = Clock::now(); now - last_sweep >= kSweepPeriod) {
            last_sweep = now;
            expire_pending();
        }
    }
}

void TdRuntime::handle(const std::string& raw) {
    messages_.fetch_add(1, std::memory_order_relaxed);
    Json message = Json::parse(raw, nullptr, /*allow_exceptions=*/false);
    if (!message.is_object() || !message.contains("@client_id") ||
        !message["@client_id"].is_number_integer()) {
        malformed_.fetch_add(1, std::memory_order_relaxed);
        spdlog::warn("td: dropped a message that is not a JSON object with @client_id");
        return;
    }
    const int client_id = message["@client_id"].get<int>();
    std::shared_ptr<TdClient> client;
    {
        const std::lock_guard<std::mutex> lock(mu_);
        const auto it = clients_.find(client_id);
        if (it != clients_.end()) {
            client = it->second;
        }
    }
    if (!client) {
        unknown_client_.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    client->on_message(std::move(message));
    // Закрытый клиент TDLib уничтожает сам, больше сообщений для него не будет.
    if (client->closed()) {
        const std::lock_guard<std::mutex> lock(mu_);
        clients_.erase(client_id);
    }
}

void TdRuntime::expire_pending() {
    std::vector<std::shared_ptr<TdClient>> clients;
    {
        const std::lock_guard<std::mutex> lock(mu_);
        clients.reserve(clients_.size());
        for (const auto& entry : clients_) {
            clients.push_back(entry.second);
        }
    }
    const auto now = TdClient::Clock::now();
    for (const auto& client : clients) {
        client->expire(now);
    }
}

}  // namespace telega::td
