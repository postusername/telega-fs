#include "td/td_client.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <exception>
#include <mutex>
#include <utility>
#include <vector>

namespace telega::td {

// ---- Подписки ----------------------------------------------------------------------------

// Таблица обработчиков по типу апдейта. Обработчик вызывается без общего мьютекса, под его
// собственным: remove() ждёт конца идущего вызова. Мьютекс рекурсивный, чтобы обработчик мог
// отписать сам себя (поток приёма уже держит его).
class SubscriptionRegistry {
  public:
    struct DispatchResult {
        size_t invoked = 0;
        size_t failed = 0;
    };

    uint64_t add(const std::string& type, TdClient::Handler fn) {
        auto entry = std::make_shared<Entry>();
        entry->fn = std::move(fn);
        const std::lock_guard<std::mutex> lock(mu_);
        const uint64_t id = next_id_++;
        by_type_[type].emplace_back(id, std::move(entry));
        return id;
    }

    void remove(const std::string& type, uint64_t id) {
        std::shared_ptr<Entry> entry;
        {
            const std::lock_guard<std::mutex> lock(mu_);
            const auto it = by_type_.find(type);
            if (it == by_type_.end()) {
                return;
            }
            auto& list = it->second;
            const auto pos = std::find_if(list.begin(), list.end(),
                                          [id](const auto& p) { return p.first == id; });
            if (pos == list.end()) {
                return;
            }
            entry = std::move(pos->second);
            list.erase(pos);
            if (list.empty()) {
                by_type_.erase(it);
            }
        }
        const std::lock_guard<std::recursive_mutex> run(entry->run);
        entry->removed = true;
    }

    DispatchResult dispatch(const std::string& type, const Json& update) {
        std::vector<std::shared_ptr<Entry>> snapshot;
        {
            const std::lock_guard<std::mutex> lock(mu_);
            const auto it = by_type_.find(type);
            if (it == by_type_.end()) {
                return {};
            }
            for (const auto& p : it->second) {
                snapshot.push_back(p.second);
            }
        }
        DispatchResult result;
        for (const auto& entry : snapshot) {
            const std::lock_guard<std::recursive_mutex> run(entry->run);
            if (entry->removed) {
                continue;
            }
            ++result.invoked;
            try {
                entry->fn(update);
            } catch (const std::exception& e) {
                ++result.failed;
                spdlog::warn("td: handler of {} threw: {}", type, e.what());
            } catch (...) {
                ++result.failed;
                spdlog::warn("td: handler of {} threw a non-standard exception", type);
            }
        }
        return result;
    }

  private:
    struct Entry {
        std::recursive_mutex run;
        TdClient::Handler fn;
        bool removed = false;
    };
    std::mutex mu_;
    uint64_t next_id_ = 1;
    std::unordered_map<std::string, std::vector<std::pair<uint64_t, std::shared_ptr<Entry>>>>
        by_type_;
};

Subscription::Subscription(Subscription&& other) noexcept
    : registry_(std::move(other.registry_)), type_(std::move(other.type_)), id_(other.id_) {
    other.id_ = 0;
}

Subscription& Subscription::operator=(Subscription&& other) noexcept {
    if (this != &other) {
        reset();
        registry_ = std::move(other.registry_);
        type_ = std::move(other.type_);
        id_ = other.id_;
        other.id_ = 0;
    }
    return *this;
}

Subscription::~Subscription() { reset(); }

void Subscription::reset() {
    if (id_ == 0) {
        return;
    }
    if (const auto registry = registry_.lock()) {
        registry->remove(type_, id_);
    }
    id_ = 0;
}

// ---- TdClient ----------------------------------------------------------------------------

namespace {

// Запас на доставку таймаута потоком приёма: блокирующий call() сам снимает запрос, если поток
// приёма застрял.
constexpr std::chrono::milliseconds kCallGrace{2'000};

std::exception_ptr to_exception(TdError error) {
    return std::make_exception_ptr(TdException(std::move(error)));
}

}  // namespace

TdClient::TdClient(std::shared_ptr<TdTransport> transport, int client_id)
    : transport_(std::move(transport)),
      client_id_(client_id),
      registry_(std::make_shared<SubscriptionRegistry>()) {}

TdClient::~TdClient() { fail_all(TdError::make(TdError::kClosed, "client destroyed")); }

TdClient::Submitted TdClient::submit(Json request, std::chrono::milliseconds timeout) {
    std::future<Json> future;
    std::optional<TdError> refused;
    uint64_t extra = 0;
    {
        const std::lock_guard<std::mutex> lock(mu_);
        if (stopped_) {
            refused = TdError::make(TdError::kStopped, "runtime stopped");
        } else if (closed_) {
            refused = TdError::make(TdError::kClosed, "client closed");
        } else {
            extra = next_extra_++;
            std::promise<Json> promise;
            future = promise.get_future();
            pending_.emplace(extra, Pending{std::move(promise), Clock::now() + timeout});
        }
    }
    if (refused) {
        std::promise<Json> failed;
        failed.set_exception(to_exception(std::move(*refused)));
        return {0, failed.get_future()};
    }
    request["@extra"] = extra;
    requests_.fetch_add(1, std::memory_order_relaxed);
    try {
        transport_->send(client_id_, request.dump());
    } catch (...) {
        const std::lock_guard<std::mutex> lock(mu_);
        pending_.erase(extra);
        throw;
    }
    return {extra, std::move(future)};
}

void TdClient::cancel(uint64_t extra, TdError error) {
    std::optional<Pending> taken;
    {
        const std::lock_guard<std::mutex> lock(mu_);
        const auto it = pending_.find(extra);
        if (it == pending_.end()) {
            return;  // ответ или таймаут уже обработаны потоком приёма
        }
        taken.emplace(std::move(it->second));
        pending_.erase(it);
    }
    timeouts_.fetch_add(1, std::memory_order_relaxed);
    taken->promise.set_exception(to_exception(std::move(error)));
}

std::future<Json> TdClient::call_async(Json request, std::chrono::milliseconds timeout) {
    return submit(std::move(request), timeout).future;
}

Json TdClient::call(Json request, std::chrono::milliseconds timeout) {
    auto submitted = submit(std::move(request), timeout);
    if (submitted.extra != 0 &&
        submitted.future.wait_for(timeout + kCallGrace) == std::future_status::timeout) {
        cancel(submitted.extra, TdError::make(TdError::kTimeout, "request timed out"));
    }
    return submitted.future.get();
}

Subscription TdClient::subscribe(const std::string& type, Handler handler) {
    Subscription sub;
    sub.id_ = registry_->add(type, std::move(handler));
    sub.type_ = type;
    sub.registry_ = registry_;
    return sub;
}

// ---- Поток приёма ------------------------------------------------------------------------

void TdClient::on_message(Json message) {
    if (!message.is_object()) {
        return;
    }
    const auto extra_it = message.find("@extra");
    if (extra_it != message.end()) {
        const bool numeric = extra_it->is_number_integer();
        const uint64_t extra = numeric ? extra_it->get<uint64_t>() : 0;
        message.erase("@extra");
        message.erase("@client_id");
        std::optional<Pending> taken;
        if (numeric) {
            const std::lock_guard<std::mutex> lock(mu_);
            const auto it = pending_.find(extra);
            if (it != pending_.end()) {
                taken.emplace(std::move(it->second));
                pending_.erase(it);
            }
        }
        if (!taken) {
            late_responses_.fetch_add(1, std::memory_order_relaxed);
            spdlog::debug("td: dropped response without a waiting request ({})",
                          message.value("@type", std::string{"?"}));
            return;
        }
        if (message.value("@type", std::string{}) == "error") {
            taken->promise.set_exception(to_exception(TdError::from_json(message)));
        } else {
            taken->promise.set_value(std::move(message));
        }
        return;
    }

    message.erase("@client_id");
    updates_.fetch_add(1, std::memory_order_relaxed);
    const std::string type = message.value("@type", std::string{});
    if (type == "updateAuthorizationState") {
        on_auth_update(message);
    }
    const auto result = registry_->dispatch(type, message);
    if (result.failed != 0) {
        handler_failures_.fetch_add(result.failed, std::memory_order_relaxed);
    }
    if (result.invoked == 0 && type != "updateAuthorizationState") {
        dropped_updates_.fetch_add(1, std::memory_order_relaxed);
    }
}

void TdClient::on_auth_update(const Json& update) {
    if (!update.contains("authorization_state") || !update["authorization_state"].is_object()) {
        return;
    }
    const Json& state = update["authorization_state"];
    const bool is_closed = state.value("@type", std::string{}) == "authorizationStateClosed";
    {
        const std::lock_guard<std::mutex> lock(mu_);
        // Ответ getAuthorizationState мог прийти раньше апдейта с тем же состоянием: первый
        // апдейт после seed_auth_state() с тем же содержимым — это то же самое состояние, а не
        // новое. Иначе авторизация обработала бы его дважды (второй setTdlibParameters — 400).
        const bool duplicate_of_seed = seeded_state_.has_value() && *seeded_state_ == state;
        seeded_state_.reset();
        if (!duplicate_of_seed) {
            auth_ = AuthState{auth_ ? auth_->seq + 1 : 1, state};
        }
        closed_ = closed_ || is_closed;
    }
    cv_.notify_all();
    if (is_closed) {
        fail_all(TdError::make(TdError::kClosed, "client closed"));
    }
}

void TdClient::expire(Clock::time_point now) {
    std::vector<Pending> expired;
    {
        const std::lock_guard<std::mutex> lock(mu_);
        for (auto it = pending_.begin(); it != pending_.end();) {
            if (it->second.deadline <= now) {
                expired.push_back(std::move(it->second));
                it = pending_.erase(it);
            } else {
                ++it;
            }
        }
    }
    timeouts_.fetch_add(expired.size(), std::memory_order_relaxed);
    for (auto& p : expired) {
        p.promise.set_exception(
            to_exception(TdError::make(TdError::kTimeout, "request timed out")));
    }
}

void TdClient::fail_all(const TdError& reason) {
    std::unordered_map<uint64_t, Pending> taken;
    {
        const std::lock_guard<std::mutex> lock(mu_);
        taken.swap(pending_);
    }
    for (auto& entry : taken) {
        entry.second.promise.set_exception(to_exception(reason));
    }
}

void TdClient::shutdown(const TdError& reason) {
    {
        const std::lock_guard<std::mutex> lock(mu_);
        stopped_ = true;
    }
    fail_all(reason);
    cv_.notify_all();
}

// ---- Авторизация и закрытие --------------------------------------------------------------

std::optional<AuthState> TdClient::auth_state() const {
    const std::lock_guard<std::mutex> lock(mu_);
    return auth_;
}

AuthState TdClient::wait_auth_state(uint64_t after_seq, std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mu_);
    const bool ready =
        cv_.wait_for(lock, timeout, [&] { return (auth_ && auth_->seq > after_seq) || stopped_; });
    if (auth_ && auth_->seq > after_seq) {
        return *auth_;
    }
    throw TdException(ready ? TdError::make(TdError::kStopped, "runtime stopped")
                            : TdError::make(TdError::kTimeout, "authorization state timeout"));
}

void TdClient::seed_auth_state(const Json& state) {
    if (!state.is_object() || !state.contains("@type")) {
        return;
    }
    {
        const std::lock_guard<std::mutex> lock(mu_);
        if (auth_) {
            return;
        }
        auth_ = AuthState{1, state};
        seeded_state_ = state;
    }
    cv_.notify_all();
}

bool TdClient::wait_closed(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mu_);
    cv_.wait_for(lock, timeout, [&] { return closed_ || stopped_; });
    return closed_;
}

bool TdClient::close_with(const char* request_type, std::chrono::milliseconds timeout) {
    if (closed()) {
        return true;
    }
    const auto deadline = Clock::now() + timeout;
    try {
        call(Json{{"@type", request_type}}, timeout);
    } catch (const TdException& e) {
        // Closed мог прийти раньше ответа `ok`, а при ошибке всё равно проверяем состояние.
        spdlog::debug("td: {} answered with {}", request_type, e.error().to_string());
    }
    return wait_closed(
        std::max(std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now()),
                 std::chrono::milliseconds{0}));
}

bool TdClient::close(std::chrono::milliseconds timeout) { return close_with("close", timeout); }

bool TdClient::log_out(std::chrono::milliseconds timeout) { return close_with("logOut", timeout); }

bool TdClient::closed() const {
    const std::lock_guard<std::mutex> lock(mu_);
    return closed_;
}

std::optional<TdError> TdClient::dead_reason() const {
    const std::lock_guard<std::mutex> lock(mu_);
    if (stopped_) {
        return TdError::make(TdError::kStopped, "runtime stopped");
    }
    if (closed_) {
        return TdError::make(TdError::kClosed, "client closed");
    }
    return std::nullopt;
}

TdClientStats TdClient::stats() const {
    return TdClientStats{requests_.load(), timeouts_.load(),        late_responses_.load(),
                         updates_.load(),  dropped_updates_.load(), handler_failures_.load()};
}

}  // namespace telega::td
