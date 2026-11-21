#pragma once

// Фейковый транспорт TDLib для юнит-тестов telega_td: очередь входящих сообщений и
// программируемый ответчик, который вызывается на каждый send().

#include <nlohmann/json.hpp>

#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "td/td_error.h"
#include "td/td_transport.h"

namespace telega::td::test {

class FakeTransport final : public TdTransport {
  public:
    using Responder = std::function<void(FakeTransport&, int client_id, const Json& request)>;

    int create_client_id() override {
        const std::lock_guard<std::mutex> lock(mu_);
        return next_client_id_++;
    }

    void send(int client_id, const std::string& request_json) override {
        const Json request = Json::parse(request_json);
        Responder responder;
        {
            const std::lock_guard<std::mutex> lock(mu_);
            sent_.emplace_back(client_id, request);
            responder = responder_;
        }
        sent_cv_.notify_all();
        if (responder) {
            responder(*this, client_id, request);
        }
    }

    std::optional<std::string> receive(std::chrono::milliseconds timeout) override {
        std::unique_lock<std::mutex> lock(mu_);
        if (!cv_.wait_for(lock, timeout, [&] { return !incoming_.empty(); })) {
            return std::nullopt;
        }
        std::string s = std::move(incoming_.front());
        incoming_.pop_front();
        return s;
    }

    std::string execute(const std::string& request_json) override {
        const std::lock_guard<std::mutex> lock(mu_);
        executed_.push_back(request_json);
        return R"({"@type":"ok"})";
    }

    void set_responder(Responder responder) {
        const std::lock_guard<std::mutex> lock(mu_);
        responder_ = std::move(responder);
    }

    // Сообщение от TDLib клиенту (апдейт или ответ).
    void push(int client_id, Json message) {
        message["@client_id"] = client_id;
        push_raw(message.dump());
    }
    void push_raw(std::string raw) {
        {
            const std::lock_guard<std::mutex> lock(mu_);
            incoming_.push_back(std::move(raw));
        }
        cv_.notify_all();
    }
    // Ответ на запрос: копирует @extra.
    void respond(int client_id, const Json& request, Json response) {
        if (request.contains("@extra")) {
            response["@extra"] = request["@extra"];
        }
        push(client_id, std::move(response));
    }
    void push_update(int client_id, const Json& update) { push(client_id, update); }

    std::vector<Json> sent(int client_id) const {
        const std::lock_guard<std::mutex> lock(mu_);
        std::vector<Json> out;
        for (const auto& [id, request] : sent_) {
            if (id == client_id) {
                out.push_back(request);
            }
        }
        return out;
    }
    std::vector<std::string> executed() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return executed_;
    }
    // Ждёт, пока клиенту не отправят хотя бы n запросов.
    bool wait_sent(int client_id, size_t n, std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mu_);
        return sent_cv_.wait_for(lock, timeout, [&] {
            size_t count = 0;
            for (const auto& entry : sent_) {
                count += entry.first == client_id ? 1 : 0;
            }
            return count >= n;
        });
    }

  private:
    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::condition_variable sent_cv_;
    int next_client_id_ = 1;
    std::deque<std::string> incoming_;
    std::vector<std::pair<int, Json>> sent_;
    std::vector<std::string> executed_;
    Responder responder_;
};

}  // namespace telega::td::test
