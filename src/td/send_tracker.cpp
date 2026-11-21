#include "td/send_tracker.h"

#include <algorithm>

namespace telega::td {

namespace {

// Как часто ожидание проверяет, что клиент жив: закрытие и остановка рантайма не будят cv трекера.
constexpr std::chrono::milliseconds kLivenessSlice{50};

}  // namespace

SendTracker::SendTracker(TdClient& client, size_t max_buffered)
    : client_(client), max_buffered_(std::max<size_t>(max_buffered, 1)) {
    succeeded_ = client_.subscribe("updateMessageSendSucceeded",
                                   [this](const Json& u) { record(u, /*failed=*/false); });
    failed_ = client_.subscribe("updateMessageSendFailed",
                                [this](const Json& u) { record(u, /*failed=*/true); });
}

void SendTracker::record(const Json& update, bool failed) {
    // Работает в потоке приёма: только раскладывает данные и будит ожидающих.
    if (!update.contains("message") || !update["message"].is_object() ||
        !update.contains("old_message_id") || !update["old_message_id"].is_number_integer()) {
        return;
    }
    const Json& message = update["message"];
    if (!message.contains("chat_id") || !message["chat_id"].is_number_integer()) {
        return;
    }
    const Key key{message["chat_id"].get<int64_t>(), update["old_message_id"].get<int64_t>()};
    Outcome outcome;
    outcome.message = message;
    if (failed) {
        outcome.error = update.contains("error")
                            ? TdError::from_json(update["error"])
                            : TdError::make(TdError::kUnexpected, "send failed without error");
    }
    {
        const std::lock_guard<std::mutex> lock(mu_);
        if (outcomes_.insert_or_assign(key, std::move(outcome)).second) {
            order_.push_back(key);
        }
        while (outcomes_.size() > max_buffered_) {
            outcomes_.erase(order_.front());
            order_.pop_front();
        }
    }
    cv_.notify_all();
}

Json SendTracker::wait(int64_t chat_id, int64_t old_message_id, std::chrono::milliseconds timeout) {
    const Key key{chat_id, old_message_id};
    const auto deadline = TdClient::Clock::now() + timeout;
    std::unique_lock<std::mutex> lock(mu_);
    for (;;) {
        if (const auto it = outcomes_.find(key); it != outcomes_.end()) {
            Outcome outcome = std::move(it->second);
            outcomes_.erase(it);
            order_.erase(std::find(order_.begin(), order_.end(), key));
            if (outcome.error) {
                throw TdException(std::move(*outcome.error));
            }
            return std::move(outcome.message);
        }
        if (const auto dead = client_.dead_reason()) {
            throw TdException(*dead);
        }
        const auto now = TdClient::Clock::now();
        if (now >= deadline) {
            throw TdException(TdError::make(
                TdError::kTimeout, "no send result for message " + std::to_string(old_message_id)));
        }
        cv_.wait_for(lock, std::min<TdClient::Clock::duration>(deadline - now, kLivenessSlice));
    }
}

Json SendTracker::send(Json request, std::chrono::milliseconds call_timeout,
                       std::chrono::milliseconds send_timeout) {
    const Json sent = client_.call(std::move(request), call_timeout);
    if (!sent.contains("id") || !sent.contains("chat_id") || !sent["id"].is_number_integer() ||
        !sent["chat_id"].is_number_integer()) {
        throw TdException(TdError::make(TdError::kUnexpected, "sendMessage returned no message"));
    }
    return wait(sent["chat_id"].get<int64_t>(), sent["id"].get<int64_t>(), send_timeout);
}

std::vector<Json> SendTracker::send_album(Json request, std::chrono::milliseconds call_timeout,
                                          std::chrono::milliseconds send_timeout) {
    const Json sent = client_.call(std::move(request), call_timeout);
    if (!sent.contains("messages") || !sent["messages"].is_array()) {
        throw TdException(
            TdError::make(TdError::kUnexpected, "sendMessageAlbum returned no messages"));
    }
    const auto deadline = TdClient::Clock::now() + send_timeout;
    std::vector<Json> result;
    for (const Json& m : sent["messages"]) {
        if (!m.contains("id") || !m.contains("chat_id")) {
            throw TdException(TdError::make(TdError::kUnexpected, "malformed album message"));
        }
        const auto left = std::max(std::chrono::duration_cast<std::chrono::milliseconds>(
                                       deadline - TdClient::Clock::now()),
                                   std::chrono::milliseconds{0});
        result.push_back(wait(m["chat_id"].get<int64_t>(), m["id"].get<int64_t>(), left));
    }
    return result;
}

std::optional<Json> SendTracker::take_document(std::string_view file_name) {
    const std::lock_guard<std::mutex> lock(mu_);
    for (auto it = outcomes_.begin(); it != outcomes_.end(); ++it) {
        if (it->second.error) {
            continue;
        }
        const Json& message = it->second.message;
        if (!message.contains("content") || !message["content"].is_object() ||
            !message["content"].contains("document") ||
            !message["content"]["document"].is_object()) {
            continue;
        }
        const Json& name = message["content"]["document"].value("file_name", Json{});
        if (name.is_string() && name.get<std::string>() == file_name) {
            Json found = std::move(it->second.message);
            order_.erase(std::find(order_.begin(), order_.end(), it->first));
            outcomes_.erase(it);
            return found;
        }
    }
    return std::nullopt;
}

size_t SendTracker::buffered() const {
    const std::lock_guard<std::mutex> lock(mu_);
    return outcomes_.size();
}

}  // namespace telega::td
