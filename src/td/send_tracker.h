#pragma once

// Итог отправки сообщения (SPEC 2, 10.1). sendMessage сразу возвращает временное сообщение, а
// результат приходит апдейтом updateMessageSendSucceeded / updateMessageSendFailed с
// old_message_id = id временного сообщения. Без разбора этих апдейтов операция зависла бы
// навсегда. Нужен и TelegramStore, и бенчу.
//
// Трекер подписывается в конструкторе, то есть ДО отправки, и буферизует все итоги: апдейт мог
// прийти раньше ответа на sendMessage.

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <map>
#include <mutex>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

#include "td/td_client.h"

namespace telega::td {

class SendTracker {
  public:
    // max_buffered — сколько неразобранных итогов хранить (самые старые вытесняются).
    explicit SendTracker(TdClient& client, size_t max_buffered = 4096);
    SendTracker(const SendTracker&) = delete;
    SendTracker& operator=(const SendTracker&) = delete;
    ~SendTracker() = default;

    // Ждёт итог отправки сообщения (chat_id, old_message_id) и забирает его. Успех — итоговое
    // сообщение (объект `message` с настоящим id). Неудача — TdException с ошибкой из
    // updateMessageSendFailed (в том числе retry_after для 429), таймаут — kTimeout, закрытие
    // клиента — kClosed или kStopped.
    Json wait(int64_t chat_id, int64_t old_message_id, std::chrono::milliseconds timeout);

    // sendMessage / sendMessageAlbum и ожидание итога: request — полный запрос, call_timeout —
    // на ответ, send_timeout — на итог (считается от ответа). send возвращает итоговое
    // сообщение, send_album — итоговые сообщения в порядке альбома.
    Json send(Json request, std::chrono::milliseconds call_timeout,
              std::chrono::milliseconds send_timeout);
    std::vector<Json> send_album(Json request, std::chrono::milliseconds call_timeout,
                                 std::chrono::milliseconds send_timeout);

    // Забирает из буфера успешный итог документа с этим именем файла (nullopt, если такого нет).
    // Для отправок, у которых не пришёл даже ответ на sendMessage и потому неизвестен id
    // временного сообщения.
    std::optional<Json> take_document(std::string_view file_name);

    // Итогов в буфере (для тестов и диагностики).
    [[nodiscard]] size_t buffered() const;

  private:
    using Key = std::pair<int64_t, int64_t>;
    struct Outcome {
        Json message;
        std::optional<TdError> error;
    };

    void record(const Json& update, bool failed);

    TdClient& client_;
    const size_t max_buffered_;

    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::map<Key, Outcome> outcomes_;
    std::deque<Key> order_;

    // Последними, чтобы отписка шла раньше остальных членов.
    Subscription succeeded_;
    Subscription failed_;
};

}  // namespace telega::td
