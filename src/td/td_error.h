#pragma once

// Ошибки TDLib: ответ `error` превращается в TdError, из текста ошибок лимита
// извлекается retry_after.

#include <nlohmann/json.hpp>

#include <chrono>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

namespace telega::td {

using Json = nlohmann::json;

struct TdError {
    // Коды локальных ошибок (не от TDLib): у настоящих ошибок TDLib код положительный.
    static constexpr int kTimeout = -1;  // ответ не пришёл за отведённое время
    static constexpr int kStopped = -2;  // TdRuntime остановлен
    static constexpr int kClosed = -3;  // клиент закрыт (Closed, LoggingOut) или закрывается
    static constexpr int kUnexpected = -4;  // ответ или состояние, которых мы не ждали

    int code = 0;
    std::string message;
    // Сколько ждать до повтора (FLOOD_WAIT_N, «Too Many Requests: retry after N»).
    std::optional<std::chrono::seconds> retry_after;

    // Из объекта `{"@type":"error","code":...,"message":...}` или любого объекта с полями
    // code и message. Некорректный объект даёт kUnexpected.
    static TdError from_json(const Json& error_object);
    // Из кода и текста, retry_after вычисляется.
    static TdError make(int code, std::string message);

    [[nodiscard]] bool is_local() const { return code < 0; }
    [[nodiscard]] bool is_timeout() const { return code == kTimeout; }
    // Ограничение частоты: код 429 или найден retry_after.
    [[nodiscard]] bool is_rate_limit() const { return code == 429 || retry_after.has_value(); }
    // «код: текст [retry after N s]», секретов не содержит.
    [[nodiscard]] std::string to_string() const;
};

// Разбор текста ошибки: «FLOOD_WAIT_N» (в том числе FLOOD_PREMIUM_WAIT_N) и
// «Too Many Requests: retry after N».
std::optional<std::chrono::seconds> parse_retry_after(std::string_view message);

// Исключение для блокирующих вызовов и future.
class TdException : public std::runtime_error {
  public:
    explicit TdException(TdError error);
    [[nodiscard]] const TdError& error() const noexcept { return error_; }

  private:
    TdError error_;
};

}  // namespace telega::td
