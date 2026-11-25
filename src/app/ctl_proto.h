#pragma once

// Протокол управляющего сокета: одна JSON-строка запроса, одна JSON-строка ответа.
//
//   запрос:  {"proto": 1, "cmd": "<имя>", "args": {...}}
//   успех:   {"proto": 1, "ok": true,  "result": {...}}
//   ошибка:  {"proto": 1, "ok": false, "error": {"code": "...", "message": "..."}}
//
// Здесь только разбор и сборка сообщений и диспетчеризация по именам команд: ни сокетов, ни
// потоков (они в ctl_socket.h), поэтому всё проверяется юнит-тестами без сети.

#include <nlohmann/json.hpp>

#include <functional>
#include <stdexcept>
#include <string>
#include <string_view>

namespace telega::app::ctl {

inline constexpr int kProtoVersion = 1;
// Больше этого размера строка запроса не принимается (в запросах v1 нет ничего крупного).
inline constexpr size_t kMaxLineBytes = size_t{64} * 1024;

// Коды ошибок ответа (протокол фиксирует только `unsupported`; остальные — наши).
namespace code {
inline constexpr std::string_view unsupported = "unsupported";  // версия протокола или команда
inline constexpr std::string_view bad_request = "bad_request";  // не JSON или неверная форма
inline constexpr std::string_view read_only = "read_only";  // ФС в read-only, коммит невозможен
inline constexpr std::string_view io = "io";  // заливка или коммит не удались (EIO)
inline constexpr std::string_view shutting_down = "shutting_down";  // идёт размонтирование
inline constexpr std::string_view busy = "busy";  // слишком много клиентов
inline constexpr std::string_view internal = "internal";
}  // namespace code

// Ошибка обработчика команды: уходит клиенту как {"ok": false, "error": {...}}.
class Error : public std::runtime_error {
  public:
    Error(std::string_view code, const std::string& message)
        : std::runtime_error(message), code_(code) {}
    [[nodiscard]] const std::string& code() const noexcept { return code_; }

  private:
    std::string code_;
};

// Обработчики команд v1. Каждый возвращает объект `result` или бросает Error; любое другое
// исключение превращается в `internal`. Пустой обработчик — команда недоступна (`unsupported`).
// Вызываются из потоков сервера одновременно, поэтому должны быть потокобезопасными.
struct Handlers {
    std::function<nlohmann::json()> status;
    std::function<nlohmann::json()> sync;
    std::function<nlohmann::json()> snapshot;
};

// Собрать ответ-успех и ответ-ошибку: одна строка без перевода строки.
std::string make_ok(const nlohmann::json& result);
std::string make_error(std::string_view code, std::string_view message);

// Обработать одну строку запроса (без перевода строки) и вернуть строку ответа. Не бросает.
std::string dispatch(std::string_view line, const Handlers& handlers);

}  // namespace telega::app::ctl
