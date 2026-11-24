#pragma once

// Клиент управляющего сокета (SPEC 12.4) для команд `status` и `sync`: отправить одну
// JSON-строку, прочитать одну.

#include <nlohmann/json.hpp>

#include <chrono>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>

namespace telega::app::ctl {

// Ответ сервера, разобранный по SPEC 12.4.
struct Reply {
    bool ok = false;
    nlohmann::json result;  // при ok
    std::string code;       // при !ok
    std::string message;    // при !ok
};

// Отправить строку запроса как есть (без '\n': он добавляется) и вернуть строку ответа.
// nullopt — сокета нет или его никто не слушает (ФС не смонтирована, в том числе сокет остался
// от упавшего процесса: ENOENT и ECONNREFUSED). Прочие сбои — исключение. timeout = 0 — ждать
// ответа без ограничения (sync ждёт заливки, предел ставит sync_timeout на стороне сервера).
std::optional<std::string> exchange(const std::filesystem::path& sock, std::string_view request,
                                    std::chrono::milliseconds timeout);

// Запрос команды cmd. nullopt — не смонтировано. Ответ не по протоколу — исключение.
std::optional<Reply> call(const std::filesystem::path& sock, std::string_view cmd,
                          std::chrono::milliseconds timeout = std::chrono::milliseconds{0},
                          const nlohmann::json& args = nlohmann::json::object());

}  // namespace telega::app::ctl
