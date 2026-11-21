#pragma once

// Транспорт TDLib (SPEC 10.1): тонкая обёртка над td_create_client_id / td_send / td_receive /
// td_execute. Всё, что выше (TdRuntime, TdClient, авторизация), работает только через этот
// интерфейс и поэтому тестируется на фейке без TDLib. Настоящая реализация — telega_tdjson.

#include <chrono>
#include <optional>
#include <string>

namespace telega::td {

class TdTransport {
  public:
    TdTransport() = default;
    TdTransport(const TdTransport&) = delete;
    TdTransport& operator=(const TdTransport&) = delete;
    virtual ~TdTransport() = default;

    // Новый экземпляр TDLib. Апдейты он начинает слать после первого send.
    virtual int create_client_id() = 0;

    // Отправка запроса (JSON). Можно звать из любого потока.
    virtual void send(int client_id, const std::string& request_json) = 0;

    // Следующее сообщение любого клиента или nullopt по истечении timeout. Вызывается только из
    // одного потока (потока приёма TdRuntime).
    virtual std::optional<std::string> receive(std::chrono::milliseconds timeout) = 0;

    // Синхронный запрос (td_execute): setLogStream, getOption и другие, помеченные в схеме
    // «Can be called synchronously». Пустая строка — TDLib ничего не вернул.
    virtual std::string execute(const std::string& request_json) = 0;
};

}  // namespace telega::td
