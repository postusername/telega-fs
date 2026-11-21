#pragma once

// TdRuntime (SPEC 10.1): один поток приёма на процесс. td_receive у процесса общий для всех
// клиентов, поэтому поток разбирает сообщение и по @client_id отдаёт его нужному TdClient. Тот же
// поток снимает запросы с истёкшим таймаутом.

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>

#include "td/td_client.h"
#include "td/td_transport.h"

namespace telega::td {

struct TdRuntimeStats {
    uint64_t messages = 0;        // принято сообщений
    uint64_t malformed = 0;       // не разобрались как JSON-объект
    uint64_t unknown_client = 0;  // сообщений для неизвестного client_id
};

class TdRuntime {
  public:
    // poll — максимальное ожидание td_receive: им ограничены задержка остановки и точность
    // таймаутов запросов.
    explicit TdRuntime(std::shared_ptr<TdTransport> transport,
                       std::chrono::milliseconds poll = std::chrono::milliseconds{100});
    TdRuntime(const TdRuntime&) = delete;
    TdRuntime& operator=(const TdRuntime&) = delete;
    ~TdRuntime();

    // Новый клиент TDLib. Поток приёма стартует при первом вызове. Рантайм держит клиента до
    // authorizationStateClosed или stop(), поэтому потерять незакрытого клиента нельзя.
    // После stop() бросает TdException(kStopped).
    std::shared_ptr<TdClient> create_client();

    // Останавливает поток приёма, все висящие запросы всех клиентов получают kStopped. Клиенты
    // TDLib при этом НЕ закрываются: закрывать надо до stop() (TdClient::close). Идемпотентна.
    void stop();

    [[nodiscard]] const std::shared_ptr<TdTransport>& transport() const { return transport_; }
    [[nodiscard]] TdRuntimeStats stats() const;

  private:
    void run();
    void handle(const std::string& raw);
    void expire_pending();

    std::shared_ptr<TdTransport> transport_;
    const std::chrono::milliseconds poll_;

    std::mutex mu_;
    std::unordered_map<int, std::shared_ptr<TdClient>> clients_;
    std::thread thread_;
    std::atomic<bool> stop_{false};
    bool stopped_ = false;

    std::atomic<uint64_t> messages_{0};
    std::atomic<uint64_t> malformed_{0};
    std::atomic<uint64_t> unknown_client_{0};
};

}  // namespace telega::td
