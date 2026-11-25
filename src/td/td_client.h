#pragma once

// TdClient: один экземпляр TDLib. Запросы с @extra и таймаутом, подписки на апдейты
// по @type, слежение за состоянием авторизации. Создаётся через TdRuntime::create_client().
//
// Потоки. Ответы и апдейты разбираются в потоке приёма TdRuntime. Обработчики подписок вызываются
// именно в нём, поэтому они только перекладывают данные и будят ожидающих, блокирующая работа в
// них НЕ ДОПУСКАЕТСЯ. Всё остальное можно звать из любого потока, но не из обработчика подписки:
// блокирующий call() оттуда зависнет до таймаута.

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

#include "td/td_error.h"
#include "td/td_transport.h"

namespace telega::td {

class SubscriptionRegistry;

// Владелец подписки. Деструктор и reset() отписывают и дожидаются конца уже идущего вызова
// обработчика (если он идёт в потоке приёма; из самого обработчика отписка тоже безопасна).
// Нельзя отписываться, держа мьютекс, который берёт обработчик.
class Subscription {
  public:
    Subscription() = default;
    Subscription(const Subscription&) = delete;
    Subscription& operator=(const Subscription&) = delete;
    Subscription(Subscription&& other) noexcept;
    Subscription& operator=(Subscription&& other) noexcept;
    ~Subscription();

    void reset();
    [[nodiscard]] bool active() const { return id_ != 0; }

  private:
    friend class TdClient;
    std::weak_ptr<SubscriptionRegistry> registry_;
    std::string type_;
    uint64_t id_ = 0;
};

// Состояние авторизации: seq растёт с каждым updateAuthorizationState (начиная с 1).
struct AuthState {
    uint64_t seq = 0;
    Json state;  // объект authorizationState*, поле @type всегда есть

    [[nodiscard]] std::string type() const { return state.value("@type", std::string{}); }
};

struct TdClientStats {
    uint64_t requests = 0;        // отправлено запросов
    uint64_t timeouts = 0;        // запросов, закончившихся таймаутом
    uint64_t late_responses = 0;  // ответов без ожидающего запроса (запоздавших) — отброшено
    uint64_t updates = 0;         // принято апдейтов
    uint64_t dropped_updates = 0;  // апдейтов без подписчиков — отброшено
    uint64_t handler_failures = 0;  // обработчиков, бросивших исключение
};

// Итог запроса: ответ или ошибка. Ошибка передаётся значением, а не через exception_ptr:
// счётчик ссылок exception_ptr живёт в неинструментированной libstdc++, и TSan видел ложную гонку
// между потоком приёма и вызывающим (в CI). Исключение бросается уже в вызывающем потоке.
struct TdOutcome {
    Json value;
    std::optional<TdError> error;
};

// Будущий ответ call_async. get() бросает TdException в вызывающем потоке.
class TdFuture {
  public:
    TdFuture() = default;
    explicit TdFuture(std::future<TdOutcome> future) : future_(std::move(future)) {}

    [[nodiscard]] bool valid() const { return future_.valid(); }
    void wait() const { future_.wait(); }
    template <class Rep, class Period>
    [[nodiscard]] std::future_status wait_for(const std::chrono::duration<Rep, Period>& d) const {
        return future_.wait_for(d);
    }
    Json get();

  private:
    std::future<TdOutcome> future_;
};

class TdClient {
  public:
    using Handler = std::function<void(const Json&)>;
    using Clock = std::chrono::steady_clock;
    static constexpr std::chrono::milliseconds kDefaultTimeout{30'000};

    TdClient(std::shared_ptr<TdTransport> transport, int client_id);
    TdClient(const TdClient&) = delete;
    TdClient& operator=(const TdClient&) = delete;
    ~TdClient();

    [[nodiscard]] int id() const { return client_id_; }

    // Запрос и ожидание ответа. Ошибка TDLib, таймаут, остановка рантайма и закрытие клиента
    // приходят как TdException (error().code: у локальных ошибок отрицательный, TdError::k*).
    // Запоздавший после таймаута ответ отбрасывается. Поле @extra запроса перезаписывается.
    Json call(Json request, std::chrono::milliseconds timeout = kDefaultTimeout);

    // То же без блокировки: get() бросает TdException. Таймаут считается от отправки,
    // срабатывает в потоке приёма (точность — период опроса TdRuntime).
    TdFuture call_async(Json request, std::chrono::milliseconds timeout = kDefaultTimeout);

    // Обработчик для апдейтов типа `type` (например "updateFile"). Апдейты без подписчиков
    // отбрасываются, поэтому подписываться надо ДО действия, которое их вызовет.
    [[nodiscard]] Subscription subscribe(const std::string& type, Handler handler);

    // Состояние авторизации. До первого апдейта nullopt.
    [[nodiscard]] std::optional<AuthState> auth_state() const;
    // Ждёт состояние с seq > after_seq. TdException при таймауте и остановке рантайма. Если
    // клиент закрыт, возвращается Closed-состояние.
    AuthState wait_auth_state(uint64_t after_seq, std::chrono::milliseconds timeout);
    // Запоминает состояние из ответа getAuthorizationState, если апдейтов ещё не было. Первый
    // апдейт после этого с тем же состоянием не считается новым (seq не растёт): ответ мог
    // опередить апдейт.
    void seed_auth_state(const Json& state);

    // Закрытие: `close` / `logOut` и ожидание authorizationStateClosed. false —
    // не успели за timeout. Закрыть клиент надо до выхода из процесса, иначе TDLib не
    // сбросит данные.
    bool close(std::chrono::milliseconds timeout = std::chrono::seconds{30});
    bool log_out(std::chrono::milliseconds timeout = std::chrono::seconds{30});
    [[nodiscard]] bool closed() const;
    // nullopt, пока клиент жив; иначе причина: kClosed (Closed) или kStopped (рантайм остановлен).
    [[nodiscard]] std::optional<TdError> dead_reason() const;

    [[nodiscard]] TdClientStats stats() const;

  private:
    friend class TdRuntime;

    struct Pending {
        std::promise<TdOutcome> promise;
        Clock::time_point deadline;
    };
    struct Submitted {
        uint64_t extra;
        std::future<TdOutcome> future;
    };

    Submitted submit(Json request, std::chrono::milliseconds timeout);
    void cancel(uint64_t extra, TdError error);
    bool close_with(const char* request_type, std::chrono::milliseconds timeout);
    bool wait_closed(std::chrono::milliseconds timeout);

    // Интерфейс потока приёма (TdRuntime).
    void on_message(Json message);
    void expire(Clock::time_point now);
    void shutdown(const TdError& reason);

    void on_auth_update(const Json& update);
    void fail_all(const TdError& reason);

    std::shared_ptr<TdTransport> transport_;
    const int client_id_;
    std::shared_ptr<SubscriptionRegistry> registry_;

    mutable std::mutex mu_;
    std::condition_variable cv_;
    uint64_t next_extra_ = 1;
    std::unordered_map<uint64_t, Pending> pending_;
    std::optional<AuthState> auth_;
    std::optional<Json>
        seeded_state_;  // состояние из seed_auth_state до первого апдейта после него
    bool closed_ = false;
    bool stopped_ = false;

    std::atomic<uint64_t> requests_{0};
    std::atomic<uint64_t> timeouts_{0};
    std::atomic<uint64_t> late_responses_{0};
    std::atomic<uint64_t> updates_{0};
    std::atomic<uint64_t> dropped_updates_{0};
    std::atomic<uint64_t> handler_failures_{0};
};

}  // namespace telega::td
