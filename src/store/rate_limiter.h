#pragma once

// Лимитер действий в канале: два token bucket и общая пауза после FLOOD_WAIT.
//
// Действие — отправка документа, pin, unpin, удаление пачки сообщений; альбом стоит
// kAlbumActions действий. Байты считаются по размеру загружаемых документов (у альбома — сумма).
// Чтение (getMessages при переборе истории, downloadFile) в бакеты не входит: у перебора своя
// пауза read_pause, а скачивание не лимитируется (E3, E4).
//
// Потокобезопасен. Ожидание блокирующее, без занятого цикла; stop() будит всех ждущих.
// Ждущие обслуживаются в порядке прихода: крупный запрос (альбом) не голодает за потоком мелких.

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <optional>
#include <random>

namespace telega::store {

struct RateLimiterOptions {
    double actions_per_minute = 12;            // send_rate (E5, режим бота)
    double actions_burst = 5;                  // ёмкость бакета действий
    uint64_t bytes_per_minute = 224ULL << 20;  // send_bytes_rate (E5)
    uint64_t bytes_burst = 64ULL << 20;  // ёмкость бакета байтов; документ крупнее
                                         // ёмкости проходит, уводя бакет в минус
    double flood_jitter = 0.10;  // к паузе FLOOD_WAIT добавляется до +10 %
    // actions_per_minute <= 0 или bytes_per_minute == 0 отключают соответствующий бакет.
};

inline constexpr double kAlbumActions = 2;  // E6: альбом из 10 документов ≈ 2 действия

class RateLimiter {
  public:
    using Clock = std::chrono::steady_clock;

    explicit RateLimiter(RateLimiterOptions options);
    // Для тестов: now_fn подменяет источник времени для расчётов (бакеты, пауза, deadline).
    // Само ожидание идёт по condition variable в реальном времени на вычисленный интервал, так
    // что подменные часы годятся для проверок без блокировки (deadline в прошлом, paused_until).
    RateLimiter(RateLimiterOptions options, std::function<Clock::time_point()> now_fn);

    RateLimiter(const RateLimiter&) = delete;
    RateLimiter& operator=(const RateLimiter&) = delete;

    // Дождаться, пока в обоих бакетах хватит токенов и не идёт пауза FLOOD_WAIT, и списать их.
    // deadline = nullopt — ждать бесконечно (фоновые загрузки).
    // false — истёк deadline или вызван stop(); токены при этом не списываются.
    [[nodiscard]] bool acquire(double actions, uint64_t bytes,
                               std::optional<Clock::time_point> deadline = std::nullopt);

    // Сервер ответил FLOOD_WAIT (любой код с retry_after): все acquire ждут
    // retry_after плюс случайный джиттер. Пауза не сокращается более коротким retry_after.
    void on_flood_wait(std::chrono::seconds retry_after);

    // Конец текущей паузы FLOOD_WAIT, если она идёт (для status).
    [[nodiscard]] std::optional<Clock::time_point> paused_until() const;

    // Разбудить и отпустить всех ждущих с false; дальнейшие acquire сразу возвращают false.
    void stop();

  private:
    void refill_locked(Clock::time_point now);
    // Когда запрос (actions, bytes) пройдёт, если паузы нет: now, если уже хватает.
    [[nodiscard]] Clock::time_point ready_at_locked(Clock::time_point now, double actions,
                                                    uint64_t bytes) const;

    RateLimiterOptions options_;
    std::function<Clock::time_point()> now_;
    mutable std::mutex mu_;
    std::condition_variable cv_;
    double actions_ = 0;  // текущий запас; стартует полным бакетом
    double bytes_ = 0;  // может уходить в минус (документ крупнее ёмкости)
    Clock::time_point last_refill_;
    std::optional<Clock::time_point> paused_until_;
    bool stopped_ = false;
    std::deque<uint64_t> queue_;  // очередь ждущих (номера билетов), обслуживается с головы
    uint64_t next_ticket_ = 1;
    std::mt19937_64 rng_;  // джиттер паузы, под mu_
};

}  // namespace telega::store
