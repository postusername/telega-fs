#pragma once

// StagingBudget — учёт занятого места staging/ + tmp/ и обратное давление.
//
// Что считается: длины staging-файлов грязных чанков (их ведёт Overlay), файлы tmp/ в полёте
// (зашифрованные чанки и пакеты, собранные пакеты — их ведут загрузки через Lease) и «внешние»
// файлы tmp/ (снимки индекса: их создаёт коммитер, блокировать их нельзя, поэтому они только
// входят в сумму, см. set_external). Счётчик живёт в памяти и пересчитывается при
// монтировании (reset).
//
// Два вида запросов места:
// - пользовательский (write, включая RMW, который он запускает): помещается, пока занятое
//   не превысит limit − reserve. Резерв reserve = число одновременных put_chunk × размер
//   зашифрованного чанка нужен самим загрузкам, чтобы полный staging не остановил заливку;
// - внутренний (шифрование чанка, сборка пакета): помещается до limit. Такой запрос берётся
//   сразу на всю нужную сумму (без «держать и ждать»), поэтому внутренние операции не могут
//   заблокировать друг друга. Если внутренних держателей нет, а место всё равно не
//   помещается (после перезапуска с уменьшенным staging_limit занятое уже больше лимита),
//   запрос проходит поверх лимита: иначе заливка, которая одна и освобождает место, не
//   стартовала бы никогда. Такие случаи считает overcommits().
//
// Ожидание места делается вызывающим вне блокировок inode: применение пакета,
// которое освобождает место, берёт те же полосы.
//
// Все методы потокобезопасны. Мьютекс mu_ — лист: под ним не берётся ничего из Vfs.

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>

namespace telega::vfs {

struct StagingConfig {
    uint64_t limit = 0;    // staging_limit
    uint64_t high = 0;     // staging_high_watermark (уже не больше limit)
    uint64_t reserve = 0;  // R: резерв заливки
};

class StagingBudget {
  public:
    enum class WaitResult : uint8_t { ok, timeout, aborted };

    // Занятое место, выданное внутренней операции (RAII). Освобождается в деструкторе.
    class Lease {
      public:
        Lease() = default;
        Lease(const Lease&) = delete;
        Lease& operator=(const Lease&) = delete;
        Lease(Lease&& other) noexcept
            : budget_(other.budget_), bytes_(other.bytes_), internal_(other.internal_) {
            other.budget_ = nullptr;
            other.bytes_ = 0;
            other.internal_ = false;
        }
        Lease& operator=(Lease&& other) noexcept {
            if (this != &other) {
                release();
                budget_ = other.budget_;
                bytes_ = other.bytes_;
                internal_ = other.internal_;  // иначе внутренняя аренда стала бы пользовательской
                other.budget_ = nullptr;
                other.bytes_ = 0;
                other.internal_ = false;
            }
            return *this;
        }
        ~Lease() { release(); }

        [[nodiscard]] uint64_t bytes() const noexcept { return bytes_; }
        // Оставить занятыми не больше n байт (лишнее возвращается; увеличивать нельзя).
        void shrink_to(uint64_t n);
        // Вернуть всё.
        void release();
        // Отдать учёт другому владельцу (Overlay): место остаётся занятым, Lease пустеет.
        void keep() noexcept {
            budget_ = nullptr;
            bytes_ = 0;
            internal_ = false;
        }

      private:
        friend class StagingBudget;
        Lease(StagingBudget* budget, uint64_t bytes, bool internal) noexcept
            : budget_(budget), bytes_(bytes), internal_(internal) {}
        StagingBudget* budget_ = nullptr;
        uint64_t bytes_ = 0;
        bool internal_ = false;
    };

    // probe вызывается под мьютексом после каждого изменения счётчика с новой суммой «занято»:
    // тесты проверяют по нему инвариант «занятое ≤ limit». Должен быть быстрым и не звать Vfs.
    explicit StagingBudget(StagingConfig config, std::function<void(uint64_t used)> probe = {});
    StagingBudget(const StagingBudget&) = delete;
    StagingBudget& operator=(const StagingBudget&) = delete;

    [[nodiscard]] const StagingConfig& config() const noexcept { return cfg_; }
    // Сколько пользовательская запись может занять всего: limit − reserve (0, если резерв больше).
    [[nodiscard]] uint64_t user_cap() const noexcept { return user_cap_; }
    // Порог запечатывания: min(high, user_cap). Выше high запечатывание идёт сразу,
    // а выше user_cap писатели всё равно заблокированы, и ждать seal_delay им нечего.
    [[nodiscard]] uint64_t pressure_threshold() const noexcept { return threshold_; }

    // --- пользовательские запросы ---

    // Занять n байт, если занятое + n ≤ user_cap. Возвращённый Lease отдаёт учёт Overlay
    // через keep(); если запись не удалась, он сам вернёт место.
    [[nodiscard]] std::optional<Lease> try_acquire_user(uint64_t n);
    // Ждать, пока n байт не поместятся в user_cap (место не занимается: после пробуждения
    // вызывающий снова берёт полосу и пробует try_acquire_user). abort() — досрочный выход.
    [[nodiscard]] WaitResult wait_user(uint64_t n, std::chrono::steady_clock::time_point deadline,
                                       const std::function<bool()>& abort);
    // Запрос заведомо не поместится даже при пустом staging.
    [[nodiscard]] bool never_fits_user(uint64_t n) const noexcept { return n > user_cap_; }

    // --- внутренние запросы ---

    // Занять n байт до limit; ждёт, пока не поместится. nullopt — abort() вернул true.
    [[nodiscard]] std::optional<Lease> acquire_internal(uint64_t n,
                                                        const std::function<bool()>& abort);

    // --- учёт ---

    // Занять без проверки лимита и без ожидания: малые ограниченные объёмы под полосой
    // (восстановление позиции 0 инлайн-файла, не больше inline_max).
    void charge_unchecked(uint64_t n);
    // Вернуть n байт (с насыщением: после reset учёт не уходит ниже нуля).
    void release(uint64_t n);
    // Пересчёт при монтировании: занято ровно столько.
    void reset(uint64_t used);
    // Файлы tmp/, которые создаёт не Vfs (снимки индекса): только входят в сумму.
    void set_external(uint64_t bytes);
    // Разбудить ждущих (остановка, фатальная ошибка).
    void notify_all();

    [[nodiscard]] uint64_t used() const;
    [[nodiscard]] uint64_t peak() const;
    [[nodiscard]] uint64_t overcommits() const;
    // Сколько из занятого держат внутренние операции (Lease): оно освободится само, когда они
    // закончат, и запечатыванием его не освободить.
    [[nodiscard]] uint64_t leased() const;
    // Занятое + extra выше порога запечатывания.
    [[nodiscard]] bool above_threshold(uint64_t extra = 0) const;

  private:
    [[nodiscard]] uint64_t total_locked() const { return used_ + external_; }
    void changed_locked();
    void release_internal(uint64_t n, bool internal);
    void shrink(uint64_t n, uint64_t to, bool internal);

    StagingConfig cfg_;
    uint64_t user_cap_ = 0;
    uint64_t threshold_ = 0;
    std::function<void(uint64_t)> probe_;

    mutable std::mutex mu_;
    std::condition_variable cv_;
    uint64_t used_ = 0;
    uint64_t external_ = 0;
    uint64_t peak_ = 0;
    uint64_t internal_leases_ = 0;  // число живых внутренних Lease
    uint64_t leased_ = 0;           // их байты
    uint64_t overcommits_ = 0;
};

}  // namespace telega::vfs
