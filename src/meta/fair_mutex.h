#pragma once

// Честный (FIFO) рекурсивный мьютекс для MetaStore (SPEC 9).
//
// std::recursive_mutex не даёт порядка: писатель в замкнутом цикле перехватывает мьютекс сразу
// после отпускания, и коммит с промежуточным забором патча ждут секунды (E11: 6,6–14,5 с на
// 1M инодов). Здесь потоки получают мьютекс строго в порядке обращения: билетная схема
// (next_ticket_ выдаёт билет, serving_ — чей черёд). Поток, уже владеющий мьютексом, входит
// повторно без очереди (счётчик рекурсии), иначе with_lock → write/take_patchset не работали бы.
//
// Внутри нет std::mutex: ожидание идёт через std::atomic::wait (futex). Это сделано не только
// ради скорости. Внутренний std::mutex с тривиальным деструктором оставался бы в графе
// блокировок TSan после разрушения объекта, а новый объект на том же адресе стека (init_fs,
// тесты) наследовал бы его рёбра: так получались ложные lock-order-inversion между
// CommitManager и MetaStore. Атомики в граф блокировок не попадают, а упорядочение данных
// остаётся видимым TSan (release в unlock → acquire в lock).
//
// Под TSan мьютекс размечен аннотациями __tsan_mutex_*: TSan видит его владельца, порядок
// захвата (lock-order-inversion между MetaStore и другими мьютексами) и «мьютекс удерживается»
// в отчётах. Конструктор и деструктор шлют __tsan_mutex_create и __tsan_mutex_destroy: без
// destroy рёбра графа блокировок пережили бы объект, и новый мьютекс на том же адресе стека
// (init_fs, тесты) унаследовал бы их, как это было бы с внутренним std::mutex. Внутри
// размеченных участков атомики TSan игнорирует, отношение «происходит до» даёт сама разметка.
//
// Мьютекс не проверяет, что unlock зовёт владелец: как у std::mutex, это забота вызывающего.
// Для std::lock_guard / std::unique_lock подходят lock() и unlock() (BasicLockable).

#include <atomic>
#include <cstdint>
#include <thread>

#if defined(__SANITIZE_THREAD__)
#define TELEGA_FAIR_MUTEX_TSAN 1
#elif defined(__has_feature)
#if __has_feature(thread_sanitizer)
#define TELEGA_FAIR_MUTEX_TSAN 1
#endif
#endif

#ifdef TELEGA_FAIR_MUTEX_TSAN
#include <sanitizer/tsan_interface.h>
#endif

namespace telega::meta {

class FairRecursiveMutex {
  public:
    FairRecursiveMutex() {
#ifdef TELEGA_FAIR_MUTEX_TSAN
        __tsan_mutex_create(this, __tsan_mutex_write_reentrant);
#endif
    }
    FairRecursiveMutex(const FairRecursiveMutex&) = delete;
    FairRecursiveMutex& operator=(const FairRecursiveMutex&) = delete;
    FairRecursiveMutex(FairRecursiveMutex&&) = delete;
    FairRecursiveMutex& operator=(FairRecursiveMutex&&) = delete;
    ~FairRecursiveMutex() {
#ifdef TELEGA_FAIR_MUTEX_TSAN
        __tsan_mutex_destroy(this, 0);
#endif
    }

    void lock() {
        const std::thread::id self = std::this_thread::get_id();
#ifdef TELEGA_FAIR_MUTEX_TSAN
        __tsan_mutex_pre_lock(this, 0);
#endif
        // owner_ равен self только если его записали мы сами (при unlock мы его очищаем), так что
        // relaxed достаточно: чужой поток не может подделать наш идентификатор.
        if (owner_.load(std::memory_order_relaxed) == self) {
            ++depth_;
#ifdef TELEGA_FAIR_MUTEX_TSAN
            __tsan_mutex_post_lock(this, 0, 0);
#endif
            return;
        }
        const uint32_t ticket = next_ticket_.fetch_add(1, std::memory_order_acq_rel);
        for (;;) {
            const uint32_t now = serving_.load(std::memory_order_acquire);
            if (now == ticket) {
                break;
            }
            serving_.wait(now, std::memory_order_acquire);  // спит, пока serving_ == now
        }
        owner_.store(self, std::memory_order_relaxed);
        depth_ = 1;
#ifdef TELEGA_FAIR_MUTEX_TSAN
        __tsan_mutex_post_lock(this, 0, 0);
#endif
    }

    void unlock() {
#ifdef TELEGA_FAIR_MUTEX_TSAN
        __tsan_mutex_pre_unlock(this, 0);
#endif
        if (--depth_ > 0) {
#ifdef TELEGA_FAIR_MUTEX_TSAN
            __tsan_mutex_post_unlock(this, 0);
#endif
            return;
        }
        owner_.store(std::thread::id{}, std::memory_order_relaxed);
        serving_.fetch_add(1, std::memory_order_release);
        // Ждут разные билеты; проснётся и пройдёт только тот, чей черёд.
        serving_.notify_all();
#ifdef TELEGA_FAIR_MUTEX_TSAN
        __tsan_mutex_post_unlock(this, 0);
#endif
    }

    // Сколько потоков стоит в очереди (билет выдан, мьютекса ещё нет). Для тестов и
    // диагностики: показание мгновенное.
    [[nodiscard]] uint64_t waiting() const {
        // Выданные билеты минус обслуженные минус текущий владелец. Билеты 32-битные и
        // сравниваются по равенству; разность по модулю 2^32 верна, пока в очереди меньше 2^32.
        const uint32_t issued = next_ticket_.load(std::memory_order_acquire);
        const uint32_t served = serving_.load(std::memory_order_acquire);
        const uint32_t outstanding = issued - served;
        const bool held = owner_.load(std::memory_order_acquire) != std::thread::id{};
        return outstanding > 0 && held ? outstanding - 1 : outstanding;
    }

  private:
    static_assert(std::atomic<std::thread::id>::is_always_lock_free);

    std::atomic<uint32_t> next_ticket_{0};  // следующий билет
    std::atomic<uint32_t> serving_{0};  // билет, которому разрешено владеть
    std::atomic<std::thread::id> owner_;
    int depth_ = 0;  // только владелец; следующему владельцу виден через serving_
};

}  // namespace telega::meta
