#pragma once

// CommitManager — коммит индекса в канал (SPEC 6.6, 7.1) и отложенная уборка (SPEC 7.4).
//
// Устройство:
// - коммиты идут строго по одному (commit_mu_). Их запускает либо вызывающий поток
//   (commit_now, fsync, размонтирование), либо фоновый поток (commit_interval,
//   commit_every_chunks, повтор после ошибки с экспоненциальной задержкой);
// - источник правды о закоммиченном состоянии — state.sqlite (base_generation, head_msg_id,
//   head_hash, chain, pending_patchsets, force_snapshot, index_dirty). В памяти лежат только
//   флаги для фонового потока и кэш base_generation для Vfs;
// - Store вызывается только без блокировки MetaStore (SPEC 9): под ней делаются лишь забор
//   патча / VACUUM INTO (шаг 2) и вычисление index_dirty (шаг 8). Запись забранного патча в
//   pending_patchsets идёт после того, как блокировка MetaStore отпущена (SPEC 6.6);
// - порядок патчей в pending_patchsets совпадает с порядком забора: каждый забор (коммит или
//   промежуточный) берёт patch_queue_mu_ до захвата MetaStore и держит его до конца записи
//   патча (и до чтения pending у коммита). Снимок держит его на время VACUUM INTO, чтобы ни
//   один забранный до него патч не лёг в pending позже и не повторил уже вошедшее в снимок;
// - промежуточный забор (SPEC 6.6): фоновый поток забирает патч в pending_patchsets, когда
//   MetaStore насчитал patch_take_rows изменённых строк. Он идёт и во время заливки блоба,
//   поэтому коммит склеивает и удаляет только свои патчи (по seq);
// - порядок блокировок: commit_mu_ → patch_queue_mu_ → MetaStore → StateStore;
//   MetaStore → mu_ (dirty-хук и хук порога строк). Под mu_ не вызываются ни MetaStore, ни
//   StateStore.

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "common/bytes.h"
#include "crypto/index_blob.h"
#include "crypto/keys.h"
#include "meta/committer.h"
#include "meta/index_db.h"
#include "meta/state_db.h"
#include "store/store.h"

namespace telega::meta {

// ---------------------------------------------------------------------------------------------
// Часы фонового потока. Подменяются в тестах, чтобы не ждать commit_interval и задержек повтора.

class Clock {
  public:
    using time_point = std::chrono::steady_clock::time_point;

    Clock() = default;
    Clock(const Clock&) = delete;
    Clock& operator=(const Clock&) = delete;
    Clock(Clock&&) = delete;
    Clock& operator=(Clock&&) = delete;
    virtual ~Clock() = default;

    virtual time_point now() = 0;
    // Ждать на cv (lock захвачен) до deadline по этим часам или до уведомления; nullopt —
    // только уведомления. Ложные пробуждения допустимы: вызывающий перепроверяет условие.
    virtual void wait(std::condition_variable& cv, std::unique_lock<std::mutex>& lock,
                      std::optional<time_point> deadline) = 0;
};

// std::chrono::steady_clock.
Clock& steady_clock();

// ---------------------------------------------------------------------------------------------
// Тело блоба индекса (SPEC 6.3)

// Тело дельты до сжатия: u32 LE n, n × i64 LE msg_id цепочки, затем patchset.
Bytes encode_delta_body(const std::vector<int64_t>& chain, ByteSpan patchset);

struct DeltaBody {
    std::vector<int64_t> chain;
    Bytes patchset;
};
// Разобрать тело дельты (уже распакованное). Неверный формат — crypto::CorruptError.
DeltaBody decode_delta_body(ByteSpan body);

// Уровни zstd (SPEC 7.1, шаг 3).
inline constexpr int kSnapshotZstdLevel = 9;
inline constexpr int kDeltaZstdLevel = 3;

// Временный файл снимка на шаге 2: tmp/index-<generation>.sqlite. Живёт до шага 8: по нему
// монтирование узнаёт свой снимок, если процесс упал между pin и записью state.sqlite.
std::filesystem::path snapshot_tmp_path(const std::filesystem::path& tmp_dir, uint64_t generation);

// Поставить на MetaStore dirty-хук (SPEC 6.6): долговечно index_dirty = 1 в state.sqlite,
// затем extra (если задан). Хук вызывается под мьютексом MetaStore.
void install_dirty_hook(MetaStore& meta, StateStore& state, std::function<void()> extra = {});

// ---------------------------------------------------------------------------------------------
// Параметры и результат

// Ключи и поля заголовка, общие для всех блобов ФС (SPEC 6.3, 11).
struct BlobKeys {
    crypto::Key k_index;
    FsUuid fs_uuid{};
    crypto::KdfParams kdf;                // из keyring
    crypto::WrappedKey wrapped_master{};  // из keyring
};

struct CommitConfig {
    std::filesystem::path tmp_dir;  // <data>/tmp: снимок для VACUUM INTO и файл блоба

    std::chrono::milliseconds commit_interval{60'000};
    uint64_t commit_every_chunks = 64;
    uint32_t max_chain = 16;
    double checkpoint_ratio = 0.25;
    uint32_t retain_generations = 8;

    // Отсрочка коммита по commit_interval, пока очередь заливки не пуста (SPEC 7.1): не дольше
    // чем commit_defer_factor · commit_interval с прошлого коммита. Очередь опрашивается раз в
    // defer_recheck. Остальные условия коммита (commit_every_chunks, fsync, размонтирование)
    // идут без отсрочки.
    uint32_t commit_defer_factor = 5;
    std::chrono::milliseconds defer_recheck{1'000};

    // CAS в шаге 4 (SPEC 7.1): голова канала — 404 или более старый наш блоб, а свой последний
    // set_head был меньше cas_retry_window назад (кэш TDLib, SPEC 2). Проверка повторяется раз
    // в cas_retry_period до конца окна, и только потом это конфликт.
    std::chrono::milliseconds cas_retry_window{90'000};
    std::chrono::milliseconds cas_retry_period{10'000};

    // Задержка повтора фонового коммита после ошибки: retry_min, удваивается до retry_max.
    std::chrono::milliseconds retry_min{1'000};
    std::chrono::milliseconds retry_max{60'000};

    // Промежуточный забор патча после стольких изменённых строк (SPEC 6.6); 0 — выключен.
    uint64_t patch_take_rows = 2000;

    bool read_only = false;  // монтирование с --ro

    // Тестовый хук: вызывается после отпускания блокировки MetaStore, перед записью забранного
    // патча в pending_patchsets (под patch_queue_mu_). Исключение считается сбоем записи.
    std::function<void()> before_append_pending;
    Clock* clock = nullptr;  // nullptr — steady_clock()
};

// Замеры одного коммита (эксперимент E1, SPEC 17.4).
struct CommitMetrics {
    std::optional<crypto::BlobKind> kind;
    uint64_t generation = 0;
    int64_t msg_id = 0;
    size_t raw_bytes = 0;  // тело до zstd: patchset со списком цепочки или файл снимка
    size_t patch_bytes = 0;  // только patchset (склейка pending), у снимка 0
    size_t compressed_bytes = 0;  // после zstd
    size_t blob_bytes = 0;        // блоб целиком: 256 + body
    size_t pending_merged = 0;    // сколько pending_patchsets склеено в дельту
    TakeStats take;  // забор патча под блокировкой (дельта)
    std::chrono::nanoseconds vacuum{0};  // VACUUM INTO под блокировкой (снимок)
    std::chrono::nanoseconds compress{0};
    std::chrono::nanoseconds encrypt{0};
    std::chrono::nanoseconds upload{0};  // CAS + put_index + set_head
    std::chrono::nanoseconds total{0};
    size_t gc_chunks_deleted = 0;
    size_t gc_blobs_deleted = 0;
};

struct CommitResult {
    bool committed = false;  // false — индекс не менялся, коммит не нужен
    CommitMetrics metrics;
};

struct CommitStats {
    uint64_t commits = 0;
    uint64_t snapshots = 0;
    uint64_t deltas = 0;
    uint64_t failures = 0;
    uint64_t intermediate_takes = 0;  // промежуточные заборы патча (SPEC 6.6)
    std::optional<CommitMetrics> last;
    std::string last_error;  // без секретов
};

// ---------------------------------------------------------------------------------------------

class CommitManager final : public Committer {
  public:
    // meta, state и store должны пережить объект. Ставит свой dirty-хук на meta (с
    // уведомлением фонового потока); деструктор возвращает хук без уведомления.
    CommitManager(MetaStore& meta, StateStore& state, store::Store& store, BlobKeys keys,
                  CommitConfig config);
    CommitManager(const CommitManager&) = delete;
    CommitManager& operator=(const CommitManager&) = delete;
    CommitManager(CommitManager&&) = delete;
    CommitManager& operator=(CommitManager&&) = delete;
    ~CommitManager() override;  // stop()

    // Запустить фоновый поток. Без него коммиты идут только через commit_now/commit.
    void start();
    // Остановить фоновый поток, дождавшись текущего коммита. Повторный вызов безопасен.
    void stop();

    // --- Committer ---
    void commit_now() override;
    void chunk_uploaded() override;
    [[nodiscard]] uint64_t base_generation() const override;
    [[nodiscard]] bool read_only() const override { return read_only_.load(); }

    // Синхронный коммит с результатом и замерами. force_snapshot — следующий блоб обязан быть
    // снимком (команда snapshot управляющего сокета). Ошибка — CommitError.
    CommitResult commit(bool force_snapshot = false);

    // mount --force-local (SPEC 7.2, G_r < G_l): CAS следующего коммита идёт против реальной
    // головы канала (msg_id, generation), а не против (H, G) из state.sqlite.
    void set_cas_override(int64_t head_msg_id, uint64_t head_generation);

    // Промежуточный забор: забрать патч в pending_patchsets, не делая коммит (SPEC 6.6). Не
    // делается (nullopt), если есть неподтверждённый pin, следующий блоб обязан быть снимком,
    // файловая система read-only или забирать нечего. Сбой записи патча ставит
    // force_snapshot = 1 и бросает CommitError. Зовёт фоновый поток; открыт для тестов.
    std::optional<TakeStats> take_intermediate();

    // Очередь заливки не пуста? (SPEC 7.1: пока да, коммит по commit_interval откладывается.)
    // Зовётся из фонового потока без блокировок CommitManager. Vfs ставит пробник после
    // создания и ДОЛЖЕН снять (пустая функция) до своего уничтожения; снятие ждёт идущего
    // вызова. Без пробника очередь считается пустой.
    void set_upload_probe(std::function<bool()> busy);

    // Пересчитать сроки фонового потока (после сдвига подменённых часов в тестах).
    void poke();
    // Ждать, пока base_generation не станет ≥ generation (тесты, sync). false — таймаут.
    bool wait_for_generation(uint64_t generation, std::chrono::milliseconds timeout);

    [[nodiscard]] CommitStats stats() const;

  private:
    // Сведения о залитом блобе для шага 8.
    struct Uploaded {
        crypto::BlobKind kind = crypto::BlobKind::snapshot;
        uint64_t generation = 0;
        int64_t msg_id = 0;
        crypto::Hash32 hash{};
        int64_t blob_size = 0;
        // Наибольший seq из pending_patchsets, вошедший в блоб (дельта: склеен; снимок: уже в
        // теле). Патчи с большим seq дописал промежуточный забор, шаг 8 их не трогает.
        int64_t max_pending_seq = 0;
        // false: pin точно не прошёл (set_head вернул permanent). Тогда кэш TDLib ничего не
        // скрывает, и неподтверждённая попытка разбирается одним чтением головы (SPEC 7.1).
        bool pin_may_have_passed = true;
    };
    struct CasExpect {
        std::optional<int64_t> msg_id;  // nullopt — канал пуст (первый коммит после init)
        uint64_t generation = 0;
    };
    // Чанк, строка которого уже удалена уборкой, а сообщение — ещё нет.
    struct GcVictim {
        int64_t chunk_id = 0;
        int64_t msg_id = 0;
    };

    CommitResult commit_locked(bool force_snapshot);
    bool index_changed();
    void finish_commit(const Uploaded& up);                // шаг 8
    void gc(uint64_t generation, CommitMetrics& metrics);  // шаг 9
    size_t gc_chunks(uint64_t generation);
    size_t gc_blobs(uint64_t generation);
    void delete_victims(std::vector<GcVictim> victims);
    void mark_force_snapshot();
    void set_unconfirmed(const Uploaded& up);
    void clear_unconfirmed();
    void append_patch(const Bytes& patch);
    [[nodiscard]] bool take_due() const;
    [[nodiscard]] bool uploads_busy();
    // Голова для разбора неподтверждённого pin с повтором в окне кэша TDLib (SPEC 7.1).
    std::optional<store::HeadInfo> read_head_for_unconfirmed(const Uploaded& up);
    // CAS шага 4 с повтором после своего pin (SPEC 7.1). true — голова совпала.
    bool cas_with_retry(const std::optional<store::HeadInfo>& first, const CasExpect& expect);
    [[nodiscard]] bool is_stale_own_head(const std::optional<store::HeadInfo>& head,
                                         const CasExpect& expect) const;
    // Ждать d по часам CommitManager; false — идёт остановка.
    bool sleep_for(std::chrono::milliseconds d);
    void notify_rows();
    void on_dirty();
    void background_loop();
    void record_failure(const std::string& what);

    MetaStore& meta_;
    StateStore& state_;
    store::Store& store_;
    const BlobKeys keys_;
    const CommitConfig config_;
    Clock& clock_;

    std::mutex commit_mu_;  // один коммит за раз
    // Под commit_mu_:
    std::optional<CasExpect> cas_override_;
    std::optional<Uploaded> unconfirmed_;  // set_head или шаг 8 не удались: pin мог состояться
    std::vector<GcVictim> gc_retry_;

    // Читает фоновый поток без commit_mu_ (промежуточный забор), пишет коммит под commit_mu_.
    std::atomic<bool> unconfirmed_pin_{false};  // зеркало unconfirmed_.has_value()
    // Дублирует force_snapshot, если его не удалось записать; может поставить и сбой записи
    // патча при промежуточном заборе.
    std::atomic<bool> force_next_snapshot_{false};

    // Очередь патчей: порядок забора = порядок записи в pending_patchsets (см. начало файла).
    std::mutex patch_queue_mu_;

    // Под commit_mu_: когда мы последний раз звали set_head (pin мог состояться, даже если
    // вызов вернул ошибку). Нужно CAS-повтору (SPEC 7.1, шаг 4).
    std::optional<Clock::time_point> last_pin_;

    std::mutex probe_mu_;  // листовой: под ним зовётся только пробник
    std::function<bool()> upload_probe_;

    std::atomic<bool> reprobe_{false};  // фону нужна проба очереди заливки сейчас
    std::atomic<bool> read_only_{false};
    std::atomic<uint64_t> base_generation_{0};
    std::atomic<uint64_t> attempts_started_{0};
    std::atomic<uint64_t> last_success_attempt_{0};

    mutable std::mutex mu_;  // фоновый поток и статистика
    std::condition_variable cv_;
    // Под mu_:
    bool stopping_ = false;
    bool dirty_ = false;  // индекс менялся после забора патча последнего коммита
    uint64_t chunks_since_commit_ = 0;
    Clock::time_point last_commit_;
    std::optional<Clock::time_point> retry_at_;
    std::chrono::milliseconds backoff_{0};
    CommitStats stats_;
    uint64_t stats_committed_gen_ = 0;  // поколение последнего завершённого шага 8
    std::thread thread_;
};

}  // namespace telega::meta
