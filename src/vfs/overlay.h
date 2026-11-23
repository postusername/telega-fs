#pragma once

// Overlay — незалитые данные и атрибуты (SPEC 6.4, 8.1): строки dirty_chunks и dirty_attrs
// в state.sqlite, открытый текст грязных чанков в staging/<ino>-<idx> и их отражение в памяти
// с расписанием загрузок.
//
// Каждое изменение сначала долговечно пишется в state.sqlite, затем отражается в памяти.
// Согласованность записей одного inode с его staging-файлами обеспечивает вызывающий (Vfs):
// меняющие методы зовутся под исключительной полосой inode, читающие — хотя бы под
// разделяемой. Сами структуры в памяти — под std::shared_mutex (SPEC 9); StateStore
// вызывается без него. Флаги расписания (queued, uploading, failed) меняются без полосы,
// только под мьютексом Overlay.

#include <chrono>
#include <compare>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <map>
#include <optional>
#include <shared_mutex>
#include <span>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "meta/state_db.h"
#include "vfs/staging_budget.h"

namespace telega::vfs {

struct ChunkKey {
    int64_t ino = 0;
    int64_t idx = 0;
    auto operator<=>(const ChunkKey&) const = default;
};

// Что видно снаружи о грязном чанке.
struct DirtyView {
    int64_t version = 0;
    // Номер воплощения записи: уникален для каждого создания строки в этом процессе.
    // Строка, удалённая (truncate, unlink, инлайн) и созданная заново, — другое воплощение,
    // и залитый снимок старого к ней не применяется.
    uint64_t incarnation = 0;
    int64_t last_write_ns = 0;
    // Наименьшая длина, до которой truncate обрезал позицию после начала текущей заливки
    // (INT64_MAX — не обрезал). Залитый снимок применяется не длиннее её: иначе хвост,
    // обрезанный и снова расширенный, ожил бы в индексе (SPEC 6.2).
    int64_t cut_len = INT64_MAX;
};

// Одна позиция, затронутая записью: [from, to) — байты внутри чанка, записанные этим вызовом
// (пусто — позиция только создана, например копией инлайна).
struct ChunkWrite {
    int64_t idx = 0;
    uint64_t from = 0;
    uint64_t to = 0;
    // Длина staging-файла позиции после этой записи (SPEC 8.3): Overlay запоминает её как
    // учтённую. Место под рост уже занято вызывающим (StagingBudget).
    uint64_t file_len = 0;
};

// Цель ожидания fsync/flush_all.
struct UploadTarget {
    ChunkKey key;
    uint64_t incarnation = 0;
    int64_t version = 0;
};

// Объект пакета, чья заливка применена (SPEC 6.5).
struct PackedObject {
    ChunkKey key;  // позиция 0 файла
    uint64_t incarnation = 0;
    int64_t version = 0;
};

// План запечатывания под давлением на staging (SPEC 8.1, 8.3).
struct PressurePlan {
    // Учтённые байты чанков, которые уже освобождаются сами: в очереди, в заливке, в пакете,
    // залитые (последние учитываются, когда пакет закрыт).
    uint64_t in_flight = 0;
    // inode, которые надо запечатать, от самых старых по last_write_ns.
    std::vector<int64_t> inos;
};

struct OverlayCounts {
    size_t chunks = 0;
    size_t inodes = 0;
    size_t queued = 0;
    size_t uploading = 0;
    size_t failed = 0;
};

class Overlay {
  public:
    Overlay(meta::StateStore& state, std::filesystem::path staging_dir, int64_t chunk_size);

    // Поднять записи из state.sqlite (при старте, до любых операций).
    void load();

    // Учёт места staging (SPEC 8.3). Ставится до запуска потоков; без него Overlay места не
    // считает. Overlay возвращает место, когда убирает позицию или обрезает файл; рост
    // занимает вызывающий до записи (StagingBudget::try_acquire_user).
    void set_budget(StagingBudget* budget) { budget_ = budget; }
    // Пересчёт после восстановления: длины staging-файлов с диска. Возвращает сумму.
    uint64_t recount_staged();
    // Учтённая длина staging-файла позиции (0 — позиции нет).
    [[nodiscard]] uint64_t staged(const ChunkKey& key) const;
    // План запечатывания: освободить need_free байт, помимо того, что и так освобождается.
    // skip — inode, которые запечатывать нельзя (удалены, но открыты).
    [[nodiscard]] PressurePlan plan_pressure(uint64_t need_free,
                                             const std::unordered_set<int64_t>& skip) const;

    // Позиция 0 файла ушла из overlay (unlink, truncate, инлайн, пустой файл): строка открытого
    // пакета удаляется в той же транзакции state.sqlite, а потом вызывается hook(ino), чтобы
    // PackWriter убрал объект из своего образа. Ставится до запуска потоков.
    void set_pack_hook(std::function<void(int64_t ino)> hook);

    static std::string file_name(const ChunkKey& key);
    [[nodiscard]] std::filesystem::path path_of(const ChunkKey& key) const;
    [[nodiscard]] const std::filesystem::path& staging_dir() const noexcept { return dir_; }

    // --- чтение ---
    [[nodiscard]] std::optional<DirtyView> chunk(const ChunkKey& key) const;
    [[nodiscard]] std::vector<int64_t> indices(int64_t ino) const;
    [[nodiscard]] bool has_chunks(int64_t ino) const;
    [[nodiscard]] std::optional<meta::DirtyAttrs> attrs(int64_t ino) const;
    // Есть ли хоть один dirty_attrs (быстрая проверка перед чтением индекса).
    [[nodiscard]] bool has_attrs() const;
    // Копия всех dirty_attrs. Берётся ДО снимка индекса (см. Vfs::make_attr).
    [[nodiscard]] std::unordered_map<int64_t, meta::DirtyAttrs> attrs_snapshot() const;
    // ino, у которых есть грязные чанки / dirty_attrs.
    [[nodiscard]] std::vector<int64_t> chunk_inos() const;
    [[nodiscard]] std::vector<int64_t> attr_inos() const;
    // Позиция целиком записана с момента создания строки (для потоковой заливки).
    [[nodiscard]] bool fully_covered(const ChunkKey& key) const;
    [[nodiscard]] OverlayCounts counts() const;

    // --- изменения ---

    // Одной транзакцией state.sqlite: upsert каждой позиции (version++) и dirty_attrs.
    void commit_write(int64_t ino, std::span<const ChunkWrite> writes,
                      const meta::DirtyAttrs& attrs, int64_t now_ns);
    // version++ без записи данных: staging обрезан truncate до cut_len байт (учёт уменьшается).
    void cut(const ChunkKey& key, int64_t cut_len, int64_t now_ns);
    void set_attrs(const meta::DirtyAttrs& attrs);
    void clear_attrs(int64_t ino);
    // Удалить позиции idx ≥ first_idx (строки и staging-файлы).
    void drop_from(int64_t ino, int64_t first_idx);
    // Удалить всё о inode: позиции, staging-файлы, dirty_attrs.
    void drop_inode(int64_t ino);
    // Удалить одну позицию (строку и staging-файл), при clear_attrs и dirty_attrs inode: одной
    // транзакцией state.sqlite. persist = false — только в памяти (read-only монтирование не
    // меняет state.sqlite).
    void drop_position(const ChunkKey& key, bool clear_attrs, bool persist);
    // Залитый чанк применён (SPEC 8.1 шаги 4–5): одной транзакцией state.sqlite удалить строку,
    // если версия не изменилась (тогда и staging-файл), dirty_attrs, если clear_attrs,
    // и запись журнала загрузок. Иначе запомнить, что версия version залита.
    void finish_upload(const ChunkKey& key, uint64_t incarnation, int64_t version, bool clear_attrs,
                       int64_t chunk_id);

    // --- пакеты (SPEC 6.5) ---

    // Позиция запечатана в открытый пакет на версии version: таймер её не трогает, пока
    // version не изменилась. false — у чанка постоянная ошибка той же версии, а запрос не
    // явный (как mark_queued): в пакет не класть. Явный запрос сбрасывает ошибку.
    bool mark_packed(const ChunkKey& key, int64_t version, bool explicit_request);
    // Позиция больше не считается лежащей в открытом пакете (стала позиционной).
    void clear_packed(const ChunkKey& key);
    // Пакет не удался постоянной ошибкой: ожидающие fsync получают EIO, таймер позицию не берёт,
    // пока её не запечатают явно.
    void mark_failed(const ChunkKey& key, uint64_t incarnation, int64_t version);
    // Пакет применён к индексу (SPEC 6.5): одной транзакцией state.sqlite для каждого объекта
    // удалить строку dirty_chunks (при той же version), dirty_attrs и строку open_pack, затем
    // запись журнала загрузок. Файлы staging удаляются, ожидающие просыпаются.
    void finish_pack(int64_t chunk_id, std::span<const PackedObject> applied);

    // --- расписание загрузок ---

    // Пометить чанк к заливке. true — нужно поставить задачу в очередь (раньше не стояла).
    // Уже заливаемый чанк помечается к повтору после текущей заливки. Чанк с постоянной
    // ошибкой той же версии ставится только по явному запросу (fsync, flush_all).
    bool mark_queued(const ChunkKey& key, bool explicit_request);
    // Взять чанк из очереди в работу: версия и воплощение снимка. nullopt — строки нет.
    // Вызывается под полосой inode (снимок согласован с записью).
    std::optional<DirtyView> begin_upload(const ChunkKey& key);
    // Заливка закончилась (успешно, отменена или с ошибкой). failed_version — версия снимка
    // при постоянной ошибке. true — чанк всё ещё грязный и помечен к повтору: поставить снова.
    bool end_upload(const ChunkKey& key, uint64_t incarnation,
                    std::optional<int64_t> failed_version);
    // Ключи чанков, которые пора запечатать по таймеру: не в очереди, не заливаются, без
    // постоянной ошибки текущей версии и с последней записью не позже deadline_ns.
    [[nodiscard]] std::vector<ChunkKey> expired(int64_t deadline_ns) const;
    // Позиция ждёт, пока позиция 0 инлайн-файла не уйдёт из overlay (SPEC 6.2: у инлайн-файла
    // нет экстентов, поэтому экстент idx ≥ 1 нельзя применять раньше позиции 0). Помеченную
    // позицию таймер не берёт; take_blocked снимает пометки inode и возвращает ключи.
    void set_blocked(const ChunkKey& key, uint64_t incarnation);
    [[nodiscard]] std::vector<ChunkKey> take_blocked(int64_t ino);
    // Цели ожидания для всех чанков inode (текущие версии).
    [[nodiscard]] std::vector<UploadTarget> targets(int64_t ino) const;

    // Ждать, пока каждая цель не залита (строки нет, другое воплощение или залита версия не
    // ниже нужной), или пока одна из них не упала с постоянной ошибкой, или abort() не
    // вернёт true, или пока не наступит deadline. true — все залиты, false — ошибка, abort
    // или таймаут (timed_out выставляется только при таймауте). Загрузки при таймауте не
    // трогаются: ожидание отменяется, состояние overlay не меняется.
    bool wait(std::span<const UploadTarget> targets, const std::function<bool()>& abort,
              std::chrono::steady_clock::time_point deadline, bool& timed_out);
    void notify_all();

  private:
    struct Entry {
        std::string file;
        int64_t version = 0;
        int64_t last_write_ns = 0;
        uint64_t incarnation = 0;
        // Покрытие записями [from, to) по возрастанию, слитое; coverage_lost — перестали
        // считать (слишком много дыр).
        std::vector<std::pair<uint64_t, uint64_t>> covered;
        bool coverage_lost = false;
        bool queued = false;
        bool uploading = false;
        bool requeue = false;
        bool blocked = false;
        int64_t applied_version = 0;
        int64_t failed_version = 0;
        int64_t cut_len = INT64_MAX;
        // Версия, запечатанная в открытый пакет (0 — не в пакете). Только в памяти.
        int64_t packed_version = 0;
        // Длина staging-файла, учтённая в StagingBudget (SPEC 8.3).
        uint64_t staged = 0;
    };
    enum class TargetState : uint8_t { pending, done, failed };

    Entry& insert_locked(const ChunkKey& key, int64_t version, int64_t now_ns);
    void remove_file(const ChunkKey& key) const;
    TargetState target_state_locked(const UploadTarget& t) const;
    static void add_coverage(Entry& e, uint64_t from, uint64_t to);
    void release_staged(uint64_t bytes) const {
        if (budget_ != nullptr && bytes != 0) {
            budget_->release(bytes);
        }
    }

    meta::StateStore& state_;
    std::filesystem::path dir_;
    uint64_t chunk_size_;

    mutable std::shared_mutex mu_;
    std::condition_variable_any cv_;
    std::map<ChunkKey, Entry> chunks_;
    std::unordered_map<int64_t, meta::DirtyAttrs> attrs_;
    uint64_t next_incarnation_ = 1;
    std::function<void(int64_t)> pack_hook_;
    StagingBudget* budget_ = nullptr;
};

}  // namespace telega::vfs
