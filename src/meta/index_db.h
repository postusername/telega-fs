#pragma once

// MetaStore — владелец index.sqlite (SPEC 6.2, 6.6, 9).
//
// Устройство:
// - одно писательское соединение под мьютексом; на нём всегда открыта сессия session
//   extension, подключённая ко всем таблицам. Из неё коммит забирает patchset (дельта) или
//   при фиксации среза снимка (cut_snapshot) просто пересоздаёт её;
// - читательские соединения только для чтения, по одному на поток (WAL позволяет читать
//   параллельно с писателем). Соединение потока создаётся при первом read и живёт до
//   уничтожения MetaStore;
// - строки, изменённые с последнего забора патча, считаются (rows_since_take): по ним
//   CommitManager решает, когда делать промежуточный забор (SPEC 6.6);
// - перед первой пользовательской записью после забора патча (или после открытия) вызывается
//   dirty-хук: в продукте он долговечно ставит index_dirty = 1 в state.sqlite (SPEC 6.6).
//   Служебные записи (WriteKind::housekeeping: строка index_history, удаление строк chunks
//   уборкой) хук не вызывают, но попадают в сессию.
//
// Порядок блокировок (SPEC 9): мьютекс MetaStore берётся раньше блокировок overlay и
// StateStore. Вызывать Store под ним НЕ ДОПУСКАЕТСЯ. Мьютекс рекурсивный и честный (FIFO,
// fair_mutex.h): внутри with_lock можно звать write, take_patchset и cut_snapshot; внутри
// write — нельзя (logic_error).
//
// Снимок индекса (SPEC 6.6, «Снимок без блокировки записи»): под мьютексом только cut_snapshot
// (пересоздание сессии и фиксация среза читательской транзакцией), копирование идёт после
// блокировки через SnapshotCopy (snapshot.h).

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <utility>

#include "common/bytes.h"
#include "meta/errors.h"
#include "meta/fair_mutex.h"
#include "meta/snapshot.h"
#include "meta/sqlite.h"

namespace telega::meta {

inline constexpr int64_t kFormatVersion = 1;

// PRAGMA journal_size_limit писательского соединения (SPEC 6.6): пока идёт копирование среза,
// контрольная точка WAL стоит, и файл растёт на объём записей; лимит усекает его после неё.
inline constexpr int64_t kJournalSizeLimit = 64LL << 20;

// Параметры init (SPEC 12.2): пишутся в meta и потом не меняются.
struct InitParams {
    FsUuid fs_uuid{};
    int64_t chunk_size = 16LL << 20;
    int64_t inline_max = 4LL << 10;
    int64_t inline_budget = 16LL << 20;
    int64_t pack_threshold = 1LL << 20;
    int64_t pack_target = 16LL << 20;
    uint32_t root_uid = 0;
    uint32_t root_gid = 0;
    uint32_t root_mode = 0755;
    int64_t now_ns = 0;
};

enum class WriteKind : uint8_t {
    user,  // обычная операция ФС: ставит dirty (SPEC 6.6)
    housekeeping,  // служебная запись коммита или уборки: dirty не ставит
};

// Замер забора патча (для эксперимента E1, SPEC 17.4).
struct TakeStats {
    std::chrono::nanoseconds wait{0};    // ожидание мьютекса
    std::chrono::nanoseconds locked{0};  // время под мьютексом
    size_t patch_bytes = 0;
};

class MetaStore {
  public:
    using DirtyHook = std::function<void()>;

    // Создать новый index.sqlite: схема 6.2, ключи meta, корень (ino 1). Файл пишется рядом
    // и переименовывается, path не должен существовать (иначе std::runtime_error).
    static void create(const std::filesystem::path& path, const InitParams& params);

    // Открыть существующий файл. Хук можно задать и позже (set_dirty_hook).
    explicit MetaStore(std::filesystem::path path, DirtyHook dirty_hook = {});
    MetaStore(const MetaStore&) = delete;
    MetaStore& operator=(const MetaStore&) = delete;
    MetaStore(MetaStore&&) = delete;
    MetaStore& operator=(MetaStore&&) = delete;
    ~MetaStore();

    void set_dirty_hook(DirtyHook hook);

    // Выполнить fn(Db& writer) под мьютексом в транзакции BEGIN IMMEDIATE и закоммитить.
    // Исключение из fn — ROLLBACK и проброс. Для kind = user перед первой записью после
    // take_patchset/cut_snapshot/vacuum_into (или открытия) вызывается dirty-хук (его исключение
    // отменяет запись) и выставляется dirty_since_take.
    template <typename F>
    auto write(F&& fn, WriteKind kind = WriteKind::user) -> std::invoke_result_t<F&, Db&> {
        using R = std::invoke_result_t<F&, Db&>;
        if constexpr (std::is_void_v<R>) {
            write_impl([&](Db& db) { fn(db); }, kind);
        } else {
            std::optional<R> out;
            write_impl([&](Db& db) { out.emplace(fn(db)); }, kind);
            if (!out) {
                throw std::logic_error("callback did not produce a value");
            }
            return std::move(*out);
        }
    }

    // Выполнить fn(Db& reader) на читательском соединении текущего потока в одной читающей
    // транзакции (согласованный снимок). Вложенные read на том же потоке идут в ту же
    // транзакцию. Записи, сделанные внутри текущего write, отсюда не видны.
    template <typename F>
    auto read(F&& fn) -> std::invoke_result_t<F&, Db&> {
        using R = std::invoke_result_t<F&, Db&>;
        if constexpr (std::is_void_v<R>) {
            read_impl([&](Db& db) { fn(db); });
        } else {
            std::optional<R> out;
            read_impl([&](Db& db) { out.emplace(fn(db)); });
            if (!out) {
                throw std::logic_error("callback did not produce a value");
            }
            return std::move(*out);
        }
    }

    // Выполнить fn() под мьютексом записи (например, CommitManager на шаге 8 пишет
    // index_dirty = dirty_since_take() атомарно относительно записей).
    template <typename F>
    auto with_lock(F&& fn) -> std::invoke_result_t<F&> {
        const std::lock_guard lock(mutex_);
        return fn();
    }

    // Сколько потоков стоит в очереди на мьютекс записи (тесты честности, диагностика).
    [[nodiscard]] uint64_t lock_waiters() const { return mutex_.waiting(); }

    // Забрать patchset сессии и пересоздать её, сбросить dirty_since_take и счётчик строк
    // (SPEC 7.1, шаг 2). Пустой результат — изменений не было. stats, если задан, получает время
    // ожидания и удержания мьютекса и размер патча. Запись патча в pending_patchsets делает
    // вызывающий уже без блокировки MetaStore (SPEC 6.6).
    Bytes take_patchset(TakeStats* stats = nullptr);

    // Сколько строк изменено удачными записями (user и housekeeping) с последнего
    // take_patchset/cut_snapshot/vacuum_into (SPEC 6.6, промежуточный забор). Оценка сверху: одна
    // строка, изменённая дважды, считается дважды.
    [[nodiscard]] int64_t rows_since_take() const noexcept { return rows_.load(); }

    // Хук порога: вызывается под мьютексом, когда rows_since_take() переходит через threshold
    // вверх (threshold ≤ 0 или пустой hook — выключено). Хук должен быть лёгким и не звать
    // MetaStore.
    void set_rows_hook(int64_t threshold, std::function<void()> hook);

    // Фиксация среза снимка (SPEC 6.6, 7.1 шаг 2): под мьютексом пересоздать сессию БЕЗ забора
    // патча (строки сессии уже в срезе), сбросить dirty_since_take и счётчик строк и на
    // читательском соединении снимка выполнить BEGIN с чтением страницы. Срез приходится ровно на
    // точку пересоздания сессии: записи после неё попадут в новую сессию, то есть в следующую
    // дельту. Время под мьютексом не зависит от размера индекса. Читательская транзакция
    // остаётся открытой: закрыть её (SnapshotCopy::copy или SnapshotReader::end) обязан
    // вызывающий. Ошибка — исключение, в этом случае сессия и счётчики не тронуты, транзакция
    // закрыта. Вызывать из write нельзя (logic_error). stats, если задан, получает ожидание и
    // удержание мьютекса (patch_bytes = 0). Запись патча не делается: при сбое снимка
    // вызывающий ставит force_snapshot (SPEC 7.1).
    void cut_snapshot(SnapshotReader& reader, TakeStats* stats = nullptr);

    // VACUUM INTO path под мьютексом (path не должен существовать), пересоздать сессию, сбросить
    // dirty_since_take. Держит блокировку на всё время копии (около 4 с на 1M инодов), поэтому
    // коммит его не использует (SPEC 6.6: вместо него cut_snapshot и SnapshotCopy); остаётся
    // для тестов и инструментов. Результат может быть не в WAL-режиме.
    void vacuum_into(const std::filesystem::path& path);

    // Была ли пользовательская запись после последнего take_patchset/cut_snapshot/vacuum_into.
    [[nodiscard]] bool dirty_since_take() const noexcept { return dirty_.load(); }

    [[nodiscard]] const std::filesystem::path& path() const noexcept { return path_; }

    // --- дельта-журнал (SPEC 6.6) ---

    // Склеить патчи по порядку через sqlite3changegroup. Порча — IndexCorrupt.
    static Bytes concat_patchsets(std::span<const Bytes> patches);
    // Применить патч к db (вызывающий заранее выключает foreign_keys). Любой конфликт —
    // SQLITE_CHANGESET_ABORT, патч не применяется, бросается IndexCorrupt.
    static void apply_patchset(Db& db, ByteSpan patch);
    // PRAGMA foreign_key_check, PRAGMA integrity_check и наличие ключей meta с
    // format_version = 1. Иначе IndexCorrupt.
    static void verify(Db& db);
    // Собрать index.sqlite из цепочки: скопировать снимок, применить патчи по порядку в одной
    // транзакции с выключенными foreign_keys, проверить (verify), перевести в WAL и атомарно
    // положить в out_path (tmp + rename + fsync каталога; старые -wal/-shm удаляются).
    // out_path не должен быть открыт. Порча — IndexCorrupt, out_path тогда не меняется.
    static void build_from_chain(const std::filesystem::path& snapshot_file,
                                 std::span<const Bytes> patches,
                                 const std::filesystem::path& out_path);
    // Каноничное текстовое представление схемы и всех таблиц, строки по первичному ключу.
    static std::string dump(Db& db);

  private:
    struct Reader {
        Db db;
        int depth = 0;
    };

    void write_impl(const std::function<void(Db&)>& fn, WriteKind kind);
    void read_impl(const std::function<void(Db&)>& fn);
    Reader& reader_for_this_thread();
    void reset_session();

    std::filesystem::path path_;

    FairRecursiveMutex mutex_;  // писательское соединение, сессия, хук; честный (SPEC 9)
    std::unique_ptr<Db> writer_;
    std::unique_ptr<Session> session_;  // уничтожается раньше writer_
    DirtyHook dirty_hook_;
    std::atomic<bool> dirty_{false};
    std::atomic<int64_t> rows_{0};  // пишется под mutex_
    int64_t rows_threshold_ = 0;
    std::function<void()> rows_hook_;
    bool in_write_ = false;

    std::mutex readers_mutex_;  // только таблица readers_; сами соединения — у своих потоков
    std::unordered_map<std::thread::id, std::unique_ptr<Reader>> readers_;
};

}  // namespace telega::meta
