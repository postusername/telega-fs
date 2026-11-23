#pragma once

// StateStore — локальное состояние state.sqlite (SPEC 6.4): ключи коммита, незалитые патчи,
// текущая цепочка, overlay (грязные чанки и атрибуты), журнал загрузок, открытый пакет.
// В облако не попадает.
//
// Одно соединение под рекурсивным мьютексом: StateStore можно звать из нескольких потоков,
// а внутри transaction(fn) — вызывать любые методы того же объекта. Каждый метод без
// внешней транзакции — отдельная долговечная транзакция (WAL, synchronous = FULL).
// Порядок блокировок (SPEC 9): мьютекс MetaStore, если нужен, берётся раньше этого.

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/bytes.h"
#include "meta/sqlite.h"

namespace telega::meta {

// Ключи таблицы state (SPEC 6.4).
namespace state_key {
inline constexpr std::string_view fs_uuid = "fs_uuid";                  // BLOB 16 байт
inline constexpr std::string_view base_generation = "base_generation";  // INTEGER
inline constexpr std::string_view head_msg_id = "head_msg_id";          // INTEGER
inline constexpr std::string_view head_hash = "head_hash";              // BLOB 32 байта
inline constexpr std::string_view clean_shutdown = "clean_shutdown";    // INTEGER 0/1
inline constexpr std::string_view force_snapshot = "force_snapshot";    // INTEGER 0/1
inline constexpr std::string_view index_dirty = "index_dirty";          // INTEGER 0/1
}  // namespace state_key

struct PendingPatchset {
    int64_t seq = 0;
    Bytes patchset;
};

struct ChainElement {
    int64_t pos = 0;  // 0 — снимок
    int64_t msg_id = 0;
    int64_t blob_size = 0;
};

struct DirtyChunk {
    int64_t ino = 0;
    int64_t idx = 0;
    std::string path;  // файл в staging/
    int64_t version = 0;
    int64_t last_write_ns = 0;
};

struct DirtyAttrs {
    int64_t ino = 0;
    int64_t size = 0;
    int64_t mtime_ns = 0;
};

struct UploadRecord {
    int64_t chunk_id = 0;
    std::optional<int64_t> msg_id;  // nullopt, пока загрузка не завершилась
    int64_t started_ns = 0;
};

// Объект открытого пакета (SPEC 6.4, 6.5).
struct OpenPackRow {
    int64_t ino = 0;
    int64_t version = 0;  // version грязного чанка позиции 0 на момент добавления
    int64_t len = 0;  // размер объекта на момент добавления
    int64_t added_ns = 0;  // возраст пакета считается от самого старого объекта
};

class StateStore {
  public:
    // Создать state.sqlite со схемой 6.4. path не должен существовать.
    static void create(const std::filesystem::path& path);
    // Открыть существующий файл. Схема M2 мигрирует: open_pack пересоздаётся (она была пуста),
    // read_cache добавляется (SPEC 6.4).
    // migrate = false (--ro): схема не меняется, state.sqlite остаётся нетронутым (SPEC 7.2).
    explicit StateStore(const std::filesystem::path& path, bool migrate = true);
    StateStore(const StateStore&) = delete;
    StateStore& operator=(const StateStore&) = delete;
    StateStore(StateStore&&) = delete;
    StateStore& operator=(StateStore&&) = delete;
    ~StateStore();

    // Выполнить fn() атомарно: одна транзакция BEGIN IMMEDIATE под мьютексом; вложенный
    // вызов — точка сохранения. Исключение — откат и проброс. Шаг 8 из SPEC 7.1 делается так.
    template <typename F>
    auto transaction(F&& fn) -> std::invoke_result_t<F&> {
        using R = std::invoke_result_t<F&>;
        if constexpr (std::is_void_v<R>) {
            transaction_impl([&] { fn(); });
        } else {
            std::optional<R> out;
            transaction_impl([&] { out.emplace(fn()); });
            if (!out) {
                throw std::logic_error("callback did not produce a value");
            }
            return std::move(*out);
        }
    }

    // --- state (ключ–значение) ---
    [[nodiscard]] std::optional<int64_t> get_int(std::string_view key);
    // Значение по умолчанию, если ключа нет.
    [[nodiscard]] int64_t get_int_or(std::string_view key, int64_t fallback);
    void set_int(std::string_view key, int64_t value);
    [[nodiscard]] std::optional<Bytes> get_blob(std::string_view key);
    void set_blob(std::string_view key, ByteSpan value);
    void erase(std::string_view key);

    // --- pending_patchsets ---
    // Добавить патч, вернуть его seq (растёт).
    int64_t append_pending(ByteSpan patchset);
    // Все патчи по возрастанию seq.
    [[nodiscard]] std::vector<PendingPatchset> list_pending();
    // Удалить патчи с seq ≤ max_seq (включённые в успешный коммит).
    void delete_pending_upto(int64_t max_seq);
    // Наибольший seq в pending_patchsets; 0, если таблица пуста (seq растёт с 1).
    [[nodiscard]] int64_t max_pending_seq();
    void clear_pending();

    // --- chain ---
    [[nodiscard]] std::vector<ChainElement> list_chain();
    // Добавить элемент в конец цепочки, вернуть его pos. Пустая цепочка — logic_error:
    // первым элементом всегда идёт снимок (reset_chain_to_snapshot).
    int64_t append_chain(int64_t msg_id, int64_t blob_size);
    // Очистить цепочку и записать снимок элементом 0.
    void reset_chain_to_snapshot(int64_t msg_id, int64_t blob_size);

    // --- dirty_chunks ---
    // Вставить или обновить запись. version берётся из монотонного счётчика StateStore (растёт
    // строго, не сбрасывается при пересоздании строки). Вернуть version.
    int64_t upsert_dirty_chunk(int64_t ino, int64_t idx, const std::string& path,
                               int64_t last_write_ns);
    [[nodiscard]] std::optional<DirtyChunk> get_dirty_chunk(int64_t ino, int64_t idx);
    // Все записи по (ino, idx).
    [[nodiscard]] std::vector<DirtyChunk> list_dirty_chunks();
    [[nodiscard]] std::vector<DirtyChunk> list_dirty_chunks_for_ino(int64_t ino);
    // Удалить запись, только если version не изменилась с момента запечатывания.
    // Вернуть true, если удалена.
    bool delete_dirty_chunk_if_version(int64_t ino, int64_t idx, int64_t version);
    // Удалить все записи файла (unlink, truncate(0)).
    void delete_dirty_chunks_for_ino(int64_t ino);

    // --- dirty_attrs ---
    void set_dirty_attrs(const DirtyAttrs& attrs);
    [[nodiscard]] std::optional<DirtyAttrs> get_dirty_attrs(int64_t ino);
    void delete_dirty_attrs(int64_t ino);
    [[nodiscard]] std::vector<DirtyAttrs> list_dirty_attrs();

    // --- upload_journal ---
    void upload_started(int64_t chunk_id, int64_t started_ns);
    // Нет записи — logic_error.
    void upload_finished(int64_t chunk_id, int64_t msg_id);
    [[nodiscard]] std::vector<UploadRecord> list_uploads();
    void remove_upload(int64_t chunk_id);

    // --- open_pack (SPEC 6.5) ---
    // Вставить или заменить строку объекта.
    void upsert_open_pack(const OpenPackRow& row);
    [[nodiscard]] std::vector<OpenPackRow> list_open_pack();
    void delete_open_pack(int64_t ino);
    // Удалить строку, только если version совпала (транзакция применения пакета).
    bool delete_open_pack_if_version(int64_t ino, int64_t version);

    // Срок ожидания чужой блокировки state.sqlite (по умолчанию 5 с); для тестов.
    void set_busy_timeout(std::chrono::milliseconds timeout);

  private:
    void transaction_impl(const std::function<void()>& fn);
    // Выполнить запись под mutex_; SQLITE_BUSY (соседнее соединение, например ReadCache, держит
    // блокировку дольше busy_timeout) повторяется с паузой между попытками, мьютекс между
    // ними отпускается. Повторять можно: при BUSY оператор не выполнялся.
    template <typename F>
    auto write_retry(F&& fn);
    std::vector<DirtyChunk> query_dirty_chunks(std::string_view sql, std::optional<int64_t> ino);

    std::recursive_mutex mutex_;
    Db db_;
    int64_t version_seq_ = 0;  // последняя выданная version грязного чанка (под mutex_)
    bool legacy_schema_ = false;  // open_pack старой схемы (только --ro): список пуст
};

}  // namespace telega::meta
