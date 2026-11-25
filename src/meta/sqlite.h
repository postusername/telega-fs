#pragma once

// Тонкие RAII-обёртки над своей сборкой SQLite: соединение, подготовленные
// выражения, транзакции и session extension для дельта-журнала.
//
// Сборка с SQLITE_THREADSAFE=2: одно соединение НЕ ДОЛЖНО использоваться из нескольких потоков
// одновременно. Обёртки этого не проверяют, разграничение — забота владельца соединения
// (MetaStore, StateStore).

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>

#include "common/bytes.h"

struct sqlite3;
struct sqlite3_stmt;
struct sqlite3_session;
struct sqlite3_changeset_iter;

namespace telega::meta {

// Ошибка SQLite: расширенный код результата и текст (sqlite3_errmsg плюс контекст).
class SqliteError : public std::runtime_error {
  public:
    SqliteError(int code, const std::string& what);
    // Расширенный код (например, SQLITE_CONSTRAINT_UNIQUE).
    [[nodiscard]] int code() const noexcept { return code_; }
    // Основной код (младший байт расширенного, например SQLITE_CONSTRAINT).
    [[nodiscard]] int primary_code() const noexcept { return code_ & 0xFF; }
    // SQLITE_BUSY или SQLITE_LOCKED: другое соединение держит блокировку дольше busy_timeout.
    // Транзиентная ошибка: оператор не выполнялся, его можно повторить.
    [[nodiscard]] bool busy() const noexcept;

  private:
    int code_;
};

struct OpenOptions {
    bool read_only = false;  // SQLITE_OPEN_READONLY; журнал и synchronous тогда не трогаются
    bool create = false;  // создать файл, если его нет
    bool foreign_keys = true;  // PRAGMA foreign_keys; выключается только для применения патчей
    bool wal = true;  // PRAGMA journal_mode = WAL (только для соединений на запись)
    std::chrono::milliseconds busy_timeout{5000};
};

class Stmt;

// Соединение SQLite. Все соединения на запись открываются с journal_mode = WAL и
// synchronous = FULL: overlay и индекс должны оставаться согласованными после сбоя питания.
// Расширенные коды результата включены.
class Db {
  public:
    // Открыть файл. Ошибка — SqliteError.
    static Db open(const std::filesystem::path& path, const OpenOptions& options = {});

    Db(Db&& other) noexcept;
    Db& operator=(Db&& other) noexcept;
    Db(const Db&) = delete;
    Db& operator=(const Db&) = delete;
    ~Db();

    // Выполнить один или несколько SQL-операторов без параметров, результат отбрасывается.
    void exec(std::string_view sql);
    void busy_timeout(std::chrono::milliseconds timeout);
    // PRAGMA foreign_keys. Вне транзакции, иначе SQLite молча игнорирует.
    void set_foreign_keys(bool on);

    [[nodiscard]] int64_t last_insert_rowid() const;
    [[nodiscard]] int64_t changes() const;
    // sqlite3_total_changes64: строки, изменённые с открытия соединения (включая откатанные).
    [[nodiscard]] int64_t total_changes() const;
    // true, если открытой транзакции нет.
    [[nodiscard]] bool autocommit() const;
    [[nodiscard]] const std::filesystem::path& path() const;
    [[nodiscard]] sqlite3* handle() const noexcept;

    // Выражение из кэша соединения: подготавливается один раз, после использования
    // сбрасывается (reset + clear_bindings) деструктором Stmt. Если то же выражение уже занято
    // (вложенное использование), возвращается новое, не из кэша.
    Stmt cached(std::string_view sql);

    // Бросить SqliteError по коду rc с текстом соединения и контекстом.
    [[noreturn]] void raise(int rc, std::string_view context) const;

  private:
    struct Impl;
    explicit Db(std::unique_ptr<Impl> impl);
    std::unique_ptr<Impl> impl_;
    friend class Stmt;
};

// Подготовленное выражение. Индексы параметров с 1, колонок с 0, как в SQLite.
class Stmt {
  public:
    // Подготовить выражение, которым владеет этот объект (finalize в деструкторе).
    Stmt(Db& db, std::string_view sql);
    Stmt(Stmt&& other) noexcept;
    Stmt& operator=(Stmt&&) = delete;
    Stmt(const Stmt&) = delete;
    Stmt& operator=(const Stmt&) = delete;
    ~Stmt();

    Stmt& bind(int idx, int64_t value);
    // Пустой буфер привязывается как пустой BLOB (не NULL).
    Stmt& bind(int idx, ByteSpan value);
    Stmt& bind_text(int idx, std::string_view value);
    Stmt& bind_null(int idx);
    Stmt& bind(int idx, const std::optional<int64_t>& value);

    // Шаг: true — есть строка, false — выражение выполнено. Ошибка — SqliteError.
    bool step();
    // Выполнить до конца, строки результата не ожидаются.
    void run();
    // Сбросить для повторного выполнения и отвязать параметры.
    void reset();

    [[nodiscard]] bool is_null(int col) const;
    [[nodiscard]] int64_t column_int64(int col) const;
    [[nodiscard]] Bytes column_blob(int col) const;
    [[nodiscard]] std::string column_text(int col) const;
    [[nodiscard]] std::optional<int64_t> column_opt_int64(int col) const;
    [[nodiscard]] std::optional<Bytes> column_opt_blob(int col) const;
    [[nodiscard]] int column_type(int col) const;  // SQLITE_INTEGER, SQLITE_BLOB, ...
    [[nodiscard]] int column_count() const;
    [[nodiscard]] sqlite3_stmt* handle() const noexcept { return stmt_; }

  private:
    Stmt(Db& db, sqlite3_stmt* stmt, bool* lease) noexcept;
    void check_bind(int rc, int idx);

    Db* db_;
    sqlite3_stmt* stmt_;
    bool* lease_;  // флаг занятости в кэше Db; nullptr — выражение принадлежит объекту
    friend class Db;
};

// Транзакция. На верхнем уровне BEGIN IMMEDIATE (или DEFERRED), внутри уже открытой
// транзакции — SAVEPOINT. Деструктор откатывает, если commit() не был вызван.
class Transaction {
  public:
    enum class Mode : uint8_t { immediate, deferred };
    explicit Transaction(Db& db, Mode mode = Mode::immediate);
    Transaction(const Transaction&) = delete;
    Transaction& operator=(const Transaction&) = delete;
    Transaction(Transaction&&) = delete;
    Transaction& operator=(Transaction&&) = delete;
    ~Transaction();

    void commit();
    void rollback() noexcept;

  private:
    Db& db_;
    bool savepoint_;
    bool done_ = false;
};

// Сессия session extension на схеме "main", подключённая ко всем таблицам
// (sqlite3session_attach(s, nullptr)), в том числе созданным позже.
// Должна быть уничтожена раньше своего соединения.
class Session {
  public:
    explicit Session(Db& db);
    Session(const Session&) = delete;
    Session& operator=(const Session&) = delete;
    Session(Session&&) = delete;
    Session& operator=(Session&&) = delete;
    ~Session();

    // Patchset всех изменений с создания сессии. Сессию не сбрасывает.
    [[nodiscard]] Bytes patchset() const;
    [[nodiscard]] bool empty() const;
    [[nodiscard]] int64_t memory_used() const;

  private:
    sqlite3_session* session_ = nullptr;
};

// Склеить патчи по порядку через sqlite3changegroup. Пустые патчи пропускаются.
// Порча входа — SqliteError (обычно SQLITE_CORRUPT).
Bytes concat_patchsets(std::span<const Bytes> patches);

// Что делать с конфликтом при применении патча (SQLITE_CHANGESET_OMIT/REPLACE/ABORT).
enum class ConflictAction : uint8_t { omit, replace, abort };
// Аргументы: вид конфликта (SQLITE_CHANGESET_DATA, _NOTFOUND, _CONFLICT, _CONSTRAINT,
// _FOREIGN_KEY) и итератор на конфликтующее изменение.
using ConflictHandler = std::function<ConflictAction(int conflict, sqlite3_changeset_iter* it)>;

// Применить patchset через sqlite3changeset_apply. Патч применяется целиком или никак
// (своя точка сохранения). ABORT из обработчика или ошибка разбора — SqliteError.
void apply_patchset(Db& db, ByteSpan patch, const ConflictHandler& on_conflict);

}  // namespace telega::meta
