#pragma once

// Снимок индекса без долгой блокировки записи (шаг 2 коммита; выбор способа — E11,
// docs/experiments/12-e11-snapshot.md).
//
// Три шага:
// 1. Под блокировкой MetaStore: MetaStore::cut_snapshot(reader) пересоздаёт сессию без забора
//    патча и на читательском соединении снимка открывает транзакцию, прочитав одну страницу
//    (в WAL это фиксирует срез ровно в точке пересоздания сессии).
// 2. Блокировка отпущена, записи продолжаются.
// 3. SnapshotCopy::copy(): backup API копирует срез в tmp/index-<G+1>.sqlite.raw, читательская
//    транзакция закрывается сразу после backup, затем на копии PRAGMA journal_mode = DELETE и
//    VACUUM INTO итогового файла; .raw удаляется. Ни один шаг после первого блокировку
//    MetaStore не берёт.
//
// Читательская транзакция закрывается на любом пути выхода (деструктор SnapshotReader): пока она
// открыта, контрольная точка WAL не идёт дальше среза, и WAL растёт на объём записей.
//
// Соединения: читательское (только чтение, живёт от среза до COMMIT) и копии (только поток,
// который делает снимок). Оба открываются заранее, до блокировки. Читательские соединения
// MetaStore::read для снимка не годятся: у них свои транзакции на каждую операцию.

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>

#include "meta/sqlite.h"

namespace telega::meta {

// Читательское соединение снимка. Срез живёт от begin() до end() (или до уничтожения).
class SnapshotReader {
  public:
    // Открыть index.sqlite только для чтения и прогреть схему (BEGIN, чтение страницы, COMMIT),
    // чтобы под блокировкой остались лишь BEGIN и чтение одной страницы.
    explicit SnapshotReader(const std::filesystem::path& index);
    SnapshotReader(const SnapshotReader&) = delete;
    SnapshotReader& operator=(const SnapshotReader&) = delete;
    SnapshotReader(SnapshotReader&&) = delete;
    SnapshotReader& operator=(SnapshotReader&&) = delete;
    ~SnapshotReader();

    // BEGIN и чтение одной страницы: фиксация среза. Зовёт MetaStore::cut_snapshot под
    // блокировкой. Повторный вызов при открытой транзакции — std::logic_error. При ошибке
    // транзакция закрыта.
    void begin();
    // COMMIT; без открытой транзакции ничего не делает, ошибок не бросает.
    void end() noexcept;
    [[nodiscard]] bool in_transaction() const noexcept { return open_; }
    [[nodiscard]] Db& db() noexcept { return db_; }

  private:
    Db db_;
    bool open_ = false;
};

// Этапы копирования, перед которыми вызывается хук (тесты: синхронизация и инъекция сбоев).
enum class SnapshotStage : uint8_t {
    backup,  // срез зафиксирован, блокировка MetaStore отпущена, backup ещё не начат
    vacuum,  // backup закончен, читательская транзакция закрыта, VACUUM INTO ещё не начат
};
// copy — соединение копии (на этапе backup — пустая база назначения). Исключение из хука —
// сбой этапа.
using SnapshotHook = std::function<void(SnapshotStage stage, Db& copy)>;

struct SnapshotCopyStats {
    std::chrono::nanoseconds backup{0};
    std::chrono::nanoseconds vacuum{0};  // PRAGMA journal_mode = DELETE и VACUUM INTO
};

// Читательское соединение снимка и соединение копии. Деструктор закрывает транзакцию и
// соединения и удаляет .raw (с -journal/-wal/-shm); итоговый файл при сбое copy() удаляется
// самим copy() (если его не было до вызова).
class SnapshotCopy {
  public:
    // raw — промежуточная копия (tmp/index-<G+1>.sqlite.raw), out — итоговый файл (не должен
    // существовать). Соединение копии без WAL и с synchronous = OFF: копия воспроизводима, при
    // сбое tmp/ выбрасывается.
    SnapshotCopy(const std::filesystem::path& index, std::filesystem::path raw,
                 std::filesystem::path out, SnapshotHook hook = {});
    SnapshotCopy(const SnapshotCopy&) = delete;
    SnapshotCopy& operator=(const SnapshotCopy&) = delete;
    SnapshotCopy(SnapshotCopy&&) = delete;
    SnapshotCopy& operator=(SnapshotCopy&&) = delete;
    ~SnapshotCopy();

    // Читательское соединение для MetaStore::cut_snapshot.
    [[nodiscard]] SnapshotReader& reader() noexcept { return reader_; }

    // Шаг копии: backup (после cut_snapshot), COMMIT читательской транзакции, затем
    // journal_mode = DELETE, VACUUM INTO out, закрытие копии, удаление .raw. Любой сбой —
    // исключение (SqliteError или из хука), читательская транзакция закрыта, .raw и out убраны.
    SnapshotCopyStats copy();

  private:
    void remove_raw() noexcept;

    SnapshotReader reader_;
    std::filesystem::path raw_;
    std::filesystem::path out_;
    SnapshotHook hook_;
    std::unique_ptr<Db> dst_;
};

}  // namespace telega::meta
