#pragma once

// Граница между путями данных (Vfs, src/vfs) и коммитом (CommitManager, SPEC 7.1).
// Vfs зависит только от этого интерфейса; в тестах Vfs его заменяет фейк.

#include <cstdint>
#include <stdexcept>

namespace telega::meta {

// Коммит не удался (сеть, ошибка Store) или ФС в read-only. Vfs превращает в EIO / EROFS.
class CommitError : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

class Committer {
  public:
    Committer() = default;
    Committer(const Committer&) = delete;
    Committer& operator=(const Committer&) = delete;
    virtual ~Committer() = default;

    // Синхронный коммит (fsync, fsyncdir, sync, размонтирование). Возвращается после точки
    // коммита (pin, SPEC 7.1 шаг 6) и записи state.sqlite (шаг 8), либо бросает CommitError.
    // Если индекс не менялся с прошлого коммита, сразу возвращается. Потокобезопасен:
    // параллельные вызовы сливаются в один коммит или идут друг за другом.
    // Вызывающий НЕ ДОЛЖЕН держать блокировку MetaStore (SPEC 9).
    virtual void commit_now() = 0;

    // Залит и внесён в index.sqlite очередной чанк (счётчик commit_every_chunks, SPEC 7.1).
    // Не блокирует: коммит, если нужен, идёт в фоне.
    virtual void chunk_uploaded() = 0;

    // Текущий base_generation (G). Нужен для dead_since_gen при освобождении чанков (SPEC 7.4).
    [[nodiscard]] virtual uint64_t base_generation() const = 0;

    // ФС переведена в read-only (CAS-конфликт, SPEC 7.1 шаг 4, или монтирование с --ro).
    // Vfs на любые изменения отвечает EROFS.
    [[nodiscard]] virtual bool read_only() const = 0;
};

}  // namespace telega::meta
