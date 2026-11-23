#pragma once

// Учёт кэша чтения на диске (SPEC 8.2 «Кэш чтения на диске»): файлы TDLib в tdfiles/. Модуль ведёт
// только учёт и решения; сами удаления (getRemoteFile + deleteFile) делает TelegramStore.
//
// - note(path, remote_file_id): файл скачивался или читался. Размер берётся по st_blocks (файлы
//   разреженные, SPEC 2), время доступа обновляется. Файлы вне каталога кэша (отправленные этим
//   процессом лежат вне files_directory) в учёт не входят.
// - Состояние живёт в памяти и сбрасывается в таблицу read_cache state.sqlite (SPEC 6.4) по
//   flush(); периодичность (раз в минуту) и вызов при размонтировании — забота владельца.
//   Модуль открывает свою связь с state.sqlite (WAL, busy_timeout), основной StateStore его не
//   видит; CREATE TABLE IF NOT EXISTS — тем же текстом, что у StateStore.
// - choose_victims(): когда сумма больше limit, самые давно использованные файлы в порядке LRU,
//   пока сумма не опустится до 90 % лимита. Занятые (активное скачивание, pread) пропускаются.
// - reconcile() при монтировании: файлы каталога, которых нет в таблице, учитываются с временем
//   доступа из mtime, строки без файла удаляются.
//
// Потокобезопасен.

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace telega::meta {
class Db;
}

namespace telega::store {

struct ReadCacheOptions {
    std::filesystem::path state_db;  // state.sqlite профиля; пустой — таблица не ведётся
    std::filesystem::path dir;            // tdfiles/
    uint64_t limit = uint64_t{4} << 30U;  // cache_limit; 0 — без вытеснения
    // Часы для времени доступа (наносекунды с эпохи); пусто — system_clock. Для тестов.
    std::function<int64_t()> now_ns;
};

struct ReadCacheEntry {
    std::string path;
    std::string remote_file_id;
    uint64_t bytes = 0;  // st_blocks · 512 на момент последнего учёта
    int64_t last_access_ns = 0;
};

struct ReconcileReport {
    size_t adopted = 0;  // файлы без строки (после сбоя), учтены по mtime
    size_t dropped = 0;  // строки без файла, удалены
    size_t kept = 0;
};

class ReadCache {
  public:
    explicit ReadCache(ReadCacheOptions options);
    ReadCache(const ReadCache&) = delete;
    ReadCache& operator=(const ReadCache&) = delete;
    ~ReadCache();  // сбрасывает состояние в таблицу

    // Сверка каталога с таблицей: читает таблицу, обходит каталог, исправляет расхождения и
    // сбрасывает результат в таблицу. Зовётся один раз при монтировании.
    ReconcileReport reconcile();

    // Файл скачивался или читался сейчас. Несуществующий файл из учёта убирается.
    void note(const std::string& path, const std::string& remote_file_id);
    void forget(const std::string& path);

    // Файл, который может вытеснить TelegramStore, если сумма выше лимита. busy(entry) == true —
    // файл нельзя удалять сейчас. Результат пуст, пока сумма не больше лимита.
    [[nodiscard]] std::vector<ReadCacheEntry> choose_victims(
        const std::function<bool(const ReadCacheEntry&)>& busy) const;

    [[nodiscard]] bool over_limit() const;
    [[nodiscard]] uint64_t total_bytes() const;
    [[nodiscard]] uint64_t limit() const { return opts_.limit; }
    [[nodiscard]] std::vector<ReadCacheEntry> entries() const;  // для тестов и status

    // Записать изменения в таблицу read_cache. Ошибки пишутся в лог и не бросаются: учёт кэша не
    // должен ронять чтение.
    void flush();

  private:
    struct Row {
        std::string remote_file_id;
        uint64_t bytes = 0;
        int64_t last_access_ns = 0;
    };

    [[nodiscard]] int64_t now() const;
    [[nodiscard]] bool inside(const std::string& path) const;
    void open_db();  // под db_mu_

    const ReadCacheOptions opts_;
    std::string dir_a_;  // каталог как задан
    std::string dir_b_;  // его canonical
    mutable std::mutex mu_;
    std::unordered_map<std::string, Row> rows_;
    uint64_t total_ = 0;
    std::unordered_set<std::string> dirty_;
    std::unordered_set<std::string> removed_;

    std::mutex db_mu_;
    std::unique_ptr<meta::Db> db_;
};

}  // namespace telega::store
