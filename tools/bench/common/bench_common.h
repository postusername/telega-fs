#pragma once

// Общая обвязка экспериментов: часы, статистика (медиана, минимум, максимум — SPEC 17.2),
// CSV, временная ФС на LocalDirStore (init_fs + mount_fs) и описание окружения.

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <initializer_list>
#include <memory>
#include <string>
#include <vector>

#include "meta/commit.h"
#include "meta/init.h"
#include "meta/mount.h"
#include "store/local_dir_store.h"

namespace telega::bench {

namespace fs = std::filesystem;
using Clock = std::chrono::steady_clock;

[[nodiscard]] inline int64_t ns_since(Clock::time_point t) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t).count();
}
[[nodiscard]] inline int64_t to_ns(std::chrono::nanoseconds d) { return d.count(); }

// Детерминированный генератор (splitmix64): один seed — одна и та же последовательность на
// любой машине и любой версии стандартной библиотеки (std::mt19937 с distribution — нет).
class Rng {
  public:
    explicit Rng(uint64_t seed) : state_(seed) {}
    uint64_t next();
    // Равномерно в [0, n), n > 0 (смещение от деления по модулю ничтожно при n << 2^64).
    uint64_t below(uint64_t n) { return next() % n; }
    // Равномерно по логарифму в [lo, hi].
    int64_t log_uniform(int64_t lo, int64_t hi);
    void fill(std::byte* out, size_t n);

  private:
    uint64_t state_;
};

struct Summary {
    size_t n = 0;
    double median = 0;
    double min = 0;
    double max = 0;
};
Summary summarize(std::vector<double> values);

// CSV с заголовком; значения без запятых и кавычек (проверяется).
class Csv {
  public:
    Csv(const fs::path& path, const std::vector<std::string>& header);
    void row(const std::vector<std::string>& cells);
    void flush() { out_.flush(); }

  private:
    std::ofstream out_;
    size_t columns_;
};

std::string cell(int64_t v);
std::string cell(uint64_t v);
std::string cell(int v);
std::string cell(double v);
std::string cell(const std::string& v);
std::string cell(const char* v);

// Размер всех файлов каталога (байты), для отчёта о занятом месте.
uint64_t dir_bytes(const fs::path& dir);

// ФС в каталоге root: канал root/channel (LocalDirStore), профиль root/data, keyfile.
// Параметры init — как в SPEC 12.2 (значения по умолчанию InitOptions).
struct ScratchFs {
    fs::path root;
    fs::path keyfile;
    std::unique_ptr<store::LocalDirStore> store;
    meta::FsPaths paths;
    std::unique_ptr<meta::MountedFs> mounted;

    // Очищает root, инициализирует ФС (первый коммит — снимок поколения 1) и монтирует её.
    // Фоновый поток коммитов не запускается: коммиты ведёт эксперимент.
    ScratchFs(fs::path root, const meta::CommitConfig& commit,
              const meta::InitOptions& init_options = {});
    ScratchFs(const ScratchFs&) = delete;
    ScratchFs& operator=(const ScratchFs&) = delete;
    ~ScratchFs();

    [[nodiscard]] meta::KeySource key() const { return meta::KeySource{std::nullopt, keyfile}; }
    // Размер блоба индекса в канале.
    [[nodiscard]] uint64_t message_size(int64_t msg_id) const;
};

// Монтирование цепочки канала на пустой профиль (SPEC 7.2, «локального индекса нет»).
meta::MountOptions fresh_mount_options(const meta::KeySource& key);

// Текст окружения для отчёта: дата и время, CPU, ядро, ФС каталога work, тип сборки.
std::string environment_report(const fs::path& work_dir);

}  // namespace telega::bench
