#pragma once

// E8: инлайн-файлы (SPEC 17.11). Два режима: index (размеры снимков и дельт индекса при
// разных inline_max и inline_budget на наборах C1, C2, C4) и hist (список размеров C4 для
// tgfs-sim).

#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "common/bench_common.h"

namespace telega::bench::e8 {

// Источник содержимого: исходные файлы C1 или случайные данные трёх видов.
enum class Content : uint8_t {
    real,    // содержимое файлов C1 (только набор c1)
    random,  // случайные байты, не сжимаются (C2 по SPEC 17.3)
    base64,  // алфавит base64, как в C4 у E1 (сжимается примерно на четверть)
    text,  // куски исходников C1 (сжимаются, как текст): чувствительность
};

const char* content_name(Content c);

struct Dataset {
    std::string name;  // c1 | c2 | c4-100000 | c4-1000000
    Content content = Content::random;
    std::vector<int64_t> sizes;  // размеры файлов по порядку создания
    // Содержимое файла при создании и новое содержимое той же длины при перезаписи.
    std::function<Bytes(Rng&, int64_t idx, int64_t size)> initial;
    std::function<Bytes(Rng&, int64_t size)> fresh;
    std::shared_ptr<std::vector<Bytes>> corpus_files;  // C1: содержимое файлов (real)
    std::shared_ptr<Bytes> corpus;  // мелкие файлы C1 (до 16 КиБ) одним буфером
};

struct Options {
    std::filesystem::path work;
    std::filesystem::path out;
    std::filesystem::path c1_dir;
    std::filesystem::path c2_sizes;
    std::vector<std::string> datasets{"c1", "c2", "c4-100000", "c4-1000000"};
    std::vector<int64_t> inline_maxes{0, 1024, 2048, 4096, 8192, 16384};
    std::vector<int64_t> budgets{int64_t{4} << 20U, int64_t{16} << 20U, int64_t{64} << 20U};
    std::string content = "auto";  // auto | text
    std::string tag;  // суффикс имён CSV (для чувствительности)
    int repeats = 3;
    uint64_t seed = 20261121;
    int64_t pack_threshold = int64_t{4} << 20U;
    int64_t pack_target = int64_t{16} << 20U;
    bool keep = false;
};

// Набор по имени. C4: размеры по модели E8 (см. c4_e8_sizes).
Dataset make_dataset(const std::string& name, const Options& o);

// Модель размеров C4 для E8: 5 % файлов позиционные (логарифмически равномерно от
// pack_threshold + 1 до 128 МиБ, SPEC 17.3), остальные — логарифмически равномерно от 16 Б
// до pack_threshold. Класс хранения выводится из порогов, а не фиксируется долями.
std::vector<int64_t> c4_e8_sizes(int64_t files, int64_t pack_threshold, uint64_t seed);

int run_index(const Options& o);
int run_hist(const Options& o, const std::string& dataset, const std::filesystem::path& out,
             bool small_only);

}  // namespace telega::bench::e8
