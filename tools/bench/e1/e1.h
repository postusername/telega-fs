#pragma once

// E1: размеры патчей дельта-журнала. Три прогона: c1 (нагрузки 1–5 на корпусе C1),
// c4 (индекс C4, цепочки дельт и монтирование), stress (время забора патча от числа строк).

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

namespace telega::bench::e1 {

struct Options {
    std::filesystem::path work;  // временные данные (не в дереве репозитория)
    std::filesystem::path out;     // CSV
    std::filesystem::path c1_dir;  // распакованный C1 (corpora/fetch-c1.sh)
    int repeats = 3;               // повторы замеров (не меньше 3)
    int cycles = 3;  // циклы нагрузок 1–5 в прогонах checkpoint_ratio
    uint64_t seed = 20261117;
    std::vector<int64_t> c4_sizes{100'000, 1'000'000};
    int chains = 3;  // цепочек из 64 дельт на каждый размер C4
    int mount_iters = 3;  // монтирований на точку цепочки в каждой цепочке
    std::string profile = "typical";  // typical | heavy: размер дельт на C4
    bool keep = false;                // не удалять временные данные
};

int run_c1(const Options& o);
int run_c4(const Options& o);
int run_stress(const Options& o);

}  // namespace telega::bench::e1
