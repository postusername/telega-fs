#pragma once

// Генератор набора C4 «Большое дерево» (SPEC 17.3): синтетика только для индекса, без данных.
// Описание и параметры — corpora/README.md.

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "common/bench_common.h"
#include "meta/index_db.h"

namespace telega::bench {

struct C4Params {
    int64_t inodes = 100'000;  // всего inode, включая каталоги и корень
    int64_t fanout = 50;       // записей в каталоге
    uint64_t seed = 20261117;
    // Доли классов файлов в промилле (SPEC 17.3): 90 % в пакетах, 5 % инлайн, остальные
    // 5 % позиционные.
    int pack_permille = 900;
    int inline_permille = 50;
    int64_t inline_min = 16;  // размер инлайн-файла, равномерно [min, max]
    int64_t inline_max = 600;  // 50 000 файлов × ~308 байт укладываются в inline_budget 16 МиБ
    int64_t positional_max = int64_t{128} << 20U;  // размер позиционного: от pack_threshold
};

struct C4Dir {
    int64_t ino = 0;
    int64_t parent = 0;
    std::string name;
};

// Что сгенерировано: счётчики и списки, нужные для имитации нагрузок 2–4 поверх C4.
struct C4Index {
    C4Params params;
    int64_t dirs = 0;  // включая корень
    int64_t files = 0;
    int64_t pack_files = 0;
    int64_t inline_files = 0;
    int64_t positional_files = 0;
    int64_t inline_overflow = 0;  // инлайн-файлы, не влезшие в бюджет и ушедшие в пакеты
    int64_t pack_chunks = 0;
    int64_t positional_chunks = 0;
    int64_t extents = 0;
    int64_t inline_bytes = 0;
    int64_t depth = 0;
    std::vector<C4Dir> leaf_dirs;       // каталоги с файлами (цели rename)
    std::vector<int64_t> file_inos;     // все файлы по порядку создания
    std::vector<int64_t> inline_inos;   // инлайн-файлы
    std::vector<int64_t> inline_sizes;  // их размеры (в том же порядке)
};

// Раскладка по уровням: сколько каталогов на каждом уровне (от корня), сколько файлов.
struct C4Layout {
    std::vector<int64_t> dirs_per_level;
    int64_t files = 0;
};
// Подобрать раскладку так, чтобы всего было ровно inodes (каталоги + файлы), в каждом каталоге
// не больше fanout записей: листья — каталоги с файлами, выше — каталоги с подкаталогами.
C4Layout plan_c4_layout(int64_t inodes, int64_t fanout);

// Вставить C4 в индекс прямо через index_ops (без Vfs и без данных). Индекс должен быть
// пустым (только корень). base_gen — поколение для учёта чанков (index_ops). progress, если
// задан, вызывается с числом вставленных inode после каждой транзакции.
C4Index generate_c4(meta::MetaStore& meta, const C4Params& params, int64_t base_gen,
                    const std::function<void(int64_t)>& progress = {});

}  // namespace telega::bench
