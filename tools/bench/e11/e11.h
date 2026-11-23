#pragma once

// E11: снимок индекса без блокировки записи (SPEC 17.14). Два прогона: snapshots (варианты
// (a), (b), (c) на корпусах C1 и C4 с непрерывным писателем) и probe (проверка тонких мест
// backup API и WAL, от которых зависит корректность варианта (b)).

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "common/bench_common.h"

namespace telega::bench::e11 {

struct Options {
    std::filesystem::path work;  // временные данные (не в дереве репозитория)
    std::filesystem::path out;     // CSV
    std::filesystem::path c1_dir;  // распакованный C1 (corpora/fetch-c1.sh)
    int repeats = 3;               // повторы замеров (SPEC 17.2: не меньше 3)
    uint64_t seed = 20261122;
    std::vector<std::string> corpora{"c1", "c4-100000", "c4-1000000"};
    std::vector<std::string> variants{"a", "b", "bs", "c"};
    int64_t take_rows = 2000;  // patch_take_rows (SPEC 6.6): порог промежуточного забора
    int writer_rows = 10;  // строк на транзакцию писателя
    bool fair = false;  // писатель уступает коммиту и забору (имитация честной блокировки)
    int writer_pause_us = 0;  // пауза между транзакциями писателя (0 = непрерывно)
    int pre_ms = 1000;   // окно «без снимка» перед первым снимком
    int post_ms = 1000;  // окно после снимка (на 1M удваивается)
    int rounds_small = 10;  // снимков в одном прогоне на C1 и C4 100k (на 1M один)
    std::string probe_corpus = "c4-100000";  // на каком индексе идут проверки probe
    bool keep = false;                       // не удалять временные данные
};

// Что построено: счётчики корпуса.
struct CorpusInfo {
    std::string name;
    int64_t inodes = 0;
    int64_t files = 0;
    int64_t inline_files = 0;
    int64_t index_bytes = 0;  // index.sqlite после TRUNCATE-checkpoint
    double build_s = 0;
    int rounds = 1;
    int post_ms = 1000;
};

// Построить ФС с корпусом (C1 через Vfs, C4 через generate_c4). Фоновые потоки коммита и Vfs
// остановлены, индекс открыт у env.mounted->meta().
std::unique_ptr<ScratchFs> build_corpus(const Options& o, const std::string& name,
                                        CorpusInfo& info);

// Отрицательный контроль проверки корректности: тот же снимок, но без патчей после среза, обязан
// не совпасть с живым индексом (иначе построчное сравнение ничего не доказывает). Возвращает
// текст для probe.
std::string negative_control(const Options& o, ScratchFs& env);

int run_snapshots(const Options& o);
int run_probe(const Options& o);

}  // namespace telega::bench::e11
