// tgfs-e11: эксперимент E11 «Снимок без блокировки записи».
// Запуск и методика — docs/experiments/12-e11-snapshot.md.

#include <CLI/CLI.hpp>
#include <spdlog/spdlog.h>

#include <fstream>
#include <iostream>

#include "common/bench_common.h"
#include "crypto/keys.h"
#include "e11/e11.h"

int main(int argc, char** argv) {
    using namespace telega::bench;
    e11::Options o;
    std::string c1_dir;
    std::string work;
    std::string out;
    std::string corpora;
    std::string variants;
    CLI::App app{"tgfs-e11: snapshot of index.sqlite without blocking writes"};
    app.require_subcommand(1);
    app.add_option("--work", work, "scratch directory for temporary data")->required();
    app.add_option("--out", out, "directory for CSV files")->required();
    app.add_option("--repeats", o.repeats, "repeats of each measurement (>= 3)");
    app.add_option("--seed", o.seed, "seed of all pseudo-random choices");
    app.add_flag("--keep", o.keep, "keep temporary data");

    auto* snap =
        app.add_subcommand("snapshots", "variants (a), (b), (c) under a continuous writer");
    snap->add_option("--c1-dir", c1_dir, "unpacked C1 (tools/bench/corpora/fetch-c1.sh)");
    snap->add_option("--corpora", corpora, "comma-separated: c1, c4-100000, c4-1000000");
    snap->add_option("--variants", variants, "comma-separated: a, b, bs, bi, bn, c, m");
    snap->add_option("--take-rows", o.take_rows,
                     "patch_take_rows: threshold of the intermediate take");
    snap->add_option("--writer-rows", o.writer_rows, "rows changed per writer transaction");
    snap->add_flag("--fair", o.fair, "writer yields while a commit or a take waits for the lock");
    snap->add_option("--writer-pause-us", o.writer_pause_us, "pause between writer transactions");
    snap->add_option("--pre-ms", o.pre_ms, "idle window before the first snapshot");
    snap->add_option("--post-ms", o.post_ms, "window after each snapshot (doubled on 1M)");
    snap->add_option("--rounds", o.rounds_small, "snapshots per run on C1 and C4 100k");

    auto* probe =
        app.add_subcommand("probe", "checks of backup API and WAL behaviour on a C4 index");
    probe->add_option("--corpus", o.probe_corpus, "c4-100000 (default) or c4-1000000");

    CLI11_PARSE(app, argc, argv);
    if (o.repeats < 3) {
        std::cerr << "repeats must be >= 3\n";
        return 2;
    }
    o.work = work;
    o.out = out;
    o.c1_dir = c1_dir;
    if (!corpora.empty()) {
        o.corpora = split_list(corpora);
    }
    if (!variants.empty()) {
        o.variants = split_list(variants);
    }
    std::filesystem::create_directories(o.work);
    std::filesystem::create_directories(o.out);
    spdlog::set_level(spdlog::level::warn);
    std::cout << std::unitbuf;
    telega::crypto::init();

    const std::string env = environment_report(o.work);
    {
        std::ofstream f(o.out / "environment.txt", std::ios::app);
        f << "--- " << app.get_subcommands().front()->get_name() << " seed=" << o.seed
          << " fair=" << o.fair << " writer_pause_us=" << o.writer_pause_us
          << " writer_rows=" << o.writer_rows << " take_rows=" << o.take_rows << '\n'
          << env;
    }
    std::cout << env;
    try {
        if (snap->parsed()) {
            return e11::run_snapshots(o);
        }
        return e11::run_probe(o);
    } catch (const std::exception& e) {
        std::cerr << "error: " << e.what() << '\n';
        return 1;
    }
}
