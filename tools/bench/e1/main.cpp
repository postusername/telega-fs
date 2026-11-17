// tgfs-e1: эксперимент E1 «Размеры патчей дельта-журнала» (SPEC 17.4).
// Запуск и методика — docs/experiments/01-e1-patch-sizes.md.

#include <CLI/CLI.hpp>
#include <spdlog/spdlog.h>

#include <fstream>
#include <iostream>

#include "common/bench_common.h"
#include "crypto/keys.h"
#include "e1/e1.h"

int main(int argc, char** argv) {
    using namespace telega::bench;
    e1::Options o;
    std::string c1_dir;
    std::string work;
    std::string out;
    std::string sizes;
    CLI::App app{"tgfs-e1: patch sizes of the delta journal (SPEC 17.4)"};
    app.require_subcommand(1);
    app.add_option("--work", work, "scratch directory for temporary data")->required();
    app.add_option("--out", out, "directory for CSV files")->required();
    app.add_option("--repeats", o.repeats, "repeats of each measurement (>= 3)");
    app.add_option("--seed", o.seed, "seed of all pseudo-random choices");
    app.add_flag("--keep", o.keep, "keep temporary data");

    auto* c1 = app.add_subcommand("c1", "workloads 1-5 on the C1 corpus");
    c1->add_option("--c1-dir", c1_dir, "unpacked C1 (tools/bench/corpora/fetch-c1.sh)")->required();
    c1->add_option("--cycles", o.cycles, "cycles of workloads 1-5 in checkpoint_ratio runs");

    auto* c4 = app.add_subcommand("c4", "C4 index: snapshot, chains of 64 deltas, mount");
    c4->add_option("--sizes", sizes, "comma-separated inode counts (default 100000,1000000)");
    c4->add_option("--chains", o.chains, "chains of 64 deltas per size");
    c4->add_option("--mount-iters", o.mount_iters, "mounts per chain point");
    c4->add_option("--profile", o.profile, "typical | heavy");

    app.add_subcommand("stress", "patch take time versus number of changed rows");

    CLI11_PARSE(app, argc, argv);
    if (o.repeats < 3) {
        std::cerr << "repeats must be >= 3 (SPEC 17.2)\n";
        return 2;
    }
    o.work = work;
    o.out = out;
    o.c1_dir = c1_dir;
    if (!sizes.empty()) {
        o.c4_sizes.clear();
        size_t at = 0;
        while (at <= sizes.size()) {
            const size_t comma = sizes.find(',', at);
            o.c4_sizes.push_back(std::stoll(sizes.substr(at, comma - at)));
            if (comma == std::string::npos) {
                break;
            }
            at = comma + 1;
        }
    }
    std::filesystem::create_directories(o.work);
    std::filesystem::create_directories(o.out);
    spdlog::set_level(spdlog::level::warn);
    std::cout << std::unitbuf;
    telega::crypto::init();

    const std::string env = environment_report(o.work);
    {
        std::ofstream f(o.out / "environment.txt", std::ios::app);
        f << "--- " << app.get_subcommands().front()->get_name() << " seed=" << o.seed << '\n'
          << env;
    }
    std::cout << env;
    try {
        if (c1->parsed()) {
            return e1::run_c1(o);
        }
        if (c4->parsed()) {
            return e1::run_c4(o);
        }
        return e1::run_stress(o);
    } catch (const std::exception& e) {
        std::cerr << "error: " << e.what() << '\n';
        return 1;
    }
}
