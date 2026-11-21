// tgfs-e8: эксперимент E8 «Инлайн» (SPEC 17.11). Методика и результаты —
// docs/experiments/08-e8-inline.md.

#include <CLI/CLI.hpp>
#include <spdlog/spdlog.h>

#include <fstream>
#include <iostream>

#include "crypto/keys.h"
#include "e8/e8.h"

int main(int argc, char** argv) {
    using namespace telega::bench;
    e8::Options o;
    std::string work;
    std::string out;
    std::string c1_dir;
    std::string c2_sizes;
    std::string datasets;
    std::string inline_maxes;
    std::string budgets;
    std::string hist_dataset;
    std::string hist_out;
    bool small_only = false;
    CLI::App app{"tgfs-e8: inline files (SPEC 17.11)"};
    app.require_subcommand(1);
    app.add_option("--work", work, "scratch directory for temporary data")->required();
    app.add_option("--out", out, "directory for CSV files")->required();
    app.add_option("--c1-dir", c1_dir, "unpacked C1 (tools/bench/corpora/fetch-c1.sh)");
    app.add_option("--c2-sizes", c2_sizes, "c2-sizes.csv (tools/bench/corpora)");
    app.add_option("--seed", o.seed, "seed of all pseudo-random choices");
    app.add_option("--pack-threshold", o.pack_threshold, "bytes (SPEC 12.2, E9: 4 MiB)");
    app.add_option("--pack-target", o.pack_target, "bytes (SPEC 12.2, E9: 16 MiB)");

    auto* index = app.add_subcommand("index", "snapshots and deltas of the index (SPEC 17.11)");
    index->add_option("--datasets", datasets, "c1,c2,c4-100000,c4-1000000 (default: all)");
    index->add_option("--inline-max", inline_maxes, "list, e.g. 0,1KiB,2KiB,4KiB,8KiB,16KiB");
    index->add_option("--budgets", budgets, "inline_budget list, e.g. 4MiB,16MiB,64MiB");
    index->add_option("--content", o.content, "auto | text (C1 sources as the file content)");
    index->add_option("--tag", o.tag, "suffix of CSV names");
    index->add_option("--repeats", o.repeats, "repeats of each measurement (>= 3)");
    index->add_flag("--keep", o.keep, "keep temporary data");

    auto* hist =
        app.add_subcommand("hist", "file sizes of a data set as `size,count` for tgfs-sim");
    hist->add_option("--dataset", hist_dataset, "c1 | c2 | c4-100000 | c4-1000000")->required();
    hist->add_option("--file", hist_out, "output CSV")->required();
    hist->add_flag("--small-only", small_only, "only files up to pack_threshold");

    CLI11_PARSE(app, argc, argv);
    if (o.repeats < 3) {
        std::cerr << "repeats must be >= 3 (SPEC 17.2)\n";
        return 2;
    }
    o.work = work;
    o.out = out;
    o.c1_dir = c1_dir;
    o.c2_sizes = c2_sizes;
    if (!datasets.empty()) {
        o.datasets = split_list(datasets);
    }
    if (!inline_maxes.empty()) {
        o.inline_maxes.clear();
        for (const std::string& s : split_list(inline_maxes)) {
            o.inline_maxes.push_back(parse_bytes(s));
        }
    }
    if (!budgets.empty()) {
        o.budgets.clear();
        for (const std::string& s : split_list(budgets)) {
            o.budgets.push_back(parse_bytes(s));
        }
    }
    std::filesystem::create_directories(o.work);
    std::filesystem::create_directories(o.out);
    spdlog::set_level(spdlog::level::warn);
    std::cout << std::unitbuf;
    telega::crypto::init();
    try {
        if (hist->parsed()) {
            return e8::run_hist(o, hist_dataset, hist_out, small_only);
        }
        const std::string env = environment_report(o.work);
        {
            std::ofstream f(o.out / "environment.txt", std::ios::app);
            f << "--- index seed=" << o.seed << " tag=" << o.tag << '\n' << env;
        }
        std::cout << env;
        return e8::run_index(o);
    } catch (const std::exception& e) {
        std::cerr << "error: " << e.what() << '\n';
        return 1;
    }
}
