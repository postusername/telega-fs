// tgfs-sim: офлайн-симуляция пакетов мелких файлов (эксперимент E9).
// Методика и результаты — docs/experiments/09-e9-packs.md.

#include <CLI/CLI.hpp>

#include <cstdlib>
#include <iostream>
#include <map>

#include "common/bench_common.h"
#include "sim/sim.h"

namespace {

using telega::bench::Csv;
using telega::bench::sim::Metrics;
using telega::bench::sim::Op;
using telega::bench::sim::Params;

struct Config {
    std::string name;  // подпись для CSV
    int64_t threshold;
    int64_t target;
    double age;
    double gap;
    double files_per_s;
    double fsync_prob;
    int64_t inline_max;
    int64_t inline_budget;
    bool limits;
};

std::string label(int64_t bytes) {
    using namespace telega::bench::sim;
    if (bytes % kGiB == 0 && bytes > 0) {
        return std::to_string(bytes / kGiB) + "GiB";
    }
    if (bytes % kMiB == 0 && bytes > 0) {
        return std::to_string(bytes / kMiB) + "MiB";
    }
    if (bytes % kKiB == 0 && bytes > 0) {
        return std::to_string(bytes / kKiB) + "KiB";
    }
    return std::to_string(bytes);
}

}  // namespace

int main(int argc, char** argv) {
    using namespace telega::bench;
    std::string events;
    std::string hist;
    std::string dataset;
    std::string out_runs;
    std::string out_summary;
    std::string thresholds = "1MiB";
    std::string targets = "16MiB";
    std::string ages = "30";
    std::string inline_maxes = "4KiB";
    std::string inline_budget = "16MiB";
    std::string limits = "on";
    std::string scenario = "unpack";
    std::string files_per_s = "200";
    std::string step_gaps = "60";
    std::string fsync_probs = "0";
    bool baselines = false;
    double send_bytes_mib = 224;
    int seeds = 3;
    uint64_t seed_base = 20261121;
    Params base;

    CLI::App app{"tgfs-sim: offline model of small-file packs"};
    app.add_option("--events", events, "CSV step,op,id,size (tools/bench/corpora/c1*.csv)");
    app.add_option("--hist", hist, "CSV size,count (tools/bench/corpora/c2-sizes.csv)");
    app.add_option("--dataset", dataset, "label for the dataset column")->required();
    app.add_option("--scenario", scenario, "label for the scenario column");
    app.add_option("--out", out_runs, "CSV of every run")->required();
    app.add_option("--summary", out_summary, "CSV median/min/max over seeds");
    app.add_option("--thresholds", thresholds, "pack_threshold list, e.g. 64KiB,256KiB,1MiB");
    app.add_option("--targets", targets, "pack_target list");
    app.add_option("--ages", ages, "pack_max_age list, seconds");
    app.add_option("--inline-max", inline_maxes, "inline_max list");
    app.add_option("--inline-budget", inline_budget, "inline_budget");
    app.add_option("--limits", limits, "on | off | both (token buckets)");
    app.add_flag("--baselines", baselines,
                 "add baselines: no packs (inline as configured) and no packs, no inline");
    app.add_option("--seeds", seeds, "repeats with different seeds (>= 3)");
    app.add_option("--seed-base", seed_base, "seed of run k is seed-base + k");
    app.add_option("--files-per-s", files_per_s, "writer: files created per second, list");
    app.add_option("--write-mib-s", base.write_mib_per_s, "writer: data MiB per second");
    app.add_option("--step-gap", step_gaps, "pause between steps (checkouts), seconds, list");
    app.add_option("--fsync-prob", fsync_probs, "share of files followed by fsync, list");
    app.add_flag("--seal-lag", base.seal_lag, "files stay open: sealed seal_delay after write");
    app.add_option("--commit-interval", base.commit_interval_s, "seconds");
    app.add_option("--commit-defer", base.commit_defer_factor,
                   "defer the timer commit while uploads are queued, up to this many commit "
                   "intervals (default 5); 0 - no deferral");
    app.add_option("--send-rate", base.send_rate_per_min, "actions per minute");
    app.add_option("--send-bytes-mib", send_bytes_mib, "bytes per minute, MiB");
    CLI11_PARSE(app, argc, argv);
    if (events.empty() == hist.empty()) {
        std::cerr << "exactly one of --events and --hist is required\n";
        return 2;
    }
    if (seeds < 3) {
        std::cerr << "seeds must be >= 3\n";
        return 2;
    }
    base.send_bytes_per_min = send_bytes_mib * sim::kMiB;
    base.inline_budget = parse_bytes(inline_budget);

    std::vector<bool> limit_modes;
    if (limits == "on" || limits == "both") {
        limit_modes.push_back(true);
    }
    if (limits == "off" || limits == "both") {
        limit_modes.push_back(false);
    }
    std::vector<Config> configs;
    const auto add = [&](const std::string& name, int64_t thr, int64_t target, double age,
                         int64_t inline_max) {
        for (const bool lim : limit_modes) {
            for (const std::string& gap : split_list(step_gaps)) {
                for (const std::string& fps : split_list(files_per_s)) {
                    for (const std::string& fsp : split_list(fsync_probs)) {
                        configs.push_back(Config{name, thr, target, age, std::stod(gap),
                                                 std::stod(fps), std::stod(fsp), inline_max,
                                                 base.inline_budget, lim});
                    }
                }
            }
        }
    };
    for (const std::string& im : split_list(inline_maxes)) {
        for (const std::string& t : split_list(thresholds)) {
            for (const std::string& g : split_list(targets)) {
                for (const std::string& a : split_list(ages)) {
                    add("packs", parse_bytes(t), parse_bytes(g), std::stod(a), parse_bytes(im));
                }
            }
        }
    }
    if (baselines) {
        const int64_t target = parse_bytes(split_list(targets).front());
        const double age = std::stod(split_list(ages).front());
        add("nopack", 0, target, age, parse_bytes(split_list(inline_maxes).front()));
        add("nopack-noinline", 0, target, age, 0);
    }

    std::vector<std::string> header = {"dataset", "scenario",   "config",      "threshold",
                                       "target",  "max_age_s",  "inline_max",  "inline_budget",
                                       "limits",  "step_gap_s", "files_per_s", "fsync_prob",
                                       "seed"};
    const Metrics probe;
    for (const auto& [name, v] : probe.columns()) {
        header.push_back(name);
    }
    Csv runs(out_runs, header);
    struct Group {
        std::vector<std::string> key;
        std::vector<std::vector<double>> values;
    };
    std::map<std::string, Group> groups;
    std::vector<std::string> group_order;

    std::vector<Op> loaded;
    if (!events.empty()) {
        loaded = sim::load_events(events);
    }
    for (int k = 0; k < seeds; ++k) {
        const uint64_t seed = seed_base + static_cast<uint64_t>(k);
        const std::vector<Op> ops = events.empty() ? sim::load_hist(hist, seed) : loaded;
        for (const Config& c : configs) {
            Params p = base;
            p.seed = seed;
            p.pack_threshold = c.threshold;
            p.pack_target = c.target;
            p.pack_max_age_s = c.age;
            p.inline_max = c.inline_max;
            p.inline_budget = c.inline_budget;
            p.limits = c.limits;
            p.step_gap_s = c.gap;
            p.files_per_s = c.files_per_s;
            p.fsync_prob = c.fsync_prob;
            const Metrics m = sim::simulate(ops, p);
            const std::vector<std::string> key = {
                dataset,
                scenario,
                c.name,
                label(c.threshold),
                label(c.target),
                cell(c.age),
                label(c.inline_max),
                label(c.inline_budget),
                c.limits ? "on" : "off",
                cell(c.gap),
                cell(c.files_per_s),
                cell(c.fsync_prob),
            };
            std::vector<std::string> row = key;
            row.push_back(std::to_string(seed));
            const auto cols = m.columns();
            for (const auto& [name, v] : cols) {
                row.push_back(cell(v));
            }
            runs.row(row);
            std::string gk;
            for (const std::string& s : key) {
                gk += s + "|";
            }
            auto [it, fresh] = groups.try_emplace(gk);
            if (fresh) {
                it->second.key = key;
                it->second.values.resize(cols.size());
                group_order.push_back(gk);
            }
            for (size_t i = 0; i < cols.size(); ++i) {
                it->second.values[i].push_back(cols[i].second);
            }
        }
        std::cout << "seed " << seed << " done (" << configs.size() << " configs)\n";
    }
    runs.flush();

    if (!out_summary.empty()) {
        std::vector<std::string> sh(header.begin(), header.begin() + 12);
        sh.insert(sh.end(), {"metric", "n", "median", "min", "max"});
        Csv sum(out_summary, sh);
        const auto cols = probe.columns();
        for (const std::string& gk : group_order) {
            const Group& g = groups[gk];
            for (size_t i = 0; i < cols.size(); ++i) {
                const Summary s = summarize(g.values[i]);
                std::vector<std::string> row = g.key;
                row.push_back(cols[i].first);
                row.push_back(cell(static_cast<int64_t>(s.n)));
                row.push_back(cell(s.median));
                row.push_back(cell(s.min));
                row.push_back(cell(s.max));
                sum.row(row);
            }
        }
        sum.flush();
    }
    return 0;
}
