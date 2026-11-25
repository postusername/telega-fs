// E8: индекс при разных inline_max и inline_budget. Для каждого набора и каждой
// пары порогов: настоящая ФС на LocalDirStore, индекс собирается через index_ops (как C4 в E1,
// c4_gen), затем `repeats` раз: снимок (VACUUM INTO, zstd, шифрование) с чтением и распаковкой
// блоба назад, и дельты после правки 1, 100 и 1000 инлайн-файлов.

#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <stdexcept>

#include "corpora/c4_gen.h"
#include "e8/e8.h"
#include "meta/chain.h"
#include "meta/index_ops.h"

namespace telega::bench::e8 {

namespace {

constexpr int kRewriteCounts[] = {1, 100, 1000};

int64_t wall_now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

int64_t peak_rss_mib() {
    std::ifstream in("/proc/self/status");
    for (std::string line; std::getline(in, line);) {
        if (line.starts_with("VmHWM:")) {
            return std::stoll(line.substr(6)) / 1024;
        }
    }
    return -1;
}

std::string label(int64_t bytes) {
    if (bytes > 0 && bytes % (int64_t{1} << 20U) == 0) {
        return std::to_string(bytes >> 20U) + "MiB";
    }
    if (bytes > 0 && bytes % 1024 == 0) {
        return std::to_string(bytes >> 10U) + "KiB";
    }
    return std::to_string(bytes);
}

struct Csvs {
    Csv build;
    Csv snaps;
    Csv deltas;
};

// Правка count инлайн-файлов: новое содержимое той же длины, mtime и ctime (как в E1, нагрузка 4).
// Файлы выбираются без повторов из тех, что оказались инлайн.
int64_t rewrite_inline(meta::MetaStore& meta, const C4Index& c4, const Dataset& ds, int count,
                       Rng& rng, int64_t base_gen, int64_t* bytes) {
    const auto avail = c4.inline_inos.size();
    const auto n = std::min<size_t>(avail, static_cast<size_t>(count));
    std::set<size_t> picked;
    while (picked.size() < n) {
        picked.insert(rng.below(avail));
    }
    const int64_t now = wall_now_ns();
    *bytes = 0;
    meta.write([&](meta::Db& db) {
        for (const size_t at : picked) {
            const int64_t size = c4.inline_sizes[at];
            const Bytes data = ds.fresh(rng, size);
            if (!meta::set_inline(db, c4.inline_inos[at], data, base_gen)) {
                throw std::runtime_error("inline rewrite did not fit the budget");
            }
            meta::AttrUpdate u;
            u.mtime_ns = now;
            u.ctime_ns = now;
            meta::update_attrs(db, c4.inline_inos[at], u);
            *bytes += size;
        }
    });
    return static_cast<int64_t>(n);
}

void run_config(const Options& o, const Dataset& ds, int64_t inline_max, int64_t budget,
                Csvs& csv) {
    const auto started = Clock::now();
    meta::CommitConfig cfg;
    cfg.max_chain = 16;
    cfg.checkpoint_ratio = 1e9;  // вид блоба задаёт прогон (commit(true) или commit(false))
    meta::InitOptions init;
    init.inline_max = inline_max;
    init.inline_budget = budget;
    init.pack_threshold = o.pack_threshold;
    init.pack_target = o.pack_target;
    const std::string root =
        "e8-" + ds.name + "-" + std::to_string(inline_max) + "-" + std::to_string(budget);
    ScratchFs env(o.work / root, cfg, init);
    auto& cm = env.mounted->committer();
    auto& mstore = env.mounted->meta();

    C4Params params;
    params.seed = o.seed;
    params.sizes = &ds.sizes;
    params.content = ds.initial;
    auto t = Clock::now();
    const C4Index c4 = generate_c4(mstore, params, static_cast<int64_t>(cm.base_generation()));
    const double build_s = static_cast<double>(ns_since(t)) / 1e9;
    const std::string im = label(inline_max);
    const std::string bd = inline_max == 0 ? "-" : label(budget);
    const std::string content = content_name(ds.content);
    std::cout << "  " << ds.name << " inline_max " << im << " budget " << bd << ": built in "
              << build_s << " s, files " << c4.files << " (inline " << c4.inline_files << ", "
              << c4.inline_bytes << " B, overflow " << c4.inline_overflow << "; pack "
              << c4.pack_files << "; positional " << c4.positional_files << ")\n";
    const std::vector<std::string> key = {ds.name, content, im, bd};
    const auto with_key = [&](std::vector<std::string> tail) {
        std::vector<std::string> row = key;
        row.insert(row.end(), tail.begin(), tail.end());
        return row;
    };
    csv.build.row(
        with_key({cell(o.seed), cell(build_s), cell(c4.dirs), cell(c4.files), cell(c4.empty_files),
                  cell(c4.inline_files), cell(c4.inline_bytes), cell(c4.inline_overflow),
                  cell(c4.pack_files), cell(c4.pack_chunks), cell(c4.positional_files),
                  cell(c4.positional_chunks), cell(c4.extents)}));
    csv.build.flush();

    for (int rep = 1; rep <= o.repeats; ++rep) {
        const meta::CommitResult snap = cm.commit(true);
        const meta::CommitMetrics& m = snap.metrics;
        // Что увидит монтирование: чтение блоба из канала, расшифровка, распаковка.
        t = Clock::now();
        const Bytes blob = env.store->read_index(m.msg_id);
        const int64_t read_ns = ns_since(t);
        t = Clock::now();
        const meta::ChainBlob cb = meta::open_chain_blob(env.mounted->keys().index,
                                                         env.mounted->fs_uuid(), m.msg_id, blob);
        const int64_t open_ns = ns_since(t);
        if (cb.payload.size() != m.raw_bytes) {
            throw std::runtime_error("snapshot read back with a different size");
        }
        csv.snaps.row(with_key({cell(rep), cell(static_cast<int64_t>(m.raw_bytes)),
                                cell(static_cast<int64_t>(m.compressed_bytes)),
                                cell(static_cast<int64_t>(m.blob_bytes)), cell(to_ns(m.vacuum)),
                                cell(to_ns(m.compress)), cell(to_ns(m.encrypt)),
                                cell(to_ns(m.total)), cell(read_ns), cell(open_ns)}));
        csv.snaps.flush();
        std::cout << "    snapshot " << rep << ": " << m.raw_bytes << " -> " << m.blob_bytes
                  << " B, vacuum " << static_cast<double>(to_ns(m.vacuum)) / 1e9 << " s, zstd "
                  << static_cast<double>(to_ns(m.compress)) / 1e9 << " s\n";
    }
    // Дельты после снимков: размеры снимков не должны зависеть от того, сколько файлов
    // перезаписано.
    for (int rep = 1; rep <= o.repeats; ++rep) {
        for (const int count : kRewriteCounts) {
            if (c4.inline_inos.empty()) {
                break;
            }
            Rng rng(o.seed + 7919ULL * static_cast<uint64_t>(rep) + static_cast<uint64_t>(count));
            int64_t bytes = 0;
            const int64_t rewritten = rewrite_inline(
                mstore, c4, ds, count, rng, static_cast<int64_t>(cm.base_generation()), &bytes);
            const meta::CommitResult d = cm.commit(false);
            const meta::CommitMetrics& dm = d.metrics;
            if (!d.committed || dm.kind != crypto::BlobKind::delta) {
                throw std::runtime_error("expected a delta commit");
            }
            csv.deltas.row(with_key({cell(rep), cell(rewritten), cell(bytes),
                                     cell(static_cast<int64_t>(dm.patch_bytes)),
                                     cell(static_cast<int64_t>(dm.raw_bytes)),
                                     cell(static_cast<int64_t>(dm.compressed_bytes)),
                                     cell(static_cast<int64_t>(dm.blob_bytes)),
                                     cell(to_ns(dm.take.locked)), cell(to_ns(dm.compress)),
                                     cell(to_ns(dm.encrypt)), cell(to_ns(dm.total))}));
        }
        csv.deltas.flush();
    }
    std::cout << "  done in " << static_cast<double>(ns_since(started)) / 1e9 << " s, peak RSS "
              << peak_rss_mib() << " MiB\n";
}

}  // namespace

int run_index(const Options& o) {
    const std::string suffix = o.tag.empty() ? "" : "_" + o.tag;
    const std::vector<std::string> key = {"dataset", "content", "inline_max", "inline_budget"};
    const auto header = [&](std::vector<std::string> tail) {
        std::vector<std::string> h = key;
        h.insert(h.end(), tail.begin(), tail.end());
        return h;
    };
    Csvs csv{Csv(o.out / ("e8_build" + suffix + ".csv"),
                 header({"seed", "build_s", "dirs", "files", "empty_files", "inline_files",
                         "inline_bytes", "inline_overflow", "pack_files", "pack_chunks",
                         "positional_files", "positional_chunks", "extents"})),
             Csv(o.out / ("e8_snapshots" + suffix + ".csv"),
                 header({"repeat", "raw_bytes", "compressed_bytes", "blob_bytes", "vacuum_ns",
                         "compress_ns", "encrypt_ns", "total_ns", "read_ns", "open_ns"})),
             Csv(o.out / ("e8_deltas" + suffix + ".csv"),
                 header({"repeat", "files_rewritten", "inline_bytes_rewritten", "patch_bytes",
                         "raw_bytes", "compressed_bytes", "blob_bytes", "take_locked_ns",
                         "compress_ns", "encrypt_ns", "total_ns"}))};
    for (const std::string& name : o.datasets) {
        std::cout << "dataset " << name << '\n';
        const Dataset ds = make_dataset(name, o);
        for (const int64_t inline_max : o.inline_maxes) {
            // Без инлайна бюджет ни на что не влияет: одна прогонка.
            const std::vector<int64_t> budgets =
                inline_max == 0 ? std::vector<int64_t>{o.budgets.front()} : o.budgets;
            for (const int64_t budget : budgets) {
                run_config(o, ds, inline_max, budget, csv);
                if (!o.keep) {
                    std::error_code ec;
                    fs::remove_all(o.work / ("e8-" + ds.name + "-" + std::to_string(inline_max) +
                                             "-" + std::to_string(budget)),
                                   ec);
                }
            }
        }
    }
    return 0;
}

int run_hist(const Options& o, const std::string& dataset, const fs::path& out, bool small_only) {
    const Dataset ds = make_dataset(dataset, o);
    std::map<int64_t, int64_t> counts;
    for (const int64_t size : ds.sizes) {
        if (!small_only || size <= o.pack_threshold) {
            ++counts[size];
        }
    }
    std::ofstream f(out, std::ios::trunc);
    f << "size,count\n";
    for (const auto& [size, count] : counts) {
        f << size << ',' << count << '\n';
    }
    return f ? 0 : 1;
}

}  // namespace telega::bench::e8
