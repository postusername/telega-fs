// E1, корпус C4 «Большое дерево» (SPEC 17.3, 17.4, нагрузка 6): индекс на 100k и 1M inode,
// снимок, цепочки из 64 дельт и время монтирования цепочки на пустой профиль.

#include <spdlog/spdlog.h>

#include <algorithm>
#include <fstream>
#include <iostream>
#include <set>
#include <stdexcept>
#include <string>

#include "common/bench_common.h"
#include "common/files.h"
#include "corpora/c4_gen.h"
#include "e1/e1.h"
#include "meta/chain.h"
#include "meta/index_ops.h"
#include "meta/sqlite.h"

namespace telega::bench::e1 {

namespace {

constexpr int kChainLength = 64;
const int kMountPoints[] = {0, 8, 16, 32, 64};

int64_t wall_now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

int64_t peak_rss_kib() {
    std::ifstream in("/proc/self/status");
    for (std::string line; std::getline(in, line);) {
        if (line.starts_with("VmHWM:")) {
            return std::stoll(line.substr(6));
        }
    }
    return -1;
}

Bytes to_bytes(const std::string& s) {
    Bytes b(s.size());
    std::transform(s.begin(), s.end(), b.begin(), [](char c) { return static_cast<std::byte>(c); });
    return b;
}

// Аналоги нагрузок 2–4 на C4 прямо через index_ops (те же операции, что делает Vfs, но без
// fsync на каждую запись: на миллионе файлов это часы).
struct DeltaOps {
    C4Index& c4;
    meta::MetaStore& meta;
    int64_t files_per_touch;
    int inline_rewrites = 100;

    // Нагрузка 2: переименовать каталог (одна строка inodes плюс время родителя).
    void rename_dir(uint64_t tag, Rng& rng) {
        C4Dir& d = c4.leaf_dirs[rng.below(c4.leaf_dirs.size())];
        const std::string name = d.name + "-r" + std::to_string(tag);
        const int64_t now = wall_now_ns();
        meta.write([&](meta::Db& db) {
            meta::move_inode(db, d.ino, d.parent, to_bytes(name));
            meta::AttrUpdate parent;
            parent.mtime_ns = now;
            parent.ctime_ns = now;
            meta::update_attrs(db, d.parent, parent);
            meta::AttrUpdate self;
            self.ctime_ns = now;
            meta::update_attrs(db, d.ino, self);
        });
        d.name = name;
    }

    // Нагрузка 3: touch части файлов. Время у каждого файла своё (шаг 1-5 мкс, как при обходе
    // дерева), иначе одинаковые значения сжались бы zstd лучше, чем настоящие.
    void touch(Rng& rng) {
        int64_t t = wall_now_ns();
        meta.write([&](meta::Db& db) {
            for (int64_t i = 0; i < files_per_touch; ++i) {
                t += 1000 + static_cast<int64_t>(rng.below(4000));
                meta::AttrUpdate u;
                u.atime_ns = t;
                u.mtime_ns = t;
                u.ctime_ns = t;
                meta::update_attrs(db, c4.file_inos[rng.below(c4.file_inos.size())], u);
            }
        });
    }

    // Нагрузка 4: перезапись инлайн-файлов (новое содержимое того же размера).
    void rewrite_inline(Rng& rng, int64_t base_gen) {
        const int64_t now = wall_now_ns();
        meta.write([&](meta::Db& db) {
            for (int i = 0; i < inline_rewrites; ++i) {
                const size_t at = rng.below(c4.inline_inos.size());
                Bytes data(static_cast<size_t>(c4.inline_sizes[at]));
                rng.fill(data.data(), data.size());
                if (!meta::set_inline(db, c4.inline_inos[at], data, base_gen)) {
                    throw std::runtime_error("inline rewrite did not fit the budget");
                }
                meta::AttrUpdate u;
                u.mtime_ns = now;
                u.ctime_ns = now;
                meta::update_attrs(db, c4.inline_inos[at], u);
            }
        });
    }
};

struct SizeRun {
    const Options& o;
    int64_t n;
    Csv& build;
    Csv& snaps;
    Csv& deltas;
    Csv& mounts;

    void mount_sample(ScratchFs& env, int chain_no, int k, int iter) {
        auto& st = env.mounted->state();
        const auto chain = st.list_chain();
        if (chain.size() != static_cast<size_t>(k) + 1) {
            throw std::logic_error("chain length does not match the point");
        }
        const fs::path mnt = env.root / "mnt";
        std::error_code ec;
        fs::remove_all(mnt, ec);

        // (A) настоящее монтирование на пустой профиль: сборка индекса из цепочки.
        int64_t total_ns = 0;
        {
            auto t = Clock::now();
            auto m = meta::mount_fs(*env.store, meta::FsPaths{mnt / "data"},
                                    fresh_mount_options(env.key()));
            total_ns = ns_since(t);
            if (m->report().situation != meta::MountSituation::built_fresh) {
                throw std::runtime_error("fresh mount did not rebuild the index from the chain");
            }
            m.reset();
        }
        fs::remove_all(mnt, ec);
        fs::create_directories(mnt);

        // (Б) то же по шагам: чтение и распаковка снимка, чтение и распаковка дельт, сборка.
        const auto& keys = env.mounted->keys();
        const auto fs_uuid = env.mounted->fs_uuid();
        int64_t snap_read = 0;
        int64_t snap_open = 0;
        int64_t snap_write = 0;
        int64_t snap_blob_bytes = 0;
        int64_t snap_raw_bytes = 0;
        int64_t delta_read = 0;
        int64_t delta_open = 0;
        int64_t delta_blob_bytes = 0;
        int64_t delta_patch_bytes = 0;
        const fs::path snap_file = mnt / "chain-snapshot.sqlite";
        std::vector<Bytes> patches;
        {
            auto t = Clock::now();
            Bytes blob = env.store->read_index(chain[0].msg_id);
            snap_read = ns_since(t);
            snap_blob_bytes = static_cast<int64_t>(blob.size());
            t = Clock::now();
            meta::ChainBlob cb = meta::open_chain_blob(keys.index, fs_uuid, chain[0].msg_id, blob);
            snap_open = ns_since(t);
            snap_raw_bytes = static_cast<int64_t>(cb.payload.size());
            t = Clock::now();
            write_file_atomic(snap_file, cb.payload);
            snap_write = ns_since(t);
        }
        for (size_t i = 1; i < chain.size(); ++i) {
            auto t = Clock::now();
            Bytes blob = env.store->read_index(chain[i].msg_id);
            delta_read += ns_since(t);
            delta_blob_bytes += static_cast<int64_t>(blob.size());
            t = Clock::now();
            meta::ChainBlob cb = meta::open_chain_blob(keys.index, fs_uuid, chain[i].msg_id, blob);
            delta_open += ns_since(t);
            delta_patch_bytes += static_cast<int64_t>(cb.payload.size());
            patches.push_back(std::move(cb.payload));
        }
        // Сборка, как в MetaStore::build_from_chain, но по шагам (без перевода в WAL, fsync и
        // rename: они не зависят от длины цепочки).
        const fs::path replica = mnt / "replica.sqlite";
        auto t = Clock::now();
        fs::copy_file(snap_file, replica, fs::copy_options::overwrite_existing);
        const int64_t copy_ns = ns_since(t);
        int64_t open_ns = 0;
        int64_t apply_ns = 0;
        int64_t verify_ns = 0;
        int64_t commit_ns = 0;
        {
            t = Clock::now();
            meta::OpenOptions opts;
            opts.foreign_keys = false;
            opts.wal = false;
            meta::Db db = meta::Db::open(replica, opts);
            open_ns = ns_since(t);
            meta::Transaction tx{db};
            t = Clock::now();
            for (const Bytes& p : patches) {
                meta::MetaStore::apply_patchset(db, p);
            }
            apply_ns = ns_since(t);
            t = Clock::now();
            meta::MetaStore::verify(db);
            verify_ns = ns_since(t);
            t = Clock::now();
            tx.commit();
            commit_ns = ns_since(t);
        }
        fs::remove_all(mnt, ec);

        mounts.row({cell(n), cell(chain_no), cell(k), cell(iter), cell(snap_blob_bytes),
                    cell(snap_raw_bytes), cell(delta_blob_bytes), cell(delta_patch_bytes),
                    cell(total_ns), cell(snap_read), cell(snap_open), cell(snap_write),
                    cell(delta_read), cell(delta_open), cell(copy_ns), cell(open_ns),
                    cell(apply_ns), cell(verify_ns), cell(commit_ns)});
        mounts.flush();
    }

    void run() {
        meta::CommitConfig cfg;
        cfg.max_chain = kChainLength;
        cfg.checkpoint_ratio = 1e9;  // длину цепочки задаёт прогон, а не доля (см. отчёт)
        const auto size_started = Clock::now();
        ScratchFs env(o.work / ("c4-" + std::to_string(n)), cfg);
        auto& cm = env.mounted->committer();
        auto& mstore = env.mounted->meta();

        // Сборка индекса C4.
        C4Params params;
        params.inodes = n;
        params.seed = o.seed;
        auto t = Clock::now();
        C4Index c4 = generate_c4(mstore, params, static_cast<int64_t>(cm.base_generation()),
                                 [&](int64_t done) {
                                     if (done % 200'000 < 20'000) {
                                         std::cout << "    built " << done << " inodes\n";
                                     }
                                 });
        const double build_s = static_cast<double>(ns_since(t)) / 1e9;
        std::cout << "  C4-" << n << " built in " << build_s << " s: " << c4.dirs << " dirs, "
                  << c4.files << " files (" << c4.pack_files << " pack, " << c4.inline_files
                  << " inline, " << c4.positional_files << " positional), " << c4.extents
                  << " extents, " << c4.pack_chunks << " pack chunks, " << c4.positional_chunks
                  << " positional chunks\n";
        build.row({cell(n), cell(params.seed), cell(build_s), cell(c4.dirs), cell(c4.files),
                   cell(c4.pack_files), cell(c4.inline_files), cell(c4.positional_files),
                   cell(c4.inline_overflow), cell(c4.pack_chunks), cell(c4.positional_chunks),
                   cell(c4.extents), cell(c4.inline_bytes), cell(c4.depth)});
        build.flush();

        const int64_t files_per_touch = std::max<int64_t>(1, c4.files / 100);  // 1 % файлов
        DeltaOps ops{c4, mstore, files_per_touch};

        for (int chain_no = 1; chain_no <= o.chains; ++chain_no) {
            // Снимок: первый — после сборки, остальные — повтор замера на текущем индексе.
            const meta::CommitResult snap = cm.commit(true);
            const meta::CommitMetrics& m = snap.metrics;
            std::cout << "  chain " << chain_no << ": snapshot " << m.raw_bytes << " -> "
                      << m.compressed_bytes << " B, vacuum "
                      << static_cast<double>(to_ns(m.vacuum)) / 1e9 << " s, compress "
                      << static_cast<double>(to_ns(m.compress)) / 1e9 << " s, encrypt "
                      << static_cast<double>(to_ns(m.encrypt)) / 1e9 << " s\n";
            snaps.row({cell(n), cell(chain_no), cell(static_cast<int64_t>(m.raw_bytes)),
                       cell(static_cast<int64_t>(m.compressed_bytes)),
                       cell(static_cast<int64_t>(m.blob_bytes)), cell(to_ns(m.vacuum)),
                       cell(to_ns(m.compress)), cell(to_ns(m.encrypt)), cell(to_ns(m.upload)),
                       cell(to_ns(m.total))});
            snaps.flush();

            for (int iter = 1; iter <= o.mount_iters; ++iter) {
                mount_sample(env, chain_no, 0, iter);
            }
            for (int k = 1; k <= kChainLength; ++k) {
                Rng rng(o.seed + 1000ULL * static_cast<uint64_t>(chain_no) +
                        static_cast<uint64_t>(k));
                const auto gen = static_cast<int64_t>(cm.base_generation());
                const bool heavy = o.profile == "heavy";
                std::string what;
                const std::string tag = std::to_string(chain_no * 100 + k);
                if (heavy || k % 3 == 1) {
                    ops.rename_dir(static_cast<uint64_t>(chain_no) * 100 + static_cast<uint64_t>(k),
                                   rng);
                    what += "rename+";
                }
                if (heavy || k % 3 == 2) {
                    ops.touch(rng);
                    what += "touch+";
                }
                if (heavy || k % 3 == 0) {
                    ops.rewrite_inline(rng, gen);
                    what += "inline+";
                }
                what.pop_back();
                const meta::CommitResult d = cm.commit(false);
                const meta::CommitMetrics& dm = d.metrics;
                if (!d.committed || dm.kind != crypto::BlobKind::delta) {
                    throw std::runtime_error("expected a delta commit at chain point " + tag);
                }
                deltas.row({cell(n), cell(chain_no), cell(k), what,
                            cell(static_cast<int64_t>(dm.patch_bytes)),
                            cell(static_cast<int64_t>(dm.raw_bytes)),
                            cell(static_cast<int64_t>(dm.compressed_bytes)),
                            cell(static_cast<int64_t>(dm.blob_bytes)), cell(to_ns(dm.take.wait)),
                            cell(to_ns(dm.take.locked)), cell(to_ns(dm.compress)),
                            cell(to_ns(dm.encrypt)), cell(to_ns(dm.upload)),
                            cell(to_ns(dm.total))});
                if (std::find(std::begin(kMountPoints), std::end(kMountPoints), k) !=
                    std::end(kMountPoints)) {
                    for (int iter = 1; iter <= o.mount_iters; ++iter) {
                        mount_sample(env, chain_no, k, iter);
                    }
                    std::cout << "    chain " << chain_no << " k=" << k << " mounted\n";
                }
            }
            deltas.flush();
        }
        std::cout << "  C4-" << n << " done in "
                  << static_cast<double>(ns_since(size_started)) / 1e9 << " s, peak RSS "
                  << peak_rss_kib() / 1024 << " MiB, work dir "
                  << static_cast<double>(dir_bytes(env.root)) / (1024.0 * 1024.0) << " MiB\n";
    }
};

}  // namespace

int run_c4(const Options& o) {
    if (o.profile != "typical" && o.profile != "heavy") {
        throw std::invalid_argument("profile must be typical or heavy");
    }
    const std::string suffix = o.profile == "heavy" ? "_heavy" : "";
    Csv build(o.out / ("c4_build" + suffix + ".csv"),
              {"inodes", "seed", "build_s", "dirs", "files", "pack_files", "inline_files",
               "positional_files", "inline_overflow", "pack_chunks", "positional_chunks", "extents",
               "inline_bytes", "depth"});
    Csv snaps(o.out / ("c4_snapshots" + suffix + ".csv"),
              {"inodes", "chain", "raw_bytes", "compressed_bytes", "blob_bytes", "vacuum_ns",
               "compress_ns", "encrypt_ns", "upload_ns", "total_ns"});
    Csv deltas(o.out / ("c4_deltas" + suffix + ".csv"),
               {"inodes", "chain", "k", "ops", "patch_bytes", "raw_bytes", "compressed_bytes",
                "blob_bytes", "take_wait_ns", "take_locked_ns", "compress_ns", "encrypt_ns",
                "upload_ns", "total_ns"});
    Csv mounts(o.out / ("c4_mount" + suffix + ".csv"),
               {"inodes", "chain", "k", "iter", "snapshot_blob_bytes", "snapshot_raw_bytes",
                "delta_blob_bytes", "delta_patch_bytes", "mount_total_ns", "snap_read_ns",
                "snap_open_ns", "snap_write_ns", "delta_read_ns", "delta_open_ns", "copy_ns",
                "dbopen_ns", "apply_ns", "verify_ns", "txcommit_ns"});
    for (const int64_t n : o.c4_sizes) {
        std::cout << "C4 " << n << " inodes, profile " << o.profile << '\n';
        SizeRun{o, n, build, snaps, deltas, mounts}.run();
        if (!o.keep) {
            std::error_code ec;
            fs::remove_all(o.work / ("c4-" + std::to_string(n)), ec);
        }
    }
    return 0;
}

}  // namespace telega::bench::e1
