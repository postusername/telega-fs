// E1, корпус C1 «Исходники»: нагрузки 1–5 через Vfs в процессе, без FUSE.
// После каждой нагрузки — запечатывание и заливка чанков (flush_all) и коммит индекса.

#include <fcntl.h>
#include <spdlog/spdlog.h>
#include <unistd.h>

#include <algorithm>
#include <fstream>
#include <iostream>
#include <map>
#include <ranges>
#include <span>
#include <stdexcept>
#include <string>
#include <unordered_map>

#include "common/bench_common.h"
#include "e1/e1.h"
#include "meta/commit.h"
#include "vfs/vfs.h"

namespace telega::bench::e1 {

namespace {

constexpr int64_t kInlineMax = 4096;  // inline_max по умолчанию

ByteSpan bs(const std::string& s) { return std::as_bytes(std::span{s.data(), s.size()}); }

struct Entry {
    std::string rel;  // путь внутри C1
    std::string parent_rel;
    std::string name;
    bool dir = false;
    int64_t size = 0;
    int64_t ino = 0;  // заполняется распаковкой
};

struct Tree {
    std::vector<Entry> entries;  // по пути: родитель раньше потомков
    int64_t files = 0;
    int64_t dirs = 0;
    int64_t bytes = 0;
};

Tree scan_tree(const fs::path& root) {
    Tree t;
    for (const auto& de : fs::recursive_directory_iterator(root)) {
        Entry e;
        e.rel = fs::relative(de.path(), root).generic_string();
        e.name = de.path().filename().string();
        const auto parent = fs::relative(de.path().parent_path(), root).generic_string();
        e.parent_rel = parent == "." ? "" : parent;
        if (de.is_symlink()) {
            throw std::runtime_error("C1 contains a symlink: " + e.rel);
        }
        e.dir = de.is_directory();
        if (!e.dir) {
            e.size = static_cast<int64_t>(de.file_size());
            ++t.files;
            t.bytes += e.size;
        } else {
            ++t.dirs;
        }
        t.entries.push_back(std::move(e));
    }
    std::sort(t.entries.begin(), t.entries.end(),
              [](const Entry& a, const Entry& b) { return a.rel < b.rel; });
    return t;
}

Bytes read_whole(const fs::path& p, int64_t size) {
    Bytes data(static_cast<size_t>(size));
    std::ifstream in(p, std::ios::binary);
    in.read(reinterpret_cast<char*>(data.data()), static_cast<std::streamsize>(size));
    if (!in && size > 0) {
        throw std::runtime_error("cannot read " + p.string());
    }
    return data;
}

enum class Mode : uint8_t { delta, lockhold, snapshot, policy };

const char* mode_name(Mode m) {
    switch (m) {
        case Mode::delta:
            return "delta";
        case Mode::lockhold:
            return "lockhold";
        case Mode::snapshot:
            return "snapshot";
        case Mode::policy:
            return "policy";
    }
    return "?";
}

const std::vector<std::string> kCommitColumns{"mode",
                                              "ratio",
                                              "rep",
                                              "cycle",
                                              "workload",
                                              "seq",
                                              "kind",
                                              "generation",
                                              "patch_bytes",
                                              "raw_bytes",
                                              "compressed_bytes",
                                              "blob_bytes",
                                              "pending_merged",
                                              "take_wait_ns",
                                              "take_locked_ns",
                                              "take_patch_bytes",
                                              "lockhold_ns",
                                              "vacuum_ns",
                                              "compress_ns",
                                              "encrypt_ns",
                                              "upload_ns",
                                              "total_ns",
                                              "gc_chunks_deleted",
                                              "gc_blobs_deleted",
                                              "chain_len_after",
                                              "index_file_bytes",
                                              "workload_ops",
                                              "workload_ns"};

// Строка по именам колонок; не заданные — 0.
std::vector<std::string> make_row(const std::map<std::string, std::string>& values) {
    std::vector<std::string> row;
    row.reserve(kCommitColumns.size());
    for (const auto& c : kCommitColumns) {
        const auto it = values.find(c);
        row.push_back(it == values.end() ? "0" : it->second);
    }
    for (const auto& [k, v] : values) {
        if (std::find(kCommitColumns.begin(), kCommitColumns.end(), k) == kCommitColumns.end()) {
            throw std::logic_error("unknown csv column " + k);
        }
    }
    return row;
}

const char* const kWorkloadNames[] = {"init",    "1-unpack", "2-rename",
                                      "3-touch", "4-inline", "5-delete"};

struct RunSpec {
    Mode mode = Mode::delta;
    double ratio = 0;  // только для policy
    int rep = 0;
    int cycles = 1;
};

class Runner {
  public:
    Runner(const Options& o, const Tree& tree, Csv& commits, Csv& workloads)
        : o_(o), tree_(tree), commits_(commits), workloads_(workloads) {}

    void run(const RunSpec& spec) {
        const auto started = Clock::now();
        meta::CommitConfig cfg;
        cfg.max_chain = 64;
        cfg.checkpoint_ratio = spec.mode == Mode::policy ? spec.ratio : 1e9;
        if (spec.mode != Mode::policy) {
            cfg.max_chain = 100'000;  // «чистые» режимы: вид блоба задаёт сам прогон
        }
        ScratchFs env(o_.work / "c1-run", cfg);
        vfs::VfsOptions vo;
        vo.seal_delay = std::chrono::hours(1);  // запечатывает flush_all
        vo.upload_concurrency = 4;
        vo.sync_timeout = std::chrono::minutes(10);
        vfs::Vfs vfs(env.mounted->meta(), env.mounted->state(), *env.store,
                     env.mounted->committer(), env.mounted->keys(),
                     vfs::VfsPaths{env.paths.staging(), env.paths.tmp()}, vo);

        // Нулевая строка: снимок поколения 1, который создал init (пустая ФС).
        {
            const int64_t head = env.mounted->state().get_int_or("head_msg_id", 0);
            commits_.row(make_row({{"mode", mode_name(spec.mode)},
                                   {"ratio", cell(spec.ratio)},
                                   {"rep", cell(spec.rep)},
                                   {"workload", "init"},
                                   {"kind", "snapshot"},
                                   {"generation", "1"},
                                   {"blob_bytes", cell(env.message_size(head))},
                                   {"chain_len_after", "1"}}));
        }

        int seq = 0;
        for (int cycle = 1; cycle <= spec.cycles; ++cycle) {
            std::vector<Entry> tree = tree_.entries;
            for (int w = 1; w <= 5; ++w) {
                const auto t0 = Clock::now();
                const int64_t ops = workload(vfs, env, tree, w, cycle, spec.rep);
                vfs.flush_all();
                const int64_t wall = ns_since(t0);
                workloads_.row(
                    {mode_name(spec.mode), cell(spec.ratio), cell(spec.rep), cell(cycle),
                     kWorkloadNames[w], cell(ops), cell(static_cast<double>(wall) / 1e9),
                     cell(static_cast<double>(ops) / (static_cast<double>(wall) / 1e9))});
                commit(env, spec, cycle, w, ++seq, ops, wall);
            }
        }
        std::cout << "  run " << mode_name(spec.mode) << " ratio=" << spec.ratio
                  << " rep=" << spec.rep << " cycles=" << spec.cycles << ": "
                  << static_cast<double>(ns_since(started)) / 1e9 << " s\n";
        vfs.shutdown();
    }

  private:
    // --- нагрузки ---

    int64_t workload(vfs::Vfs& vfs, ScratchFs& env, std::vector<Entry>& tree, int w, int cycle,
                     int rep) {
        const uint32_t uid = ::getuid();
        const uint32_t gid = ::getgid();
        (void)env;
        switch (w) {
            case 1:
                return unpack(vfs, tree, uid, gid);
            case 2: {
                vfs.rename(meta::kRootIno, bs("c1"), meta::kRootIno, bs("c1-renamed"), 0);
                return 1;
            }
            case 3: {
                int64_t n = 0;
                vfs::SetAttr s;
                s.atime_now = true;
                s.mtime_now = true;
                for (const Entry& e : tree) {
                    if (!e.dir) {
                        vfs.setattr(e.ino, s);
                        ++n;
                    }
                }
                return n;
            }
            case 4:
                return rewrite_inline(vfs, tree, cycle, rep);
            case 5:
                return remove_all(vfs, tree);
            default:
                throw std::logic_error("workload");
        }
    }

    int64_t unpack(vfs::Vfs& vfs, std::vector<Entry>& tree, uint32_t uid, uint32_t gid) {
        std::unordered_map<std::string, int64_t> dir_ino;
        const auto top = vfs.mkdir(meta::kRootIno, bs("c1"), 0755, uid, gid);
        vfs.forget(top.ino, 1);
        dir_ino[""] = top.ino;
        int64_t n = 1;
        for (Entry& e : tree) {
            const int64_t parent = dir_ino.at(e.parent_rel);
            if (e.dir) {
                const auto a = vfs.mkdir(parent, bs(e.name), 0755, uid, gid);
                vfs.forget(a.ino, 1);
                e.ino = a.ino;
                dir_ino[e.rel] = a.ino;
            } else {
                const auto c =
                    vfs.create(parent, bs(e.name), 0644, O_WRONLY | O_CREAT | O_EXCL, uid, gid);
                e.ino = c.attr.ino;
                if (e.size > 0) {
                    const Bytes data = read_whole(o_.c1_dir / e.rel, e.size);
                    vfs.write(e.ino, 0, data);
                }
                vfs.release(c.fh);
                vfs.forget(e.ino, 1);  // ядро забыло запись: unlink удалит inode сразу
            }
            ++n;
        }
        top_ino_ = top.ino;
        return n;
    }

    // 100 случайных файлов размером 1..4096 байт (они инлайн) получают новое содержимое
    // того же размера. Выбор зависит от seed, повтора и цикла.
    int64_t rewrite_inline(vfs::Vfs& vfs, const std::vector<Entry>& tree, int cycle,
                           int rep) const {
        std::vector<const Entry*> small;
        for (const Entry& e : tree) {
            if (!e.dir && e.size > 0 && e.size <= kInlineMax) {
                small.push_back(&e);
            }
        }
        Rng rng(o_.seed ^ (static_cast<uint64_t>(cycle) << 32U) ^ static_cast<uint64_t>(rep));
        for (size_t i = 0; i + 1 < small.size() && i < 100; ++i) {
            std::swap(small[i], small[i + rng.below(small.size() - i)]);
        }
        int64_t n = 0;
        for (size_t i = 0; i < small.size() && i < 100; ++i) {
            const Entry& e = *small[i];
            Bytes data(static_cast<size_t>(e.size));
            rng.fill(data.data(), data.size());
            const uint64_t fh = vfs.open(e.ino, O_WRONLY | O_TRUNC);
            vfs.write(e.ino, 0, data);
            vfs.release(fh);
            ++n;
        }
        return n;
    }

    int64_t remove_all(vfs::Vfs& vfs, const std::vector<Entry>& tree) const {
        std::unordered_map<std::string, int64_t> dir_ino;
        dir_ino[""] = top_ino_;
        for (const Entry& e : tree) {
            if (e.dir) {
                dir_ino[e.rel] = e.ino;
            }
        }
        int64_t n = 0;
        for (const Entry& e : tree) {
            if (!e.dir) {
                vfs.unlink(dir_ino.at(e.parent_rel), bs(e.name));
                ++n;
            }
        }
        for (const Entry& e : std::ranges::reverse_view(tree)) {
            if (e.dir) {
                vfs.rmdir(dir_ino.at(e.parent_rel), bs(e.name));
                ++n;
            }
        }
        vfs.rmdir(meta::kRootIno, bs("c1-renamed"));
        return n + 1;
    }

    // --- коммит и замеры ---

    void commit(ScratchFs& env, const RunSpec& spec, int cycle, int w, int seq, int64_t ops,
                int64_t wall_ns) {
        auto& cm = env.mounted->committer();
        int64_t lockhold = 0;
        int64_t take_ns = 0;
        if (spec.mode == Mode::lockhold) {
            // То, что держит блокировку записи на шаге 2 для дельты (commit.cpp): забор патча,
            // запись его в pending_patchsets (state.sqlite, fsync) и чтение pending.
            meta::TakeStats ts;
            const auto t0 = Clock::now();
            env.mounted->meta().with_lock([&] {
                const Bytes patch = env.mounted->meta().take_patchset(&ts);
                if (!patch.empty()) {
                    env.mounted->state().append_pending(patch);
                }
                (void)env.mounted->state().list_pending();
            });
            lockhold = ns_since(t0);
            take_ns = to_ns(ts.locked);
        }
        const meta::CommitResult res = cm.commit(spec.mode == Mode::snapshot);
        if (!res.committed) {
            throw std::runtime_error(std::string("workload ") + kWorkloadNames[w] +
                                     " produced no index changes");
        }
        const meta::CommitMetrics& m = res.metrics;
        if (spec.mode == Mode::delta || spec.mode == Mode::lockhold) {
            if (m.kind != crypto::BlobKind::delta) {
                throw std::runtime_error("a delta run produced a snapshot");
            }
        }
        const auto chain_len = env.mounted->state().list_chain().size();
        const std::string index_bytes =
            cell(static_cast<int64_t>(fs::file_size(env.paths.index_db())));
        commits_.row(make_row(
            {{"mode", mode_name(spec.mode)},
             {"ratio", cell(spec.ratio)},
             {"rep", cell(spec.rep)},
             {"cycle", cell(cycle)},
             {"workload", kWorkloadNames[w]},
             {"seq", cell(seq)},
             {"kind", m.kind == crypto::BlobKind::snapshot ? "snapshot" : "delta"},
             {"generation", cell(m.generation)},
             {"patch_bytes", cell(static_cast<int64_t>(m.patch_bytes))},
             {"raw_bytes", cell(static_cast<int64_t>(m.raw_bytes))},
             {"compressed_bytes", cell(static_cast<int64_t>(m.compressed_bytes))},
             {"blob_bytes", cell(static_cast<int64_t>(m.blob_bytes))},
             {"pending_merged", cell(static_cast<int64_t>(m.pending_merged))},
             {"take_wait_ns", cell(to_ns(m.take.wait))},
             {"take_locked_ns", cell(spec.mode == Mode::lockhold ? take_ns : to_ns(m.take.locked))},
             {"take_patch_bytes", cell(static_cast<int64_t>(m.take.patch_bytes))},
             {"lockhold_ns", cell(lockhold)},
             {"vacuum_ns", cell(to_ns(m.vacuum))},
             {"compress_ns", cell(to_ns(m.compress))},
             {"encrypt_ns", cell(to_ns(m.encrypt))},
             {"upload_ns", cell(to_ns(m.upload))},
             {"total_ns", cell(to_ns(m.total))},
             {"gc_chunks_deleted", cell(static_cast<int64_t>(m.gc_chunks_deleted))},
             {"gc_blobs_deleted", cell(static_cast<int64_t>(m.gc_blobs_deleted))},
             {"chain_len_after", cell(static_cast<int64_t>(chain_len))},
             {"index_file_bytes", index_bytes},
             {"workload_ops", cell(ops)},
             {"workload_ns", cell(wall_ns)}}));
    }

    const Options& o_;
    const Tree& tree_;
    Csv& commits_;
    Csv& workloads_;
    int64_t top_ino_ = 0;
};

}  // namespace

int run_c1(const Options& o) {
    const Tree tree = scan_tree(o.c1_dir);
    int64_t le4k = 0;
    int64_t le64k = 0;
    int64_t big = 0;
    for (const Entry& e : tree.entries) {
        if (e.dir) {
            continue;
        }
        if (e.size <= 4096) {
            ++le4k;
        } else if (e.size <= 65536) {
            ++le64k;
        } else {
            ++big;
        }
    }
    {
        Csv c(o.out / "c1_corpus.csv",
              {"files", "dirs", "bytes", "files_le_4KiB", "files_4_to_64KiB", "files_gt_64KiB"});
        c.row({cell(tree.files), cell(tree.dirs), cell(tree.bytes), cell(le4k), cell(le64k),
               cell(big)});
    }
    std::cout << "C1: " << tree.files << " files, " << tree.dirs << " dirs, "
              << static_cast<double>(tree.bytes) / (1024.0 * 1024.0) << " MiB\n";

    Csv commits(o.out / "c1_commits.csv", kCommitColumns);
    Csv workloads(o.out / "c1_workloads.csv",
                  {"mode", "ratio", "rep", "cycle", "workload", "ops", "seconds", "ops_per_s"});
    Runner runner(o, tree, commits, workloads);
    for (int rep = 1; rep <= o.repeats; ++rep) {
        std::cout << "repeat " << rep << '\n';
        runner.run({Mode::delta, 0, rep, 1});
        runner.run({Mode::lockhold, 0, rep, 1});
        runner.run({Mode::snapshot, 0, rep, 1});
        for (const double ratio : {0.1, 0.25, 0.5}) {
            runner.run({Mode::policy, ratio, rep, o.cycles});
        }
        commits.flush();
        workloads.flush();
    }
    if (!o.keep) {
        std::error_code ec;
        fs::remove_all(o.work / "c1-run", ec);
    }
    return 0;
}

}  // namespace telega::bench::e1
