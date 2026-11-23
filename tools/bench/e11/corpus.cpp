// E11: построение корпусов. C1 распаковывается через Vfs, как нагрузка 1 в E1 (e1/c1.cpp),
// C4 вставляется прямо через index_ops (corpora/c4_gen.cpp).

#include <fcntl.h>
#include <sqlite3.h>
#include <unistd.h>

#include <algorithm>
#include <fstream>
#include <iostream>
#include <span>
#include <stdexcept>
#include <unordered_map>

#include "common/bench_common.h"
#include "corpora/c4_gen.h"
#include "e11/e11.h"
#include "meta/commit.h"
#include "vfs/vfs.h"

namespace telega::bench::e11 {

namespace {

ByteSpan bs(const std::string& s) { return std::as_bytes(std::span{s.data(), s.size()}); }

struct Entry {
    std::string rel;
    std::string parent_rel;
    std::string name;
    bool dir = false;
    int64_t size = 0;
};

std::vector<Entry> scan_tree(const fs::path& root) {
    std::vector<Entry> out;
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
        e.size = e.dir ? 0 : static_cast<int64_t>(de.file_size());
        out.push_back(std::move(e));
    }
    std::sort(out.begin(), out.end(), [](const Entry& a, const Entry& b) { return a.rel < b.rel; });
    return out;
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

void unpack_c1(const Options& o, ScratchFs& env) {
    if (o.c1_dir.empty()) {
        throw std::runtime_error("--c1-dir is required for the c1 corpus");
    }
    const std::vector<Entry> tree = scan_tree(o.c1_dir);
    vfs::VfsOptions vo;
    vo.seal_delay = std::chrono::hours(1);  // запечатывает flush_all
    vo.upload_concurrency = 4;
    vo.sync_timeout = std::chrono::minutes(10);
    vfs::Vfs vfs(env.mounted->meta(), env.mounted->state(), *env.store, env.mounted->committer(),
                 env.mounted->keys(), vfs::VfsPaths{env.paths.staging(), env.paths.tmp()}, vo);
    const uint32_t uid = ::getuid();
    const uint32_t gid = ::getgid();
    std::unordered_map<std::string, int64_t> dir_ino;
    const auto top = vfs.mkdir(meta::kRootIno, bs("c1"), 0755, uid, gid);
    vfs.forget(top.ino, 1);
    dir_ino[""] = top.ino;
    for (const Entry& e : tree) {
        const int64_t parent = dir_ino.at(e.parent_rel);
        if (e.dir) {
            const auto a = vfs.mkdir(parent, bs(e.name), 0755, uid, gid);
            vfs.forget(a.ino, 1);
            dir_ino[e.rel] = a.ino;
        } else {
            const auto c =
                vfs.create(parent, bs(e.name), 0644, O_WRONLY | O_CREAT | O_EXCL, uid, gid);
            if (e.size > 0) {
                const Bytes data = read_whole(o.c1_dir / e.rel, e.size);
                vfs.write(c.attr.ino, 0, data);
            }
            vfs.release(c.fh);
            vfs.forget(c.attr.ino, 1);
        }
    }
    vfs.flush_all();
    vfs.shutdown();
}

int64_t scalar(meta::MetaStore& m, const char* sql) {
    return m.read([&](meta::Db& db) {
        meta::Stmt st{db, sql};
        return st.step() ? st.column_int64(0) : int64_t{0};
    });
}

// TRUNCATE-checkpoint на отдельном соединении: WAL пуст, индекс целиком в основном файле.
void truncate_wal(const fs::path& index) {
    meta::Db db = meta::Db::open(index);
    meta::Stmt st{db, "PRAGMA wal_checkpoint(TRUNCATE)"};
    st.step();
    if (st.column_int64(0) != 0) {
        throw std::runtime_error("wal_checkpoint(TRUNCATE) was busy");
    }
}

}  // namespace

std::unique_ptr<ScratchFs> build_corpus(const Options& o, const std::string& name,
                                        CorpusInfo& info) {
    meta::CommitConfig cfg;
    cfg.max_chain = 100'000;
    cfg.checkpoint_ratio = 1e9;
    auto env = std::make_unique<ScratchFs>(o.work / ("fs-" + name), cfg);
    info = CorpusInfo{};
    info.name = name;
    const auto t = Clock::now();
    if (name == "c1") {
        unpack_c1(o, *env);
        info.rounds = o.rounds_small;
        info.post_ms = o.post_ms;
    } else if (name.starts_with("c4-")) {
        C4Params params;
        params.inodes = std::stoll(name.substr(3));
        params.seed = o.seed;
        auto& cm = env->mounted->committer();
        const C4Index c4 =
            generate_c4(env->mounted->meta(), params, static_cast<int64_t>(cm.base_generation()));
        (void)c4;
        info.rounds = params.inodes >= 1'000'000 ? 1 : o.rounds_small;
        info.post_ms = params.inodes >= 1'000'000 ? 2 * o.post_ms : o.post_ms;
    } else {
        throw std::runtime_error("unknown corpus " + name);
    }
    info.build_s = static_cast<double>(ns_since(t)) / 1e9;
    auto& m = env->mounted->meta();
    info.inodes = scalar(m, "SELECT COUNT(*) FROM inodes");
    info.files = scalar(m, "SELECT COUNT(*) FROM inodes WHERE kind = 1");
    info.inline_files = scalar(m, "SELECT COUNT(*) FROM inodes WHERE inline_data IS NOT NULL");
    truncate_wal(env->paths.index_db());
    info.index_bytes = static_cast<int64_t>(fs::file_size(env->paths.index_db()));
    return env;
}

}  // namespace telega::bench::e11
