// E1, дополнительный прогон: время забора патча под блокировкой записи в зависимости от числа
// изменённых строк (SPEC 17.4, порог 10 мс). Работает на индексе, без Vfs: те же операции
// index_ops, что выполняет Vfs, но за одну транзакцию.

#include <iostream>
#include <stdexcept>
#include <string>

#include "common/bench_common.h"
#include "e1/e1.h"
#include "meta/index_ops.h"

namespace telega::bench::e1 {

namespace {

constexpr int64_t kInlineBytes = 256;

Bytes to_bytes(const std::string& s) {
    Bytes b(s.size());
    for (size_t i = 0; i < s.size(); ++i) {
        b[i] = static_cast<std::byte>(s[i]);
    }
    return b;
}

meta::NewInode new_file(int64_t parent, const std::string& name) {
    meta::NewInode n;
    n.parent = parent;
    n.name = to_bytes(name);
    n.kind = meta::InodeKind::file;
    n.mode = 0644;
    n.uid = 1000;
    n.gid = 1000;
    n.now_ns = 1'700'000'000'000'000'000;
    return n;
}

// n файлов по 50 в каталоге, каталоги — в одном общем каталоге top. Возвращает inode файлов.
std::vector<int64_t> create_files(meta::MetaStore& m, int64_t top, int64_t n, bool with_inline,
                                  Rng& rng) {
    std::vector<int64_t> inos;
    inos.reserve(static_cast<size_t>(n));
    m.write([&](meta::Db& db) {
        int64_t dir = 0;
        for (int64_t i = 0; i < n; ++i) {
            if (i % 50 == 0) {
                meta::NewInode d = new_file(top, "d" + std::to_string(i / 50));
                d.kind = meta::InodeKind::dir;
                d.mode = 0755;
                dir = meta::insert_inode(db, d);
            }
            const int64_t ino = meta::insert_inode(db, new_file(dir, "f" + std::to_string(i)));
            if (with_inline) {
                Bytes data(kInlineBytes);
                rng.fill(data.data(), data.size());
                if (!meta::set_inline(db, ino, data, 1)) {
                    throw std::runtime_error("inline budget exhausted in stress run");
                }
                meta::AttrUpdate u;
                u.size = kInlineBytes;
                meta::update_attrs(db, ino, u);
            }
            inos.push_back(ino);
        }
    });
    return inos;
}

int64_t make_top(meta::MetaStore& m, const std::string& name) {
    return m.write([&](meta::Db& db) {
        meta::NewInode d = new_file(meta::kRootIno, name);
        d.kind = meta::InodeKind::dir;
        d.mode = 0755;
        return meta::insert_inode(db, d);
    });
}

void touch_all(meta::MetaStore& m, const std::vector<int64_t>& inos) {
    int64_t t = 1'800'000'000'000'000'000;
    m.write([&](meta::Db& db) {
        for (const int64_t ino : inos) {
            t += 3000;
            meta::AttrUpdate u;
            u.atime_ns = t;
            u.mtime_ns = t;
            u.ctime_ns = t;
            meta::update_attrs(db, ino, u);
        }
    });
}

struct Take {
    size_t patch_bytes = 0;
    int64_t locked_ns = 0;
    int64_t wait_ns = 0;
    int64_t lockhold_ns = 0;  // забор + запись в pending_patchsets + чтение pending
};

// То, что коммит-дельта делает под блокировкой записи (commit.cpp, шаг 2).
Take take_under_lock(meta::MountedFs& fs) {
    Take t;
    meta::TakeStats ts;
    const auto t0 = Clock::now();
    fs.meta().with_lock([&] {
        const Bytes patch = fs.meta().take_patchset(&ts);
        if (!patch.empty()) {
            fs.state().append_pending(patch);
        }
        (void)fs.state().list_pending();
    });
    t.lockhold_ns = ns_since(t0);
    t.patch_bytes = ts.patch_bytes;
    t.locked_ns = to_ns(ts.locked);
    t.wait_ns = to_ns(ts.wait);
    fs.state().clear_pending();
    return t;
}

}  // namespace

int run_stress(const Options& o) {
    Csv out(o.out / "stress_take.csv", {"scenario", "files", "rep", "patch_bytes", "take_locked_ns",
                                        "take_wait_ns", "lockhold_ns"});
    meta::CommitConfig cfg;
    cfg.checkpoint_ratio = 1e9;
    cfg.max_chain = 100'000;
    const int reps = std::max(o.repeats, 5);
    for (const std::string scenario : {"new-empty", "new-inline256", "touch", "rename+touch"}) {
        for (const int64_t n : {1'000, 2'000, 5'000, 10'000, 20'000, 50'000}) {
            for (int rep = 1; rep <= reps; ++rep) {
                ScratchFs env(o.work / "stress", cfg);
                auto& m = env.mounted->meta();
                Rng rng(o.seed + static_cast<uint64_t>(rep));
                const int64_t top = make_top(m, "top");
                Take t;
                if (scenario == "new-empty" || scenario == "new-inline256") {
                    (void)take_under_lock(*env.mounted);  // забрать создание top и сбросить сессию
                    create_files(m, top, n, scenario == "new-inline256", rng);
                    t = take_under_lock(*env.mounted);
                } else {
                    const auto inos = create_files(m, top, n, false, rng);
                    (void)take_under_lock(*env.mounted);  // создание уже «закоммичено»
                    if (scenario == "rename+touch") {
                        m.write([&](meta::Db& db) {
                            meta::move_inode(db, top, meta::kRootIno, to_bytes("top-renamed"));
                        });
                    }
                    touch_all(m, inos);
                    t = take_under_lock(*env.mounted);
                }
                out.row({scenario, cell(n), cell(rep), cell(static_cast<int64_t>(t.patch_bytes)),
                         cell(t.locked_ns), cell(t.wait_ns), cell(t.lockhold_ns)});
            }
            std::cout << "  stress " << scenario << " n=" << n << " done\n";
            out.flush();
        }
    }
    if (!o.keep) {
        std::error_code ec;
        fs::remove_all(o.work / "stress", ec);
    }
    return 0;
}

}  // namespace telega::bench::e1
