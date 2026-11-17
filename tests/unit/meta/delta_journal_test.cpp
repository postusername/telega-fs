// Дельта-журнал (SPEC 6.6, 14): индекс, собранный из снимка и патчей, совпадает с живым
// построчно; порча, перестановка и пропуск элемента цепочки обнаруживаются.

#include <gtest/gtest.h>
#include <sqlite3.h>

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <fstream>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "meta/errors.h"
#include "meta/index_db.h"
#include "meta/index_ops.h"
#include "test_util.h"

namespace telega::meta {
namespace {

using test::bytes_of;
using test::new_node;

std::string dump_file(const std::filesystem::path& path) {
    OpenOptions options;
    options.read_only = true;
    Db db = Db::open(path, options);
    return MetaStore::dump(db);
}

std::string journal_mode(const std::filesystem::path& path) {
    OpenOptions options;
    options.read_only = true;
    Db db = Db::open(path, options);
    Stmt st{db, "PRAGMA journal_mode"};
    st.step();
    return st.column_text(0);
}

class DeltaJournal : public ::testing::Test {
  protected:
    void SetUp() override {
        MetaStore::create(dir_ / "index.sqlite", test::test_params());
        store_ = std::make_unique<MetaStore>(dir_ / "index.sqlite");
        // Первый коммит после init — снимок.
        snapshot(snapshot_);
    }

    void snapshot(std::filesystem::path& out) {
        out = dir_ / ("snap-" + std::to_string(++files_) + ".sqlite");
        store_->vacuum_into(out);
    }

    std::string live_dump() {
        return store_->read([](Db& db) { return MetaStore::dump(db); });
    }

    // Собрать индекс из цепочки и вернуть его дамп.
    std::string rebuild(const std::filesystem::path& snap, const std::vector<Bytes>& patches) {
        const auto out = dir_ / ("built-" + std::to_string(++files_) + ".sqlite");
        MetaStore::build_from_chain(snap, patches, out);
        return dump_file(out);
    }

    int64_t create(int64_t parent, std::string_view name, InodeKind kind = InodeKind::file) {
        return store_->write(
            [&](Db& db) { return insert_inode(db, new_node(parent, name, kind)); });
    }

    test::TempDir dir_;
    std::unique_ptr<MetaStore> store_;
    std::filesystem::path snapshot_;
    int files_ = 0;
};

// --- property-тест ---

// Случайные операции ФС над индексом. Ошибки вида EEXIST — часть сценария: они либо
// откатывают всю транзакцию, либо ловятся внутри неё.
class RandomOps {
  public:
    RandomOps(MetaStore& store, uint64_t seed) : store_(store), rng_(seed) {}

    void step(int64_t base_gen) {
        base_gen_ = base_gen;
        const int op = pick(0, 99);
        try {
            if (op < 40) {
                // Несколько операций в одной транзакции, как одна операция ФС с побочными.
                store_.write([&](Db& db) {
                    const int n = pick(1, 3);
                    for (int i = 0; i < n; ++i) {
                        one_op(db);
                    }
                });
            } else if (op < 55) {
                store_.write([&](Db& db) { create_one(db); });
            } else if (op < 62) {
                store_.write([&](Db& db) { housekeeping(db); }, WriteKind::housekeeping);
            } else if (op < 66) {
                // Неудачная транзакция: изменения откатываются и в патч не попадают.
                store_.write([&](Db& db) {
                    one_op(db);
                    one_op(db);
                    throw std::runtime_error("injected");
                });
            } else {
                store_.write([&](Db& db) { one_op(db); });
            }
        } catch (const FsError&) {
            ++rolled_back_;
        } catch (const std::runtime_error& e) {
            if (std::string(e.what()) != "injected") {
                throw;
            }
            ++rolled_back_;
        }
    }

    [[nodiscard]] int rolled_back() const { return rolled_back_; }
    [[nodiscard]] int caught() const { return caught_; }

  private:
    int pick(int lo, int hi) { return std::uniform_int_distribution<int>(lo, hi)(rng_); }

    std::string random_name() {
        static constexpr std::string_view kLetters = "abcdefgh";
        std::string name(1, kLetters[static_cast<size_t>(pick(0, 7))]);
        if (pick(0, 1) == 1) {
            name += std::to_string(pick(0, 3));
        }
        return name;
    }

    // Случайный inode заданного вида (0 — любой не корень) или 0.
    int64_t random_ino(Db& db, const char* where) {
        const std::string count_sql = std::string("SELECT COUNT(*) FROM inodes WHERE ") + where;
        Stmt count{db, count_sql};
        count.step();
        const int64_t n = count.column_int64(0);
        if (n == 0) {
            return 0;
        }
        Stmt st{db, std::string("SELECT ino FROM inodes WHERE ") + where +
                        " ORDER BY ino LIMIT 1 OFFSET ?"};
        st.bind(1, static_cast<int64_t>(pick(0, static_cast<int>(n - 1))));
        st.step();
        return st.column_int64(0);
    }

    int64_t random_dir(Db& db) { return random_ino(db, "kind = 2"); }

    int64_t random_chunk(Db& db) {
        Stmt count{db, "SELECT COUNT(*) FROM chunks"};
        count.step();
        const int64_t n = count.column_int64(0);
        if (n == 0) {
            return 0;
        }
        Stmt st{db, "SELECT chunk_id FROM chunks ORDER BY chunk_id LIMIT 1 OFFSET ?"};
        st.bind(1, static_cast<int64_t>(pick(0, static_cast<int>(n - 1))));
        st.step();
        return st.column_int64(0);
    }
    int64_t random_file(Db& db) { return random_ino(db, "kind = 1"); }

    static bool is_ancestor(Db& db, int64_t ancestor, int64_t ino) {
        for (std::optional<int64_t> cur = ino; cur;) {
            if (*cur == ancestor) {
                return true;
            }
            cur = test::must(get_inode(db, *cur)).parent;
        }
        return false;
    }

    void create_one(Db& db) {
        const int64_t parent = random_dir(db);
        const int k = pick(0, 9);
        InodeKind kind = InodeKind::symlink;
        if (k < 6) {
            kind = InodeKind::file;
        } else if (k < 9) {
            kind = InodeKind::dir;
        }
        NewInode n = new_node(parent, random_name(), kind);
        n.now_ns = pick(1, 1'000'000);
        if (pick(0, 3) == 0) {
            // Пойманная ошибка внутри транзакции не мешает остальному.
            try {
                insert_inode(db, n);
            } catch (const FsError& e) {
                EXPECT_EQ(e.err(), EEXIST);
                ++caught_;
            }
        } else {
            insert_inode(db, n);
        }
    }

    void one_op(Db& db) {
        switch (pick(0, 10)) {
            case 0:
                create_one(db);
                break;
            case 1: {  // rename
                const int64_t ino = random_ino(db, "ino <> 1 AND parent IS NOT NULL");
                const int64_t to = random_dir(db);
                if (ino != 0 && !is_ancestor(db, ino, to)) {
                    move_inode(db, ino, to, bytes_of(random_name()));
                }
                break;
            }
            case 2: {  // unlink: сразу или через «открытый удалённый» файл
                const int64_t ino = random_ino(db, "kind <> 2 AND parent IS NOT NULL");
                if (ino != 0) {
                    if (pick(0, 1) == 0) {
                        delete_inode(db, ino, base_gen_);
                    } else {
                        detach_inode(db, ino);
                    }
                }
                break;
            }
            case 3: {  // release удалённого или rmdir
                const int64_t detached = random_ino(db, "parent IS NULL AND ino <> 1");
                if (detached != 0) {
                    delete_inode(db, detached, base_gen_);
                }
                const int64_t empty_dir =
                    random_ino(db,
                               "kind = 2 AND ino <> 1 AND NOT EXISTS "
                               "(SELECT 1 FROM inodes c WHERE c.parent = inodes.ino)");
                if (empty_dir != 0 && pick(0, 1) == 0) {
                    delete_inode(db, empty_dir, base_gen_);
                }
                break;
            }
            case 4:
            case 5: {  // запись: новый чанк или уже существующий (дедупликация)
                const int64_t ino = random_file(db);
                if (ino == 0) {
                    break;
                }
                if (test::must(get_inode(db, ino)).inline_len) {
                    clear_inline(db, ino);
                }
                int64_t chunk = pick(0, 2) == 0 ? random_chunk(db) : 0;
                int64_t plain = 0;
                if (chunk != 0) {
                    plain = test::must(get_chunk(db, chunk)).plain_size;
                } else {
                    plain = pick(1, 5000);
                    chunk =
                        test::new_chunk(db, plain, base_gen_,
                                        pick(0, 4) == 0 ? ChunkKind::pack : ChunkKind::positional);
                }
                const int64_t off = pick(0, static_cast<int>(plain - 1));
                const int64_t len = pick(1, static_cast<int>(plain - off));
                put_extent(db, {ino, pick(0, 5), chunk, off, len}, base_gen_);
                AttrUpdate u;
                u.size = pick(0, 6 * 5000);
                u.mtime_ns = pick(1, 1'000'000);
                update_attrs(db, ino, u);
                break;
            }
            case 6: {  // truncate
                const int64_t ino = random_file(db);
                if (ino == 0) {
                    break;
                }
                const int64_t first = pick(0, 5);
                drop_extents_from(db, ino, first, base_gen_);
                if (first > 0) {
                    if (const auto last = get_extent(db, ino, first - 1); last && last->len > 1) {
                        set_extent_len(db, ino, first - 1, pick(1, static_cast<int>(last->len)));
                    }
                }
                AttrUpdate u;
                u.size = first * 5000;
                update_attrs(db, ino, u);
                break;
            }
            case 7: {  // инлайн
                const int64_t ino = random_file(db);
                if (ino != 0) {
                    Bytes data(static_cast<size_t>(pick(0, 80)));
                    for (auto& b : data) {
                        b = static_cast<std::byte>(pick(0, 255));
                    }
                    set_inline(db, ino, data, base_gen_);
                }
                break;
            }
            case 8: {  // chmod, chown, utimens
                const int64_t ino = random_ino(db, "1");
                AttrUpdate u;
                if (pick(0, 1) == 0) {
                    u.mode = static_cast<uint32_t>(pick(0, 07777));
                }
                if (pick(0, 1) == 0) {
                    u.uid = static_cast<uint32_t>(pick(0, 5));
                }
                u.atime_ns = pick(1, 1'000'000);
                u.ctime_ns = pick(1, 1'000'000);
                update_attrs(db, ino, u);
                break;
            }
            case 9: {  // переименование с заменой цели
                const int64_t ino = random_ino(db, "kind = 1 AND parent IS NOT NULL");
                const int64_t victim = random_ino(db, "kind = 1 AND parent IS NOT NULL");
                if (ino != 0 && victim != 0 && ino != victim) {
                    const Inode target = test::must(get_inode(db, victim));
                    delete_inode(db, victim, base_gen_);
                    move_inode(db, ino, test::must(target.parent), test::must(target.name));
                }
                break;
            }
            default:
                create_one(db);
                break;
        }
    }

    void housekeeping(Db& db) {
        ++history_gen_;
        add_history(db, history_gen_, 10'000 + history_gen_,
                    pick(0, 1) == 0 ? BlobKind::snapshot : BlobKind::delta);
        for (const Chunk& c : list_gc_candidates(db, base_gen_, pick(0, 3))) {
            delete_chunk_row(db, c.chunk_id);
        }
        if (pick(0, 3) == 0) {
            delete_history_upto(db, history_gen_ - 3);
        }
    }

    MetaStore& store_;
    std::mt19937_64 rng_;
    int64_t base_gen_ = 0;
    int64_t history_gen_ = 0;
    int rolled_back_ = 0;
    int caught_ = 0;
};

class DeltaJournalProperty : public DeltaJournal, public ::testing::WithParamInterface<uint64_t> {};

TEST_P(DeltaJournalProperty, ChainRebuildsLiveIndex) {
    const uint64_t seed = GetParam();
    RandomOps ops{*store_, seed};
    std::mt19937_64 commits{seed ^ 0x9E3779B97F4A7C15ULL};
    auto chance = [&](int percent) {
        return std::uniform_int_distribution<int>(0, 99)(commits) < percent;
    };

    int64_t generation = 1;    // снимок из SetUp
    std::vector<Bytes> chain;  // дельты текущей цепочки
    std::vector<Bytes> pending;  // забранные, но не закоммиченные патчи
    int snapshots = 1;
    int deltas = 0;
    int failed = 0;
    int checks = 0;

    auto check = [&] {
        ++checks;
        ASSERT_EQ(rebuild(snapshot_, chain), live_dump())
            << "seed " << seed << ", generation " << generation;
    };

    constexpr int kOps = 400;
    for (int i = 0; i < kOps; ++i) {
        ops.step(generation);
        if (!chance(12)) {
            continue;
        }
        if (chance(15)) {
            // Снимок начинает новую цепочку и поглощает накопленные патчи.
            snapshot(snapshot_);
            chain.clear();
            pending.clear();
            ++snapshots;
            ++generation;
            ASSERT_NO_FATAL_FAILURE(check());
            continue;
        }
        pending.push_back(store_->take_patchset());
        if (chance(25)) {
            ++failed;  // заливка не удалась, патч остаётся в pending_patchsets
            continue;
        }
        chain.push_back(MetaStore::concat_patchsets(pending));
        pending.clear();
        ++deltas;
        ++generation;
        if (chance(30)) {
            ASSERT_NO_FATAL_FAILURE(check());
        }
    }
    pending.push_back(store_->take_patchset());
    chain.push_back(MetaStore::concat_patchsets(pending));
    ASSERT_NO_FATAL_FAILURE(check());

    // Сценарий не вырожденный.
    const std::string live = live_dump();
    EXPECT_GT(snapshots + deltas, 10);
    EXPECT_GT(failed, 0);
    EXPECT_GT(ops.rolled_back(), 0);
    RecordProperty("caught_in_tx", ops.caught());
    EXPECT_NE(live.find("\nextents|"), std::string::npos);
    EXPECT_NE(live.find("\nchunks|"), std::string::npos);
    EXPECT_NE(live.find("\nindex_history|"), std::string::npos);
    store_->read([](Db& db) { EXPECT_TRUE(test::chunk_accounting_consistent(db)); });
    RecordProperty("snapshots", snapshots);
    RecordProperty("deltas", deltas);
    RecordProperty("checks", checks);
}

INSTANTIATE_TEST_SUITE_P(Seeds, DeltaJournalProperty,
                         ::testing::Values(1ULL, 2ULL, 3ULL, 42ULL, 20261117ULL));

// --- отдельные сценарии ---

TEST_F(DeltaJournal, ConcatOfSeveralPending) {
    const int64_t d = create(kRootIno, "d", InodeKind::dir);
    const int64_t f = create(d, "f");
    std::vector<Bytes> pending;
    pending.push_back(store_->take_patchset());
    store_->write([&](Db& db) {
        const int64_t c = test::new_chunk(db, 100, 1);
        put_extent(db, {f, 0, c, 0, 100}, 1);
        move_inode(db, f, kRootIno, bytes_of("g"));
        insert_inode(db, new_node(d, "tmp"));
    });
    pending.push_back(store_->take_patchset());
    store_->write([&](Db& db) {
        delete_inode(db, lookup(db, d, bytes_of("tmp"))->ino, 2);
        delete_inode(db, f, 2);
        delete_inode(db, d, 2);
    });
    pending.push_back(store_->take_patchset());

    const std::string live = live_dump();
    EXPECT_EQ(rebuild(snapshot_, {MetaStore::concat_patchsets(pending)}), live);
    EXPECT_EQ(rebuild(snapshot_, pending), live);
    // Склейка ассоциативна по смыслу: (1+2)+3 даёт тот же индекс.
    const Bytes first_two = MetaStore::concat_patchsets(std::vector<Bytes>{pending[0], pending[1]});
    EXPECT_EQ(rebuild(snapshot_,
                      {MetaStore::concat_patchsets(std::vector<Bytes>{first_two, pending[2]})}),
              live);
    // Вставка и удаление внутри склейки взаимно уничтожаются.
    EXPECT_LT(MetaStore::concat_patchsets(pending).size(),
              pending[0].size() + pending[1].size() + pending[2].size());
}

TEST_F(DeltaJournal, ReorderedOrMissingPatchIsDetected) {
    const int64_t f = create(kRootIno, "f");
    const Bytes p1 = store_->take_patchset();  // INSERT f
    store_->write([&](Db& db) {
        AttrUpdate u;
        u.size = 10;
        update_attrs(db, f, u);
    });
    const Bytes p2 = store_->take_patchset();  // UPDATE f: строки нет без p1
    EXPECT_EQ(rebuild(snapshot_, {p1, p2}), live_dump());

    const auto out = dir_ / "bad.sqlite";
    EXPECT_THROW(MetaStore::build_from_chain(snapshot_, std::vector<Bytes>{p2, p1}, out),
                 IndexCorrupt);
    EXPECT_THROW(MetaStore::build_from_chain(snapshot_, std::vector<Bytes>{p2}, out), IndexCorrupt);
    // Повтор патча: INSERT уже существующей строки.
    EXPECT_THROW(MetaStore::build_from_chain(snapshot_, std::vector<Bytes>{p1, p1}, out),
                 IndexCorrupt);
    EXPECT_FALSE(std::filesystem::exists(out));
    EXPECT_FALSE(std::filesystem::exists(dir_ / "bad.sqlite.tmp-build"));
}

TEST_F(DeltaJournal, DanglingParentFailsForeignKeyCheck) {
    const int64_t d = create(kRootIno, "d", InodeKind::dir);
    const Bytes p1 = store_->take_patchset();
    create(d, "f");
    const Bytes p2 = store_->take_patchset();
    // Без p1 патч p2 применяется без конфликтов (вставка f и UPDATE next_ino), но f ссылается
    // на несуществующий каталог — это ловит foreign_key_check.
    try {
        MetaStore::build_from_chain(snapshot_, std::vector<Bytes>{p2}, dir_ / "bad.sqlite");
        FAIL() << "expected IndexCorrupt";
    } catch (const IndexCorrupt& e) {
        EXPECT_NE(std::string(e.what()).find("foreign key"), std::string::npos) << e.what();
    }
    EXPECT_EQ(rebuild(snapshot_, {p1, p2}), live_dump());
}

TEST_F(DeltaJournal, CorruptedPatchIsRejectedWithoutCrash) {
    const int64_t d = create(kRootIno, "dir", InodeKind::dir);
    store_->write([&](Db& db) {
        for (int i = 0; i < 5; ++i) {
            const int64_t f = insert_inode(db, new_node(d, "f" + std::to_string(i)));
            const int64_t c = test::new_chunk(db, 1000, 1);
            put_extent(db, {f, 0, c, 0, 1000}, 1);
        }
        add_history(db, 1, 77, BlobKind::snapshot);
    });
    const Bytes patch = store_->take_patchset();
    const std::string live = live_dump();
    ASSERT_EQ(rebuild(snapshot_, {patch}), live);

    int corrupt = 0;
    int diverged = 0;
    int same = 0;
    std::mt19937_64 rng{7};
    // Начало патча (заголовок таблицы и первые записи) целиком, дальше — выборка позиций.
    std::vector<size_t> positions;
    for (size_t pos = 0; pos < std::min<size_t>(64, patch.size()); ++pos) {
        positions.push_back(pos);
    }
    for (int i = 0; i < 150; ++i) {
        positions.push_back(static_cast<size_t>(rng() % patch.size()));
    }
    for (const size_t pos : positions) {
        Bytes bad = patch;
        const auto flip = static_cast<std::byte>(1U << (rng() % 8));
        bad[pos] ^= flip;
        const auto out = dir_ / ("corrupt-" + std::to_string(pos) + ".sqlite");
        try {
            MetaStore::build_from_chain(snapshot_, std::vector<Bytes>{bad}, out);
        } catch (const IndexCorrupt&) {
            ++corrupt;
            EXPECT_FALSE(std::filesystem::exists(out));
            continue;
        }
        (dump_file(out) == live ? same : diverged)++;
        std::filesystem::remove(out);
    }
    // Обрезанный патч.
    EXPECT_THROW(MetaStore::build_from_chain(
                     snapshot_, std::vector<Bytes>{Bytes(patch.begin(), patch.begin() + 7)},
                     dir_ / "short.sqlite"),
                 IndexCorrupt);
    RecordProperty("flips", static_cast<int>(positions.size()));
    RecordProperty("corrupt", corrupt);
    RecordProperty("diverged", diverged);
    RecordProperty("same", same);
    // Часть байтов (например, флаг indirect) на результат не влияет, остальное ловится
    // как порча или даёт другой индекс. Главное — без падений и UB.
    EXPECT_GT(corrupt, 0);
    EXPECT_GT(corrupt + diverged, static_cast<int>(positions.size()) / 2);
}

TEST_F(DeltaJournal, FailedSnapshotThenDeltaLosesChanges) {
    // SPEC 7.1: снимок сделан (VACUUM INTO, сессия пересоздана), но залить его не удалось.
    // Дельта от прежнего снимка не содержит изменений до VACUUM INTO.
    const std::filesystem::path old_snapshot = snapshot_;
    create(kRootIno, "before-vacuum");
    std::filesystem::path lost_snapshot;
    snapshot(lost_snapshot);
    std::filesystem::remove(lost_snapshot);  // неудавшийся снимок выброшен
    create(kRootIno, "after-vacuum");
    const Bytes delta = store_->take_patchset();

    const std::string live = live_dump();
    const std::string rebuilt = rebuild(old_snapshot, {delta});
    EXPECT_NE(rebuilt, live) << "the delta silently misses changes made before VACUUM INTO";
    const std::string lost_name = to_hex(bytes_of("before-vacuum"));
    EXPECT_NE(live.find(lost_name), std::string::npos);
    EXPECT_EQ(rebuilt.find(lost_name), std::string::npos);
    EXPECT_NE(rebuilt.find(to_hex(bytes_of("after-vacuum"))), std::string::npos);

    // Поэтому выставляется force_snapshot: следующий коммит — снимок, и он полон.
    std::filesystem::path forced;
    snapshot(forced);
    EXPECT_EQ(rebuild(forced, {}), live);
}

TEST_F(DeltaJournal, BuiltIndexIsWalAndUsable) {
    create(kRootIno, "a");
    const Bytes p = store_->take_patchset();
    const auto out = dir_ / "rebuilt.sqlite";
    // Старый файл с чужим WAL рядом заменяется целиком.
    std::filesystem::copy_file(snapshot_, out);
    { std::ofstream(dir_ / "rebuilt.sqlite-wal") << "garbage"; }
    MetaStore::build_from_chain(snapshot_, std::vector<Bytes>{p}, out);
    EXPECT_FALSE(std::filesystem::exists(dir_ / "rebuilt.sqlite-wal"));
    EXPECT_EQ(journal_mode(out), "wal");
    EXPECT_EQ(dump_file(out), live_dump());

    MetaStore reopened{out};
    reopened.write([](Db& db) { insert_inode(db, new_node(kRootIno, "b")); });
    reopened.read([](Db& db) {
        EXPECT_TRUE(lookup(db, kRootIno, bytes_of("a")));
        EXPECT_TRUE(lookup(db, kRootIno, bytes_of("b")));
        MetaStore::verify(db);
    });
}

TEST_F(DeltaJournal, GarbageSnapshotIsCorrupt) {
    const auto garbage = dir_ / "garbage.sqlite";
    { std::ofstream(garbage) << std::string(4096, 'z'); }
    EXPECT_THROW(MetaStore::build_from_chain(garbage, {}, dir_ / "out.sqlite"), IndexCorrupt);
    EXPECT_FALSE(std::filesystem::exists(dir_ / "out.sqlite"));
}

}  // namespace
}  // namespace telega::meta
