// Операции над индексом (meta/index_ops.h): дерево, экстенты, учёт чанков, инлайн.

#include "meta/index_ops.h"

#include <gtest/gtest.h>

#include <cerrno>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "meta/errors.h"
#include "meta/index_db.h"
#include "test_util.h"

namespace telega::meta {
namespace {

using test::bytes_of;
using test::new_chunk;
using test::new_node;

constexpr int64_t kGen = 5;

class IndexOps : public ::testing::Test {
  protected:
    void SetUp() override {
        MetaStore::create(dir_ / "index.sqlite", test::test_params());
        store_ = std::make_unique<MetaStore>(dir_ / "index.sqlite");
    }

    template <typename F>
    auto w(F&& fn) {
        return store_->write(std::forward<F>(fn));
    }
    template <typename F>
    auto r(F&& fn) {
        return store_->read(std::forward<F>(fn));
    }

    int64_t create(int64_t parent, std::string_view name, InodeKind kind = InodeKind::file) {
        return w([&](Db& db) { return insert_inode(db, new_node(parent, name, kind)); });
    }

    test::TempDir dir_;
    std::unique_ptr<MetaStore> store_;
};

// Код errno из FsError, брошенной fn, или 0.
template <typename F>
int fs_errno(F&& fn) {
    try {
        fn();
    } catch (const FsError& e) {
        return e.err();
    }
    return 0;
}

std::vector<std::string> names(const std::vector<Inode>& nodes) {
    std::vector<std::string> out;
    for (const Inode& n : nodes) {
        const Bytes b = test::must(n.name);
        out.emplace_back(reinterpret_cast<const char*>(b.data()), b.size());
    }
    return out;
}

TEST_F(IndexOps, CreatedIndexHasRootAndParams) {
    r([&](Db& db) {
        const auto root = get_inode(db, kRootIno);
        ASSERT_TRUE(root);
        EXPECT_EQ(root->kind, InodeKind::dir);
        EXPECT_FALSE(root->parent);
        EXPECT_FALSE(root->name);
        EXPECT_EQ(root->mode, 0755U);
        EXPECT_EQ(root->uid, 1000U);
        EXPECT_EQ(root->mtime_ns, 1'000'000'000);
        const IndexParams p = read_params(db);
        const InitParams expected = test::test_params();
        EXPECT_EQ(p.fs_uuid, expected.fs_uuid);
        EXPECT_EQ(p.format_version, 1);
        EXPECT_EQ(p.chunk_size, expected.chunk_size);
        EXPECT_EQ(p.inline_max, expected.inline_max);
        EXPECT_EQ(p.inline_budget, expected.inline_budget);
        EXPECT_EQ(p.pack_threshold, expected.pack_threshold);
        EXPECT_EQ(p.pack_target, expected.pack_target);
        Stmt st{db, "SELECT key FROM meta ORDER BY key"};
        std::vector<std::string> keys;
        while (st.step()) {
            keys.push_back(st.column_text(0));
        }
        EXPECT_EQ(keys,
                  (std::vector<std::string>{
                      "chunk_size", "format_version", "fs_uuid", "inline_budget", "inline_bytes",
                      "inline_max", "next_chunk_id", "next_ino", "pack_target", "pack_threshold"}));
    });
    EXPECT_THROW(MetaStore::create(dir_ / "index.sqlite", test::test_params()), std::runtime_error);
}

TEST_F(IndexOps, CreateLookupAndList) {
    const int64_t dir = create(kRootIno, "dir", InodeKind::dir);
    EXPECT_EQ(dir, 2);
    const int64_t b = create(kRootIno, "b");
    const int64_t a = create(kRootIno, "a");
    const int64_t link = create(dir, "link", InodeKind::symlink);
    create(dir, "sub", InodeKind::dir);

    r([&](Db& db) {
        const auto found = lookup(db, kRootIno, bytes_of("a"));
        ASSERT_TRUE(found);
        EXPECT_EQ(found->ino, a);
        EXPECT_EQ(found->kind, InodeKind::file);
        EXPECT_EQ(found->mode, 0644U);
        EXPECT_EQ(found->parent, kRootIno);
        EXPECT_FALSE(found->inline_len);
        EXPECT_FALSE(lookup(db, kRootIno, bytes_of("missing")));
        EXPECT_FALSE(lookup(db, dir, bytes_of("a")));

        const auto l = get_inode(db, link);
        ASSERT_TRUE(l);
        EXPECT_EQ(l->target, bytes_of("target"));
        EXPECT_EQ(l->size, 6);

        EXPECT_EQ(names(list_children(db, kRootIno, std::nullopt, 100)),
                  (std::vector<std::string>{"a", "b", "dir"}));
        EXPECT_EQ(names(list_children(db, kRootIno, std::nullopt, 2)),
                  (std::vector<std::string>{"a", "b"}));
        EXPECT_EQ(names(list_children(db, kRootIno, bytes_of("b"), 2)),
                  (std::vector<std::string>{"dir"}));
        EXPECT_TRUE(list_children(db, kRootIno, bytes_of("dir"), 2).empty());
        EXPECT_EQ(count_children(db, kRootIno), 3);
        EXPECT_EQ(count_subdirs(db, kRootIno), 1);
        EXPECT_EQ(count_children(db, dir), 2);
        EXPECT_EQ(count_subdirs(db, dir), 1);
        EXPECT_EQ(get_inode(db, b)->name, bytes_of("b"));
        EXPECT_FALSE(get_inode(db, 999));
    });
}

TEST_F(IndexOps, NamesAreBytesAndValidated) {
    // Имена — произвольные байты, сравниваются побайтово.
    const std::string high = "\xff\xfe";
    create(kRootIno, high);
    create(kRootIno, std::string(255, 'x'));
    r([&](Db& db) {
        EXPECT_TRUE(lookup(db, kRootIno, bytes_of(high)));
        EXPECT_EQ(names(list_children(db, kRootIno, std::nullopt, 10)).back(), high);
    });

    EXPECT_EQ(fs_errno([&] { create(kRootIno, std::string(256, 'x')); }), ENAMETOOLONG);
    EXPECT_EQ(fs_errno([&] { create(kRootIno, ""); }), EINVAL);
    EXPECT_EQ(fs_errno([&] { create(kRootIno, "a/b"); }), EINVAL);
    EXPECT_EQ(fs_errno([&] { create(kRootIno, std::string("a\0b", 3)); }), EINVAL);
    EXPECT_EQ(fs_errno([&] { create(kRootIno, "."); }), EINVAL);
    EXPECT_EQ(fs_errno([&] { create(kRootIno, ".."); }), EINVAL);
    EXPECT_EQ(fs_errno([&] { create(kRootIno, "..."); }), 0);
    EXPECT_EQ(fs_errno([&] {
                  r([&](Db& db) { lookup(db, kRootIno, bytes_of(std::string(300, 'y'))); });
              }),
              ENAMETOOLONG);
    EXPECT_EQ(fs_errno([&] {
                  w([&](Db& db) {
                      NewInode n = new_node(kRootIno, "l", InodeKind::symlink);
                      n.target.clear();
                      insert_inode(db, n);
                  });
              }),
              EINVAL);
    EXPECT_EQ(fs_errno([&] {
                  w([&](Db& db) {
                      NewInode n = new_node(kRootIno, "l", InodeKind::symlink);
                      n.target.assign(kSymlinkMax + 1, std::byte{'t'});
                      insert_inode(db, n);
                  });
              }),
              ENAMETOOLONG);
}

TEST_F(IndexOps, CreateErrorsAndRollback) {
    const int64_t file = create(kRootIno, "file");
    EXPECT_EQ(fs_errno([&] { create(kRootIno, "file"); }), EEXIST);
    EXPECT_EQ(fs_errno([&] { create(file, "x"); }), ENOTDIR);
    EXPECT_EQ(fs_errno([&] { create(999, "x"); }), ENOENT);

    // Исключение из write откатывает всю транзакцию, включая успешные операции до него.
    EXPECT_EQ(fs_errno([&] {
                  w([&](Db& db) {
                      insert_inode(db, new_node(kRootIno, "first"));
                      insert_inode(db, new_node(kRootIno, "file"));
                  });
              }),
              EEXIST);
    r([&](Db& db) { EXPECT_FALSE(lookup(db, kRootIno, bytes_of("first"))); });

    // Пойманная внутри write ошибка не портит транзакцию и не тратит номер inode.
    const int64_t next = w([&](Db& db) {
        EXPECT_THROW(insert_inode(db, new_node(kRootIno, "file")), FsError);
        return insert_inode(db, new_node(kRootIno, "second"));
    });
    EXPECT_EQ(next, file + 1);
}

TEST_F(IndexOps, RenameDetachDelete) {
    const int64_t d1 = create(kRootIno, "d1", InodeKind::dir);
    const int64_t d2 = create(kRootIno, "d2", InodeKind::dir);
    const int64_t f = create(d1, "f");
    const int64_t g = create(d2, "g");

    w([&](Db& db) { move_inode(db, f, d2, bytes_of("f2")); });
    r([&](Db& db) {
        EXPECT_FALSE(lookup(db, d1, bytes_of("f")));
        EXPECT_EQ(lookup(db, d2, bytes_of("f2"))->ino, f);
    });
    // Переименование в себя же — не ошибка.
    w([&](Db& db) { move_inode(db, f, d2, bytes_of("f2")); });
    EXPECT_EQ(fs_errno([&] { w([&](Db& db) { move_inode(db, f, d2, bytes_of("g")); }); }), EEXIST);
    EXPECT_EQ(fs_errno([&] { w([&](Db& db) { move_inode(db, kRootIno, d2, bytes_of("r")); }); }),
              EINVAL);
    EXPECT_EQ(fs_errno([&] { w([&](Db& db) { move_inode(db, f, g, bytes_of("x")); }); }), ENOTDIR);
    EXPECT_EQ(fs_errno([&] { w([&](Db& db) { move_inode(db, 999, d1, bytes_of("x")); }); }),
              ENOENT);
    EXPECT_EQ(fs_errno([&] { w([&](Db& db) { move_inode(db, f, d1, bytes_of("a/b")); }); }),
              EINVAL);

    // rename поверх существующего: вызывающий удаляет цель в той же транзакции.
    w([&](Db& db) {
        delete_inode(db, g, kGen);
        move_inode(db, f, d2, bytes_of("g"));
    });
    r([&](Db& db) {
        EXPECT_EQ(lookup(db, d2, bytes_of("g"))->ino, f);
        EXPECT_FALSE(get_inode(db, g));
    });

    // unlink открытого файла: запись исчезает, inode живёт до release.
    w([&](Db& db) { detach_inode(db, f); });
    r([&](Db& db) {
        EXPECT_FALSE(lookup(db, d2, bytes_of("g")));
        const auto node = get_inode(db, f);
        ASSERT_TRUE(node);
        EXPECT_FALSE(node->parent);
        EXPECT_FALSE(node->name);
        EXPECT_EQ(list_detached(db), (std::vector<int64_t>{f}));
        EXPECT_EQ(count_children(db, d2), 0);
    });
    create(d2, "g");  // имя свободно
    EXPECT_EQ(fs_errno([&] { w([&](Db& db) { detach_inode(db, kRootIno); }); }), EINVAL);

    EXPECT_EQ(fs_errno([&] { w([&](Db& db) { delete_inode(db, d2, kGen); }); }), ENOTEMPTY);
    EXPECT_EQ(fs_errno([&] { w([&](Db& db) { delete_inode(db, kRootIno, kGen); }); }), EINVAL);
    EXPECT_EQ(fs_errno([&] { w([&](Db& db) { delete_inode(db, 999, kGen); }); }), ENOENT);
    w([&](Db& db) {
        delete_inode(db, f, kGen);
        delete_inode(db, d1, kGen);
    });
    r([&](Db& db) {
        EXPECT_TRUE(list_detached(db).empty());
        EXPECT_FALSE(get_inode(db, d1));
        EXPECT_EQ(count_subdirs(db, kRootIno), 1);
    });
}

TEST_F(IndexOps, UpdateAttrsIsSelective) {
    const int64_t f = create(kRootIno, "f");
    w([&](Db& db) {
        AttrUpdate u;
        u.mode = 0100600;  // тип отбрасывается, хранятся только биты прав
        u.size = 12345;
        u.mtime_ns = 77;
        update_attrs(db, f, u);
    });
    r([&](Db& db) {
        const auto n = get_inode(db, f);
        EXPECT_EQ(n->mode, 0600U);
        EXPECT_EQ(n->size, 12345);
        EXPECT_EQ(n->mtime_ns, 77);
        EXPECT_EQ(n->atime_ns, 2'000'000'000);
        EXPECT_EQ(n->ctime_ns, 2'000'000'000);
        EXPECT_EQ(n->uid, 1000U);
    });
    w([&](Db& db) {
        AttrUpdate u;
        u.uid = 0;
        u.gid = 4000000000U;
        u.atime_ns = 1;
        u.ctime_ns = 2;
        update_attrs(db, f, u);
        update_attrs(db, f, AttrUpdate{});
    });
    r([&](Db& db) {
        const auto n = get_inode(db, f);
        EXPECT_EQ(n->uid, 0U);
        EXPECT_EQ(n->gid, 4000000000U);
        EXPECT_EQ(n->atime_ns, 1);
        EXPECT_EQ(n->ctime_ns, 2);
        EXPECT_EQ(n->mode, 0600U);
    });
    EXPECT_EQ(fs_errno([&] { w([&](Db& db) { update_attrs(db, 999, AttrUpdate{}); }); }), ENOENT);
    EXPECT_EQ(fs_errno([&] {
                  w([&](Db& db) {
                      AttrUpdate u;
                      u.mode = 0;
                      update_attrs(db, 999, u);
                  });
              }),
              ENOENT);
}

TEST_F(IndexOps, ExtentReplacementKeepsChunkAccounting) {
    const int64_t f = create(kRootIno, "f");
    w([&](Db& db) {
        const int64_t a = new_chunk(db, 100, kGen);
        const int64_t b = new_chunk(db, 100, kGen);
        EXPECT_EQ(get_chunk(db, a)->dead_since_gen, kGen) << "unreferenced chunk is dead";

        put_extent(db, {f, 0, a, 0, 100}, kGen);
        put_extent(db, {f, 1, a, 0, 50}, kGen);
        EXPECT_EQ(get_chunk(db, a)->refcount, 2);
        EXPECT_EQ(get_chunk(db, a)->live_bytes, 150);
        EXPECT_FALSE(get_chunk(db, a)->dead_since_gen);

        // Замена на другой чанк.
        put_extent(db, {f, 0, b, 0, 100}, kGen + 1);
        EXPECT_EQ(get_chunk(db, a)->refcount, 1);
        EXPECT_EQ(get_chunk(db, a)->live_bytes, 50);
        EXPECT_EQ(get_chunk(db, b)->refcount, 1);
        EXPECT_FALSE(get_chunk(db, b)->dead_since_gen);

        // Замена в пределах того же чанка: refcount тот же, live_bytes на разницу.
        put_extent(db, {f, 1, a, 10, 80}, kGen + 1);
        EXPECT_EQ(get_chunk(db, a)->refcount, 1);
        EXPECT_EQ(get_chunk(db, a)->live_bytes, 80);
        EXPECT_EQ(get_extent(db, f, 1)->chunk_off, 10);

        // Последняя ссылка уходит: чанк мёртв с поколения base_gen.
        put_extent(db, {f, 1, b, 0, 60}, kGen + 2);
        EXPECT_EQ(get_chunk(db, a)->refcount, 0);
        EXPECT_EQ(get_chunk(db, a)->live_bytes, 0);
        EXPECT_EQ(get_chunk(db, a)->dead_since_gen, kGen + 2);
        EXPECT_EQ(get_chunk(db, b)->refcount, 2);
        EXPECT_EQ(get_chunk(db, b)->live_bytes, 160);

        // Воскрешение (дедупликация): dead_since_gen сбрасывается.
        put_extent(db, {f, 7, a, 0, 1}, kGen + 3);
        EXPECT_EQ(get_chunk(db, a)->refcount, 1);
        EXPECT_FALSE(get_chunk(db, a)->dead_since_gen);

        const auto extents = list_extents(db, f);
        ASSERT_EQ(extents.size(), 3U);
        EXPECT_EQ(extents[0].idx, 0);
        EXPECT_EQ(extents[2].idx, 7);
        EXPECT_TRUE(test::chunk_accounting_consistent(db));
    });
}

TEST_F(IndexOps, PutExtentValidation) {
    const int64_t f = create(kRootIno, "f");
    const int64_t d = create(kRootIno, "d", InodeKind::dir);
    const int64_t c = w([&](Db& db) { return new_chunk(db, 100, kGen); });
    EXPECT_THROW(w([&](Db& db) { put_extent(db, {f, 0, c, 50, 51}, kGen); }),
                 std::invalid_argument);
    EXPECT_THROW(w([&](Db& db) { put_extent(db, {f, 0, c, 0, 0}, kGen); }), std::invalid_argument);
    EXPECT_THROW(w([&](Db& db) { put_extent(db, {f, -1, c, 0, 1}, kGen); }), std::invalid_argument);
    EXPECT_THROW(w([&](Db& db) { put_extent(db, {f, 0, 999, 0, 1}, kGen); }),
                 std::invalid_argument);
    EXPECT_EQ(fs_errno([&] { w([&](Db& db) { put_extent(db, {d, 0, c, 0, 1}, kGen); }); }), EINVAL);
    EXPECT_EQ(fs_errno([&] { w([&](Db& db) { put_extent(db, {999, 0, c, 0, 1}, kGen); }); }),
              ENOENT);
    w([&](Db& db) { ASSERT_TRUE(set_inline(db, f, bytes_of("x"), kGen)); });
    EXPECT_THROW(w([&](Db& db) { put_extent(db, {f, 0, c, 0, 1}, kGen); }), std::logic_error);
    // Переход из инлайн в позиционный: clear_inline и экстент одной транзакцией.
    w([&](Db& db) {
        clear_inline(db, f);
        put_extent(db, {f, 0, c, 0, 100}, kGen);
    });
    r([&](Db& db) {
        EXPECT_FALSE(get_inline(db, f));
        EXPECT_EQ(get_chunk(db, c)->refcount, 1);
    });
}

TEST_F(IndexOps, TruncateDropsAndShortensExtents) {
    const int64_t f = create(kRootIno, "f");
    w([&](Db& db) {
        const int64_t a = new_chunk(db, 1000, kGen);
        const int64_t b = new_chunk(db, 1000, kGen);
        put_extent(db, {f, 0, a, 0, 1000}, kGen);
        put_extent(db, {f, 1, b, 0, 1000}, kGen);
        put_extent(db, {f, 2, a, 0, 1000}, kGen);
        put_extent(db, {f, 3, b, 0, 400}, kGen);
        put_extent(db, {f, 5, b, 0, 10}, kGen);  // за дырой
        AttrUpdate u;
        u.size = 5 * 1000 + 10;
        update_attrs(db, f, u);
    });
    // truncate до 2,5 позиций.
    w([&](Db& db) {
        drop_extents_from(db, f, 3, kGen + 1);
        set_extent_len(db, f, 2, 500);
        AttrUpdate u;
        u.size = 2500;
        update_attrs(db, f, u);
        const auto a = get_chunk(db, 1);
        const auto b = get_chunk(db, 2);
        EXPECT_EQ(a->refcount, 2);
        EXPECT_EQ(a->live_bytes, 1500);
        EXPECT_EQ(b->refcount, 1);
        EXPECT_EQ(b->live_bytes, 1000);
        EXPECT_EQ(list_extents(db, f).size(), 3U);
        EXPECT_EQ(get_extent(db, f, 2)->len, 500);
        EXPECT_TRUE(test::chunk_accounting_consistent(db));
    });
    // Увеличить len нельзя: хвост после обратного расширения обязан читаться нулями.
    EXPECT_THROW(w([&](Db& db) { set_extent_len(db, f, 2, 501); }), std::invalid_argument);
    EXPECT_THROW(w([&](Db& db) { set_extent_len(db, f, 2, 0); }), std::invalid_argument);
    EXPECT_THROW(w([&](Db& db) { set_extent_len(db, f, 4, 1); }), std::logic_error);
    // truncate(0).
    w([&](Db& db) {
        drop_extents_from(db, f, 0, kGen + 2);
        EXPECT_TRUE(list_extents(db, f).empty());
        EXPECT_EQ(get_chunk(db, 1)->dead_since_gen, kGen + 2);
        EXPECT_EQ(get_chunk(db, 2)->dead_since_gen, kGen + 2);
        EXPECT_TRUE(test::chunk_accounting_consistent(db));
    });
}

TEST_F(IndexOps, DeleteInodeFreesChunks) {
    const int64_t f = create(kRootIno, "f");
    const int64_t g = create(kRootIno, "g");
    w([&](Db& db) {
        const int64_t a = new_chunk(db, 100, kGen);
        put_extent(db, {f, 0, a, 0, 100}, kGen);
        put_extent(db, {f, 1, a, 0, 100}, kGen);
        put_extent(db, {g, 0, a, 0, 30}, kGen);
    });
    w([&](Db& db) {
        delete_inode(db, f, kGen + 1);
        EXPECT_EQ(get_chunk(db, 1)->refcount, 1);
        EXPECT_EQ(get_chunk(db, 1)->live_bytes, 30);
        EXPECT_FALSE(get_chunk(db, 1)->dead_since_gen);
        detach_inode(db, g);
    });
    w([&](Db& db) {
        delete_inode(db, g, kGen + 2);
        EXPECT_EQ(get_chunk(db, 1)->refcount, 0);
        EXPECT_EQ(get_chunk(db, 1)->dead_since_gen, kGen + 2);
        EXPECT_TRUE(test::chunk_accounting_consistent(db));
    });
}

TEST_F(IndexOps, GcCandidatesAndChunkRows) {
    const int64_t f = create(kRootIno, "f");
    w([&](Db& db) {
        EXPECT_EQ(alloc_chunk_id(db), 1);
        EXPECT_EQ(alloc_chunk_id(db), 2);
        const int64_t a = new_chunk(db, 10, 3);
        const int64_t b = new_chunk(db, 10, 4);
        const int64_t c = new_chunk(db, 10, 4);
        EXPECT_EQ(a, 3);
        put_extent(db, {f, 0, c, 0, 10}, 4);
        EXPECT_THROW(delete_chunk_row(db, c), std::logic_error);
        (void)b;
    });
    r([&](Db& db) {
        std::vector<int64_t> ids;
        for (const Chunk& c : list_gc_candidates(db, 4)) {
            ids.push_back(c.chunk_id);
        }
        EXPECT_EQ(ids, (std::vector<int64_t>{3, 4}));
        EXPECT_EQ(list_gc_candidates(db, 3).size(), 1U);
        EXPECT_EQ(list_gc_candidates(db, 4, 1).size(), 1U);
        EXPECT_TRUE(list_gc_candidates(db, 2).empty());
        const auto c = get_chunk(db, 3);
        ASSERT_TRUE(c);
        EXPECT_EQ(c->kind, ChunkKind::positional);
        EXPECT_EQ(c->msg_id, 1003);
        EXPECT_EQ(c->remote_file_id, "remote-3");
        EXPECT_EQ(c->enc_size, 50);
        EXPECT_EQ(c->content_mac, test::mac_of(3));
    });
    store_->write([&](Db& db) { delete_chunk_row(db, 3); }, WriteKind::housekeeping);
    r([&](Db& db) {
        EXPECT_FALSE(get_chunk(db, 3));
        EXPECT_EQ(list_gc_candidates(db, 100).size(), 1U);
    });
}

TEST_F(IndexOps, FindChunkByMacOnlyPositional) {
    w([&](Db& db) {
        const int64_t a = new_chunk(db, 100, kGen);
        const int64_t p = new_chunk(db, 100, kGen, ChunkKind::pack);
        const auto found = find_chunk_by_mac(db, test::mac_of(a), 100);
        ASSERT_TRUE(found);
        EXPECT_EQ(found->chunk_id, a);
        EXPECT_FALSE(find_chunk_by_mac(db, test::mac_of(a), 101));
        EXPECT_FALSE(find_chunk_by_mac(db, test::mac_of(p), 100)) << "packs are not deduplicated";
    });
}

TEST_F(IndexOps, InlineDataAndBudget) {
    // inline_max = 64, inline_budget = 256 (test_params).
    std::vector<int64_t> files;
    files.reserve(6);
    for (int i = 0; i < 6; ++i) {
        files.push_back(create(kRootIno, "f" + std::to_string(i)));
    }
    const Bytes max_data(64, std::byte{'m'});
    w([&](Db& db) {
        EXPECT_FALSE(set_inline(db, files[0], Bytes(65, std::byte{'x'}), kGen)) << "> inline_max";
        for (int i = 0; i < 4; ++i) {
            EXPECT_TRUE(set_inline(db, files[i], max_data, kGen));
        }
        EXPECT_EQ(inline_total(db), 256);
        EXPECT_FALSE(set_inline(db, files[4], bytes_of("a"), kGen)) << "budget exhausted";
        // Перезапись уже инлайн-файла учитывает его старые данные.
        EXPECT_TRUE(set_inline(db, files[0], bytes_of("short"), kGen));
        EXPECT_EQ(inline_total(db), 192 + 5);
        EXPECT_TRUE(set_inline(db, files[0], max_data, kGen));
        // Пустой файл инлайн: пустой BLOB, а не NULL.
        EXPECT_FALSE(set_inline(db, files[5], Bytes(1), kGen));
        clear_inline(db, files[1]);
        EXPECT_TRUE(set_inline(db, files[5], Bytes{}, kGen));
    });
    r([&](Db& db) {
        EXPECT_EQ(inline_total(db), 192);
        EXPECT_EQ(get_inline(db, files[0]), max_data);
        EXPECT_FALSE(get_inline(db, files[1]));
        const auto empty = get_inline(db, files[5]);
        ASSERT_TRUE(empty);
        EXPECT_TRUE(empty->empty());
        EXPECT_EQ(get_inode(db, files[5])->inline_len, 0);
        EXPECT_EQ(get_inode(db, files[0])->inline_len, 64);
        EXPECT_FALSE(get_inode(db, files[1])->inline_len);
    });
    // Удаление инлайн-файла освобождает бюджет; счётчик meta.inline_bytes сходится с данными.
    w([&](Db& db) { delete_inode(db, files[0], kGen); });
    r([&](Db& db) {
        EXPECT_EQ(inline_total(db), 128);
        EXPECT_NO_THROW(MetaStore::verify(db));
    });
    EXPECT_EQ(fs_errno([&] { r([&](Db& db) { get_inline(db, 999); }); }), ENOENT);
}

TEST_F(IndexOps, SetInlineReleasesExtents) {
    const int64_t f = create(kRootIno, "f");
    const int64_t d = create(kRootIno, "d", InodeKind::dir);
    w([&](Db& db) {
        const int64_t a = new_chunk(db, 100, kGen);
        put_extent(db, {f, 0, a, 0, 100}, kGen);
        put_extent(db, {f, 3, a, 0, 20}, kGen);
    });
    w([&](Db& db) {
        ASSERT_TRUE(set_inline(db, f, bytes_of("tiny"), kGen + 1));
        EXPECT_TRUE(list_extents(db, f).empty());
        EXPECT_EQ(get_chunk(db, 1)->refcount, 0);
        EXPECT_EQ(get_chunk(db, 1)->dead_since_gen, kGen + 1);
        EXPECT_TRUE(test::chunk_accounting_consistent(db));
    });
    // Отказ по бюджету ничего не меняет.
    const int64_t g = create(kRootIno, "g");
    w([&](Db& db) {
        const int64_t b = new_chunk(db, 100, kGen);
        put_extent(db, {g, 0, b, 0, 100}, kGen);
        EXPECT_FALSE(set_inline(db, g, Bytes(100), kGen));
        EXPECT_EQ(list_extents(db, g).size(), 1U);
    });
    EXPECT_EQ(fs_errno([&] { w([&](Db& db) { set_inline(db, d, bytes_of("x"), kGen); }); }),
              EINVAL);
}

TEST_F(IndexOps, History) {
    store_->write(
        [&](Db& db) {
            add_history(db, 1, 100, BlobKind::snapshot);
            add_history(db, 2, 101, BlobKind::delta);
            add_history(db, 3, 102, BlobKind::delta);
        },
        WriteKind::housekeeping);
    r([&](Db& db) {
        const auto h = list_history(db);
        ASSERT_EQ(h.size(), 3U);
        EXPECT_EQ(h[0].kind, BlobKind::snapshot);
        EXPECT_EQ(h[2].msg_id, 102);
    });
    store_->write([&](Db& db) { delete_history_upto(db, 2); }, WriteKind::housekeeping);
    r([&](Db& db) {
        const auto h = list_history(db);
        ASSERT_EQ(h.size(), 1U);
        EXPECT_EQ(h[0].generation, 3);
    });
}

}  // namespace
}  // namespace telega::meta
