// recover --export-dirty: конфликт G_r > G_l с локальными изменениями, выгрузка
// незакоммиченных данных из overlay в каталог. Частично перезаписанный большой файл дочитывается
// из залитых чанков; без Store и с пропавшими сообщениями неполные файлы перечисляются.

#include "vfs/export_dirty.h"

#include <fcntl.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <fstream>
#include <map>
#include <set>
#include <string>
#include <thread>

#include "../vfs/vfs_test_util.h"
#include "commit_test_util.h"
#include "vfs/vfs.h"

namespace telega::vfs {
namespace {

using meta::kRootIno;
using meta::MountError;
using meta::MountErrorKind;
using meta::test::bytes_of;
using meta::test::CommitEnv;
using test::pattern;

constexpr size_t kBigSize = 200 * 1024 + 123;  // 4 позиции по 64 КиБ (init_options)
constexpr size_t kSmall = 1500;                // класс «в пакете»

namespace fs = std::filesystem;

Bytes slurp(const fs::path& p) {
    std::ifstream in(p, std::ios::binary);
    Bytes out;
    std::array<char, 4096> buf{};
    while (in.read(buf.data(), buf.size()) || in.gcount() > 0) {
        const auto* b = reinterpret_cast<const std::byte*>(buf.data());
        out.insert(out.end(), b, b + in.gcount());
    }
    return out;
}

std::map<std::string, ExportedFile> by_path(const ExportReport& r) {
    std::map<std::string, ExportedFile> out;
    for (const ExportedFile& f : r.files) {
        out[f.path] = f;
    }
    return out;
}

class ExportDirty : public CommitEnv {
  protected:
    static VfsOptions options() {
        VfsOptions o = test::fast_options();
        o.pack_max_age = std::chrono::hours(1);
        o.sync_timeout = std::chrono::seconds(20);
        return o;
    }

    // Создать файл и закрыть (release ставит заливку): для того, что должно быть закоммичено.
    static int64_t put(Vfs& v, const std::string& name, const Bytes& data,
                       int64_t parent = kRootIno) {
        const Created c = v.create(parent, bytes_of(name), 0640, O_RDWR, 1000, 1000);
        EXPECT_EQ(v.write(c.attr.ino, 0, data), data.size());
        v.release(c.fh);
        v.forget(c.attr.ino, 1);
        return c.attr.ino;
    }
    // Создать и НЕ закрывать: файл остаётся в overlay, пока seal_delay (1 ч) не истёк.
    static int64_t put_open(Vfs& v, const std::string& name, const Bytes& data,
                            int64_t parent = kRootIno) {
        const Created c = v.create(parent, bytes_of(name), 0640, O_RDWR, 1000, 1000);
        EXPECT_EQ(v.write(c.attr.ino, 0, data), data.size());
        return c.attr.ino;
    }

    // Ждать, пока всё залитое применено к индексу (overlay пуст), в том числе закрытые пакеты.
    static void wait_applied(Vfs& v) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
        for (;;) {
            v.close_open_pack();  // запечатывание файлов в пакет идёт асинхронно после release
            const VfsStatus st = v.status();
            if (st.dirty_chunks == 0 && st.queued == 0 && st.uploading == 0 &&
                v.pack_status().open_objects == 0) {
                return;
            }
            ASSERT_LT(std::chrono::steady_clock::now(), deadline) << "uploads did not finish";
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
    }

    // Состояние A: закоммиченные файлы и незакоммиченные изменения, затем «падение» процесса,
    // затем машина B коммитит чужое поколение. Заполняет expected_.
    void build_conflict() {
        init();
        {
            auto fs = mount(a_);
            Vfs v{fs->meta(),      fs->state(), fs->store(),
                  fs->committer(), fs->keys(),  VfsPaths{a_.staging(), a_.tmp()},
                  options()};
            big_ = pattern(kBigSize, 1);
            const int64_t big = put(v, "big", big_);
            const Bytes keep = pattern(kSmall, 2);
            const int64_t keepi = put(v, "keep", keep);
            put(v, "gone", pattern(kSmall, 3));
            const int64_t ren = put(v, "ren", pattern(kSmall, 4));
            put(v, "committed_only", pattern(kSmall, 5));
            wait_applied(v);  // применение к индексу ставит ctime: сбрасывать время — после него
            // Время закоммиченных файлов — в прошлое: иначе все они попали бы в запас на скачки
            // часов (5 минут до committed_take_ns).
            fs->meta().write([&](meta::Db& db) {
                db.exec(
                    "UPDATE inodes SET ctime_ns = 1000, mtime_ns = 1000 "
                    "WHERE ino != 1 AND kind != 2");
            });
            v.sync();  // поколение 2: всё это в канале
            keys_ = fs->keys();

            // --- незакоммиченное ---
            // Частичная перезапись большого файла: позиции 1 и 3 в overlay (3 — с ростом файла),
            // позиции 0 и 2 остаются в залитых чанках.
            Bytes patch1 = pattern(5000, 11);
            Bytes patch2 = pattern(20, 12);
            v.write(big, 70000, patch1);
            v.write(big, kBigSize - 10, patch2);
            expected_["/big"] = big_;
            std::copy(patch1.begin(), patch1.end(), expected_["/big"].begin() + 70000);
            expected_["/big"].resize(kBigSize + 10);
            std::copy(patch2.begin(), patch2.end(), expected_["/big"].begin() + (kBigSize - 10));
            // Укорочение закоммиченного файла без грязных позиций меняет только индекс, строк
            // overlay нет: файл находится по ctime (committed_take_ns).
            SetAttr cut;
            cut.size = 1000;
            v.setattr(keepi, cut);
            expected_["/keep"] = Bytes(keep.begin(), keep.begin() + 1000);
            // Новые файлы: в пакет и инлайн-размера, оба в overlay.
            fresh_ = pattern(3000, 21);
            put_open(v, "fresh", fresh_);
            expected_["/fresh"] = fresh_;
            const Bytes tiny = pattern(10, 22);
            put_open(v, "fresh_tiny", tiny);
            expected_["/fresh_tiny"] = tiny;
            // Грязный файл переименован в новый каталог: путь по индексу.
            const Bytes ren_new = pattern(2500, 23);
            v.write(ren, 0, ren_new);
            const Attr dir = v.mkdir(kRootIno, bytes_of("dir"), 0755, 1000, 1000);
            v.rename(kRootIno, bytes_of("ren"), dir.ino, bytes_of("renamed"), 0);
            Bytes ren_expect = pattern(kSmall, 4);
            ren_expect.assign(ren_new.begin(), ren_new.end());
            expected_["/dir/renamed"] = ren_expect;
            // Грязный файл удалён: выгружать нечего.
            const int64_t dying = put_open(v, "dying", pattern(2000, 31));
            (void)dying;
            v.unlink(kRootIno, bytes_of("dying"));
            // Удалён закоммиченный файл.
            v.unlink(kRootIno, bytes_of("gone"));
            // «Падение»: без shutdown и unmount.
        }
        reopen_store();
        {
            auto fb = mount(b_);
            mkfile(*fb, "from_b");
            fb->committer().commit_now();
            fb->unmount();
        }
    }

    MountError conflict_error() {
        try {
            mount(a_);
        } catch (const MountError& e) {
            return e;
        }
        ADD_FAILURE() << "mount did not fail";
        return {MountErrorKind::local_state, "none"};
    }

    // Снимок локального состояния: строки overlay, содержимое staging, дамп индекса.
    struct Snapshot {
        std::string index_dump;
        std::vector<std::string> dirty;
        std::vector<std::string> staging;
        int64_t generation = 0;
        bool operator==(const Snapshot&) const = default;
    };
    Snapshot snapshot() {
        Snapshot s;
        {
            meta::OpenOptions ro;
            ro.read_only = true;
            meta::Db db = meta::Db::open(a_.index_db(), ro);
            s.index_dump = meta::MetaStore::dump(db);
        }
        meta::StateStore state{a_.state_db(), false};
        for (const auto& d : state.list_dirty_chunks()) {
            s.dirty.push_back(std::to_string(d.ino) + ":" + std::to_string(d.idx) + ":" +
                              std::to_string(d.version));
        }
        for (const auto& d : state.list_dirty_attrs()) {
            s.dirty.push_back("a" + std::to_string(d.ino) + ":" + std::to_string(d.size));
        }
        s.generation = state.get_int_or(meta::state_key::base_generation, 0);
        for (const auto& e : fs::directory_iterator(a_.staging())) {
            s.staging.push_back(e.path().filename().string() + ":" +
                                std::to_string(fs::file_size(e.path())));
        }
        std::sort(s.staging.begin(), s.staging.end());
        return s;
    }

    [[nodiscard]] fs::path out() const { return tmp_ / "export" / "dirty"; }

    void expect_matches_model(const ExportReport& r) {
        const auto files = by_path(r);
        for (const auto& [path, data] : expected_) {
            const auto it = files.find(path);
            ASSERT_NE(it, files.end()) << path;
            EXPECT_EQ(slurp(out() / path.substr(1)), data) << path;
            EXPECT_TRUE(it->second.complete()) << path;
            EXPECT_EQ(it->second.size, data.size()) << path;
        }
        EXPECT_EQ(files.size(), expected_.size());
        // Удалённое и не тронутое в overlay не выгружается.
        EXPECT_FALSE(fs::exists(out() / "dying"));
        EXPECT_FALSE(fs::exists(out() / "gone"));
        EXPECT_FALSE(fs::exists(out() / "committed_only"));
        EXPECT_FALSE(fs::exists(out() / "from_b"));
    }

    Bytes big_;
    Bytes fresh_;
    crypto::SubKeys keys_;
    std::map<std::string, Bytes> expected_;
};

TEST_F(ExportDirty, ConflictWithLocalChangesIsExportedAndNothingIsChanged) {
    build_conflict();
    const MountError e = conflict_error();
    EXPECT_EQ(e.kind(), MountErrorKind::local_conflict);
    EXPECT_NE(std::string(e.what()).find("export-dirty"), std::string::npos);

    const Snapshot before = snapshot();
    const std::vector<int64_t> channel_before = store_->list_messages();
    const std::vector<int64_t> pins_before = store_->pinned();
    ASSERT_FALSE(before.dirty.empty());

    const ExportReport report = export_dirty(a_, store_.get(), keys_, out());
    expect_matches_model(report);
    // Открытый, но удалённый файл (dying) не имеет имени в индексе: предупреждение, не выгрузка.
    ASSERT_EQ(report.warnings.size(), 1U);
    EXPECT_NE(report.warnings.front().find("unlinked"), std::string::npos);
    EXPECT_EQ(report.incomplete_count(), 0U);
    // Большой файл: позиции 1 и 3 из overlay, 0 и 2 дочитаны из залитых чанков.
    const auto files = by_path(report);
    EXPECT_EQ(files.at("/big").overlay_positions, 2U);
    EXPECT_EQ(fs::status(out() / "big").permissions() & fs::perms::owner_read,
              fs::perms::owner_read);

    // Ни локальное состояние, ни канал не тронуты; конфликт на месте.
    EXPECT_EQ(snapshot(), before);
    EXPECT_EQ(store_->list_messages(), channel_before);
    EXPECT_EQ(store_->pinned(), pins_before);
    EXPECT_EQ(conflict_error().kind(), MountErrorKind::local_conflict);
}

TEST_F(ExportDirty, WithoutStoreTheCommittedPartsOfAPartiallyOverwrittenFileAreListedAsMissing) {
    build_conflict();
    const ExportReport report = export_dirty(a_, nullptr, keys_, out());
    const auto files = by_path(report);
    ASSERT_TRUE(files.contains("/big"));
    // Позиции 0 и 2 лежат только в канале.
    EXPECT_EQ(files.at("/big").missing, (std::vector<int64_t>{0, 2}));
    EXPECT_GE(report.incomplete_count(), 1U);
    // Что смогли, записали: позиции 1 и 3 из staging на своих местах, недостающее — нули.
    const Bytes got = slurp(out() / "big");
    ASSERT_EQ(got.size(), kBigSize + 10);
    const Bytes& want = expected_["/big"];
    EXPECT_TRUE(std::equal(got.begin() + 65536, got.begin() + 131072, want.begin() + 65536));
    EXPECT_TRUE(std::equal(got.begin() + 196608, got.end(), want.begin() + 196608));
    EXPECT_TRUE(std::all_of(got.begin(), got.begin() + 65536,
                            [](std::byte b) { return b == std::byte{0}; }));
    // Файлы целиком из overlay выгружены полностью.
    EXPECT_EQ(slurp(out() / "fresh"), expected_["/fresh"]);
    EXPECT_EQ(slurp(out() / "fresh_tiny"), expected_["/fresh_tiny"]);
    EXPECT_TRUE(files.at("/fresh").complete());
}

TEST_F(ExportDirty, LostChunkMessagesMakeOnlyTheirFilesIncomplete) {
    build_conflict();
    // Пропало сообщение чанка позиции 2 большого файла.
    int64_t msg = 0;
    {
        meta::OpenOptions ro;
        ro.read_only = true;
        meta::Db db = meta::Db::open(a_.index_db(), ro);
        const int64_t big = meta::test::must(meta::lookup(db, kRootIno, bytes_of("big"))).ino;
        const auto ext = meta::test::must(meta::get_extent(db, big, 2));
        msg = meta::test::must(meta::get_chunk(db, ext.chunk_id)).msg_id;
    }
    store_->delete_messages(std::vector<int64_t>{msg});
    ExportOptions opts;
    opts.read_timeout = std::chrono::milliseconds{200};
    opts.fast_timeout = std::chrono::milliseconds{50};
    const ExportReport report = export_dirty(a_, store_.get(), keys_, out(), opts);
    const auto files = by_path(report);
    EXPECT_EQ(files.at("/big").missing, (std::vector<int64_t>{2}));
    EXPECT_EQ(files.at("/big").overlay_positions, 2U);
    const Bytes got = slurp(out() / "big");
    const Bytes& want = expected_["/big"];
    EXPECT_TRUE(std::equal(got.begin(), got.begin() + 65536, want.begin()));  // позиция 0 дочитана
    EXPECT_TRUE(files.at("/fresh").complete());
    EXPECT_EQ(report.incomplete_count(), 1U);
}

TEST_F(ExportDirty, OutputDirectoryMustBeEmptyAndIsCreatedPrivate) {
    build_conflict();
    fs::create_directories(tmp_ / "occupied");
    write_file_atomic(tmp_ / "occupied" / "x", bytes_of("x"));
    EXPECT_THROW(export_dirty(a_, store_.get(), keys_, tmp_ / "occupied"), std::runtime_error);
    // Пустой существующий каталог подходит.
    fs::create_directories(tmp_ / "empty");
    EXPECT_NO_THROW(export_dirty(a_, store_.get(), keys_, tmp_ / "empty"));
    // Новый вложенный создаётся с правами 0700.
    export_dirty(a_, store_.get(), keys_, out());
    EXPECT_EQ(fs::status(out()).permissions() & fs::perms::others_all, fs::perms::none);
    EXPECT_EQ(fs::status(out()).permissions() & fs::perms::owner_all, fs::perms::owner_all);
}

TEST_F(ExportDirty, NoLocalIndexIsAnError) {
    init();
    fs::remove(a_.index_db());
    EXPECT_THROW(export_dirty(a_, store_.get(), keys_, out()), std::runtime_error);
}

// --- залитое, но не закоммиченное (committed_take_ns) ---

int64_t wall_now() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

class ExportUncommitted : public ExportDirty {
  protected:
    // Закоммиченные "old*" (их время сброшено в прошлое), затем залитые, но не закоммиченные
    // файлы, пустой каталог и симлинк; «падение»; чужой коммит на машине B.
    void build_uploaded_uncommitted() {
        init();
        {
            auto fs = mount(a_);
            Vfs v{fs->meta(),      fs->state(), fs->store(),
                  fs->committer(), fs->keys(),  VfsPaths{a_.staging(), a_.tmp()},
                  options()};
            old_small_ = pattern(kSmall, 41);
            old_big_ = pattern(kBigSize, 42);
            put(v, "old_small", old_small_);
            put(v, "old_big", old_big_);
            put(v, "old_inline", pattern(10, 43));
            wait_applied(v);
            fs->meta().write([&](meta::Db& db) {
                db.exec(
                    "UPDATE inodes SET ctime_ns = 1000, mtime_ns = 1000, atime_ns = 1000 "
                    "WHERE ino != 1 AND kind != 2");
            });
            v.sync();  // committed_take_ns появляется здесь
            keys_ = fs->keys();

            uploaded_big_ = pattern(kBigSize, 51);
            uploaded_small_ = pattern(kSmall + 7, 52);
            inline_ = pattern(10, 53);
            put(v, "uploaded_big", uploaded_big_);
            put(v, "uploaded_small", uploaded_small_);
            put(v, "inl", inline_);
            const Attr dir = v.mkdir(kRootIno, bytes_of("newdir"), 0755, 1000, 1000);
            (void)dir;
            const Attr ln = v.symlink(kRootIno, bytes_of("ln"), bytes_of("old_small"), 1000, 1000);
            v.forget(ln.ino, 1);
            wait_applied(v);
            EXPECT_TRUE(fs->state().list_dirty_chunks().empty());
            EXPECT_TRUE(fs->state().list_dirty_attrs().empty());
            // Коммита не было: изменения только в локальном индексе. «Падение».
        }
        reopen_store();
        {
            auto fb = mount(b_);
            mkfile(*fb, "from_b");
            fb->committer().commit_now();
            fb->unmount();
        }
    }

    Bytes old_small_, old_big_, uploaded_big_, uploaded_small_, inline_;
};

TEST_F(ExportUncommitted, UploadedButNotCommittedDataIsExportedAndUntouchedFilesAreNot) {
    build_uploaded_uncommitted();
    EXPECT_EQ(conflict_error().kind(), MountErrorKind::local_conflict);
    const ExportReport report = export_dirty(a_, store_.get(), keys_, out());
    const auto files = by_path(report);
    EXPECT_EQ(slurp(out() / "uploaded_big"), uploaded_big_);
    EXPECT_EQ(slurp(out() / "uploaded_small"), uploaded_small_);
    EXPECT_EQ(slurp(out() / "inl"), inline_);
    EXPECT_TRUE(files.contains("/uploaded_big"));
    EXPECT_TRUE(files.contains("/inl"));
    ASSERT_TRUE(files.contains("/ln"));
    EXPECT_TRUE(files.at("/ln").symlink);
    EXPECT_EQ(fs::read_symlink(out() / "ln"), fs::path("old_small"));
    // Пустой каталог, изменённый после коммита, создан и перечислен.
    EXPECT_TRUE(fs::is_directory(out() / "newdir"));
    EXPECT_EQ(report.empty_dirs, (std::vector<std::string>{"/newdir"}));
    // Не менявшееся с коммита не выгружается.
    EXPECT_FALSE(fs::exists(out() / "old_small"));
    EXPECT_FALSE(fs::exists(out() / "old_big"));
    EXPECT_FALSE(fs::exists(out() / "old_inline"));
    EXPECT_FALSE(fs::exists(out() / "from_b"));
    EXPECT_EQ(report.incomplete_count(), 0U);
    EXPECT_TRUE(report.warnings.empty());
}

TEST_F(ExportUncommitted, WithoutCommittedTakeTimeTheOverlayAndAllInlineFilesAreExported) {
    build_uploaded_uncommitted();
    {
        meta::StateStore state{a_.state_db()};
        ASSERT_TRUE(state.get_int(meta::state_key::committed_take_ns).has_value());
        state.erase(meta::state_key::committed_take_ns);  // состояние без ключа committed_take_ns
    }
    const ExportReport report = export_dirty(a_, store_.get(), keys_, out());
    ASSERT_EQ(report.warnings.size(), 1U);
    EXPECT_NE(report.warnings.front().find("committed_take_ns"), std::string::npos);
    // Все инлайн-файлы, в том числе давно закоммиченный.
    EXPECT_EQ(slurp(out() / "inl"), inline_);
    EXPECT_TRUE(fs::exists(out() / "old_inline"));
    // Залитые чанки без overlay в запасном режиме не находятся.
    EXPECT_FALSE(fs::exists(out() / "uploaded_big"));
    EXPECT_FALSE(fs::exists(out() / "old_big"));
    EXPECT_TRUE(report.empty_dirs.empty());
}

TEST_F(ExportUncommitted, CommittedTakeTimeIsRecordedByEveryCommit) {
    init();
    auto fs = mount(a_);
    const auto read_take = [&] {
        return meta::test::must(fs->state().get_int(meta::state_key::committed_take_ns));
    };
    const int64_t first = read_take();  // init: снимок
    EXPECT_GT(first, 0);
    mkfile(*fs, "x");
    int64_t before = wall_now();
    fs->committer().commit_now();  // дельта
    int64_t after = wall_now();
    EXPECT_GE(read_take(), before);
    EXPECT_LE(read_take(), after);
    mkfile(*fs, "y");
    before = wall_now();
    fs->committer().commit(true);  // снимок
    after = wall_now();
    EXPECT_GE(read_take(), before);
    EXPECT_LE(read_take(), after);
    fs->unmount();
    // Новая машина: индекс собран из цепочки, изменённое позже ещё не в канале.
    before = wall_now();
    auto fb = mount(b_);
    EXPECT_GE(meta::test::must(fb->state().get_int(meta::state_key::committed_take_ns)), before);
}

// Каждая операция, меняющая inode, двигает ctime или mtime в индексе (или в dirty_attrs): иначе
// recover --export-dirty не нашёл бы файл.
TEST_F(ExportUncommitted, EveryInodeChangingOperationMovesCtimeOrMtime) {
    init();
    auto fs = mount(a_);
    Vfs v{fs->meta(),      fs->state(), fs->store(),
          fs->committer(), fs->keys(),  VfsPaths{a_.staging(), a_.tmp()},
          options()};
    const int64_t a = put(v, "a", pattern(kSmall, 1));
    const int64_t b = put(v, "b", pattern(kSmall, 2));
    const int64_t c = put(v, "c", pattern(kBigSize, 3));
    const int64_t d = put(v, "d", pattern(10, 4));
    wait_applied(v);
    const auto reset = [&] {
        fs->meta().write([&](meta::Db& db) {
            db.exec("UPDATE inodes SET ctime_ns = 1000, mtime_ns = 1000 WHERE ino != 1");
        });
    };
    const auto newest = [&](int64_t ino) {
        const Attr at = v.getattr(ino);
        return std::max(at.ctime_ns, at.mtime_ns);
    };
    const auto expect_moved = [&](int64_t ino, int64_t t0, const char* what) {
        EXPECT_GE(newest(ino), t0) << what;
    };

    reset();
    int64_t t0 = wall_now();
    SetAttr chmod;
    chmod.mode = 0600;
    v.setattr(a, chmod);
    expect_moved(a, t0, "chmod");
    t0 = wall_now();
    SetAttr chown;
    chown.uid = 1234;
    chown.gid = 4321;
    v.setattr(a, chown);
    expect_moved(a, t0, "chown");
    t0 = wall_now();
    SetAttr utimens;
    utimens.mtime_ns = 500;
    utimens.atime_ns = 500;
    v.setattr(a, utimens);
    expect_moved(a, t0, "utimens");
    t0 = wall_now();
    SetAttr shrink;
    shrink.size = 100;
    v.setattr(b, shrink);
    expect_moved(b, t0, "truncate");
    t0 = wall_now();
    v.write(c, 70000, pattern(5000, 9));
    expect_moved(c, t0, "write");
    t0 = wall_now();
    v.rename(kRootIno, bytes_of("d"), kRootIno, bytes_of("d2"), 0);
    expect_moved(d, t0, "rename");
    // Запечатывание и привязка залитого чанка (в том числе дедуплицированного): ctime —
    // момент изменения индекса, а не момент записи.
    v.sync();
    reset();
    const int64_t e = put_open(v, "e", pattern(kSmall, 5));
    const int64_t f_ino = put_open(v, "f", pattern(10, 6));
    const int64_t g = put_open(v, "g", pattern(kBigSize, 7));
    t0 = wall_now();  // после записей: mtime меньше t0
    v.sync();
    expect_moved(e, t0, "seal into a pack");
    expect_moved(f_ino, t0, "seal into inline");
    expect_moved(g, t0, "positional chunks");
    const Bytes same = pattern(1 << 16, 8);
    const int64_t h1 = put(v, "h1", same);
    wait_applied(v);
    reset();
    const int64_t h2 = put_open(v, "h2", same);
    t0 = wall_now();
    v.sync();
    expect_moved(h2, t0, "dedup binding");
    (void)h1;
    v.shutdown();
}

}  // namespace
}  // namespace telega::vfs
