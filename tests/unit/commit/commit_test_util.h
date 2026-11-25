#pragma once

// Общие помощники тестов коммита и монтирования: канал на LocalDirStore во временном
// каталоге, «машины» (data-каталоги профиля), keyfile, подменённые часы, операции над индексом.

#include <gtest/gtest.h>
#include <spdlog/spdlog.h>

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "../meta/test_util.h"
#include "common/bytes.h"
#include "common/files.h"
#include "crypto/index_blob.h"
#include "crypto/keys.h"
#include "meta/commit.h"
#include "meta/index_db.h"
#include "meta/index_ops.h"
#include "meta/init.h"
#include "meta/mount.h"
#include "meta/state_db.h"
#include "store/local_dir_store.h"

namespace telega::meta::test {

// Часы, которые двигает тест. wait() не ждёт реального срока: просыпается каждые 2 мс и даёт
// фоновому потоку перепроверить условие по подменённому времени.
class FakeClock final : public Clock {
  public:
    time_point now() override {
        const std::lock_guard lock(mu_);
        return now_;
    }
    void advance(std::chrono::milliseconds d) {
        const std::lock_guard lock(mu_);
        now_ += d;
    }
    void wait(std::condition_variable& cv, std::unique_lock<std::mutex>& lock,
              std::optional<time_point> /*deadline*/) override {
        cv.wait_for(lock, std::chrono::milliseconds(2));
    }

  private:
    std::mutex mu_;
    time_point now_{std::chrono::hours(1)};
};

// Store, который прячет голову, как TDLib с кэшем ChannelFull: несколько ответов
// read_head подряд возвращают `shown` (nullopt — «нет головы») вместо настоящей головы. Всё
// остальное пробрасывает. Может ещё закрепить и ответить ошибкой («потерянный ответ» pin).
class MaskedHeadStore final : public store::Store {
  public:
    explicit MaskedHeadStore(store::Store& inner) : inner_(inner) {}

    // Следующие `reads` чтений головы вернут `shown`.
    void mask_next(int reads, std::optional<store::HeadInfo> shown = std::nullopt) {
        const std::lock_guard lock(mu_);
        masked_ = reads;
        shown_ = std::move(shown);
    }
    // После следующего set_head: `reads` чтений вернут `shown`. lose_reply: set_head закрепляет,
    // но бросает transient («ответ потерян»).
    void arm_after_pin(int reads, std::optional<store::HeadInfo> shown = std::nullopt,
                       bool lose_reply = false) {
        const std::lock_guard lock(mu_);
        armed_reads_ = reads;
        armed_shown_ = std::move(shown);
        lose_reply_ = lose_reply;
    }
    [[nodiscard]] int head_reads() const {
        const std::lock_guard lock(mu_);
        return head_reads_;
    }

    store::ChunkRef put_chunk(const std::filesystem::path& f) override {
        return inner_.put_chunk(f);
    }
    void read_range(const store::ChunkRef& r, uint64_t off, std::span<std::byte> out) override {
        inner_.read_range(r, off, out);
    }
    void delete_messages(std::span<const int64_t> ids) override { inner_.delete_messages(ids); }
    std::optional<store::HeadInfo> read_head() override {
        {
            const std::lock_guard lock(mu_);
            ++head_reads_;
            if (masked_ > 0) {
                --masked_;
                return shown_;
            }
        }
        return inner_.read_head();
    }
    Bytes read_index(int64_t id) override { return inner_.read_index(id); }
    int64_t put_index(const std::filesystem::path& f) override { return inner_.put_index(f); }
    void set_head(int64_t id, std::optional<int64_t> old) override {
        inner_.set_head(id, old);
        bool lose = false;
        {
            const std::lock_guard lock(mu_);
            masked_ = armed_reads_;
            shown_ = armed_shown_;
            armed_reads_ = 0;
            lose = std::exchange(lose_reply_, false);
        }
        if (lose) {
            throw store::StoreError(store::StoreErrorKind::transient, "reply lost");
        }
    }

  private:
    store::Store& inner_;
    mutable std::mutex mu_;
    int head_reads_ = 0;
    int masked_ = 0;
    std::optional<store::HeadInfo> shown_;
    int armed_reads_ = 0;
    std::optional<store::HeadInfo> armed_shown_;
    bool lose_reply_ = false;
};

// Дамп без служебных строк: index_history и мёртвые чанки (dead_since_gen не NULL) пишутся
// уборкой и шагом 8 после коммита и в блоб этого поколения не попадают.
inline std::string normalized(const std::string& dump) {
    std::istringstream in(dump);
    std::string out;
    for (std::string line; std::getline(in, line);) {
        if (line.starts_with("index_history|")) {
            continue;
        }
        if (line.starts_with("chunks|") && !line.ends_with("|NULL")) {
            continue;
        }
        out += line;
        out += '\n';
    }
    return out;
}

inline std::string dump_of(MountedFs& fs) {
    return fs.meta().read([](Db& db) { return MetaStore::dump(db); });
}

class CommitEnv : public ::testing::Test {
  public:
    static void SetUpTestSuite() {
        crypto::init();
        spdlog::set_level(spdlog::level::off);
    }

  protected:
    void SetUp() override {
        // Снимок пустого индекса — сотни байт, и уже одна дельта превысила бы 0,25 от него.
        // Тесты, которым нужны снимки по размеру, выставляют долю сами.
        commit_config_.checkpoint_ratio = 1e9;
        // Окно кэша TDLib в тестах на настоящих часах не ждём: тесты окна идут на
        // подменённых часах (commit_timing_test.cpp) и выставляют его сами.
        commit_config_.cas_retry_window = std::chrono::milliseconds{0};
        keyfile_ = tmp_ / "keyfile";
        const crypto::Key k = crypto::Key::random();
        write_file_atomic(keyfile_, k.span());
        reopen_store();
    }

    // Новый объект LocalDirStore на том же каталоге канала: «перезапуск процесса».
    void reopen_store() {
        store_ = std::make_unique<store::LocalDirStore>(channel_dir());
        store_->set_crash_handler([] {});  // вернуть управление: run_op бросит CrashInjected
    }

    [[nodiscard]] std::filesystem::path channel_dir() const { return tmp_ / "channel"; }
    [[nodiscard]] FsPaths machine(std::string_view name) const { return FsPaths{tmp_ / name}; }
    [[nodiscard]] KeySource key() const { return KeySource{std::nullopt, keyfile_}; }

    [[nodiscard]] InitOptions init_options() const {
        InitOptions o;
        o.key = key();
        o.chunk_size = 1 << 16;
        o.inline_max = 64;
        o.inline_budget = 4096;
        o.pack_threshold = 1 << 12;
        o.pack_target = 1 << 16;
        return o;
    }

    [[nodiscard]] MountOptions mount_options() const {
        MountOptions o;
        o.key = key();
        o.start_background = false;
        o.commit = commit_config_;
        return o;
    }

    void init(const FsPaths& paths) { init_fs(*store_, paths, init_options()); }
    void init() { init(a_); }

    std::unique_ptr<MountedFs> mount(const FsPaths& paths, std::optional<MountOptions> o = {}) {
        return mount_fs(*store_, paths, o ? *o : mount_options());
    }
    std::unique_ptr<MountedFs> mount_ro(const FsPaths& paths) {
        MountOptions o = mount_options();
        o.read_only = true;
        return mount(paths, o);
    }

    // Нормализованный дамп индекса, собранного из канала на пустом каталоге (--ro, чтобы
    // монтирование само ничего не меняло).
    std::string remote_state() {
        const FsPaths p = machine("verify-" + std::to_string(++verify_n_));
        std::string out;
        {
            auto fs = mount_ro(p);
            EXPECT_EQ(fs->report().situation, MountSituation::built_fresh);
            out = normalized(dump_of(*fs));
            fs->unmount();
        }
        std::filesystem::remove_all(p.data_dir);
        return out;
    }

    crypto::IndexHeader head_header() {
        const auto head = store_->read_head();
        if (!head) {
            throw std::logic_error("no head");
        }
        return crypto::parse_index_header(head->header);
    }

    [[nodiscard]] std::filesystem::path msg_file(int64_t msg_id) const {
        return channel_dir() / "msgs" / std::to_string(msg_id);
    }

    [[nodiscard]] bool has_message(int64_t msg_id) const {
        return std::filesystem::exists(msg_file(msg_id));
    }

    // Ошибка Store на следующем вызове op.
    void fault_next(store::StoreOp op, store::FaultAction action, uint64_t count = 1) {
        std::vector<store::FaultRule> rules;
        const uint64_t base = store_->call_count(op);
        for (uint64_t i = 1; i <= count; ++i) {
            rules.push_back(store::FaultRule{.op = op, .nth = base + i, .action = action});
        }
        store_->set_faults(rules);
    }

    // --- операции над индексом ---

    static int64_t mkfile(MountedFs& fs, std::string_view name, int64_t parent = kRootIno) {
        return fs.meta().write([&](Db& db) { return insert_inode(db, new_node(parent, name)); });
    }

    static int64_t mkdir(MountedFs& fs, std::string_view name, int64_t parent = kRootIno) {
        return fs.meta().write(
            [&](Db& db) { return insert_inode(db, new_node(parent, name, InodeKind::dir)); });
    }

    static void touch(MountedFs& fs, int64_t ino, int64_t mtime) {
        fs.meta().write([&](Db& db) {
            AttrUpdate u;
            u.mtime_ns = mtime;
            update_attrs(db, ino, u);
        });
    }

    // Залить настоящий чанк в канал и поставить его экстентом (ino, idx).
    int64_t put_real_chunk(MountedFs& fs, int64_t ino, int64_t idx, size_t size = 100) {
        const auto file = tmp_ / "chunk.bin";
        Bytes data(size);
        for (size_t i = 0; i < size; ++i) {
            data[i] = static_cast<std::byte>(i * 7 + static_cast<size_t>(idx));
        }
        write_file_atomic(file, data);
        const auto ref = store_->put_chunk(file);
        const uint64_t g = fs.committer().base_generation();
        return fs.meta().write([&](Db& db) {
            const int64_t id = alloc_chunk_id(db);
            NewChunk c;
            c.chunk_id = id;
            c.msg_id = ref.msg_id;
            c.remote_file_id = ref.remote_file_id;
            c.plain_size = static_cast<int64_t>(size);
            c.enc_size = static_cast<int64_t>(size);
            c.content_mac = mac_of(id);
            insert_chunk(db, c, static_cast<int64_t>(g));
            put_extent(db, Extent{ino, idx, id, 0, static_cast<int64_t>(size)},
                       static_cast<int64_t>(g));
            return id;
        });
    }

    static int64_t state_int(MountedFs& fs, std::string_view key) {
        return fs.state().get_int_or(key, -1);
    }

    TempDir tmp_;
    std::filesystem::path keyfile_;
    std::unique_ptr<store::LocalDirStore> store_;
    FsPaths a_{tmp_ / "a"};
    FsPaths b_{tmp_ / "b"};
    CommitConfig commit_config_;
    int verify_n_ = 0;
};

}  // namespace telega::meta::test
