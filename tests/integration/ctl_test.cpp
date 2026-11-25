// Интеграционные тесты управляющего сокета и обслуживающих команд status, sync, gc
// на LocalDirStore: настоящий бинарник telega-fs. Без /dev/fuse
// пропускаются (GTEST_SKIP).

#include <fcntl.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

#include <array>
#include <chrono>
#include <csignal>
#include <cstring>
#include <fstream>
#include <iostream>
#include <set>
#include <string>
#include <thread>

#include "harness.h"

namespace telega::itest {
namespace {

using nlohmann::json;
using namespace std::chrono_literals;

constexpr uint64_t kMiB = uint64_t{1024} * 1024;

// Сырой клиент сокета: независим от кода приложения. Одна строка туда, одна обратно; пустая
// строка — соединение закрыто без ответа.
std::string ctl_raw(const fs::path& sock, const std::string& line, milliseconds timeout = 60s) {
    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    std::string path = sock.string();
    int dirfd = -1;
    if (path.size() >= sizeof(addr.sun_path)) {  // длинный путь: через /proc/self/fd
        dirfd = ::open(sock.parent_path().c_str(), O_PATH | O_DIRECTORY | O_CLOEXEC);
        path = "/proc/self/fd/" + std::to_string(dirfd) + "/" + sock.filename().string();
    }
    EXPECT_LT(path.size(), sizeof(addr.sun_path));
    std::memcpy(addr.sun_path, path.c_str(), path.size() + 1);
    const int fd = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    std::string reply;
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0) {
        const std::string out = line + "\n";
        EXPECT_EQ(::send(fd, out.data(), out.size(), MSG_NOSIGNAL),
                  static_cast<ssize_t>(out.size()));
        timeval tv{};
        tv.tv_sec = static_cast<time_t>(timeout.count() / 1000);
        ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        std::array<char, 4096> buf{};
        for (;;) {
            const ssize_t n = ::recv(fd, buf.data(), buf.size(), 0);
            if (n <= 0) {
                break;
            }
            reply.append(buf.data(), static_cast<size_t>(n));
            if (reply.find('\n') != std::string::npos) {
                break;
            }
        }
    } else {
        ADD_FAILURE() << "cannot connect to " << sock << ": "
                      << std::generic_category().message(errno);
    }
    ::close(fd);
    if (dirfd >= 0) {
        ::close(dirfd);
    }
    while (!reply.empty() && reply.back() == '\n') {
        reply.pop_back();
    }
    return reply;
}

json ctl_cmd(const fs::path& sock, const std::string& cmd) {
    const std::string reply = ctl_raw(sock, R"({"proto":1,"cmd":")" + cmd + "\"}");
    return json::parse(reply);
}

class CtlTest : public ::testing::Test {
  protected:
    void SetUp() override {
        if (const std::string why = fuse_skip_reason(); !why.empty()) {
            GTEST_SKIP() << why;
        }
    }
    void TearDown() override {
        if (HasFailure() && env_) {
            std::cerr << "---- telega-fs.log (last machine) ----\n" << env_->log_text() << '\n';
        }
    }
    Env& start(ProfileSpec spec = {}) {
        env_ = std::make_unique<Env>(std::move(spec));
        env_->init();
        return *env_;
    }
    Env& env() { return *env_; }
    [[nodiscard]] fs::path sock() const { return env_->data_dir() / "ctl.sock"; }

    json status_json() {
        const RunResult r = env().run({"status", "--json"});
        EXPECT_TRUE(r.exit.ok()) << r.output;
        return json::parse(r.output);
    }
    size_t message_count() {
        size_t n = 0;
        for (const auto& de : fs::directory_iterator(env().local_dir() / "msgs")) {
            (void)de;
            ++n;
        }
        return n;
    }

    std::unique_ptr<Env> env_;
};

ProfileSpec manual_commits() {
    ProfileSpec spec;
    spec.extra = "commit_interval = \"1h\"\n";  // коммитов по таймеру нет: только явные
    return spec;
}

// --- сокет: права, протокол, жизненный цикл ---

TEST_F(CtlTest, SocketPermissionsProtocolAndCleanup) {
    Env& e = start();
    auto m = e.mount();
    struct stat st {};
    ASSERT_EQ(::lstat(sock().c_str(), &st), 0) << "the socket exists as soon as mount returns";
    EXPECT_TRUE(S_ISSOCK(st.st_mode));
    EXPECT_EQ(st.st_mode & 0777U, 0600U);
    ASSERT_EQ(::stat(e.data_dir().c_str(), &st), 0);
    EXPECT_EQ(st.st_mode & 0777U, 0700U);

    // Плохой JSON, неизвестные версия и команда: ошибка в ответе, демон жив.
    EXPECT_EQ(json::parse(ctl_raw(sock(), "{nope"))["error"]["code"], "bad_request");
    EXPECT_EQ(json::parse(ctl_raw(sock(), R"({"proto":2,"cmd":"status"})"))["error"]["code"],
              "unsupported");
    EXPECT_EQ(json::parse(ctl_raw(sock(), R"({"proto":1,"cmd":"format-disk"})"))["error"]["code"],
              "unsupported");
    const json ok = json::parse(ctl_raw(sock(), R"({"proto":1,"cmd":"status","args":{}})"));
    EXPECT_EQ(ok["proto"], 1);
    EXPECT_TRUE(ok["ok"].get<bool>());
    EXPECT_TRUE(ok["result"]["mounted"].get<bool>());
    EXPECT_TRUE(m->alive());

    ASSERT_TRUE(m->unmount().ok());
    EXPECT_FALSE(fs::exists(sock())) << "the socket is removed on unmount";
}

TEST_F(CtlTest, UnmountDoesNotHangOnIdleClientsAndRunningSync) {
    ProfileSpec spec;
    spec.sync_timeout = "30s";
    Env& e = start(spec);
    auto m = e.mount({}, {{"TELEGA_FS_FAULTS", "put_chunk:*:delay=2000"}});
    write_pattern_file(m->path("big"), 3 * kMiB, 3);

    // Молчащий клиент и `sync`, который ждёт медленной заливки.
    const int idle = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    std::strncpy(addr.sun_path, sock().c_str(), sizeof(addr.sun_path) - 1);
    ASSERT_EQ(::connect(idle, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
    std::string sync_reply;
    std::thread syncer([&] { sync_reply = ctl_raw(sock(), R"({"proto":1,"cmd":"sync"})"); });
    std::this_thread::sleep_for(500ms);

    const auto t0 = std::chrono::steady_clock::now();
    const Exit x = m->unmount(90s);
    EXPECT_LT(std::chrono::steady_clock::now() - t0, 80s);
    EXPECT_EQ(x.signal, 0);
    syncer.join();
    ::close(idle);
    EXPECT_FALSE(sync_reply.empty()) << "the running sync gets an answer instead of hanging";
    EXPECT_FALSE(fs::exists(sock()));
}

TEST_F(CtlTest, StaleSocketAfterSigkillIsReplacedOnNextMount) {
    Env& e = start(manual_commits());
    auto m = e.mount();
    write_pattern_file(m->path("dirty"), 300'000, 5);
    m->kill_hard();
    ASSERT_TRUE(fs::exists(sock())) << "SIGKILL leaves the socket file behind";

    // Без демона: «не смонтировано», а не зависание или ошибка соединения.
    RunResult r = e.run({"sync"});
    EXPECT_EQ(r.exit.code, 1);
    EXPECT_NE(r.output.find("not mounted"), std::string::npos) << r.output;
    r = e.run({"status"});
    ASSERT_TRUE(r.exit.ok()) << r.output;
    EXPECT_NE(r.output.find("not mounted"), std::string::npos) << r.output;
    EXPECT_NE(r.output.find("ended uncleanly"), std::string::npos) << r.output;

    m = e.mount();
    EXPECT_NE(e.log_text().find("removed the stale control socket"), std::string::npos);
    r = e.run({"sync"});
    EXPECT_TRUE(r.exit.ok()) << r.output;
    EXPECT_EQ(verify_pattern_file(m->path("dirty"), 300'000, 5), "");
    EXPECT_TRUE(m->unmount().ok());
}

// --- status ---

TEST_F(CtlTest, StatusMountedAndOffline) {
    Env& e = start(manual_commits());
    const json fresh = status_json();
    EXPECT_FALSE(fresh["mounted"].get<bool>());
    EXPECT_EQ(fresh["dirty_chunks"], 0);

    auto m = e.mount();
    write_pattern_file(m->path("pack"), 300'000, 9);  // мелкий файл: пакет, пока не sync
    ASSERT_EQ(::mkdir(m->path("dir").c_str(), 0755), 0);
    const json before = status_json();
    EXPECT_TRUE(before["mounted"].get<bool>());
    EXPECT_FALSE(before["read_only"].get<bool>());
    EXPECT_TRUE(before["index_dirty"].get<bool>());
    EXPECT_GE(before["dirty_chunks"].get<int>(), 1);
    EXPECT_GE(before["dirty_bytes"].get<int64_t>(), 300'000);
    EXPECT_GE(before["chain_length"].get<int>(), 1);
    EXPECT_EQ(before["fs_uuid"].get<std::string>().size(), 32U);

    RunResult r = e.run({"sync"});
    ASSERT_TRUE(r.exit.ok()) << r.output;
    EXPECT_NE(r.output.find("synced"), std::string::npos) << r.output;
    const json after = status_json();
    EXPECT_GT(after["generation"].get<uint64_t>(), before["generation"].get<uint64_t>());
    EXPECT_FALSE(after["index_dirty"].get<bool>());
    EXPECT_EQ(after["dirty_chunks"], 0);
    EXPECT_EQ(after["dirty_bytes"], 0);
    EXPECT_EQ(after["upload_queued"], 0);
    EXPECT_EQ(after["errors"].size(), 0U);

    // Человекочитаемый вывод в смонтированном режиме.
    r = e.run({"status"});
    ASSERT_TRUE(r.exit.ok()) << r.output;
    for (const char* word : {"mounted", "generation", "chain", "dirty data", "upload queue"}) {
        EXPECT_NE(r.output.find(word), std::string::npos) << word << "\n" << r.output;
    }
    EXPECT_EQ(r.output.find("not mounted"), std::string::npos) << r.output;

    ASSERT_TRUE(m->unmount().ok());
    // Без монтирования: те же поколение и цепочка, читаются из state.sqlite.
    const json off = status_json();
    EXPECT_FALSE(off["mounted"].get<bool>());
    EXPECT_EQ(off["generation"], after["generation"]);
    EXPECT_EQ(off["head_msg_id"], after["head_msg_id"]);
    EXPECT_EQ(off["chain_length"], after["chain_length"]);
    EXPECT_EQ(off["fs_uuid"], after["fs_uuid"]);
    EXPECT_TRUE(off["clean_shutdown"].get<bool>());
    r = e.run({"status"});
    ASSERT_TRUE(r.exit.ok()) << r.output;
    EXPECT_NE(r.output.find("not mounted"), std::string::npos) << r.output;

    // Нет профиля с ФС — понятная ошибка.
    r = e.run({"status", "-p", "nosuch"});
    EXPECT_EQ(r.exit.code, 1);
}

TEST_F(CtlTest, StatusReportsUploadErrors) {
    ProfileSpec spec = manual_commits();
    spec.sync_timeout = "3s";
    Env& e = start(spec);
    auto m = e.mount({}, {{"TELEGA_FS_FAULTS", "put_chunk:*:permanent"}});
    write_pattern_file(m->path("big"), 2 * kMiB, 4);
    const RunResult r = e.run({"sync"});
    EXPECT_EQ(r.exit.code, 1) << "sync fails when uploads fail";
    EXPECT_NE(r.output.find("sync failed"), std::string::npos) << r.output;
    const json st = status_json();
    EXPECT_GE(st["errors"].size(), 1U);
    EXPECT_GE(st["upload_failed"].get<int>(), 1);
    const RunResult human = e.run({"status"});
    EXPECT_NE(human.output.find("upload:"), std::string::npos) << human.output;
    m->kill_hard();
}

// --- sync ---

TEST_F(CtlTest, SyncMakesChangesSurviveSigkillOnAnotherMachine) {
    Env& e = start(manual_commits());
    auto m = e.mount();
    ASSERT_EQ(::mkdir(m->path("d").c_str(), 0755), 0);
    write_file(m->path("d/tiny"), {'t', 'i', 'n', 'y'});
    write_pattern_file(m->path("d/pack"), 200'000, 1);
    write_pattern_file(m->path("d/big"), 3 * kMiB + 7, 2);
    write_pattern_file(m->path("moved-src"), 50'000, 3);
    write_pattern_file(m->path("gone"), 60'000, 4);
    ASSERT_EQ(::rename(m->path("moved-src").c_str(), m->path("d/moved").c_str()), 0);
    ASSERT_EQ(::unlink(m->path("gone").c_str()), 0);
    ASSERT_EQ(::symlink("d/tiny", m->path("link").c_str()), 0);

    const RunResult r = e.run({"sync"});
    ASSERT_TRUE(r.exit.ok()) << r.output;

    // Контроль: правка после sync без sync в канал не попадает (коммитов по таймеру нет).
    write_pattern_file(m->path("late"), 10'000, 6);
    m->kill_hard();

    // «Другая машина»: индекс собирается только из канала.
    e.new_machine();
    auto m2 = e.mount();
    EXPECT_EQ(read_file(m2->path("d/tiny")), (std::vector<uint8_t>{'t', 'i', 'n', 'y'}));
    EXPECT_EQ(verify_pattern_file(m2->path("d/pack"), 200'000, 1), "");
    EXPECT_EQ(verify_pattern_file(m2->path("d/big"), 3 * kMiB + 7, 2), "");
    EXPECT_EQ(verify_pattern_file(m2->path("d/moved"), 50'000, 3), "");
    EXPECT_FALSE(fs::exists(m2->path("gone")));
    EXPECT_FALSE(fs::exists(m2->path("moved-src")));
    EXPECT_EQ(fs::read_symlink(m2->path("link")).string(), "d/tiny");
    EXPECT_FALSE(fs::exists(m2->path("late"))) << "an unsynced change must not be in the channel";
    EXPECT_TRUE(m2->unmount().ok());
}

TEST_F(CtlTest, SyncWhenNotMountedAndOnReadOnlyMount) {
    Env& e = start();
    RunResult r = e.run({"sync"});
    EXPECT_EQ(r.exit.code, 1);
    EXPECT_NE(r.output.find("not mounted"), std::string::npos) << r.output;

    {
        auto m = e.mount();
        write_pattern_file(m->path("f"), 1000, 1);
        ASSERT_TRUE(m->unmount().ok());
    }
    auto m = e.mount({"--ro"});
    r = e.run({"sync"});
    EXPECT_TRUE(r.exit.ok()) << "sync on --ro succeeds: nothing to save\n" << r.output;
    const json st = status_json();
    EXPECT_TRUE(st["mounted"].get<bool>());
    EXPECT_TRUE(st["read_only"].get<bool>());
    // snapshot в --ro: коммитить нечем.
    EXPECT_EQ(ctl_cmd(sock(), "snapshot")["error"]["code"], "read_only");
    EXPECT_TRUE(m->unmount().ok());
}

TEST_F(CtlTest, SyncAfterCasConflictFails) {
    Env& e = start(manual_commits());
    const fs::path data_a = e.data_dir().parent_path().parent_path();
    auto a = e.mount();
    write_pattern_file(a->path("a-file"), 1000, 1);

    // Другая машина с тем же каналом сдвигает голову.
    e.new_machine();
    {
        auto b = e.mount({}, {}, "", "mnt-b");
        write_pattern_file(b->path("b-file"), 1000, 2);
        ASSERT_TRUE(b->unmount().ok());
    }

    // Машина A: коммит видит чужую голову, ФС становится read-only, sync отвечает ошибкой.
    const EnvMap on_a{{"XDG_DATA_HOME", data_a.string()}};
    const RunResult r = e.run({"sync"}, on_a);
    EXPECT_EQ(r.exit.code, 1) << r.output;
    EXPECT_NE(r.output.find("sync failed"), std::string::npos) << r.output;
    const RunResult st = e.run({"status", "--json"}, on_a);
    ASSERT_TRUE(st.exit.ok()) << st.output;
    EXPECT_TRUE(json::parse(st.output)["read_only"].get<bool>());
    EXPECT_EQ(json::parse(ctl_raw(data_a / "telega-fs" / "main" / "ctl.sock",
                                  R"({"proto":1,"cmd":"sync"})"))["error"]["code"],
              "read_only");
    a->kill_hard();
}

TEST_F(CtlTest, SnapshotCommandMakesNextCommitASnapshot) {
    ProfileSpec spec = manual_commits();
    spec.extra += "checkpoint_ratio = 100\n";  // на крошечном индексе дельты остаются дельтами
    Env& e = start(spec);
    auto m = e.mount();
    write_pattern_file(m->path("one"), 1000, 1);
    ASSERT_TRUE(e.run({"sync"}).exit.ok());
    write_pattern_file(m->path("two"), 1000, 2);
    ASSERT_TRUE(e.run({"sync"}).exit.ok());
    const json before = status_json();
    EXPECT_GE(before["chain_length"].get<int>(), 2) << "the second commit is a delta";

    const json reply = ctl_cmd(sock(), "snapshot");
    ASSERT_TRUE(reply["ok"].get<bool>()) << reply;
    const json after = status_json();
    EXPECT_EQ(reply["result"]["generation"], after["generation"]);
    EXPECT_EQ(after["generation"].get<uint64_t>(), before["generation"].get<uint64_t>() + 1);
    EXPECT_EQ(after["chain_length"], 1) << "a snapshot starts a new chain";
    EXPECT_TRUE(m->unmount().ok());

    e.new_machine();
    auto m2 = e.mount();
    EXPECT_EQ(verify_pattern_file(m2->path("one"), 1000, 1), "");
    EXPECT_EQ(verify_pattern_file(m2->path("two"), 1000, 2), "");
    EXPECT_TRUE(m2->unmount().ok());
}

// --- gc ---

TEST_F(CtlTest, GcRefusesWhileMountedAndFailsWithoutFileSystem) {
    Env& e = start();
    auto m = e.mount();
    const RunResult r = e.run({"gc"});
    EXPECT_EQ(r.exit.code, 1);
    EXPECT_NE(r.output.find("mounted"), std::string::npos) << r.output;
    EXPECT_TRUE(m->alive());
    ASSERT_TRUE(m->unmount().ok());
    EXPECT_TRUE(e.run({"gc"}).exit.ok());

    const RunResult none = e.run({"gc", "-p", "nosuch"});
    EXPECT_EQ(none.exit.code, 1);
}

TEST_F(CtlTest, GcDeletesMaturedChunks) {
    ProfileSpec spec = manual_commits();
    spec.extra += "retain_generations = 100\n";  // демон ничего не убирает
    Env& e = start(spec);
    {
        auto m = e.mount();
        write_pattern_file(m->path("doomed"), 2 * kMiB + 5, 1);  // 3 чанка
        write_pattern_file(m->path("keep"), 2 * kMiB + 9, 2);    // 3 чанка
        ASSERT_TRUE(e.run({"sync"}).exit.ok());
        ASSERT_EQ(::unlink(m->path("doomed").c_str()), 0);
        // Несколько поколений после удаления.
        for (uint64_t i = 0; i < 3; ++i) {
            write_pattern_file(m->path("tick" + std::to_string(i)), 100, 10 + i);
            ASSERT_TRUE(e.run({"sync"}).exit.ok());
        }
        ASSERT_TRUE(m->unmount().ok());
    }
    const size_t before = message_count();

    // retain_generations = 100: уборка ничего не находит.
    RunResult r = e.run({"gc"});
    ASSERT_TRUE(r.exit.ok()) << r.output;
    EXPECT_NE(r.output.find("gc: deleted 0 chunk message(s)"), std::string::npos) << r.output;
    EXPECT_EQ(message_count(), before);

    // Профиль с retain_generations = 1: мёртвые чанки созрели.
    ProfileSpec lowered = spec;
    lowered.extra = "commit_interval = \"1h\"\nretain_generations = 1\n";
    e.write_config(lowered);
    r = e.run({"gc"});
    ASSERT_TRUE(r.exit.ok()) << r.output;
    EXPECT_NE(r.output.find("gc: deleted 3 chunk message(s)"), std::string::npos) << r.output;
    EXPECT_LE(message_count(), before - 3);

    // Повтор ничего не находит, данные целы, ФС монтируется.
    r = e.run({"gc"});
    ASSERT_TRUE(r.exit.ok()) << r.output;
    EXPECT_NE(r.output.find("gc: deleted 0 chunk message(s)"), std::string::npos) << r.output;
    auto m = e.mount();
    EXPECT_EQ(verify_pattern_file(m->path("keep"), 2 * kMiB + 9, 2), "");
    EXPECT_FALSE(fs::exists(m->path("doomed")));
    EXPECT_TRUE(m->unmount().ok());
    EXPECT_NE(e.log_text().find("mounted"), std::string::npos);
}

TEST_F(CtlTest, GcCompactRepacksDeadPacksAndDataStaysReadable) {
    ProfileSpec spec = manual_commits();
    spec.extra += "retain_generations = 1\n";
    Env& e = start(spec);  // chunk_size 1 МиБ: pack_threshold 512 КиБ, pack_target 1 МиБ
    constexpr int kFiles = 200;
    const auto size_of = [](int i) -> uint64_t {
        return 4'200 + static_cast<uint64_t>((i * 7919) % 90'000);
    };
    const auto name_of = [](int i) { return "f" + std::to_string(i); };
    {
        auto m = e.mount();
        for (int i = 0; i < kFiles; ++i) {
            write_file(m->path(name_of(i)), pattern(0, size_of(i), static_cast<uint64_t>(i) + 1));
        }
        ASSERT_TRUE(e.run({"sync"}).exit.ok());
        // Удаляем 9 файлов из 10: пакеты почти целиком мёртвые.
        for (int i = 0; i < kFiles; ++i) {
            if (i % 10 != 0) {
                ASSERT_EQ(::unlink(m->path(name_of(i)).c_str()), 0);
            }
        }
        ASSERT_TRUE(m->unmount().ok());
    }
    const size_t before = message_count();

    const RunResult r = e.run({"gc", "--compact"});
    ASSERT_TRUE(r.exit.ok()) << r.output;
    EXPECT_NE(r.output.find("compact:"), std::string::npos) << r.output;
    EXPECT_EQ(r.output.find("compact: 0 pack(s)"), std::string::npos) << r.output;
    EXPECT_NE(r.output.find(", committed"), std::string::npos) << r.output;
    std::cout << r.output;

    // Состояние после: ФС чистая, данные читаются и на этой, и на другой машине.
    EXPECT_TRUE(status_json()["clean_shutdown"].get<bool>());
    const auto check = [&](Mount& mnt) {
        for (int i = 0; i < kFiles; ++i) {
            if (i % 10 == 0) {
                EXPECT_EQ(verify_pattern_file(mnt.path(name_of(i)), size_of(i),
                                              static_cast<uint64_t>(i) + 1),
                          "")
                    << i;
            } else {
                EXPECT_FALSE(fs::exists(mnt.path(name_of(i)))) << i;
            }
        }
    };
    {
        auto m = e.mount();
        check(*m);
        ASSERT_TRUE(m->unmount().ok());
    }
    // Старые пакеты уходят уборкой: сообщений в канале не больше, чем было, плюс новые пакеты.
    EXPECT_LE(message_count(), before + 4);
    // Повторное уплотнение нечего делать: пакеты уже плотные.
    const RunResult again = e.run({"gc", "--compact"});
    ASSERT_TRUE(again.exit.ok()) << again.output;
    EXPECT_NE(again.output.find("compact: 0 pack(s)"), std::string::npos) << again.output;
    e.new_machine();
    auto m = e.mount();
    check(*m);
    EXPECT_TRUE(m->unmount().ok());
}

}  // namespace
}  // namespace telega::itest
