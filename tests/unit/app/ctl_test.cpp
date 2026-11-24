// Управляющий сокет (SPEC 12.4) и состояние для `status` (SPEC 12.3): протокол без сокетов,
// сервер на настоящем unix-сокете с фейковыми обработчиками, чтение state.sqlite только на чтение.

#include <gtest/gtest.h>
#include <stdlib.h>  // NOLINT(modernize-deprecated-headers): mkdtemp
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "app/ctl_client.h"
#include "app/ctl_proto.h"
#include "app/ctl_socket.h"
#include "app/status.h"
#include "meta/state_db.h"

namespace telega::app {
namespace {

namespace fs = std::filesystem;
using nlohmann::json;
using std::chrono::milliseconds;
using std::chrono::seconds;

class TempDir {
  public:
    explicit TempDir(const std::string& prefix = "telega-ctl-") {
        std::string tmpl = (fs::temp_directory_path() / (prefix + "XXXXXX")).string();
        if (::mkdtemp(tmpl.data()) == nullptr) {
            throw std::runtime_error("mkdtemp failed");
        }
        path_ = tmpl;
    }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path_, ec);
    }
    [[nodiscard]] const fs::path& path() const { return path_; }

  private:
    fs::path path_;
};

json parse(const std::string& line) { return json::parse(line); }

ctl::Handlers simple_handlers() {
    ctl::Handlers h;
    h.status = [] { return json{{"generation", 7}}; };
    h.sync = [] { return json{{"generation", 8}}; };
    h.snapshot = [] { return json{{"generation", 9}}; };
    return h;
}

// --- протокол ---

TEST(CtlProto, Commands) {
    const ctl::Handlers h = simple_handlers();
    for (const auto& [cmd, gen] : {std::pair{"status", 7}, {"sync", 8}, {"snapshot", 9}}) {
        const json r = parse(ctl::dispatch(R"({"proto":1,"cmd":")" + std::string(cmd) + "\"}", h));
        EXPECT_EQ(r["proto"], 1);
        EXPECT_TRUE(r["ok"].get<bool>());
        EXPECT_EQ(r["result"]["generation"], gen);
    }
    // args необязательны и могут быть пустым объектом или null.
    EXPECT_TRUE(parse(ctl::dispatch(R"({"proto":1,"cmd":"status","args":{}})", h))["ok"]);
    EXPECT_TRUE(parse(ctl::dispatch(R"({"proto":1,"cmd":"status","args":null})", h))["ok"]);
}

TEST(CtlProto, BadJson) {
    const ctl::Handlers h = simple_handlers();
    for (const char* line :
         {"", "not json", "{", R"({"proto":1,)", "[1,2]", "42", "null", R"("status")"}) {
        const json r = parse(ctl::dispatch(line, h));
        EXPECT_FALSE(r["ok"].get<bool>()) << line;
        EXPECT_EQ(r["error"]["code"], "bad_request") << line;
    }
}

TEST(CtlProto, BadShape) {
    const ctl::Handlers h = simple_handlers();
    for (const char* line :
         {R"({"cmd":"status"})", R"({"proto":1})", R"({"proto":1,"cmd":5})",
          R"({"proto":1,"cmd":"status","args":[1]})", R"({"proto":1,"cmd":"status","args":"x"})"}) {
        const json r = parse(ctl::dispatch(line, h));
        EXPECT_EQ(r["error"]["code"], "bad_request") << line;
    }
}

TEST(CtlProto, UnsupportedVersionAndCommand) {
    const ctl::Handlers h = simple_handlers();
    for (const char* line : {R"({"proto":2,"cmd":"status"})", R"({"proto":0,"cmd":"status"})",
                             R"({"proto":"1","cmd":"status"})", R"({"proto":1.5,"cmd":"status"})",
                             R"({"proto":1,"cmd":"unmount"})", R"({"proto":1,"cmd":""})",
                             R"({"proto":1,"cmd":"STATUS"})"}) {
        const json r = parse(ctl::dispatch(line, h));
        EXPECT_FALSE(r["ok"].get<bool>()) << line;
        EXPECT_EQ(r["error"]["code"], "unsupported") << line;
    }
    // Команда без обработчика недоступна.
    const ctl::Handlers none;
    EXPECT_EQ(parse(ctl::dispatch(R"({"proto":1,"cmd":"sync"})", none))["error"]["code"],
              "unsupported");
}

TEST(CtlProto, HandlerErrors) {
    ctl::Handlers h;
    h.status = []() -> json { throw ctl::Error(ctl::code::io, "disk on fire"); };
    h.sync = []() -> json { throw std::runtime_error("boom"); };
    h.snapshot = []() -> json { throw 42; };  // NOLINT(hicpp-exception-baseclass)
    json r = parse(ctl::dispatch(R"({"proto":1,"cmd":"status"})", h));
    EXPECT_EQ(r["error"]["code"], "io");
    EXPECT_EQ(r["error"]["message"], "disk on fire");
    r = parse(ctl::dispatch(R"({"proto":1,"cmd":"sync"})", h));
    EXPECT_EQ(r["error"]["code"], "internal");
    EXPECT_EQ(r["error"]["message"], "boom");
    r = parse(ctl::dispatch(R"({"proto":1,"cmd":"snapshot"})", h));
    EXPECT_EQ(r["error"]["code"], "internal");
}

TEST(CtlProto, InvalidUtf8InErrorDoesNotThrow) {
    ctl::Handlers h;
    h.status = []() -> json { throw ctl::Error(ctl::code::io, "bad name \xff\xfe"); };
    const std::string out = ctl::dispatch(R"({"proto":1,"cmd":"status"})", h);
    EXPECT_EQ(out.find('\n'), std::string::npos);
    EXPECT_FALSE(parse(out)["ok"].get<bool>());
}

TEST(CtlProto, PeerUid) {
    EXPECT_TRUE(peer_uid_allowed(1000, 1000));
    EXPECT_FALSE(peer_uid_allowed(0, 1000));
    EXPECT_FALSE(peer_uid_allowed(1001, 1000));
    EXPECT_EQ(CtlOptions{}.allowed_uid, ::geteuid());
}

// --- сервер ---

struct Gate {
    std::mutex mu;
    std::condition_variable cv;
    int entered = 0;
    bool open = false;

    void enter_and_wait() {
        std::unique_lock lock(mu);
        ++entered;
        cv.notify_all();
        cv.wait(lock, [&] { return open; });
    }
    bool wait_entered(int n, milliseconds timeout = seconds(10)) {
        std::unique_lock lock(mu);
        return cv.wait_for(lock, timeout, [&] { return entered >= n; });
    }
    void release() {
        const std::lock_guard lock(mu);
        open = true;
        cv.notify_all();
    }
};

// Обёртки без optional: в этих тестах сервер обязан быть живым и отвечать.
std::string ask(const fs::path& sock, const std::string& line, milliseconds timeout = seconds(20)) {
    auto r = ctl::exchange(sock, line, timeout);
    if (!r) {
        throw std::runtime_error("the control socket is not listening");
    }
    return *std::move(r);
}

ctl::Reply call_now(const fs::path& sock, const std::string& cmd) {
    auto r = ctl::call(sock, cmd, seconds(20));
    if (!r) {
        throw std::runtime_error("the control socket is not listening");
    }
    return *std::move(r);
}

TEST(CtlServer, RoundTripAndPermissions) {
    const TempDir dir;
    const fs::path sock = dir.path() / "ctl.sock";
    CtlServer server(sock, simple_handlers());
    server.start();

    struct stat st {};
    ASSERT_EQ(::lstat(sock.c_str(), &st), 0);
    EXPECT_TRUE(S_ISSOCK(st.st_mode));
    EXPECT_EQ(st.st_mode & 0777U, 0600U);

    const auto reply = call_now(sock, "status");
    EXPECT_TRUE(reply.ok);
    EXPECT_EQ(reply.result["generation"], 7);
    const auto sync = call_now(sock, "sync");
    EXPECT_EQ(sync.result["generation"], 8);
}

TEST(CtlServer, ProtocolErrorsOverSocket) {
    const TempDir dir;
    const fs::path sock = dir.path() / "ctl.sock";
    CtlServer server(sock, simple_handlers());
    server.start();

    EXPECT_EQ(parse(ask(sock, "{broken"))["error"]["code"], "bad_request");
    EXPECT_EQ(parse(ask(sock, R"({"proto":9,"cmd":"status"})"))["error"]["code"], "unsupported");
    EXPECT_EQ(parse(ask(sock, R"({"proto":1,"cmd":"reboot"})"))["error"]["code"], "unsupported");
    // Сервер пережил плохие запросы.
    EXPECT_TRUE(parse(ask(sock, R"({"proto":1,"cmd":"status"})"))["ok"]);
    // Слишком длинная строка.
    const std::string huge(ctl::kMaxLineBytes + 100, 'x');
    EXPECT_EQ(parse(ask(sock, huge))["error"]["code"], "bad_request");
}

TEST(CtlServer, RequestWithoutNewlineAtEof) {
    const TempDir dir;
    const fs::path sock = dir.path() / "ctl.sock";
    CtlServer server(sock, simple_handlers());
    server.start();
    const Fd fd = connect_unix(sock);
    const std::string req = R"({"proto":1,"cmd":"status"})";  // без '\n', как у `nc -N`
    ASSERT_EQ(::send(fd.get(), req.data(), req.size(), MSG_NOSIGNAL),
              static_cast<ssize_t>(req.size()));
    ::shutdown(fd.get(), SHUT_WR);
    std::string out(4096, '\0');
    const ssize_t n = ::recv(fd.get(), out.data(), out.size(), 0);
    ASSERT_GT(n, 0);
    out.resize(static_cast<size_t>(n));
    EXPECT_TRUE(parse(out)["ok"]);
}

TEST(CtlServer, ConcurrentClients) {
    const TempDir dir;
    const fs::path sock = dir.path() / "ctl.sock";
    Gate gate;
    ctl::Handlers h = simple_handlers();
    h.sync = [&] {
        gate.enter_and_wait();
        return json{{"generation", 1}};
    };
    CtlServer server(sock, h);
    server.start();

    constexpr int kClients = 4;
    std::vector<std::thread> threads;
    threads.reserve(kClients);
    std::atomic<int> ok{0};
    for (int i = 0; i < kClients; ++i) {
        threads.emplace_back([&] {
            const auto r = call_now(sock, "sync");
            if (r.ok) {
                ++ok;
            }
        });
    }
    // Все четыре `sync` внутри обработчика одновременно, а status при этом отвечает.
    ASSERT_TRUE(gate.wait_entered(kClients));
    const auto st = call_now(sock, "status");
    EXPECT_TRUE(st.ok);
    gate.release();
    for (auto& t : threads) {
        t.join();
    }
    EXPECT_EQ(ok.load(), kClients);
}

TEST(CtlServer, TooManyClientsGetBusy) {
    const TempDir dir;
    const fs::path sock = dir.path() / "ctl.sock";
    Gate gate;
    ctl::Handlers h = simple_handlers();
    h.sync = [&] {
        gate.enter_and_wait();
        return json::object();
    };
    CtlOptions opts;
    opts.max_clients = 2;
    CtlServer server(sock, h, opts);
    server.start();
    std::vector<std::thread> threads;
    threads.reserve(2);
    for (int i = 0; i < 2; ++i) {
        threads.emplace_back([&] { (void)ctl::call(sock, "sync", seconds(20)); });
    }
    ASSERT_TRUE(gate.wait_entered(2));
    const auto r = call_now(sock, "status");
    EXPECT_FALSE(r.ok);
    EXPECT_EQ(r.code, "busy");
    gate.release();
    for (auto& t : threads) {
        t.join();
    }
}

TEST(CtlServer, StaleSocketIsReplaced) {
    const TempDir dir;
    const fs::path sock = dir.path() / "ctl.sock";
    {
        // Сокет «упавшего процесса»: файл остался, слушателя нет.
        sockaddr_un addr{};
        addr.sun_family = AF_UNIX;
        std::strncpy(addr.sun_path, sock.c_str(), sizeof(addr.sun_path) - 1);
        const Fd fd{::socket(AF_UNIX, SOCK_STREAM, 0)};
        ASSERT_EQ(::bind(fd.get(), reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
    }
    ASSERT_TRUE(fs::exists(sock));
    // Клиент видит «не смонтировано», а не ошибку.
    EXPECT_FALSE(ctl::call(sock, "status", seconds(5)).has_value());

    CtlServer server(sock, simple_handlers());
    server.start();
    const auto r = call_now(sock, "status");
    EXPECT_TRUE(r.ok);
}

TEST(CtlServer, NonSocketFileIsNotTouched) {
    const TempDir dir;
    const fs::path sock = dir.path() / "ctl.sock";
    std::ofstream(sock) << "precious";
    CtlServer server(sock, simple_handlers());
    EXPECT_THROW(server.start(), SysError);
    std::ifstream in(sock);
    std::string text;
    in >> text;
    EXPECT_EQ(text, "precious");
}

TEST(CtlServer, ForeignUidIsDisconnected) {
    const TempDir dir;
    const fs::path sock = dir.path() / "ctl.sock";
    CtlOptions opts;
    opts.allowed_uid = ::geteuid() + 1;  // мы для сервера — «чужой» пользователь
    CtlServer server(sock, simple_handlers(), opts);
    server.start();
    // Соединение принято и закрыто без ответа.
    EXPECT_THROW((void)ctl::exchange(sock, R"({"proto":1,"cmd":"status"})", seconds(20)),
                 std::runtime_error);
}

TEST(CtlServer, StopRemovesSocketAndDoesNotWaitForIdleClients) {
    const TempDir dir;
    const fs::path sock = dir.path() / "ctl.sock";
    auto server = std::make_unique<CtlServer>(sock, simple_handlers());
    server->start();
    // Клиент подключился и молчит: не должен задерживать остановку.
    const Fd idle = connect_unix(sock);
    const auto t0 = std::chrono::steady_clock::now();
    server->begin_stop();
    EXPECT_FALSE(fs::exists(sock));
    server.reset();
    EXPECT_LT(std::chrono::steady_clock::now() - t0, seconds(5));
    EXPECT_FALSE(ctl::call(sock, "status", seconds(5)).has_value());
}

TEST(CtlServer, StopDuringRunningCommandAnswersNewRequestsAndJoinsAfterRelease) {
    const TempDir dir;
    const fs::path sock = dir.path() / "ctl.sock";
    Gate gate;
    ctl::Handlers h = simple_handlers();
    h.sync = [&] {
        gate.enter_and_wait();
        return json{{"generation", 5}};
    };
    auto server = std::make_unique<CtlServer>(sock, h);
    server->start();
    ctl::Reply sync_reply;
    std::thread client([&] { sync_reply = call_now(sock, "sync"); });
    ASSERT_TRUE(gate.wait_entered(1));

    // begin_stop возвращается сразу, пока sync висит в обработчике.
    const auto t0 = std::chrono::steady_clock::now();
    server->begin_stop();
    EXPECT_LT(std::chrono::steady_clock::now() - t0, seconds(5));
    EXPECT_FALSE(fs::exists(sock));

    // Деструктор ждёт обработчик; отпускаем его из другого потока.
    std::thread releaser([&] {
        std::this_thread::sleep_for(milliseconds(200));
        gate.release();
    });
    server.reset();
    releaser.join();
    client.join();
    EXPECT_TRUE(sync_reply.ok);
}

TEST(CtlServer, RequestAfterBeginStopGetsShuttingDown) {
    const TempDir dir;
    const fs::path sock = dir.path() / "ctl.sock";
    CtlServer server(sock, simple_handlers());
    server.start();
    // Клиент уже подключён (соединение принято), запрос шлёт после начала остановки.
    const Fd fd = connect_unix(sock);
    server.begin_stop();
    const std::string req = "{\"proto\":1,\"cmd\":\"status\"}\n";
    ::send(fd.get(), req.data(), req.size(), MSG_NOSIGNAL);
    std::string out(4096, '\0');
    const ssize_t n = ::recv(fd.get(), out.data(), out.size(), 0);
    // Либо ответ shutting_down, либо закрытие без ответа (соединение ещё не было принято).
    if (n > 0) {
        out.resize(static_cast<size_t>(n));
        EXPECT_EQ(parse(out)["error"]["code"], "shutting_down");
    }
}

TEST(CtlServer, LongSocketPath) {
    // Путь длиннее sun_path (108 байт): bind и connect идут через /proc/self/fd.
    const TempDir dir;
    fs::path deep = dir.path();
    while (deep.string().size() < 150) {
        deep /= "very-long-directory-name";
    }
    fs::create_directories(deep);
    const fs::path sock = deep / "ctl.sock";
    ASSERT_GT(sock.string().size(), sizeof(sockaddr_un::sun_path));
    CtlServer server(sock, simple_handlers());
    server.start();
    const auto r = call_now(sock, "status");
    EXPECT_TRUE(r.ok);
}

TEST(CtlClient, NotMountedWhenNoSocket) {
    const TempDir dir;
    EXPECT_FALSE(ctl::call(dir.path() / "ctl.sock", "status", seconds(5)).has_value());
}

// --- status по state.sqlite ---

TEST(Status, ReadsStateReadOnlyBesideOpenWriter) {
    const TempDir dir;
    const fs::path state_path = dir.path() / "state.sqlite";
    const fs::path staging = dir.path() / "staging";
    fs::create_directories(staging);
    meta::StateStore::create(state_path);
    meta::StateStore state(
        state_path);  // писатель остаётся открытым, как в смонтированном процессе
    state.set_int(meta::state_key::base_generation, 12);
    state.set_int(meta::state_key::head_msg_id, 345);
    state.set_int(meta::state_key::index_dirty, 1);
    state.set_int(meta::state_key::force_snapshot, 1);
    state.set_int(meta::state_key::clean_shutdown, 0);
    state.reset_chain_to_snapshot(100, 1000);
    state.append_chain(200, 50);
    state.append_chain(300, 60);
    std::ofstream(staging / "a") << std::string(1000, 'a');
    std::ofstream(staging / "b") << std::string(24, 'b');
    state.upsert_dirty_chunk(5, 0, "a", 1);
    state.upsert_dirty_chunk(5, 1, "b", 2);
    state.upsert_dirty_chunk(6, 0, "missing", 3);  // файла нет: в байты не входит
    state.upsert_open_pack(meta::OpenPackRow{5, 1, 1000, 10});
    const Bytes patch(3, std::byte{1});
    state.append_pending(patch);

    const FsStatus s = read_state_status(state_path, staging);
    EXPECT_FALSE(s.mounted);
    EXPECT_EQ(s.generation, 12U);
    EXPECT_EQ(s.head_msg_id, 345);
    EXPECT_EQ(s.chain_length, 3U);
    EXPECT_EQ(s.chain_bytes, 1110);
    EXPECT_TRUE(s.index_dirty);
    EXPECT_TRUE(s.force_snapshot);
    EXPECT_FALSE(s.clean_shutdown);
    EXPECT_EQ(s.pending_patchsets, 1U);
    EXPECT_EQ(s.dirty_chunks, 3U);
    EXPECT_EQ(s.dirty_inodes, 2U);
    EXPECT_EQ(s.dirty_bytes, 1024U);
    EXPECT_EQ(s.open_pack_objects, 1U);
    EXPECT_EQ(s.open_pack_bytes, 1000);
    EXPECT_EQ(s.upload_queued, 3U);

    // JSON туда и обратно и человекочитаемый вывод.
    const FsStatus back = status_from_json(status_to_json(s));
    EXPECT_EQ(back.generation, s.generation);
    EXPECT_EQ(back.dirty_bytes, s.dirty_bytes);
    EXPECT_EQ(back.chain_length, s.chain_length);
    const std::string text = format_status(s, "main");
    EXPECT_NE(text.find("not mounted"), std::string::npos);
    EXPECT_NE(text.find("12 (head message 345)"), std::string::npos);
    EXPECT_NE(text.find("1.0 KiB"), std::string::npos);
    EXPECT_NE(text.find("ended uncleanly"), std::string::npos);

    // Читатель ничего не меняет: писатель по-прежнему работает.
    state.set_int(meta::state_key::base_generation, 13);
    EXPECT_EQ(read_state_status(state_path, staging).generation, 13U);
}

TEST(Status, MissingStateFileIsAnError) {
    const TempDir dir;
    EXPECT_THROW((void)read_state_status(dir.path() / "state.sqlite", dir.path()),
                 std::runtime_error);
}

TEST(Status, LiveFieldsAndFormat) {
    FsStatus s;
    vfs::VfsStatus v;
    v.queued = 3;
    v.uploading = 2;
    v.failed = 1;
    v.staging_used = 5U << 20U;
    v.staging_limit = 8ULL << 30U;
    v.last_error = "network down";
    meta::CommitStats c;
    c.commits = 4;
    c.last_error = "pin failed";
    add_live_status(s, v, c, /*read_only=*/true);
    EXPECT_TRUE(s.mounted);
    EXPECT_TRUE(s.read_only);
    ASSERT_EQ(s.errors.size(), 2U);
    const std::string text = format_status(s, "p");
    EXPECT_NE(text.find("mounted, read-only"), std::string::npos);
    EXPECT_NE(text.find("3 queued, 2 uploading, 1 failed"), std::string::npos);
    EXPECT_NE(text.find("network down"), std::string::npos);
    EXPECT_NE(text.find("pin failed"), std::string::npos);
    EXPECT_EQ(format_bytes(0), "0 B");
    EXPECT_EQ(format_bytes(1536), "1.5 KiB");
}

}  // namespace
}  // namespace telega::app
