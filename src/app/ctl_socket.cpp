#include "app/ctl_socket.h"

#include <fcntl.h>
#include <poll.h>
#include <spdlog/spdlog.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstring>
#include <string>
#include <utility>

namespace telega::app {

namespace fs = std::filesystem;

namespace {

constexpr std::chrono::milliseconds kPollSlice{100};

// Адрес сокета. Путь длиннее sun_path (108 байт) задаётся через /proc/self/fd/<каталог>/<имя>:
// ядро разрешает путь при bind и connect, а дескриптор каталога живёт в этой структуре.
struct SockAddr {
    sockaddr_un addr{};
    socklen_t len = 0;
    Fd dir;
};

SockAddr make_address(const fs::path& path) {
    SockAddr out;
    out.addr.sun_family = AF_UNIX;
    std::string text = path.string();
    if (text.size() >= sizeof(out.addr.sun_path)) {
        out.dir = Fd::open(path.parent_path(), O_PATH | O_DIRECTORY);
        text = "/proc/self/fd/" + std::to_string(out.dir.get()) + "/" + path.filename().string();
        if (text.size() >= sizeof(out.addr.sun_path)) {
            throw SysError(ENAMETOOLONG, "socket path " + path.string());
        }
    }
    std::memcpy(out.addr.sun_path, text.c_str(), text.size() + 1);
    out.len = static_cast<socklen_t>(offsetof(sockaddr_un, sun_path) + text.size() + 1);
    return out;
}

// Прочитать строку запроса до '\n'. Конец ввода без перевода строки тоже завершает запрос,
// если что-то прочитано (удобно для `nc`). Возвращает false при таймауте, остановке без
// запроса, закрытии без данных или ошибке; too_long = true, если строка длиннее kMaxLineBytes.
bool read_request(int fd, std::chrono::milliseconds timeout, const std::atomic<bool>& stopping,
                  std::string& line, bool& too_long) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    std::array<char, 4096> buf{};
    for (;;) {
        pollfd p{fd, POLLIN, 0};
        const int rc = ::poll(&p, 1, static_cast<int>(kPollSlice.count()));
        if (rc < 0 && errno != EINTR) {
            return false;
        }
        if (rc <= 0) {
            // Тишина: при остановке или по таймауту клиент отпускается. Уже присланный запрос
            // читается и при остановке (на него ответит shutting_down).
            if (stopping.load() || std::chrono::steady_clock::now() >= deadline) {
                return false;
            }
            continue;
        }
        const ssize_t n = ::recv(fd, buf.data(), buf.size(), 0);
        if (n < 0 && (errno == EINTR || errno == EAGAIN)) {
            continue;
        }
        if (n < 0) {
            return false;
        }
        if (n == 0) {
            return !line.empty();
        }
        line.append(buf.data(), static_cast<size_t>(n));
        const auto nl = line.find('\n');
        if (nl != std::string::npos) {
            line.resize(nl);
            return true;
        }
        if (line.size() > ctl::kMaxLineBytes) {
            too_long = true;
            return true;
        }
    }
}

void send_line(int fd, std::string text) {
    text += '\n';
    const char* p = text.data();
    size_t left = text.size();
    while (left > 0) {
        const ssize_t n = ::send(fd, p, left, MSG_NOSIGNAL);
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n <= 0) {
            return;  // клиент ушёл: отвечать некому
        }
        p += n;
        left -= static_cast<size_t>(n);
    }
}

}  // namespace

bool peer_uid_allowed(uid_t peer, uid_t allowed) noexcept { return peer == allowed; }

CtlOptions::CtlOptions() : allowed_uid(::geteuid()) {}

Fd connect_unix(const fs::path& path) {
    const SockAddr sa = make_address(path);
    Fd fd{::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0)};
    if (!fd) {
        throw SysError(errno, "socket");
    }
    if (::connect(fd.get(), reinterpret_cast<const sockaddr*>(&sa.addr), sa.len) != 0) {
        throw SysError(errno, "connect " + path.string());
    }
    return fd;
}

CtlServer::CtlServer(fs::path path, ctl::Handlers handlers, CtlOptions options)
    : path_(std::move(path)), handlers_(std::move(handlers)), opts_(options) {}

CtlServer::~CtlServer() {
    begin_stop();
    std::list<std::unique_ptr<Client>> clients;
    {
        const std::lock_guard lock(mu_);
        clients = std::move(clients_);
    }
    for (auto& c : clients) {
        if (c->thread.joinable()) {
            c->thread.join();
        }
    }
}

void CtlServer::start() {
    // Оставшийся от упавшего процесса сокет: lock уже у нас, значит, его никто не слушает.
    // Удаляем только сокет: обычный файл с таким именем — не наш, его не трогаем.
    struct stat st {};
    if (::lstat(path_.c_str(), &st) == 0) {
        if (!S_ISSOCK(st.st_mode)) {
            throw SysError(EEXIST, path_.string() + " exists and is not a socket");
        }
        if (::unlink(path_.c_str()) != 0 && errno != ENOENT) {
            throw SysError(errno, "unlink " + path_.string());
        }
        spdlog::info("removed the stale control socket {}", path_.string());
    }
    const SockAddr sa = make_address(path_);
    Fd fd{::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0)};
    if (!fd) {
        throw SysError(errno, "socket");
    }
    if (::bind(fd.get(), reinterpret_cast<const sockaddr*>(&sa.addr), sa.len) != 0) {
        throw SysError(errno, "bind " + path_.string());
    }
    // Каталог data имеет права 0700, поэтому окно между bind и chmod недоступно другим
    // пользователям; права сокета при этом всё равно 0600.
    if (::chmod(path_.c_str(), 0600) != 0) {
        const int err = errno;
        ::unlink(path_.c_str());
        throw SysError(err, "chmod " + path_.string());
    }
    if (::listen(fd.get(), 16) != 0) {
        const int err = errno;
        ::unlink(path_.c_str());
        throw SysError(err, "listen " + path_.string());
    }
    std::array<int, 2> wake{};
    if (::pipe2(wake.data(), O_CLOEXEC | O_NONBLOCK) != 0) {
        const int err = errno;
        ::unlink(path_.c_str());
        throw SysError(err, "pipe2");
    }
    wake_read_ = Fd{wake[0]};
    wake_write_ = Fd{wake[1]};
    listen_fd_ = std::move(fd);
    bound_ = true;
    accept_thread_ = std::thread([this] { accept_loop(); });
    spdlog::info("control socket {}", path_.string());
}

void CtlServer::begin_stop() noexcept {
    std::call_once(stop_once_, [this] {
        stopping_.store(true);
        if (wake_write_) {
            const char b = 0;
            [[maybe_unused]] const ssize_t n = ::write(wake_write_.get(), &b, 1);
        }
        if (accept_thread_.joinable()) {
            accept_thread_.join();  // закрывает слушающий сокет
        }
        if (bound_) {
            std::error_code ec;
            fs::remove(path_, ec);
        }
    });
}

void CtlServer::reap_finished() {
    std::list<std::unique_ptr<Client>> finished;
    {
        const std::lock_guard lock(mu_);
        for (auto it = clients_.begin(); it != clients_.end();) {
            if ((*it)->done.load()) {
                finished.push_back(std::move(*it));
                it = clients_.erase(it);
            } else {
                ++it;
            }
        }
    }
    for (auto& c : finished) {
        c->thread.join();
    }
}

void CtlServer::accept_loop() {
    while (!stopping()) {
        std::array<pollfd, 2> fds{{{listen_fd_.get(), POLLIN, 0}, {wake_read_.get(), POLLIN, 0}}};
        const int rc = ::poll(fds.data(), fds.size(), -1);
        if (rc < 0 && errno != EINTR) {
            spdlog::error("control socket: poll: {}", std::generic_category().message(errno));
            break;
        }
        if (rc <= 0 || stopping() || (fds[1].revents & POLLIN) != 0) {
            continue;
        }
        if ((fds[0].revents & POLLIN) == 0) {
            continue;
        }
        Fd client{::accept4(listen_fd_.get(), nullptr, nullptr, SOCK_CLOEXEC)};
        if (!client) {
            if (errno != EINTR && errno != EAGAIN && errno != ECONNABORTED) {
                spdlog::warn("control socket: accept: {}", std::generic_category().message(errno));
            }
            continue;
        }
        ucred cred{};
        socklen_t len = sizeof(cred);
        if (::getsockopt(client.get(), SOL_SOCKET, SO_PEERCRED, &cred, &len) != 0 ||
            !peer_uid_allowed(cred.uid, opts_.allowed_uid)) {
            spdlog::warn("control socket: connection from uid {} rejected", cred.uid);
            continue;  // Fd закроет соединение без ответа
        }
        reap_finished();
        bool too_many = false;
        {
            const std::lock_guard lock(mu_);
            too_many = clients_.size() >= opts_.max_clients;
        }
        if (too_many) {
            send_line(client.get(), ctl::make_error(ctl::code::busy, "too many clients"));
            continue;
        }
        auto entry = std::make_unique<Client>();
        Client* raw = entry.get();
        {
            // Поток стартует под mu_: запись в clients_ и поток видны reap_finished согласованно.
            const std::lock_guard lock(mu_);
            raw->thread = std::thread([this, raw, fd = std::move(client)]() mutable {
                serve_client(std::move(fd));
                raw->done.store(true);
            });
            clients_.push_back(std::move(entry));
        }
    }
    listen_fd_ = Fd{};  // закрыть: новые подключения получают ECONNREFUSED
}

void CtlServer::serve_client(Fd fd) {
    std::string line;
    bool too_long = false;
    if (!read_request(fd.get(), opts_.request_timeout, stopping_, line, too_long)) {
        return;
    }
    if (too_long) {
        send_line(fd.get(), ctl::make_error(ctl::code::bad_request, "the request is too long"));
        return;
    }
    if (stopping()) {
        send_line(fd.get(),
                  ctl::make_error(ctl::code::shutting_down, "the file system is unmounting"));
        return;
    }
    send_line(fd.get(), ctl::dispatch(line, handlers_));
}

}  // namespace telega::app
