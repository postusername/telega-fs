#include "app/ctl_client.h"

#include <poll.h>
#include <sys/socket.h>

#include <array>
#include <cerrno>
#include <stdexcept>

#include "app/ctl_proto.h"
#include "app/ctl_socket.h"
#include "common/files.h"

namespace telega::app::ctl {

namespace fs = std::filesystem;

std::optional<std::string> exchange(const fs::path& sock, std::string_view request,
                                    std::chrono::milliseconds timeout) {
    Fd fd;
    try {
        fd = connect_unix(sock);
    } catch (const SysError& e) {
        if (e.err() == ENOENT || e.err() == ECONNREFUSED) {
            return std::nullopt;
        }
        throw;
    }
    std::string out{request};
    out += '\n';
    const char* p = out.data();
    size_t left = out.size();
    while (left > 0) {
        const ssize_t n = ::send(fd.get(), p, left, MSG_NOSIGNAL);
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n <= 0) {
            throw SysError(errno, "send to the control socket");
        }
        p += n;
        left -= static_cast<size_t>(n);
    }

    const auto deadline = std::chrono::steady_clock::now() + timeout;
    std::string reply;
    std::array<char, 4096> buf{};
    for (;;) {
        int wait_ms = -1;
        if (timeout.count() > 0) {
            const auto left_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                deadline - std::chrono::steady_clock::now());
            if (left_ms.count() <= 0) {
                throw std::runtime_error("the control socket did not answer in time");
            }
            wait_ms = static_cast<int>(left_ms.count());
        }
        pollfd pf{fd.get(), POLLIN, 0};
        const int rc = ::poll(&pf, 1, wait_ms);
        if (rc < 0 && errno == EINTR) {
            continue;
        }
        if (rc < 0) {
            throw SysError(errno, "poll the control socket");
        }
        if (rc == 0) {
            continue;
        }
        const ssize_t n = ::recv(fd.get(), buf.data(), buf.size(), 0);
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n < 0) {
            throw SysError(errno, "recv from the control socket");
        }
        if (n == 0) {
            break;
        }
        reply.append(buf.data(), static_cast<size_t>(n));
        const auto nl = reply.find('\n');
        if (nl != std::string::npos) {
            reply.resize(nl);
            return reply;
        }
    }
    if (reply.empty()) {
        throw std::runtime_error(
            "the control socket closed the connection without a reply (is the file system "
            "unmounting, or owned by another user?)");
    }
    return reply;
}

std::optional<Reply> call(const fs::path& sock, std::string_view cmd,
                          std::chrono::milliseconds timeout, const nlohmann::json& args) {
    const nlohmann::json req{{"proto", kProtoVersion}, {"cmd", std::string(cmd)}, {"args", args}};
    const auto line = exchange(sock, req.dump(), timeout);
    if (!line) {
        return std::nullopt;
    }
    const auto j = nlohmann::json::parse(*line, nullptr, /*allow_exceptions=*/false);
    if (!j.is_object() || !j.contains("ok") || !j["ok"].is_boolean()) {
        throw std::runtime_error("the control socket sent a malformed reply");
    }
    Reply r;
    r.ok = j["ok"].get<bool>();
    if (r.ok) {
        r.result = j.value("result", nlohmann::json::object());
    } else if (j.contains("error") && j["error"].is_object()) {
        r.code = j["error"].value("code", "");
        r.message = j["error"].value("message", "");
    }
    return r;
}

}  // namespace telega::app::ctl
