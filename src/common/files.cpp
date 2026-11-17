#include "common/files.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <system_error>
#include <utility>

namespace telega {

namespace {

std::string describe(int err, const std::string& what) {
    return what + ": " + std::strerror(err);  // NOLINT(concurrency-mt-unsafe)
}

}  // namespace

SysError::SysError(int err, const std::string& what)
    : std::runtime_error(describe(err, what)), err_(err) {}

Fd& Fd::operator=(Fd&& other) noexcept {
    if (this != &other) {
        if (fd_ >= 0) {
            ::close(fd_);
        }
        fd_ = other.release();
    }
    return *this;
}

Fd::~Fd() {
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

int Fd::release() noexcept { return std::exchange(fd_, -1); }

Fd Fd::open(const std::filesystem::path& path, int flags, mode_t mode) {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
    const int fd = ::open(path.c_str(), flags | O_CLOEXEC, mode);
    if (fd < 0) {
        throw SysError(errno, "open " + path.string());
    }
    return Fd{fd};
}

size_t pread_full(int fd, MutableByteSpan out, uint64_t offset) {
    size_t done = 0;
    while (done < out.size()) {
        const ssize_t n =
            ::pread(fd, out.data() + done, out.size() - done, static_cast<off_t>(offset + done));
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            throw SysError(errno, "pread");
        }
        if (n == 0) {
            break;
        }
        done += static_cast<size_t>(n);
    }
    return done;
}

void pwrite_full(int fd, ByteSpan data, uint64_t offset) {
    size_t done = 0;
    while (done < data.size()) {
        const ssize_t n =
            ::pwrite(fd, data.data() + done, data.size() - done, static_cast<off_t>(offset + done));
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            throw SysError(errno, "pwrite");
        }
        done += static_cast<size_t>(n);
    }
}

Bytes read_file(const std::filesystem::path& path) {
    const Fd fd = Fd::open(path, O_RDONLY);
    Bytes data;
    const auto size = std::filesystem::file_size(path);
    data.resize(size);
    const size_t n = pread_full(fd.get(), data, 0);
    data.resize(n);
    return data;
}

void fsync_dir(const std::filesystem::path& dir) {
    const Fd fd = Fd::open(dir, O_RDONLY | O_DIRECTORY);
    if (::fsync(fd.get()) != 0) {
        throw SysError(errno, "fsync " + dir.string());
    }
}

void write_file_atomic(const std::filesystem::path& path, ByteSpan data, mode_t mode) {
    auto tmp = path;
    tmp += ".tmp";
    {
        const Fd fd = Fd::open(tmp, O_WRONLY | O_CREAT | O_TRUNC, mode);
        pwrite_full(fd.get(), data, 0);
        if (::fsync(fd.get()) != 0) {
            throw SysError(errno, "fsync " + tmp.string());
        }
    }
    std::error_code ec;
    std::filesystem::rename(tmp, path, ec);
    if (ec) {
        throw SysError(ec.value(), "rename " + tmp.string());
    }
    fsync_dir(path.has_parent_path() ? path.parent_path() : std::filesystem::path{"."});
}

}  // namespace telega
