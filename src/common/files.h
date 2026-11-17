#pragma once

// Файловые операции с явной долговечностью: всё, что должно пережить сбой (SPEC 7.3),
// пишется через эти функции.

#include <sys/types.h>

#include <filesystem>
#include <stdexcept>
#include <string>

#include "common/bytes.h"

namespace telega {

// Ошибка системного вызова с errno и контекстом (путь, операция). Секреты в текст не попадают.
class SysError : public std::runtime_error {
  public:
    SysError(int err, const std::string& what);
    [[nodiscard]] int err() const noexcept { return err_; }

  private:
    int err_;
};

// Прочитать файл целиком.
Bytes read_file(const std::filesystem::path& path);

// Записать файл атомарно: tmp рядом, fsync файла, rename, fsync каталога.
void write_file_atomic(const std::filesystem::path& path, ByteSpan data, mode_t mode = 0600);

// fsync каталога, чтобы пережили сбой созданные и переименованные в нём записи.
void fsync_dir(const std::filesystem::path& dir);

// RAII-дескриптор.
class Fd {
  public:
    Fd() = default;
    explicit Fd(int fd) noexcept : fd_(fd) {}
    Fd(const Fd&) = delete;
    Fd& operator=(const Fd&) = delete;
    Fd(Fd&& other) noexcept : fd_(other.release()) {}
    Fd& operator=(Fd&& other) noexcept;
    ~Fd();

    [[nodiscard]] int get() const noexcept { return fd_; }
    explicit operator bool() const noexcept { return fd_ >= 0; }
    int release() noexcept;

    // open(2) с O_CLOEXEC; при ошибке SysError.
    static Fd open(const std::filesystem::path& path, int flags, mode_t mode = 0600);

  private:
    int fd_ = -1;
};

// pread/pwrite целиком (повторяются до конца диапазона, EINTR обрабатывается).
// pread_full возвращает число прочитанных байт: меньше запрошенного только на EOF.
size_t pread_full(int fd, MutableByteSpan out, uint64_t offset);
void pwrite_full(int fd, ByteSpan data, uint64_t offset);

}  // namespace telega
