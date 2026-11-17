#pragma once

// Исключения модуля meta.

#include <stdexcept>
#include <string>

namespace telega::meta {

// Ошибка операции ФС с errno (EINVAL, ENAMETOOLONG, EEXIST, ENOENT, ENOTDIR, ENOTEMPTY).
// Слой Vfs превращает её в ответ FUSE.
class FsError : public std::runtime_error {
  public:
    FsError(int err, const std::string& what);
    [[nodiscard]] int err() const noexcept { return err_; }

  private:
    int err_;
};

// Индекс или дельта-журнал испорчены: конфликт при применении патча, проваленная
// foreign_key_check или integrity_check, нарушенный инвариант схемы (SPEC 6.6, 7.2).
class IndexCorrupt : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

}  // namespace telega::meta
