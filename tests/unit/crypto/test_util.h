#pragma once

// Общие помощники тестов crypto: инициализация libsodium, временный каталог, случайные байты.

#include <gtest/gtest.h>
#include <sodium.h>
#include <stdlib.h>

#include <cerrno>
#include <cstddef>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <system_error>

#include "common/bytes.h"
#include "crypto/keys.h"

namespace telega::crypto::test {

// Уникальный временный каталог; удаляется вместе с содержимым в деструкторе.
class TempDir {
  public:
    TempDir() {
        std::string tmpl =
            (std::filesystem::temp_directory_path() / "telega-crypto-XXXXXX").string();
        if (::mkdtemp(tmpl.data()) == nullptr) {
            throw std::system_error(errno, std::generic_category(), "mkdtemp");
        }
        path_ = tmpl;
    }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;
    ~TempDir() {
        std::error_code ec;
        std::filesystem::remove_all(path_, ec);
    }

    [[nodiscard]] const std::filesystem::path& path() const noexcept { return path_; }
    std::filesystem::path operator/(const std::string& name) const { return path_ / name; }

  private:
    std::filesystem::path path_;
};

// Фикстура: sodium_init до тестов набора и свой временный каталог на каждый тест.
class CryptoTest : public ::testing::Test {
  public:
    static void SetUpTestSuite() { init(); }

  protected:
    TempDir tmp;
};

inline Bytes random_bytes(size_t n) {
    Bytes out(n);
    if (n != 0) {  // у пустого вектора data() == nullptr, а randombytes_buf его не принимает
        randombytes_buf(out.data(), out.size());
    }
    return out;
}

inline Bytes to_bytes(ByteSpan s) { return {s.begin(), s.end()}; }

inline void flip(Bytes& data, size_t pos) { data.at(pos) ^= std::byte{0x01}; }

}  // namespace telega::crypto::test
