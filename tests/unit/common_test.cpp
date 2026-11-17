// common/: little-endian кодирование, hex, атомарная запись файла.

#include <fcntl.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <filesystem>
#include <string>

#include "common/bytes.h"
#include "common/files.h"

namespace telega {
namespace {

TEST(Bytes, LittleEndianRoundTrip) {
    Bytes buf;
    append_u64_le(buf, 0x0102030405060708ULL);
    ASSERT_EQ(buf.size(), 8U);
    EXPECT_EQ(buf[0], std::byte{0x08});
    EXPECT_EQ(buf[7], std::byte{0x01});
    EXPECT_EQ(get_u64_le(buf), 0x0102030405060708ULL);

    std::array<std::byte, 4> small{};
    put_u32_le(small, 0xA1B2C3D4U);
    EXPECT_EQ(get_u32_le(small), 0xA1B2C3D4U);
}

TEST(Bytes, Hex) {
    const std::array<std::byte, 3> raw{std::byte{0x00}, std::byte{0xAB}, std::byte{0x7F}};
    EXPECT_EQ(to_hex(raw), "00ab7f");
}

TEST(Files, AtomicWriteAndRead) {
    const auto dir =
        std::filesystem::temp_directory_path() / ("telega-common-" + std::to_string(::getpid()));
    std::filesystem::create_directories(dir);
    const auto path = dir / "file";
    const std::string text = "hello";
    write_file_atomic(path, as_bytes(text));
    write_file_atomic(path, as_bytes(text + " world"));
    const Bytes back = read_file(path);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(back.data()), back.size()), "hello world");
    EXPECT_EQ(std::filesystem::status(path).permissions() & std::filesystem::perms::all,
              std::filesystem::perms::owner_read | std::filesystem::perms::owner_write);
    EXPECT_FALSE(std::filesystem::exists(dir / "file.tmp"));
    std::filesystem::remove_all(dir);
}

TEST(Files, OpenMissingThrowsSysError) {
    try {
        (void)Fd::open("/nonexistent/telega/x", O_RDONLY);
        FAIL();
    } catch (const SysError& e) {
        EXPECT_EQ(e.err(), ENOENT);
    }
}

}  // namespace
}  // namespace telega
