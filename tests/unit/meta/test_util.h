#pragma once

// Общие помощники тестов модуля meta.

#include <gtest/gtest.h>
#include <stdlib.h>  // NOLINT(modernize-deprecated-headers): mkdtemp есть только в POSIX-заголовке

#include <cerrno>
#include <filesystem>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "common/bytes.h"
#include "meta/index_db.h"
#include "meta/index_ops.h"

namespace telega::meta::test {

// Временный каталог, удаляется вместе с содержимым.
class TempDir {
  public:
    TempDir() {
        std::string tmpl = (std::filesystem::temp_directory_path() / "telega-meta-XXXXXX").string();
        if (::mkdtemp(tmpl.data()) == nullptr) {
            throw std::system_error(errno, std::generic_category(), "mkdtemp");
        }
        path_ = tmpl;
    }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;
    TempDir(TempDir&&) = delete;
    TempDir& operator=(TempDir&&) = delete;
    ~TempDir() {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }
    [[nodiscard]] const std::filesystem::path& path() const { return path_; }
    [[nodiscard]] std::filesystem::path operator/(std::string_view name) const {
        return path_ / name;
    }

  private:
    std::filesystem::path path_;
};

// Значение optional или исключение (тест падает с понятной ошибкой, без UB).
template <typename T>
T must(std::optional<T> value) {
    if (!value) {
        throw std::logic_error("unexpected empty optional");
    }
    return std::move(*value);
}

inline Bytes bytes_of(std::string_view s) {
    Bytes out(s.size());
    for (size_t i = 0; i < s.size(); ++i) {
        out[i] = static_cast<std::byte>(s[i]);
    }
    return out;
}

inline InitParams test_params() {
    InitParams p;
    for (size_t i = 0; i < p.fs_uuid.size(); ++i) {
        p.fs_uuid[i] = static_cast<std::byte>(0xA0 + i);
    }
    p.chunk_size = 1 << 16;
    p.inline_max = 64;
    p.inline_budget = 256;
    p.pack_threshold = 1 << 12;
    p.pack_target = 1 << 16;
    p.root_uid = 1000;
    p.root_gid = 1000;
    p.root_mode = 0755;
    p.now_ns = 1'000'000'000;
    return p;
}

inline Bytes mac_of(int64_t seed) {
    Bytes mac(32);
    for (size_t i = 0; i < mac.size(); ++i) {
        mac[i] = static_cast<std::byte>((static_cast<uint64_t>(seed) >> ((i % 8) * 8)) + i);
    }
    return mac;
}

// Выделить номер и вставить позиционный чанк размера plain_size.
inline int64_t new_chunk(Db& db, int64_t plain_size, int64_t base_gen,
                         ChunkKind kind = ChunkKind::positional) {
    const int64_t id = alloc_chunk_id(db);
    NewChunk c;
    c.chunk_id = id;
    c.kind = kind;
    c.msg_id = 1000 + id;
    c.remote_file_id = "remote-" + std::to_string(id);
    c.plain_size = plain_size;
    c.enc_size = plain_size + 40;
    c.content_mac = mac_of(id);
    insert_chunk(db, c, base_gen);
    return id;
}

inline NewInode new_node(int64_t parent, std::string_view name, InodeKind kind = InodeKind::file) {
    NewInode n;
    n.parent = parent;
    n.name = bytes_of(name);
    n.kind = kind;
    n.mode = kind == InodeKind::dir ? 0755 : 0644;
    n.uid = 1000;
    n.gid = 1000;
    n.now_ns = 2'000'000'000;
    if (kind == InodeKind::symlink) {
        n.target = bytes_of("target");
    }
    return n;
}

// Сверка учёта чанков с экстентами: refcount, live_bytes и dead_since_gen.
inline ::testing::AssertionResult chunk_accounting_consistent(Db& db) {
    Stmt st{db,
            "SELECT c.chunk_id, c.refcount, c.live_bytes, c.dead_since_gen, "
            "  (SELECT COUNT(*) FROM extents e WHERE e.chunk_id = c.chunk_id), "
            "  (SELECT COALESCE(SUM(len), 0) FROM extents e WHERE e.chunk_id = c.chunk_id) "
            "FROM chunks c"};
    while (st.step()) {
        const int64_t id = st.column_int64(0);
        if (st.column_int64(1) != st.column_int64(4) || st.column_int64(2) != st.column_int64(5)) {
            return ::testing::AssertionFailure()
                   << "chunk " << id << ": refcount " << st.column_int64(1) << " vs "
                   << st.column_int64(4) << ", live_bytes " << st.column_int64(2) << " vs "
                   << st.column_int64(5);
        }
        if ((st.column_int64(1) == 0) != !st.is_null(3)) {
            return ::testing::AssertionFailure()
                   << "chunk " << id << ": dead_since_gen does not match refcount "
                   << st.column_int64(1);
        }
    }
    return ::testing::AssertionSuccess();
}

}  // namespace telega::meta::test
