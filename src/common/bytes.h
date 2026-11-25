#pragma once

// Байтовые буферы и little-endian кодирование для форматов на диске и в канале.

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <vector>

namespace telega {

using Bytes = std::vector<std::byte>;
using ByteSpan = std::span<const std::byte>;
using MutableByteSpan = std::span<std::byte>;

// Идентификатор ФС (meta.fs_uuid).
using FsUuid = std::array<std::byte, 16>;

inline void put_u64_le(MutableByteSpan out, uint64_t v) {
    for (size_t i = 0; i < 8; ++i) {
        out[i] = static_cast<std::byte>((v >> (8 * i)) & 0xFFU);
    }
}

inline uint64_t get_u64_le(ByteSpan in) {
    uint64_t v = 0;
    for (size_t i = 0; i < 8; ++i) {
        v |= static_cast<uint64_t>(in[i]) << (8 * i);
    }
    return v;
}

inline void put_u32_le(MutableByteSpan out, uint32_t v) {
    for (size_t i = 0; i < 4; ++i) {
        out[i] = static_cast<std::byte>((v >> (8 * i)) & 0xFFU);
    }
}

inline uint32_t get_u32_le(ByteSpan in) {
    uint32_t v = 0;
    for (size_t i = 0; i < 4; ++i) {
        v |= static_cast<uint32_t>(in[i]) << (8 * i);
    }
    return v;
}

inline void append_u64_le(Bytes& out, uint64_t v) {
    const size_t at = out.size();
    out.resize(at + 8);
    put_u64_le(MutableByteSpan{out}.subspan(at, 8), v);
}

inline ByteSpan as_bytes(const std::string& s) { return std::as_bytes(std::span{s}); }

std::string to_hex(ByteSpan data);

}  // namespace telega
