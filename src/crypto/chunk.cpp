#include "crypto/chunk.h"

#include <fcntl.h>
#include <sodium.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <stdexcept>
#include <string>
#include <system_error>

#include "common/files.h"
#include "crypto/detail.h"

namespace telega::crypto {

using detail::uc;

namespace {

// Раскладка заголовка чанка (SPEC 5).
constexpr std::array<char, 8> kMagic{'T', 'G', 'F', 'S', 'C', 'H', 'K', '1'};
constexpr uint8_t kVersion = 1;
constexpr size_t kOffVersion = 8;
constexpr size_t kOffReserved = 9;
constexpr size_t kReservedSize = 7;
constexpr size_t kOffNonce = 16;
constexpr size_t kOffPlainSize = 32;

// nonce сегмента: chunk_nonce(16) || u64_le(i).
using SegmentNonce = std::array<std::byte, crypto_aead_xchacha20poly1305_ietf_NPUBBYTES>;
// AD сегмента: fs_uuid(16) || u64(chunk_id) || u64(i) || u8(is_last) || u64(plain_size).
using SegmentAd = std::array<std::byte, 16 + 8 + 8 + 1 + 8>;

static_assert(std::tuple_size_v<SegmentNonce> == 16 + 8);
static_assert(kSegmentTagSize == crypto_aead_xchacha20poly1305_ietf_ABYTES);
static_assert(Key::kSize == crypto_aead_xchacha20poly1305_ietf_KEYBYTES);

SegmentNonce segment_nonce(const ChunkHeader& header, uint64_t i) {
    SegmentNonce nonce{};
    std::copy(header.nonce.begin(), header.nonce.end(), nonce.begin());
    put_u64_le(MutableByteSpan{nonce}.subspan(16, 8), i);
    return nonce;
}

SegmentAd segment_ad(const FsUuid& fs_uuid, uint64_t chunk_id, uint64_t i, bool is_last,
                     uint64_t plain_size) {
    SegmentAd ad{};
    const MutableByteSpan out{ad};
    std::copy(fs_uuid.begin(), fs_uuid.end(), ad.begin());
    put_u64_le(out.subspan(16, 8), chunk_id);
    put_u64_le(out.subspan(24, 8), i);
    ad[32] = is_last ? std::byte{1} : std::byte{0};
    put_u64_le(out.subspan(33, 8), plain_size);
    return ad;
}

// Длина открытого текста сегмента i (i < числа сегментов).
uint64_t segment_plain_len(uint64_t plain_size, uint64_t i) {
    return std::min(kSegmentSize, plain_size - i * kSegmentSize);
}

std::array<std::byte, kChunkHeaderSize> serialize_chunk_header(const ChunkHeader& header) {
    std::array<std::byte, kChunkHeaderSize> raw{};
    const MutableByteSpan out{raw};
    std::transform(kMagic.begin(), kMagic.end(), raw.begin(),
                   [](char c) { return static_cast<std::byte>(c); });
    raw[kOffVersion] = std::byte{kVersion};
    std::copy(header.nonce.begin(), header.nonce.end(), raw.begin() + kOffNonce);
    put_u64_le(out.subspan(kOffPlainSize, 8), header.plain_size);
    return raw;
}

}  // namespace

uint64_t chunk_segment_count(uint64_t plain_size) {
    return plain_size / kSegmentSize + (plain_size % kSegmentSize != 0 ? 1 : 0);
}

uint64_t chunk_encrypted_size(uint64_t plain_size) {
    return kChunkHeaderSize + plain_size + kSegmentTagSize * chunk_segment_count(plain_size);
}

SegmentSpan chunk_segments_for(uint64_t plain_size, uint64_t offset, uint64_t len) {
    SegmentSpan span;
    if (len == 0 || offset >= plain_size) {
        return span;
    }
    // Обрезка по plain_size без переполнения offset + len.
    const uint64_t end = len > plain_size - offset ? plain_size : offset + len;
    const uint64_t first = offset / kSegmentSize;
    const uint64_t last = (end - 1) / kSegmentSize;
    span.first = first;
    span.count = last - first + 1;
    span.plain_offset = first * kSegmentSize;
    span.enc_offset = kChunkHeaderSize + first * kEncSegmentSize;
    span.enc_len =
        (span.count - 1) * kEncSegmentSize + segment_plain_len(plain_size, last) + kSegmentTagSize;
    return span;
}

ChunkHeader encrypt_chunk_file(const Key& k_data, const FsUuid& fs_uuid, uint64_t chunk_id,
                               int in_fd, uint64_t plain_size,
                               const std::filesystem::path& out_path) {
    ChunkHeader header;
    randombytes_buf(header.nonce.data(), header.nonce.size());
    header.plain_size = plain_size;

    Fd out = Fd::open(out_path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    try {
        pwrite_full(out.get(), serialize_chunk_header(header), 0);

        // Буферы на один сегмент: память не зависит от размера чанка.
        Bytes plain(kSegmentSize);
        Bytes enc(kEncSegmentSize);
        const uint64_t count = chunk_segment_count(plain_size);
        for (uint64_t i = 0; i < count; ++i) {
            const auto len = static_cast<size_t>(segment_plain_len(plain_size, i));
            const MutableByteSpan part{plain.data(), len};
            const size_t got = pread_full(in_fd, part, i * kSegmentSize);
            // Недостающее за EOF — нули (staging-файлы разреженные и могут быть короче).
            std::fill(part.begin() + static_cast<std::ptrdiff_t>(got), part.end(), std::byte{0});

            const SegmentNonce nonce = segment_nonce(header, i);
            const SegmentAd ad = segment_ad(fs_uuid, chunk_id, i, i + 1 == count, plain_size);
            unsigned long long enc_len = 0;
            crypto_aead_xchacha20poly1305_ietf_encrypt(
                uc(enc.data()), &enc_len, uc(part.data()), part.size(), uc(ad.data()), ad.size(),
                nullptr, uc(nonce.data()), uc(k_data.data()));
            pwrite_full(out.get(), ByteSpan{enc.data(), static_cast<size_t>(enc_len)},
                        kChunkHeaderSize + i * kEncSegmentSize);
        }
        // O_TRUNC уже обрезал старое содержимое; размер задаёт последняя запись.
        if (::fsync(out.get()) != 0) {
            throw SysError(errno, "fsync " + out_path.string());
        }
    } catch (...) {
        out = Fd{};
        std::error_code ec;
        std::filesystem::remove(out_path, ec);  // недописанный чанк не оставляем
        throw;
    }
    return header;
}

ChunkHeader parse_chunk_header(ByteSpan raw) {
    if (raw.size() != kChunkHeaderSize) {
        throw CorruptError("chunk header: wrong size");
    }
    if (!std::equal(kMagic.begin(), kMagic.end(), raw.begin(),
                    [](char c, std::byte b) { return static_cast<std::byte>(c) == b; })) {
        throw CorruptError("chunk header: bad magic");
    }
    if (raw[kOffVersion] != std::byte{kVersion}) {
        throw CorruptError("chunk header: unsupported version");
    }
    if (!detail::all_zero(raw.subspan(kOffReserved, kReservedSize))) {
        throw CorruptError("chunk header: non-zero reserved bytes");
    }
    ChunkHeader header;
    std::copy_n(raw.begin() + kOffNonce, header.nonce.size(), header.nonce.begin());
    header.plain_size = get_u64_le(raw.subspan(kOffPlainSize, 8));
    return header;
}

Bytes decrypt_chunk_segments(const Key& k_data, const FsUuid& fs_uuid, uint64_t chunk_id,
                             const ChunkHeader& header, uint64_t first_segment, ByteSpan enc) {
    const uint64_t count = chunk_segment_count(header.plain_size);
    if (first_segment > count || (first_segment == count && !enc.empty())) {
        throw CorruptError("chunk " + std::to_string(chunk_id) + ": segment out of range");
    }
    Bytes plain;
    plain.reserve(enc.size());
    uint64_t i = first_segment;
    size_t pos = 0;
    while (pos < enc.size()) {
        if (i >= count) {
            throw CorruptError("chunk " + std::to_string(chunk_id) + ": more data than segments");
        }
        const auto len = static_cast<size_t>(segment_plain_len(header.plain_size, i));
        const size_t enc_len = len + kSegmentTagSize;
        if (enc.size() - pos < enc_len) {
            throw CorruptError("chunk " + std::to_string(chunk_id) + ": truncated segment " +
                               std::to_string(i));
        }
        const SegmentNonce nonce = segment_nonce(header, i);
        const SegmentAd ad = segment_ad(fs_uuid, chunk_id, i, i + 1 == count, header.plain_size);
        const size_t at = plain.size();
        plain.resize(at + len);
        unsigned long long out_len = 0;
        if (crypto_aead_xchacha20poly1305_ietf_decrypt(
                uc(plain.data() + at), &out_len, nullptr, uc(enc.data() + pos), enc_len,
                uc(ad.data()), ad.size(), uc(nonce.data()), uc(k_data.data())) != 0) {
            throw CorruptError("chunk " + std::to_string(chunk_id) +
                               ": authentication failed in segment " + std::to_string(i));
        }
        pos += enc_len;
        ++i;
    }
    return plain;
}

}  // namespace telega::crypto
