// crypto/chunk: формат зашифрованного чанка (SPEC 5).

#include "crypto/chunk.h"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sodium.h>
#include <unistd.h>

#include <algorithm>
#include <string>
#include <vector>

#include "common/files.h"
#include "test_util.h"

namespace telega::crypto {
namespace {

using test::CryptoTest;
using test::flip;
using test::random_bytes;

constexpr uint64_t S = kSegmentSize;
constexpr uint64_t kChunkId = 0x1122334455667788ULL;

class ChunkTest : public CryptoTest {
  protected:
    struct Encrypted {
        ChunkHeader header;
        Bytes file;  // зашифрованный файл целиком

        [[nodiscard]] ByteSpan raw_header() const { return ByteSpan{file}.first(kChunkHeaderSize); }
        [[nodiscard]] ByteSpan segments() const { return ByteSpan{file}.subspan(kChunkHeaderSize); }
    };

    // Записать input во входной файл, зашифровать plain_size байт, прочитать результат.
    Encrypted encrypt(ByteSpan input, uint64_t plain_size, uint64_t chunk_id = kChunkId) {
        const auto in_path = tmp / "in";
        const auto out_path = tmp / "out";
        {
            const Fd in = Fd::open(in_path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
            pwrite_full(in.get(), input, 0);
        }
        const Fd in = Fd::open(in_path, O_RDONLY);
        Encrypted e;
        e.header = encrypt_chunk_file(k_data, fs_uuid, chunk_id, in.get(), plain_size, out_path);
        e.file = read_file(out_path);
        return e;
    }

    [[nodiscard]] Bytes decrypt_all(const Encrypted& e) const { return decrypt_file(e.file); }

    // Полный путь чтения: разобрать заголовок, расшифровать все сегменты.
    [[nodiscard]] Bytes decrypt_file(ByteSpan file, uint64_t chunk_id = kChunkId) const {
        const ChunkHeader h = parse_chunk_header(file.first(kChunkHeaderSize));
        return decrypt_chunk_segments(k_data, fs_uuid, chunk_id, h, 0,
                                      file.subspan(kChunkHeaderSize));
    }

    Key k_data = Key::random();
    FsUuid fs_uuid = random_fs_uuid();
};

// Смещение начала сегмента i в зашифрованном файле.
size_t seg_off(uint64_t i) { return static_cast<size_t>(kChunkHeaderSize + i * kEncSegmentSize); }

TEST(ChunkLayout, SegmentCountAndEncryptedSize) {
    EXPECT_EQ(chunk_segment_count(0), 0U);
    EXPECT_EQ(chunk_segment_count(1), 1U);
    EXPECT_EQ(chunk_segment_count(S - 1), 1U);
    EXPECT_EQ(chunk_segment_count(S), 1U);
    EXPECT_EQ(chunk_segment_count(S + 1), 2U);
    EXPECT_EQ(chunk_segment_count(3 * S), 3U);
    EXPECT_EQ(chunk_segment_count(uint64_t{16} * 1024 * 1024), 256U);

    EXPECT_EQ(chunk_encrypted_size(0), 40U);
    EXPECT_EQ(chunk_encrypted_size(1), 40U + 1 + 16);
    EXPECT_EQ(chunk_encrypted_size(S), 40U + S + 16);
    EXPECT_EQ(chunk_encrypted_size(S + 1), 40U + S + 1 + 32);
    EXPECT_EQ(chunk_encrypted_size(uint64_t{16} * 1024 * 1024),
              40U + uint64_t{16} * 1024 * 1024 + uint64_t{256} * 16);
}

TEST(ChunkLayout, SegmentsForRanges) {
    const uint64_t plain = 3 * S + 100;  // 4 сегмента, последний 100 байт

    // Внутри одного сегмента.
    SegmentSpan s = chunk_segments_for(plain, 10, 20);
    EXPECT_EQ(s.first, 0U);
    EXPECT_EQ(s.count, 1U);
    EXPECT_EQ(s.enc_offset, 40U);
    EXPECT_EQ(s.enc_len, kEncSegmentSize);
    EXPECT_EQ(s.plain_offset, 0U);

    // Через границу сегментов 0/1.
    s = chunk_segments_for(plain, S - 1, 2);
    EXPECT_EQ(s.first, 0U);
    EXPECT_EQ(s.count, 2U);
    EXPECT_EQ(s.enc_offset, 40U);
    EXPECT_EQ(s.enc_len, 2 * kEncSegmentSize);

    // Ровно сегмент 1.
    s = chunk_segments_for(plain, S, S);
    EXPECT_EQ(s.first, 1U);
    EXPECT_EQ(s.count, 1U);
    EXPECT_EQ(s.enc_offset, 40U + kEncSegmentSize);
    EXPECT_EQ(s.enc_len, kEncSegmentSize);
    EXPECT_EQ(s.plain_offset, S);

    // Последний байт: короткий последний сегмент.
    s = chunk_segments_for(plain, plain - 1, 1);
    EXPECT_EQ(s.first, 3U);
    EXPECT_EQ(s.count, 1U);
    EXPECT_EQ(s.enc_offset, 40U + 3 * kEncSegmentSize);
    EXPECT_EQ(s.enc_len, 100U + 16);
    EXPECT_EQ(s.plain_offset, 3 * S);

    // Диапазон за концом обрезается по plain_size.
    s = chunk_segments_for(plain, 2 * S + 5, 10 * S);
    EXPECT_EQ(s.first, 2U);
    EXPECT_EQ(s.count, 2U);
    EXPECT_EQ(s.enc_offset, 40U + 2 * kEncSegmentSize);
    EXPECT_EQ(s.enc_len, kEncSegmentSize + 100 + 16);

    // Весь чанк; огромный len без переполнения.
    s = chunk_segments_for(plain, 0, UINT64_MAX);
    EXPECT_EQ(s.first, 0U);
    EXPECT_EQ(s.count, 4U);
    EXPECT_EQ(s.enc_offset + s.enc_len, chunk_encrypted_size(plain));
    s = chunk_segments_for(plain, S, UINT64_MAX - 1);
    EXPECT_EQ(s.count, 3U);
    EXPECT_EQ(s.enc_offset + s.enc_len, chunk_encrypted_size(plain));

    // Пустые диапазоны.
    EXPECT_EQ(chunk_segments_for(plain, 0, 0).count, 0U);
    EXPECT_EQ(chunk_segments_for(plain, 5, 0).count, 0U);
    EXPECT_EQ(chunk_segments_for(plain, plain, 10).count, 0U);
    EXPECT_EQ(chunk_segments_for(plain, plain + 1000, 10).count, 0U);
    EXPECT_EQ(chunk_segments_for(0, 0, 10).count, 0U);

    // Чанк, кратный сегменту: последний сегмент полный.
    s = chunk_segments_for(2 * S, 2 * S - 1, 1);
    EXPECT_EQ(s.first, 1U);
    EXPECT_EQ(s.count, 1U);
    EXPECT_EQ(s.enc_len, kEncSegmentSize);
}

TEST_F(ChunkTest, RoundTripAtSegmentBoundaries) {
    for (const uint64_t size :
         {uint64_t{0}, uint64_t{1}, S - 1, S, S + 1, 3 * S, uint64_t{200003}}) {
        SCOPED_TRACE(size);
        const Bytes data = random_bytes(static_cast<size_t>(size));
        const Encrypted e = encrypt(data, size);
        EXPECT_EQ(e.file.size(), chunk_encrypted_size(size));
        EXPECT_EQ(e.header.plain_size, size);

        const ChunkHeader parsed = parse_chunk_header(e.raw_header());
        EXPECT_EQ(parsed.nonce, e.header.nonce);
        EXPECT_EQ(parsed.plain_size, size);
        EXPECT_EQ(decrypt_all(e), data);
    }
}

TEST_F(ChunkTest, HeaderLayout) {
    const Bytes data = random_bytes(1000);
    const Encrypted e = encrypt(data, data.size());
    const std::string magic(reinterpret_cast<const char*>(e.file.data()), 8);
    EXPECT_EQ(magic, "TGFSCHK1");
    EXPECT_EQ(e.file[8], std::byte{1});
    for (size_t i = 9; i < 16; ++i) {
        EXPECT_EQ(e.file[i], std::byte{0});
    }
    EXPECT_TRUE(std::equal(e.header.nonce.begin(), e.header.nonce.end(), e.file.begin() + 16));
    EXPECT_EQ(get_u64_le(ByteSpan{e.file}.subspan(32, 8)), 1000U);
}

// Независимая проверка раскладки SPEC 5: каждый сегмент расшифровывается напрямую libsodium
// с nonce = chunk_nonce || u64(i) и AD = fs_uuid || u64(chunk_id) || u64(i) || u8(is_last) ||
// u64(plain_size), по смещениям [40 + i * 65552, ... + len_i + 16).
TEST_F(ChunkTest, SegmentFormatMatchesSpec) {
    const uint64_t size = 2 * S + 777;
    const Bytes data = random_bytes(static_cast<size_t>(size));
    const Encrypted e = encrypt(data, size);
    for (uint64_t i = 0; i < 3; ++i) {
        SCOPED_TRACE(i);
        const size_t len = i < 2 ? S : 777;
        Bytes nonce(e.header.nonce.begin(), e.header.nonce.end());
        append_u64_le(nonce, i);
        Bytes ad(fs_uuid.begin(), fs_uuid.end());
        append_u64_le(ad, kChunkId);
        append_u64_le(ad, i);
        ad.push_back(i == 2 ? std::byte{1} : std::byte{0});
        append_u64_le(ad, size);
        ASSERT_EQ(nonce.size(), 24U);
        ASSERT_EQ(ad.size(), 41U);

        Bytes plain(len);
        unsigned long long plain_len = 0;
        ASSERT_EQ(crypto_aead_xchacha20poly1305_ietf_decrypt(
                      reinterpret_cast<unsigned char*>(plain.data()), &plain_len, nullptr,
                      reinterpret_cast<const unsigned char*>(e.file.data() + seg_off(i)),
                      len + kSegmentTagSize, reinterpret_cast<const unsigned char*>(ad.data()),
                      ad.size(), reinterpret_cast<const unsigned char*>(nonce.data()),
                      reinterpret_cast<const unsigned char*>(k_data.data())),
                  0);
        ASSERT_EQ(plain_len, len);
        EXPECT_TRUE(std::equal(plain.begin(), plain.end(),
                               data.begin() + static_cast<std::ptrdiff_t>(i * S)));
    }
}

TEST_F(ChunkTest, DecryptArbitraryRanges) {
    const uint64_t size = 5 * S + 1234;
    const Bytes data = random_bytes(static_cast<size_t>(size));
    const Encrypted e = encrypt(data, size);
    struct Range {
        uint64_t off;
        uint64_t len;
    };
    for (const Range r :
         {Range{0, 1}, Range{S - 1, 2}, Range{S, S}, Range{2 * S + 7, 2 * S}, Range{size - 1, 1},
          Range{5 * S, 1234}, Range{4 * S + 1, 10 * S}, Range{0, size}}) {
        SCOPED_TRACE(r.off);
        const SegmentSpan span = chunk_segments_for(size, r.off, r.len);
        ASSERT_GT(span.count, 0U);
        const Bytes plain =
            decrypt_chunk_segments(k_data, fs_uuid, kChunkId, e.header, span.first,
                                   ByteSpan{e.file}.subspan(span.enc_offset, span.enc_len));
        const uint64_t end = std::min(size, span.plain_offset + span.count * S);
        ASSERT_EQ(plain.size(), end - span.plain_offset);
        EXPECT_TRUE(std::equal(plain.begin(), plain.end(),
                               data.begin() + static_cast<std::ptrdiff_t>(span.plain_offset)));
    }
}

TEST_F(ChunkTest, ShortInputIsZeroPadded) {
    const Bytes data = random_bytes(1000);
    const uint64_t size = 2 * S + 17;
    const Encrypted e = encrypt(data, size);
    Bytes expected = data;
    expected.resize(size);
    EXPECT_EQ(decrypt_all(e), expected);

    // Пустой вход — все нули.
    const Encrypted zeros = encrypt({}, S + 3);
    EXPECT_EQ(decrypt_all(zeros), Bytes(S + 3));
}

TEST_F(ChunkTest, SparseInputReadsZeros) {
    const Bytes data = random_bytes(5000);
    const uint64_t size = 3 * S;
    {
        const Fd in = Fd::open(tmp / "sparse", O_RDWR | O_CREAT | O_TRUNC, 0600);
        ASSERT_EQ(::ftruncate(in.get(), static_cast<off_t>(size)), 0);
        pwrite_full(in.get(), data, S + 100);  // дыра до и после
    }
    const Fd in = Fd::open(tmp / "sparse", O_RDONLY);
    const ChunkHeader h =
        encrypt_chunk_file(k_data, fs_uuid, kChunkId, in.get(), size, tmp / "out");
    Bytes expected(size);
    std::copy(data.begin(), data.end(), expected.begin() + S + 100);
    EXPECT_EQ(decrypt_chunk_segments(k_data, fs_uuid, kChunkId, h, 0,
                                     ByteSpan{read_file(tmp / "out")}.subspan(kChunkHeaderSize)),
              expected);
}

TEST_F(ChunkTest, LongerInputIsCutAtPlainSize) {
    const Bytes data = random_bytes(3 * S);
    const Encrypted e = encrypt(data, S + 5);
    EXPECT_EQ(decrypt_all(e), Bytes(data.begin(), data.begin() + S + 5));
}

TEST_F(ChunkTest, OutputOverwritesExistingFile) {
    const Bytes big = random_bytes(4 * S);
    (void)encrypt(big, big.size());
    const Bytes small = random_bytes(10);
    const Encrypted e = encrypt(small, small.size());
    EXPECT_EQ(e.file.size(), chunk_encrypted_size(10));
    EXPECT_EQ(decrypt_all(e), small);
}

TEST_F(ChunkTest, SameDataEncryptsDifferently) {
    const Bytes data = random_bytes(S + 10);
    const Encrypted a = encrypt(data, data.size());
    const Encrypted b = encrypt(data, data.size());
    EXPECT_NE(a.header.nonce, b.header.nonce);
    EXPECT_NE(Bytes(a.file.begin() + 40, a.file.end()), Bytes(b.file.begin() + 40, b.file.end()));
    EXPECT_EQ(decrypt_all(a), data);
    EXPECT_EQ(decrypt_all(b), data);
}

TEST_F(ChunkTest, ParseHeaderRejectsBadFields) {
    const Encrypted e = encrypt(random_bytes(10), 10);
    const Bytes raw(e.raw_header().begin(), e.raw_header().end());

    EXPECT_THROW(parse_chunk_header(ByteSpan{raw}.first(39)), CorruptError);
    Bytes longer = raw;
    longer.push_back(std::byte{0});
    EXPECT_THROW(parse_chunk_header(longer), CorruptError);
    EXPECT_THROW(parse_chunk_header({}), CorruptError);

    for (size_t i = 0; i < 8; ++i) {  // magic
        Bytes bad = raw;
        flip(bad, i);
        EXPECT_THROW(parse_chunk_header(bad), CorruptError) << "magic byte " << i;
    }
    for (const auto v : {std::byte{0}, std::byte{2}, std::byte{0xFF}}) {  // version
        Bytes bad = raw;
        bad[8] = v;
        EXPECT_THROW(parse_chunk_header(bad), CorruptError);
    }
    for (size_t i = 9; i < 16; ++i) {  // reserved
        Bytes bad = raw;
        bad[i] = std::byte{0x40};
        EXPECT_THROW(parse_chunk_header(bad), CorruptError) << "reserved byte " << i;
    }
}

// Порча любого байта файла (заголовок, шифртекст, тег) — CorruptError на полном чтении.
TEST_F(ChunkTest, EveryByteOfSmallChunkIsAuthenticated) {
    const Bytes data = random_bytes(100);
    const Encrypted e = encrypt(data, data.size());
    ASSERT_EQ(e.file.size(), 40U + 100 + 16);
    for (size_t i = 0; i < e.file.size(); ++i) {
        Bytes bad = e.file;
        flip(bad, i);
        EXPECT_THROW(decrypt_file(bad), CorruptError) << "byte " << i;
    }
}

TEST_F(ChunkTest, CorruptionInEachSegmentIsDetected) {
    const uint64_t size = 3 * S + 500;
    const Bytes data = random_bytes(static_cast<size_t>(size));
    const Encrypted e = encrypt(data, size);
    for (uint64_t seg = 0; seg < 4; ++seg) {
        const size_t len = seg < 3 ? S : 500;
        const size_t start = seg_off(seg);
        // Первый и последний байт шифртекста, первый и последний байт тега.
        for (const size_t pos :
             {start, start + len / 2, start + len - 1, start + len, start + len + 15}) {
            Bytes bad = e.file;
            flip(bad, pos);
            EXPECT_THROW(decrypt_file(bad), CorruptError) << "segment " << seg << " pos " << pos;
            // Порча одного сегмента не мешает читать остальные.
            const uint64_t other = (seg + 1) % 4;
            const SegmentSpan span = chunk_segments_for(size, other * S, 1);
            EXPECT_NO_THROW(
                decrypt_chunk_segments(k_data, fs_uuid, kChunkId, e.header, span.first,
                                       ByteSpan{bad}.subspan(span.enc_offset, span.enc_len)));
        }
    }
}

TEST_F(ChunkTest, HeaderFieldsAreBoundToCiphertext) {
    const uint64_t size = 2 * S + 10;
    const Encrypted e = encrypt(random_bytes(static_cast<size_t>(size)), size);

    ChunkHeader other_nonce = e.header;
    other_nonce.nonce[3] ^= std::byte{1};
    EXPECT_THROW(decrypt_chunk_segments(k_data, fs_uuid, kChunkId, other_nonce, 0, e.segments()),
                 CorruptError);

    // Другой plain_size с тем же числом сегментов: длины совпадают, ломается AD.
    for (const uint64_t ps : {size + 1, size - 1, 2 * S + 1, 3 * S}) {
        SCOPED_TRACE(ps);
        ChunkHeader h = e.header;
        h.plain_size = ps;
        EXPECT_THROW(decrypt_chunk_segments(k_data, fs_uuid, kChunkId, h, 0, e.segments()),
                     CorruptError);
        // Первый сегмент полный и в обоих случаях не последний: AD всё равно отличается.
        EXPECT_THROW(decrypt_chunk_segments(k_data, fs_uuid, kChunkId, h, 0,
                                            e.segments().first(kEncSegmentSize)),
                     CorruptError);
    }
}

TEST_F(ChunkTest, WrongKeyChunkIdOrFsUuidFails) {
    const Bytes data = random_bytes(S + 1);
    const Encrypted e = encrypt(data, data.size());
    EXPECT_THROW(
        decrypt_chunk_segments(Key::random(), fs_uuid, kChunkId, e.header, 0, e.segments()),
        CorruptError);
    EXPECT_THROW(decrypt_chunk_segments(k_data, fs_uuid, kChunkId + 1, e.header, 0, e.segments()),
                 CorruptError);
    FsUuid other = fs_uuid;
    other[15] ^= std::byte{1};
    EXPECT_THROW(decrypt_chunk_segments(k_data, other, kChunkId, e.header, 0, e.segments()),
                 CorruptError);

    // Чанк другого id с теми же данными целиком не подставляется вместо нашего.
    const Encrypted foreign = encrypt(data, data.size(), kChunkId + 1);
    EXPECT_THROW(decrypt_file(foreign.file), CorruptError);
}

TEST_F(ChunkTest, SwappedSegmentsFail) {
    const uint64_t size = 3 * S;
    const Encrypted e = encrypt(random_bytes(static_cast<size_t>(size)), size);
    Bytes swapped = e.file;
    std::swap_ranges(swapped.begin() + static_cast<std::ptrdiff_t>(seg_off(0)),
                     swapped.begin() + static_cast<std::ptrdiff_t>(seg_off(1)),
                     swapped.begin() + static_cast<std::ptrdiff_t>(seg_off(1)));
    EXPECT_THROW(decrypt_file(swapped), CorruptError);

    // Сегмент 1, поданный как сегмент 0.
    EXPECT_THROW(decrypt_chunk_segments(k_data, fs_uuid, kChunkId, e.header, 0,
                                        ByteSpan{e.file}.subspan(seg_off(1), kEncSegmentSize)),
                 CorruptError);
}

TEST_F(ChunkTest, TruncationIsDetected) {
    const uint64_t size = 3 * S;
    const Encrypted e = encrypt(random_bytes(static_cast<size_t>(size)), size);

    // Отрезан хвост последнего сегмента.
    for (const size_t cut : {size_t{1}, size_t{16}, size_t{17}, size_t{100}}) {
        EXPECT_THROW(decrypt_file(ByteSpan{e.file}.first(e.file.size() - cut)), CorruptError)
            << cut;
    }
    // Отрезан последний сегмент целиком: сегменты 0 и 1 — законный частичный диапазон,
    // поэтому полноту файла проверяет читатель по chunk_encrypted_size (здесь она нарушена).
    EXPECT_NE(seg_off(2), chunk_encrypted_size(size));

    // Отрезан последний сегмент и подправлен plain_size: сегмент 1 не был последним (AD).
    Bytes cut = Bytes(e.file.begin(), e.file.begin() + static_cast<std::ptrdiff_t>(seg_off(2)));
    put_u64_le(MutableByteSpan{cut}.subspan(32, 8), 2 * S);
    EXPECT_THROW(decrypt_file(cut), CorruptError);
}

TEST_F(ChunkTest, EncLengthAndRangeChecks) {
    const uint64_t size = 2 * S + 10;
    const Encrypted e = encrypt(random_bytes(static_cast<size_t>(size)), size);
    const ByteSpan segs = e.segments();

    // Лишний байт после последнего сегмента.
    Bytes longer(segs.begin(), segs.end());
    longer.push_back(std::byte{0});
    EXPECT_THROW(decrypt_chunk_segments(k_data, fs_uuid, kChunkId, e.header, 0, longer),
                 CorruptError);
    // Лишние байты после промежуточного сегмента (неполный следующий).
    EXPECT_THROW(decrypt_chunk_segments(k_data, fs_uuid, kChunkId, e.header, 0,
                                        segs.first(kEncSegmentSize + 5)),
                 CorruptError);
    // first_segment + count за числом сегментов.
    EXPECT_THROW(decrypt_chunk_segments(k_data, fs_uuid, kChunkId, e.header, 2,
                                        ByteSpan{e.file}.subspan(seg_off(1))),
                 CorruptError);
    EXPECT_THROW(decrypt_chunk_segments(k_data, fs_uuid, kChunkId, e.header, 4, {}), CorruptError);
    EXPECT_THROW(decrypt_chunk_segments(k_data, fs_uuid, kChunkId, e.header, 3,
                                        ByteSpan{e.file}.subspan(seg_off(2))),
                 CorruptError);
    EXPECT_THROW(decrypt_chunk_segments(k_data, fs_uuid, kChunkId, e.header, UINT64_MAX,
                                        ByteSpan{e.file}.subspan(seg_off(2))),
                 CorruptError);
    // Пустой диапазон в пределах чанка (в том числе сразу за последним сегментом) — пусто.
    EXPECT_TRUE(decrypt_chunk_segments(k_data, fs_uuid, kChunkId, e.header, 1, {}).empty());
    EXPECT_TRUE(decrypt_chunk_segments(k_data, fs_uuid, kChunkId, e.header, 3, {}).empty());
}

TEST_F(ChunkTest, EmptyChunk) {
    const Encrypted e = encrypt({}, 0);
    ASSERT_EQ(e.file.size(), kChunkHeaderSize);
    EXPECT_TRUE(decrypt_all(e).empty());
    // Сегментов нет — любые данные лишние.
    EXPECT_THROW(
        decrypt_chunk_segments(k_data, fs_uuid, kChunkId, e.header, 0, Bytes(kSegmentTagSize)),
        CorruptError);
}

}  // namespace
}  // namespace telega::crypto
