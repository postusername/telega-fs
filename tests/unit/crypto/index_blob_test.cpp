// crypto/index_blob: заголовок и тело блоба индекса, zstd.

#include "crypto/index_blob.h"

#include <gtest/gtest.h>
#include <sodium.h>
#include <zstd.h>

#include <algorithm>
#include <string>
#include <vector>

#include "test_util.h"

namespace telega::crypto {
namespace {

using test::CryptoTest;
using test::flip;
using test::random_bytes;

constexpr size_t B = kIndexStreamBlock;
constexpr size_t kStreamHeader = 24;
constexpr size_t kBlockOverhead = 17;

const unsigned char kTagMessage = crypto_secretstream_xchacha20poly1305_TAG_MESSAGE;
const unsigned char kTagFinal = crypto_secretstream_xchacha20poly1305_TAG_FINAL;

IndexHeader sample_header(BlobKind kind = BlobKind::delta) {
    IndexHeader h;
    h.kind = kind;
    h.fs_uuid = random_fs_uuid();
    h.generation = 0x0102030405060708ULL;
    h.snapshot_gen = 0x1112131415161718ULL;
    h.snapshot_msg_id = -0x2122232425262728LL;
    h.prev_msg_id = 0x3132333435363738LL;
    randombytes_buf(h.prev_hash.data(), h.prev_hash.size());
    h.kdf = passphrase_kdf_params(0x4142434445464748ULL, 0x5152535455565758ULL);
    randombytes_buf(h.wrapped_master.data(), h.wrapped_master.size());
    h.body_size = 0x6162636465666768ULL;
    return h;
}

size_t sealed_size(size_t body) {
    const size_t blocks = (body + B - 1) / B;
    return kIndexHeaderSize + kStreamHeader + body + blocks * kBlockOverhead;
}

// Ручная сборка блоба: для проверки правил тегов и AD, которых seal_index_blob не нарушает.
struct ManualBlock {
    size_t size;
    unsigned char tag;
    bool with_header_ad;
};

Bytes build_blob(const Key& key, IndexHeader header, const std::vector<ManualBlock>& blocks) {
    header.body_size = kStreamHeader;
    for (const ManualBlock& b : blocks) {
        header.body_size += b.size + kBlockOverhead;
    }
    const auto raw = serialize_index_header(header);
    Bytes blob(raw.begin(), raw.end());
    blob.resize(kIndexHeaderSize + header.body_size);
    crypto_secretstream_xchacha20poly1305_state st;
    auto* out = reinterpret_cast<unsigned char*>(blob.data() + kIndexHeaderSize);
    crypto_secretstream_xchacha20poly1305_init_push(
        &st, out, reinterpret_cast<const unsigned char*>(key.data()));
    out += kStreamHeader;
    for (const ManualBlock& b : blocks) {
        const Bytes plain = random_bytes(b.size);
        unsigned long long n = 0;
        crypto_secretstream_xchacha20poly1305_push(
            &st, out, &n, reinterpret_cast<const unsigned char*>(plain.data()), plain.size(),
            b.with_header_ad ? reinterpret_cast<const unsigned char*>(raw.data()) : nullptr,
            b.with_header_ad ? raw.size() : 0, b.tag);
        out += n;
    }
    return blob;
}

class IndexBlobTest : public CryptoTest {
  protected:
    Key k_index = Key::random();
};

TEST(IndexHeaderFormat, ExactLayout) {
    const IndexHeader h = sample_header(BlobKind::delta);
    const auto raw = serialize_index_header(h);
    const ByteSpan s{raw};
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(raw.data()), 8), "TGFSIDX1");
    EXPECT_EQ(raw[8], std::byte{1});
    EXPECT_EQ(raw[9], std::byte{2});
    EXPECT_TRUE(std::all_of(raw.begin() + 10, raw.begin() + 16,
                            [](std::byte b) { return b == std::byte{0}; }));
    EXPECT_TRUE(std::equal(h.fs_uuid.begin(), h.fs_uuid.end(), raw.begin() + 16));
    EXPECT_EQ(get_u64_le(s.subspan(32, 8)), h.generation);
    EXPECT_EQ(get_u64_le(s.subspan(40, 8)), h.snapshot_gen);
    EXPECT_EQ(static_cast<int64_t>(get_u64_le(s.subspan(48, 8))), h.snapshot_msg_id);
    EXPECT_EQ(static_cast<int64_t>(get_u64_le(s.subspan(56, 8))), h.prev_msg_id);
    EXPECT_TRUE(std::equal(h.prev_hash.begin(), h.prev_hash.end(), raw.begin() + 64));
    EXPECT_EQ(raw[96], std::byte{1});
    EXPECT_EQ(get_u64_le(s.subspan(97, 8)), h.kdf.opslimit);
    EXPECT_EQ(get_u64_le(s.subspan(105, 8)), h.kdf.memlimit);
    EXPECT_TRUE(std::equal(h.kdf.salt.begin(), h.kdf.salt.end(), raw.begin() + 113));
    EXPECT_TRUE(std::equal(h.wrapped_master.begin(), h.wrapped_master.end(), raw.begin() + 129));
    EXPECT_EQ(get_u64_le(s.subspan(201, 8)), h.body_size);
    EXPECT_TRUE(
        std::all_of(raw.begin() + 209, raw.end(), [](std::byte b) { return b == std::byte{0}; }));
}

TEST(IndexHeaderFormat, RoundTrip) {
    for (const BlobKind kind : {BlobKind::snapshot, BlobKind::delta}) {
        IndexHeader h = sample_header(kind);
        EXPECT_EQ(parse_index_header(serialize_index_header(h)), h);
        h.kdf = keyfile_kdf_params();
        EXPECT_EQ(parse_index_header(serialize_index_header(h)), h);
    }
    // Берутся первые 256 байт, хвост (тело) не влияет.
    const IndexHeader h = sample_header();
    const auto raw = serialize_index_header(h);
    Bytes longer(raw.begin(), raw.end());
    longer.resize(1000, std::byte{0xAA});
    EXPECT_EQ(parse_index_header(longer), h);
}

TEST(IndexHeaderFormat, ParseRejectsBadFields) {
    const IndexHeader h = sample_header();
    const auto arr = serialize_index_header(h);
    const Bytes raw(arr.begin(), arr.end());
    const auto expect_corrupt = [](const Bytes& bad, const std::string& what) {
        EXPECT_THROW(parse_index_header(bad), CorruptError) << what;
    };

    expect_corrupt(Bytes(raw.begin(), raw.end() - 1), "short");
    expect_corrupt({}, "empty");
    for (size_t i = 0; i < 8; ++i) {
        Bytes bad = raw;
        flip(bad, i);
        expect_corrupt(bad, "magic " + std::to_string(i));
    }
    for (const int v : {0, 2, 255}) {
        Bytes bad = raw;
        bad[8] = static_cast<std::byte>(v);
        expect_corrupt(bad, "version " + std::to_string(v));
    }
    for (const int v : {0, 3, 255}) {
        Bytes bad = raw;
        bad[9] = static_cast<std::byte>(v);
        expect_corrupt(bad, "kind " + std::to_string(v));
    }
    for (size_t i = 10; i < 16; ++i) {
        Bytes bad = raw;
        bad[i] = std::byte{1};
        expect_corrupt(bad, "reserved " + std::to_string(i));
    }
    for (const int v : {2, 255}) {
        Bytes bad = raw;
        bad[96] = static_cast<std::byte>(v);
        expect_corrupt(bad, "kdf_alg " + std::to_string(v));
    }
    for (size_t i = 209; i < kIndexHeaderSize; ++i) {
        Bytes bad = raw;
        bad[i] = std::byte{0x80};
        expect_corrupt(bad, "padding " + std::to_string(i));
    }

    // keyfile: opslimit, memlimit и salt обязаны быть нулевыми.
    IndexHeader kf = h;
    kf.kdf = keyfile_kdf_params();
    const auto kf_arr = serialize_index_header(kf);
    const Bytes kf_raw(kf_arr.begin(), kf_arr.end());
    ASSERT_NO_THROW(parse_index_header(kf_raw));
    for (size_t i = 97; i < 129; ++i) {
        Bytes bad = kf_raw;
        bad[i] = std::byte{1};
        expect_corrupt(bad, "keyfile kdf param byte " + std::to_string(i));
    }
}

TEST_F(IndexBlobTest, SealOpenRoundTripAcrossBlocks) {
    for (const size_t size : {size_t{1}, size_t{1000}, B - 1, B, B + 1, 2 * B, 2 * B + 12345}) {
        SCOPED_TRACE(size);
        const Bytes body = random_bytes(size);
        IndexHeader h = sample_header();
        const Bytes blob = seal_index_blob(k_index, h, body);
        ASSERT_EQ(blob.size(), sealed_size(size));

        const OpenedBlob opened = open_index_blob(k_index, blob);
        h.body_size = blob.size() - kIndexHeaderSize;  // seal перезаписывает переданное значение
        EXPECT_EQ(opened.header, h);
        EXPECT_EQ(parse_index_header(blob), h);
        EXPECT_EQ(opened.body, body);
    }
}

TEST_F(IndexBlobTest, SealRejectsEmptyBody) {
    EXPECT_THROW(seal_index_blob(k_index, sample_header(), {}), std::invalid_argument);
}

TEST_F(IndexBlobTest, SealIsRandomized) {
    const Bytes body = random_bytes(100);
    const IndexHeader h = sample_header();
    EXPECT_NE(seal_index_blob(k_index, h, body), seal_index_blob(k_index, h, body));
}

TEST_F(IndexBlobTest, WrongKeyFails) {
    const Bytes blob = seal_index_blob(k_index, sample_header(), random_bytes(100));
    EXPECT_THROW(open_index_blob(Key::random(), blob), CorruptError);
}

// Заголовок — AD первого блока: порча любого его байта обнаруживается при open.
TEST_F(IndexBlobTest, EveryHeaderByteIsAuthenticated) {
    const Bytes blob = seal_index_blob(k_index, sample_header(), random_bytes(300));
    for (size_t i = 0; i < kIndexHeaderSize; ++i) {
        Bytes bad = blob;
        flip(bad, i);
        EXPECT_THROW(open_index_blob(k_index, bad), CorruptError) << "header byte " << i;
    }
}

TEST_F(IndexBlobTest, HeaderChangeAcceptedByParseFailsOnOpen) {
    const Bytes blob = seal_index_blob(k_index, sample_header(), random_bytes(B + 10));
    struct Field {
        size_t off;
        const char* name;
    };
    for (const Field f :
         {Field{16, "fs_uuid"}, Field{32, "generation"}, Field{40, "snapshot_gen"},
          Field{48, "snapshot_msg_id"}, Field{56, "prev_msg_id"}, Field{64, "prev_hash"},
          Field{97, "opslimit"}, Field{113, "salt"}, Field{129, "wrapped_master"}}) {
        Bytes bad = blob;
        flip(bad, f.off);
        EXPECT_NO_THROW(parse_index_header(bad)) << f.name;
        EXPECT_THROW(open_index_blob(k_index, bad), CorruptError) << f.name;
    }
    // Подмена вида блоба (снимок <-> дельта).
    Bytes bad = blob;
    bad[9] = std::byte{1};
    EXPECT_NO_THROW(parse_index_header(bad));
    EXPECT_THROW(open_index_blob(k_index, bad), CorruptError);
}

TEST_F(IndexBlobTest, BodyCorruptionIsDetected) {
    const size_t size = 2 * B + 100;
    const Bytes blob = seal_index_blob(k_index, sample_header(), random_bytes(size));
    const size_t body = kIndexHeaderSize;
    const size_t block0 = body + kStreamHeader;
    const size_t block1 = block0 + B + kBlockOverhead;
    const size_t block2 = block1 + B + kBlockOverhead;
    for (const size_t pos :
         {body, body + kStreamHeader - 1,                   // заголовок потока
          block0, block0 + 1, block0 + 500000, block1 - 1,  // блок 0: тег, шифртекст, mac
          block1, block1 + 17, block2 - 1,                  // блок 1
          block2, block2 + kBlockOverhead + 50, blob.size() - 1}) {  // последний блок
        Bytes bad = blob;
        flip(bad, pos);
        EXPECT_THROW(open_index_blob(k_index, bad), CorruptError) << "pos " << pos;
    }
}

TEST_F(IndexBlobTest, SwappedBlocksFail) {
    const Bytes blob = seal_index_blob(k_index, sample_header(), random_bytes(3 * B + 5));
    const auto block = [](size_t i) {
        return static_cast<std::ptrdiff_t>(kIndexHeaderSize + kStreamHeader +
                                           i * (B + kBlockOverhead));
    };
    Bytes bad = blob;
    std::swap_ranges(bad.begin() + block(1), bad.begin() + block(2), bad.begin() + block(2));
    EXPECT_THROW(open_index_blob(k_index, bad), CorruptError);
}

TEST_F(IndexBlobTest, TrailingBytesAreRejected) {
    const Bytes blob = seal_index_blob(k_index, sample_header(), random_bytes(B + 3));
    Bytes longer = blob;
    longer.push_back(std::byte{0});
    EXPECT_THROW(open_index_blob(k_index, longer), CorruptError);

    // Даже с исправленным body_size: заголовок аутентифицирован.
    put_u64_le(MutableByteSpan{longer}.subspan(201, 8), longer.size() - kIndexHeaderSize);
    EXPECT_THROW(open_index_blob(k_index, longer), CorruptError);
}

TEST_F(IndexBlobTest, TruncationIsDetected) {
    const Bytes blob = seal_index_blob(k_index, sample_header(), random_bytes(2 * B + 7));
    for (const size_t cut : {size_t{1}, size_t{17}, size_t{7 + kBlockOverhead}}) {
        Bytes bad(blob.begin(), blob.end() - static_cast<std::ptrdiff_t>(cut));
        EXPECT_THROW(open_index_blob(k_index, bad), CorruptError) << cut;
        // С подогнанным body_size.
        put_u64_le(MutableByteSpan{bad}.subspan(201, 8), bad.size() - kIndexHeaderSize);
        EXPECT_THROW(open_index_blob(k_index, bad), CorruptError) << cut;
    }
    // Только заголовок; заголовок и заголовок потока.
    for (const size_t keep : {kIndexHeaderSize, kIndexHeaderSize + kStreamHeader,
                              kIndexHeaderSize + kStreamHeader + kBlockOverhead}) {
        Bytes bad(blob.begin(), blob.begin() + static_cast<std::ptrdiff_t>(keep));
        put_u64_le(MutableByteSpan{bad}.subspan(201, 8), bad.size() - kIndexHeaderSize);
        EXPECT_THROW(open_index_blob(k_index, bad), CorruptError) << keep;
    }
    EXPECT_THROW(open_index_blob(k_index, Bytes(blob.begin(), blob.begin() + 100)), CorruptError);
}

TEST_F(IndexBlobTest, ManualBuilderControl) {
    // Проверка самого сборщика: корректный поток открывается.
    const IndexHeader h = sample_header();
    EXPECT_NO_THROW(open_index_blob(k_index, build_blob(k_index, h, {{10, kTagFinal, true}})));
    EXPECT_NO_THROW(open_index_blob(
        k_index, build_blob(k_index, h, {{B, kTagMessage, true}, {B, kTagFinal, false}})));
}

TEST_F(IndexBlobTest, MissingFinalTagIsRejected) {
    const IndexHeader h = sample_header();
    // Обрезка по границе блока: последний оставшийся блок с TAG_MESSAGE.
    EXPECT_THROW(open_index_blob(k_index, build_blob(k_index, h, {{10, kTagMessage, true}})),
                 CorruptError);
    EXPECT_THROW(
        open_index_blob(k_index,
                        build_blob(k_index, h, {{B, kTagMessage, true}, {B, kTagMessage, false}})),
        CorruptError);
}

TEST_F(IndexBlobTest, FinalTagMustBeOnlyOnLastBlock) {
    const IndexHeader h = sample_header();
    EXPECT_THROW(
        open_index_blob(k_index,
                        build_blob(k_index, h, {{B, kTagFinal, true}, {10, kTagFinal, false}})),
        CorruptError);
    EXPECT_THROW(
        open_index_blob(k_index,
                        build_blob(k_index, h, {{B, kTagFinal, true}, {10, kTagMessage, false}})),
        CorruptError);
    // Другие теги тоже не допускаются.
    EXPECT_THROW(open_index_blob(
                     k_index, build_blob(k_index, h,
                                         {{B, crypto_secretstream_xchacha20poly1305_TAG_PUSH, true},
                                          {10, kTagFinal, false}})),
                 CorruptError);
}

TEST_F(IndexBlobTest, BlockStructureRules) {
    const IndexHeader h = sample_header();
    // Пустой последний блок.
    EXPECT_THROW(
        open_index_blob(k_index,
                        build_blob(k_index, h, {{B, kTagMessage, true}, {0, kTagFinal, false}})),
        CorruptError);
    EXPECT_THROW(open_index_blob(k_index, build_blob(k_index, h, {{0, kTagFinal, true}})),
                 CorruptError);
    // Неполный промежуточный блок.
    EXPECT_THROW(
        open_index_blob(k_index,
                        build_blob(k_index, h, {{100, kTagMessage, true}, {10, kTagFinal, false}})),
        CorruptError);
    // Заголовок не передан как AD первого блока.
    EXPECT_THROW(open_index_blob(k_index, build_blob(k_index, h, {{10, kTagFinal, false}})),
                 CorruptError);
    // AD у второго блока.
    EXPECT_THROW(
        open_index_blob(k_index,
                        build_blob(k_index, h, {{B, kTagMessage, true}, {10, kTagFinal, true}})),
        CorruptError);
}

TEST(Zstd, RoundTrip) {
    Bytes text;
    for (int i = 0; i < 20000; ++i) {
        const std::string line = "row " + std::to_string(i % 97) + " of the index\n";
        const ByteSpan s = as_bytes(line);
        text.insert(text.end(), s.begin(), s.end());
    }
    for (const int level : {3, 9}) {
        const Bytes packed = zstd_compress(text, level);
        EXPECT_LT(packed.size(), text.size() / 10);
        EXPECT_EQ(zstd_decompress(packed), text);
    }
    const Bytes noise = test::random_bytes(300000);
    EXPECT_EQ(zstd_decompress(zstd_compress(noise, 3)), noise);

    // Пустой вход даёт непустой кадр.
    const Bytes empty_frame = zstd_compress({}, 3);
    EXPECT_FALSE(empty_frame.empty());
    EXPECT_TRUE(zstd_decompress(empty_frame).empty());
}

TEST(Zstd, UnknownContentSize) {
    const Bytes data(3 * 1024 * 1024 + 17, std::byte{0x5A});
    // Потоковое сжатие без заявленного размера: размер кадра неизвестен.
    ZSTD_CCtx* cctx = ZSTD_createCCtx();
    ASSERT_NE(cctx, nullptr);
    Bytes packed(ZSTD_compressBound(data.size()));
    ZSTD_inBuffer in{data.data(), data.size() / 2, 0};
    ZSTD_outBuffer out{packed.data(), packed.size(), 0};
    ASSERT_EQ(ZSTD_isError(ZSTD_compressStream2(cctx, &out, &in, ZSTD_e_continue)), 0U);
    in = ZSTD_inBuffer{data.data() + data.size() / 2, data.size() - data.size() / 2, 0};
    size_t rest = 1;
    while (rest != 0) {
        rest = ZSTD_compressStream2(cctx, &out, &in, ZSTD_e_end);
        ASSERT_EQ(ZSTD_isError(rest), 0U);
    }
    ZSTD_freeCCtx(cctx);
    packed.resize(out.pos);
    ASSERT_EQ(ZSTD_getFrameContentSize(packed.data(), packed.size()), ZSTD_CONTENTSIZE_UNKNOWN);

    EXPECT_EQ(zstd_decompress(packed), data);
    EXPECT_EQ(zstd_decompress(packed, data.size()), data);
    EXPECT_THROW(zstd_decompress(packed, data.size() - 1), CorruptError);
}

TEST(Zstd, MaxSizeLimit) {
    const Bytes zeros(size_t{1024} * 1024, std::byte{0});
    const Bytes packed = zstd_compress(zeros, 9);
    EXPECT_EQ(zstd_decompress(packed, zeros.size()), zeros);
    EXPECT_THROW(zstd_decompress(packed, zeros.size() - 1), CorruptError);
    EXPECT_THROW(zstd_decompress(packed, 0), CorruptError);
    EXPECT_TRUE(zstd_decompress(zstd_compress({}, 3), 0).empty());
}

TEST(Zstd, CorruptInputIsRejected) {
    const Bytes data = test::random_bytes(100000);
    const Bytes packed = zstd_compress(data, 3);

    EXPECT_THROW(zstd_decompress({}), CorruptError);
    EXPECT_THROW(zstd_decompress(test::random_bytes(1000)), CorruptError);
    for (size_t i = 0; i < 4; ++i) {  // magic
        Bytes bad = packed;
        flip(bad, i);
        EXPECT_THROW(zstd_decompress(bad), CorruptError) << i;
    }
    for (const size_t keep : {size_t{1}, size_t{10}, packed.size() / 2, packed.size() - 1}) {
        EXPECT_THROW(zstd_decompress(ByteSpan{packed}.first(keep)), CorruptError) << keep;
    }
    Bytes trailing = packed;
    trailing.push_back(std::byte{0x42});
    EXPECT_THROW(zstd_decompress(trailing), CorruptError);
    // Неверный размер содержимого в заголовке кадра.
    const Bytes small = zstd_compress(as_bytes(std::string("hello hello hello")), 3);
    Bytes bad_size = small;
    flip(bad_size, 5);  // однобайтовое поле Frame_Content_Size после дескриптора
    EXPECT_THROW(zstd_decompress(bad_size), CorruptError);
}

}  // namespace
}  // namespace telega::crypto
