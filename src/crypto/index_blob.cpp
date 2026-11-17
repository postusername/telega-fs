#include "crypto/index_blob.h"

#include <sodium.h>
#include <zstd.h>

#include <algorithm>
#include <memory>
#include <stdexcept>
#include <string>

#include "crypto/detail.h"

namespace telega::crypto {

using detail::uc;

namespace {

// Раскладка открытого заголовка блоба (SPEC 6.3). Всё, что после body_size, — нули.
constexpr std::array<char, 8> kMagic{'T', 'G', 'F', 'S', 'I', 'D', 'X', '1'};
constexpr uint8_t kVersion = 1;
constexpr size_t kOffVersion = 8;
constexpr size_t kOffKind = 9;
constexpr size_t kOffReserved = 10;
constexpr size_t kReservedSize = 6;
constexpr size_t kOffFsUuid = 16;
constexpr size_t kOffGeneration = 32;
constexpr size_t kOffSnapshotGen = 40;
constexpr size_t kOffSnapshotMsgId = 48;
constexpr size_t kOffPrevMsgId = 56;
constexpr size_t kOffPrevHash = 64;
constexpr size_t kOffKdfAlg = 96;
constexpr size_t kOffKdfLimits = 97;  // opslimit(8) || memlimit(8) || salt(16)
constexpr size_t kOffWrappedMaster = kOffKdfLimits + detail::kKdfLimitsSize;        // 129
constexpr size_t kOffBodySize = kOffWrappedMaster + std::tuple_size_v<WrappedKey>;  // 201
constexpr size_t kOffPadding = kOffBodySize + 8;                                    // 209

static_assert(kOffBodySize == 201 && kOffPadding == 209);
static_assert(kOffPadding <= kIndexHeaderSize);

constexpr size_t kStreamHeaderSize = crypto_secretstream_xchacha20poly1305_HEADERBYTES;
constexpr size_t kStreamABytes = crypto_secretstream_xchacha20poly1305_ABYTES;
constexpr size_t kEncBlockSize = kIndexStreamBlock + kStreamABytes;

static_assert(kStreamHeaderSize == 24 && kStreamABytes == 17);
static_assert(Key::kSize == crypto_secretstream_xchacha20poly1305_KEYBYTES);

// Состояние потока содержит производный ключ — обнуляется при выходе.
struct StreamState {
    crypto_secretstream_xchacha20poly1305_state st{};
    StreamState() = default;
    StreamState(const StreamState&) = delete;
    StreamState& operator=(const StreamState&) = delete;
    ~StreamState() { sodium_memzero(&st, sizeof(st)); }
};

void put_i64_le(MutableByteSpan out, int64_t v) { put_u64_le(out, static_cast<uint64_t>(v)); }
int64_t get_i64_le(ByteSpan in) { return static_cast<int64_t>(get_u64_le(in)); }

struct ZstdDCtxDeleter {
    void operator()(ZSTD_DCtx* ctx) const noexcept { ZSTD_freeDCtx(ctx); }
};

}  // namespace

std::array<std::byte, kIndexHeaderSize> serialize_index_header(const IndexHeader& header) {
    std::array<std::byte, kIndexHeaderSize> raw{};
    const MutableByteSpan out{raw};
    std::transform(kMagic.begin(), kMagic.end(), raw.begin(),
                   [](char c) { return static_cast<std::byte>(c); });
    raw[kOffVersion] = std::byte{kVersion};
    raw[kOffKind] = static_cast<std::byte>(header.kind);
    std::copy(header.fs_uuid.begin(), header.fs_uuid.end(), raw.begin() + kOffFsUuid);
    put_u64_le(out.subspan(kOffGeneration, 8), header.generation);
    put_u64_le(out.subspan(kOffSnapshotGen, 8), header.snapshot_gen);
    put_i64_le(out.subspan(kOffSnapshotMsgId, 8), header.snapshot_msg_id);
    put_i64_le(out.subspan(kOffPrevMsgId, 8), header.prev_msg_id);
    std::copy(header.prev_hash.begin(), header.prev_hash.end(), raw.begin() + kOffPrevHash);
    raw[kOffKdfAlg] = static_cast<std::byte>(header.kdf.alg);
    detail::put_kdf_limits(out.subspan(kOffKdfLimits, detail::kKdfLimitsSize), header.kdf);
    std::copy(header.wrapped_master.begin(), header.wrapped_master.end(),
              raw.begin() + kOffWrappedMaster);
    put_u64_le(out.subspan(kOffBodySize, 8), header.body_size);
    return raw;
}

IndexHeader parse_index_header(ByteSpan raw) {
    if (raw.size() < kIndexHeaderSize) {
        throw CorruptError("index blob: shorter than header");
    }
    raw = raw.first(kIndexHeaderSize);
    if (!std::equal(kMagic.begin(), kMagic.end(), raw.begin(),
                    [](char c, std::byte b) { return static_cast<std::byte>(c) == b; })) {
        throw CorruptError("index blob: bad magic");
    }
    if (raw[kOffVersion] != std::byte{kVersion}) {
        throw CorruptError("index blob: unsupported version");
    }
    IndexHeader header;
    const auto kind = static_cast<uint8_t>(raw[kOffKind]);
    if (kind == static_cast<uint8_t>(BlobKind::snapshot)) {
        header.kind = BlobKind::snapshot;
    } else if (kind == static_cast<uint8_t>(BlobKind::delta)) {
        header.kind = BlobKind::delta;
    } else {
        throw CorruptError("index blob: unknown blob_kind");
    }
    if (!detail::all_zero(raw.subspan(kOffReserved, kReservedSize))) {
        throw CorruptError("index blob: non-zero reserved bytes");
    }
    if (!detail::all_zero(raw.subspan(kOffPadding))) {
        throw CorruptError("index blob: non-zero header padding");
    }
    std::copy_n(raw.begin() + kOffFsUuid, header.fs_uuid.size(), header.fs_uuid.begin());
    header.generation = get_u64_le(raw.subspan(kOffGeneration, 8));
    header.snapshot_gen = get_u64_le(raw.subspan(kOffSnapshotGen, 8));
    header.snapshot_msg_id = get_i64_le(raw.subspan(kOffSnapshotMsgId, 8));
    header.prev_msg_id = get_i64_le(raw.subspan(kOffPrevMsgId, 8));
    std::copy_n(raw.begin() + kOffPrevHash, header.prev_hash.size(), header.prev_hash.begin());
    header.kdf = detail::parse_kdf(static_cast<uint8_t>(raw[kOffKdfAlg]),
                                   raw.subspan(kOffKdfLimits, detail::kKdfLimitsSize));
    std::copy_n(raw.begin() + kOffWrappedMaster, header.wrapped_master.size(),
                header.wrapped_master.begin());
    header.body_size = get_u64_le(raw.subspan(kOffBodySize, 8));
    return header;
}

Bytes seal_index_blob(const Key& k_index, IndexHeader header, ByteSpan body) {
    if (body.empty()) {
        throw std::invalid_argument("seal_index_blob: empty body");
    }
    const size_t blocks = (body.size() + kIndexStreamBlock - 1) / kIndexStreamBlock;
    header.body_size = kStreamHeaderSize + body.size() + blocks * kStreamABytes;
    const auto raw_header = serialize_index_header(header);

    Bytes blob(kIndexHeaderSize + header.body_size);
    std::copy(raw_header.begin(), raw_header.end(), blob.begin());
    size_t out_pos = kIndexHeaderSize;

    StreamState state;
    crypto_secretstream_xchacha20poly1305_init_push(&state.st, uc(blob.data() + out_pos),
                                                    uc(k_index.data()));
    out_pos += kStreamHeaderSize;
    for (size_t b = 0; b < blocks; ++b) {
        const size_t in_pos = b * kIndexStreamBlock;
        const size_t len = std::min(kIndexStreamBlock, body.size() - in_pos);
        const bool last = b + 1 == blocks;
        // Весь заголовок — AD первого блока; у остальных блоков AD нет (SPEC 6.3).
        const bool first = b == 0;
        unsigned long long written = 0;
        crypto_secretstream_xchacha20poly1305_push(
            &state.st, uc(blob.data() + out_pos), &written, uc(body.data() + in_pos), len,
            first ? uc(raw_header.data()) : nullptr, first ? raw_header.size() : 0,
            last ? crypto_secretstream_xchacha20poly1305_TAG_FINAL
                 : crypto_secretstream_xchacha20poly1305_TAG_MESSAGE);
        out_pos += static_cast<size_t>(written);
    }
    if (out_pos != blob.size()) {
        throw std::logic_error("seal_index_blob: size mismatch");
    }
    return blob;
}

OpenedBlob open_index_blob(const Key& k_index, ByteSpan blob) {
    OpenedBlob opened;
    opened.header = parse_index_header(blob);
    const ByteSpan raw_header = blob.first(kIndexHeaderSize);
    const ByteSpan body = blob.subspan(kIndexHeaderSize);
    if (opened.header.body_size != body.size()) {
        throw CorruptError("index blob: body_size does not match blob size");
    }
    if (body.size() < kStreamHeaderSize + kStreamABytes + 1) {
        throw CorruptError("index blob: body too short");
    }

    StreamState state;
    if (crypto_secretstream_xchacha20poly1305_init_pull(&state.st, uc(body.data()),
                                                        uc(k_index.data())) != 0) {
        throw CorruptError("index blob: bad stream header");
    }
    opened.body.reserve(body.size());
    size_t pos = kStreamHeaderSize;
    bool first = true;
    while (pos < body.size()) {
        // Все блоки, кроме последнего, полные; последний короче или равен, но не пустой.
        const size_t enc_len = std::min(kEncBlockSize, body.size() - pos);
        if (enc_len <= kStreamABytes) {
            throw CorruptError("index blob: empty or truncated stream block");
        }
        const bool last = pos + enc_len == body.size();
        const size_t at = opened.body.size();
        opened.body.resize(at + enc_len - kStreamABytes);
        unsigned long long out_len = 0;
        unsigned char tag = 0;
        if (crypto_secretstream_xchacha20poly1305_pull(
                &state.st, uc(opened.body.data() + at), &out_len, &tag, uc(body.data() + pos),
                enc_len, first ? uc(raw_header.data()) : nullptr,
                first ? raw_header.size() : 0) != 0) {
            throw CorruptError("index blob: authentication failed");
        }
        const unsigned char want = last ? crypto_secretstream_xchacha20poly1305_TAG_FINAL
                                        : crypto_secretstream_xchacha20poly1305_TAG_MESSAGE;
        if (tag != want) {
            throw CorruptError(last ? "index blob: stream not finalized"
                                    : "index blob: unexpected stream tag");
        }
        pos += enc_len;
        first = false;
    }
    return opened;
}

Bytes zstd_compress(ByteSpan data, int level) {
    Bytes out(ZSTD_compressBound(data.size()));
    const size_t n = ZSTD_compress(out.data(), out.size(), data.data(), data.size(), level);
    if (ZSTD_isError(n) != 0U) {
        throw std::runtime_error(std::string("zstd compress: ") + ZSTD_getErrorName(n));
    }
    out.resize(n);
    return out;
}

Bytes zstd_decompress(ByteSpan data, uint64_t max_size) {
    const std::unique_ptr<ZSTD_DCtx, ZstdDCtxDeleter> ctx{ZSTD_createDCtx()};
    if (!ctx) {
        throw std::bad_alloc();
    }
    // Ёмкость не больше max_size + 1: лишний байт показывает превышение лимита.
    const uint64_t cap_limit = max_size == UINT64_MAX ? max_size : max_size + 1;
    const size_t step = ZSTD_DStreamOutSize();
    Bytes out;
    const unsigned long long hint = ZSTD_getFrameContentSize(data.data(), data.size());
    // Если размер кадра известен и укладывается в лимит, выделить сразу, но не больше
    // kMaxReserve: размер из заголовка кадра — ещё не проверенное обещание.
    constexpr uint64_t kMaxReserve = uint64_t{256} << 20U;
    if (hint != ZSTD_CONTENTSIZE_UNKNOWN && hint != ZSTD_CONTENTSIZE_ERROR && hint <= max_size) {
        out.reserve(static_cast<size_t>(std::min<uint64_t>(hint, kMaxReserve)));
    }

    ZSTD_inBuffer in{data.data(), data.size(), 0};
    size_t filled = 0;
    for (;;) {
        if (filled == out.size()) {
            const uint64_t grow = std::max<uint64_t>(step, out.size());
            out.resize(static_cast<size_t>(std::min<uint64_t>(cap_limit, out.size() + grow)));
        }
        ZSTD_outBuffer dst{out.data() + filled, out.size() - filled, 0};
        const size_t ret = ZSTD_decompressStream(ctx.get(), &dst, &in);
        if (ZSTD_isError(ret) != 0U) {
            throw CorruptError(std::string("zstd decompress: ") + ZSTD_getErrorName(ret));
        }
        filled += dst.pos;
        if (filled > max_size) {
            throw CorruptError("zstd decompress: output exceeds limit");
        }
        if (in.pos == in.size && ret == 0) {
            break;  // кадр закончен и весь вход прочитан
        }
        if (in.pos == in.size && dst.pos < dst.size) {
            throw CorruptError("zstd decompress: truncated frame");
        }
    }
    out.resize(filled);
    return out;
}

}  // namespace telega::crypto
