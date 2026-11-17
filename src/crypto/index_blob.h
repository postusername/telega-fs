#pragma once

// Блоб индекса: открытый заголовок 256 байт и зашифрованное тело (SPEC 6.3).
// Здесь только формат и шифрование. Тело (zstd от снимка или от chain || patchset)
// собирает и разбирает CommitManager.

#include <array>
#include <cstddef>
#include <cstdint>

#include "common/bytes.h"
#include "crypto/keys.h"

namespace telega::crypto {

inline constexpr size_t kIndexHeaderSize = 256;
inline constexpr size_t kIndexStreamBlock = size_t{1024} * 1024;  // блок открытого текста потока

enum class BlobKind : uint8_t { snapshot = 1, delta = 2 };

struct IndexHeader {
    BlobKind kind = BlobKind::snapshot;
    FsUuid fs_uuid{};
    uint64_t generation = 0;
    uint64_t snapshot_gen = 0;
    int64_t snapshot_msg_id = 0;
    int64_t prev_msg_id = 0;
    Hash32 prev_hash{};
    KdfParams kdf;
    WrappedKey wrapped_master{};
    uint64_t body_size = 0;  // заполняет seal_index_blob

    bool operator==(const IndexHeader&) const = default;
};

std::array<std::byte, kIndexHeaderSize> serialize_index_header(const IndexHeader& header);

// Разобрать заголовок (не меньше kIndexHeaderSize байт, берутся первые 256).
// Проверяет magic, version, допустимый blob_kind и kdf_alg, нулевые reserved и padding.
// Аутентичность здесь не проверяется: она подтверждается только open_index_blob.
IndexHeader parse_index_header(ByteSpan raw);

// Собрать блоб: заголовок (body_size вычисляется) + secretstream от body.
// body не пустой (zstd-выход всегда не пуст), иначе std::invalid_argument.
Bytes seal_index_blob(const Key& k_index, IndexHeader header, ByteSpan body);

struct OpenedBlob {
    IndexHeader header;
    Bytes body;  // открытое тело (то, что передали в seal_index_blob)
};

// Проверить и расшифровать блоб целиком: заголовок, body_size == blob.size() - 256,
// теги всех блоков, TAG_FINAL ровно у последнего, отсутствие лишних байт.
// Любое расхождение — CorruptError.
OpenedBlob open_index_blob(const Key& k_index, ByteSpan blob);

// zstd (SPEC 6.3, 7.1): уровень 9 для снимка, 3 для дельты.
Bytes zstd_compress(ByteSpan data, int level);
// Ошибка распаковки — CorruptError. max_size — защита от бомбы (по умолчанию 16 ГиБ).
Bytes zstd_decompress(ByteSpan data, uint64_t max_size = uint64_t{16} << 30U);

}  // namespace telega::crypto
