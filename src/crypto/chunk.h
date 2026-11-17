#pragma once

// Формат зашифрованного чанка (SPEC 5).

#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>

#include "common/bytes.h"
#include "crypto/keys.h"

namespace telega::crypto {

inline constexpr size_t kChunkHeaderSize = 40;
inline constexpr uint64_t kSegmentSize = 65536;
inline constexpr uint64_t kSegmentTagSize = 16;
inline constexpr uint64_t kEncSegmentSize = kSegmentSize + kSegmentTagSize;  // 65552

struct ChunkHeader {
    std::array<std::byte, 16> nonce{};
    uint64_t plain_size = 0;
};

// Размер зашифрованного файла чанка: 40 + plain_size + 16 * число сегментов.
// plain_size == 0 — ноль сегментов, файл состоит из одного заголовка.
uint64_t chunk_encrypted_size(uint64_t plain_size);
uint64_t chunk_segment_count(uint64_t plain_size);

// Какие сегменты покрывают диапазон открытого текста [offset, offset + len), обрезанный
// по plain_size, и какой диапазон зашифрованного файла их содержит.
// count == 0, если диапазон пуст после обрезки.
struct SegmentSpan {
    uint64_t first = 0;  // номер первого сегмента
    uint64_t count = 0;  // число сегментов
    uint64_t enc_offset = 0;  // смещение в зашифрованном файле (с учётом заголовка)
    uint64_t enc_len = 0;  // длина диапазона в зашифрованном файле
    uint64_t plain_offset = 0;  // = first * kSegmentSize
};
SegmentSpan chunk_segments_for(uint64_t plain_size, uint64_t offset, uint64_t len);

// Зашифровать plain_size байт файла in_fd (с позиции 0, потоково, по сегменту) в out_path.
// Если in_fd короче plain_size, недостающее — нули (staging-файлы разреженные).
// Новый случайный chunk_nonce на каждый вызов. out_path создаётся (O_EXCL не нужен) и fsync-ится.
ChunkHeader encrypt_chunk_file(const Key& k_data, const FsUuid& fs_uuid, uint64_t chunk_id,
                               int in_fd, uint64_t plain_size,
                               const std::filesystem::path& out_path);

// Разобрать заголовок (ровно kChunkHeaderSize байт). Неверный magic, version или
// ненулевой reserved — CorruptError.
ChunkHeader parse_chunk_header(ByteSpan raw);

// Расшифровать подряд идущие сегменты, начиная с first_segment. enc — ровно их зашифрованные
// байты (как задаёт chunk_segments_for). Возвращает открытый текст этих сегментов.
// Ошибка тега, неверная длина или выход за число сегментов — CorruptError.
// AD сегмента i: fs_uuid || u64(chunk_id) || u64(i) || u8(is_last) || u64(plain_size).
Bytes decrypt_chunk_segments(const Key& k_data, const FsUuid& fs_uuid, uint64_t chunk_id,
                             const ChunkHeader& header, uint64_t first_segment, ByteSpan enc);

}  // namespace telega::crypto
