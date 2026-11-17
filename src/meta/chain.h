#pragma once

// Чтение и проверка цепочки блобов индекса из канала (SPEC 6.3, 7.2 шаг 4).
//
// Порядок цепочки защищает только prev_hash и подряд идущие generation, а не сами патчи
// (SPEC 6.3), поэтому fetch_chain проверяет у каждого элемента всё: тег (open_index_blob),
// fs_uuid, вид, snapshot_gen и snapshot_msg_id, generation = snapshot_gen + позиция,
// prev_msg_id и prev_hash = BLAKE2b-256 предыдущего блоба целиком, список цепочки в теле
// каждой дельты. Любое расхождение — ChainCorrupt.

#include <cstdint>
#include <stdexcept>
#include <vector>

#include "common/bytes.h"
#include "crypto/index_blob.h"
#include "crypto/keys.h"
#include "store/store.h"

namespace telega::meta {

class ChainCorrupt : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

struct ChainBlob {
    int64_t msg_id = 0;
    crypto::IndexHeader header;  // аутентифицирован
    crypto::Hash32 hash{};       // BLAKE2b-256 блоба целиком
    int64_t blob_size = 0;
    // Снимок: файл index.sqlite. Дельта: patchset (без списка цепочки).
    Bytes payload;
    std::vector<int64_t> listed_chain;  // дельта: msg_id предыдущих элементов из её тела
};

// Расшифровать и проверить один блоб (тег, fs_uuid), распаковать тело.
ChainBlob open_chain_blob(const crypto::Key& k_index, const FsUuid& fs_uuid, int64_t msg_id,
                          ByteSpan blob);

// Скачать и проверить всю цепочку головы. head_blob — уже скачанный блоб головы.
// Результат — элементы по порядку от снимка до головы. Ошибки Store, кроме not_found
// (это пропуск элемента, ChainCorrupt), пробрасываются как есть.
std::vector<ChainBlob> fetch_chain(store::Store& store, const crypto::Key& k_index,
                                   const FsUuid& fs_uuid, int64_t head_msg_id, ByteSpan head_blob);

}  // namespace telega::meta
