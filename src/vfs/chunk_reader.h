#pragma once

// Чтение залитых чанков (SPEC 5, 8.2): нужные сегменты через Store::read_range, проверка и
// расшифровка, RAM-кэш открытого текста (LRU по байтам, ключ chunk_id + сегмент).
//
// Чанк неизменен после заливки, а chunk_id не переиспользуется, поэтому кэш не нужно
// инвалидировать. Store вызывается без блокировок кэша; одновременных вызовов Store —
// не больше download_concurrency. Транзиентные ошибки повторяются с задержкой до
// read_timeout (SPEC 10.2), затем, как и порча, — FsError(EIO).

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "common/bytes.h"
#include "crypto/chunk.h"
#include "crypto/keys.h"
#include "meta/index_ops.h"
#include "store/store.h"

namespace telega::vfs {

struct ReaderOptions {
    uint64_t cache_bytes = uint64_t{64} << 20U;
    uint32_t concurrency = 4;
    std::chrono::milliseconds read_timeout{60'000};
    std::chrono::milliseconds retry_initial{1'000};
    std::chrono::milliseconds retry_max{60'000};
};

// LRU открытого текста. Потокобезопасен (свой мьютекс — последний в порядке блокировок).
class PlainCache {
  public:
    explicit PlainCache(uint64_t capacity_bytes) : capacity_(capacity_bytes) {}

    using Value = std::shared_ptr<const Bytes>;
    Value get(int64_t chunk_id, uint64_t segment);
    void put(int64_t chunk_id, uint64_t segment, Value value);
    [[nodiscard]] uint64_t bytes() const;

  private:
    struct Key {
        int64_t chunk_id = 0;
        uint64_t segment = 0;
        bool operator==(const Key&) const = default;
    };
    struct KeyHash {
        size_t operator()(const Key& k) const noexcept {
            return std::hash<int64_t>{}(k.chunk_id) * 1'000'003U ^ std::hash<uint64_t>{}(k.segment);
        }
    };
    using List = std::list<std::pair<Key, Value>>;

    uint64_t capacity_;
    mutable std::mutex mu_;
    List lru_;  // голова — самые свежие
    std::unordered_map<Key, List::iterator, KeyHash> map_;
    uint64_t bytes_ = 0;
};

class ChunkReader {
  public:
    ChunkReader(store::Store& store, const crypto::Key& k_data, const FsUuid& fs_uuid,
                ReaderOptions options);

    // Прочитать [off, off + out.size()) открытого текста чанка. Выход за plain_size, порча,
    // несовпадение plain_size заголовка с индексом, исчерпание read_timeout — FsError(EIO).
    void read(const meta::Chunk& chunk, uint64_t off, MutableByteSpan out);

    [[nodiscard]] uint64_t cache_bytes() const { return cache_.bytes(); }

  private:
    crypto::ChunkHeader header(const meta::Chunk& chunk, const store::ChunkRef& ref);
    // Store::read_range с повторами и семафором скачиваний.
    void fetch(const meta::Chunk& chunk, const store::ChunkRef& ref, uint64_t offset,
               MutableByteSpan out);
    [[noreturn]] static void corrupt(const meta::Chunk& chunk, const std::string& what);

    store::Store& store_;
    crypto::Key k_data_;
    FsUuid fs_uuid_;
    ReaderOptions opts_;
    PlainCache cache_;

    std::mutex sem_mu_;  // лист: семафор скачиваний
    std::condition_variable sem_cv_;
    uint32_t in_flight_ = 0;
};

}  // namespace telega::vfs
