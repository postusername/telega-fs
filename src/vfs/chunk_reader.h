#pragma once

// Чтение залитых чанков: нужные сегменты через Store::read_range, проверка и
// расшифровка, RAM-кэш открытого текста (LRU по байтам, ключ chunk_id + сегмент).
//
// Чанк неизменен после заливки, а chunk_id не переиспользуется, поэтому кэш не нужно
// инвалидировать. Store вызывается без блокировок кэша; одновременных вызовов Store —
// не больше download_concurrency. Транзиентные ошибки повторяются с задержкой до
// read_timeout, затем, как и порча, — FsError(EIO). Срок один на весь read()
// (заголовок, данные, повторы, ожидание места среди скачиваний) и передаётся в
// Store::read_range_until.
//
// Readahead. readahead() получает каждое чтение файла вместе с дескриптором fh,
// определяет последовательность и просит Store::prefetch следующие байты файла. Окно начинается с
// `readahead`, удваивается при каждом попадании до `readahead_max` и сбрасывается
// непоследовательным чтением. Подсказка уходит, когда запрошенное раньше окно дошло до половины
// (иначе каждое чтение в 128 КиБ слало бы подсказку). Окно режется по чанкам файла: на каждый чанк
// один prefetch на зашифрованный диапазон до конца окна или чанка, расстояние (в чанках от позиции
// чтения) даёт приоритет. Заголовок чанка входит в диапазон, если диапазон начинается в первом
// мегабайте: тогда первое чтение чанка идёт одним вызовом. Readahead только заполняет дисковый
// кэш Store и ничего не расшифровывает и не кладёт в RAM-кэш.

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
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
    uint64_t readahead = uint64_t{4} << 20U;  // начальное окно readahead, 0 — выключен
    uint64_t readahead_max = uint64_t{32} << 20U;  // окно удваивается до этого значения
};

// Участок чанка, в котором лежит позиция файла (экстент): откуда readahead берёт байты.
struct ReadaheadTarget {
    meta::Chunk chunk;
    uint64_t chunk_off = 0;  // начало валидных байт позиции в открытом тексте чанка
    uint64_t len = 0;  // число валидных байт позиции
};
// Позиция файла → чанк, который надо читать вперёд. nullopt: позиция в overlay, инлайн, нули
// или экстента нет — такие позиции читать вперёд нечего.
using ReadaheadResolver = std::function<std::optional<ReadaheadTarget>(int64_t idx)>;

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

    // Чтение [off, off + len) файла дескриптором fh (fh == 0 — дескриптор неизвестен, readahead
    // не ведётся). Зовётся до самого чтения, чтобы скачивание шло параллельно с ним.
    // chunk_size — размер позиции файла, file_size — размер файла.
    void readahead(uint64_t fh, uint64_t off, uint64_t len, uint64_t file_size, uint64_t chunk_size,
                   const ReadaheadResolver& resolve);
    // Дескриптор закрыт.
    void forget_handle(uint64_t fh);

    [[nodiscard]] uint64_t cache_bytes() const { return cache_.bytes(); }

  private:
    struct RaState {
        bool have_last = false;
        uint64_t last_end = 0;
        uint64_t window = 0;  // 0 — последовательность ещё не подтверждена
        uint64_t ra_end = 0;  // до куда файл уже запрошен
    };

    // Проверяет заголовок по индексу и кладёт его в RAM-кэш; CorruptError превращает в EIO.
    crypto::ChunkHeader accept_header(const meta::Chunk& chunk, ByteSpan raw);
    using Deadline = std::chrono::steady_clock::time_point;
    crypto::ChunkHeader header(const meta::Chunk& chunk, const store::ChunkRef& ref,
                               Deadline deadline);
    void prefetch_window(uint64_t start, uint64_t end, uint64_t cur_idx, uint64_t chunk_size,
                         const ReadaheadResolver& resolve);
    // Store::read_range_until с повторами и семафором скачиваний; всё в пределах deadline.
    void fetch(const meta::Chunk& chunk, const store::ChunkRef& ref, uint64_t offset,
               MutableByteSpan out, Deadline deadline);
    [[noreturn]] static void corrupt(const meta::Chunk& chunk, const std::string& what);

    store::Store& store_;
    crypto::Key k_data_;
    FsUuid fs_uuid_;
    ReaderOptions opts_;
    PlainCache cache_;

    std::mutex ra_mu_;  // лист: состояния readahead
    std::unordered_map<uint64_t, RaState> ra_;

    std::mutex sem_mu_;  // лист: семафор скачиваний
    std::condition_variable sem_cv_;
    uint32_t in_flight_ = 0;
};

}  // namespace telega::vfs
