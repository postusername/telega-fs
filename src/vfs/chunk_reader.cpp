#include "vfs/chunk_reader.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <limits>
#include <string>
#include <thread>
#include <utility>

#include "meta/errors.h"

namespace telega::vfs {

namespace {

// Заголовок чанка лежит в том же кэше под этим номером «сегмента».
constexpr uint64_t kHeaderSlot = std::numeric_limits<uint64_t>::max();

}  // namespace

PlainCache::Value PlainCache::get(int64_t chunk_id, uint64_t segment) {
    const std::lock_guard lock(mu_);
    const auto it = map_.find({chunk_id, segment});
    if (it == map_.end()) {
        return nullptr;
    }
    lru_.splice(lru_.begin(), lru_, it->second);
    return it->second->second;
}

void PlainCache::put(int64_t chunk_id, uint64_t segment, Value value) {
    if (!value || value->size() > capacity_) {
        return;
    }
    const std::lock_guard lock(mu_);
    const Key key{chunk_id, segment};
    if (map_.contains(key)) {
        return;  // уже положил другой поток: содержимое то же
    }
    bytes_ += value->size();
    lru_.emplace_front(key, std::move(value));
    map_.emplace(key, lru_.begin());
    while (bytes_ > capacity_ && !lru_.empty()) {
        auto& last = lru_.back();
        bytes_ -= last.second->size();
        map_.erase(last.first);
        lru_.pop_back();
    }
}

uint64_t PlainCache::bytes() const {
    const std::lock_guard lock(mu_);
    return bytes_;
}

ChunkReader::ChunkReader(store::Store& store, const crypto::Key& k_data, const FsUuid& fs_uuid,
                         ReaderOptions options)
    : store_(store),
      k_data_(k_data),
      fs_uuid_(fs_uuid),
      opts_(options),
      cache_(options.cache_bytes) {
    opts_.concurrency = std::max<uint32_t>(opts_.concurrency, 1);
}

void ChunkReader::corrupt(const meta::Chunk& chunk, const std::string& what) {
    spdlog::error("chunk {} (msg {}) is corrupt: {}", chunk.chunk_id, chunk.msg_id, what);
    throw meta::FsError(EIO, "chunk " + std::to_string(chunk.chunk_id) + " is corrupt");
}

void ChunkReader::fetch(const meta::Chunk& chunk, const store::ChunkRef& ref, uint64_t offset,
                        MutableByteSpan out) {
    using Clock = std::chrono::steady_clock;
    const auto deadline = Clock::now() + opts_.read_timeout;
    auto delay = opts_.retry_initial;
    for (;;) {
        {
            std::unique_lock lock(sem_mu_);
            sem_cv_.wait(lock, [&] { return in_flight_ < opts_.concurrency; });
            ++in_flight_;
        }
        std::optional<store::StoreError> error;
        try {
            store_.read_range(ref, offset, out);
        } catch (const store::StoreError& e) {
            error = e;
        } catch (...) {
            {
                const std::lock_guard lock(sem_mu_);
                --in_flight_;
            }
            sem_cv_.notify_one();
            throw;
        }
        {
            const std::lock_guard lock(sem_mu_);
            --in_flight_;
        }
        sem_cv_.notify_one();
        if (!error) {
            return;
        }
        if (error->kind() != store::StoreErrorKind::transient) {
            spdlog::error("read of chunk {} (msg {}) failed: {}", chunk.chunk_id, chunk.msg_id,
                          error->what());
            throw meta::FsError(EIO, "chunk " + std::to_string(chunk.chunk_id) + " unreadable");
        }
        const auto now = Clock::now();
        if (now >= deadline) {
            spdlog::error("read of chunk {} (msg {}) timed out: {}", chunk.chunk_id, chunk.msg_id,
                          error->what());
            throw meta::FsError(EIO, "chunk " + std::to_string(chunk.chunk_id) + " timed out");
        }
        const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
        std::this_thread::sleep_for(std::min(delay, left + std::chrono::milliseconds(1)));
        delay = std::min(delay * 2, opts_.retry_max);
    }
}

crypto::ChunkHeader ChunkReader::header(const meta::Chunk& chunk, const store::ChunkRef& ref) {
    PlainCache::Value raw = cache_.get(chunk.chunk_id, kHeaderSlot);
    if (!raw) {
        auto buf = std::make_shared<Bytes>(crypto::kChunkHeaderSize);
        fetch(chunk, ref, 0, *buf);
        raw = buf;
        // В кэш кладём только проверенный заголовок (ниже).
        try {
            const crypto::ChunkHeader h = crypto::parse_chunk_header(*raw);
            if (h.plain_size != static_cast<uint64_t>(chunk.plain_size)) {
                corrupt(chunk, "plain_size in header differs from index");
            }
        } catch (const crypto::CorruptError& e) {
            corrupt(chunk, e.what());
        }
        cache_.put(chunk.chunk_id, kHeaderSlot, raw);
    }
    return crypto::parse_chunk_header(*raw);
}

void ChunkReader::read(const meta::Chunk& chunk, uint64_t off, MutableByteSpan out) {
    if (out.empty()) {
        return;
    }
    const auto plain_size = static_cast<uint64_t>(chunk.plain_size);
    if (off > plain_size || out.size() > plain_size - off) {
        corrupt(chunk, "read beyond plain_size");
    }
    if (static_cast<uint64_t>(chunk.enc_size) != crypto::chunk_encrypted_size(plain_size)) {
        corrupt(chunk, "enc_size does not match plain_size");
    }
    const crypto::SegmentSpan span = crypto::chunk_segments_for(plain_size, off, out.size());
    const store::ChunkRef ref{chunk.msg_id, chunk.remote_file_id.value_or("")};

    std::vector<PlainCache::Value> segs(span.count);
    for (uint64_t i = 0; i < span.count; ++i) {
        segs[i] = cache_.get(chunk.chunk_id, span.first + i);
    }
    // Недостающие подряд идущие сегменты — одним read_range.
    std::optional<crypto::ChunkHeader> hdr;
    for (uint64_t i = 0; i < span.count;) {
        if (segs[i]) {
            ++i;
            continue;
        }
        uint64_t j = i;
        while (j < span.count && !segs[j]) {
            ++j;
        }
        if (!hdr) {
            hdr = header(chunk, ref);
        }
        const uint64_t first = span.first + i;
        const uint64_t run_off = first * crypto::kSegmentSize;
        const uint64_t run_end = std::min(plain_size, (span.first + j) * crypto::kSegmentSize);
        const crypto::SegmentSpan run =
            crypto::chunk_segments_for(plain_size, run_off, run_end - run_off);
        Bytes enc(run.enc_len);
        fetch(chunk, ref, run.enc_offset, enc);
        Bytes plain;
        try {
            plain = crypto::decrypt_chunk_segments(
                k_data_, fs_uuid_, static_cast<uint64_t>(chunk.chunk_id), *hdr, first, enc);
        } catch (const crypto::CorruptError& e) {
            corrupt(chunk, e.what());
        }
        if (plain.size() != run_end - run_off) {
            corrupt(chunk, "decrypted size mismatch");
        }
        for (uint64_t k = i; k < j; ++k) {
            const uint64_t s_off = (k - i) * crypto::kSegmentSize;
            const uint64_t s_len = std::min<uint64_t>(crypto::kSegmentSize, plain.size() - s_off);
            auto seg = std::make_shared<const Bytes>(
                plain.begin() + static_cast<std::ptrdiff_t>(s_off),
                plain.begin() + static_cast<std::ptrdiff_t>(s_off + s_len));
            cache_.put(chunk.chunk_id, span.first + k, seg);
            segs[k] = std::move(seg);
        }
        i = j;
    }
    // Скопировать нужный диапазон из сегментов.
    size_t pos = 0;
    for (uint64_t i = 0; i < span.count && pos < out.size(); ++i) {
        const uint64_t seg_start = (span.first + i) * crypto::kSegmentSize;
        const uint64_t from = std::max(off + pos, seg_start) - seg_start;
        const Bytes& seg = *segs[i];
        if (from >= seg.size()) {
            corrupt(chunk, "segment shorter than expected");
        }
        const size_t n = std::min<size_t>(seg.size() - from, out.size() - pos);
        std::memcpy(out.data() + pos, seg.data() + from, n);
        pos += n;
    }
    if (pos != out.size()) {
        corrupt(chunk, "short read");
    }
}

}  // namespace telega::vfs
