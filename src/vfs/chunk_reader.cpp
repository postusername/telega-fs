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

// Диапазон, который начинается в первом мегабайте чанка, читается вместе с заголовком одним
// вызовом: отдельный вызов за заголовком стоил бы лишней задержки первого байта (E10).
constexpr uint64_t kHeaderMergeLimit = uint64_t{1} << 20U;

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
                        MutableByteSpan out, Deadline deadline) {
    using Clock = std::chrono::steady_clock;
    const auto timed_out = [&](const std::string& what) {
        spdlog::error("read of chunk {} (msg {}) timed out: {}", chunk.chunk_id, chunk.msg_id,
                      what);
        throw meta::FsError(EIO, "chunk " + std::to_string(chunk.chunk_id) + " timed out");
    };
    auto delay = opts_.retry_initial;
    for (;;) {
        // Срок один на всё чтение чанка: ни ожидание места среди скачиваний, ни повтор
        // не получают его заново, а повтор без остатка срока не начинается.
        {
            std::unique_lock lock(sem_mu_);
            if (!sem_cv_.wait_until(lock, deadline,
                                    [&] { return in_flight_ < opts_.concurrency; })) {
                timed_out("waiting for a download slot");
            }
            if (Clock::now() >= deadline) {
                timed_out("the read deadline has passed");
            }
            ++in_flight_;
        }
        std::optional<store::StoreError> error;
        try {
            store_.read_range_until(ref, offset, out, deadline);
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
            timed_out(error->what());
        }
        std::this_thread::sleep_until(std::min(now + delay, deadline));
        delay = std::min(delay * 2, opts_.retry_max);
    }
}

crypto::ChunkHeader ChunkReader::accept_header(const meta::Chunk& chunk, ByteSpan raw) {
    // В кэш кладём только проверенный заголовок.
    try {
        const crypto::ChunkHeader h = crypto::parse_chunk_header(raw);
        if (h.plain_size != static_cast<uint64_t>(chunk.plain_size)) {
            corrupt(chunk, "plain_size in header differs from index");
        }
        cache_.put(chunk.chunk_id, kHeaderSlot,
                   std::make_shared<const Bytes>(raw.begin(), raw.end()));
        return h;
    } catch (const crypto::CorruptError& e) {
        corrupt(chunk, e.what());
    }
}

crypto::ChunkHeader ChunkReader::header(const meta::Chunk& chunk, const store::ChunkRef& ref,
                                        Deadline deadline) {
    if (const PlainCache::Value raw = cache_.get(chunk.chunk_id, kHeaderSlot)) {
        return crypto::parse_chunk_header(*raw);
    }
    Bytes buf(crypto::kChunkHeaderSize);
    fetch(chunk, ref, 0, buf, deadline);
    return accept_header(chunk, buf);
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
    // read_timeout отсчитывается от постановки чтения и общий для заголовка, данных и повторов.
    const Deadline deadline = std::chrono::steady_clock::now() + opts_.read_timeout;

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
        const uint64_t first = span.first + i;
        const uint64_t run_off = first * crypto::kSegmentSize;
        const uint64_t run_end = std::min(plain_size, (span.first + j) * crypto::kSegmentSize);
        const crypto::SegmentSpan run =
            crypto::chunk_segments_for(plain_size, run_off, run_end - run_off);
        if (!hdr) {
            if (const PlainCache::Value raw = cache_.get(chunk.chunk_id, kHeaderSlot)) {
                hdr = crypto::parse_chunk_header(*raw);
            }
        }
        Bytes enc;
        ByteSpan enc_view;
        if (!hdr && run.enc_offset <= kHeaderMergeLimit) {
            // Заголовка в кэше нет, а диапазон рядом с ним: одним вызовом [0, конец диапазона).
            enc.resize(run.enc_offset + run.enc_len);
            fetch(chunk, ref, 0, enc, deadline);
            hdr = accept_header(chunk, ByteSpan{enc}.first(crypto::kChunkHeaderSize));
            enc_view = ByteSpan{enc}.subspan(run.enc_offset);
        } else {
            if (!hdr) {
                hdr = header(chunk, ref, deadline);
            }
            enc.resize(run.enc_len);
            fetch(chunk, ref, run.enc_offset, enc, deadline);
            enc_view = enc;
        }
        Bytes plain;
        try {
            plain = crypto::decrypt_chunk_segments(
                k_data_, fs_uuid_, static_cast<uint64_t>(chunk.chunk_id), *hdr, first, enc_view);
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

// ---- Readahead ---------------------------------------------------------------

void ChunkReader::forget_handle(uint64_t fh) {
    const std::lock_guard lock(ra_mu_);
    ra_.erase(fh);
}

void ChunkReader::readahead(uint64_t fh, uint64_t off, uint64_t len, uint64_t file_size,
                            uint64_t chunk_size, const ReadaheadResolver& resolve) {
    if (fh == 0 || len == 0 || opts_.readahead == 0 || chunk_size == 0) {
        return;
    }
    const uint64_t end = std::min(off + len, file_size);
    uint64_t start = 0;
    uint64_t target = 0;
    {
        const std::lock_guard lock(ra_mu_);
        RaState& st = ra_[fh];
        // Ядро читает параллельно и не по порядку, поэтому допуск — окно (но не меньше начального).
        const uint64_t tolerance = std::max(st.window, opts_.readahead);
        const bool sequential =
            st.have_last && off + tolerance >= st.last_end && off <= st.last_end + tolerance;
        st.have_last = true;
        st.last_end = end;
        if (!sequential) {
            st.window = 0;
            st.ra_end = 0;
            return;
        }
        st.window = st.window == 0
                        ? opts_.readahead
                        : std::min(st.window * 2, std::max(opts_.readahead_max, opts_.readahead));
        if (st.ra_end >= end + st.window / 2) {
            return;  // запрошенное раньше окно ещё далеко
        }
        start = std::max(st.ra_end, end);
        target = std::min(file_size, end + st.window);
        if (start >= target) {
            return;
        }
        st.ra_end = std::max(st.ra_end, target);
    }
    prefetch_window(start, target, off / chunk_size, chunk_size, resolve);
}

void ChunkReader::prefetch_window(uint64_t start, uint64_t end, uint64_t cur_idx,
                                  uint64_t chunk_size, const ReadaheadResolver& resolve) {
    for (uint64_t pos = start; pos < end;) {
        const uint64_t idx = pos / chunk_size;
        const uint64_t in_pos = pos % chunk_size;
        const uint64_t n = std::min(end - pos, chunk_size - in_pos);
        pos += n;
        std::optional<ReadaheadTarget> target;
        try {
            target = resolve(static_cast<int64_t>(idx));
        } catch (const std::exception& e) {
            spdlog::debug("readahead: cannot resolve position {}: {}", idx, e.what());
            continue;
        }
        if (!target || in_pos >= target->len) {
            continue;
        }
        const auto plain_size = static_cast<uint64_t>(target->chunk.plain_size);
        const uint64_t plain_off = target->chunk_off + in_pos;
        const uint64_t plain_len = std::min(n, target->len - in_pos);
        if (plain_off >= plain_size) {
            continue;
        }
        const crypto::SegmentSpan span =
            crypto::chunk_segments_for(plain_size, plain_off, plain_len);
        if (span.count == 0) {
            continue;
        }
        // Заголовок входит в диапазон, если тот начинается в первом мегабайте (см. read()).
        const uint64_t a = span.enc_offset <= kHeaderMergeLimit ? 0 : span.enc_offset;
        const uint64_t b = span.enc_offset + span.enc_len;
        const store::ChunkRef ref{target->chunk.msg_id, target->chunk.remote_file_id.value_or("")};
        const auto distance = static_cast<uint32_t>(std::min<uint64_t>(
            idx >= cur_idx ? idx - cur_idx : 0, std::numeric_limits<uint32_t>::max()));
        try {
            store_.prefetch(ref, a, b - a, distance);
        } catch (const std::exception& e) {
            spdlog::debug("readahead: prefetch of chunk {} failed: {}", target->chunk.chunk_id,
                          e.what());
        }
    }
}

}  // namespace telega::vfs
