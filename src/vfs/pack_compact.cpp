// Уплотнение пакетов (SPEC 6.5), см. pack_compact.h.

#include "vfs/pack_compact.h"

#include <fcntl.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <map>
#include <system_error>
#include <thread>

#include "common/files.h"
#include "crypto/chunk.h"
#include "meta/index_ops.h"
#include "vfs/chunk_reader.h"

namespace telega::vfs {

using meta::Db;

namespace {

int64_t now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

constexpr size_t kPiece = size_t{1} << 20U;

class Compactor {
  public:
    Compactor(const CompactContext& ctx, const CompactOptions& opts)
        : ctx_(ctx),
          opts_(opts),
          params_(ctx.meta.read([](Db& db) { return meta::read_params(db); })),
          reader_(
              ctx.store, ctx.keys.data, params_.fs_uuid,
              ReaderOptions{uint64_t{8} << 20U, std::max<uint32_t>(opts.download_concurrency, 1),
                            opts.read_timeout, opts.retry_initial, opts.retry_max}) {}

    CompactResult run() {
        CompactResult result;
        const auto ppm =
            static_cast<int64_t>(std::llround(std::clamp(opts_.ratio, 0.0, 1.0) * 1e6));
        std::vector<meta::Extent> extents;
        ctx_.meta.read([&](Db& db) {
            for (meta::Chunk& c : meta::list_compaction_candidates(db, ppm)) {
                const int64_t id = c.chunk_id;
                sources_.emplace(id, std::move(c));
            }
            extents = meta::list_compaction_extents(db, ppm);
        });
        result.packs_selected = sources_.size();
        if (sources_.empty()) {
            return result;
        }
        spdlog::info("compact: {} pack(s) below ratio {}, {} live object(s)", sources_.size(),
                     opts_.ratio, extents.size());
        const int64_t target = std::min(params_.pack_target, params_.chunk_size);
        std::vector<meta::Extent> batch;
        int64_t total = 0;
        for (const meta::Extent& e : extents) {
            if (!batch.empty() && total + e.len > params_.chunk_size) {
                repack(batch, total, result);
                batch.clear();
                total = 0;
                if (result.aborted) {
                    break;
                }
            }
            batch.push_back(e);
            total += e.len;
            if (total >= target) {
                repack(batch, total, result);
                batch.clear();
                total = 0;
            }
            if (result.aborted) {
                break;
            }
        }
        if (!batch.empty() && !result.aborted) {
            repack(batch, total, result);
        }
        if (result.new_packs > 0 || result.objects_moved > 0) {
            ctx_.committer.commit_now();
            result.committed = true;
        }
        return result;
    }

  private:
    // Собрать, залить и применить один новый пакет из живых объектов batch.
    void repack(const std::vector<meta::Extent>& batch, int64_t total, CompactResult& result) {
        // Место под открытый текст и зашифрованный файл (SPEC 8.3): целиком и до всего остального.
        std::optional<StagingBudget::Lease> lease;
        if (ctx_.budget != nullptr) {
            const auto plain_max = static_cast<uint64_t>(total);
            lease =
                ctx_.budget->acquire_internal(plain_max + crypto::chunk_encrypted_size(plain_max),
                                              [this] { return ctx_.abort && ctx_.abort(); });
            if (!lease) {
                result.aborted = true;
                return;
            }
        }
        const int64_t chunk_id = ctx_.meta.write([](Db& db) { return meta::alloc_chunk_id(db); });
        const auto plain = ctx_.tmp_dir / ("pack-" + std::to_string(chunk_id));
        const auto enc = ctx_.tmp_dir / ("chunk-" + std::to_string(chunk_id));
        auto cleanup = [&] {
            std::error_code ec;
            std::filesystem::remove(plain, ec);
            std::filesystem::remove(enc, ec);
        };
        try {
            // Сборка: объекты подряд, смещение каждого — сумма длин предыдущих.
            std::vector<int64_t> offsets;
            offsets.reserve(batch.size());
            crypto::Hash32 mac{};
            {
                const Fd out = Fd::open(plain, O_RDWR | O_CREAT | O_TRUNC, 0600);
                Bytes buf(kPiece);
                int64_t off = 0;
                for (const meta::Extent& e : batch) {
                    const meta::Chunk& src = sources_.at(e.chunk_id);
                    for (int64_t done = 0; done < e.len;) {
                        const auto n = static_cast<size_t>(std::min<int64_t>(e.len - done, kPiece));
                        reader_.read(src, static_cast<uint64_t>(e.chunk_off + done),
                                     MutableByteSpan{buf.data(), n});
                        pwrite_full(out.get(), ByteSpan{buf.data(), n},
                                    static_cast<uint64_t>(off + done));
                        done += static_cast<int64_t>(n);
                    }
                    offsets.push_back(off);
                    off += e.len;
                }
                mac = crypto::content_mac(ctx_.keys.mac, out.get(), static_cast<uint64_t>(total));
                crypto::encrypt_chunk_file(ctx_.keys.data, params_.fs_uuid,
                                           static_cast<uint64_t>(chunk_id), out.get(),
                                           static_cast<uint64_t>(total), enc);
            }
            const auto enc_size =
                static_cast<int64_t>(crypto::chunk_encrypted_size(static_cast<uint64_t>(total)));
            if (lease) {
                std::error_code rec;
                std::filesystem::remove(plain, rec);  // открытого текста больше нет
                lease->shrink_to(static_cast<uint64_t>(enc_size));
            }

            // Заливка: журнал, чтобы сбой не оставил сироту без следа (SPEC 7.2, шаг 7).
            ctx_.state.upload_started(chunk_id, now_ns());
            store::ChunkRef ref;
            try {
                ref = put_with_retries(enc);
            } catch (const store::StoreError&) {
                if (!transient_exhausted_) {
                    ctx_.state.remove_upload(chunk_id);
                }
                throw;
            }
            ctx_.state.upload_finished(chunk_id, ref.msg_id);
            cleanup();
            if (lease) {
                lease->release();
            }

            // Применение: экстент должен остаться таким, каким прочитан.
            size_t moved = 0;
            ctx_.meta.write([&](Db& db) {
                std::vector<size_t> ok;
                for (size_t i = 0; i < batch.size(); ++i) {
                    const meta::Extent& e = batch[i];
                    const auto cur = meta::get_extent(db, e.ino, e.idx);
                    if (cur && cur->chunk_id == e.chunk_id && cur->chunk_off == e.chunk_off &&
                        cur->len == e.len) {
                        ok.push_back(i);
                    }
                }
                if (ok.empty()) {
                    return;
                }
                const auto gen = static_cast<int64_t>(ctx_.committer.base_generation());
                meta::NewChunk nc;
                nc.chunk_id = chunk_id;
                nc.kind = meta::ChunkKind::pack;
                nc.msg_id = ref.msg_id;
                if (!ref.remote_file_id.empty()) {
                    nc.remote_file_id = ref.remote_file_id;
                }
                nc.plain_size = total;
                nc.enc_size = enc_size;
                nc.content_mac.assign(mac.begin(), mac.end());
                meta::insert_chunk(db, nc, gen);
                for (const size_t i : ok) {
                    meta::put_extent(db,
                                     meta::Extent{batch[i].ino, batch[i].idx, chunk_id, offsets[i],
                                                  batch[i].len},
                                     gen);
                    result.bytes_moved += static_cast<uint64_t>(batch[i].len);
                }
                moved = ok.size();
            });
            result.objects_moved += moved;
            result.objects_skipped += batch.size() - moved;
            if (moved == 0) {
                // Все экстенты изменились: новый пакет не нужен.
                const std::array<int64_t, 1> ids{ref.msg_id};
                ctx_.store.delete_messages(ids);
            } else {
                ++result.new_packs;
                ctx_.committer.chunk_uploaded();
            }
            ctx_.state.remove_upload(chunk_id);
        } catch (...) {
            cleanup();
            throw;
        }
    }

    store::ChunkRef put_with_retries(const std::filesystem::path& enc) {
        auto delay = opts_.retry_initial;
        for (uint32_t attempt = 1;; ++attempt) {
            try {
                return ctx_.store.put_chunk(enc);
            } catch (const store::StoreError& e) {
                if (e.kind() != store::StoreErrorKind::transient) {
                    throw;  // постоянная ошибка: сообщения нет, вызывающий убирает запись журнала
                }
                if (attempt >= std::max<uint32_t>(opts_.max_upload_attempts, 1)) {
                    transient_exhausted_ = true;  // таймаут мог и залить: журнал оставляем
                    throw;
                }
                spdlog::warn("compact: upload failed, will retry: {}", e.what());
                std::this_thread::sleep_for(delay);
                delay = std::min(delay * 2, opts_.retry_max);
            }
        }
    }

    bool transient_exhausted_ = false;
    CompactContext ctx_;
    CompactOptions opts_;
    meta::IndexParams params_;
    ChunkReader reader_;
    std::map<int64_t, meta::Chunk> sources_;
};

}  // namespace

CompactResult compact_packs(const CompactContext& ctx, const CompactOptions& options) {
    crypto::init();
    return Compactor(ctx, options).run();
}

}  // namespace telega::vfs
