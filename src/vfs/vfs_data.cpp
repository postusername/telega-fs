// Vfs: пути данных — запись в overlay, чтение, truncate, fsync (SPEC 6.2, 8.1, 8.2).

#include <fcntl.h>
#include <spdlog/spdlog.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <limits>

#include "common/files.h"
#include "meta/errors.h"
#include "vfs/vfs.h"

namespace telega::vfs {

using meta::Db;
using meta::FsError;
using meta::Inode;
using meta::InodeKind;

namespace {

// Предел размера файла: смещения и размеры держим в int64_t с запасом на сложение.
constexpr uint64_t kMaxFileSize = uint64_t{1} << 60U;

int64_t now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

}  // namespace

std::optional<meta::Chunk> Vfs::chunk_row(int64_t chunk_id) {
    return meta_.read([&](Db& db) { return meta::get_chunk(db, chunk_id); });
}

void Vfs::read_staging(const ChunkKey& key, uint64_t off, MutableByteSpan out) {
    const Fd fd = Fd::open(overlay_->path_of(key), O_RDONLY);
    const size_t got = pread_full(fd.get(), out, off);
    // Разреженный staging-файл короче позиции: дальше — нули.
    std::fill(out.begin() + static_cast<std::ptrdiff_t>(got), out.end(), std::byte{0});
}

// --- чтение (SPEC 8.2) ---

Bytes Vfs::read(int64_t ino, uint64_t off, size_t size, uint64_t fh) {
    check_alive();
    // Полоса держится только на метаданные, staging и инлайн: сетевое чтение залитого чанка идёт
    // без неё. std::shared_mutex (glibc) предпочитает читателей, и перекрывающиеся медленные
    // чтения не пускали бы write, fsync и применение чанков и пакетов на этой полосе. Чанк в
    // индексе неизменяем, а его сообщение живёт ещё retain_generations коммитов после того, как
    // refcount упал до нуля (SPEC 7.4), так что чтение, обогнавшее запись, вернёт прежние данные
    // (допустимо: чтение, конкурентное с записью, не обязано видеть её, SPEC 9).
    struct Remote {
        meta::Chunk chunk;
        uint64_t off = 0;
        MutableByteSpan dst;
    };
    std::vector<Remote> remote;
    Bytes out;
    {
        std::shared_lock lock(stripe(ino));
        const Inode inode = get_inode_or_throw(ino);
        if (inode.kind == InodeKind::dir) {
            throw FsError(EISDIR, "read of a directory");
        }
        if (inode.kind != InodeKind::file) {
            throw FsError(EINVAL, "read of a symlink");
        }
        const auto file_size = static_cast<uint64_t>(effective_size(inode));
        if (off >= file_size || size == 0) {
            return {};
        }
        const uint64_t n = std::min<uint64_t>(size, file_size - off);
        out.assign(static_cast<size_t>(n), std::byte{0});
        std::optional<Bytes> inline_data;
        if (inode.inline_len) {
            inline_data = meta_.read([&](Db& db) { return meta::get_inline(db, ino); });
        }
        const auto s = static_cast<uint64_t>(chunk_size_);
        // Readahead (SPEC 8.2) идёт до самого чтения: скачивание вперёд — параллельно с ним.
        if (fh != 0) {
            reader_->readahead(
                fh, off, n, file_size, s, [&](int64_t idx) -> std::optional<ReadaheadTarget> {
                    if (overlay_->chunk({ino, idx}) || (inline_data && idx == 0)) {
                        return std::nullopt;
                    }
                    const auto ext =
                        meta_.read([&](Db& db) { return meta::get_extent(db, ino, idx); });
                    if (!ext) {
                        return std::nullopt;
                    }
                    const auto chunk = chunk_row(ext->chunk_id);
                    if (!chunk) {
                        return std::nullopt;
                    }
                    return ReadaheadTarget{*chunk, static_cast<uint64_t>(ext->chunk_off),
                                           static_cast<uint64_t>(ext->len)};
                });
        }
        uint64_t pos = off;
        while (pos < off + n) {
            const auto idx = static_cast<int64_t>(pos / s);
            const uint64_t in_chunk = pos % s;
            const uint64_t len = std::min(s - in_chunk, off + n - pos);
            const MutableByteSpan dst{out.data() + (pos - off), static_cast<size_t>(len)};
            const ChunkKey key{ino, idx};
            if (overlay_->chunk(key)) {
                read_staging(key, in_chunk, dst);
            } else if (inline_data && idx == 0) {
                // Инлайн-файл: данные из индекса, дальше их длины — нули.
                std::fill(dst.begin(), dst.end(), std::byte{0});
                if (in_chunk < inline_data->size()) {
                    const size_t k = std::min<size_t>(dst.size(), inline_data->size() - in_chunk);
                    std::memcpy(dst.data(), inline_data->data() + in_chunk, k);
                }
            } else {
                std::fill(dst.begin(), dst.end(), std::byte{0});
                const auto ext = meta_.read([&](Db& db) { return meta::get_extent(db, ino, idx); });
                if (ext && in_chunk < static_cast<uint64_t>(ext->len)) {
                    const uint64_t valid =
                        std::min(len, static_cast<uint64_t>(ext->len) - in_chunk);
                    const auto chunk = chunk_row(ext->chunk_id);
                    if (!chunk) {
                        spdlog::error("extent {}:{} refers to missing chunk {}", ino, idx,
                                      ext->chunk_id);
                        throw FsError(EIO, "missing chunk");
                    }
                    remote.push_back({*chunk, static_cast<uint64_t>(ext->chunk_off) + in_chunk,
                                      dst.first(static_cast<size_t>(valid))});
                }
            }
            pos += len;
        }
        lock.unlock();
    }
    for (const Remote& r : remote) {
        reader_->read(r.chunk, r.off, r.dst);
    }
    return out;
}

// --- запись (SPEC 8.1) ---

Vfs::Seed Vfs::seed_for(int64_t ino, int64_t idx, const Inode& inode,
                        const std::optional<std::pair<uint64_t, uint64_t>>& write_range,
                        std::optional<Bytes>& inline_data) {
    // Сначала узнать старое содержимое (может упасть с EIO), потом создавать файл.
    Seed seed;
    if (inode.inline_len && idx == 0) {
        if (!inline_data) {
            inline_data = meta_.read([&](Db& db) { return meta::get_inline(db, ino); });
        }
        if (inline_data) {
            seed.from_inline = true;
            seed.len = inline_data->size();
        }
    } else if (const auto ext =
                   meta_.read([&](Db& db) { return meta::get_extent(db, ino, idx); })) {
        const auto len = static_cast<uint64_t>(ext->len);
        // Запись перекрывает все валидные байты позиции — старое содержимое не нужно.
        const bool covered = write_range && write_range->first == 0 && write_range->second >= len;
        if (!covered) {
            seed.chunk = chunk_row(ext->chunk_id);
            if (!seed.chunk) {
                spdlog::error("extent {}:{} refers to missing chunk {}", ino, idx, ext->chunk_id);
                throw FsError(EIO, "missing chunk");
            }
            // Read-modify-write: валидные байты чанка; дальше len — нули (разреженный хвост).
            seed.len = len;
            seed.chunk_off = static_cast<uint64_t>(ext->chunk_off);
        }
    }
    return seed;
}

void Vfs::materialize(int64_t ino, int64_t idx, const Seed& seed,
                      const std::optional<Bytes>& inline_data) {
    const auto path = overlay_->path_of({ino, idx});
    const Fd fd = Fd::open(path, O_RDWR | O_CREAT | O_TRUNC, 0600);
    try {
        if (seed.from_inline) {
            if (inline_data && !inline_data->empty()) {
                pwrite_full(fd.get(), *inline_data, 0);
            }
        } else if (seed.chunk && seed.len > 0) {
            // RMW (SPEC 8.1): расшифровка прямо в staging-файл кусками, без буфера на чанк.
            const uint64_t seg = std::max<uint64_t>(opts_.rmw_segment, 1);
            Bytes buf(static_cast<size_t>(std::min(seg, seed.len)));
            uint64_t peak = rmw_peak_.load();
            while (buf.size() > peak && !rmw_peak_.compare_exchange_weak(peak, buf.size())) {
            }
            for (uint64_t pos = 0; pos < seed.len; pos += buf.size()) {
                const auto n = static_cast<size_t>(std::min<uint64_t>(buf.size(), seed.len - pos));
                const MutableByteSpan part{buf.data(), n};
                reader_->read(*seed.chunk, seed.chunk_off + pos, part);
                pwrite_full(fd.get(), part, pos);
            }
        }
    } catch (...) {
        std::error_code ec;
        std::filesystem::remove(path, ec);
        throw;
    }
}

size_t Vfs::write(int64_t ino, uint64_t off, ByteSpan data) {
    check_writable();
    if (data.empty()) {
        return 0;
    }
    if (off >= kMaxFileSize || data.size() > kMaxFileSize - off) {
        throw FsError(EFBIG, "file too large");
    }
    const auto deadline = std::chrono::steady_clock::now() + opts_.staging_full_timeout;
    for (;;) {
        uint64_t shortage = 0;
        if (write_locked(ino, off, data, shortage)) {
            // Выше порога: запечатать самые старые без seal_delay (SPEC 8.1, 8.3). Вне полосы.
            relieve_pressure(0);
            return data.size();
        }
        // Места не хватило: ждать вне полосы inode, иначе применение пакета, которое освобождает
        // место, встало бы на той же полосе (SPEC 6.5, 8.3).
        wait_for_space(shortage, deadline);
    }
}

bool Vfs::write_locked(int64_t ino, uint64_t off, ByteSpan data, uint64_t& shortage) {
    const int64_t now = now_ns();
    const std::unique_lock lock(stripe(ino));
    const Inode inode = get_inode_or_throw(ino);
    if (inode.kind == InodeKind::dir) {
        throw FsError(EISDIR, "write to a directory");
    }
    if (inode.kind != InodeKind::file) {
        throw FsError(EINVAL, "write to a symlink");
    }
    const auto s = static_cast<uint64_t>(chunk_size_);
    const uint64_t end = off + data.size();
    const auto first = static_cast<int64_t>(off / s);
    const auto last = static_cast<int64_t>((end - 1) / s);

    struct Step {
        int64_t idx = 0;
        uint64_t from = 0;
        uint64_t to = 0;
        bool create = false;  // staging-файл создаётся этим вызовом
        Seed seed;
        uint64_t prev_len = 0;  // учтённая длина существующего файла
        uint64_t new_len = 0;   // длина после записи
    };
    std::vector<Step> steps;
    std::optional<Bytes> inline_data;
    // План: что создаётся, чем наполняется и сколько места нужно (SPEC 8.3). Растёт только то,
    // что реально увеличивает staging-файл: truncate вверх и дыры места не занимают.
    uint64_t need = 0;
    const auto plan = [&](int64_t idx, uint64_t from, uint64_t to,
                          const std::optional<std::pair<uint64_t, uint64_t>>& range) {
        Step step;
        step.idx = idx;
        step.from = from;
        step.to = to;
        const ChunkKey key{ino, step.idx};
        if (overlay_->chunk(key)) {
            step.prev_len = overlay_->staged(key);
            step.new_len = std::max(step.prev_len, step.to);
        } else {
            step.create = true;
            step.seed = seed_for(ino, step.idx, inode, range, inline_data);
            step.new_len = std::max(step.seed.len, step.to);
        }
        need += step.new_len - step.prev_len;
        steps.push_back(std::move(step));
    };
    // Инлайн-файл, в который пишут дальше позиции 0: его данные переезжают в overlay позиции 0,
    // иначе при заливке остальных позиций они бы потерялись.
    if (inode.inline_len && first >= 1 && !overlay_->chunk({ino, 0})) {
        plan(0, 0, 0, std::nullopt);
    }
    for (int64_t idx = first; idx <= last; ++idx) {
        const uint64_t base = static_cast<uint64_t>(idx) * s;
        const uint64_t from = std::max(off, base) - base;
        const uint64_t to = std::min(end, base + s) - base;
        plan(idx, from, to, std::make_pair(from, to));
    }

    auto granted = budget_->try_acquire_user(need);
    if (!granted) {
        shortage = need;
        return false;
    }
    StagingBudget::Lease lease = std::move(*granted);

    std::vector<ChunkWrite> writes;
    std::vector<int64_t> created;  // staging-файлы, созданные этим вызовом
    std::vector<std::pair<int64_t, uint64_t>> grown;  // существующие файлы и их учтённая длина
    const int64_t new_size = std::max<int64_t>(effective_size(inode), static_cast<int64_t>(end));
    try {
        for (const Step& st : steps) {
            if (st.create) {
                materialize(ino, st.idx, st.seed, inline_data);
                created.push_back(st.idx);
            } else {
                grown.emplace_back(st.idx, st.prev_len);
            }
            writes.push_back({st.idx, st.from, st.to, st.new_len});
        }
        // Данные — в staging (без fsync: его делает fsync файла).
        for (const ChunkWrite& w : writes) {
            if (w.from == w.to) {
                continue;
            }
            const uint64_t base = static_cast<uint64_t>(w.idx) * s;
            const Fd fd = Fd::open(overlay_->path_of({ino, w.idx}), O_WRONLY);
            pwrite_full(fd.get(), data.subspan(base + w.from - off, w.to - w.from), w.from);
        }
        // Строки dirty_chunks и dirty_attrs — долговечно до возврата из write.
        overlay_->commit_write(ino, writes, meta::DirtyAttrs{ino, new_size, now}, now);
    } catch (...) {
        // Новые staging-файлы без строк не нужны (иначе их уберёт следующий старт). Существующие
        // возвращаются к учтённой длине: хвост за ней не содержал данных.
        for (const int64_t idx : created) {
            std::error_code ec;
            std::filesystem::remove(overlay_->path_of({ino, idx}), ec);
        }
        for (const auto& [idx, prev] : grown) {
            std::error_code ec;
            const auto path = overlay_->path_of({ino, idx});
            if (std::filesystem::file_size(path, ec) > prev && !ec) {
                std::filesystem::resize_file(path, prev, ec);
            }
        }
        throw;  // lease вернёт занятое место
    }
    lease.keep();  // место теперь учитывает Overlay (длины staging-файлов)

    // Потоковая заливка: запись в idx при полностью записанном idx - 1.
    if (!read_only() && inode.parent && new_size > params_.inline_max) {
        for (int64_t idx = std::max<int64_t>(first, 1); idx <= last; ++idx) {
            const ChunkKey prev{ino, idx - 1};
            if (overlay_->fully_covered(prev)) {
                enqueue_upload(prev, false);
            }
        }
    }
    return true;
}

// --- truncate (SPEC 6.2) ---

void Vfs::clamp_index(Db& db, int64_t ino, int64_t size) {
    const int64_t gen = base_gen();
    const int64_t full = size / chunk_size_;
    const int64_t rem = size % chunk_size_;
    meta::drop_extents_from(db, ino, rem == 0 ? full : full + 1, gen);
    if (rem != 0) {
        const auto e = meta::get_extent(db, ino, full);
        if (e && e->len > rem) {
            meta::set_extent_len(db, ino, full, rem);
        }
    }
    const auto data = meta::get_inline(db, ino);
    if (data && static_cast<int64_t>(data->size()) > size) {
        const ByteSpan prefix{data->data(), static_cast<size_t>(size)};
        if (!meta::set_inline(db, ino, prefix, gen)) {
            throw std::logic_error("shrinking inline data must fit the budget");
        }
    }
}

void Vfs::truncate_locked(int64_t ino, const Inode& inode, int64_t new_size) {
    if (static_cast<uint64_t>(new_size) > kMaxFileSize) {
        throw FsError(EFBIG, "file too large");
    }
    const int64_t now = now_ns();
    const int64_t old_size = effective_size(inode);
    // Индекс надо обрезать, если файл уменьшается или индекс по размеру длиннее нового
    // (раньше при грязных чанках inodes.size не понижался до конца заливки).
    const bool shrink = new_size < old_size;
    const bool lower_index_size = inode.size > new_size;
    if (shrink) {
        // Порядок для сбоя в любой точке: сначала новый размер в dirty_attrs (если они есть:
        // иначе после падения между drop_from и записью индекса старт увидел бы старый размер
        // без чанков и записал его в индекс), затем позиции за концом и частичная последняя
        // в overlay, затем индекс. При сбое посреди старт Vfs (recover) доделает остальное по
        // dirty_attrs: уберёт позиции за концом, обрежет staging и индекс.
        if (overlay_->attrs(ino)) {
            overlay_->set_attrs(meta::DirtyAttrs{ino, new_size, now});
        }
        const int64_t full = new_size / chunk_size_;
        const int64_t rem = new_size % chunk_size_;
        overlay_->drop_from(ino, rem == 0 ? full : full + 1);
        if (rem != 0 && overlay_->chunk({ino, full})) {
            const ChunkKey key{ino, full};
            const Fd fd = Fd::open(overlay_->path_of(key), O_WRONLY);
            const off_t cur = ::lseek(fd.get(), 0, SEEK_END);
            if (cur > rem && ::ftruncate(fd.get(), static_cast<off_t>(rem)) != 0) {
                throw SysError(errno, "ftruncate staging");
            }
            overlay_->cut(key, rem, now);
        }
    }
    const bool dirty = overlay_->has_chunks(ino);
    if (dirty) {
        overlay_->set_attrs(meta::DirtyAttrs{ino, new_size, now});
    }
    meta_.write([&](Db& db) {
        if (shrink || lower_index_size) {
            clamp_index(db, ino, new_size);
        }
        if (!dirty) {
            meta::AttrUpdate u;
            u.size = new_size;
            u.mtime_ns = now;
            u.ctime_ns = now;
            meta::update_attrs(db, ino, u);
        } else if (lower_index_size) {
            // Индекс, закоммитенный до конца заливки, не должен показывать старый размер с
            // обнулённым хвостом: размер понижается в той же транзакции, что и обрезка.
            meta::AttrUpdate u;
            u.size = new_size;
            meta::update_attrs(db, ino, u);
        }
    });
    if (!dirty && overlay_->attrs(ino)) {
        overlay_->clear_attrs(ino);
    }
}

// --- fsync (SPEC 7.3, 8.1) ---

void Vfs::fsync(int64_t ino) {
    check_alive();
    if (opts_.read_only) {
        return;  // --ro: писать было нечего
    }
    if (committer_.read_only()) {
        throw FsError(EIO, "file system is read-only after a commit conflict: cannot commit");
    }
    {
        // Локальная долговечность staging: на случай, если загрузка не дойдёт.
        const std::shared_lock lock(stripe(ino));
        for (const int64_t idx : overlay_->indices(ino)) {
            const Fd fd = Fd::open(overlay_->path_of({ino, idx}), O_RDONLY);
            if (::fdatasync(fd.get()) != 0) {
                throw SysError(errno, "fdatasync staging");
            }
        }
    }
    const auto targets = seal_file(ino, SealMode::explicit_request);
    // fsync файла из открытого пакета закрывает пакет; если объект уже в заливаемом пакете,
    // закрывать нечего: ждём его заливку (SPEC 6.5).
    packs_->close_for(ino);
    wait_targets(targets);
    commit_or_throw();
}

}  // namespace telega::vfs
