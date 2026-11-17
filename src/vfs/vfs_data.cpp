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

Bytes Vfs::read(int64_t ino, uint64_t off, size_t size) {
    check_alive();
    const std::shared_lock lock(stripe(ino));
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
    Bytes out(static_cast<size_t>(n));
    std::optional<Bytes> inline_data;
    if (inode.inline_len) {
        inline_data = meta_.read([&](Db& db) { return meta::get_inline(db, ino); });
    }
    const auto s = static_cast<uint64_t>(chunk_size_);
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
                const uint64_t valid = std::min(len, static_cast<uint64_t>(ext->len) - in_chunk);
                const auto chunk = chunk_row(ext->chunk_id);
                if (!chunk) {
                    spdlog::error("extent {}:{} refers to missing chunk {}", ino, idx,
                                  ext->chunk_id);
                    throw FsError(EIO, "missing chunk");
                }
                reader_->read(*chunk, static_cast<uint64_t>(ext->chunk_off) + in_chunk,
                              dst.first(static_cast<size_t>(valid)));
            }
        }
        pos += len;
    }
    return out;
}

// --- запись (SPEC 8.1) ---

void Vfs::materialize(int64_t ino, int64_t idx, const Inode& inode,
                      const std::optional<std::pair<uint64_t, uint64_t>>& write_range,
                      std::optional<Bytes>& inline_data) {
    // Сначала получить старое содержимое (может упасть с EIO), потом создавать файл.
    Bytes content;
    if (inode.inline_len && idx == 0) {
        if (!inline_data) {
            inline_data = meta_.read([&](Db& db) { return meta::get_inline(db, ino); });
        }
        if (inline_data) {
            content = *inline_data;
        }
    } else if (const auto ext =
                   meta_.read([&](Db& db) { return meta::get_extent(db, ino, idx); })) {
        const auto len = static_cast<uint64_t>(ext->len);
        // Запись перекрывает все валидные байты позиции — старое содержимое не нужно.
        const bool covered = write_range && write_range->first == 0 && write_range->second >= len;
        if (!covered) {
            const auto chunk = chunk_row(ext->chunk_id);
            if (!chunk) {
                spdlog::error("extent {}:{} refers to missing chunk {}", ino, idx, ext->chunk_id);
                throw FsError(EIO, "missing chunk");
            }
            // Read-modify-write: валидные байты чанка; дальше len — нули (разреженный хвост).
            content.resize(static_cast<size_t>(len));
            reader_->read(*chunk, static_cast<uint64_t>(ext->chunk_off), content);
        }
    }
    const Fd fd = Fd::open(overlay_->path_of({ino, idx}), O_RDWR | O_CREAT | O_TRUNC, 0600);
    if (!content.empty()) {
        pwrite_full(fd.get(), content, 0);
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

    std::vector<ChunkWrite> writes;
    std::vector<int64_t> created;  // staging-файлы, созданные этим вызовом
    std::optional<Bytes> inline_data;
    const int64_t new_size = std::max<int64_t>(effective_size(inode), static_cast<int64_t>(end));
    try {
        // Инлайн-файл, в который пишут дальше позиции 0: его данные переезжают в overlay
        // позиции 0, иначе при заливке остальных позиций они бы потерялись.
        if (inode.inline_len && first >= 1 && !overlay_->chunk({ino, 0})) {
            materialize(ino, 0, inode, std::nullopt, inline_data);
            created.push_back(0);
            writes.push_back({0, 0, 0});
        }
        for (int64_t idx = first; idx <= last; ++idx) {
            const uint64_t base = static_cast<uint64_t>(idx) * s;
            const uint64_t from = std::max(off, base) - base;
            const uint64_t to = std::min(end, base + s) - base;
            if (!overlay_->chunk({ino, idx})) {
                materialize(ino, idx, inode, std::make_pair(from, to), inline_data);
                created.push_back(idx);
            }
            writes.push_back({idx, from, to});
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
        // Новые staging-файлы без строк не нужны (иначе их уберёт следующий старт).
        for (const int64_t idx : created) {
            std::error_code ec;
            std::filesystem::remove(overlay_->path_of({ino, idx}), ec);
        }
        throw;
    }

    // Потоковая заливка: запись в idx при полностью записанном idx - 1.
    if (!read_only() && inode.parent && new_size > params_.inline_max) {
        for (int64_t idx = std::max<int64_t>(first, 1); idx <= last; ++idx) {
            const ChunkKey prev{ino, idx - 1};
            if (overlay_->fully_covered(prev)) {
                enqueue_upload(prev, false);
            }
        }
    }
    return data.size();
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
    wait_targets(targets);
    commit_or_throw();
}

}  // namespace telega::vfs
