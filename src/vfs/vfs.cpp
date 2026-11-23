// Vfs: конструктор, общие помощники и операции пространства имён (SPEC 6.2, 9).

#include "vfs/vfs.h"

#include <fcntl.h>
#include <linux/fs.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <system_error>

#include "common/files.h"
#include "crypto/chunk.h"
#include "crypto/keys.h"
#include "meta/errors.h"

namespace telega::vfs {

using meta::Db;
using meta::FsError;
using meta::Inode;
using meta::InodeKind;

namespace {

int64_t now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

void set_timespec(struct timespec& ts, int64_t ns) {
    ts.tv_sec = static_cast<time_t>(ns / 1'000'000'000);
    ts.tv_nsec = static_cast<long>(ns % 1'000'000'000);
    if (ts.tv_nsec < 0) {
        ts.tv_nsec += 1'000'000'000;
        --ts.tv_sec;
    }
}

}  // namespace

void fill_stat(const Attr& attr, struct stat& st) {
    st = {};
    st.st_ino = static_cast<ino_t>(attr.ino);
    mode_t type = S_IFREG;
    if (attr.kind == InodeKind::dir) {
        type = S_IFDIR;
    } else if (attr.kind == InodeKind::symlink) {
        type = S_IFLNK;
    }
    st.st_mode = type | static_cast<mode_t>(attr.mode & meta::kPermMask);
    st.st_nlink = attr.nlink;
    st.st_uid = attr.uid;
    st.st_gid = attr.gid;
    st.st_size = static_cast<off_t>(attr.size);
    st.st_blocks = static_cast<blkcnt_t>(attr.blocks);
    st.st_blksize = static_cast<blksize_t>(attr.blksize);
    set_timespec(st.st_atim, attr.atime_ns);
    set_timespec(st.st_mtim, attr.mtime_ns);
    set_timespec(st.st_ctim, attr.ctime_ns);
}

int to_errno(const std::exception& e) noexcept {
    if (const auto* fs = dynamic_cast<const FsError*>(&e)) {
        return fs->err();
    }
    if (const auto* sys = dynamic_cast<const SysError*>(&e)) {
        if (sys->err() == ENOSPC || sys->err() == EDQUOT) {
            return sys->err();
        }
    }
    return EIO;
}

// --- конструктор и завершение ---

Vfs::Vfs(meta::MetaStore& meta, meta::StateStore& state, store::Store& store,
         meta::Committer& committer, crypto::SubKeys keys, VfsPaths paths, VfsOptions options)
    : meta_(meta),
      state_(state),
      store_(store),
      committer_(committer),
      keys_(std::move(keys)),
      paths_(std::move(paths)),
      opts_(std::move(options)) {
    crypto::init();
    params_ = meta_.read([](Db& db) { return meta::read_params(db); });
    chunk_size_ = params_.chunk_size;
    if (chunk_size_ <= 0) {
        throw std::runtime_error("bad chunk_size in index");
    }
    opts_.upload_concurrency = std::max<uint32_t>(opts_.upload_concurrency, 1);
    std::filesystem::create_directories(paths_.staging);
    std::filesystem::create_directories(paths_.tmp);
    // Учёт staging (SPEC 8.3). Резерв заливки R = число одновременных put_chunk × размер
    // зашифрованного чанка, плюс открытый текст одного собираемого пакета (до chunk_size):
    // сборка и шифрование пакетов идут по одному (PackWriter::stage_mu_), а в tmp/ при этом
    // лежат и открытый текст, и зашифрованный файл.
    {
        const uint64_t enc = crypto::chunk_encrypted_size(static_cast<uint64_t>(chunk_size_));
        const uint64_t reserve =
            uint64_t{upload_workers()} * enc + static_cast<uint64_t>(chunk_size_);
        StagingConfig sc;
        sc.limit = opts_.staging_limit;
        sc.high = opts_.staging_high_watermark != 0 ? opts_.staging_high_watermark
                                                    : opts_.staging_limit / 4 * 3;
        sc.reserve = reserve;
        if (sc.limit <= reserve) {
            throw std::invalid_argument(
                "staging_limit (" + std::to_string(sc.limit) +
                " bytes) must be larger than the upload reserve " + std::to_string(reserve) +
                " bytes (concurrent uploads x encrypted chunk + one pack); raise staging_limit "
                "or lower upload_concurrency");
        }
        if (sc.high > sc.limit) {
            throw std::invalid_argument("staging_high_watermark must not exceed staging_limit");
        }
        budget_ = std::make_unique<StagingBudget>(sc, opts_.staging_probe);
    }
    overlay_ = std::make_unique<Overlay>(state_, paths_.staging, chunk_size_);
    overlay_->set_budget(budget_.get());
    reader_ = std::make_unique<ChunkReader>(
        store_, keys_.data, params_.fs_uuid,
        ReaderOptions{opts_.plain_cache, opts_.download_concurrency, opts_.read_timeout,
                      opts_.retry_initial, opts_.retry_max, opts_.readahead, opts_.readahead_max});
    PackConfig pc;
    pc.chunk_size = chunk_size_;
    pc.pack_threshold = params_.pack_threshold;
    pc.pack_target = params_.pack_target;
    pc.max_age = opts_.pack_max_age;
    pc.retry_initial = opts_.retry_initial;
    pc.retry_max = opts_.retry_max;
    PackHooks ph;
    ph.stripe = [this](int64_t ino) -> std::shared_mutex& { return stripe(ino); };
    ph.base_gen = [this] { return base_gen(); };
    ph.read_only = [this] { return read_only(); };
    ph.stopping = [this] { return stop_flag_.load(); };
    ph.backoff = [this](std::chrono::milliseconds& delay) { return backoff(delay); };
    ph.note_error = [this](const std::string& what) { note_error(what); };
    ph.submit = [this](uint64_t seq) {
        Job job;
        job.kind = Job::Kind::pack;
        job.seq = seq;
        push_job(job);
    };
    ph.reseal_explicit = [this](int64_t ino) {
        Job job;
        job.kind = Job::Kind::seal;
        job.ino = ino;
        job.explicit_close = true;
        push_job(job);
    };
    ph.resolve_empty = [this](const ChunkKey& key, uint64_t incarnation) {
        resolve_empty_position(key, incarnation);
    };
    ph.chunk_uploaded = [this] {
        uploaded_.fetch_add(1);
        committer_.chunk_uploaded();
    };
    ph.acquire_tmp = [this](uint64_t bytes) {
        return budget_->acquire_internal(bytes, [this] { return stop_flag_.load(); });
    };
    ph.probe = opts_.pack_probe;
    packs_ = std::make_unique<PackWriter>(meta_, state_, store_, *overlay_, keys_, params_.fs_uuid,
                                          paths_.tmp, pc, std::move(ph));
    overlay_->set_pack_hook([this](int64_t ino) { packs_->on_dropped(ino); });
    recover();
    packs_->load(!read_only());
    start_threads();
}

Vfs::~Vfs() {
    try {
        stop_threads();
    } catch (const std::exception& e) {
        spdlog::error("vfs: stopping threads failed: {}", e.what());
    }
}

// --- помощники ---

std::shared_mutex& Vfs::stripe(int64_t ino) {
    return stripes_[static_cast<size_t>(static_cast<uint64_t>(ino) % kStripes)];
}

void Vfs::check_alive() const {
    if (fatal_.load()) {
        throw FsError(EIO, "filesystem failed, remount required");
    }
}

bool Vfs::read_only() const { return opts_.read_only || committer_.read_only(); }

void Vfs::check_writable() const {
    check_alive();
    if (read_only()) {
        throw FsError(EROFS, "read-only filesystem");
    }
}

int64_t Vfs::base_gen() const { return static_cast<int64_t>(committer_.base_generation()); }

int64_t Vfs::effective_size(const Inode& inode) const {
    const auto a = overlay_->attrs(inode.ino);
    return a ? a->size : inode.size;
}

Attr Vfs::make_attr(Db& db, const Inode& inode, const std::optional<meta::DirtyAttrs>& dirty) {
    Attr a;
    a.ino = inode.ino;
    a.kind = inode.kind;
    a.mode = inode.mode;
    a.uid = inode.uid;
    a.gid = inode.gid;
    a.size = inode.size;
    a.atime_ns = inode.atime_ns;
    a.mtime_ns = inode.mtime_ns;
    a.ctime_ns = inode.ctime_ns;
    switch (inode.kind) {
        case InodeKind::dir:
            a.nlink = static_cast<uint32_t>(2 + meta::count_subdirs(db, inode.ino));
            break;
        case InodeKind::file:
            // Удалённый, но ещё открытый файл: ссылок на него больше нет.
            a.nlink = (inode.ino != meta::kRootIno && !inode.parent) ? 0 : 1;
            break;
        case InodeKind::symlink:
            a.nlink = 1;
            break;
    }
    if (inode.kind == InodeKind::file && dirty) {
        a.size = dirty->size;
        a.mtime_ns = dirty->mtime_ns;
        a.ctime_ns = std::max(a.ctime_ns, dirty->mtime_ns);
    }
    a.blocks = (a.size + kBlockSize - 1) / kBlockSize * (kBlockSize / 512);
    return a;
}

Inode Vfs::get_inode_or_throw(int64_t ino) {
    auto inode = meta_.read([&](Db& db) { return meta::get_inode(db, ino); });
    if (!inode) {
        throw FsError(ENOENT, "no inode " + std::to_string(ino));
    }
    return std::move(*inode);
}

void Vfs::add_lookup(int64_t ino, uint64_t n) {
    const std::lock_guard lock(refs_mu_);
    refs_[ino].lookups += n;
}

void Vfs::remember_lookup(int64_t ino) { add_lookup(ino, 1); }

bool Vfs::in_use(int64_t ino) {
    const std::lock_guard lock(refs_mu_);
    const auto it = refs_.find(ino);
    return it != refs_.end() && (it->second.lookups > 0 || it->second.opens > 0);
}

void Vfs::touch_dir(Db& db, int64_t dir, int64_t now) {
    meta::AttrUpdate u;
    u.mtime_ns = now;
    u.ctime_ns = now;
    meta::update_attrs(db, dir, u);
}

bool Vfs::remove_victim(Db& db, const Inode& victim) {
    if (victim.kind == InodeKind::file && in_use(victim.ino)) {
        meta::detach_inode(db, victim.ino);
        const std::lock_guard lock(refs_mu_);
        detached_.insert(victim.ino);
        return false;
    }
    meta::delete_inode(db, victim.ino, base_gen());
    return victim.kind == InodeKind::file;
}

void Vfs::drop_file_data(int64_t ino) { overlay_->drop_inode(ino); }

void Vfs::maybe_reap(int64_t ino) {
    if (ino == meta::kRootIno || in_use(ino) || fatal_.load()) {
        return;
    }
    const auto inode = meta_.read([&](Db& db) { return meta::get_inode(db, ino); });
    if (!inode || inode->parent || inode->kind != InodeKind::file) {
        return;
    }
    if (read_only()) {
        return;  // удалит следующее монтирование (SPEC 7.2, шаг 8)
    }
    const std::unique_lock lock(stripe(ino));
    if (in_use(ino)) {
        return;
    }
    const bool deleted = meta_.write([&](Db& db) {
        const auto cur = meta::get_inode(db, ino);
        if (!cur || cur->parent) {
            return false;
        }
        meta::delete_inode(db, ino, base_gen());
        return true;
    });
    if (deleted) {
        drop_file_data(ino);
    }
    const std::lock_guard refs(refs_mu_);
    detached_.erase(ino);
}

// --- пространство имён ---

Attr Vfs::lookup(int64_t parent, ByteSpan name) {
    check_alive();
    // dirty_attrs файла надо прочитать до снимка индекса (см. make_attr), а какой это файл,
    // известно только из снимка. Поэтому, пока dirty_attrs вообще есть, первый проход узнаёт
    // файл, dirty_attrs читаются вне снимка, второй проход берёт новый снимок и проверяет,
    // что имя всё ещё указывает на тот же inode. Без dirty_attrs хватает одного прохода.
    const bool any_dirty = overlay_->has_attrs();
    int64_t known_ino = 0;  // файл, чьи dirty_attrs уже прочитаны
    std::optional<meta::DirtyAttrs> dirty;
    for (;;) {
        std::optional<Attr> got = meta_.read([&](Db& db) -> std::optional<Attr> {
            if (name.size() > meta::kNameMax) {
                throw FsError(ENAMETOOLONG, "name too long");
            }
            const auto p = meta::get_inode(db, parent);
            if (!p) {
                throw FsError(ENOENT, "no parent");
            }
            if (p->kind != InodeKind::dir) {
                throw FsError(ENOTDIR, "parent is not a directory");
            }
            const auto child = meta::lookup(db, parent, name);
            if (!child) {
                throw FsError(ENOENT, "no entry");
            }
            if (child->kind != InodeKind::file || !any_dirty) {
                return make_attr(db, *child, std::nullopt);
            }
            if (child->ino != known_ino) {
                known_ino = child->ino;
                return std::nullopt;
            }
            return make_attr(db, *child, dirty);
        });
        if (got) {
            add_lookup(got->ino, 1);
            return *got;
        }
        dirty = overlay_->attrs(known_ino);
    }
}

void Vfs::forget(int64_t ino, uint64_t nlookup) {
    bool gone = false;
    {
        const std::lock_guard lock(refs_mu_);
        auto it = refs_.find(ino);
        if (it == refs_.end()) {
            return;
        }
        it->second.lookups -= std::min(it->second.lookups, nlookup);
        if (it->second.lookups == 0 && it->second.opens == 0) {
            refs_.erase(it);
            gone = true;
        }
    }
    if (gone) {
        try {
            maybe_reap(ino);
        } catch (const std::exception& e) {
            // forget не имеет ответа; inode уберёт следующее монтирование.
            spdlog::error("vfs: reaping inode {} failed: {}", ino, e.what());
        }
    }
}

Attr Vfs::getattr(int64_t ino) {
    check_alive();
    const auto dirty = overlay_->attrs(ino);
    return meta_.read([&](Db& db) {
        const auto inode = meta::get_inode(db, ino);
        if (!inode) {
            throw FsError(ENOENT, "no inode");
        }
        return make_attr(db, *inode, inode->kind == InodeKind::file ? dirty : std::nullopt);
    });
}

Attr Vfs::setattr(int64_t ino, const SetAttr& change) {
    check_writable();
    const int64_t now = now_ns();
    const std::unique_lock lock(stripe(ino));
    const Inode inode = get_inode_or_throw(ino);
    if (change.size) {
        if (inode.kind == InodeKind::dir) {
            throw FsError(EISDIR, "truncate of a directory");
        }
        if (inode.kind != InodeKind::file) {
            throw FsError(EINVAL, "truncate of a symlink");
        }
        if (*change.size < 0) {
            throw FsError(EINVAL, "negative size");
        }
        truncate_locked(ino, inode, *change.size);
    }
    std::optional<int64_t> mtime = change.mtime_ns;
    if (change.mtime_now) {
        mtime = now;
    }
    // mtime файла с грязными атрибутами живёт в dirty_attrs (иначе их mtime перекрыл бы новый).
    if (mtime && inode.kind == InodeKind::file) {
        if (auto d = overlay_->attrs(ino)) {
            d->mtime_ns = *mtime;
            overlay_->set_attrs(*d);
            mtime.reset();
        }
    }
    meta::AttrUpdate u;
    u.mode = change.mode;
    u.uid = change.uid;
    u.gid = change.gid;
    u.atime_ns = change.atime_now ? std::optional<int64_t>(now) : change.atime_ns;
    u.mtime_ns = mtime;
    u.ctime_ns = now;
    meta_.write([&](Db& db) { meta::update_attrs(db, ino, u); });
    return getattr(ino);
}

Attr Vfs::mkdir(int64_t parent, ByteSpan name, uint32_t mode, uint32_t uid, uint32_t gid) {
    check_writable();
    const int64_t now = now_ns();
    Attr attr = meta_.write([&](Db& db) {
        meta::NewInode n;
        n.parent = parent;
        n.name.assign(name.begin(), name.end());
        n.kind = InodeKind::dir;
        n.mode = mode;
        n.uid = uid;
        n.gid = gid;
        n.now_ns = now;
        const int64_t ino = meta::insert_inode(db, n);
        touch_dir(db, parent, now);
        return make_attr(db, *meta::get_inode(db, ino), std::nullopt);
    });
    add_lookup(attr.ino, 1);
    return attr;
}

void Vfs::rmdir(int64_t parent, ByteSpan name) {
    check_writable();
    const int64_t now = now_ns();
    meta_.write([&](Db& db) {
        const auto p = meta::get_inode(db, parent);
        if (!p) {
            throw FsError(ENOENT, "no parent");
        }
        if (p->kind != InodeKind::dir) {
            throw FsError(ENOTDIR, "parent is not a directory");
        }
        const auto child = meta::lookup(db, parent, name);
        if (!child) {
            throw FsError(ENOENT, "no entry");
        }
        if (child->kind != InodeKind::dir) {
            throw FsError(ENOTDIR, "not a directory");
        }
        meta::delete_inode(db, child->ino, base_gen());  // непустой — ENOTEMPTY
        touch_dir(db, parent, now);
    });
}

Created Vfs::create(int64_t parent, ByteSpan name, uint32_t mode, int flags, uint32_t uid,
                    uint32_t gid) {
    check_writable();
    const int64_t now = now_ns();
    const Attr attr = meta_.write([&](Db& db) {
        meta::NewInode n;
        n.parent = parent;
        n.name.assign(name.begin(), name.end());
        n.kind = InodeKind::file;
        n.mode = mode;
        n.uid = uid;
        n.gid = gid;
        n.now_ns = now;
        const int64_t ino = meta::insert_inode(db, n);
        touch_dir(db, parent, now);
        return make_attr(db, *meta::get_inode(db, ino), std::nullopt);
    });
    Created out;
    out.attr = attr;
    {
        const std::lock_guard lock(refs_mu_);
        Refs& r = refs_[attr.ino];
        r.lookups += 1;
        r.opens += 1;
        out.fh = next_fh_++;
        handles_[out.fh] = Handle{attr.ino, (flags & O_ACCMODE) != O_RDONLY};
    }
    return out;
}

Attr Vfs::mknod(int64_t parent, ByteSpan name, uint32_t mode, uint32_t uid, uint32_t gid) {
    if ((mode & S_IFMT) != 0 && (mode & S_IFMT) != S_IFREG) {
        throw FsError(EPERM, "special files are not supported");
    }
    check_writable();
    const int64_t now = now_ns();
    Attr attr = meta_.write([&](Db& db) {
        meta::NewInode n;
        n.parent = parent;
        n.name.assign(name.begin(), name.end());
        n.kind = InodeKind::file;
        n.mode = mode;
        n.uid = uid;
        n.gid = gid;
        n.now_ns = now;
        const int64_t ino = meta::insert_inode(db, n);
        touch_dir(db, parent, now);
        return make_attr(db, *meta::get_inode(db, ino), std::nullopt);
    });
    add_lookup(attr.ino, 1);
    return attr;
}

Attr Vfs::symlink(int64_t parent, ByteSpan name, ByteSpan target, uint32_t uid, uint32_t gid) {
    check_writable();
    const int64_t now = now_ns();
    Attr attr = meta_.write([&](Db& db) {
        meta::NewInode n;
        n.parent = parent;
        n.name.assign(name.begin(), name.end());
        n.kind = InodeKind::symlink;
        n.mode = 0777;
        n.uid = uid;
        n.gid = gid;
        n.now_ns = now;
        n.target.assign(target.begin(), target.end());
        const int64_t ino = meta::insert_inode(db, n);
        touch_dir(db, parent, now);
        return make_attr(db, *meta::get_inode(db, ino), std::nullopt);
    });
    add_lookup(attr.ino, 1);
    return attr;
}

Bytes Vfs::readlink(int64_t ino) {
    check_alive();
    const Inode inode = get_inode_or_throw(ino);
    if (inode.kind != InodeKind::symlink || !inode.target) {
        throw FsError(EINVAL, "not a symlink");
    }
    return *inode.target;
}

void Vfs::unlink(int64_t parent, ByteSpan name) {
    check_writable();
    meta::validate_name(name);
    for (;;) {
        const auto child = meta_.read([&](Db& db) -> std::optional<Inode> {
            const auto p = meta::get_inode(db, parent);
            if (!p) {
                throw FsError(ENOENT, "no parent");
            }
            if (p->kind != InodeKind::dir) {
                throw FsError(ENOTDIR, "parent is not a directory");
            }
            return meta::lookup(db, parent, name);
        });
        if (!child) {
            throw FsError(ENOENT, "no entry");
        }
        if (child->kind == InodeKind::dir) {
            throw FsError(EISDIR, "unlink of a directory");
        }
        // Полоса жертвы: её данные не должны меняться, пока удаляется overlay.
        const std::unique_lock lock(stripe(child->ino));
        const int64_t now = now_ns();
        enum class Result : uint8_t { retry, kept, deleted };
        const Result r = meta_.write([&](Db& db) {
            const auto cur = meta::lookup(db, parent, name);
            if (!cur || cur->ino != child->ino) {
                return Result::retry;
            }
            const bool deleted = remove_victim(db, *cur);
            touch_dir(db, parent, now);
            return deleted ? Result::deleted : Result::kept;
        });
        if (r == Result::retry) {
            continue;
        }
        if (r == Result::deleted) {
            drop_file_data(child->ino);
        } else {
            // Открытый удалённый файл: его данные умрут вместе с ним и в пакет не попадут.
            packs_->drop(child->ino);
        }
        return;
    }
}

void Vfs::rename(int64_t parent, ByteSpan name, int64_t new_parent, ByteSpan new_name,
                 unsigned flags) {
    if ((flags & ~static_cast<unsigned>(RENAME_NOREPLACE)) != 0) {
        throw FsError(EINVAL, "unsupported rename flags");
    }
    check_writable();
    meta::validate_name(name);
    meta::validate_name(new_name);
    const bool noreplace = (flags & RENAME_NOREPLACE) != 0;
    for (;;) {
        // Цель, если это файл, может потребовать уборки overlay: берём её полосу заранее.
        const auto dst = meta_.read(
            [&](Db& db) -> std::optional<Inode> { return meta::lookup(db, new_parent, new_name); });
        std::unique_lock<std::shared_mutex> lock;
        if (dst && dst->kind == InodeKind::file) {
            lock = std::unique_lock(stripe(dst->ino));
        }
        const int64_t now = now_ns();
        enum class Result : uint8_t { retry, done, victim_deleted };
        const Result r = meta_.write([&](Db& db) {
            for (const int64_t dir : {parent, new_parent}) {
                const auto d = meta::get_inode(db, dir);
                if (!d) {
                    throw FsError(ENOENT, "no parent");
                }
                if (d->kind != InodeKind::dir) {
                    throw FsError(ENOTDIR, "parent is not a directory");
                }
            }
            const auto src = meta::lookup(db, parent, name);
            if (!src) {
                throw FsError(ENOENT, "no source");
            }
            const auto cur_dst = meta::lookup(db, new_parent, new_name);
            if ((cur_dst ? cur_dst->ino : 0) != (dst ? dst->ino : 0)) {
                return Result::retry;
            }
            if (cur_dst && cur_dst->ino == src->ino) {
                return Result::done;  // то же самое имя той же записи
            }
            if (cur_dst && noreplace) {
                throw FsError(EEXIST, "target exists");
            }
            const bool src_dir = src->kind == InodeKind::dir;
            if (cur_dst) {
                const bool dst_dir = cur_dst->kind == InodeKind::dir;
                if (src_dir && !dst_dir) {
                    throw FsError(ENOTDIR, "directory over non-directory");
                }
                if (!src_dir && dst_dir) {
                    throw FsError(EISDIR, "non-directory over directory");
                }
                if (dst_dir && meta::count_children(db, cur_dst->ino) > 0) {
                    throw FsError(ENOTEMPTY, "target directory not empty");
                }
            }
            if (src_dir) {
                // Нельзя перенести каталог внутрь его же поддерева.
                int64_t walk = new_parent;
                while (walk != meta::kRootIno) {
                    if (walk == src->ino) {
                        throw FsError(EINVAL, "directory into its own subtree");
                    }
                    const auto w = meta::get_inode(db, walk);
                    if (!w || !w->parent) {
                        break;
                    }
                    walk = *w->parent;
                }
            }
            bool victim_deleted = false;
            if (cur_dst) {
                victim_deleted = remove_victim(db, *cur_dst);
            }
            meta::move_inode(db, src->ino, new_parent, new_name);
            meta::AttrUpdate u;
            u.ctime_ns = now;
            meta::update_attrs(db, src->ino, u);
            touch_dir(db, parent, now);
            if (new_parent != parent) {
                touch_dir(db, new_parent, now);
            }
            return victim_deleted ? Result::victim_deleted : Result::done;
        });
        if (r == Result::retry) {
            continue;
        }
        if (r == Result::victim_deleted) {
            drop_file_data(dst->ino);
        } else if (dst && dst->kind == InodeKind::file) {
            // Открытый удалённый файл: данные умрут вместе с ним, в пакет не попадут.
            const auto now_dst = meta_.read([&](Db& db) { return meta::get_inode(db, dst->ino); });
            if (now_dst && !now_dst->parent) {
                packs_->drop(dst->ino);
            }
        }
        return;
    }
}

void Vfs::link() { throw FsError(EPERM, "hard links are not supported"); }
void Vfs::xattr() { throw FsError(ENOTSUP, "xattrs are not supported"); }
void Vfs::fallocate() { throw FsError(EOPNOTSUPP, "fallocate is not supported"); }

// --- открытие файлов ---

uint64_t Vfs::open(int64_t ino, int flags) {
    check_alive();
    const bool writable = (flags & O_ACCMODE) != O_RDONLY;
    if (writable) {
        check_writable();
    }
    const Inode inode = get_inode_or_throw(ino);
    if (inode.kind == InodeKind::dir) {
        if (writable) {
            throw FsError(EISDIR, "directory opened for writing");
        }
    } else if (inode.kind != InodeKind::file) {
        throw FsError(EINVAL, "not a regular file");
    }
    uint64_t fh = 0;
    {
        const std::lock_guard lock(refs_mu_);
        refs_[ino].opens += 1;
        fh = next_fh_++;
        handles_[fh] = Handle{ino, writable};
    }
    if (writable && (flags & O_TRUNC) != 0 && inode.kind == InodeKind::file) {
        try {
            SetAttr s;
            s.size = 0;
            s.mtime_now = true;
            setattr(ino, s);
        } catch (...) {
            release(fh);
            throw;
        }
    }
    return fh;
}

void Vfs::release(uint64_t fh) {
    int64_t ino = 0;
    bool last = false;
    {
        const std::lock_guard lock(refs_mu_);
        const auto it = handles_.find(fh);
        if (it == handles_.end()) {
            return;
        }
        ino = it->second.ino;
        handles_.erase(it);
        reader_->forget_handle(fh);
        auto r = refs_.find(ino);
        if (r != refs_.end()) {
            r->second.opens -= std::min<uint64_t>(r->second.opens, 1);
            last = r->second.opens == 0;
            if (last && r->second.lookups == 0) {
                refs_.erase(r);
            }
        }
    }
    if (!last || fatal_.load()) {
        return;
    }
    try {
        maybe_reap(ino);
        if (!read_only() && overlay_->has_chunks(ino)) {
            push_job(Job{Job::Kind::seal, ino, 0, false, false, 0});
        }
    } catch (const std::exception& e) {
        spdlog::error("vfs: release of inode {} failed: {}", ino, e.what());
    }
}

void Vfs::flush(uint64_t /*fh*/) {
    // close() не ждёт загрузки (SPEC 8.1): данные уже в overlay.
}

// --- каталоги ---

uint64_t Vfs::opendir(int64_t ino) {
    check_alive();
    const Inode inode = get_inode_or_throw(ino);
    if (inode.kind != InodeKind::dir) {
        throw FsError(ENOTDIR, "not a directory");
    }
    auto h = std::make_shared<DirHandle>();
    h->ino = ino;
    h->parent = inode.parent.value_or(meta::kRootIno);
    const std::lock_guard lock(refs_mu_);
    const uint64_t fh = next_fh_++;
    dir_handles_[fh] = std::move(h);
    return fh;
}

std::vector<DirEntry> Vfs::readdir(uint64_t fh, uint64_t off, size_t max, bool plus) {
    check_alive();
    std::shared_ptr<DirHandle> h;
    {
        const std::lock_guard lock(refs_mu_);
        const auto it = dir_handles_.find(fh);
        if (it == dir_handles_.end()) {
            throw FsError(EBADF, "bad directory handle");
        }
        h = it->second;
    }
    const std::lock_guard lock(h->mu);
    std::vector<DirEntry> out;
    if (max == 0 || off > h->names.size()) {
        return out;
    }
    // Номера записей после off выдаются заново: соответствие номера и имени перезаписывается.
    h->names.resize(off);
    const Bytes dot{std::byte{'.'}};
    const Bytes dotdot{std::byte{'.'}, std::byte{'.'}};
    // dirty_attrs — до снимка индекса (см. make_attr).
    std::unordered_map<int64_t, meta::DirtyAttrs> dirty;
    if (plus) {
        dirty = overlay_->attrs_snapshot();
    }
    meta_.read([&](Db& db) {
        const auto self = meta::get_inode(db, h->ino);
        if (!self) {
            throw FsError(ENOENT, "directory removed");
        }
        auto emit = [&](Bytes name, const Inode& inode) {
            DirEntry e;
            e.name = name;
            e.ino = inode.ino;
            e.kind = inode.kind;
            if (plus) {
                std::optional<meta::DirtyAttrs> d;
                if (inode.kind == InodeKind::file) {
                    if (const auto it = dirty.find(inode.ino); it != dirty.end()) {
                        d = it->second;
                    }
                }
                e.attr = make_attr(db, inode, d);
            }
            h->names.push_back(std::move(name));
            e.off = h->names.size();
            out.push_back(std::move(e));
        };
        if (h->names.empty() && out.size() < max) {
            emit(dot, *self);
        }
        if (h->names.size() == 1 && out.size() < max) {
            const auto p = meta::get_inode(db, h->parent);
            emit(dotdot, p ? *p : *self);
        }
        if (out.size() >= max) {
            return;
        }
        std::optional<Bytes> after;
        if (h->names.size() > 2) {
            after = h->names.back();
        }
        const auto children =
            meta::list_children(db, h->ino, after, static_cast<int64_t>(max - out.size()));
        for (const Inode& c : children) {
            emit(*c.name, c);
        }
    });
    return out;
}

void Vfs::releasedir(uint64_t fh) {
    const std::lock_guard lock(refs_mu_);
    dir_handles_.erase(fh);
}

void Vfs::fsyncdir(int64_t /*ino*/) {
    check_alive();
    commit_or_throw();
}

StatFs Vfs::statfs() {
    check_alive();
    StatFs s;
    const auto [used, files] = meta_.read([](Db& db) {
        meta::Stmt st = db.cached("SELECT COALESCE(SUM(plain_size), 0) FROM chunks");
        st.step();
        const int64_t chunk_bytes = st.column_int64(0);
        meta::Stmt cnt = db.cached("SELECT COUNT(*) FROM inodes");
        cnt.step();
        return std::pair<int64_t, int64_t>{chunk_bytes + meta::inline_total(db),
                                           cnt.column_int64(0)};
    });
    s.blocks = opts_.virtual_capacity / kBlockSize;
    const uint64_t used_blocks = (static_cast<uint64_t>(used) + kBlockSize - 1) / kBlockSize;
    s.bfree = s.blocks > used_blocks ? s.blocks - used_blocks : 0;
    s.bavail = s.bfree;
    s.files = static_cast<uint64_t>(files) + s.bfree;
    s.ffree = s.bfree;
    return s;
}

VfsStatus Vfs::status() {
    const OverlayCounts c = overlay_->counts();
    VfsStatus s;
    s.dirty_chunks = c.chunks;
    s.dirty_inodes = c.inodes;
    s.queued = c.queued;
    s.uploading = c.uploading;
    s.failed = c.failed;
    const PackStatus ps = packs_->status();
    s.uploading += ps.closing_packs;
    s.open_pack_objects = ps.open_objects;
    s.open_pack_bytes = ps.open_bytes;
    s.staging_used = budget_->used();
    s.staging_limit = budget_->config().limit;
    s.uploaded_chunks = uploaded_.load();
    s.fatal = fatal_.load();
    const std::lock_guard lock(err_mu_);
    s.last_error = last_error_;
    return s;
}

bool Vfs::close_open_pack() {
    if (read_only() || fatal_.load()) {
        return false;
    }
    return packs_->close_open(false);
}

PackStatus Vfs::pack_status() const { return packs_->status(); }

// --- учёт staging и обратное давление (SPEC 8.3) ---

void Vfs::wait_for_space(uint64_t shortage, std::chrono::steady_clock::time_point deadline) {
    if (budget_->never_fits_user(shortage)) {
        throw FsError(ENOSPC, "write does not fit into staging_limit minus the upload reserve");
    }
    constexpr std::chrono::milliseconds kSlice{50};
    const auto abort = [this] { return fatal_.load() || stop_flag_.load(); };
    for (;;) {
        // Сначала освободить место самим: запечатать старое, закрыть пакет.
        relieve_pressure(shortage);
        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline) {
            throw FsError(ENOSPC, "staging is full: uploads did not free space in time");
        }
        const auto r = budget_->wait_user(shortage, std::min(deadline, now + kSlice), abort);
        if (r == StagingBudget::WaitResult::ok) {
            return;
        }
        if (r == StagingBudget::WaitResult::aborted) {
            check_alive();
            throw FsError(EIO, "file system is shutting down");
        }
    }
}

void Vfs::relieve_pressure(uint64_t extra) {
    if (read_only() || fatal_.load() || !budget_->above_threshold(extra)) {
        return;
    }
    // Один обходчик за раз: остальные писатели не толкаются, у ждущих свой цикл.
    const std::unique_lock lock(relieve_mu_, std::try_to_lock);
    if (!lock.owns_lock()) {
        return;
    }
    try {
        // Меру давления дают длины staging-файлов и внешние файлы без аренды заливок: аренда
        // освободится сама, а запечатывание освобождает только staging-файлы.
        const uint64_t leased = budget_->leased();
        const uint64_t total = budget_->used();
        const uint64_t used = total > leased ? total - leased : 0;
        const uint64_t threshold = budget_->pressure_threshold();
        const uint64_t need_free = used + extra > threshold ? used + extra - threshold : 0;
        if (need_free == 0) {
            return;
        }
        std::unordered_set<int64_t> detached;
        {
            const std::lock_guard refs(refs_mu_);
            detached = detached_;
        }
        const PressurePlan plan = overlay_->plan_pressure(need_free, detached);
        for (const int64_t ino : plan.inos) {
            // Без seal_delay: запечатывается весь файл, в том числе недописанные чанки (SPEC 8.1).
            seal_file(ino, SealMode::release);
        }
        // Объекты, запечатанные в открытый пакет, освободят место только после его заливки.
        packs_->close_open(false);
    } catch (const FsError& e) {
        note_error(std::string("relieving staging pressure: ") + e.what());
    } catch (const SysError& e) {
        note_error(std::string("relieving staging pressure: ") + e.what());
    }
}

}  // namespace telega::vfs
