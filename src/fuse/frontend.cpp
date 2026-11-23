#include "fuse/frontend.h"

#include <fuse_log.h>
#include <fuse_lowlevel.h>
#include <spdlog/spdlog.h>
#include <sys/stat.h>
#include <sys/statvfs.h>

#include <array>
#include <cerrno>
#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <exception>
#include <mutex>
#include <utility>
#include <vector>

// Весь файл — граница с C API libfuse: сырые указатели, reinterpret_cast между char и std::byte
// и C-обратные вызовы неизбежны. Логика ФС живёт в Vfs.

namespace telega::fuse {

namespace {

#if FUSE_USE_VERSION >= 312
// Предел рабочих потоков цикла FUSE (libfuse 3.12+; по умолчанию там 10).
constexpr unsigned kMaxWorkerThreads = 64;
#endif

using vfs::Attr;
using vfs::Vfs;

std::mutex g_log_mu;
std::string g_last_message;  // под g_log_mu

void log_func(enum fuse_log_level level, const char* fmt, va_list ap) {
    std::array<char, 1024> buf{};
    // NOLINTNEXTLINE(clang-analyzer-valist.Uninitialized,cert-err33-c): ap выдаёт libfuse
    vsnprintf(buf.data(), buf.size(), fmt, ap);
    std::string text{buf.data()};
    while (!text.empty() && (text.back() == '\n' || text.back() == '\r')) {
        text.pop_back();
    }
    if (text.empty()) {
        return;
    }
    switch (level) {
        case FUSE_LOG_EMERG:
        case FUSE_LOG_ALERT:
        case FUSE_LOG_CRIT:
        case FUSE_LOG_ERR: {
            spdlog::error("fuse: {}", text);
            const std::lock_guard lock(g_log_mu);
            g_last_message = text;
            break;
        }
        case FUSE_LOG_WARNING: {
            spdlog::warn("fuse: {}", text);
            const std::lock_guard lock(g_log_mu);
            g_last_message = text;
            break;
        }
        case FUSE_LOG_NOTICE:
        case FUSE_LOG_INFO:
            spdlog::info("fuse: {}", text);
            break;
        default:
            spdlog::debug("fuse: {}", text);
            break;
    }
}

ByteSpan name_span(const char* name) {
    return {reinterpret_cast<const std::byte*>(name), std::strlen(name)};
}

std::string to_string(const Bytes& bytes) {
    return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
}

int64_t to_ns(const timespec& ts) { return int64_t{ts.tv_sec} * 1'000'000'000 + ts.tv_nsec; }

mode_t type_bits(meta::InodeKind kind) {
    switch (kind) {
        case meta::InodeKind::dir:
            return S_IFDIR;
        case meta::InodeKind::symlink:
            return S_IFLNK;
        default:
            return S_IFREG;
    }
}

}  // namespace

struct Frontend::Impl {
    Vfs& vfs;
    Options options;
    std::function<void()> on_destroy;
    std::once_flag destroy_once;

    fuse_session* session = nullptr;
    bool mounted = false;
    bool signals = false;

    Impl(Vfs& v, Options o, std::function<void()> d)
        : vfs(v), options(std::move(o)), on_destroy(std::move(d)) {}

    void run_destroy() {
        std::call_once(destroy_once, [this] {
            if (on_destroy) {
                on_destroy();
            }
        });
    }
};

namespace {

Frontend::Impl& self(fuse_req_t req) {
    return *static_cast<Frontend::Impl*>(fuse_req_userdata(req));
}

// Выполнить операцию; исключение превращается в ответ с errno. f сам отвечает ядру.
template <class F>
void guarded(fuse_req_t req, F&& f) noexcept {
    try {
        std::forward<F>(f)();
    } catch (const std::exception& e) {
        spdlog::debug("fuse op failed: {}", e.what());
        fuse_reply_err(req, vfs::to_errno(e));
    } catch (...) {
        fuse_reply_err(req, EIO);
    }
}

fuse_entry_param entry_of(const Frontend::Impl& impl, const Attr& attr) {
    fuse_entry_param e{};
    e.ino = static_cast<fuse_ino_t>(attr.ino);
    e.generation = 0;
    vfs::fill_stat(attr, e.attr);
    e.attr_timeout = impl.options.attr_timeout;
    e.entry_timeout = impl.options.entry_timeout;
    return e;
}

// Ответить записью каталога. Счётчик lookup уже увеличен Vfs; если ответ не доставлен
// (запрос прерван), его надо вернуть.
void reply_entry(fuse_req_t req, Frontend::Impl& impl, const Attr& attr) {
    const fuse_entry_param e = entry_of(impl, attr);
    if (fuse_reply_entry(req, &e) != 0) {
        impl.vfs.forget(attr.ino, 1);
    }
}

int64_t ino_of(fuse_ino_t ino) { return static_cast<int64_t>(ino); }

// --- init / destroy ---

void op_init(void* userdata, fuse_conn_info* conn) {
    auto& impl = *static_cast<Frontend::Impl*>(userdata);
    // Метки времени ФС — наносекунды.
    conn->time_gran = 1;
    // Чтение (SPEC 9): max_read не ограничиваем. max_readahead не трогаем: ядро берёт минимум из
    // него и read_ahead_kb устройства (128 КиБ), так что сервер readahead ядра не поднимет;
    // чтение вперёд делает ChunkReader (SPEC 8.2).
    if ((conn->capable & FUSE_CAP_ASYNC_READ) != 0) {
        conn->want |= FUSE_CAP_ASYNC_READ;
    }
    spdlog::info("fuse: mounted at {} (proto {}.{}, max_write {})",
                 impl.options.mountpoint.string(), conn->proto_major, conn->proto_minor,
                 conn->max_write);
}

void op_destroy(void* userdata) { static_cast<Frontend::Impl*>(userdata)->run_destroy(); }

// --- пространство имён ---

void op_lookup(fuse_req_t req, fuse_ino_t parent, const char* name) {
    guarded(req, [&] {
        auto& impl = self(req);
        reply_entry(req, impl, impl.vfs.lookup(ino_of(parent), name_span(name)));
    });
}

void op_forget(fuse_req_t req, fuse_ino_t ino, uint64_t nlookup) {
    try {
        self(req).vfs.forget(ino_of(ino), nlookup);
    } catch (const std::exception& e) {
        spdlog::warn("forget failed: {}", e.what());
    }
    fuse_reply_none(req);
}

void op_forget_multi(fuse_req_t req, size_t count, fuse_forget_data* forgets) {
    auto& impl = self(req);
    for (size_t i = 0; i < count; ++i) {
        try {
            impl.vfs.forget(ino_of(forgets[i].ino), forgets[i].nlookup);  // NOLINT
        } catch (const std::exception& e) {
            spdlog::warn("forget failed: {}", e.what());
        }
    }
    fuse_reply_none(req);
}

void reply_attr(fuse_req_t req, const Frontend::Impl& impl, const Attr& attr) {
    struct stat st {};
    vfs::fill_stat(attr, st);
    fuse_reply_attr(req, &st, impl.options.attr_timeout);
}

void op_getattr(fuse_req_t req, fuse_ino_t ino, fuse_file_info* /*fi*/) {
    guarded(req, [&] {
        auto& impl = self(req);
        reply_attr(req, impl, impl.vfs.getattr(ino_of(ino)));
    });
}

void op_setattr(fuse_req_t req, fuse_ino_t ino, struct stat* attr, int to_set,
                fuse_file_info* /*fi*/) {
    guarded(req, [&] {
        auto& impl = self(req);
        vfs::SetAttr change;
        if ((to_set & FUSE_SET_ATTR_MODE) != 0) {
            change.mode = attr->st_mode & 07777U;
        }
        if ((to_set & FUSE_SET_ATTR_UID) != 0) {
            change.uid = attr->st_uid;
        }
        if ((to_set & FUSE_SET_ATTR_GID) != 0) {
            change.gid = attr->st_gid;
        }
        if ((to_set & FUSE_SET_ATTR_SIZE) != 0) {
            change.size = attr->st_size;
        }
        if ((to_set & FUSE_SET_ATTR_ATIME_NOW) != 0) {
            change.atime_now = true;
        } else if ((to_set & FUSE_SET_ATTR_ATIME) != 0) {
            change.atime_ns = to_ns(attr->st_atim);
        }
        if ((to_set & FUSE_SET_ATTR_MTIME_NOW) != 0) {
            change.mtime_now = true;
        } else if ((to_set & FUSE_SET_ATTR_MTIME) != 0) {
            change.mtime_ns = to_ns(attr->st_mtim);
        }
        reply_attr(req, impl, impl.vfs.setattr(ino_of(ino), change));
    });
}

void op_readlink(fuse_req_t req, fuse_ino_t ino) {
    guarded(req, [&] {
        const std::string target = to_string(self(req).vfs.readlink(ino_of(ino)));
        fuse_reply_readlink(req, target.c_str());
    });
}

void op_mknod(fuse_req_t req, fuse_ino_t parent, const char* name, mode_t mode, dev_t /*rdev*/) {
    guarded(req, [&] {
        auto& impl = self(req);
        const fuse_ctx* ctx = fuse_req_ctx(req);
        reply_entry(req, impl,
                    impl.vfs.mknod(ino_of(parent), name_span(name), mode, ctx->uid, ctx->gid));
    });
}

void op_mkdir(fuse_req_t req, fuse_ino_t parent, const char* name, mode_t mode) {
    guarded(req, [&] {
        auto& impl = self(req);
        const fuse_ctx* ctx = fuse_req_ctx(req);
        reply_entry(req, impl,
                    impl.vfs.mkdir(ino_of(parent), name_span(name), mode, ctx->uid, ctx->gid));
    });
}

void op_unlink(fuse_req_t req, fuse_ino_t parent, const char* name) {
    guarded(req, [&] {
        self(req).vfs.unlink(ino_of(parent), name_span(name));
        fuse_reply_err(req, 0);
    });
}

void op_rmdir(fuse_req_t req, fuse_ino_t parent, const char* name) {
    guarded(req, [&] {
        self(req).vfs.rmdir(ino_of(parent), name_span(name));
        fuse_reply_err(req, 0);
    });
}

void op_symlink(fuse_req_t req, const char* link, fuse_ino_t parent, const char* name) {
    guarded(req, [&] {
        auto& impl = self(req);
        const fuse_ctx* ctx = fuse_req_ctx(req);
        reply_entry(
            req, impl,
            impl.vfs.symlink(ino_of(parent), name_span(name), name_span(link), ctx->uid, ctx->gid));
    });
}

void op_rename(fuse_req_t req, fuse_ino_t parent, const char* name, fuse_ino_t newparent,
               const char* newname, unsigned int flags) {
    guarded(req, [&] {
        self(req).vfs.rename(ino_of(parent), name_span(name), ino_of(newparent), name_span(newname),
                             flags);
        fuse_reply_err(req, 0);
    });
}

void op_link(fuse_req_t req, fuse_ino_t /*ino*/, fuse_ino_t /*newparent*/,
             const char* /*newname*/) {
    guarded(req, [&] { Vfs::link(); });
}

// --- файлы ---

void op_open(fuse_req_t req, fuse_ino_t ino, fuse_file_info* fi) {
    guarded(req, [&] {
        auto& impl = self(req);
        fi->fh = impl.vfs.open(ino_of(ino), fi->flags);
        fi->direct_io = 0;  // mmap требует обычного режима с кэшем страниц
        // Писатель один, и все изменения идут через ядро этого монтирования: страничный кэш
        // остаётся верным между открытиями (SPEC 9).
        fi->keep_cache = 1;
        if (fuse_reply_open(req, fi) != 0) {
            impl.vfs.release(fi->fh);
        }
    });
}

void op_create(fuse_req_t req, fuse_ino_t parent, const char* name, mode_t mode,
               fuse_file_info* fi) {
    guarded(req, [&] {
        auto& impl = self(req);
        const fuse_ctx* ctx = fuse_req_ctx(req);
        const vfs::Created created =
            impl.vfs.create(ino_of(parent), name_span(name), mode, fi->flags, ctx->uid, ctx->gid);
        fi->fh = created.fh;
        fi->direct_io = 0;
        const fuse_entry_param e = entry_of(impl, created.attr);
        if (fuse_reply_create(req, &e, fi) != 0) {
            impl.vfs.release(created.fh);
            impl.vfs.forget(created.attr.ino, 1);
        }
    });
}

void op_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, fuse_file_info* fi) {
    guarded(req, [&] {
        const Bytes data =
            self(req).vfs.read(ino_of(ino), static_cast<uint64_t>(off), size, fi->fh);
        fuse_reply_buf(req, reinterpret_cast<const char*>(data.data()), data.size());
    });
}

void op_write(fuse_req_t req, fuse_ino_t ino, const char* buf, size_t size, off_t off,
              fuse_file_info* /*fi*/) {
    guarded(req, [&] {
        const size_t n =
            self(req).vfs.write(ino_of(ino), static_cast<uint64_t>(off),
                                ByteSpan{reinterpret_cast<const std::byte*>(buf), size});
        fuse_reply_write(req, n);
    });
}

void op_flush(fuse_req_t req, fuse_ino_t /*ino*/, fuse_file_info* fi) {
    guarded(req, [&] {
        self(req).vfs.flush(fi->fh);
        fuse_reply_err(req, 0);
    });
}

void op_release(fuse_req_t req, fuse_ino_t /*ino*/, fuse_file_info* fi) {
    guarded(req, [&] {
        self(req).vfs.release(fi->fh);
        fuse_reply_err(req, 0);
    });
}

void op_fsync(fuse_req_t req, fuse_ino_t ino, int /*datasync*/, fuse_file_info* /*fi*/) {
    guarded(req, [&] {
        self(req).vfs.fsync(ino_of(ino));
        fuse_reply_err(req, 0);
    });
}

void op_fallocate(fuse_req_t req, fuse_ino_t /*ino*/, int /*mode*/, off_t /*offset*/,
                  off_t /*length*/, fuse_file_info* /*fi*/) {
    guarded(req, [&] { Vfs::fallocate(); });
}

// --- каталоги ---

void op_opendir(fuse_req_t req, fuse_ino_t ino, fuse_file_info* fi) {
    guarded(req, [&] {
        auto& impl = self(req);
        fi->fh = impl.vfs.opendir(ino_of(ino));
        if (fuse_reply_open(req, fi) != 0) {
            impl.vfs.releasedir(fi->fh);
        }
    });
}

void readdir_common(fuse_req_t req, size_t size, off_t off, fuse_file_info* fi, bool plus) {
    guarded(req, [&] {
        auto& impl = self(req);
        // Верхняя оценка числа записей, которые поместятся в буфер: минимальная запись
        // 32 байта, с атрибутами — около 160.
        const size_t max_entries = size / (plus ? 160 : 32) + 1;
        const std::vector<vfs::DirEntry> entries =
            impl.vfs.readdir(fi->fh, static_cast<uint64_t>(off), max_entries, plus);
        std::vector<char> buf(size);
        size_t used = 0;
        // Счётчик lookup увеличивается до ответа (ядро может прислать forget сразу после него);
        // если ответ не доставлен, возвращается, как в reply_entry.
        std::vector<int64_t> remembered;
        for (const vfs::DirEntry& de : entries) {
            const std::string name = to_string(de.name);
            const size_t room = size - used;
            size_t need = 0;
            if (plus) {
                fuse_entry_param e{};
                if (de.attr) {
                    e = entry_of(impl, *de.attr);
                } else {
                    // ino = 0: ядро не создаёт inode для записи, только отдаёт имя.
                    e.attr.st_ino = static_cast<ino_t>(de.ino);
                    e.attr.st_mode = type_bits(de.kind);
                }
                need = fuse_add_direntry_plus(req, buf.data() + used, room, name.c_str(), &e,
                                              static_cast<off_t>(de.off));
                if (need > room) {
                    break;
                }
                // «.» и «..» ядро не запоминает; остальные записи — это lookup.
                if (de.attr && name != "." && name != "..") {
                    impl.vfs.remember_lookup(de.attr->ino);
                    remembered.push_back(de.attr->ino);
                }
            } else {
                struct stat st {};
                st.st_ino = static_cast<ino_t>(de.ino);
                st.st_mode = type_bits(de.kind);
                need = fuse_add_direntry(req, buf.data() + used, room, name.c_str(), &st,
                                         static_cast<off_t>(de.off));
                if (need > room) {
                    break;
                }
            }
            used += need;
        }
        if (fuse_reply_buf(req, buf.data(), used) != 0) {
            for (const int64_t ino : remembered) {
                impl.vfs.forget(ino, 1);
            }
        }
    });
}

void op_readdir(fuse_req_t req, fuse_ino_t /*ino*/, size_t size, off_t off, fuse_file_info* fi) {
    readdir_common(req, size, off, fi, false);
}

void op_readdirplus(fuse_req_t req, fuse_ino_t /*ino*/, size_t size, off_t off,
                    fuse_file_info* fi) {
    readdir_common(req, size, off, fi, true);
}

void op_releasedir(fuse_req_t req, fuse_ino_t /*ino*/, fuse_file_info* fi) {
    guarded(req, [&] {
        self(req).vfs.releasedir(fi->fh);
        fuse_reply_err(req, 0);
    });
}

void op_fsyncdir(fuse_req_t req, fuse_ino_t ino, int /*datasync*/, fuse_file_info* /*fi*/) {
    guarded(req, [&] {
        self(req).vfs.fsyncdir(ino_of(ino));
        fuse_reply_err(req, 0);
    });
}

void op_statfs(fuse_req_t req, fuse_ino_t /*ino*/) {
    guarded(req, [&] {
        const vfs::StatFs s = self(req).vfs.statfs();
        struct statvfs sv {};
        sv.f_bsize = s.bsize;
        sv.f_frsize = s.frsize;
        sv.f_blocks = s.blocks;
        sv.f_bfree = s.bfree;
        sv.f_bavail = s.bavail;
        sv.f_files = s.files;
        sv.f_ffree = s.ffree;
        sv.f_favail = s.ffree;
        sv.f_namemax = s.namemax;
        fuse_reply_statfs(req, &sv);
    });
}

// xattr не поддерживаются (SPEC 9). Обработчики намеренно не регистрируются: тогда libfuse
// отвечает ENOSYS, ядро запоминает это (no_getxattr и т. п.) и больше не присылает запросы,
// а приложение получает EOPNOTSUPP (= ENOTSUP). С ответом ENOTSUP ядро спрашивало бы
// security.capability перед каждой записью.

fuse_lowlevel_ops make_ops() {
    fuse_lowlevel_ops ops{};
    ops.init = op_init;
    ops.destroy = op_destroy;
    ops.lookup = op_lookup;
    ops.forget = op_forget;
    ops.forget_multi = op_forget_multi;
    ops.getattr = op_getattr;
    ops.setattr = op_setattr;
    ops.readlink = op_readlink;
    ops.mknod = op_mknod;
    ops.mkdir = op_mkdir;
    ops.unlink = op_unlink;
    ops.rmdir = op_rmdir;
    ops.symlink = op_symlink;
    ops.rename = op_rename;
    ops.link = op_link;
    ops.open = op_open;
    ops.read = op_read;
    ops.write = op_write;
    ops.flush = op_flush;
    ops.release = op_release;
    ops.fsync = op_fsync;
    ops.opendir = op_opendir;
    ops.readdir = op_readdir;
    ops.releasedir = op_releasedir;
    ops.fsyncdir = op_fsyncdir;
    ops.statfs = op_statfs;
    ops.create = op_create;
    ops.readdirplus = op_readdirplus;
    ops.fallocate = op_fallocate;
    return ops;
}

}  // namespace

std::string last_libfuse_message() {
    const std::lock_guard lock(g_log_mu);
    return g_last_message;
}

Frontend::Frontend(Vfs& vfs, Options options, std::function<void()> on_destroy)
    : impl_(std::make_unique<Impl>(vfs, std::move(options), std::move(on_destroy))) {}

Frontend::~Frontend() { close(); }

void Frontend::mount() {
    if (impl_->session != nullptr) {
        throw std::logic_error("Frontend::mount called twice");
    }
    fuse_set_log_func(log_func);
    {
        const std::lock_guard lock(g_log_mu);
        g_last_message.clear();
    }

    std::string opts = "default_permissions,fsname=telega-fs,subtype=telega-fs";
    if (impl_->options.read_only) {
        opts += ",ro";
    }
    if (!impl_->options.mount_options.empty()) {
        opts += "," + impl_->options.mount_options;
    }

    fuse_args args = FUSE_ARGS_INIT(0, nullptr);
    fuse_opt_add_arg(&args, "telega-fs");
    fuse_opt_add_arg(&args, "-o");
    fuse_opt_add_arg(&args, opts.c_str());

    static const fuse_lowlevel_ops ops = make_ops();
    impl_->session = fuse_session_new(&args, &ops, sizeof(ops), impl_.get());
    fuse_opt_free_args(&args);
    if (impl_->session == nullptr) {
        const std::string why = last_libfuse_message();
        throw FuseError("cannot create FUSE session (check -o options)" +
                        (why.empty() ? std::string{} : ": " + why));
    }

    if (fuse_set_signal_handlers(impl_->session) != 0) {
        close();
        throw FuseError("cannot install signal handlers");
    }
    impl_->signals = true;

    if (fuse_session_mount(impl_->session, impl_->options.mountpoint.c_str()) != 0) {
        const std::string why = last_libfuse_message();
        close();
        throw FuseError("cannot mount " + impl_->options.mountpoint.string() +
                        (why.empty() ? std::string{} : ": " + why));
    }
    impl_->mounted = true;
}

void Frontend::serve() {
    if (!impl_->mounted) {
        throw std::logic_error("Frontend::serve before mount");
    }
#if FUSE_USE_VERSION >= 312
    // Без явного предела libfuse 3.12+ держит не больше 10 рабочих потоков: десять write,
    // ждущих места в staging (SPEC 8.3), заняли бы их все, и остальные запросы стояли бы до
    // ENOSPC. Потоки создаются по требованию, поэтому большой предел ничего не стоит в покое.
    fuse_loop_config* config = fuse_loop_cfg_create();
    fuse_loop_cfg_set_clone_fd(config, 1);
    fuse_loop_cfg_set_idle_threads(config, 10);
    fuse_loop_cfg_set_max_threads(config, kMaxWorkerThreads);
    const int rc = fuse_session_loop_mt(impl_->session, config);
    fuse_loop_cfg_destroy(config);
#else
    // libfuse 3.10-3.11: предела числа потоков нет.
    fuse_loop_config config{};
    config.clone_fd = 1;
    config.max_idle_threads = 10;
    const int rc = fuse_session_loop_mt(impl_->session, &config);
#endif
    if (rc < 0) {
        throw FuseError(std::string("FUSE loop failed: ") + std::generic_category().message(-rc));
    }
}

void Frontend::close() noexcept {
    if (impl_->session == nullptr) {
        return;
    }
    fuse_session* se = impl_->session;
    if (impl_->mounted) {
        fuse_session_unmount(se);
        impl_->mounted = false;
    }
    if (impl_->signals) {
        fuse_remove_signal_handlers(se);
        impl_->signals = false;
    }
    // Если ядро не присылало DESTROY, libfuse вызовет destroy здесь (при got_init), а если
    // INIT не было — run_destroy() ниже.
    fuse_session_destroy(se);
    impl_->session = nullptr;
    impl_->run_destroy();
}

}  // namespace telega::fuse
