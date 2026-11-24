#include "app/ctl_handlers.h"

#include <exception>
#include <string>

#include "app/status.h"
#include "meta/committer.h"
#include "meta/errors.h"

namespace telega::app {

ctl::Handlers make_mount_handlers(vfs::Vfs& vfs, meta::MountedFs& mounted,
                                  const ProfilePaths& paths, bool read_only_mount) {
    using nlohmann::json;
    ctl::Handlers h;

    // status: state.sqlite читается отдельным читательским соединением (WAL), без блокировок
    // MetaStore и StateStore, поэтому не мешает ни FUSE, ни заливке; живые счётчики — от Vfs.
    h.status = [&vfs, &mounted, paths]() -> json {
        FsStatus s = read_state_status(paths.state_db(), paths.staging());
        add_live_status(s, vfs.status(), mounted.committer().stats(), mounted.read_only());
        return status_to_json(s);
    };

    // sync: SPEC 7.3 — после успеха пространство имён переживает сбой. Vfs::sync заливает всё
    // грязное и коммитит; в --ro он успешен (сохранять нечего), после CAS-конфликта — EIO.
    h.sync = [&vfs, &mounted, read_only_mount]() -> json {
        try {
            vfs.sync();
        } catch (const std::exception& e) {
            const bool conflict = mounted.read_only() && !read_only_mount;
            throw ctl::Error(conflict ? ctl::code::read_only : ctl::code::io, e.what());
        }
        return json{{"generation", mounted.committer().base_generation()},
                    {"read_only", read_only_mount}};
    };

    // snapshot: следующий коммит — снимок (CommitManager::commit(true)), ответ после него.
    h.snapshot = [&mounted]() -> json {
        if (mounted.read_only()) {
            throw ctl::Error(ctl::code::read_only,
                             "the file system is read-only: no commit can be made");
        }
        try {
            const meta::CommitResult r = mounted.committer().commit(/*force_snapshot=*/true);
            return json{{"generation", r.metrics.generation},
                        {"msg_id", r.metrics.msg_id},
                        {"blob_bytes", r.metrics.blob_bytes},
                        {"committed", r.committed}};
        } catch (const meta::CommitError& e) {
            throw ctl::Error(ctl::code::io, e.what());
        }
    };
    return h;
}

}  // namespace telega::app
