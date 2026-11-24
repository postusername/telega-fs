#include "app/cmd_maint.h"

#include <spdlog/spdlog.h>
#include <unistd.h>

#include <chrono>
#include <iostream>
#include <memory>
#include <string>

#include "app/cli_util.h"
#include "app/ctl_client.h"
#include "app/maint_session.h"
#include "app/status.h"
#include "app/telegram_cmds.h"
#include "common/files.h"
#include "meta/mount.h"
#include "store/local_dir_store.h"
#include "vfs/pack_compact.h"

namespace telega::app {

namespace fs = std::filesystem;

namespace {

std::string profile_hint(const GlobalArgs& global) {
    return global.profile == "main" ? std::string{} : " -p " + global.profile;
}

// --- status ---

int status_impl(const StatusArgs& args) {
    const Loaded l = load_profile(args.global);
    FsStatus status;
    // Смонтирована ли ФС, решает сокет: оставшийся от упавшего процесса файл отвечает
    // ECONNREFUSED и считается «не смонтирована» (SPEC 12.4).
    const auto reply = ctl::call(l.paths.ctl_sock(), "status", std::chrono::seconds(30));
    if (reply) {
        if (!reply->ok) {
            throw CommandError("status failed: " + reply->message + " (" + reply->code + ")");
        }
        status = status_from_json(reply->result);
    } else {
        // Без монтирования: state.sqlite только на чтение, без lock (SPEC 12.3).
        status = read_state_status(l.paths.state_db(), l.paths.staging());
    }
    if (args.json) {
        std::cout << status_to_json(status).dump(2) << '\n';
    } else {
        std::cout << format_status(status, args.global.profile);
    }
    return 0;
}

// --- sync ---

int sync_impl(const SyncArgs& args) {
    const Loaded l = load_profile(args.global);
    // Без ограничения по времени: сервер сам ждёт не дольше sync_timeout (SPEC 10.2).
    const auto reply = ctl::call(l.paths.ctl_sock(), "sync");
    if (!reply) {
        throw CommandError("profile '" + args.global.profile +
                           "' is not mounted: sync works only for a mounted file system "
                           "(telega-fs mount" +
                           profile_hint(args.global) + " DIR)");
    }
    if (!reply->ok) {
        throw CommandError("sync failed: " + reply->message);
    }
    std::cout << "synced: generation " << reply->result.value("generation", 0) << '\n';
    return 0;
}

// --- gc ---

int gc_impl(const GcArgs& args) {
    const Loaded l = load_profile(args.global);
    const Profile& p = l.profile;
    crypto::init();
    if (!fs::exists(l.paths.state_db())) {
        throw CommandError("no file system in profile '" + args.global.profile +
                           "': run `telega-fs init` first");
    }
    setup_logging(args.global.verbose, ::isatty(STDERR_FILENO) != 0);

    // lock держит смонтированный процесс: при нём gc отказывается (SPEC 12.3).
    std::unique_ptr<MaintSession> session;
    try {
        session = std::make_unique<MaintSession>(args.global);
    } catch (const ConfigError& e) {
        throw CommandError(std::string(e.what()) +
                           ": gc works only without a mounted file system; unmount it (or wait "
                           "for the other maintenance command) first");
    }

    // ФС без FUSE и Vfs: ключи разблокированы, индекс выбран по таблице 7.2, коммитер готов
    // (SPEC 6.5, «Уплотнение»: нужны только MetaStore, state.sqlite, Store и коммит).
    store::Store& store = session->open_store(false);
    meta::MountOptions mo;
    mo.key = session->key();
    if (p.store == StoreKind::telegram) {
        // Кэш TDLib может до минуты скрывать голову после размонтирования (SPEC 2).
        mo.stale_head_window = mo.commit.cas_retry_window;
        mo.stale_head_period = mo.commit.cas_retry_period;
    }
    mo.commit.commit_interval = p.commit_interval;
    mo.commit.commit_every_chunks = p.commit_every_chunks;
    mo.commit.patch_take_rows = p.patch_take_rows;
    mo.commit.max_chain = p.max_chain;
    mo.commit.checkpoint_ratio = p.checkpoint_ratio;
    mo.commit.retain_generations = p.retain_generations;
    mo.start_background = false;  // коммиты только явные
    std::unique_ptr<meta::MountedFs> mounted = meta::mount_fs(store, session->paths(), mo);
    session->start_read_cache();  // уплотнение скачивает пакеты (SPEC 8.2)
    session->wipe_secrets();
    wipe(mo.key.passphrase);

    bool failed = false;
    try {
        // Обычная отложенная уборка (SPEC 7.4) с сегодняшним retain_generations.
        const auto gc = mounted->committer().collect_garbage();
        std::cout << "gc: deleted " << gc.chunks_deleted << " chunk message(s) and "
                  << gc.blobs_deleted << " old index blob(s)\n";

        if (args.compact) {
            const vfs::CompactContext cc{.meta = mounted->meta(),
                                         .state = mounted->state(),
                                         .store = mounted->store(),
                                         .committer = mounted->committer(),
                                         .keys = mounted->keys(),
                                         .tmp_dir = l.paths.tmp(),
                                         .budget = nullptr,
                                         .abort = {}};
            vfs::CompactOptions co;
            co.ratio = p.compact_ratio;
            co.download_concurrency = p.download_concurrency;
            co.read_timeout = p.read_timeout;
            const vfs::CompactResult r = vfs::compact_packs(cc, co);
            std::cout << "compact: " << r.packs_selected << " pack(s) below ratio "
                      << p.compact_ratio << ", " << r.objects_moved << " object(s) moved into "
                      << r.new_packs << " new pack(s) (" << r.bytes_moved << " bytes)";
            if (r.objects_skipped > 0) {
                std::cout << ", " << r.objects_skipped << " skipped (changed meanwhile)";
            }
            std::cout << (r.committed ? ", committed" : ", nothing to commit") << '\n';
            if (r.committed) {
                // Старые пакеты стали мёртвыми только в новом поколении: их сообщения уйдут
                // уборкой после retain_generations (SPEC 7.4); шаг 9 коммита уже отработал.
                std::cout << "compacted packs are deleted from the channel after "
                          << p.retain_generations << " more generation(s) (retain_generations)\n";
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "telega-fs: " << describe_exception(e) << '\n';
        failed = true;
    }
    try {
        mounted->unmount();  // clean_shutdown = 1; финальный коммит, если что-то осталось
    } catch (const std::exception& e) {
        std::cerr << "telega-fs: " << describe_exception(e) << '\n';
        failed = true;
    }
    mounted.reset();  // раньше хранилища и клиента TDLib, которые живут в session
    return failed ? 1 : 0;
}

template <typename F>
int guarded(F&& f) {
    try {
        return f();
    } catch (const std::exception& e) {
        std::cerr << "telega-fs: " << describe_exception(e) << '\n';
        return 1;
    }
}

}  // namespace

int run_status(const StatusArgs& args) {
    return guarded([&] { return status_impl(args); });
}

int run_sync(const SyncArgs& args) {
    return guarded([&] { return sync_impl(args); });
}

int run_gc(const GcArgs& args) {
    return guarded([&] { return gc_impl(args); });
}

}  // namespace telega::app
