#include <unistd.h>

#include <chrono>
#include <iostream>
#include <memory>
#include <string>

#include "app/cli_util.h"
#include "app/maint_cmds.h"
#include "app/maint_session.h"
#include "crypto/keys.h"
#include "meta/fsck.h"
#include "meta/mount.h"

namespace telega::app {

namespace {

const char* const kHeadline = "fsck: ";

void print_issue(const meta::FsckIssue& issue) {
    std::cout << "  [" << meta::fsck_kind_name(issue.kind) << "] " << issue.detail
              << (issue.repairable ? "" : "  (needs manual attention)") << '\n';
}

void print_known_bad(meta::MetaStore& meta, const meta::FsckReport& report) {
    if (report.known_bad_chunks.empty()) {
        return;
    }
    std::cout << "Chunks marked as lost or damaged earlier (content_mac is zero): "
              << report.known_bad_chunks.size() << '\n';
    for (const std::string& path : meta::files_using_chunks(meta, report.known_bad_chunks)) {
        std::cout << "  affected file: " << path << '\n';
    }
}

void print_summary(const meta::FsckReport& report, meta::MountedFs& fs, bool verify) {
    std::cout << kHeadline << "generation " << fs.committer().base_generation() << ", "
              << report.chunks << " chunks, " << report.extents << " extents, "
              << report.messages_checked << " messages checked";
    if (verify) {
        std::cout << ", " << report.chunks_verified << " chunks verified";
    }
    std::cout << '\n';
}

int fsck_impl(const FsckArgs& args) {
    crypto::init();
    MaintSession session{args.global};
    const Profile& p = session.profile();
    store::Store& store = session.open_store(/*read_only=*/!args.repair);

    meta::MountOptions mo;
    mo.key = session.key();
    mo.read_only = !args.repair;
    mo.maintenance = true;  // fsck ничего не удаляет в канале
    mo.start_background = false;
    if (p.store == StoreKind::telegram) {
        mo.stale_head_window = mo.commit.cas_retry_window;
        mo.stale_head_period = mo.commit.cas_retry_period;
    }
    mo.commit.max_chain = p.max_chain;
    mo.commit.checkpoint_ratio = p.checkpoint_ratio;
    mo.commit.retain_generations = p.retain_generations;
    const std::unique_ptr<meta::MountedFs> fs = meta::mount_fs(store, session.paths(), mo);
    session.wipe_secrets();
    session.start_read_cache();

    meta::FsckOptions options;
    options.verify_data = args.verify_data;
    if (p.store == StoreKind::telegram) {
        options.batch_pause = p.read_pause;
    }
    const bool tty = ::isatty(STDERR_FILENO) != 0;
    if (tty) {
        options.progress = [](const std::string& text) { std::cerr << '\r' << text << std::flush; };
    }
    const meta::FsckReport report =
        meta::fsck_check(fs->meta(), &fs->state(), store, fs->keys(), options);
    if (tty) {
        std::cerr << '\r' << std::string(60, ' ') << '\r';
    }
    print_summary(report, *fs, args.verify_data);
    for (const meta::FsckIssue& issue : report.issues) {
        print_issue(issue);
    }
    print_known_bad(fs->meta(), report);

    if (report.clean()) {
        std::cout << "OK: no problems found\n";
        return 0;
    }
    std::cout << report.issues.size() << " problem(s) found\n";
    if (!args.repair) {
        std::cout << "Run `telega-fs fsck --repair` to fix the index (nothing is deleted in the "
                     "channel)\n";
        return 2;
    }

    const auto generation = static_cast<int64_t>(fs->committer().base_generation());
    const meta::RepairResult repaired = meta::fsck_repair(fs->meta(), report, generation);
    std::cout << "Repaired the index: refcount " << repaired.refcounts_fixed << ", live_bytes "
              << repaired.live_bytes_fixed << ", dead_since_gen " << repaired.dead_since_fixed
              << ", counters " << repaired.counters_fixed << ", chunks marked lost "
              << repaired.macs_zeroed << (repaired.inline_bytes_fixed ? ", inline_bytes" : "")
              << '\n';
    if (!repaired.affected_files.empty()) {
        std::cout << "Files whose data is lost or damaged (reads of them fail with EIO):\n";
        for (const std::string& path : repaired.affected_files) {
            std::cout << "  " << path << '\n';
        }
    }
    if (repaired.changed()) {
        try {
            fs->committer().commit_now();
            std::cout << "Committed generation " << fs->committer().base_generation() << '\n';
        } catch (const std::exception& e) {
            std::cerr << "telega-fs: the index is repaired locally, but the commit failed: "
                      << describe_exception(e) << "\nThe next mount commits the repair.\n";
            return 1;
        }
    }
    // Что осталось после исправления: повторная проверка индекса (Store не нужен).
    meta::FsckOptions again;
    again.check_messages = false;
    const meta::FsckReport left =
        meta::fsck_check(fs->meta(), &fs->state(), store, fs->keys(), again);
    const bool manual = report.has_unrepairable() || !left.clean();
    for (const meta::FsckIssue& issue : left.issues) {
        print_issue(issue);
    }
    if (manual) {
        std::cout << "Some problems cannot be repaired automatically (see above)\n";
        return 2;
    }
    return 0;
}

}  // namespace

int run_fsck(const FsckArgs& args) {
    try {
        return fsck_impl(args);
    } catch (const std::exception& e) {
        std::cerr << "telega-fs: " << describe_exception(e) << '\n';
        return 1;
    }
}

}  // namespace telega::app
