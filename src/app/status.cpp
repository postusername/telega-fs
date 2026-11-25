#include "app/status.h"

#include <sys/stat.h>

#include <array>
#include <cstdio>
#include <iterator>
#include <set>
#include <sstream>
#include <stdexcept>

#include "common/bytes.h"
#include "meta/state_db.h"

namespace telega::app {

namespace fs = std::filesystem;
using meta::Db;
using meta::Stmt;
namespace state_key = meta::state_key;

namespace {

std::optional<int64_t> state_int(Db& db, std::string_view key) {
    Stmt st{db, "SELECT value FROM state WHERE key = ?"};
    st.bind_text(1, key);
    if (!st.step() || st.is_null(0)) {
        return std::nullopt;
    }
    return st.column_int64(0);
}

std::optional<Bytes> state_blob(Db& db, std::string_view key) {
    Stmt st{db, "SELECT value FROM state WHERE key = ?"};
    st.bind_text(1, key);
    if (!st.step() || st.is_null(0)) {
        return std::nullopt;
    }
    return st.column_blob(0);
}

std::string plural(uint64_t n, const char* one, const char* many) {
    return std::to_string(n) + ' ' + (n == 1 ? one : many);
}

}  // namespace

std::string format_bytes(uint64_t bytes) {
    static constexpr std::array<const char*, 6> kUnits{"B", "KiB", "MiB", "GiB", "TiB", "PiB"};
    if (bytes < 1024) {
        return std::to_string(bytes) + " B";
    }
    auto v = static_cast<double>(bytes);
    size_t unit = 0;
    while (v >= 1024.0 && unit + 1 < kUnits.size()) {
        v /= 1024.0;
        ++unit;
    }
    std::array<char, 32> buf{};
    std::snprintf(buf.data(), buf.size(), "%.1f %s", v, kUnits[unit]);
    return buf.data();
}

FsStatus read_state_status(const fs::path& state_db, const fs::path& staging_dir) {
    std::error_code ec;
    if (!fs::exists(state_db, ec)) {
        throw std::runtime_error("no file system state in " + state_db.parent_path().string() +
                                 ": run `telega-fs init` first");
    }
    meta::OpenOptions oo;
    oo.read_only = true;
    oo.wal = false;
    Db db = Db::open(state_db, oo);

    FsStatus s;
    if (const auto uuid = state_blob(db, state_key::fs_uuid)) {
        s.fs_uuid = to_hex(*uuid);
    }
    s.generation = static_cast<uint64_t>(state_int(db, state_key::base_generation).value_or(0));
    s.head_msg_id = state_int(db, state_key::head_msg_id).value_or(0);
    s.index_dirty = state_int(db, state_key::index_dirty).value_or(0) != 0;
    s.force_snapshot = state_int(db, state_key::force_snapshot).value_or(0) != 0;
    s.clean_shutdown = state_int(db, state_key::clean_shutdown).value_or(1) != 0;
    {
        Stmt st{db, "SELECT COUNT(*), COALESCE(SUM(blob_size), 0) FROM chain"};
        st.step();
        s.chain_length = static_cast<size_t>(st.column_int64(0));
        s.chain_bytes = st.column_int64(1);
    }
    {
        Stmt st{db, "SELECT COUNT(*) FROM pending_patchsets"};
        st.step();
        s.pending_patchsets = static_cast<size_t>(st.column_int64(0));
    }
    {
        // Размер — длина staging-файла (логическая, как в учёте staging). Файл мог исчезнуть
        // между чтением строки и stat: тогда он не считается.
        Stmt st{db, "SELECT ino, path FROM dirty_chunks"};
        std::set<int64_t> inos;
        while (st.step()) {
            ++s.dirty_chunks;
            inos.insert(st.column_int64(0));
            struct stat sb {};
            if (::stat((staging_dir / st.column_text(1)).c_str(), &sb) == 0) {
                s.dirty_bytes += static_cast<uint64_t>(sb.st_size);
            }
        }
        s.dirty_inodes = inos.size();
    }
    try {
        // В state.sqlite старой схемы open_pack без столбца added_ns и всегда пуста.
        Stmt st{db, "SELECT COUNT(*), COALESCE(SUM(len), 0) FROM open_pack"};
        st.step();
        s.open_pack_objects = static_cast<size_t>(st.column_int64(0));
        s.open_pack_bytes = st.column_int64(1);
    } catch (const meta::SqliteError&) {
        s.open_pack_objects = 0;
        s.open_pack_bytes = 0;
    }
    s.upload_queued = s.dirty_chunks;
    return s;
}

void add_live_status(FsStatus& status, const vfs::VfsStatus& vfs, const meta::CommitStats& commit,
                     bool read_only) {
    status.mounted = true;
    status.read_only = read_only;
    status.upload_queued = vfs.queued;
    status.upload_active = vfs.uploading;
    status.upload_failed = vfs.failed;
    status.staging_used = vfs.staging_used;
    status.staging_limit = vfs.staging_limit;
    status.uploaded_chunks = vfs.uploaded_chunks;
    status.deduped_chunks = vfs.deduped_chunks;
    status.commits = commit.commits;
    if (!vfs.last_error.empty()) {
        status.errors.push_back("upload: " + vfs.last_error);
    }
    if (!commit.last_error.empty()) {
        status.errors.push_back("commit: " + commit.last_error);
    }
    if (vfs.fatal) {
        status.errors.emplace_back("the upload machinery stopped after a fatal error");
    }
}

nlohmann::json status_to_json(const FsStatus& s) {
    nlohmann::json j;
    j["mounted"] = s.mounted;
    j["read_only"] = s.read_only;
    j["fs_uuid"] = s.fs_uuid;
    j["generation"] = s.generation;
    j["head_msg_id"] = s.head_msg_id;
    j["chain_length"] = s.chain_length;
    j["chain_bytes"] = s.chain_bytes;
    j["index_dirty"] = s.index_dirty;
    j["force_snapshot"] = s.force_snapshot;
    j["clean_shutdown"] = s.clean_shutdown;
    j["pending_patchsets"] = s.pending_patchsets;
    j["dirty_chunks"] = s.dirty_chunks;
    j["dirty_inodes"] = s.dirty_inodes;
    j["dirty_bytes"] = s.dirty_bytes;
    j["open_pack_objects"] = s.open_pack_objects;
    j["open_pack_bytes"] = s.open_pack_bytes;
    j["upload_queued"] = s.upload_queued;
    j["upload_active"] = s.upload_active;
    j["upload_failed"] = s.upload_failed;
    j["uploaded_chunks"] = s.uploaded_chunks;
    j["deduped_chunks"] = s.deduped_chunks;
    j["staging_used"] = s.staging_used;
    j["staging_limit"] = s.staging_limit;
    j["commits"] = s.commits;
    j["errors"] = s.errors;
    return j;
}

FsStatus status_from_json(const nlohmann::json& j) {
    FsStatus s;
    const FsStatus d;
    s.mounted = j.value("mounted", d.mounted);
    s.read_only = j.value("read_only", d.read_only);
    s.fs_uuid = j.value("fs_uuid", d.fs_uuid);
    s.generation = j.value("generation", d.generation);
    s.head_msg_id = j.value("head_msg_id", d.head_msg_id);
    s.chain_length = j.value("chain_length", d.chain_length);
    s.chain_bytes = j.value("chain_bytes", d.chain_bytes);
    s.index_dirty = j.value("index_dirty", d.index_dirty);
    s.force_snapshot = j.value("force_snapshot", d.force_snapshot);
    s.clean_shutdown = j.value("clean_shutdown", d.clean_shutdown);
    s.pending_patchsets = j.value("pending_patchsets", d.pending_patchsets);
    s.dirty_chunks = j.value("dirty_chunks", d.dirty_chunks);
    s.dirty_inodes = j.value("dirty_inodes", d.dirty_inodes);
    s.dirty_bytes = j.value("dirty_bytes", d.dirty_bytes);
    s.open_pack_objects = j.value("open_pack_objects", d.open_pack_objects);
    s.open_pack_bytes = j.value("open_pack_bytes", d.open_pack_bytes);
    s.upload_queued = j.value("upload_queued", d.upload_queued);
    s.upload_active = j.value("upload_active", d.upload_active);
    s.upload_failed = j.value("upload_failed", d.upload_failed);
    s.uploaded_chunks = j.value("uploaded_chunks", d.uploaded_chunks);
    s.deduped_chunks = j.value("deduped_chunks", d.deduped_chunks);
    s.staging_used = j.value("staging_used", d.staging_used);
    s.staging_limit = j.value("staging_limit", d.staging_limit);
    s.commits = j.value("commits", d.commits);
    if (j.contains("errors") && j["errors"].is_array()) {
        for (const auto& e : j["errors"]) {
            if (e.is_string()) {
                s.errors.push_back(e.get<std::string>());
            }
        }
    }
    return s;
}

std::string format_status(const FsStatus& s, const std::string& profile) {
    std::ostringstream out;
    const auto line = [&](const char* name, const std::string& value) {
        std::string label = std::string(name) + ':';
        label.resize(18, ' ');
        out << label << value << '\n';
    };
    out << "telega-fs profile '" << profile << "'\n";
    std::string state = "not mounted";
    if (s.mounted) {
        state = s.read_only ? "mounted, read-only" : "mounted";
    }
    line("state", state);
    if (!s.fs_uuid.empty()) {
        line("file system", s.fs_uuid);
    }
    line("generation",
         std::to_string(s.generation) + " (head message " + std::to_string(s.head_msg_id) + ")");
    line("chain", plural(s.chain_length, "element", "elements") + ", " +
                      format_bytes(static_cast<uint64_t>(s.chain_bytes)));
    std::string index = s.index_dirty ? "uncommitted changes" : "committed";
    if (s.pending_patchsets > 0) {
        index += ", " + plural(s.pending_patchsets, "patch", "patches") + " taken, not committed";
    }
    line("index", index);
    if (s.force_snapshot) {
        line("next commit", "snapshot");
    }
    line("dirty data", plural(s.dirty_chunks, "chunk", "chunks") + " in " +
                           plural(s.dirty_inodes, "file", "files") + ", " +
                           format_bytes(s.dirty_bytes));
    if (s.open_pack_objects > 0) {
        line("open pack", plural(s.open_pack_objects, "object", "objects") + ", " +
                              format_bytes(static_cast<uint64_t>(s.open_pack_bytes)));
    }
    if (s.mounted) {
        line("upload queue", std::to_string(s.upload_queued) + " queued, " +
                                 std::to_string(s.upload_active) + " uploading, " +
                                 std::to_string(s.upload_failed) + " failed (" +
                                 plural(s.uploaded_chunks, "chunk", "chunks") + " uploaded, " +
                                 std::to_string(s.deduped_chunks) + " deduplicated since mount)");
        line("staging", format_bytes(s.staging_used) + " of " + format_bytes(s.staging_limit));
        line("commits", std::to_string(s.commits) + " since mount");
    } else {
        line("upload queue", std::to_string(s.upload_queued) + " waiting for the next mount");
        if (!s.clean_shutdown) {
            line("previous session", "ended uncleanly");
        }
    }
    if (s.errors.empty()) {
        line("last errors",
             s.mounted ? "none" : "none recorded (errors are kept only while mounted)");
    } else {
        for (size_t i = 0; i < s.errors.size(); ++i) {
            line(i == 0 ? "last errors" : "", s.errors[i]);
        }
    }
    return out.str();
}

}  // namespace telega::app
