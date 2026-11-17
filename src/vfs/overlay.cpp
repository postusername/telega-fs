#include "vfs/overlay.h"

#include <algorithm>
#include <mutex>
#include <system_error>

namespace telega::vfs {

namespace {

constexpr size_t kMaxCoverageRanges = 64;

}  // namespace

Overlay::Overlay(meta::StateStore& state, std::filesystem::path staging_dir, int64_t chunk_size)
    : state_(state), dir_(std::move(staging_dir)), chunk_size_(static_cast<uint64_t>(chunk_size)) {}

void Overlay::load() {
    auto rows = state_.list_dirty_chunks();
    auto attrs = state_.list_dirty_attrs();
    const std::unique_lock lock(mu_);
    chunks_.clear();
    attrs_.clear();
    for (const meta::DirtyChunk& row : rows) {
        Entry& e = insert_locked({row.ino, row.idx}, row.version, row.last_write_ns);
        e.file = row.path;
        // Покрытие до перезапуска неизвестно: потоковой заливки для таких позиций не будет,
        // их запечатывает старт, release или таймер.
        e.coverage_lost = true;
    }
    for (const meta::DirtyAttrs& a : attrs) {
        attrs_[a.ino] = a;
    }
}

std::string Overlay::file_name(const ChunkKey& key) {
    return std::to_string(key.ino) + "-" + std::to_string(key.idx);
}

std::filesystem::path Overlay::path_of(const ChunkKey& key) const { return dir_ / file_name(key); }

std::optional<DirtyView> Overlay::chunk(const ChunkKey& key) const {
    const std::shared_lock lock(mu_);
    const auto it = chunks_.find(key);
    if (it == chunks_.end()) {
        return std::nullopt;
    }
    return DirtyView{it->second.version, it->second.incarnation, it->second.last_write_ns,
                     it->second.cut_len};
}

std::vector<int64_t> Overlay::indices(int64_t ino) const {
    const std::shared_lock lock(mu_);
    std::vector<int64_t> out;
    for (auto it = chunks_.lower_bound({ino, 0}); it != chunks_.end() && it->first.ino == ino;
         ++it) {
        out.push_back(it->first.idx);
    }
    return out;
}

bool Overlay::has_chunks(int64_t ino) const {
    const std::shared_lock lock(mu_);
    const auto it = chunks_.lower_bound({ino, 0});
    return it != chunks_.end() && it->first.ino == ino;
}

std::optional<meta::DirtyAttrs> Overlay::attrs(int64_t ino) const {
    const std::shared_lock lock(mu_);
    const auto it = attrs_.find(ino);
    if (it == attrs_.end()) {
        return std::nullopt;
    }
    return it->second;
}

bool Overlay::has_attrs() const {
    const std::shared_lock lock(mu_);
    return !attrs_.empty();
}

std::unordered_map<int64_t, meta::DirtyAttrs> Overlay::attrs_snapshot() const {
    const std::shared_lock lock(mu_);
    return attrs_;
}

std::vector<int64_t> Overlay::chunk_inos() const {
    const std::shared_lock lock(mu_);
    std::vector<int64_t> out;
    for (const auto& [key, e] : chunks_) {
        if (out.empty() || out.back() != key.ino) {
            out.push_back(key.ino);
        }
    }
    return out;
}

std::vector<int64_t> Overlay::attr_inos() const {
    const std::shared_lock lock(mu_);
    std::vector<int64_t> out;
    out.reserve(attrs_.size());
    for (const auto& [ino, a] : attrs_) {
        out.push_back(ino);
    }
    std::sort(out.begin(), out.end());
    return out;
}

bool Overlay::fully_covered(const ChunkKey& key) const {
    const std::shared_lock lock(mu_);
    const auto it = chunks_.find(key);
    if (it == chunks_.end() || it->second.coverage_lost) {
        return false;
    }
    const auto& cov = it->second.covered;
    return cov.size() == 1 && cov.front().first == 0 && cov.front().second >= chunk_size_;
}

OverlayCounts Overlay::counts() const {
    const std::shared_lock lock(mu_);
    OverlayCounts c;
    c.chunks = chunks_.size();
    int64_t last_ino = 0;
    for (const auto& [key, e] : chunks_) {
        if (c.inodes == 0 || key.ino != last_ino) {
            ++c.inodes;
            last_ino = key.ino;
        }
        c.queued += e.queued ? 1 : 0;
        c.uploading += e.uploading ? 1 : 0;
        c.failed += (e.failed_version != 0 && e.failed_version >= e.version) ? 1 : 0;
    }
    return c;
}

Overlay::Entry& Overlay::insert_locked(const ChunkKey& key, int64_t version, int64_t now_ns) {
    Entry& e = chunks_[key];
    e.file = file_name(key);
    e.version = version;
    e.last_write_ns = now_ns;
    e.incarnation = next_incarnation_++;
    return e;
}

void Overlay::add_coverage(Entry& e, uint64_t from, uint64_t to) {
    if (e.coverage_lost || from >= to) {
        return;
    }
    auto& cov = e.covered;
    // Вставить и слить пересекающиеся и смежные отрезки.
    cov.insert(std::lower_bound(cov.begin(), cov.end(), std::make_pair(from, to)), {from, to});
    std::vector<std::pair<uint64_t, uint64_t>> merged;
    merged.reserve(cov.size());
    for (const auto& r : cov) {
        if (!merged.empty() && r.first <= merged.back().second) {
            merged.back().second = std::max(merged.back().second, r.second);
        } else {
            merged.push_back(r);
        }
    }
    cov = std::move(merged);
    if (cov.size() > kMaxCoverageRanges) {
        e.coverage_lost = true;
        cov.clear();
    }
}

void Overlay::commit_write(int64_t ino, std::span<const ChunkWrite> writes,
                           const meta::DirtyAttrs& attrs, int64_t now_ns) {
    std::vector<int64_t> versions;
    versions.reserve(writes.size());
    state_.transaction([&] {
        for (const ChunkWrite& w : writes) {
            versions.push_back(
                state_.upsert_dirty_chunk(ino, w.idx, file_name({ino, w.idx}), now_ns));
        }
        state_.set_dirty_attrs(attrs);
    });
    const std::unique_lock lock(mu_);
    for (size_t i = 0; i < writes.size(); ++i) {
        const ChunkKey key{ino, writes[i].idx};
        auto it = chunks_.find(key);
        Entry* e = nullptr;
        if (it == chunks_.end()) {
            e = &insert_locked(key, versions[i], now_ns);
        } else {
            e = &it->second;
            e->version = versions[i];
            e->last_write_ns = now_ns;
        }
        add_coverage(*e, writes[i].from, writes[i].to);
    }
    attrs_[ino] = attrs;
}

void Overlay::cut(const ChunkKey& key, int64_t cut_len, int64_t now_ns) {
    const int64_t version = state_.upsert_dirty_chunk(key.ino, key.idx, file_name(key), now_ns);
    const std::unique_lock lock(mu_);
    auto it = chunks_.find(key);
    Entry& e = it == chunks_.end() ? insert_locked(key, version, now_ns) : it->second;
    e.version = version;
    e.last_write_ns = now_ns;
    e.cut_len = std::min(e.cut_len, cut_len);
    // Покрытие за новым концом больше не записано.
    for (auto& r : e.covered) {
        r.second = std::min(r.second, static_cast<uint64_t>(cut_len));
    }
    std::erase_if(e.covered, [](const auto& r) { return r.first >= r.second; });
}

void Overlay::set_attrs(const meta::DirtyAttrs& attrs) {
    state_.set_dirty_attrs(attrs);
    const std::unique_lock lock(mu_);
    attrs_[attrs.ino] = attrs;
}

void Overlay::clear_attrs(int64_t ino) {
    state_.delete_dirty_attrs(ino);
    const std::unique_lock lock(mu_);
    attrs_.erase(ino);
}

void Overlay::remove_file(const ChunkKey& key) const {
    std::error_code ec;
    std::filesystem::remove(path_of(key), ec);
}

void Overlay::drop_from(int64_t ino, int64_t first_idx) {
    std::vector<ChunkKey> keys;
    {
        const std::shared_lock lock(mu_);
        for (auto it = chunks_.lower_bound({ino, first_idx});
             it != chunks_.end() && it->first.ino == ino; ++it) {
            keys.push_back(it->first);
        }
    }
    if (keys.empty()) {
        return;
    }
    state_.transaction([&] {
        for (const ChunkKey& k : keys) {
            const auto row = state_.get_dirty_chunk(k.ino, k.idx);
            if (row) {
                state_.delete_dirty_chunk_if_version(k.ino, k.idx, row->version);
            }
        }
    });
    for (const ChunkKey& k : keys) {
        remove_file(k);
    }
    {
        const std::unique_lock lock(mu_);
        for (const ChunkKey& k : keys) {
            chunks_.erase(k);
        }
    }
    cv_.notify_all();
}

void Overlay::drop_inode(int64_t ino) {
    const std::vector<int64_t> idxs = indices(ino);
    state_.transaction([&] {
        state_.delete_dirty_chunks_for_ino(ino);
        state_.delete_dirty_attrs(ino);
    });
    for (const int64_t idx : idxs) {
        remove_file({ino, idx});
    }
    {
        const std::unique_lock lock(mu_);
        for (const int64_t idx : idxs) {
            chunks_.erase({ino, idx});
        }
        attrs_.erase(ino);
    }
    cv_.notify_all();
}

void Overlay::drop_position(const ChunkKey& key, bool clear_attrs, bool persist) {
    if (persist) {
        state_.transaction([&] {
            if (const auto row = state_.get_dirty_chunk(key.ino, key.idx)) {
                state_.delete_dirty_chunk_if_version(key.ino, key.idx, row->version);
            }
            if (clear_attrs) {
                state_.delete_dirty_attrs(key.ino);
            }
        });
        remove_file(key);
    }
    {
        const std::unique_lock lock(mu_);
        chunks_.erase(key);
        if (clear_attrs) {
            attrs_.erase(key.ino);
        }
    }
    cv_.notify_all();
}

void Overlay::finish_upload(const ChunkKey& key, uint64_t incarnation, int64_t version,
                            bool clear_attrs, int64_t chunk_id) {
    bool removed = false;
    state_.transaction([&] {
        removed = state_.delete_dirty_chunk_if_version(key.ino, key.idx, version);
        if (clear_attrs) {
            state_.delete_dirty_attrs(key.ino);
        }
        state_.remove_upload(chunk_id);
    });
    if (removed) {
        remove_file(key);
    }
    {
        const std::unique_lock lock(mu_);
        auto it = chunks_.find(key);
        if (it != chunks_.end() && it->second.incarnation == incarnation) {
            if (removed) {
                chunks_.erase(it);
            } else {
                it->second.applied_version = std::max(it->second.applied_version, version);
            }
        }
        if (clear_attrs) {
            attrs_.erase(key.ino);
        }
    }
    cv_.notify_all();
}

bool Overlay::mark_queued(const ChunkKey& key, bool explicit_request) {
    const std::unique_lock lock(mu_);
    auto it = chunks_.find(key);
    if (it == chunks_.end()) {
        return false;
    }
    Entry& e = it->second;
    if (e.failed_version != 0 && e.failed_version >= e.version) {
        if (!explicit_request) {
            return false;
        }
        e.failed_version = 0;  // явный запрос — новая попытка
    }
    if (e.uploading) {
        e.requeue = true;
        return false;
    }
    if (e.queued) {
        return false;
    }
    e.queued = true;
    return true;
}

std::optional<DirtyView> Overlay::begin_upload(const ChunkKey& key) {
    const std::unique_lock lock(mu_);
    auto it = chunks_.find(key);
    if (it == chunks_.end()) {
        return std::nullopt;
    }
    Entry& e = it->second;
    e.queued = false;
    e.uploading = true;
    e.requeue = false;
    e.blocked = false;
    e.cut_len = INT64_MAX;  // снимок берётся сейчас: прежние обрезки в нём уже учтены
    return DirtyView{e.version, e.incarnation, e.last_write_ns, e.cut_len};
}

bool Overlay::end_upload(const ChunkKey& key, uint64_t incarnation,
                         std::optional<int64_t> failed_version) {
    bool again = false;
    {
        const std::unique_lock lock(mu_);
        auto it = chunks_.find(key);
        if (it != chunks_.end() && it->second.incarnation == incarnation) {
            Entry& e = it->second;
            e.uploading = false;
            if (failed_version) {
                e.failed_version = *failed_version;
            }
            if (e.requeue && !e.queued && e.applied_version < e.version &&
                (e.failed_version == 0 || e.failed_version < e.version)) {
                e.queued = true;
                again = true;
            }
            e.requeue = false;
        }
        // Другое воплощение заливкой старого не затрагивается: у него свои флаги.
    }
    cv_.notify_all();
    return again;
}

std::vector<ChunkKey> Overlay::expired(int64_t deadline_ns) const {
    const std::shared_lock lock(mu_);
    std::vector<ChunkKey> out;
    for (const auto& [key, e] : chunks_) {
        if (e.queued || e.uploading || e.blocked || e.last_write_ns > deadline_ns) {
            continue;
        }
        if (e.failed_version != 0 && e.failed_version >= e.version) {
            continue;
        }
        if (e.applied_version >= e.version) {
            continue;  // уже залит, строка вот-вот уйдёт
        }
        out.push_back(key);
    }
    return out;
}

void Overlay::set_blocked(const ChunkKey& key, uint64_t incarnation) {
    const std::unique_lock lock(mu_);
    const auto it = chunks_.find(key);
    if (it != chunks_.end() && it->second.incarnation == incarnation) {
        it->second.blocked = true;
    }
}

std::vector<ChunkKey> Overlay::take_blocked(int64_t ino) {
    const std::unique_lock lock(mu_);
    std::vector<ChunkKey> out;
    for (auto it = chunks_.lower_bound({ino, 0}); it != chunks_.end() && it->first.ino == ino;
         ++it) {
        if (it->second.blocked) {
            it->second.blocked = false;
            out.push_back(it->first);
        }
    }
    return out;
}

std::vector<UploadTarget> Overlay::targets(int64_t ino) const {
    const std::shared_lock lock(mu_);
    std::vector<UploadTarget> out;
    for (auto it = chunks_.lower_bound({ino, 0}); it != chunks_.end() && it->first.ino == ino;
         ++it) {
        out.push_back({it->first, it->second.incarnation, it->second.version});
    }
    return out;
}

Overlay::TargetState Overlay::target_state_locked(const UploadTarget& t) const {
    const auto it = chunks_.find(t.key);
    if (it == chunks_.end() || it->second.incarnation != t.incarnation) {
        return TargetState::done;
    }
    const Entry& e = it->second;
    if (e.applied_version >= t.version) {
        return TargetState::done;
    }
    if (e.failed_version != 0 && e.failed_version >= t.version && !e.queued && !e.uploading) {
        return TargetState::failed;
    }
    return TargetState::pending;
}

bool Overlay::wait(std::span<const UploadTarget> targets, const std::function<bool()>& abort,
                   std::chrono::steady_clock::time_point deadline, bool& timed_out) {
    std::unique_lock lock(mu_);
    bool ok = true;
    timed_out = false;
    const bool satisfied = cv_.wait_until(lock, deadline, [&] {
        if (abort()) {
            ok = false;
            return true;
        }
        bool all_done = true;
        for (const UploadTarget& t : targets) {
            const TargetState s = target_state_locked(t);
            if (s == TargetState::failed) {
                ok = false;
                return true;
            }
            all_done = all_done && s == TargetState::done;
        }
        return all_done;
    });
    if (!satisfied) {
        timed_out = true;
        return false;
    }
    return ok;
}

void Overlay::notify_all() {
    // Пустая критическая секция: ожидающий не пропустит уведомление между проверкой
    // предиката и засыпанием.
    { const std::unique_lock lock(mu_); }
    cv_.notify_all();
}

}  // namespace telega::vfs
