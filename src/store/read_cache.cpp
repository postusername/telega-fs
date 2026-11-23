#include "store/read_cache.h"

#include <spdlog/spdlog.h>
#include <sys/stat.h>

#include <algorithm>
#include <system_error>
#include <utility>

#include "meta/sqlite.h"

namespace telega::store {

namespace fs = std::filesystem;

namespace {

// Тот же текст, что у StateStore (SPEC 6.4): таблицу может создать любой из двоих.
constexpr const char* kSchema = R"sql(
CREATE TABLE IF NOT EXISTS read_cache (
  path TEXT PRIMARY KEY,
  remote_file_id TEXT NOT NULL,
  blocks INTEGER NOT NULL,
  last_access_ns INTEGER NOT NULL
);
)sql";

constexpr uint64_t kBlockBytes = 512;

// Размер на диске по st_blocks; nullopt — файла нет или он не обычный.
bool stat_regular(const std::string& path, uint64_t& bytes, int64_t& mtime_ns) {
    struct stat st {};
    if (::stat(path.c_str(), &st) != 0 || !S_ISREG(st.st_mode)) {
        return false;
    }
    bytes = static_cast<uint64_t>(st.st_blocks) * kBlockBytes;
    mtime_ns = static_cast<int64_t>(st.st_mtim.tv_sec) * 1'000'000'000LL + st.st_mtim.tv_nsec;
    return true;
}

}  // namespace

ReadCache::ReadCache(ReadCacheOptions options) : opts_(std::move(options)) {
    dir_a_ = opts_.dir.lexically_normal().string();
    std::error_code ec;
    dir_b_ = fs::weakly_canonical(opts_.dir, ec).string();
    if (ec) {
        dir_b_ = dir_a_;
    }
    for (std::string* d : {&dir_a_, &dir_b_}) {
        while (d->size() > 1 && d->back() == '/') {
            d->pop_back();
        }
    }
}

ReadCache::~ReadCache() { flush(); }

int64_t ReadCache::now() const {
    if (opts_.now_ns) {
        return opts_.now_ns();
    }
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

bool ReadCache::inside(const std::string& path) const {
    const auto under = [&](const std::string& d) {
        return path.size() > d.size() && path.compare(0, d.size(), d) == 0 && path[d.size()] == '/';
    };
    return under(dir_a_) || under(dir_b_);
}

void ReadCache::note(const std::string& path, const std::string& remote_file_id) {
    if (!inside(path)) {
        return;
    }
    uint64_t bytes = 0;
    int64_t mtime = 0;
    if (!stat_regular(path, bytes, mtime)) {
        forget(path);
        return;
    }
    const int64_t at = now();
    const std::lock_guard<std::mutex> lock(mu_);
    Row& row = rows_[path];
    total_ = total_ - row.bytes + bytes;
    row.bytes = bytes;
    row.last_access_ns = at;
    if (!remote_file_id.empty()) {
        row.remote_file_id = remote_file_id;
    }
    dirty_.insert(path);
    removed_.erase(path);
}

void ReadCache::forget(const std::string& path) {
    const std::lock_guard<std::mutex> lock(mu_);
    const auto it = rows_.find(path);
    if (it == rows_.end()) {
        return;
    }
    total_ -= it->second.bytes;
    rows_.erase(it);
    dirty_.erase(path);
    removed_.insert(path);
}

bool ReadCache::over_limit() const {
    const std::lock_guard<std::mutex> lock(mu_);
    return opts_.limit != 0 && total_ > opts_.limit;
}

uint64_t ReadCache::total_bytes() const {
    const std::lock_guard<std::mutex> lock(mu_);
    return total_;
}

std::vector<ReadCacheEntry> ReadCache::entries() const {
    std::vector<ReadCacheEntry> out;
    const std::lock_guard<std::mutex> lock(mu_);
    out.reserve(rows_.size());
    for (const auto& [path, row] : rows_) {
        out.push_back({path, row.remote_file_id, row.bytes, row.last_access_ns});
    }
    return out;
}

std::vector<ReadCacheEntry> ReadCache::choose_victims(
    const std::function<bool(const ReadCacheEntry&)>& busy) const {
    std::vector<ReadCacheEntry> all = entries();
    uint64_t total = 0;
    for (const ReadCacheEntry& e : all) {
        total += e.bytes;
    }
    std::vector<ReadCacheEntry> victims;
    if (opts_.limit == 0 || total <= opts_.limit) {
        return victims;
    }
    // Вытеснение идёт до 90 % лимита, чтобы не повторяться на каждом файле.
    const uint64_t target = opts_.limit / 10 * 9;
    std::sort(all.begin(), all.end(), [](const ReadCacheEntry& x, const ReadCacheEntry& y) {
        return x.last_access_ns != y.last_access_ns ? x.last_access_ns < y.last_access_ns
                                                    : x.path < y.path;
    });
    for (const ReadCacheEntry& e : all) {
        if (total <= target) {
            break;
        }
        if (busy && busy(e)) {
            continue;
        }
        victims.push_back(e);
        total -= std::min(total, e.bytes);
    }
    return victims;
}

// ---- Таблица ----------------------------------------------------------------------------

void ReadCache::open_db() {
    if (db_ || opts_.state_db.empty()) {
        return;
    }
    try {
        meta::OpenOptions o;
        o.foreign_keys = false;
        db_ = std::make_unique<meta::Db>(meta::Db::open(opts_.state_db, o));
        db_->exec(kSchema);
    } catch (const std::exception& e) {
        spdlog::warn("read cache: cannot open the state database: {}", e.what());
        db_.reset();
    }
}

void ReadCache::flush() {
    std::vector<std::pair<std::string, Row>> upserts;
    std::vector<std::string> deletes;
    {
        const std::lock_guard<std::mutex> lock(mu_);
        if (dirty_.empty() && removed_.empty()) {
            return;
        }
        for (const std::string& p : dirty_) {
            const auto it = rows_.find(p);
            if (it != rows_.end()) {
                upserts.emplace_back(p, it->second);
            }
        }
        deletes.assign(removed_.begin(), removed_.end());
        dirty_.clear();
        removed_.clear();
    }
    const std::lock_guard<std::mutex> db_lock(db_mu_);
    open_db();
    bool ok = false;
    if (db_) {
        try {
            meta::Transaction tx(*db_);
            for (const std::string& p : deletes) {
                meta::Stmt st = db_->cached("DELETE FROM read_cache WHERE path = ?");
                st.bind_text(1, p);
                st.run();
            }
            for (const auto& [p, row] : upserts) {
                meta::Stmt st = db_->cached(
                    "INSERT OR REPLACE INTO read_cache(path, remote_file_id, blocks, "
                    "last_access_ns) VALUES (?, ?, ?, ?)");
                st.bind_text(1, p);
                st.bind_text(2, row.remote_file_id);
                st.bind(3, static_cast<int64_t>(row.bytes));
                st.bind(4, row.last_access_ns);
                st.run();
            }
            tx.commit();
            ok = true;
        } catch (const std::exception& e) {
            spdlog::warn("read cache: cannot save the accounting: {}", e.what());
        }
    }
    if (!ok && db_) {
        // Изменения не сохранены: вернуть их в очередь на следующий раз.
        const std::lock_guard<std::mutex> lock(mu_);
        for (const auto& [p, row] : upserts) {
            if (rows_.contains(p)) {
                dirty_.insert(p);
            }
        }
        for (const std::string& p : deletes) {
            if (!rows_.contains(p)) {
                removed_.insert(p);
            }
        }
    }
}

ReconcileReport ReadCache::reconcile() {
    ReconcileReport report;
    // Строки таблицы.
    std::unordered_map<std::string, Row> table;
    {
        const std::lock_guard<std::mutex> db_lock(db_mu_);
        open_db();
        if (db_) {
            try {
                meta::Stmt st = db_->cached(
                    "SELECT path, remote_file_id, blocks, last_access_ns FROM read_cache");
                while (st.step()) {
                    Row row;
                    row.remote_file_id = st.column_text(1);
                    row.bytes = static_cast<uint64_t>(std::max<int64_t>(st.column_int64(2), 0));
                    row.last_access_ns = st.column_int64(3);
                    table.emplace(st.column_text(0), std::move(row));
                }
            } catch (const std::exception& e) {
                spdlog::warn("read cache: cannot read the accounting table: {}", e.what());
            }
        }
    }
    // Файлы каталога.
    std::unordered_map<std::string, std::pair<uint64_t, int64_t>> on_disk;  // путь → (байты, mtime)
    std::error_code ec;
    if (fs::is_directory(opts_.dir, ec)) {
        fs::recursive_directory_iterator it(opts_.dir,
                                            fs::directory_options::skip_permission_denied, ec);
        for (; !ec && it != fs::recursive_directory_iterator(); it.increment(ec)) {
            std::error_code e2;
            if (!it->is_regular_file(e2) || it->is_symlink(e2)) {
                continue;
            }
            const std::string p = it->path().string();
            uint64_t bytes = 0;
            int64_t mtime = 0;
            if (stat_regular(p, bytes, mtime)) {
                on_disk.emplace(p, std::make_pair(bytes, mtime));
            }
        }
    }
    {
        const std::lock_guard<std::mutex> lock(mu_);
        // Файлы, учтённые до сверки (блобы индекса читаются раньше start_read_cache), несут
        // remote_file_id и свежее время доступа: сверка их не теряет, а дополняет таблицей.
        for (auto& [path, known] : rows_) {
            const auto t = table.find(path);
            if (t == table.end()) {
                table.emplace(path, std::move(known));
                continue;
            }
            if (!known.remote_file_id.empty()) {
                t->second.remote_file_id = std::move(known.remote_file_id);
            }
            t->second.last_access_ns = std::max(t->second.last_access_ns, known.last_access_ns);
        }
        rows_.clear();
        dirty_.clear();
        removed_.clear();
        total_ = 0;
        for (auto& [path, row] : table) {
            const auto d = on_disk.find(path);
            if (d == on_disk.end() || !inside(path)) {
                removed_.insert(path);  // строка без файла
                ++report.dropped;
                continue;
            }
            row.bytes = d->second.first;
            total_ += row.bytes;
            rows_.emplace(path, std::move(row));
            dirty_.insert(path);
            ++report.kept;
        }
        for (const auto& [path, info] : on_disk) {
            if (rows_.contains(path) || !inside(path)) {
                continue;
            }
            // Файл без строки (после сбоя): время доступа из mtime.
            Row row;
            row.bytes = info.first;
            row.last_access_ns = info.second;
            total_ += row.bytes;
            rows_.emplace(path, std::move(row));
            dirty_.insert(path);
            ++report.adopted;
        }
    }
    flush();
    return report;
}

}  // namespace telega::store
