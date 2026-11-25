// PackWriter: открытый пакет, закрытие, сборка, заливка и применение.

#include "vfs/pack_writer.h"

#include <fcntl.h>
#include <spdlog/spdlog.h>
#include <sys/stat.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <functional>
#include <system_error>
#include <tuple>

#include "common/files.h"
#include "crypto/chunk.h"
#include "meta/errors.h"
#include "meta/index_ops.h"

namespace telega::vfs {

using meta::Db;
using meta::InodeKind;

namespace {

int64_t now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

constexpr size_t kCopyBuffer = size_t{1} << 20U;

void remove_file(const std::filesystem::path& path) {
    std::error_code ec;
    std::filesystem::remove(path, ec);
}

}  // namespace

PackWriter::PackWriter(meta::MetaStore& meta, meta::StateStore& state, store::Store& store,
                       Overlay& overlay, crypto::SubKeys keys, const FsUuid& fs_uuid,
                       std::filesystem::path tmp_dir, PackConfig config, PackHooks hooks)
    : meta_(meta),
      state_(state),
      store_(store),
      overlay_(overlay),
      keys_(std::move(keys)),
      fs_uuid_(fs_uuid),
      tmp_(std::move(tmp_dir)),
      cfg_(config),
      hooks_(std::move(hooks)) {}

// --- образ открытого пакета (под mu_) ---

bool PackWriter::is_open_locked(int64_t ino, const Row& row) const {
    const auto it = marks_.find(ino);
    return it == marks_.end() || it->second != row.version;
}

void PackWriter::account_add_locked(int64_t ino, const Row& row) {
    open_bytes_ += row.len;
    ++open_count_;
    ages_.insert({row.added_ns, ino});
}

void PackWriter::account_remove_locked(int64_t ino, const Row& row) {
    open_bytes_ -= row.len;
    --open_count_;
    ages_.erase({row.added_ns, ino});
}

void PackWriter::set_row_locked(int64_t ino, const Row& row) {
    const auto it = rows_.find(ino);
    if (it != rows_.end()) {
        if (is_open_locked(ino, it->second)) {
            account_remove_locked(ino, it->second);
        }
        it->second = row;
    } else {
        rows_.emplace(ino, row);
    }
    if (is_open_locked(ino, row)) {
        account_add_locked(ino, row);
    }
}

bool PackWriter::demanded_locked(const Closing& pack, int64_t ino) const {
    if (pack.explicit_all || pack.explicit_inos.contains(ino) ||
        flush_stamp_ > pack.explicit_seq_at_close) {
        return true;
    }
    const auto it = fsync_stamp_.find(ino);
    return it != fsync_stamp_.end() && it->second > pack.explicit_seq_at_close;
}

void PackWriter::erase_row_locked(int64_t ino) {
    fsync_stamp_.erase(ino);
    const auto it = rows_.find(ino);
    if (it == rows_.end()) {
        return;
    }
    if (is_open_locked(ino, it->second)) {
        account_remove_locked(ino, it->second);
    }
    rows_.erase(it);
}

uint64_t PackWriter::close_locked(bool explicit_all, std::set<int64_t> explicit_inos) {
    if (open_count_ == 0) {
        return 0;
    }
    Closing pack;
    pack.explicit_all = explicit_all;
    pack.explicit_inos = std::move(explicit_inos);
    pack.explicit_seq_at_close = explicit_seq_;
    for (const auto& [ino, row] : rows_) {
        if (is_open_locked(ino, row)) {
            pack.members.push_back({ino, row.version});
            pack.total_len += row.len;
        }
    }
    // Все открытые строки теперь закрыты: в размер и возраст открытого пакета не входят.
    for (const Member& m : pack.members) {
        marks_[m.ino] = m.version;
    }
    open_bytes_ = 0;
    open_count_ = 0;
    ages_.clear();
    const uint64_t seq = next_seq_++;
    closing_.emplace(seq, std::move(pack));
    return seq;
}

// --- открытый пакет: добавление, закрытие ---

void PackWriter::load(bool persist) {
    const std::vector<meta::OpenPackRow> rows = state_.list_open_pack();
    for (const meta::OpenPackRow& r : rows) {
        if (!overlay_.chunk({r.ino, 0})) {
            // Данных объекта нет (сбой между удалением грязного чанка и строкой): строка лишняя.
            if (persist) {
                state_.delete_open_pack(r.ino);
            }
            continue;
        }
        const std::lock_guard lock(mu_);
        set_row_locked(r.ino, Row{r.version, r.len, r.added_ns});
    }
}

void PackWriter::add(int64_t ino, int64_t version, int64_t len) {
    Row row{version, len, now_ns()};
    {
        const std::lock_guard lock(mu_);
        const auto it = rows_.find(ino);
        if (it != rows_.end()) {
            if (it->second.version == version && it->second.len == len) {
                return;
            }
            if (is_open_locked(ino, it->second)) {
                row.added_ns = it->second.added_ns;  // возраст — от самого старого объекта
            }
        }
    }
    state_.upsert_open_pack({ino, version, len, row.added_ns});
    uint64_t seq = 0;
    {
        const std::lock_guard lock(mu_);
        set_row_locked(ino, row);
        if (open_bytes_ >= cfg_.pack_target) {
            seq = close_locked(false);
        }
    }
    if (seq != 0) {
        hooks_.submit(seq);
    }
}

void PackWriter::drop(int64_t ino) {
    {
        const std::lock_guard lock(mu_);
        if (!rows_.contains(ino)) {
            return;
        }
    }
    state_.delete_open_pack(ino);
    overlay_.clear_packed({ino, 0});
    const std::lock_guard lock(mu_);
    erase_row_locked(ino);
}

void PackWriter::on_dropped(int64_t ino) {
    const std::lock_guard lock(mu_);
    erase_row_locked(ino);
}

bool PackWriter::close_open(bool explicit_request) {
    uint64_t seq = 0;
    {
        const std::lock_guard lock(mu_);
        if (explicit_request) {
            flush_stamp_ = ++explicit_seq_;
        }
        seq = close_locked(explicit_request);
    }
    if (seq == 0) {
        return false;
    }
    hooks_.submit(seq);
    return true;
}

bool PackWriter::close_for(int64_t ino) {
    uint64_t seq = 0;
    {
        const std::lock_guard lock(mu_);
        const auto it = rows_.find(ino);
        if (it == rows_.end()) {
            return false;  // не пакетный
        }
        fsync_stamp_[ino] = ++explicit_seq_;
        if (!is_open_locked(ino, it->second)) {
            return false;  // уже в заливаемом пакете: fsync ждёт его
        }
        seq = close_locked(false, {ino});
    }
    if (seq == 0) {
        return false;
    }
    hooks_.submit(seq);
    return true;
}

void PackWriter::tick() {
    uint64_t seq = 0;
    {
        const std::lock_guard lock(mu_);
        if (open_count_ == 0 || ages_.empty()) {
            return;
        }
        const int64_t max_age_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(cfg_.max_age).count();
        if (now_ns() - ages_.begin()->first >= max_age_ns) {
            seq = close_locked(false);
        }
    }
    if (seq != 0) {
        hooks_.submit(seq);
    }
}

PackStatus PackWriter::status() const {
    const std::lock_guard lock(mu_);
    PackStatus st;
    st.open_objects = open_count_;
    st.open_bytes = open_bytes_;
    st.closing_packs = closing_.size();
    st.packs_uploaded = packs_uploaded_;
    st.objects_applied = objects_applied_;
    return st;
}

bool PackWriter::in_open_pack(int64_t ino) const {
    const std::lock_guard lock(mu_);
    const auto it = rows_.find(ino);
    return it != rows_.end() && is_open_locked(ino, it->second);
}

// --- заливка закрытого пакета ---

bool PackWriter::any_live(const std::vector<Placed>& placed) const {
    return std::any_of(placed.begin(), placed.end(), [&](const Placed& p) {
        const auto view = overlay_.chunk({p.ino, 0});
        return view && view->incarnation == p.incarnation && view->version == p.version;
    });
}

PackWriter::Assembled PackWriter::assemble(const Closing& pack, const std::filesystem::path& plain,
                                           bool& stop) {
    Assembled as;
    const Fd out = Fd::open(plain, O_RDWR | O_CREAT | O_TRUNC, 0600);
    Bytes buf(kCopyBuffer);
    for (const Member& m : pack.members) {
        if (stop || hooks_.stopping()) {
            stop = true;
            as.outcomes[m.ino] = Outcome::kept;
            continue;
        }
        // Снимок объекта согласован с записью: писатели держат полосу исключительно.
        const std::shared_lock lock(hooks_.stripe(m.ino));
        const ChunkKey key{m.ino, 0};
        const auto view = overlay_.chunk(key);
        if (!view) {
            as.outcomes[m.ino] = Outcome::dead;
            continue;
        }
        const uint64_t incarnation = view->incarnation;
        if (view->version != m.version) {
            as.outcomes[m.ino] = Outcome::changed;  // файл изменился: уйдёт в следующий пакет
            continue;
        }
        const auto inode = meta_.read([&](Db& db) { return meta::get_inode(db, m.ino); });
        if (!inode || inode->kind != InodeKind::file || !inode->parent) {
            as.outcomes[m.ino] = Outcome::dead;  // удалён, пусть и открытый: данные умрут с ним
            continue;
        }
        const auto attrs = overlay_.attrs(m.ino);
        const int64_t size = attrs ? attrs->size : inode->size;
        if (size > cfg_.pack_threshold || overlay_.indices(m.ino).size() != 1) {
            as.outcomes[m.ino] = Outcome::reseal;  // стал большим: позиционный класс
            continue;
        }
        const Fd in = Fd::open(overlay_.path_of(key), O_RDONLY);
        struct stat st {};
        if (::fstat(in.get(), &st) != 0) {
            throw SysError(errno, "fstat staging");
        }
        if (st.st_size == 0) {
            as.empties.push_back(key);
            as.outcomes[m.ino] = Outcome::dead;
            continue;
        }
        const int64_t len = std::min({size, static_cast<int64_t>(st.st_size), cfg_.chunk_size});
        if (len <= 0) {
            as.outcomes[m.ino] = Outcome::changed;
            continue;
        }
        if (as.total + len > cfg_.chunk_size) {
            as.outcomes[m.ino] = Outcome::kept;  // пакет не больше chunk_size: в следующий
            continue;
        }
        int64_t copied = 0;
        while (copied < len) {
            const auto n = static_cast<size_t>(std::min<int64_t>(len - copied, kCopyBuffer));
            const size_t got =
                pread_full(in.get(), MutableByteSpan{buf.data(), n}, static_cast<uint64_t>(copied));
            if (got != n) {
                throw SysError(EIO, "short read of staging file of inode " + std::to_string(m.ino));
            }
            pwrite_full(out.get(), ByteSpan{buf.data(), n},
                        static_cast<uint64_t>(as.total + copied));
            copied += static_cast<int64_t>(n);
        }
        as.placed.push_back({m.ino, m.version, incarnation, as.total, len});
        as.total += len;
    }
    return as;
}

void PackWriter::apply(int64_t chunk_id, const store::ChunkRef& ref, int64_t plain_size,
                       int64_t enc_size, ByteSpan mac, Assembled& as) {
    // Полосы всех объектов, по возрастанию адреса: см. описание в заголовке.
    std::vector<std::shared_mutex*> mutexes;
    mutexes.reserve(as.placed.size());
    for (const Placed& p : as.placed) {
        mutexes.push_back(&hooks_.stripe(p.ino));
    }
    std::sort(mutexes.begin(), mutexes.end(), std::less<>{});
    mutexes.erase(std::unique(mutexes.begin(), mutexes.end()), mutexes.end());
    std::vector<std::unique_lock<std::shared_mutex>> held;
    held.reserve(mutexes.size());
    for (std::shared_mutex* mu : mutexes) {
        held.emplace_back(*mu);
    }

    std::vector<const Placed*> cands;
    for (const Placed& p : as.placed) {
        const auto view = overlay_.chunk({p.ino, 0});
        if (!view) {
            as.outcomes[p.ino] = Outcome::dead;
        } else if (view->version != p.version || view->incarnation != p.incarnation) {
            as.outcomes[p.ino] = Outcome::changed;
        } else if (overlay_.indices(p.ino).size() != 1) {
            as.outcomes[p.ino] = Outcome::reseal;
        } else {
            cands.push_back(&p);
        }
    }
    if (cands.empty()) {
        return;
    }

    struct Applied {
        const Placed* p = nullptr;
        PackedObject obj;
    };
    std::vector<Applied> applied;
    std::map<int64_t, Outcome> rejected;
    meta_.write([&](Db& db) {
        struct Ok {
            const Placed* p;
            meta::Inode cur;
            std::optional<meta::DirtyAttrs> attrs;
            int64_t len;
        };
        std::vector<Ok> ok;
        for (const Placed* p : cands) {
            auto cur = meta::get_inode(db, p->ino);
            if (!cur || cur->kind != InodeKind::file || !cur->parent) {
                rejected[p->ino] = Outcome::dead;
                continue;
            }
            const auto attrs = overlay_.attrs(p->ino);
            const int64_t size = attrs ? attrs->size : cur->size;
            if (size > cfg_.pack_threshold) {
                rejected[p->ino] = Outcome::reseal;
                continue;
            }
            // Версия не менялась, поэтому truncate вниз не было; на всякий случай не длиннее
            // размера: индекс не держит экстент за концом файла.
            const int64_t len = std::min(p->len, size);
            if (len <= 0) {
                rejected[p->ino] = Outcome::changed;
                continue;
            }
            ok.push_back({p, std::move(*cur), attrs, len});
        }
        if (ok.empty()) {
            return;
        }
        const int64_t gen = hooks_.base_gen();
        meta::NewChunk nc;
        nc.chunk_id = chunk_id;
        nc.kind = meta::ChunkKind::pack;
        nc.msg_id = ref.msg_id;
        if (!ref.remote_file_id.empty()) {
            nc.remote_file_id = ref.remote_file_id;
        }
        nc.plain_size = plain_size;
        nc.enc_size = enc_size;
        nc.content_mac.assign(mac.begin(), mac.end());
        meta::insert_chunk(db, nc, gen);
        for (const Ok& o : ok) {
            if (o.cur.inline_len) {
                meta::clear_inline(db, o.p->ino);  // инлайн → пакет: данные переехали в пакет
            }
            meta::put_extent(db, meta::Extent{o.p->ino, 0, chunk_id, o.p->off, o.len}, gen);
            meta::AttrUpdate u;
            if (o.attrs) {
                u.size = o.attrs->size;
                u.mtime_ns = o.attrs->mtime_ns;
            } else if (o.len > o.cur.size) {
                u.size = o.len;
            }
            // ctime — момент привязки пакета в индексе: по нему recover --export-dirty находит
            // залитое после последнего коммита.
            u.ctime_ns = std::max({o.cur.ctime_ns, o.attrs ? o.attrs->mtime_ns : 0, now_ns()});
            meta::update_attrs(db, o.p->ino, u);
            applied.push_back({o.p, PackedObject{{o.p->ino, 0}, o.p->incarnation, o.p->version}});
        }
    });
    for (const auto& [ino, outcome] : rejected) {
        as.outcomes[ino] = outcome;
    }
    if (applied.empty()) {
        return;
    }
    probe("index_applied");
    std::vector<PackedObject> objs;
    objs.reserve(applied.size());
    for (const Applied& a : applied) {
        objs.push_back(a.obj);
        as.outcomes[a.p->ino] = Outcome::applied;
    }
    overlay_.finish_pack(chunk_id, objs);
    probe("overlay_cleared");
}

void PackWriter::finalize(const Closing& pack, const std::map<int64_t, Outcome>& outcomes) {
    const auto outcome_of = [&](int64_t ino) {
        const auto it = outcomes.find(ino);
        return it == outcomes.end() ? Outcome::kept : it->second;
    };
    // Строки state.sqlite: применённые удалил Overlay::finish_pack, остальные — здесь.
    for (const Member& m : pack.members) {
        switch (outcome_of(m.ino)) {
            case Outcome::dead:
            case Outcome::changed:
            case Outcome::reseal:
            case Outcome::failed:
                state_.delete_open_pack_if_version(m.ino, m.version);
                break;
            default:
                break;
        }
        if (outcome_of(m.ino) == Outcome::reseal) {
            overlay_.clear_packed({m.ino, 0});
        }
    }
    {
        const std::lock_guard lock(mu_);
        for (const Member& m : pack.members) {
            const Outcome o = outcome_of(m.ino);
            const auto it = rows_.find(m.ino);
            const bool same = it != rows_.end() && it->second.version == m.version;
            if (same && o != Outcome::kept) {
                erase_row_locked(m.ino);  // пока закрыта: в счётчики не входила
            }
            const auto mark = marks_.find(m.ino);
            if (mark != marks_.end() && mark->second == m.version) {
                marks_.erase(mark);
            }
            if (same && o == Outcome::kept) {
                account_add_locked(m.ino, it->second);  // снова в открытом пакете
            }
        }
    }
}

void PackWriter::run(uint64_t seq) {
    Closing pack;
    {
        const std::lock_guard lock(mu_);
        const auto it = closing_.find(seq);
        if (it == closing_.end()) {
            return;
        }
        pack = it->second;
    }
    std::map<int64_t, Outcome> outcomes;
    // Объекты с постоянной ошибкой: ожидающих будят только после finalize, когда строки
    // open_pack уже убраны (иначе fsync увидит ещё не согласованное состояние).
    std::vector<PackedObject> fails;
    const auto settle = [&](const std::map<int64_t, Outcome>& result) {
        finalize(pack, result);
        // Спрос по объектам: кто-то ждёт именно этот файл (его fsync) либо был глобальный запрос.
        std::set<int64_t> demanded;
        {
            const std::lock_guard lock(mu_);
            for (const Member& m : pack.members) {
                if (demanded_locked(pack, m.ino)) {
                    demanded.insert(m.ino);
                }
                fsync_stamp_.erase(m.ino);  // спрос учтён ниже
            }
            closing_.erase(seq);
        }
        for (const PackedObject& f : fails) {
            overlay_.mark_failed(f.key, f.incarnation, f.version);
        }
        std::vector<int64_t> reseal;
        std::set<int64_t> kept;  // не влезли или остановка, и их кто-то ждёт
        for (const Member& m : pack.members) {
            const auto it = result.find(m.ino);
            const Outcome o = it == result.end() ? Outcome::kept : it->second;
            if (!demanded.contains(m.ino)) {
                continue;
            }
            if (o == Outcome::kept) {
                kept.insert(m.ino);
            } else if (o == Outcome::changed || o == Outcome::reseal) {
                reseal.push_back(m.ino);
            }
        }
        // Объекты, изменившиеся за время заливки, ждёт fsync: запечатать заново. Объекты, не
        // влезшие в пакет, тоже: следующий пакет закрывается сразу.
        for (const int64_t ino : reseal) {
            hooks_.reseal_explicit(ino);
        }
        if (!kept.empty() && !hooks_.stopping()) {
            uint64_t next = 0;
            {
                const std::lock_guard lock(mu_);
                next = close_locked(false, std::move(kept));
            }
            if (next != 0) {
                hooks_.submit(next);
            }
        }
        uint64_t again = 0;
        {
            const std::lock_guard lock(mu_);
            if (open_bytes_ >= cfg_.pack_target) {
                again = close_locked(false);
            }
        }
        if (again != 0) {
            hooks_.submit(again);
        }
    };

    probe("run");
    // Место под сборку: открытый текст и зашифрованный файл одновременно. Берётся
    // целиком и до всех полос, поэтому ожидание не мешает применению других пакетов. Сборка и
    // шифрование идут по одному (stage_mu_); put_chunk — уже без него.
    std::unique_lock stage(stage_mu_);
    std::optional<StagingBudget::Lease> lease;
    if (hooks_.acquire_tmp) {
        const auto plain_max = static_cast<uint64_t>(std::min(pack.total_len, cfg_.chunk_size));
        lease = hooks_.acquire_tmp(plain_max + crypto::chunk_encrypted_size(plain_max));
        if (!lease) {
            for (const Member& m : pack.members) {
                outcomes[m.ino] = Outcome::kept;  // остановка: объекты остаются в пакете
            }
            settle(outcomes);
            return;
        }
    }
    // Открытый текст собирается под именем по номеру пакета: chunk_id выделяется (и грязнит
    // индекс) только когда ясно, что собирать есть что.
    int64_t chunk_id = 0;
    const auto plain = tmp_ / ("pack-s" + std::to_string(seq));
    std::filesystem::path enc;
    auto remove_tmp = [&] {
        remove_file(plain);
        if (!enc.empty()) {
            remove_file(enc);
        }
    };
    Assembled as;
    try {
        bool stop = false;
        as = assemble(pack, plain, stop);
        outcomes = as.outcomes;
        // Пустой staging-файл не заливается: позиция — дыра.
        for (const ChunkKey& key : as.empties) {
            const auto view = overlay_.chunk(key);
            if (view) {
                hooks_.resolve_empty(key, view->incarnation);
            }
        }
        if (stop || as.placed.empty()) {
            remove_tmp();
            stage.unlock();
            settle(outcomes);
            return;
        }
        chunk_id = meta_.write([](Db& db) { return meta::alloc_chunk_id(db); });
        enc = tmp_ / ("chunk-" + std::to_string(chunk_id));
        probe("assembled");
        const auto plain_size = static_cast<uint64_t>(as.total);
        crypto::Hash32 mac{};
        {
            const Fd fd = Fd::open(plain, O_RDONLY);
            mac = crypto::content_mac(keys_.mac, fd.get(), plain_size);
            crypto::encrypt_chunk_file(keys_.data, fs_uuid_, static_cast<uint64_t>(chunk_id),
                                       fd.get(), plain_size, enc);
        }
        remove_file(plain);
        probe("encrypted");
        const auto enc_size = static_cast<int64_t>(crypto::chunk_encrypted_size(plain_size));
        if (lease) {
            lease->shrink_to(static_cast<uint64_t>(enc_size));  // открытого текста больше нет
        }
        stage.unlock();

        // Заливка без блокировок.
        state_.upload_started(chunk_id, now_ns());
        probe("journal_started");
        store::ChunkRef ref;
        auto delay = cfg_.retry_initial;
        bool attempted = false;
        enum class Up : uint8_t { done, stopped, nothing_live, failed } up = Up::done;
        for (;;) {
            if (!any_live(as.placed)) {
                up = Up::nothing_live;
                break;
            }
            try {
                ref = store_.put_chunk(enc);
                break;
            } catch (const store::StoreError& e) {
                if (e.kind() == store::StoreErrorKind::transient) {
                    attempted = true;  // таймаут мог и залить: журнал оставляем монтированию
                    hooks_.note_error("upload of pack " + std::to_string(chunk_id) +
                                      " failed, will retry: " + e.what());
                    if (!hooks_.backoff(delay)) {
                        up = Up::stopped;
                        break;
                    }
                    continue;
                }
                hooks_.note_error("upload of pack " + std::to_string(chunk_id) +
                                  " failed: " + e.what());
                up = Up::failed;
                break;
            }
        }
        if (up != Up::done) {
            remove_tmp();
            if (up == Up::nothing_live) {
                if (!attempted) {
                    state_.remove_upload(chunk_id);
                }
                for (const Placed& p : as.placed) {
                    outcomes[p.ino] = overlay_.chunk({p.ino, 0}) ? Outcome::changed : Outcome::dead;
                }
            } else if (up == Up::failed) {
                state_.remove_upload(chunk_id);
                for (const Placed& p : as.placed) {
                    outcomes[p.ino] = Outcome::failed;
                    fails.push_back({{p.ino, 0}, p.incarnation, p.version});
                }
            }  // остановка: журнал без msg_id забудет монтирование, объекты остаются в пакете
            settle(outcomes);
            return;
        }
        probe("uploaded");
        state_.upload_finished(chunk_id, ref.msg_id);
        remove_file(enc);
        if (lease) {
            lease->release();
        }
        probe("journal_finished");

        apply(chunk_id, ref, as.total, enc_size, mac, as);
        outcomes = as.outcomes;
        const bool any_applied =
            std::any_of(as.placed.begin(), as.placed.end(),
                        [&](const Placed& p) { return outcomes[p.ino] == Outcome::applied; });
        if (!any_applied) {
            // Все объекты умерли или изменились за время заливки: сообщение не нужно.
            try {
                const std::array<int64_t, 1> ids{ref.msg_id};
                store_.delete_messages(ids);
                state_.remove_upload(chunk_id);
            } catch (const store::StoreError& e) {
                // Запись журнала остаётся: сироту уберёт монтирование.
                spdlog::warn("vfs: deleting unneeded pack {} (msg {}) failed: {}", chunk_id,
                             ref.msg_id, e.what());
            }
        }
        {
            const std::lock_guard lock(mu_);
            if (any_applied) {
                ++packs_uploaded_;
            }
            for (const Placed& p : as.placed) {
                objects_applied_ += outcomes[p.ino] == Outcome::applied ? 1 : 0;
            }
        }
        settle(outcomes);
        if (any_applied) {
            hooks_.chunk_uploaded();
        }
    } catch (const SysError& e) {
        // Диск (tmp/ или staging): пакет не удался, объекты остаются в overlay с ошибкой.
        remove_tmp();
        if (stage.owns_lock()) {
            stage.unlock();
        }
        hooks_.note_error("pack " + std::to_string(chunk_id) + ": " + e.what());
        for (const Member& m : pack.members) {
            const auto view = overlay_.chunk({m.ino, 0});
            if (view && view->version == m.version) {
                outcomes[m.ino] = Outcome::failed;
                fails.push_back({{m.ino, 0}, view->incarnation, m.version});
            }
        }
        settle(outcomes);
    }
}

}  // namespace telega::vfs
