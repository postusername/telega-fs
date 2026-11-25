#include "meta/fsck.h"

#include <algorithm>
#include <array>
#include <map>
#include <thread>
#include <utility>

#include "crypto/chunk.h"
#include "meta/index_ops.h"

namespace telega::meta {

namespace {

// Сколько проблем одного вида хранится в отчёте поимённо; остальные только считаются (подробности
// в последней строке вида), чтобы полностью испорченная таблица не раздула отчёт.
constexpr size_t kIssuesPerKind = 200;

// Сегментов чанка в одном скачивании при проверке тегов: около 1 МиБ на обращение к Store.
constexpr uint64_t kVerifySegments = 16;

class Collector {
  public:
    explicit Collector(FsckReport& report) : report_(report) {}

    void add(FsckKind kind, bool repairable, std::optional<int64_t> chunk_id, std::string detail) {
        size_t& n = counts_[static_cast<size_t>(kind)];
        ++n;
        if (n <= kIssuesPerKind) {
            report_.issues.push_back({kind, repairable, chunk_id, std::move(detail)});
        } else {
            repairable_[static_cast<size_t>(kind)] = repairable;
        }
    }

    // Строки «и ещё N» для видов, перешедших лимит.
    void finish() {
        for (size_t k = 0; k < counts_.size(); ++k) {
            if (counts_[k] > kIssuesPerKind) {
                report_.issues.push_back({static_cast<FsckKind>(k), repairable_[k], std::nullopt,
                                          "and " + std::to_string(counts_[k] - kIssuesPerKind) +
                                              " more of this kind are not listed"});
            }
        }
    }

  private:
    FsckReport& report_;
    std::array<size_t, 32> counts_{};
    std::array<bool, 32> repairable_{};
};

struct ChunkInfo {
    int64_t chunk_id = 0;
    ChunkKind kind = ChunkKind::positional;
    int64_t msg_id = 0;
    std::string remote_file_id;
    int64_t plain_size = 0;
    int64_t enc_size = 0;
    bool zero_mac = false;
    bool shape_ok = true;  // форма строки позволяет читать чанк из канала
    // На чанк ссылается хотя бы один экстент. Сообщение мёртвого чанка могли уже удалить, а его
    // строка осталась в индексе канала: это не порча, такие чанки не проверяются.
    bool live = true;
};

bool all_zero(const Bytes& b) {
    return std::all_of(b.begin(), b.end(), [](std::byte x) { return x == std::byte{0}; });
}

std::string num(int64_t v) { return std::to_string(v); }

// Читаемый вид имени: непечатные байты как \xNN.
std::string printable(const Bytes& name) {
    std::string out;
    for (const std::byte b : name) {
        const auto c = static_cast<unsigned char>(b);
        if (c >= 0x20 && c < 0x7F && c != '\\') {
            out += static_cast<char>(c);
        } else {
            constexpr std::string_view kHex = "0123456789abcdef";
            out += "\\x";
            out += kHex[c >> 4U];
            out += kHex[c & 0xFU];
        }
    }
    return out;
}

// Путь inode по цепочке parent. Отсоединённый (открытый удалённый) — «<unlinked inode N>».
std::string path_of_inode(Db& db, int64_t ino) {
    std::vector<std::string> parts;
    int64_t cur = ino;
    for (int depth = 0; depth < 4096; ++depth) {
        Stmt st{db, "SELECT parent, name FROM inodes WHERE ino = ?"};
        st.bind(1, cur);
        if (!st.step()) {
            return "<missing inode " + num(ino) + ">";
        }
        const auto parent = st.column_opt_int64(0);
        if (cur == kRootIno) {
            break;
        }
        const auto name = st.column_opt_blob(1);
        if (!parent || !name) {
            return "<unlinked inode " + num(ino) + ">";
        }
        parts.push_back(printable(*name));
        cur = *parent;
    }
    std::string path;
    std::reverse(parts.begin(), parts.end());
    for (const std::string& part : parts) {
        path += '/';
        path += part;
    }
    return path.empty() ? "/" : path;
}

void check_sqlite(Db& db, Collector& col) {
    {
        Stmt st{db, "PRAGMA integrity_check"};
        size_t shown = 0;
        while (st.step()) {
            const std::string line = st.column_text(0);
            if (line == "ok") {
                break;
            }
            if (++shown <= 10) {
                col.add(FsckKind::sqlite_integrity, false, std::nullopt, line);
            }
        }
    }
    {
        Stmt st{db, "PRAGMA foreign_key_check"};
        while (st.step()) {
            const std::string table = st.column_text(0);
            const std::string parent = st.column_text(2);
            if (table == "extents" && parent == "chunks") {
                continue;  // это extent_missing_chunk, ниже
            }
            std::string detail = "table ";
            detail += table;
            detail += ", rowid " + num(st.column_int64(1));
            detail += " refers to a missing row of ";
            detail += parent;
            col.add(FsckKind::foreign_key, false, std::nullopt, std::move(detail));
        }
    }
}

// Ключи meta, корень, счётчики, инлайн-бюджет. Возвращает параметры или nullopt, если их нельзя
// прочитать.
std::optional<IndexParams> check_meta(Db& db, Collector& col) {
    IndexParams params;
    try {
        params = read_params(db);
    } catch (const std::exception& e) {
        col.add(FsckKind::meta_params, false, std::nullopt,
                std::string("cannot read meta parameters: ") + e.what());
        return std::nullopt;
    }
    if (params.format_version != kFormatVersion) {
        col.add(FsckKind::meta_params, false, std::nullopt,
                "format_version " + num(params.format_version) + " is not supported");
    }
    {
        Stmt st{db, "SELECT 1 FROM inodes WHERE ino = 1 AND kind = 2 AND parent IS NULL"};
        if (!st.step()) {
            col.add(FsckKind::root_missing, false, std::nullopt,
                    "the root directory is missing or is not a directory");
        }
    }
    {
        Stmt st{db,
                "SELECT (SELECT value FROM meta WHERE key = 'next_ino'), "
                "       (SELECT COALESCE(MAX(ino), 0) FROM inodes), "
                "       (SELECT value FROM meta WHERE key = 'next_chunk_id'), "
                "       (SELECT COALESCE(MAX(chunk_id), 0) FROM chunks)"};
        st.step();
        const auto next_ino = st.column_opt_int64(0);
        const int64_t max_ino = st.column_int64(1);
        const auto next_chunk = st.column_opt_int64(2);
        const int64_t max_chunk = st.column_int64(3);
        if (!next_ino || *next_ino <= max_ino) {
            col.add(FsckKind::counter_behind, next_ino.has_value(), std::nullopt,
                    "meta.next_ino is " + (next_ino ? num(*next_ino) : std::string{"missing"}) +
                        ", the largest inode is " + num(max_ino));
        }
        if (!next_chunk || *next_chunk <= max_chunk) {
            col.add(FsckKind::counter_behind, next_chunk.has_value(), std::nullopt,
                    "meta.next_chunk_id is " +
                        (next_chunk ? num(*next_chunk) : std::string{"missing"}) +
                        ", the largest chunk_id is " + num(max_chunk));
        }
    }
    {
        Stmt st{db,
                "SELECT (SELECT value FROM meta WHERE key = 'inline_bytes'), "
                "       (SELECT COALESCE(SUM(LENGTH(inline_data)), 0) FROM inodes)"};
        st.step();
        const auto recorded = st.column_opt_int64(0);
        const int64_t actual = st.column_int64(1);
        if (!recorded || *recorded != actual) {
            col.add(FsckKind::inline_bytes, recorded.has_value(), std::nullopt,
                    "meta.inline_bytes is " + (recorded ? num(*recorded) : std::string{"missing"}) +
                        ", the inline data totals " + num(actual) + " bytes");
        }
        if (actual > params.inline_budget) {
            col.add(FsckKind::inline_budget, false, std::nullopt,
                    "inline data totals " + num(actual) + " bytes, the budget is " +
                        num(params.inline_budget));
        }
    }
    {
        Stmt st{db,
                "SELECT ino FROM inodes WHERE inline_data IS NOT NULL "
                "AND EXISTS (SELECT 1 FROM extents e WHERE e.ino = inodes.ino) ORDER BY ino"};
        while (st.step()) {
            col.add(FsckKind::inline_with_extents, false, std::nullopt,
                    "inode " + num(st.column_int64(0)) + " is inline but has extents");
        }
    }
    return params;
}

// Строки chunks: форма, refcount, live_bytes, dead_since_gen. Заполняет report.chunks.
std::vector<ChunkInfo> check_chunks(Db& db, const IndexParams& params, FsckReport& report,
                                    Collector& col) {
    std::vector<ChunkInfo> out;
    Stmt st{db,
            "SELECT c.chunk_id, c.kind, c.msg_id, c.remote_file_id, c.plain_size, c.enc_size, "
            "       c.content_mac, c.refcount, c.live_bytes, c.dead_since_gen, "
            "       COALESCE(e.n, 0), COALESCE(e.s, 0) "
            "FROM chunks c LEFT JOIN "
            "  (SELECT chunk_id, COUNT(*) AS n, SUM(len) AS s FROM extents GROUP BY chunk_id) e "
            "  ON e.chunk_id = c.chunk_id ORDER BY c.chunk_id"};
    while (st.step()) {
        ChunkInfo c;
        c.chunk_id = st.column_int64(0);
        const int64_t kind = st.column_int64(1);
        c.msg_id = st.column_int64(2);
        if (!st.is_null(3)) {
            c.remote_file_id = st.column_text(3);
        }
        c.plain_size = st.column_int64(4);
        c.enc_size = st.column_int64(5);
        const Bytes mac = st.column_blob(6);
        const int64_t refcount = st.column_int64(7);
        const int64_t live_bytes = st.column_int64(8);
        const auto dead_since = st.column_opt_int64(9);
        const int64_t extents_n = st.column_int64(10);
        const int64_t extents_sum = st.column_int64(11);
        ++report.chunks;
        c.kind = kind == 2 ? ChunkKind::pack : ChunkKind::positional;
        c.zero_mac = mac.size() == 32 && all_zero(mac);

        if (kind != 1 && kind != 2) {
            c.shape_ok = false;
            col.add(FsckKind::chunk_shape, false, c.chunk_id, "unknown chunk kind " + num(kind));
        }
        if (c.msg_id <= 0) {
            c.shape_ok = false;
            col.add(FsckKind::chunk_shape, false, c.chunk_id, "msg_id " + num(c.msg_id));
        }
        if (c.plain_size <= 0 || c.plain_size > params.chunk_size) {
            c.shape_ok = false;
            col.add(FsckKind::chunk_shape, false, c.chunk_id,
                    "plain_size " + num(c.plain_size) + " is outside 1.." + num(params.chunk_size));
        } else if (c.enc_size != static_cast<int64_t>(crypto::chunk_encrypted_size(
                                     static_cast<uint64_t>(c.plain_size)))) {
            c.shape_ok = false;
            col.add(
                FsckKind::chunk_shape, false, c.chunk_id,
                "enc_size " + num(c.enc_size) + " does not match plain_size " + num(c.plain_size));
        }
        if (mac.size() != 32) {
            col.add(FsckKind::chunk_shape, false, c.chunk_id,
                    "content_mac is " + num(static_cast<int64_t>(mac.size())) + " bytes, not 32");
        }
        c.live = extents_n > 0;
        if (refcount != extents_n) {
            col.add(FsckKind::refcount, true, c.chunk_id,
                    "chunk " + num(c.chunk_id) + ": refcount " + num(refcount) + ", extents " +
                        num(extents_n));
        }
        if (live_bytes != extents_sum) {
            col.add(FsckKind::live_bytes, true, c.chunk_id,
                    "chunk " + num(c.chunk_id) + ": live_bytes " + num(live_bytes) +
                        ", extents total " + num(extents_sum));
        }
        // dead_since_gen сверяется с фактическим числом экстентов: после пересчёта refcount оно
        // и будет верным.
        if (extents_n == 0 && !dead_since) {
            col.add(FsckKind::dead_since_gen, true, c.chunk_id,
                    "chunk " + num(c.chunk_id) + " has no extents but no dead_since_gen");
        } else if (extents_n > 0 && dead_since) {
            col.add(
                FsckKind::dead_since_gen, true, c.chunk_id,
                "chunk " + num(c.chunk_id) + " has extents but dead_since_gen " + num(*dead_since));
        }
        if (c.zero_mac) {
            report.known_bad_chunks.insert(c.chunk_id);
        }
        out.push_back(std::move(c));
    }
    return out;
}

// Экстенты: существование чанка, границы, chunk_off, пакеты. Заполняет report.extents.
void check_extents(Db& db, FsckReport& report, Collector& col) {
    Stmt st{db,
            "SELECT e.ino, e.idx, e.chunk_id, e.chunk_off, e.len, c.chunk_id, c.kind, "
            "       c.plain_size "
            "FROM extents e LEFT JOIN chunks c ON c.chunk_id = e.chunk_id "
            "ORDER BY e.chunk_id, e.chunk_off, e.ino"};
    int64_t pack_chunk = -1;
    int64_t pack_end = 0;
    while (st.step()) {
        ++report.extents;
        const int64_t ino = st.column_int64(0);
        const int64_t idx = st.column_int64(1);
        const int64_t chunk_id = st.column_int64(2);
        const int64_t off = st.column_int64(3);
        const int64_t len = st.column_int64(4);
        const std::string where =
            "extent " + num(ino) + ":" + num(idx) + " (chunk " + num(chunk_id) + ")";
        if (st.is_null(5)) {
            col.add(FsckKind::extent_missing_chunk, false, chunk_id,
                    where + " refers to a chunk that does not exist");
            continue;
        }
        const int64_t kind = st.column_int64(6);
        const int64_t plain = st.column_int64(7);
        if (len <= 0 || off < 0 || off + len > plain) {
            col.add(FsckKind::extent_bounds, false, chunk_id,
                    where + ": chunk_off " + num(off) + ", len " + num(len) +
                        " do not fit into plain_size " + num(plain));
        }
        if (kind == 1 && off != 0) {
            col.add(FsckKind::extent_chunk_off, false, chunk_id,
                    where + ": chunk_off " + num(off) + " in a positional chunk");
        }
        if (kind == 2) {
            if (idx != 0) {
                col.add(FsckKind::pack_extent_idx, false, chunk_id,
                        where + ": an object of a pack must be at idx 0");
            }
            if (pack_chunk != chunk_id) {
                pack_chunk = chunk_id;
                pack_end = 0;
            }
            if (off < pack_end) {
                col.add(FsckKind::pack_overlap, false, chunk_id,
                        where + ": object at " + num(off) + " overlaps the previous one (ends at " +
                            num(pack_end) + ")");
            }
            pack_end = std::max(pack_end, off + len);
        }
    }
}

enum class Verdict : uint8_t { ok, corrupt, missing, unreadable };

struct VerifyResult {
    Verdict verdict = Verdict::ok;
    std::string why;
};

// Скачать чанк целиком и проверить заголовок и теги всех сегментов.
VerifyResult verify_chunk(store::Store& store, const crypto::Key& k_data, const FsUuid& fs_uuid,
                          const ChunkInfo& chunk) {
    const store::ChunkRef ref{chunk.msg_id, chunk.remote_file_id};
    const auto plain = static_cast<uint64_t>(chunk.plain_size);
    try {
        Bytes raw(crypto::kChunkHeaderSize);
        store.read_range(ref, 0, raw);
        const crypto::ChunkHeader header = crypto::parse_chunk_header(raw);
        if (header.plain_size != plain) {
            return {Verdict::corrupt, "the header says plain_size " +
                                          std::to_string(header.plain_size) + ", the index " +
                                          num(chunk.plain_size)};
        }
        const uint64_t segments = crypto::chunk_segment_count(plain);
        for (uint64_t first = 0; first < segments; first += kVerifySegments) {
            const uint64_t n = std::min(kVerifySegments, segments - first);
            const auto span = crypto::chunk_segments_for(plain, first * crypto::kSegmentSize,
                                                         n * crypto::kSegmentSize);
            Bytes enc(static_cast<size_t>(span.enc_len));
            store.read_range(ref, span.enc_offset, enc);
            (void)crypto::decrypt_chunk_segments(
                k_data, fs_uuid, static_cast<uint64_t>(chunk.chunk_id), header, span.first, enc);
        }
    } catch (const crypto::CorruptError& e) {
        return {Verdict::corrupt, e.what()};
    } catch (const store::StoreError& e) {
        switch (e.kind()) {
            case store::StoreErrorKind::not_found:
                return {Verdict::missing, e.what()};
            case store::StoreErrorKind::permanent:
                // Например, сообщение короче, чем ждёт индекс.
                return {Verdict::corrupt, e.what()};
            case store::StoreErrorKind::transient:
                return {Verdict::unreadable, e.what()};
        }
    }
    return {};
}

void report_progress(const FsckOptions& options, const std::string& text) {
    if (options.progress) {
        options.progress(text);
    }
}

}  // namespace

std::string_view fsck_kind_name(FsckKind kind) {
    switch (kind) {
        case FsckKind::sqlite_integrity:
            return "sqlite-integrity";
        case FsckKind::foreign_key:
            return "foreign-key";
        case FsckKind::meta_params:
            return "meta";
        case FsckKind::root_missing:
            return "root";
        case FsckKind::counter_behind:
            return "counter";
        case FsckKind::inline_bytes:
            return "inline-bytes";
        case FsckKind::inline_budget:
            return "inline-budget";
        case FsckKind::inline_with_extents:
            return "inline-extents";
        case FsckKind::refcount:
            return "refcount";
        case FsckKind::live_bytes:
            return "live-bytes";
        case FsckKind::dead_since_gen:
            return "dead-since-gen";
        case FsckKind::extent_missing_chunk:
            return "extent-no-chunk";
        case FsckKind::extent_bounds:
            return "extent-bounds";
        case FsckKind::extent_chunk_off:
            return "extent-chunk-off";
        case FsckKind::pack_extent_idx:
            return "pack-idx";
        case FsckKind::pack_overlap:
            return "pack-overlap";
        case FsckKind::chunk_shape:
            return "chunk";
        case FsckKind::message_missing:
            return "message-missing";
        case FsckKind::chain_message_missing:
            return "chain-missing";
        case FsckKind::chunk_corrupt:
            return "chunk-corrupt";
        case FsckKind::chunk_unverified:
            return "chunk-unverified";
        case FsckKind::store_unavailable:
            return "store";
    }
    return "?";
}

bool FsckReport::has_unrepairable() const {
    return std::any_of(issues.begin(), issues.end(),
                       [](const FsckIssue& i) { return !i.repairable; });
}

FsckReport fsck_check(MetaStore& meta, StateStore* state, store::Store& store,
                      const crypto::SubKeys& keys, const FsckOptions& options) {
    FsckReport report;
    Collector col{report};

    // Всё, что читает индекс, — одной читающей транзакцией; Store вызывается после неё.
    std::vector<ChunkInfo> chunks;
    std::optional<IndexParams> params;
    meta.read([&](Db& db) {
        check_sqlite(db, col);
        params = check_meta(db, col);
        if (params) {
            chunks = check_chunks(db, *params, report, col);
        }
        check_extents(db, report, col);
    });

    // Существование сообщений: msg_id → чанки, плюс элементы текущей цепочки.
    std::map<int64_t, std::vector<int64_t>> by_message;
    for (const ChunkInfo& c : chunks) {
        if (c.msg_id > 0 && !c.zero_mac && c.live) {
            by_message[c.msg_id].push_back(c.chunk_id);
        }
    }
    std::set<int64_t> chain_ids;
    if (state != nullptr) {
        for (const ChainElement& el : state->list_chain()) {
            chain_ids.insert(el.msg_id);
        }
    }
    if (!options.check_messages) {
        col.finish();
        return report;
    }
    std::vector<int64_t> ids;
    ids.reserve(by_message.size() + chain_ids.size());
    for (const auto& [msg, owners] : by_message) {
        ids.push_back(msg);
    }
    for (const int64_t id : chain_ids) {
        if (!by_message.contains(id)) {
            ids.push_back(id);
        }
    }
    for (size_t at = 0; at < ids.size(); at += store::kMessagesBatch) {
        const size_t end = std::min(ids.size(), at + store::kMessagesBatch);
        const std::span<const int64_t> batch{ids.data() + at, end - at};
        std::vector<bool> exists;
        try {
            exists = store.messages_exist(batch);
        } catch (const store::StoreError& e) {
            col.add(FsckKind::store_unavailable, false, std::nullopt,
                    std::string("cannot check whether messages exist, ") +
                        num(static_cast<int64_t>(ids.size() - at)) +
                        " of them are not checked: " + e.what());
            report.message_check_incomplete = true;
            break;
        }
        if (exists.size() != batch.size()) {
            col.add(FsckKind::store_unavailable, false, std::nullopt,
                    "the store answered for a wrong number of messages");
            report.message_check_incomplete = true;
            break;
        }
        for (size_t i = 0; i < batch.size(); ++i) {
            ++report.messages_checked;
            if (exists[i]) {
                continue;
            }
            const int64_t msg = batch[i];
            if (const auto it = by_message.find(msg); it != by_message.end()) {
                for (const int64_t chunk_id : it->second) {
                    report.bad_chunks.insert(chunk_id);
                    col.add(FsckKind::message_missing, true, chunk_id,
                            "chunk " + num(chunk_id) + ": message " + num(msg) +
                                " does not exist in the channel");
                }
            }
            if (chain_ids.contains(msg)) {
                col.add(FsckKind::chain_message_missing, false, std::nullopt,
                        "index chain element: message " + num(msg) + " does not exist");
            }
        }
        report_progress(options, "messages checked: " + std::to_string(report.messages_checked) +
                                     "/" + std::to_string(ids.size()));
        if (end < ids.size() && options.batch_pause > std::chrono::milliseconds{0}) {
            std::this_thread::sleep_for(options.batch_pause);
        }
    }

    if (options.verify_data && params && !report.message_check_incomplete) {
        uint64_t done = 0;
        for (const ChunkInfo& c : chunks) {
            ++done;
            if (c.zero_mac || !c.shape_ok || !c.live || report.bad_chunks.contains(c.chunk_id)) {
                continue;
            }
            const VerifyResult v = verify_chunk(store, keys.data, params->fs_uuid, c);
            ++report.chunks_verified;
            switch (v.verdict) {
                case Verdict::ok:
                    break;
                case Verdict::corrupt:
                    report.bad_chunks.insert(c.chunk_id);
                    col.add(FsckKind::chunk_corrupt, true, c.chunk_id,
                            "chunk " + num(c.chunk_id) + " (message " + num(c.msg_id) +
                                ") is corrupted: " + v.why);
                    break;
                case Verdict::missing:
                    report.bad_chunks.insert(c.chunk_id);
                    col.add(FsckKind::message_missing, true, c.chunk_id,
                            "chunk " + num(c.chunk_id) + ": message " + num(c.msg_id) +
                                " does not exist in the channel");
                    break;
                case Verdict::unreadable:
                    col.add(
                        FsckKind::chunk_unverified, false, c.chunk_id,
                        "chunk " + num(c.chunk_id) + " could not be read, not verified: " + v.why);
                    break;
            }
            if (done % 16 == 0) {
                report_progress(options, "chunks verified: " + std::to_string(done) + "/" +
                                             std::to_string(chunks.size()));
            }
        }
    }
    col.finish();
    return report;
}

std::vector<std::string> files_using_chunks(MetaStore& meta, const std::set<int64_t>& chunk_ids) {
    std::set<std::string> paths;
    meta.read([&](Db& db) {
        for (const int64_t chunk_id : chunk_ids) {
            Stmt st{db, "SELECT DISTINCT ino FROM extents WHERE chunk_id = ?"};
            st.bind(1, chunk_id);
            while (st.step()) {
                paths.insert(path_of_inode(db, st.column_int64(0)));
            }
        }
    });
    return {paths.begin(), paths.end()};
}

RepairResult fsck_repair(MetaStore& meta, const FsckReport& report, int64_t base_generation) {
    RepairResult result;
    // Затронутые файлы — до исправления: пути берутся из индекса, который repair не переставляет.
    result.affected_files = files_using_chunks(meta, report.bad_chunks);
    meta.write([&](Db& db) {
        {
            Stmt st{db,
                    "UPDATE chunks SET refcount = "
                    "(SELECT COUNT(*) FROM extents e WHERE e.chunk_id = chunks.chunk_id) "
                    "WHERE refcount != "
                    "(SELECT COUNT(*) FROM extents e WHERE e.chunk_id = chunks.chunk_id)"};
            st.run();
            result.refcounts_fixed = static_cast<uint64_t>(db.changes());
        }
        {
            Stmt st{
                db,
                "UPDATE chunks SET live_bytes = "
                "(SELECT COALESCE(SUM(len), 0) FROM extents e WHERE e.chunk_id = chunks.chunk_id) "
                "WHERE live_bytes != "
                "(SELECT COALESCE(SUM(len), 0) FROM extents e WHERE e.chunk_id = chunks.chunk_id)"};
            st.run();
            result.live_bytes_fixed = static_cast<uint64_t>(db.changes());
        }
        {
            Stmt st{db,
                    "UPDATE chunks SET dead_since_gen = ? WHERE refcount = 0 AND dead_since_gen IS "
                    "NULL"};
            st.bind(1, base_generation).run();
            result.dead_since_fixed = static_cast<uint64_t>(db.changes());
        }
        {
            Stmt st{db,
                    "UPDATE chunks SET dead_since_gen = NULL "
                    "WHERE refcount > 0 AND dead_since_gen IS NOT NULL"};
            st.run();
            result.dead_since_fixed += static_cast<uint64_t>(db.changes());
        }
        {
            Stmt st{
                db,
                "UPDATE meta SET value = (SELECT COALESCE(MAX(ino), 0) + 1 FROM inodes) "
                "WHERE key = 'next_ino' AND value <= (SELECT COALESCE(MAX(ino), 0) FROM inodes)"};
            st.run();
            result.counters_fixed += static_cast<uint64_t>(db.changes());
        }
        {
            Stmt st{db,
                    "UPDATE meta SET value = (SELECT COALESCE(MAX(chunk_id), 0) + 1 FROM chunks) "
                    "WHERE key = 'next_chunk_id' "
                    "AND value <= (SELECT COALESCE(MAX(chunk_id), 0) FROM chunks)"};
            st.run();
            result.counters_fixed += static_cast<uint64_t>(db.changes());
        }
        {
            Stmt st{db,
                    "UPDATE meta SET value = (SELECT COALESCE(SUM(LENGTH(inline_data)), 0) FROM "
                    "inodes) "
                    "WHERE key = 'inline_bytes' "
                    "AND value != (SELECT COALESCE(SUM(LENGTH(inline_data)), 0) FROM inodes)"};
            st.run();
            result.inline_bytes_fixed = db.changes() > 0;
        }
        const Bytes zero(32, std::byte{0});
        for (const int64_t chunk_id : report.bad_chunks) {
            Stmt st{db,
                    "UPDATE chunks SET content_mac = ? WHERE chunk_id = ? AND content_mac != ?"};
            st.bind(1, ByteSpan{zero}).bind(2, chunk_id).bind(3, ByteSpan{zero}).run();
            result.macs_zeroed += static_cast<uint64_t>(db.changes());
        }
    });
    return result;
}

}  // namespace telega::meta
