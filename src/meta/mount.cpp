#include "meta/mount.h"

#include <sodium.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include "common/files.h"
#include "crypto/index_blob.h"
#include "meta/chain.h"
#include "meta/errors.h"
#include "store/local_dir_store.h"

namespace telega::meta {

namespace {

constexpr size_t kDeleteBatch = 100;

void remove_quietly(const std::filesystem::path& path) {
    std::error_code ignored;
    std::filesystem::remove(path, ignored);
}

bool same_bytes(ByteSpan a, ByteSpan b) {
    return std::equal(a.begin(), a.end(), b.begin(), b.end());
}

[[noreturn]] void corrupt(const std::string& what) {
    throw MountError(MountErrorKind::corrupt, "index chain is corrupt: " + what);
}

ChainBlob open_head(const crypto::Key& k_index, const FsUuid& fs_uuid, int64_t msg_id,
                    ByteSpan blob) {
    try {
        return open_chain_blob(k_index, fs_uuid, msg_id, blob);
    } catch (const ChainCorrupt& e) {
        corrupt(e.what());
    }
}

std::vector<ChainBlob> fetch_or_corrupt(store::Store& store, const crypto::Key& k_index,
                                        const FsUuid& fs_uuid, int64_t head_msg_id,
                                        ByteSpan head_blob) {
    try {
        return fetch_chain(store, k_index, fs_uuid, head_msg_id, head_blob);
    } catch (const ChainCorrupt& e) {
        corrupt(e.what());
    }
}

// Локальные изменения (SPEC 7.2, шаг 6): overlay не пуст, index_dirty = 1 или есть pending.
bool has_local_changes(StateStore& state) {
    return state.get_int_or(state_key::index_dirty, 0) != 0 || !state.list_pending().empty() ||
           !state.list_dirty_chunks().empty() || !state.list_dirty_attrs().empty();
}

// Удалить файл overlay, только если он лежит внутри staging/.
void remove_staging_file(const FsPaths& paths, const std::string& path) {
    std::filesystem::path p{path};
    if (p.is_relative()) {
        p = paths.staging() / p;
    }
    const auto rel = p.lexically_normal().lexically_relative(paths.staging().lexically_normal());
    if (rel.empty() || rel.native().starts_with("..")) {
        spdlog::warn("mount: overlay file outside staging is left in place");
        return;
    }
    remove_quietly(p);
}

// Забыть строки overlay и pending в state.sqlite (вызывать внутри транзакции).
void drop_local_rows(StateStore& state) {
    for (const auto& dc : state.list_dirty_chunks()) {
        state.delete_dirty_chunks_for_ino(dc.ino);
    }
    for (const auto& da : state.list_dirty_attrs()) {
        state.delete_dirty_attrs(da.ino);
    }
    state.clear_pending();
    state.set_int(state_key::index_dirty, 0);
}

void clear_staging(const FsPaths& paths) {
    std::error_code ec;
    for (const auto& entry : std::filesystem::directory_iterator(paths.staging(), ec)) {
        std::filesystem::remove_all(entry.path(), ec);
    }
}

// Коммит-состояние головы цепочки в state.sqlite (после сборки или догона). Вызывать внутри
// транзакции.
void write_chain_state(StateStore& state, const std::vector<ChainBlob>& chain,
                       const FsUuid& fs_uuid) {
    const ChainBlob& head = chain.back();
    state.set_blob(state_key::fs_uuid, fs_uuid);
    state.set_int(state_key::base_generation, static_cast<int64_t>(head.header.generation));
    state.set_int(state_key::head_msg_id, head.msg_id);
    state.set_blob(state_key::head_hash, head.hash);
    state.set_int(state_key::index_dirty, 0);
    state.clear_pending();
    state.reset_chain_to_snapshot(chain.front().msg_id, chain.front().blob_size);
    for (size_t i = 1; i < chain.size(); ++i) {
        state.append_chain(chain[i].msg_id, chain[i].blob_size);
    }
}

void set_state_from_chain(StateStore& state, const std::vector<ChainBlob>& chain,
                          const FsUuid& fs_uuid) {
    state.transaction([&] { write_chain_state(state, chain, fs_uuid); });
}

std::vector<Bytes> patches_after(const std::vector<ChainBlob>& chain, size_t first) {
    std::vector<Bytes> out;
    for (size_t i = first; i < chain.size(); ++i) {
        out.push_back(chain[i].payload);
    }
    return out;
}

// Собрать index.sqlite из цепочки целиком (SPEC 6.6). Порча — MountError(corrupt).
void rebuild_index(const FsPaths& paths, const std::vector<ChainBlob>& chain) {
    const auto snap = paths.tmp() / "chain-snapshot.sqlite";
    remove_quietly(snap);
    write_file_atomic(snap, chain.front().payload);
    try {
        MetaStore::build_from_chain(snap, patches_after(chain, 1), paths.index_db());
    } catch (const IndexCorrupt& e) {
        remove_quietly(snap);
        corrupt(e.what());
    }
    remove_quietly(snap);
}

// --discard-local (SPEC 7.2): заменить локальное состояние цепочкой головы, забыв
// незакоммиченное. Порядок важен для сбоя в любой точке: сначала индекс собирается во
// временный файл и атомарно подменяет index.sqlite (rebuild_index), потом ОДНА транзакция
// state.sqlite стирает overlay, pending и index_dirty и записывает состояние цепочки, и лишь
// затем чистится staging/. Раньше локальные изменения стирались до пересборки: сбой между
// ними оставлял «чистое» состояние поверх старого index.sqlite, и следующее монтирование
// накатывало дельты на индекс с выброшенными, но не откатанными изменениями. Теперь до
// транзакции state.sqlite всё ещё содержит локальные изменения (следующее монтирование
// снова видит конфликт), а после неё индекс и состояние согласованы.
void adopt_chain_discarding_local(StateStore& state, const FsPaths& paths,
                                  const std::vector<ChainBlob>& chain, const FsUuid& fs_uuid) {
    rebuild_index(paths, chain);
    state.transaction([&] {
        drop_local_rows(state);
        write_chain_state(state, chain, fs_uuid);
    });
    clear_staging(paths);
}

// Индекса нет (SPEC 7.2, «Локального индекса нет»): выбросить локальные изменения и собрать
// индекс из цепочки. Порядок обратный adopt_chain_discarding_local: старого индекса, которому
// нужна согласованность до подмены, здесь нет, и перед сборкой должна пройти транзакция
// state.sqlite. Если пересборка шла бы первой, сбой между ней и транзакцией оставил бы свежий
// индекс при старых base_generation и локальных строках: при G_l == G_r следующее монтирование
// увидело бы индекс, то же поколение и накатило бы pending и dirty_chunks на чужой индекс.
// Теперь сбой после транзакции даёт штатное «индекса нет, изменений нет», а сбой до неё —
// прежний конфликт «индекса нет, изменения есть».
void adopt_chain_without_index(StateStore& state, const FsPaths& paths,
                               const std::vector<ChainBlob>& chain, const FsUuid& fs_uuid) {
    state.transaction([&] {
        drop_local_rows(state);
        write_chain_state(state, chain, fs_uuid);
    });
    rebuild_index(paths, chain);
    clear_staging(paths);
}

// Догнать локальный индекс поколения g_l дельтами цепочки (SPEC 7.2, G_r > G_l). false —
// G_l не в этой цепочке или патчи не легли: тогда собирать заново.
bool catch_up(const FsPaths& paths, const std::vector<ChainBlob>& chain, uint64_t g_l, int64_t h_l,
              ByteSpan hash_l) {
    const auto it = std::find_if(chain.begin(), chain.end(),
                                 [&](const ChainBlob& b) { return b.header.generation == g_l; });
    // Свой G_l узнаём по msg_id и хешу блоба: одинаковое поколение в чужой ветке — не наше.
    if (it == chain.end() || it->msg_id != h_l || !same_bytes(it->hash, hash_l)) {
        return false;
    }
    const auto copy = paths.tmp() / "catchup.sqlite";
    remove_quietly(copy);
    try {
        {
            Db db = Db::open(paths.index_db());
            Stmt st{db, "VACUUM INTO ?"};
            st.bind_text(1, copy.string()).run();
        }
        {
            // После коммита G_l локальный индекс получил строку index_history для G_l
            // (служебная запись, SPEC 7.1 шаг 8), которой нет в блобе G_l. Дельта другой
            // машины везёт ту же строку, и patchset дал бы конфликт. Строки ≥ G_l
            // восстановятся из цепочки или сверкой с state.chain после монтирования.
            OpenOptions options;
            options.wal = false;
            Db db = Db::open(copy, options);
            Stmt st{db, "DELETE FROM index_history WHERE generation >= ?"};
            st.bind(1, static_cast<int64_t>(g_l)).run();
        }
        const auto first = static_cast<size_t>(it - chain.begin()) + 1;
        MetaStore::build_from_chain(copy, patches_after(chain, first), paths.index_db());
    } catch (const IndexCorrupt& e) {
        // Например, уборка этой машины уже удалила строки чанков, которые удаляет и дельта.
        spdlog::warn("mount: deltas do not apply to the local index ({}), rebuilding", e.what());
        remove_quietly(copy);
        return false;
    } catch (...) {
        remove_quietly(copy);
        throw;
    }
    remove_quietly(copy);
    return true;
}

// Голова — наш собственный блоб, коммит которого упал между pin (шаг 6) и записью
// state.sqlite (шаг 8)? Узнаётся по содержимому, а не по одному заголовку: такой же
// заголовок мог сделать и другой писатель.
bool is_interrupted_commit(StateStore& state, const FsPaths& paths, const ChainBlob& head,
                           int64_t h_l, ByteSpan hash_l) {
    const auto& h = head.header;
    if (h.prev_msg_id != h_l) {
        return false;
    }
    if (h.kind == crypto::BlobKind::delta) {
        if (!same_bytes(h.prev_hash, hash_l)) {
            return false;
        }
        std::vector<int64_t> local_chain;
        for (const auto& el : state.list_chain()) {
            local_chain.push_back(el.msg_id);
        }
        if (local_chain != head.listed_chain) {
            return false;
        }
        const auto pending = state.list_pending();
        if (pending.empty()) {
            return false;
        }
        // Коммит склеил pending по состоянию на свой забор, а промежуточный забор мог дописать
        // новые патчи, пока блоб заливался (SPEC 6.6): ищем склейку среди префиксов pending.
        std::vector<Bytes> patches;
        patches.reserve(pending.size());
        for (const auto& p : pending) {
            patches.push_back(p.patchset);
            if (MetaStore::concat_patchsets(patches) == head.payload) {
                return true;
            }
        }
        return false;
    }
    const auto snap = snapshot_tmp_path(paths.tmp(), h.generation);
    if (!std::filesystem::exists(snap)) {
        return false;
    }
    return read_file(snap) == head.payload;
}

// Доделать шаг 8 упавшего коммита. index_dirty не трогаем: что было записано после забора
// патча, неизвестно (сессия потеряна). force_snapshot не сбрасываем и для снимка: записи после
// VACUUM INTO (а у дельты после забора патча) жили только в сессии, и если процесс завершился,
// не доделав шаг 8, их в блобе головы нет, а в локальном индексе они есть. Доказать по
// state.sqlite, что таких записей не было, нельзя, поэтому консервативно оставляем флаг как
// есть: после краха его уже поставил clean_shutdown = 0, после неудачного финального коммита
// при размонтировании — unmount(). Лишний снимок безвреден, потерянная дельта — нет.
void replay_commit_state(StateStore& state, const ChainBlob& head) {
    state.transaction([&] {
        state.set_int(state_key::base_generation, static_cast<int64_t>(head.header.generation));
        state.set_int(state_key::head_msg_id, head.msg_id);
        state.set_blob(state_key::head_hash, head.hash);
        state.clear_pending();
        if (head.header.kind == crypto::BlobKind::delta) {
            state.append_chain(head.msg_id, head.blob_size);
        } else {
            state.reset_chain_to_snapshot(head.msg_id, head.blob_size);
        }
    });
}

// Строки index_history для всех элементов текущей цепочки. Строки головы нет ни в одном
// блобе (её пишет шаг 8 после коммита), а после сборки из цепочки или сбоя между шагом 8 и
// записью строки её нет и локально. Без строки старая цепочка не была бы убрана (SPEC 7.4).
void reconcile_history(MetaStore& meta, StateStore& state) {
    const auto chain = state.list_chain();
    if (chain.empty()) {
        return;
    }
    const int64_t g = state.get_int_or(state_key::base_generation, 0);
    const int64_t first_gen = g - static_cast<int64_t>(chain.size()) + 1;
    meta.write(
        [&](Db& db) {
            for (const auto& el : chain) {
                const int64_t gen = first_gen + el.pos;
                Stmt st{db, "SELECT 1 FROM index_history WHERE generation = ?"};
                st.bind(1, gen);
                if (!st.step()) {
                    add_history(db, gen, el.msg_id,
                                el.pos == 0 ? BlobKind::snapshot : BlobKind::delta);
                }
            }
        },
        WriteKind::housekeeping);
}

// SPEC 7.2, шаг 7: сироты upload_journal.
void cleanup_orphans(store::Store& store, MetaStore& meta, StateStore& state, MountReport& rep) {
    std::vector<UploadRecord> orphans;  // у всех есть msg_id
    for (const auto& rec : state.list_uploads()) {
        if (rec.msg_id) {
            const auto chunk = meta.read([&](Db& db) { return get_chunk(db, rec.chunk_id); });
            if (!chunk || chunk->msg_id != *rec.msg_id) {
                orphans.push_back(rec);
                continue;
            }
        }
        state.remove_upload(rec.chunk_id);  // загрузка не дошла до конца или чанк в индексе
        ++rep.orphans_forgotten;
    }
    for (size_t at = 0; at < orphans.size(); at += kDeleteBatch) {
        const size_t end = std::min(orphans.size(), at + kDeleteBatch);
        std::vector<int64_t> ids;
        ids.reserve(end - at);
        for (size_t i = at; i < end; ++i) {
            ids.push_back(orphans[i].msg_id.value_or(0));
        }
        try {
            store.delete_messages(ids);
        } catch (const store::StoreError& e) {
            spdlog::warn("mount: cannot delete {} orphan messages, will retry next mount: {}",
                         ids.size(), e.what());
            return;
        }
        state.transaction([&] {
            for (size_t i = at; i < end; ++i) {
                state.remove_upload(orphans[i].chunk_id);
            }
        });
        rep.orphans_deleted += ids.size();
    }
}

// SPEC 7.2, шаг 8: inode с parent IS NULL — файлы, удалённые при открытом дескрипторе.
void delete_detached(MetaStore& meta, StateStore& state, const FsPaths& paths, MountReport& rep) {
    const int64_t g = state.get_int_or(state_key::base_generation, 0);
    for (const int64_t ino : meta.read([](Db& db) { return list_detached(db); })) {
        try {
            meta.write([&](Db& db) { delete_inode(db, ino, g); });
        } catch (const FsError& e) {
            spdlog::warn("mount: cannot delete detached inode {}: {}", ino, e.what());
            continue;
        }
        for (const auto& dc : state.list_dirty_chunks_for_ino(ino)) {
            remove_staging_file(paths, dc.path);
        }
        state.delete_dirty_chunks_for_ino(ino);
        state.delete_dirty_attrs(ino);
        ++rep.detached_deleted;
    }
}

// Оставшиеся от упавшего процесса временные файлы коммита и монтирования.
void cleanup_tmp(const FsPaths& paths) {
    std::error_code ec;
    for (const auto& entry : std::filesystem::directory_iterator(paths.tmp(), ec)) {
        const std::string name = entry.path().filename().string();
        for (const std::string_view prefix : {"index-", "blob-", "chain-", "catchup"}) {
            if (name.starts_with(prefix)) {
                remove_quietly(entry.path());
                break;
            }
        }
    }
}

}  // namespace

void ensure_fs_dirs(const FsPaths& paths) {
    std::filesystem::create_directories(paths.data_dir);
    std::filesystem::permissions(paths.data_dir, std::filesystem::perms::owner_all,
                                 std::filesystem::perm_options::replace);
    std::filesystem::create_directories(paths.staging());
    std::filesystem::create_directories(paths.tmp());
}

crypto::Key kek_from(const KeySource& source, const crypto::KdfParams& kdf) {
    if (!source.keyfile.empty()) {
        if (kdf.alg != crypto::KdfAlg::keyfile) {
            throw MountError(MountErrorKind::bad_key_source,
                             "this file system is protected by a passphrase, not a keyfile");
        }
        try {
            return crypto::read_keyfile(source.keyfile);
        } catch (const std::exception& e) {
            throw MountError(MountErrorKind::bad_key_source,
                             std::string("cannot read keyfile: ") + e.what());
        }
    }
    if (kdf.alg != crypto::KdfAlg::argon2id13) {
        throw MountError(MountErrorKind::bad_key_source,
                         "this file system is protected by a keyfile, not a passphrase");
    }
    if (!source.passphrase) {
        throw MountError(MountErrorKind::bad_key_source, "a passphrase is required");
    }
    try {
        return crypto::derive_kek(*source.passphrase, kdf);
    } catch (const std::invalid_argument&) {
        throw MountError(MountErrorKind::corrupt, "KDF parameters are out of range");
    }
}

MountedFs::MountedFs(store::Store& store, FsPaths paths, bool read_only)
    : store_(store), paths_(std::move(paths)), ro_mount_(read_only) {}

MountedFs::~MountedFs() = default;

void MountedFs::unmount() {
    if (unmounted_) {
        return;
    }
    unmounted_ = true;
    std::optional<std::string> failure;
    if (!ro_mount_) {
        try {
            committer_->commit_now();
        } catch (const store::CrashInjected&) {
            throw;
        } catch (const std::exception& e) {
            // SPEC 7.2: разбор до конца, ошибку получает вызывающий после остановки.
            spdlog::warn("unmount: final commit failed: {}", e.what());
            failure = e.what();
        }
    }
    committer_->stop();
    if (!ro_mount_) {
        if (failure) {
            // SPEC 7.2: записи, не забранные из сессии в pending_patchsets, пропадают вместе с
            // ней, и следующая дельта их бы не содержала. force_snapshot ставится строго до
            // clean_shutdown = 1: иначе сбой между записями дал бы «чистое» состояние без флага.
            state_->set_int(state_key::force_snapshot, 1);
        }
        state_->set_int(state_key::clean_shutdown, 1);
    }
    if (failure) {
        throw CommitError("final commit failed on unmount: " + *failure);
    }
}

namespace {

// Голова канала для монтирования. Если локально известна голова H (индекс и состояние на месте),
// а канал отдал «нет головы» или голову старше H, это, скорее всего, кэш TDLib (SPEC 2, 7.2
// шаг 4): чтение повторяется в пределах options.stale_head_window. Иначе ответ берётся как есть.
std::optional<store::HeadInfo> read_head_for_mount(store::Store& store, const FsPaths& paths,
                                                   const MountOptions& options) {
    auto head = store.read_head();
    if (options.stale_head_window <= std::chrono::milliseconds{0} ||
        !std::filesystem::exists(paths.index_db()) || !std::filesystem::exists(paths.state_db())) {
        return head;
    }
    int64_t local_head = 0;
    try {
        StateStore state{paths.state_db()};
        if (state.get_int_or(state_key::base_generation, 0) > 0) {
            local_head = state.get_int_or(state_key::head_msg_id, 0);
        }
    } catch (const std::exception&) {
        return head;  // состояние повреждено: разберёт основной путь монтирования
    }
    const auto behind = [&] { return !head || head->msg_id < local_head; };
    if (local_head == 0 || !behind()) {
        return head;
    }
    const auto until = std::chrono::steady_clock::now() + options.stale_head_window;
    while (behind() && std::chrono::steady_clock::now() < until) {
        spdlog::info(
            "mount: the channel head is {} the local head (TDLib cache?), checking again in {} s",
            head ? "older than" : "missing, but there is",
            options.stale_head_period.count() / 1000);
        std::this_thread::sleep_for(options.stale_head_period);
        head = store.read_head();
    }
    return head;
}

}  // namespace

std::unique_ptr<MountedFs> mount_fs(store::Store& store, const FsPaths& paths,
                                    const MountOptions& options) {
    crypto::init();
    ensure_fs_dirs(paths);
    const bool ro = options.read_only;

    // Шаг 4, заголовок головы (без аутентификации: нужен для KDF на новой машине).
    const auto head_info = read_head_for_mount(store, paths, options);
    if (!head_info) {
        throw MountError(MountErrorKind::no_head,
                         "the channel has no head (no pinned message): run 'telega-fs init', "
                         "or 'telega-fs recover --scan' if the head was lost");
    }
    crypto::IndexHeader claimed;
    try {
        claimed = crypto::parse_index_header(head_info->header);
    } catch (const crypto::CorruptError&) {
        throw MountError(MountErrorKind::not_index_blob,
                         "the pinned message is not an index blob: run 'telega-fs recover --scan'");
    }
    const FsUuid fs_uuid = claimed.fs_uuid;

    // Шаг 1: ключи. Есть keyring с ФС — им; keyring без ФС (login до init, SPEC 11) даёт только
    // tdlib_db_key и KEK, а master_key разворачивается из головы канала; keyring нет — KDF и
    // wrapped_master тоже из заголовка головы.
    std::optional<crypto::KeyringFile> keyring;  // с ФС
    std::optional<crypto::KeyringFile> bare;     // без ФС
    if (std::filesystem::exists(paths.keyring())) {
        crypto::KeyringFile loaded;
        try {
            loaded = crypto::load_keyring(paths.keyring());
        } catch (const crypto::CorruptError& e) {
            throw MountError(MountErrorKind::local_state,
                             std::string("local keyring: ") + e.what());
        }
        if (!crypto::keyring_has_fs(loaded)) {
            bare = loaded;
        } else if (loaded.fs_uuid != fs_uuid) {
            throw MountError(MountErrorKind::foreign_fs,
                             "the channel holds a different file system than the local profile");
        } else {
            keyring = loaded;
        }
    }
    crypto::KdfParams kdf = keyring ? keyring->kdf : claimed.kdf;
    crypto::WrappedKey wrapped = keyring ? keyring->wrapped_master : claimed.wrapped_master;
    if (!keyring && kdf.alg == crypto::KdfAlg::argon2id13 &&
        (kdf.opslimit > crypto_pwhash_OPSLIMIT_SENSITIVE ||
         kdf.memlimit > crypto_pwhash_MEMLIMIT_SENSITIVE)) {
        // Заголовок до расшифровки не аутентифицирован (SPEC 11): подложенный блоб не должен
        // заставлять Argon2 выделять произвольную память или считать произвольно долго.
        throw MountError(MountErrorKind::corrupt,
                         "KDF parameters in the head header exceed the allowed limits");
    }
    std::optional<crypto::Key> master;
    crypto::Key tdlib_db;
    std::optional<crypto::Key> bare_kek;  // KEK keyring без ФС: им обернётся master_key
    if (bare) {
        // Пароль должен подойти к обоим: к keyring (по wrapped_tdlib) и к голове канала. Вид
        // источника (пароль или keyfile) тоже один на оба (SPEC 11).
        const bool keyfile_given = !options.key.keyfile.empty();
        const bool keyring_keyfile = bare->kdf.alg == crypto::KdfAlg::keyfile;
        if (keyfile_given != keyring_keyfile) {
            throw MountError(MountErrorKind::bad_key_source,
                             keyring_keyfile
                                 ? "the local keyring is protected by a keyfile, not a passphrase"
                                 : "the local keyring is protected by a passphrase, not a keyfile");
        }
        if ((claimed.kdf.alg == crypto::KdfAlg::keyfile) != keyring_keyfile) {
            throw MountError(
                MountErrorKind::bad_key_source,
                std::string("the local keyring is protected by ") +
                    (keyring_keyfile ? "a keyfile" : "a passphrase") +
                    ", but the file system in the channel by " +
                    (keyring_keyfile ? "a passphrase" : "a keyfile") +
                    ": the keyring and the file system must use one passphrase or keyfile");
        }
        bare_kek = kek_from(options.key, bare->kdf);
        const auto tdlib = crypto::unlock_tdlib_key(*bare, *bare_kek);
        if (!tdlib) {
            throw MountError(MountErrorKind::wrong_password, "wrong passphrase or keyfile");
        }
        tdlib_db = *tdlib;
    }
    const crypto::Key kek = kek_from(options.key, kdf);
    if (keyring) {
        auto unlocked = crypto::unlock_keyring(*keyring, kek);
        if (unlocked) {
            master = unlocked->master;
            tdlib_db = unlocked->tdlib_db;
        }
    } else {
        master = crypto::unwrap_key(kek, wrapped);
        if (!bare) {
            tdlib_db = crypto::Key::random();
        }
    }
    if (!master) {
        throw MountError(MountErrorKind::wrong_password, "wrong passphrase or keyfile");
    }
    const crypto::SubKeys sub = crypto::derive_subkeys(*master);

    // Шаг 4, тело головы: аутентичность, fs_uuid.
    const Bytes head_blob = store.read_index(head_info->msg_id);
    if (head_blob.size() < crypto::kIndexHeaderSize ||
        !same_bytes(ByteSpan{head_blob}.first(crypto::kIndexHeaderSize), head_info->header)) {
        corrupt("the head header does not match the head blob");
    }
    const ChainBlob head = open_head(sub.index, fs_uuid, head_info->msg_id, head_blob);
    const uint64_t g_r = head.header.generation;
    // Голова подлинная. keyring с ФС сохраняется позже, после проверки локального состояния
    // (SPEC 11): иначе он привязался бы к ФС, с которой профиль всё равно не смонтируется. В
    // --ro keyring не меняется вовсе, master_key остаётся только в памяти (SPEC 7.2).
    std::optional<crypto::KeyringFile> keyring_to_save;
    if (bare) {
        // master_key обёрнут KEK этого keyring (SPEC 11). Блобы дальше несут KDF и
        // wrapped_master из keyring, как при обычном пути.
        keyring_to_save = crypto::attach_fs(*bare, fs_uuid, *bare_kek, *master);
        kdf = keyring_to_save->kdf;
        wrapped = keyring_to_save->wrapped_master;
    } else if (!keyring) {
        // Можно сохранить keyring с тем же wrapped_master, что в заголовке.
        keyring_to_save = crypto::KeyringFile{.fs_uuid = fs_uuid,
                                              .kdf = kdf,
                                              .wrapped_master = wrapped,
                                              .wrapped_tdlib = crypto::wrap_key(kek, tdlib_db)};
    }

    // Локальное состояние.
    const bool have_index = std::filesystem::exists(paths.index_db());
    if (!std::filesystem::exists(paths.state_db())) {
        StateStore::create(paths.state_db());
    }
    // --ro: схема не мигрируется, state.sqlite не меняется (SPEC 7.2).
    auto state = std::make_unique<StateStore>(paths.state_db(), !ro);
    if (const auto local_uuid = state->get_blob(state_key::fs_uuid);
        local_uuid && !same_bytes(*local_uuid, fs_uuid)) {
        throw MountError(MountErrorKind::foreign_fs,
                         "local state belongs to a different file system than the channel");
    }
    MountReport rep;
    rep.remote_generation = g_r;
    rep.unclean_shutdown = state->get_int(state_key::clean_shutdown) == std::optional<int64_t>{0};
    // Локального индекса нет, а локальные изменения есть: overlay и патчи без индекса
    // истолковать нельзя. Молча выбрасывать их нельзя (и тем более при --ro, который ничего
    // не меняет локально, SPEC 7.2), поэтому это такой же конфликт, как G_r > G_l с
    // локальными изменениями: отказ, а выбросить их можно только явным --discard-local.
    bool discard_orphaned = false;
    if (!have_index && has_local_changes(*state)) {
        if (ro || !options.discard_local) {
            throw MountError(
                MountErrorKind::local_conflict,
                std::string("the local index is missing, but local changes (overlay or "
                            "uncommitted patches) exist and cannot be applied to a new index. ") +
                    (ro ? "Read-only mount changes nothing locally, so it cannot build one: "
                          "mount without --ro and with --discard-local to drop the changes"
                        : "Restore index.sqlite, or mount with --discard-local to drop the "
                          "changes"));
        }
        discard_orphaned = true;
    }

    uint64_t g_l =
        have_index ? static_cast<uint64_t>(state->get_int_or(state_key::base_generation, 0)) : 0;
    int64_t h_l = state->get_int_or(state_key::head_msg_id, 0);
    Bytes hash_l = state->get_blob(state_key::head_hash).value_or(Bytes{});
    rep.local_generation = g_l;

    if (have_index && g_r == g_l + 1 && is_interrupted_commit(*state, paths, head, h_l, hash_l)) {
        spdlog::warn(
            "mount: the last commit (generation {}) was interrupted after pin, finishing it", g_r);
        replay_commit_state(*state, head);
        rep.resumed_commit = true;
        g_l = g_r;
        h_l = head.msg_id;
        hash_l.assign(head.hash.begin(), head.hash.end());
    }

    bool forced_local = false;
    if (!have_index) {
        const auto chain = fetch_or_corrupt(store, sub.index, fs_uuid, head.msg_id, head_blob);
        if (discard_orphaned) {
            spdlog::warn(
                "mount: dropping local changes that have no local index (--discard-local)");
            adopt_chain_without_index(*state, paths, chain, fs_uuid);
            rep.situation = MountSituation::discarded_local;
        } else {
            rebuild_index(paths, chain);
            set_state_from_chain(*state, chain, fs_uuid);
            rep.situation = MountSituation::built_fresh;
        }
    } else if (g_r == g_l) {
        if (h_l != head.msg_id || !same_bytes(hash_l, head.hash)) {
            corrupt("the head does not match the local head_hash of generation " +
                    std::to_string(g_l));
        }
        rep.situation = MountSituation::same_generation;
    } else if (g_r > g_l) {
        if (has_local_changes(*state)) {
            if (ro) {
                rep.situation = MountSituation::local_read_only;
            } else if (options.discard_local) {
                const auto chain =
                    fetch_or_corrupt(store, sub.index, fs_uuid, head.msg_id, head_blob);
                adopt_chain_discarding_local(*state, paths, chain, fs_uuid);
                rep.situation = MountSituation::discarded_local;
            } else {
                throw MountError(
                    MountErrorKind::local_conflict,
                    "the channel is at generation " + std::to_string(g_r) +
                        ", but local changes on top of generation " + std::to_string(g_l) +
                        " are not committed: mount with --ro, or with --discard-local to drop "
                        "them, or save them with 'recover --export-dirty' (M4)");
            }
        } else {
            const auto chain = fetch_or_corrupt(store, sub.index, fs_uuid, head.msg_id, head_blob);
            if (catch_up(paths, chain, g_l, h_l, hash_l)) {
                rep.situation = MountSituation::caught_up;
            } else {
                rebuild_index(paths, chain);
                rep.situation = MountSituation::rebuilt;
            }
            set_state_from_chain(*state, chain, fs_uuid);
        }
    } else {
        if (ro) {
            rep.situation = MountSituation::local_read_only;
        } else if (options.force_local) {
            // Следующий коммит — снимок локального индекса с generation G_l + 1 (больше и G_l,
            // и G_r), CAS — против реальной головы (её msg_id и G_r), см. set_cas_override.
            // index_dirty = 1: снимок уходит первым же коммитом, даже без новых изменений.
            forced_local = true;
            state->transaction([&] {
                state->set_int(state_key::force_snapshot, 1);
                state->set_int(state_key::index_dirty, 1);
            });
            rep.situation = MountSituation::forced_local;
        } else {
            throw MountError(MountErrorKind::rollback,
                             "the channel head (generation " + std::to_string(g_r) +
                                 ") is older than the local index (generation " +
                                 std::to_string(g_l) +
                                 "): the head was rolled back. Mount with --ro, or with "
                                 "--force-local to commit the local index over it");
        }
    }

    if (!ro) {
        if (rep.unclean_shutdown) {
            state->set_int(state_key::force_snapshot, 1);  // шаг 5 (SPEC 6.6)
        }
        if (!state->get_blob(state_key::fs_uuid)) {
            state->set_blob(state_key::fs_uuid, fs_uuid);
        }
    }

    auto fs = std::unique_ptr<MountedFs>(new MountedFs(store, paths, ro));
    fs->keys_ = sub;
    fs->tdlib_db_key_ = tdlib_db;
    fs->state_ = std::move(state);
    fs->meta_ = std::make_unique<MetaStore>(paths.index_db());
    install_dirty_hook(*fs->meta_, *fs->state_);
    fs->params_ = fs->meta_->read([](Db& db) { return read_params(db); });
    if (fs->params_.fs_uuid != fs_uuid) {
        throw MountError(MountErrorKind::foreign_fs,
                         "local index belongs to a different file system than the channel");
    }
    if (keyring_to_save && !ro) {
        crypto::save_keyring(paths.keyring(), *keyring_to_save);
    }

    if (!ro) {
        reconcile_history(*fs->meta_, *fs->state_);
        cleanup_orphans(store, *fs->meta_, *fs->state_, rep);
        delete_detached(*fs->meta_, *fs->state_, paths, rep);
        cleanup_tmp(paths);
        fs->state_->set_int(state_key::clean_shutdown, 0);  // шаг 9
    }

    CommitConfig config = options.commit;
    config.tmp_dir = paths.tmp();
    config.read_only = ro;
    fs->committer_ = std::make_unique<CommitManager>(
        *fs->meta_, *fs->state_, store,
        BlobKeys{.k_index = sub.index, .fs_uuid = fs_uuid, .kdf = kdf, .wrapped_master = wrapped},
        std::move(config));
    if (forced_local) {
        fs->committer_->set_cas_override(head.msg_id, g_r);
    }
    if (!ro && options.start_background) {
        fs->committer_->start();
    }
    fs->report_ = rep;
    spdlog::info("mounted: remote generation {}, local generation {}{}", g_r, g_l,
                 ro ? " (read-only)" : "");
    return fs;
}

}  // namespace telega::meta
