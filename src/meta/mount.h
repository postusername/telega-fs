#pragma once

// Монтирование и размонтирование (SPEC 7.2 шаги 1, 4–9; шаги 2–3 про TDLib — M2).
//
// mount_fs разблокирует ключи, читает и проверяет голову канала, сводит локальное состояние
// с удалённым по таблице 7.2, убирает сирот и открытые удалённые inode и отдаёт MountedFs —
// владельца MetaStore, StateStore, CommitManager и ключей. Его берут Vfs и FUSE.
//
// Решения, которых нет в спеке (подробнее в mount.cpp):
// - «сбой между pin и шагом 8» (G_r = G_l + 1, голова — наш блоб): узнаётся по содержимому
//   (дельта — тот же список цепочки и та же склейка pending_patchsets; снимок — тот же файл
//   tmp/index-<G>.sqlite), и шаг 8 доделывается при монтировании;
// - догон дельтами: из копии локального индекса сначала удаляются строки index_history с
//   generation ≥ G_l (служебные записи после коммита G_l, которых нет в блобе G_l), иначе
//   дельта другой машины с той же строкой даёт конфликт; при любом конфликте — полная сборка;
// - --ro ничего не меняет в канале и в индексе: при конфликте или откате отдаётся локальный
//   индекс как есть.

#include <filesystem>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>

#include "crypto/keyring.h"
#include "crypto/keys.h"
#include "meta/commit.h"
#include "meta/index_db.h"
#include "meta/index_ops.h"
#include "meta/state_db.h"
#include "store/store.h"

namespace telega::meta {

// Файлы data-каталога профиля (SPEC 12.1). app::ProfilePaths переводится в эту структуру.
struct FsPaths {
    std::filesystem::path data_dir;

    [[nodiscard]] std::filesystem::path keyring() const { return data_dir / "keyring"; }
    [[nodiscard]] std::filesystem::path index_db() const { return data_dir / "index.sqlite"; }
    [[nodiscard]] std::filesystem::path state_db() const { return data_dir / "state.sqlite"; }
    [[nodiscard]] std::filesystem::path staging() const { return data_dir / "staging"; }
    [[nodiscard]] std::filesystem::path tmp() const { return data_dir / "tmp"; }
};

// Создать data_dir (0700), staging/ и tmp/.
void ensure_fs_dirs(const FsPaths& paths);

// Откуда KEK (SPEC 11): keyfile, если путь задан, иначе пароль.
struct KeySource {
    std::optional<std::string> passphrase;
    std::filesystem::path keyfile;
};

enum class MountErrorKind : uint8_t {
    wrong_password,  // неверный пароль или keyfile
    bad_key_source,  // ФС создана с keyfile, а дан пароль (или наоборот), или ничего не дано
    no_head,  // в канале нет закреплённого: init или recover --scan
    not_index_blob,  // голова — не блоб индекса
    corrupt,  // порча, подмена, перестановка или пропуск элемента цепочки
    foreign_fs,      // канал или локальные файлы от другой ФС
    local_conflict,  // G_r > G_l и есть локальные изменения
    rollback,        // G_r < G_l: откат головы
    local_state,  // локальные файлы испорчены или несогласованы
};

class MountError : public std::runtime_error {
  public:
    MountError(MountErrorKind kind, const std::string& what)
        : std::runtime_error(what), kind_(kind) {}
    [[nodiscard]] MountErrorKind kind() const noexcept { return kind_; }

  private:
    MountErrorKind kind_;
};

// KEK из источника ключа по параметрам KDF (из keyring или заголовка блоба).
// Несовпадение вида источника и kdf.alg — MountError(bad_key_source).
crypto::Key kek_from(const KeySource& source, const crypto::KdfParams& kdf);

struct MountOptions {
    KeySource key;
    bool read_only = false;  // --ro
    bool discard_local = false;  // --discard-local: G_r > G_l с локальными изменениями
    bool force_local = false;  // --force-local: G_r < G_l, следующий коммит — снимок
    CommitConfig commit;           // tmp_dir и read_only заполняет mount_fs
    bool start_background = true;  // фоновый поток CommitManager
};

// Какая строка таблицы 7.2 сработала.
enum class MountSituation : uint8_t {
    built_fresh,  // локального индекса не было: собран из цепочки
    same_generation,  // G_r == G_l
    caught_up,        // G_r > G_l: применены недостающие дельты
    rebuilt,          // G_r > G_l: индекс собран из цепочки заново
    discarded_local,  // G_r > G_l с изменениями, --discard-local
    forced_local,     // G_r < G_l, --force-local
    local_read_only,  // --ro при конфликте или откате: отдан локальный индекс как есть
};

struct MountReport {
    MountSituation situation = MountSituation::same_generation;
    uint64_t remote_generation = 0;
    uint64_t local_generation = 0;  // до монтирования (0 — локального индекса не было)
    bool unclean_shutdown = false;  // clean_shutdown = 0: следующий коммит — снимок
    bool resumed_commit = false;  // доделан шаг 8 коммита, упавшего после pin
    size_t orphans_deleted = 0;  // сообщения сирот из upload_journal
    size_t orphans_forgotten = 0;  // записи без msg_id и записи о чанках из индекса
    size_t detached_deleted = 0;  // inode с parent IS NULL
};

class MountedFs {
  public:
    MountedFs(const MountedFs&) = delete;
    MountedFs& operator=(const MountedFs&) = delete;
    MountedFs(MountedFs&&) = delete;
    MountedFs& operator=(MountedFs&&) = delete;
    // Без unmount() ведёт себя как падение процесса: фоновый поток останавливается, но
    // финального коммита и clean_shutdown = 1 нет.
    ~MountedFs();

    [[nodiscard]] MetaStore& meta() { return *meta_; }
    [[nodiscard]] StateStore& state() { return *state_; }
    [[nodiscard]] CommitManager& committer() { return *committer_; }
    [[nodiscard]] store::Store& store() { return store_; }
    [[nodiscard]] const crypto::SubKeys& keys() const { return keys_; }
    [[nodiscard]] const crypto::Key& tdlib_db_key() const { return tdlib_db_key_; }
    [[nodiscard]] const FsUuid& fs_uuid() const { return params_.fs_uuid; }
    [[nodiscard]] const IndexParams& params() const { return params_; }
    [[nodiscard]] const FsPaths& paths() const { return paths_; }
    [[nodiscard]] const MountReport& report() const { return report_; }
    [[nodiscard]] bool read_only() const { return committer_->read_only(); }

    // Размонтирование (SPEC 7.2, последний абзац): финальный коммит (его ошибка только
    // пишется в лог), остановка CommitManager, clean_shutdown = 1. Загрузки до этого
    // дожидается Vfs. При --ro только остановка. Повторный вызов ничего не делает.
    void unmount();

  private:
    friend std::unique_ptr<MountedFs> mount_fs(store::Store& store, const FsPaths& paths,
                                               const MountOptions& options);
    MountedFs(store::Store& store, FsPaths paths, bool read_only);

    store::Store& store_;
    FsPaths paths_;
    bool ro_mount_;
    bool unmounted_ = false;
    crypto::SubKeys keys_;
    crypto::Key tdlib_db_key_;
    IndexParams params_;
    MountReport report_;
    // Порядок важен: CommitManager уничтожается первым.
    std::unique_ptr<StateStore> state_;
    std::unique_ptr<MetaStore> meta_;
    std::unique_ptr<CommitManager> committer_;
};

// Смонтировать ФС из канала store в профиль paths. Ошибки: MountError (см. виды),
// store::StoreError (сеть), прочие — как есть.
std::unique_ptr<MountedFs> mount_fs(store::Store& store, const FsPaths& paths,
                                    const MountOptions& options);

}  // namespace telega::meta
