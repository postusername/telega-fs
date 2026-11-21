#pragma once

// Vfs — ядро файловой системы без FUSE (SPEC 6.2, 6.5, 8, 9): операции по inode, overlay
// (staging), запись, запечатывание и загрузка чанков, чтение, инлайн-файлы.
//
// FUSE-слой над Vfs тонкий: каждый вызов libfuse превращается в один вызов метода, ошибки —
// в errno через to_errno(). Все ошибки уровня ФС бросаются как meta::FsError с errno; любое
// другое исключение означает внутренний сбой и превращается в EIO.
//
// Модель конкурентности (SPEC 9):
// - данные одного inode защищает полосатая блокировка std::shared_mutex (полоса = ino mod N):
//   чтение и снимок чанка для шифрования — разделяемо, запись, truncate, применение залитого
//   чанка, запечатывание в инлайн и удаление файла — исключительно. Поток держит не больше
//   одной полосы. Store под полосой вызывать можно (RMW, чтение), под MetaStore — нет.
//   Цена этого — производительность, не корректность: read-modify-write скачивает чанк по
//   сети под исключительной полосой (на это время ждут все операции с inode и с любыми
//   другими inode той же полосы), а шифрование чанка в загрузке держит разделяемую полосу
//   (на это время ждут писатели). Полос 256, поэтому чужой inode задевается редко;
// - порядок блокировок: полоса inode → MetaStore → overlay (StateStore, затем структуры
//   Overlay под std::shared_mutex) → кэш открытого текста. Счётчики lookup/open, очередь
//   загрузок и семафор скачиваний — листовые мьютексы;
// - загрузки идут в пуле из upload_workers() потоков (SPEC 8.1, «Параллельность загрузок»):
//   upload_concurrency, а если Store собирает альбомы (Store::gathers_albums) —
//   min(10 · upload_concurrency, 32). Ждущие в Store.put_chunk нужны ему, чтобы набрать альбом,
//   а реальных одновременных отправок не больше upload_concurrency ограничивает сам Store.
//   Отдельный поток-таймер запечатывает чанки по seal_delay. Store при загрузке вызывается без
//   блокировок, затем результат применяется под полосой inode, если чанк ещё нужен (см.
//   upload_chunk в vfs_upload.cpp).

#include <sys/stat.h>

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <exception>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/bytes.h"
#include "crypto/keys.h"
#include "meta/committer.h"
#include "meta/index_db.h"
#include "meta/index_ops.h"
#include "meta/state_db.h"
#include "store/store.h"
#include "vfs/chunk_reader.h"
#include "vfs/overlay.h"

namespace telega::vfs {

// Параметры работы (SPEC 8, 10.2, 12.2). FUSE-слой заполняет их из app::Profile.
// Параметры ФС (chunk_size, inline_max, inline_budget, fs_uuid) берутся из index.sqlite.
struct VfsOptions {
    std::chrono::milliseconds seal_delay{5'000};
    uint32_t upload_concurrency = 2;
    uint32_t download_concurrency = 4;
    uint64_t plain_cache = uint64_t{64} << 20U;
    std::chrono::milliseconds read_timeout{60'000};
    // Задержка повтора транзиентной ошибки Store: от retry_initial с удвоением до retry_max
    // (SPEC 10.2: 1–60 с). Тесты ставят миллисекунды.
    std::chrono::milliseconds retry_initial{1'000};
    std::chrono::milliseconds retry_max{60'000};
    // Сколько fsync, sync, flush_all и shutdown ждут заливки, затем EIO (SPEC 10.2, 12.2).
    // Фоновые повторы при этом продолжаются, overlay не меняется.
    std::chrono::milliseconds sync_timeout{300'000};
    uint64_t virtual_capacity = uint64_t{1} << 50U;  // 1 ПиБ (SPEC 9, statfs)
    bool read_only = false;  // --ro; кроме того, EROFS, если committer.read_only()
};

struct VfsPaths {
    std::filesystem::path staging;  // <data>/staging, создаётся при необходимости
    std::filesystem::path tmp;  // <data>/tmp, создаётся при необходимости
};

inline constexpr uint32_t kBlockSize = 4096;
inline constexpr uint32_t kNameMax = 255;

// Атрибуты inode: inodes, поверх них dirty_attrs (SPEC 9).
struct Attr {
    int64_t ino = 0;
    meta::InodeKind kind = meta::InodeKind::file;
    uint32_t mode = 0;  // только биты прав; тип — по kind
    uint32_t nlink = 1;
    uint32_t uid = 0;
    uint32_t gid = 0;
    int64_t size = 0;
    int64_t blocks = 0;  // в единицах 512 байт: size, округлённый вверх до 4096
    uint32_t blksize = kBlockSize;
    int64_t atime_ns = 0;
    int64_t mtime_ns = 0;
    int64_t ctime_ns = 0;
};

// Заполнить struct stat (st_mode с типом S_IFREG/S_IFDIR/S_IFLNK).
void fill_stat(const Attr& attr, struct stat& st);

// Изменение атрибутов (setattr). Время — либо значение, либо «сейчас» (UTIME_NOW).
struct SetAttr {
    std::optional<uint32_t> mode;
    std::optional<uint32_t> uid;
    std::optional<uint32_t> gid;
    std::optional<int64_t> size;  // truncate
    std::optional<int64_t> atime_ns;
    bool atime_now = false;
    std::optional<int64_t> mtime_ns;
    bool mtime_now = false;
};

struct Created {
    Attr attr;
    uint64_t fh = 0;
};

// Запись readdir. off — порядковый номер записи в потоке каталога (с 1): следующий вызов
// readdir с этим off продолжит сразу после неё. Первые две записи — "." и "..".
struct DirEntry {
    Bytes name;
    int64_t ino = 0;
    meta::InodeKind kind = meta::InodeKind::file;
    uint64_t off = 0;
    std::optional<Attr> attr;  // только для readdir(plus = true)
};

struct StatFs {
    uint64_t bsize = kBlockSize;
    uint64_t frsize = kBlockSize;
    uint64_t blocks = 0;
    uint64_t bfree = 0;
    uint64_t bavail = 0;
    uint64_t files = 0;
    uint64_t ffree = 0;
    uint64_t namemax = kNameMax;
};

// Состояние загрузок (для status, SPEC 12.3).
struct VfsStatus {
    size_t dirty_chunks = 0;
    size_t dirty_inodes = 0;
    size_t queued = 0;
    size_t uploading = 0;
    size_t failed = 0;
    uint64_t uploaded_chunks = 0;
    bool fatal = false;
    std::string last_error;
};

// errno для ответа FUSE: FsError — её код, StoreError, CorruptError, CommitError и всё
// остальное — EIO (SysError с ENOSPC/EDQUOT — свой код).
int to_errno(const std::exception& e) noexcept;

class Vfs {
  public:
    // Поднимает overlay из state.sqlite и staging/ (восстановление после перезапуска) и
    // ставит в очередь дозаливку всех грязных чанков. Уборку upload_journal и
    // detached-инодов делает монтирование до создания Vfs.
    Vfs(meta::MetaStore& meta, meta::StateStore& state, store::Store& store,
        meta::Committer& committer, crypto::SubKeys keys, VfsPaths paths, VfsOptions options);
    Vfs(const Vfs&) = delete;
    Vfs& operator=(const Vfs&) = delete;
    Vfs(Vfs&&) = delete;
    Vfs& operator=(Vfs&&) = delete;
    // Останавливает фоновые потоки без заливки (как падение процесса: overlay остаётся на
    // диске и подхватывается следующим Vfs). Для размонтирования сначала shutdown().
    ~Vfs();

    // --- пространство имён ---

    // lookup, mkdir, create, mknod, symlink увеличивают счётчик lookup inode на 1
    // (как reply_entry в FUSE); forget уменьшает. Для readdirplus FUSE-слой сам вызывает
    // remember_lookup для каждой отданной ядру записи, кроме "." и "..".
    Attr lookup(int64_t parent, ByteSpan name);
    void forget(int64_t ino, uint64_t nlookup);
    void remember_lookup(int64_t ino);

    Attr getattr(int64_t ino);
    Attr setattr(int64_t ino, const SetAttr& change);

    Attr mkdir(int64_t parent, ByteSpan name, uint32_t mode, uint32_t uid, uint32_t gid);
    void rmdir(int64_t parent, ByteSpan name);
    Created create(int64_t parent, ByteSpan name, uint32_t mode, int flags, uint32_t uid,
                   uint32_t gid);
    // Только обычный файл (S_IFREG или тип 0), иначе EPERM.
    Attr mknod(int64_t parent, ByteSpan name, uint32_t mode, uint32_t uid, uint32_t gid);
    Attr symlink(int64_t parent, ByteSpan name, ByteSpan target, uint32_t uid, uint32_t gid);
    Bytes readlink(int64_t ino);
    void unlink(int64_t parent, ByteSpan name);
    // flags: 0 или RENAME_NOREPLACE; RENAME_EXCHANGE и прочее — EINVAL.
    void rename(int64_t parent, ByteSpan name, int64_t new_parent, ByteSpan new_name,
                unsigned flags);

    [[noreturn]] static void link();       // EPERM
    [[noreturn]] static void xattr();      // ENOTSUP (set/get/list/removexattr)
    [[noreturn]] static void fallocate();  // EOPNOTSUPP

    // --- файлы ---

    // O_TRUNC при открытии на запись — truncate(0). Возвращает дескриптор (fh).
    uint64_t open(int64_t ino, int flags);
    // Последний release файла запечатывает его асинхронно (SPEC 8.1).
    void release(uint64_t fh);
    void flush(uint64_t fh);
    // Прочитать до size байт с позиции off; короче — только на конце файла.
    Bytes read(int64_t ino, uint64_t off, size_t size);
    size_t write(int64_t ino, uint64_t off, ByteSpan data);
    // Залить все грязные чанки файла, дождаться и сделать коммит (SPEC 7.3, 8.1).
    void fsync(int64_t ino);

    // --- каталоги ---

    uint64_t opendir(int64_t ino);
    // До max записей, начиная после записи с номером off (0 — с начала).
    std::vector<DirEntry> readdir(uint64_t fh, uint64_t off, size_t max, bool plus);
    void releasedir(uint64_t fh);
    void fsyncdir(int64_t ino);  // commit_now

    StatFs statfs();

    // --- завершение ---

    // Запечатать и залить всё грязное (кроме удалённых открытых файлов) и дождаться, но не
    // дольше sync_timeout. Ошибка заливки или таймаут — FsError(EIO). В read-only ничего не делает.
    void flush_all();
    // flush_all + commit_now (sync).
    void sync();
    // Для размонтирования: flush_all (не дольше sync_timeout), затем остановить фоновые
    // потоки в любом случае: незалитое остаётся в overlay. Ошибки заливки и таймаут
    // пробрасываются после остановки. Повторный вызов безопасен.
    void shutdown();

    VfsStatus status();

  private:
    struct Handle {
        int64_t ino = 0;
        bool writable = false;
    };
    struct DirHandle {
        std::mutex mu;
        int64_t ino = 0;
        int64_t parent = 0;
        // names[i] — имя записи с номером i + 1 ("." и ".." — номера 1 и 2).
        std::vector<Bytes> names;
    };
    struct Refs {
        uint64_t lookups = 0;
        uint64_t opens = 0;
    };
    // Задача пула загрузчиков.
    struct Job {
        enum class Kind : uint8_t { seal, upload } kind = Kind::seal;
        int64_t ino = 0;
        int64_t idx = 0;
        bool only_expired = false;  // seal по таймеру: только чанки старше seal_delay
    };
    enum class SealMode : uint8_t { timer, release, explicit_request };

    static constexpr size_t kStripes = 256;

    // --- общие помощники (vfs.cpp) ---
    std::shared_mutex& stripe(int64_t ino);
    void check_alive() const;     // fatal → EIO
    void check_writable() const;  // EROFS, fatal → EIO
    [[nodiscard]] bool read_only() const;
    int64_t base_gen() const;
    // dirty — dirty_attrs inode, прочитанные ДО снимка индекса (то есть до входа в
    // meta_.read): применение залитого чанка сначала пишет индекс, потом убирает dirty_attrs,
    // так что этот порядок не даёт увидеть старый размер. Читать dirty_attrs внутри
    // meta_.read нельзя: снимок индекса к тому моменту может быть старее очистки. Для нового
    // inode (mkdir, create, ...) dirty_attrs быть не может: std::nullopt.
    static Attr make_attr(meta::Db& db, const meta::Inode& inode,
                          const std::optional<meta::DirtyAttrs>& dirty);
    meta::Inode get_inode_or_throw(int64_t ino);
    int64_t effective_size(const meta::Inode& inode) const;
    void add_lookup(int64_t ino, uint64_t n);
    bool in_use(int64_t ino);
    // Удалить inode файла из индекса и его overlay (полоса inode захвачена вызывающим).
    void drop_file_data(int64_t ino);
    // Удалить отсоединённый inode, если он больше не открыт и не известен ядру.
    void maybe_reap(int64_t ino);
    // Удаление или отсоединение файла-жертвы unlink/rename внутри транзакции индекса.
    // true — inode удалён (нужна уборка overlay после транзакции).
    bool remove_victim(meta::Db& db, const meta::Inode& victim);
    static void touch_dir(meta::Db& db, int64_t dir, int64_t now);

    // --- данные (vfs_data.cpp) ---
    void truncate_locked(int64_t ino, const meta::Inode& inode, int64_t new_size);
    // Обрезать экстенты и инлайн-данные индекса по размеру (SPEC 6.2).
    void clamp_index(meta::Db& db, int64_t ino, int64_t size);
    // Создать staging-файл позиции idx (RMW / копия инлайна / пустой).
    void materialize(int64_t ino, int64_t idx, const meta::Inode& inode,
                     const std::optional<std::pair<uint64_t, uint64_t>>& write_range,
                     std::optional<Bytes>& inline_data);
    void read_staging(const ChunkKey& key, uint64_t off, MutableByteSpan out);
    std::optional<meta::Chunk> chunk_row(int64_t chunk_id);

    // --- запечатывание и загрузка (vfs_upload.cpp) ---
    void recover();
    // Закоммитить индекс (fsync, fsyncdir, sync). В --ro ничего не делает; если коммитер стал
    // read-only (CAS-конфликт) или коммит не удался — FsError(EIO), SPEC 7.3.
    void commit_or_throw();
    // Пустой staging-файл позиции не заливается: позиция — дыра, строка overlay уходит
    // (SPEC 8.1). Берёт исключительную полосу сама.
    void resolve_empty_position(const ChunkKey& key, uint64_t incarnation);
    // Поставить в очередь позиции inode, ждавшие уход позиции 0 инлайн-файла.
    void release_blocked(int64_t ino);
    // Заново создать позицию 0 инлайн-файла в overlay из inline_data, если у inode есть грязные
    // позиции idx >= 1 и непустой инлайн (SPEC 8.1). false — условия не выполнены. Вызывать под
    // исключительной полосой или из recover.
    bool restore_inline_position(int64_t ino);
    // Индекс надо обрезать по size (экстенты или инлайн длиннее)?
    bool index_needs_clamp(int64_t ino, int64_t size);
    // Потоков загрузки: upload_concurrency, для Store с альбомами min(10 · upload_concurrency, 32)
    // (SPEC 8.1). Считается в 64 битах: upload_concurrency из конфига ничем не ограничена сверху.
    [[nodiscard]] uint32_t upload_workers() const;
    void start_threads();
    void stop_threads();
    void worker_loop();
    void timer_loop();
    void push_job(const Job& job);
    // Запечатать файл: выбрать класс хранения, инлайн — сразу, иначе поставить чанки в
    // очередь. Возвращает цели ожидания (для explicit_request).
    std::vector<UploadTarget> seal_file(int64_t ino, SealMode mode);
    bool try_seal_inline(int64_t ino, const meta::Inode& inode, int64_t size);
    void enqueue_upload(const ChunkKey& key, bool explicit_request);
    void upload_chunk(const ChunkKey& key);
    // Дождаться целей, но не дольше sync_timeout; ошибка заливки или таймаут — FsError(EIO).
    void wait_targets(const std::vector<UploadTarget>& targets);
    void discard_upload(int64_t chunk_id, std::optional<int64_t> msg_id);
    void set_fatal(const std::string& what);
    void note_error(const std::string& what);
    // Пауза повтора; false — Vfs останавливается.
    bool backoff(std::chrono::milliseconds& delay);

    meta::MetaStore& meta_;
    meta::StateStore& state_;
    store::Store& store_;
    meta::Committer& committer_;
    crypto::SubKeys keys_;
    VfsPaths paths_;
    VfsOptions opts_;
    meta::IndexParams params_;
    int64_t chunk_size_ = 0;

    std::unique_ptr<Overlay> overlay_;
    std::unique_ptr<ChunkReader> reader_;
    std::array<std::shared_mutex, kStripes> stripes_;

    std::atomic<bool> fatal_{false};
    std::atomic<uint64_t> uploaded_{0};

    std::mutex refs_mu_;  // лист: refs_, handles_, dir_handles_, next_fh_
    std::unordered_map<int64_t, Refs> refs_;
    std::unordered_map<uint64_t, Handle> handles_;
    std::unordered_map<uint64_t, std::shared_ptr<DirHandle>> dir_handles_;
    // Удалённые, но открытые файлы: их чанки не заливаются, таймер их не трогает.
    std::unordered_set<int64_t> detached_;
    uint64_t next_fh_ = 1;

    std::mutex err_mu_;  // лист
    std::string last_error_;

    std::mutex q_mu_;  // лист: очередь задач и флаги остановки
    std::condition_variable q_cv_;  // задачи для пула
    std::condition_variable stop_cv_;  // таймер и паузы повторов: будит только остановка
    std::atomic<bool> stop_flag_{false};
    std::deque<Job> queue_;
    std::unordered_set<int64_t> seal_pending_;  // ino с задачей seal в очереди
    bool stopping_ = false;
    bool stopped_ = false;
    std::vector<std::thread> workers_;
    std::thread timer_;
};

}  // namespace telega::vfs
