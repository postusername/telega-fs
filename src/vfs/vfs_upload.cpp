// Vfs: восстановление overlay, запечатывание, загрузка чанков, фоновые потоки (SPEC 6.5,
// 8.1, 10.2).
//
// Загрузка позиционного чанка (upload_chunk) идёт в три фазы (а позиция ≥ 1 инлайн-файла,
// пока его позиция 0 ещё в overlay, не загружается вовсе, см. «блокировку» ниже):
// 1. Под разделяемой полосой inode (запись в файл ждёт, чтение идёт): запомнить version,
//    посчитать content_mac; если в chunks уже есть такой позиционный чанк (дедупликация,
//    SPEC 8.1, шаг 2), шифрование и заливка пропускаются, иначе выделить chunk_id и
//    зашифровать staging в tmp/. Снимок согласован: писатели держат полосу исключительно.
// 2. Без блокировок: upload_started → Store::put_chunk (транзиентные ошибки — повтор
//    бесконечно с задержкой, постоянные — чанк остаётся в overlay с ошибкой) →
//    upload_finished.
// 3. Под исключительной полосой: если строка overlay того же воплощения жива и inode ещё
//    существует и не удалён, одной транзакцией index.sqlite вставить чанк и экстент (len не
//    длиннее текущего размера и обрезок truncate после снимка), при последнем грязном чанке
//    перенести dirty_attrs в inodes; затем одной транзакцией state.sqlite убрать строку
//    (если version не изменилась), dirty_attrs и запись журнала. Иначе чанк не нужен:
//    сообщение удаляется из канала, запись журнала убирается.
//
// Дедупликация (SPEC 8.1, шаг 2). Только на живые чанки (refcount > 0): мёртвый не оживляется,
// потому что уборка могла уже удалить его сообщение, а строка с refcount = 0 осталась в индексе
// канала (служебная запись без коммита, SPEC 6.6, 7.4). Поиск в фазе 1 — только подсказка
// «шифровать не нужно»: он ничего не меняет, поэтому сбой или уход чанка не оставляет ни
// лишнего refcount, ни записи в upload_journal. Привязка идёт в фазе 3 той же транзакцией, что
// и экстент: под блокировкой записи MetaStore find_chunk_by_mac и put_extent (refcount++)
// неделимы относительно уборки (commit.cpp, gc_chunks удаляет строку под той же блокировкой и
// перепроверяет refcount = 0). Если между подсказкой и привязкой чанк умер или его убрали,
// заливаемого файла нет: позиция ставится в очередь снова, и второй проход уже зальёт её
// обычным путём. После сбоя между транзакциями index.sqlite и state.sqlite (строка
// dirty_chunks пережила привязку) повторная привязка к тому же чанку не меняет refcount
// (put_extent: тот же chunk_id).
// Пакеты не дедуплицируются (SPEC 6.5).
//
// Порядок позиций инлайн-файла (SPEC 6.2: у инлайн-файла нет экстентов): запись дальше
// позиции 0 переносит его данные в overlay позиции 0, и в индексе остаётся inline_data, пока
// позиция 0 не применена. Экстент idx ≥ 1 нельзя применить раньше (clear_inline оставил бы
// индекс без начала файла, и коммит в этом окне зафиксировал бы поколение, которого не было).
// Ждать позицию 0 внутри загрузки нельзя (пул может упереться в сам себя), поэтому такая
// позиция в фазе 1 помечается «заблокированной» и уходит без заливки, а когда позиция 0
// покидает overlay (release_blocked) — ставится в очередь снова.
//
// Класс хранения (SPEC 6.5) выбирает seal_file при каждом запечатывании по размеру: инлайн
// (try_seal_inline), пакет (seal_to_pack → PackWriter, если файл не больше pack_threshold и
// не поместился в инлайн, в том числе при исчерпанном бюджете), иначе позиционный чанк
// (upload_chunk). Файл меняет класс одной транзакцией индекса: новые данные записываются,
// старые освобождаются (put_extent, set_inline, clear_inline).
//
// Место (SPEC 8.3): зашифрованная копия чанка в tmp/ берётся из StagingBudget целиком, до полосы
// inode, размером полного зашифрованного чанка (из таких аренд и состоит резерв заливки R),
// после шифрования уменьшается до фактического размера и возвращается, когда tmp-файл удалён.
//
// Пустой staging-файл (длина 0) не заливается: SPEC 8.1, позиция без валидных байт — дыра.
// Индекс не меняется (экстент, если он был, остаётся: откат не глубже последнего коммита),
// строка overlay удаляется (resolve_empty_position).

#include <fcntl.h>
#include <spdlog/spdlog.h>
#include <sys/stat.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <set>
#include <system_error>

#include "common/files.h"
#include "crypto/chunk.h"
#include "meta/errors.h"
#include "vfs/vfs.h"

namespace telega::vfs {

using meta::Db;
using meta::FsError;
using meta::Inode;
using meta::InodeKind;

namespace {

int64_t now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

constexpr std::string_view kTmpPrefix = "chunk-";
constexpr std::string_view kTmpPackPrefix = "pack-";  // собранный открытый текст пакета

}  // namespace

// --- восстановление после перезапуска ---

bool Vfs::index_needs_clamp(int64_t ino, int64_t size) {
    return meta_.read([&](Db& db) {
        const auto exts = meta::list_extents(db, ino);
        if (!exts.empty()) {
            const meta::Extent& e = exts.back();
            if (e.idx * chunk_size_ + e.len > size) {
                return true;
            }
        }
        const auto inode = meta::get_inode(db, ino);
        return inode && inode->inline_len && *inode->inline_len > size;
    });
}

bool Vfs::restore_inline_position(int64_t ino) {
    const std::vector<int64_t> idxs = overlay_->indices(ino);
    if (std::none_of(idxs.begin(), idxs.end(), [](int64_t idx) { return idx >= 1; })) {
        return false;
    }
    const auto inode = meta_.read([&](Db& db) { return meta::get_inode(db, ino); });
    if (!inode || inode->kind != InodeKind::file || !inode->inline_len || *inode->inline_len == 0) {
        return false;  // инлайна нет или он пуст (после truncate): терять нечего
    }
    const auto data = meta_.read([&](Db& db) { return meta::get_inline(db, ino); });
    if (!data || data->empty()) {
        return false;
    }
    // Как при первой записи в инлайн-файл за позицию 0 (vfs_data.cpp, write): копия inline_data
    // в staging и строка dirty_chunks. Файл пишется раньше строки: сбой между ними оставит
    // сироту без строки, которую уберёт следующий старт.
    const Fd fd = Fd::open(overlay_->path_of({ino, 0}), O_WRONLY | O_CREAT | O_TRUNC, 0600);
    pwrite_full(fd.get(), *data, 0);
    const auto attrs = overlay_->attrs(ino);
    const int64_t now = now_ns();
    // Учёт (SPEC 8.3): не больше inline_max, под полосой ждать места нельзя — берётся без
    // проверки лимита (редкий случай восстановления после сбоя).
    const uint64_t old_len = overlay_->staged({ino, 0});
    if (data->size() > old_len) {
        budget_->charge_unchecked(data->size() - old_len);
    }
    const std::array<ChunkWrite, 1> writes{ChunkWrite{0, 0, 0, data->size()}};
    overlay_->commit_write(
        ino, writes, attrs.value_or(meta::DirtyAttrs{ino, inode->size, inode->mtime_ns}), now);
    spdlog::warn("vfs: position 0 of inline inode {} is restored from the index", ino);
    return true;
}

void Vfs::recover() {
    overlay_->load();

    // tmp/: недошифрованные или незалитые файлы чанков прошлого сеанса.
    std::error_code ec;
    for (const auto& de : std::filesystem::directory_iterator(paths_.tmp, ec)) {
        const std::string name = de.path().filename().string();
        if (name.starts_with(kTmpPrefix) || name.starts_with(kTmpPackPrefix)) {
            std::filesystem::remove(de.path(), ec);
        }
    }

    // staging/: файлы без строки dirty_chunks — сироты (сбой между созданием файла и
    // записью строки).
    std::set<std::string> known;
    for (const int64_t ino : overlay_->chunk_inos()) {
        for (const int64_t idx : overlay_->indices(ino)) {
            const auto path = overlay_->path_of({ino, idx});
            if (!std::filesystem::exists(path, ec)) {
                // Строка пишется долговечно до возврата из write, а staging-файл синхронизируется
                // только по fsync: после отключения питания строка может пережить файл (SPEC
                // 8.1). Строка удаляется, позиция снова читается из индекса (откат не глубже
                // последнего коммита), ошибка видна в status. Создавать файл заново нельзя:
                // пустой файл превратил бы закоммиченные данные в нули. dirty_attrs остаются:
                // размер и mtime — отдельное долговечное состояние, их перенесёт ветка
                // ниже, когда у inode не останется строк.
                note_error("staging file of inode " + std::to_string(ino) + " position " +
                           std::to_string(idx) +
                           " is missing; the position is read from the index");
                overlay_->drop_position({ino, idx}, false, !read_only());
                continue;
            }
            known.insert(Overlay::file_name({ino, idx}));
        }
    }
    for (const auto& de : std::filesystem::directory_iterator(paths_.staging, ec)) {
        if (de.is_regular_file() && !known.contains(de.path().filename().string())) {
            std::filesystem::remove(de.path(), ec);
        }
    }

    std::set<int64_t> inos;
    for (const int64_t ino : overlay_->chunk_inos()) {
        inos.insert(ino);
    }
    for (const int64_t ino : overlay_->attr_inos()) {
        inos.insert(ino);
    }
    const bool ro = read_only();
    for (const int64_t ino : inos) {
        const auto inode = meta_.read([&](Db& db) { return meta::get_inode(db, ino); });
        if (!inode || inode->kind != InodeKind::file) {
            // Файл удалён из индекса, а overlay убрать не успели.
            if (!ro) {
                overlay_->drop_inode(ino);
            }
            continue;
        }
        auto attrs = overlay_->attrs(ino);
        if (!attrs) {
            // Строки чанков без dirty_attrs не бывает (одна транзакция); на всякий случай
            // размер берётся так, чтобы были видны все данные staging.
            int64_t size = inode->size;
            for (const int64_t idx : overlay_->indices(ino)) {
                const auto fsz = std::filesystem::file_size(overlay_->path_of({ino, idx}), ec);
                if (!ec) {
                    size = std::max(size, idx * chunk_size_ + static_cast<int64_t>(fsz));
                }
            }
            attrs = meta::DirtyAttrs{ino, size, inode->mtime_ns};
            if (!ro) {
                overlay_->set_attrs(*attrs);
            }
        }
        if (ro) {
            continue;
        }
        // Позиции за концом файла (сбой посреди truncate) не нужны.
        overlay_->drop_from(ino, (attrs->size + chunk_size_ - 1) / chunk_size_);
        // Сбой посреди truncate мог оставить в последней позиции хвост за новым размером
        // (dirty_attrs пишутся раньше обрезки staging): иначе обратное расширение оживило бы его.
        if (const int64_t last = attrs->size / chunk_size_;
            attrs->size % chunk_size_ != 0 && overlay_->chunk({ino, last})) {
            const auto path = overlay_->path_of({ino, last});
            const auto fsz = std::filesystem::file_size(path, ec);
            if (!ec && static_cast<int64_t>(fsz) > attrs->size % chunk_size_) {
                std::filesystem::resize_file(path,
                                             static_cast<uintmax_t>(attrs->size % chunk_size_), ec);
            }
        }
        const bool dirty = overlay_->has_chunks(ino);
        // Индекс не должен ссылаться на данные за размером (SPEC 6.2): truncate мог
        // упасть между overlay и индексом. Если грязные позиции остались, размер в индексе
        // тоже понижается в той же транзакции (как в truncate_locked): иначе до заливки
        // последней из них индекс и коммит в этом окне показывали бы старый больший размер с
        // обнулённым хвостом.
        const bool lower_index_size = dirty && inode->size > attrs->size;
        if (index_needs_clamp(ino, attrs->size) || !dirty || lower_index_size) {
            meta_.write([&](Db& db) {
                clamp_index(db, ino, attrs->size);
                if (!dirty) {
                    meta::AttrUpdate u;
                    u.size = attrs->size;
                    u.mtime_ns = attrs->mtime_ns;
                    // ctime = момент изменения индекса: recover --export-dirty по нему находит
                    // данные, залитые после последнего коммита (SPEC 12.3).
                    u.ctime_ns = std::max({inode->ctime_ns, attrs->mtime_ns, now_ns()});
                    meta::update_attrs(db, ino, u);
                } else if (lower_index_size) {
                    meta::AttrUpdate u;
                    u.size = attrs->size;
                    meta::update_attrs(db, ino, u);
                }
            });
        }
        // Позиция 0 инлайн-файла пропала (потерянный staging-файл), а позиции idx ≥ 1 и
        // непустой inline_data остались: позиция 0 заново создаётся из inline_data (SPEC 8.1),
        // иначе позиции idx ≥ 1 не смогли бы лечь в индекс.
        if (dirty && !overlay_->chunk({ino, 0})) {
            restore_inline_position(ino);
        }
        if (!dirty) {
            overlay_->clear_attrs(ino);
        }
    }
    // Учёт staging пересчитывается по длинам файлов на диске (SPEC 8.3).
    budget_->reset(overlay_->recount_staged());
    scan_external_tmp();
}

void Vfs::scan_external_tmp() {
    // Файлы tmp/, которые создаёт не Vfs (снимки индекса коммитера): только входят в сумму.
    // chunk-* и pack-* учтены арендой загрузок.
    uint64_t total = 0;
    std::error_code ec;
    for (const auto& de : std::filesystem::directory_iterator(paths_.tmp, ec)) {
        const std::string name = de.path().filename().string();
        if (name.starts_with(kTmpPrefix) || name.starts_with(kTmpPackPrefix)) {
            continue;
        }
        std::error_code fec;
        if (de.is_regular_file(fec)) {
            const auto size = de.file_size(fec);
            total += fec ? 0 : static_cast<uint64_t>(size);
        }
    }
    budget_->set_external(total);
}

// --- фоновые потоки ---

uint32_t Vfs::upload_workers() const {
    constexpr uint64_t kAlbumFactor = 10;  // SPEC 5: документов в альбоме
    constexpr uint64_t kMaxAlbumWorkers = 32;
    const uint64_t base = std::max<uint64_t>(opts_.upload_concurrency, 1);
    if (!store_.gathers_albums()) {
        return static_cast<uint32_t>(std::min<uint64_t>(base, UINT32_MAX));
    }
    return static_cast<uint32_t>(std::min(kAlbumFactor * base, kMaxAlbumWorkers));
}

void Vfs::start_threads() {
    // Поток на каждый возможный одновременный put_chunk (SPEC 8.1).
    const uint32_t workers = upload_workers();
    for (uint32_t i = 0; i < workers; ++i) {
        workers_.emplace_back([this] { worker_loop(); });
    }
    timer_ = std::thread([this] { timer_loop(); });
    // Дозалить overlay, оставшийся от прошлого сеанса.
    if (!read_only()) {
        for (const int64_t ino : overlay_->chunk_inos()) {
            push_job(Job{Job::Kind::seal, ino, 0, false, false, 0});
        }
    }
}

void Vfs::stop_threads() {
    {
        const std::lock_guard lock(q_mu_);
        if (stopped_) {
            return;
        }
        stopping_ = true;
        stop_flag_.store(true);
    }
    q_cv_.notify_all();
    stop_cv_.notify_all();
    overlay_->notify_all();
    budget_->notify_all();
    for (std::thread& t : workers_) {
        if (t.joinable()) {
            t.join();
        }
    }
    if (timer_.joinable()) {
        timer_.join();
    }
    const std::lock_guard lock(q_mu_);
    stopped_ = true;
    queue_.clear();
    seal_pending_.clear();
}

void Vfs::push_job(const Job& job) {
    {
        const std::lock_guard lock(q_mu_);
        if (stopping_) {
            return;
        }
        if (job.kind == Job::Kind::seal) {
            if (seal_pending_.contains(job.ino)) {
                // Уже стоит: release поднимает задачу таймера до запечатывания всего файла.
                if (!job.only_expired) {
                    for (Job& q : queue_) {
                        if (q.kind == Job::Kind::seal && q.ino == job.ino) {
                            q.only_expired = false;
                            q.explicit_close = q.explicit_close || job.explicit_close;
                        }
                    }
                }
                return;
            }
            seal_pending_.insert(job.ino);
        }
        queue_.push_back(job);
    }
    q_cv_.notify_one();
}

void Vfs::worker_loop() {
    for (;;) {
        Job job;
        {
            std::unique_lock lock(q_mu_);
            q_cv_.wait(lock, [&] { return stopping_ || !queue_.empty(); });
            if (stopping_) {
                return;
            }
            job = queue_.front();
            queue_.pop_front();
            if (job.kind == Job::Kind::seal) {
                seal_pending_.erase(job.ino);
            }
        }
        if (fatal_.load()) {
            continue;
        }
        try {
            if (job.kind == Job::Kind::seal) {
                if (job.explicit_close) {
                    seal_file(job.ino, SealMode::explicit_request);
                    packs_->close_for(job.ino);
                } else {
                    seal_file(job.ino, job.only_expired ? SealMode::timer : SealMode::release);
                }
            } else if (job.kind == Job::Kind::pack) {
                packs_->run(job.seq);
            } else {
                upload_chunk({job.ino, job.idx});
            }
        } catch (const FsError& e) {
            note_error(std::string("background seal: ") + e.what());
        } catch (const SysError& e) {
            note_error(std::string("background seal: ") + e.what());
        } catch (const std::exception& e) {
            // Неожиданная ошибка (в тестах — имитация падения процесса): дальше работать
            // нельзя, состояние на диске согласовано и подхватится следующим запуском.
            set_fatal(std::string("background upload: ") + e.what());
        }
    }
}

void Vfs::timer_loop() {
    const auto tick = std::clamp(std::min(opts_.seal_delay, opts_.pack_max_age) / 4,
                                 std::chrono::milliseconds(1), std::chrono::milliseconds(500));
    const int64_t delay_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(opts_.seal_delay).count();
    for (;;) {
        {
            std::unique_lock lock(q_mu_);
            if (stop_cv_.wait_for(lock, tick, [&] { return stopping_; })) {
                return;
            }
        }
        if (fatal_.load() || read_only()) {
            continue;
        }
        packs_->tick();
        scan_external_tmp();
        relieve_pressure(0);
        std::unordered_set<int64_t> detached;
        {
            const std::lock_guard lock(refs_mu_);
            detached = detached_;
        }
        int64_t last_ino = 0;
        for (const ChunkKey& key : overlay_->expired(now_ns() - delay_ns)) {
            if (key.ino != last_ino && !detached.contains(key.ino)) {
                push_job(Job{Job::Kind::seal, key.ino, 0, true, false, 0});
                last_ino = key.ino;
            }
        }
    }
}

bool Vfs::backoff(std::chrono::milliseconds& delay) {
    std::unique_lock lock(q_mu_);
    const bool stop = stop_cv_.wait_for(lock, delay, [&] { return stopping_; });
    delay = std::min(delay * 2, opts_.retry_max);
    return !stop;
}

void Vfs::note_error(const std::string& what) {
    spdlog::error("vfs: {}", what);
    const std::lock_guard lock(err_mu_);
    last_error_ = what;
}

void Vfs::set_fatal(const std::string& what) {
    spdlog::critical("vfs: {}; filesystem stops writing, remount required", what);
    {
        const std::lock_guard lock(err_mu_);
        last_error_ = what;
    }
    fatal_.store(true);
    overlay_->notify_all();
    budget_->notify_all();
}

// --- запечатывание (SPEC 6.5, 8.1) ---

bool Vfs::try_seal_inline(int64_t ino, const Inode& inode, int64_t size) {
    const int64_t own = inode.inline_len.value_or(0);
    const bool fits = meta_.read(
        [&](Db& db) { return meta::inline_total(db) - own + size <= params_.inline_budget; });
    if (!fits) {
        return false;  // бюджет исчерпан — файл идёт позиционным (до M3; потом в пакет)
    }
    Bytes data(static_cast<size_t>(size));
    read_staging({ino, 0}, 0, data);
    const auto attrs = overlay_->attrs(ino);
    const bool ok = meta_.write([&](Db& db) {
        if (!meta::set_inline(db, ino, data, base_gen())) {
            return false;
        }
        meta::AttrUpdate u;
        u.size = size;
        if (attrs) {
            u.mtime_ns = attrs->mtime_ns;
        }
        // ctime — момент изменения индекса (запечатывание в инлайн): SPEC 12.3, recover.
        u.ctime_ns = std::max({inode.ctime_ns, attrs ? attrs->mtime_ns : 0, now_ns()});
        meta::update_attrs(db, ino, u);
        return true;
    });
    if (ok) {
        overlay_->drop_inode(ino);
    }
    return ok;
}

std::vector<UploadTarget> Vfs::seal_file(int64_t ino, SealMode mode) {
    if (read_only() || fatal_.load()) {
        return {};
    }
    const std::unique_lock lock(stripe(ino));
    const std::vector<int64_t> idxs = overlay_->indices(ino);
    if (idxs.empty()) {
        return {};
    }
    const auto inode = meta_.read([&](Db& db) { return meta::get_inode(db, ino); });
    if (!inode || inode->kind != InodeKind::file) {
        overlay_->drop_inode(ino);
        return {};
    }
    if (!inode->parent) {
        return {};  // удалён, но открыт: данные умрут вместе с ним, заливать незачем
    }
    const int64_t size = effective_size(*inode);
    const bool explicit_request = mode == SealMode::explicit_request;
    const int64_t deadline =
        now_ns() - std::chrono::duration_cast<std::chrono::nanoseconds>(opts_.seal_delay).count();
    // Класс хранения выбирается заново при каждом запечатывании (SPEC 6.5): инлайн, пакет,
    // позиционный. Позиция 0 — единственная возможная у файла не больше pack_threshold ≤ S.
    const bool only_pos0 = idxs.size() == 1 && idxs.front() == 0;
    if (only_pos0 && size <= params_.inline_max && try_seal_inline(ino, *inode, size)) {
        return {};
    }
    if (only_pos0 && size <= params_.pack_threshold) {
        // Бюджет инлайна исчерпан или файл не влез в инлайн: в пакет.
        const auto view = overlay_->chunk({ino, 0});
        if (mode == SealMode::timer && (!view || view->last_write_ns > deadline)) {
            return {};
        }
        seal_to_pack(ino, size, explicit_request);
        return explicit_request ? overlay_->targets(ino) : std::vector<UploadTarget>{};
    }
    packs_->drop(ino);  // файл вырос из пакета: позиционный класс
    for (const int64_t idx : idxs) {
        if (mode == SealMode::timer) {
            const auto view = overlay_->chunk({ino, idx});
            if (!view || view->last_write_ns > deadline) {
                continue;
            }
        }
        enqueue_upload({ino, idx}, explicit_request);
    }
    return explicit_request ? overlay_->targets(ino) : std::vector<UploadTarget>{};
}

void Vfs::seal_to_pack(int64_t ino, int64_t size, bool explicit_request) {
    const ChunkKey key{ino, 0};
    const auto view = overlay_->chunk(key);
    if (!view) {
        return;
    }
    // Размер объекта: не больше валидной длины staging-файла (хвост из нулей не заливается).
    struct stat st {};
    {
        const Fd fd = Fd::open(overlay_->path_of(key), O_RDONLY);
        if (::fstat(fd.get(), &st) != 0) {
            throw SysError(errno, "fstat staging");
        }
    }
    const int64_t len = std::min({size, static_cast<int64_t>(st.st_size), chunk_size_});
    if (!overlay_->mark_packed(key, view->version, explicit_request)) {
        return;  // постоянная ошибка этой версии: только по явному запросу
    }
    packs_->add(ino, view->version, len);
}

void Vfs::enqueue_upload(const ChunkKey& key, bool explicit_request) {
    if (overlay_->mark_queued(key, explicit_request)) {
        push_job(Job{Job::Kind::upload, key.ino, key.idx, false, false, 0});
    }
}

void Vfs::discard_upload(int64_t chunk_id, std::optional<int64_t> msg_id) {
    if (msg_id) {
        try {
            const std::array<int64_t, 1> ids{*msg_id};
            store_.delete_messages(ids);
        } catch (const store::StoreError& e) {
            // Запись журнала остаётся: сироту уберёт монтирование (SPEC 7.2, шаг 7).
            spdlog::warn("vfs: deleting unneeded chunk {} (msg {}) failed: {}", chunk_id, *msg_id,
                         e.what());
            return;
        }
    }
    state_.remove_upload(chunk_id);
}

// --- загрузка (SPEC 8.1, шаги 1–5) ---

void Vfs::upload_chunk(const ChunkKey& key) {
    const auto s = chunk_size_;
    uint64_t inc = 0;
    int64_t ver = 0;
    int64_t valid = 0;
    int64_t chunk_id = 0;
    crypto::Hash32 mac{};
    std::filesystem::path tmp;
    auto finish = [&](std::optional<int64_t> failed) {
        if (overlay_->end_upload(key, inc, failed)) {
            push_job(Job{Job::Kind::upload, key.ino, key.idx, false, false, 0});
        }
        if (key.idx == 0 && !failed) {
            release_blocked(key.ino);  // позиция 0 ушла из работы: ждавшие позиции могут идти
        }
    };

    // Место под зашифрованную копию (SPEC 8.3): целиком и до полосы, чтобы ожидание не держало
    // ни писателей, ни применение пакетов. Берётся размером полного чанка (из него и состоит
    // резерв заливки R), после шифрования уменьшается до фактического.
    // Аренда берётся после быстрого выхода: чанка уже нет в overlay (применён или отброшен) —
    // место под копию не нужно, а ожидание его могло бы быть долгим.
    if (!overlay_->chunk(key)) {
        if (key.idx == 0) {
            release_blocked(key.ino);
        }
        return;
    }
    auto lease = budget_->acquire_internal(crypto::chunk_encrypted_size(static_cast<uint64_t>(s)),
                                           [this] { return fatal_.load() || stop_flag_.load(); });
    if (!lease) {
        return;  // остановка: чанк остаётся в overlay
    }

    // Фаза 1: снимок и шифрование.
    bool blocked = false;
    bool empty_staging = false;
    bool deduped = false;  // подходящий чанк уже есть: шифрования и заливки нет
    try {
        const std::shared_lock lock(stripe(key.ino));
        const auto view = overlay_->begin_upload(key);
        if (!view) {
            if (key.idx == 0) {
                release_blocked(key.ino);
            }
            return;
        }
        inc = view->incarnation;
        ver = view->version;
        const auto inode = meta_.read([&](Db& db) { return meta::get_inode(db, key.ino); });
        if (!inode || !inode->parent || inode->kind != InodeKind::file || read_only()) {
            finish(std::nullopt);
            return;
        }
        valid = std::min(s, effective_size(*inode) - key.idx * s);
        if (valid <= 0) {
            // Позиция за концом файла: truncate такие удаляет, сюда попасть нельзя.
            note_error("dirty chunk " + std::to_string(key.ino) + ":" + std::to_string(key.idx) +
                       " is beyond end of file");
            finish(ver);
            return;
        }
        if (key.idx != 0 && inode->inline_len && overlay_->chunk({key.ino, 0})) {
            // Пометка под разделяемой полосой: применение позиции 0 берёт её исключительно, так
            // что release_blocked после него увидит пометку.
            overlay_->set_blocked(key, inc);
            blocked = true;
        } else {
            const Fd fd = Fd::open(overlay_->path_of(key), O_RDONLY);
            // За концом staging-файла — нули (truncate обрезает его, запись расширяет), поэтому
            // валидная длина не больше его размера: хвост из нулей не заливается.
            struct stat st {};
            if (::fstat(fd.get(), &st) != 0) {
                throw SysError(errno, "fstat staging");
            }
            if (st.st_size == 0) {
                empty_staging = true;
            } else {
                valid = std::min<int64_t>(valid, st.st_size);
                mac = crypto::content_mac(keys_.mac, fd.get(), static_cast<uint64_t>(valid));
                deduped =
                    meta_.read([&](Db& db) { return meta::find_chunk_by_mac(db, mac, valid); })
                        .has_value();
                if (!deduped) {
                    chunk_id = meta_.write([](Db& db) { return meta::alloc_chunk_id(db); });
                    tmp = paths_.tmp / (std::string(kTmpPrefix) + std::to_string(chunk_id));
                    crypto::encrypt_chunk_file(keys_.data, params_.fs_uuid,
                                               static_cast<uint64_t>(chunk_id), fd.get(),
                                               static_cast<uint64_t>(valid), tmp);
                }
            }
        }
    } catch (const SysError& e) {
        note_error("sealing chunk " + std::to_string(key.ino) + ":" + std::to_string(key.idx) +
                   ": " + e.what());
        finish(ver);
        return;
    }
    if (blocked) {
        finish(std::nullopt);
        return;
    }
    if (empty_staging) {
        resolve_empty_position(key, inc);
        finish(std::nullopt);
        return;
    }
    int64_t enc_size = 0;
    store::ChunkRef ref;
    if (deduped) {
        if (opts_.upload_probe) {
            opts_.upload_probe("dedup_hinted");
        }
        lease->release();  // шифрованной копии в tmp/ нет: место под неё не нужно (SPEC 8.3)
    } else {
        enc_size = static_cast<int64_t>(crypto::chunk_encrypted_size(static_cast<uint64_t>(valid)));
        lease->shrink_to(static_cast<uint64_t>(enc_size));

        // Фаза 2: заливка без блокировок.
        auto remove_tmp = [&] {
            std::error_code ec;
            std::filesystem::remove(tmp, ec);
        };
        state_.upload_started(chunk_id, now_ns());
        auto delay = opts_.retry_initial;
        bool attempted = false;
        for (;;) {
            const auto view = overlay_->chunk(key);
            if (!view || view->incarnation != inc) {
                // Чанк больше не нужен (unlink, truncate, инлайн): не заливать.
                remove_tmp();
                if (!attempted) {
                    state_.remove_upload(chunk_id);
                }
                finish(std::nullopt);
                return;
            }
            try {
                ref = store_.put_chunk(tmp);
                break;
            } catch (const store::StoreError& e) {
                if (e.kind() == store::StoreErrorKind::transient) {
                    attempted = true;  // таймаут мог и залить: журнал оставляем монтированию
                    note_error("upload of chunk " + std::to_string(chunk_id) +
                               " failed, will retry: " + e.what());
                    if (!backoff(delay)) {
                        remove_tmp();  // остановка: журнал без msg_id забудет монтирование
                        finish(std::nullopt);
                        return;
                    }
                    continue;
                }
                note_error("upload of chunk " + std::to_string(chunk_id) + " failed: " + e.what());
                remove_tmp();
                state_.remove_upload(chunk_id);
                finish(ver);
                return;
            }
        }
        state_.upload_finished(chunk_id, ref.msg_id);
        remove_tmp();
        lease->release();
    }

    // Фаза 3: применить, если чанк ещё нужен.
    bool applied = false;
    bool inline_conflict = false;
    bool dedup_lost = false;
    {
        const std::unique_lock lock(stripe(key.ino));
        const auto view = overlay_->chunk(key);
        if (view && view->incarnation == inc) {
            const std::vector<int64_t> dirty = overlay_->indices(key.ino);
            const bool last_dirty =
                view->version == ver && dirty.size() == 1 && dirty.front() == key.idx;
            const auto attrs = overlay_->attrs(key.ino);
            const auto clean = meta_.write([&](Db& db) -> std::optional<bool> {
                const auto cur = meta::get_inode(db, key.ino);
                if (!cur || cur->kind != InodeKind::file || !cur->parent) {
                    return std::nullopt;
                }
                const int64_t size = attrs ? attrs->size : cur->size;
                const int64_t len = std::min({valid, size - key.idx * s, view->cut_len});
                if (len <= 0) {
                    return std::nullopt;
                }
                // Поиск и refcount++ (put_extent ниже) — одна транзакция под блокировкой записи
                // MetaStore: уборка не удалит чанк, на который только что сослались. Искать
                // нужно до любых изменений: nullopt транзакцию не откатывает.
                int64_t target = chunk_id;
                if (deduped) {
                    const auto found = meta::find_chunk_by_mac(db, mac, valid);
                    if (!found) {
                        dedup_lost = true;  // умер или убран после подсказки: заливать нечего
                        return std::nullopt;
                    }
                    target = found->chunk_id;
                }
                if (cur->inline_len) {
                    // Инлайн → позиционный. Позиция 0 заменяет инлайн-данные собой; пустой
                    // инлайн (после truncate) терять нечего. Непустой инлайн при экстенте
                    // idx ≥ 1 — нарушение порядка (такие позиции ждут позицию 0, см. фазу 1).
                    if (key.idx != 0 && *cur->inline_len > 0) {
                        inline_conflict = true;
                        return std::nullopt;
                    }
                    meta::clear_inline(db, key.ino);
                }
                if (!deduped) {
                    meta::NewChunk nc;
                    nc.chunk_id = chunk_id;
                    nc.kind = meta::ChunkKind::positional;
                    nc.msg_id = ref.msg_id;
                    if (!ref.remote_file_id.empty()) {
                        nc.remote_file_id = ref.remote_file_id;
                    }
                    nc.plain_size = valid;
                    nc.enc_size = enc_size;
                    nc.content_mac.assign(mac.begin(), mac.end());
                    meta::insert_chunk(db, nc, base_gen());
                }
                meta::put_extent(db, meta::Extent{key.ino, key.idx, target, 0, len}, base_gen());
                meta::AttrUpdate u;
                const bool clean_now = last_dirty && attrs.has_value();
                if (clean_now) {
                    u.size = attrs->size;
                    u.mtime_ns = attrs->mtime_ns;
                } else if (key.idx * s + len > cur->size) {
                    // Индекс сам по себе не должен держать экстент за inodes.size: иначе
                    // после обратного расширения без overlay ожил бы хвост (SPEC 6.2).
                    u.size = key.idx * s + len;
                }
                // ctime — момент привязки чанка в индексе (в том числе дедуплицированного): по нему
                // recover --export-dirty находит залитое после коммита (SPEC 12.3).
                u.ctime_ns = std::max({cur->ctime_ns, clean_now ? attrs->mtime_ns : 0, now_ns()});
                meta::update_attrs(db, key.ino, u);
                return clean_now;
            });
            if (clean) {
                if (deduped) {
                    // До finish_upload: он будит fsync, и тот должен видеть счётчик уже
                    // увеличенным.
                    deduped_.fetch_add(1);
                }
                overlay_->finish_upload(key, inc, ver, *clean, deduped ? 0 : chunk_id);
                applied = true;
            }
        }
    }
    if (!applied) {
        if (!deduped) {
            discard_upload(chunk_id, ref.msg_id);
        }
        if (dedup_lost) {
            // Подходящий чанк умер или убран: позиция заливается ещё раз (сейчас uploading).
            overlay_->mark_queued(key, false);
        }
        if (inline_conflict) {
            note_error("chunk " + std::to_string(key.ino) + ":" + std::to_string(key.idx) +
                       " cannot be applied: the inline data of position 0 is still in the index");
        }
        finish(inline_conflict ? std::optional<int64_t>(ver) : std::nullopt);
        return;
    }
    if (deduped) {
        finish(std::nullopt);
        return;  // залитых чанков не прибавилось: коммит по числу чанков не нужен
    }
    uploaded_.fetch_add(1);
    finish(std::nullopt);
    committer_.chunk_uploaded();
}

void Vfs::release_blocked(int64_t ino) {
    for (const ChunkKey& key : overlay_->take_blocked(ino)) {
        enqueue_upload(key, false);
    }
}

void Vfs::resolve_empty_position(const ChunkKey& key, uint64_t incarnation) {
    const std::unique_lock lock(stripe(key.ino));
    const auto view = overlay_->chunk(key);
    if (!view || view->incarnation != incarnation) {
        return;
    }
    // Между фазой 1 и этой полосой запись могла дописать данные.
    std::error_code ec;
    const auto fsz = std::filesystem::file_size(overlay_->path_of(key), ec);
    if (!ec && fsz != 0) {
        overlay_->mark_queued(key, false);  // заливается ещё раз (позиция сейчас uploading)
        return;
    }
    // Позиция 0 инлайн-файла пуста, а позиции idx ≥ 1 и непустой inline_data остались: убрать
    // позицию 0 нельзя, непустой инлайн остался бы в индексе при экстентах idx ≥ 1 (SPEC 8.1).
    // Позиция заново наполняется из inline_data и заливается ещё раз (сейчас она uploading).
    if (key.idx == 0 && restore_inline_position(key.ino)) {
        overlay_->mark_queued(key, false);
        return;
    }
    const auto attrs = overlay_->attrs(key.ino);
    const std::vector<int64_t> dirty = overlay_->indices(key.ino);
    const bool last_dirty = dirty.size() == 1 && dirty.front() == key.idx;
    const auto clean = meta_.write([&](Db& db) -> std::optional<bool> {
        const auto cur = meta::get_inode(db, key.ino);
        if (!cur || cur->kind != InodeKind::file || !cur->parent) {
            return std::nullopt;
        }
        // Пустой инлайн (файл усекли до нуля) нужно убрать: иначе экстенты следующих позиций
        // нельзя записать (SPEC 6.2). Непустой инлайн остаётся — это откат к коммиту.
        if (key.idx == 0 && cur->inline_len && *cur->inline_len == 0) {
            meta::clear_inline(db, key.ino);
        }
        if (last_dirty && attrs) {
            meta::AttrUpdate u;
            u.size = attrs->size;
            u.mtime_ns = attrs->mtime_ns;
            u.ctime_ns = std::max({cur->ctime_ns, attrs->mtime_ns, now_ns()});
            meta::update_attrs(db, key.ino, u);
            return true;
        }
        return false;
    });
    if (!clean) {
        return;  // inode удалён: строку уберёт unlink или старт
    }
    spdlog::debug("vfs: empty staging file of inode {} position {} is a hole", key.ino, key.idx);
    overlay_->drop_position(key, *clean, true);
}

void Vfs::wait_targets(const std::vector<UploadTarget>& targets) {
    if (targets.empty()) {
        return;
    }
    bool timed_out = false;
    const bool ok = overlay_->wait(
        targets, [this] { return fatal_.load() || stop_flag_.load(); },
        std::chrono::steady_clock::now() + opts_.sync_timeout, timed_out);
    if (timed_out) {
        std::string why;
        {
            const std::lock_guard lock(err_mu_);
            why = last_error_;
        }
        throw FsError(EIO, "timed out waiting for upload (sync_timeout)" +
                               (why.empty() ? std::string{} : ": " + why));
    }
    if (!ok) {
        std::string why;
        {
            const std::lock_guard lock(err_mu_);
            why = last_error_;
        }
        throw FsError(EIO, "upload failed: " + why);
    }
}

// --- завершение ---

void Vfs::flush_all() {
    check_alive();
    if (opts_.read_only) {
        return;
    }
    if (committer_.read_only()) {
        throw FsError(EIO, "file system is read-only after a commit conflict: cannot commit");
    }
    std::vector<UploadTarget> all;
    for (const int64_t ino : overlay_->chunk_inos()) {
        auto t = seal_file(ino, SealMode::explicit_request);
        all.insert(all.end(), t.begin(), t.end());
    }
    // Объекты запечатанных файлов лежат в открытом пакете: закрыть его (SPEC 6.5).
    packs_->close_open(true);
    wait_targets(all);
}

void Vfs::commit_or_throw() {
    if (opts_.read_only) {
        return;  // --ro: коммитить нечего
    }
    // SPEC 7.3: после CAS-конфликта данные закоммитить нельзя, молчаливый успех был бы ложью.
    if (committer_.read_only()) {
        throw FsError(EIO, "file system is read-only after a commit conflict: cannot commit");
    }
    try {
        committer_.commit_now();
    } catch (const meta::CommitError& e) {
        throw FsError(EIO, std::string("commit failed: ") + e.what());
    }
}

void Vfs::sync() {
    flush_all();
    commit_or_throw();
}

void Vfs::shutdown() {
    std::exception_ptr err;
    bool running = false;
    {
        const std::lock_guard lock(q_mu_);
        running = !stopping_;
    }
    if (running && !fatal_.load()) {
        try {
            flush_all();
        } catch (...) {
            err = std::current_exception();
        }
    }
    stop_threads();
    if (err) {
        std::rethrow_exception(err);
    }
}

}  // namespace telega::vfs
