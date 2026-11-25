#include "sim/sim.h"

#include <algorithm>
#include <cmath>
#include <deque>
#include <limits>
#include <queue>
#include <stdexcept>

#include "common/bench_common.h"

namespace telega::bench::sim {

namespace {

constexpr uint32_t kNone = std::numeric_limits<uint32_t>::max();
constexpr double kInf = std::numeric_limits<double>::infinity();
constexpr double kEps = 1e-9;

enum class Cls : uint8_t { None, Inline, Pack, Pos };
enum class Kind : uint8_t { Chunk, Pack, BlobDelta, BlobSnapshot, Pin, Unpin };
enum class Ev : uint8_t { Writer, UploadDone, Dispatch, PackAge, Seal, CommitTimer, CommitStep };
enum class PackState : uint8_t { Open, Uploading, Done, Deleted };
enum class Reason : uint8_t { Size, Age, Fsync, Unmount };

struct File {
    int64_t size = 0;     // размер последней записи
    int64_t obj_len = 0;  // сколько занято в хранилище (размер при запечатывании)
    uint32_t ver = 0;     // растёт с каждой записью и удалением
    uint32_t sealed_ver = 0;
    uint32_t pack = kNone;
    uint32_t nchunks = 0;
    uint32_t streamed = 0;  // чанков уже в очереди (потоковая заливка большого файла)
    uint32_t last_item = kNone;  // последний чанк позиционного файла (барьер fsync)
    Cls cls = Cls::None;
    bool applied = false;  // объект в пакете учтён в live_bytes
};

struct Obj {
    uint32_t id;
    uint32_t ver;
    int64_t len;
};

struct Pack {
    int64_t plain = 0;
    int64_t live = 0;
    int64_t refcount = 0;
    uint32_t item = kNone;
    PackState state = PackState::Open;
    std::vector<Obj> objs;
};

struct Item {
    Kind kind = Kind::Chunk;
    int64_t bytes = 0;
    uint32_t pack = kNone;
    bool done = false;
};

struct Event {
    double t;
    uint64_t seq;
    Ev type;
    uint64_t a;
    uint64_t b;
    bool operator>(const Event& o) const { return t != o.t ? t > o.t : seq > o.seq; }
};

// NOLINTNEXTLINE(clang-analyzer-optin.performance.Padding): состояние модели, не горячий путь
class Sim {
  public:
    Sim(const std::vector<Op>& ops, const Params& p) : ops_(ops), p_(p), rng_(p.seed) {
        uint32_t max_id = 0;
        for (const Op& op : ops) {
            max_id = std::max(max_id, op.id);
        }
        files_.resize(static_cast<size_t>(max_id) + 1);
        tok_a_ = p.send_burst;
        tok_b_ = p.send_bytes_burst;
        if (p.pack_threshold > p.chunk_size) {
            throw std::runtime_error("pack_threshold must be <= chunk_size");
        }
    }

    Metrics run() {
        writer_next_op();
        while (!heap_.empty()) {
            const Event e = heap_.top();
            heap_.pop();
            now_ = e.t;
            handle(e);
            if (finalizing_ && finished()) {
                break;
            }
        }
        if (!(finalizing_ && finished())) {
            throw std::runtime_error("simulation stalled before the final commit");
        }
        m_.t_done_s = now_;
        collect();
        return m_;
    }

  private:
    // ---- события ----
    void push(double t, Ev type, uint64_t a = 0, uint64_t b = 0) {
        heap_.push(Event{t, ++seq_, type, a, b});
    }

    void handle(const Event& e) {
        switch (e.type) {
            case Ev::Writer:
                writer_piece_done();
                break;
            case Ev::UploadDone:
                on_item_done(static_cast<uint32_t>(e.a));
                break;
            case Ev::Dispatch:
                if (e.a == dispatch_gen_) {
                    try_dispatch();
                }
                break;
            case Ev::PackAge:
                if (open_pack_ == e.a && packs_[open_pack_].state == PackState::Open) {
                    close_open_pack(Reason::Age);
                }
                break;
            case Ev::Seal: {
                const File& f = files_[e.a];
                if (f.ver == e.b && f.sealed_ver != f.ver) {
                    seal(static_cast<uint32_t>(e.a));
                }
                break;
            }
            case Ev::CommitTimer:
                if (e.a == timer_gen_) {
                    timer_scheduled_ = false;
                    maybe_commit();
                }
                break;
            case Ev::CommitStep:
                enqueue_item(commit_blob_kind_, commit_blob_bytes_, kNone, true);
                try_dispatch();
                break;
        }
    }

    [[nodiscard]] bool finished() const {
        return pending_data_ == 0 && !commit_busy_ && !dirty_ && open_pack_ == kNone &&
               hi_.empty() && lo_.empty() && !waiting_;
    }

    // ---- писатель ----
    double unit() { return static_cast<double>(rng_.next() >> 11) * (1.0 / 9007199254740992.0); }
    double jitter() { return 0.7 + 0.6 * unit(); }

    // Цепочка писатель -> коммит -> писатель идёт через события, глубина ограничена.
    // NOLINTNEXTLINE(misc-no-recursion)
    void writer_next_op() {
        if (pos_ >= ops_.size()) {
            writes_done();
            return;
        }
        const Op& op = ops_[pos_];
        double ready = now_;
        if (pos_ == 0) {
            cur_step_ = op.step;
        } else if (op.step != cur_step_) {
            cur_step_ = op.step;
            ready += p_.step_gap_s;
        }
        sub_ = 0;
        if (op.kind == 'd') {
            nsub_ = 1;
            piece_cost_ = jitter() / p_.unlink_per_s;
        } else {
            nsub_ = op.size > p_.chunk_size
                        ? static_cast<uint32_t>((op.size + p_.chunk_size - 1) / p_.chunk_size)
                        : 1;
            const double total =
                jitter() *
                (1.0 / p_.files_per_s + static_cast<double>(op.size) / (p_.write_mib_per_s * kMiB));
            piece_cost_ = total / nsub_;
            files_[op.id].streamed = 0;
        }
        piece_ready_ = ready;
        schedule_piece();
    }

    void schedule_piece() {
        if (staged_ >= p_.staging_limit) {
            staging_blocked_ = true;  // продолжит on_item_done, когда место освободится
            return;
        }
        push(std::max(now_, piece_ready_) + piece_cost_, Ev::Writer);
    }

    void writer_piece_done() {
        const Op& op = ops_[pos_];
        if (op.kind == 'd') {
            apply_delete(op.id);
            ++pos_;
            writer_next_op();
            return;
        }
        if (sub_ + 1 < nsub_) {
            // Писатель ушёл дальше по файлу: полный чанк идёт в очередь.
            enqueue_item(Kind::Chunk, p_.chunk_size, kNone, false);
            staged_ += p_.chunk_size;
            ++files_[op.id].streamed;
            ++sub_;
            piece_ready_ = now_;
            schedule_piece();
            try_dispatch();
            return;
        }
        const bool sync = p_.fsync_prob > 0 && unit() < p_.fsync_prob;
        apply_write(op, sync);
        ++pos_;
        if (sync) {
            start_fsync(op.id);
            return;
        }
        writer_next_op();
    }

    // NOLINTNEXTLINE(misc-no-recursion)
    void writes_done() {
        m_.t_writer_end_s = now_;
        finalizing_ = true;  // размонтирование: закрыть пакет, залить всё, финальный коммит
        if (open_pack_ != kNone) {
            close_open_pack(Reason::Unmount);
        }
        maybe_commit();
    }

    void apply_write(const Op& op, bool sync) {
        File& f = files_[op.id];
        ++m_.writes;
        ++f.ver;
        f.size = op.size;
        dirty_ = true;
        if (p_.seal_lag && !sync) {
            push(now_ + p_.seal_delay_s, Ev::Seal, op.id, f.ver);
        } else {
            seal(op.id);
        }
    }

    void apply_delete(uint32_t id) {
        File& f = files_[id];
        ++m_.deletes;
        ++f.ver;  // отменяет отложенное запечатывание
        f.sealed_ver = f.ver;
        release_old(f);
        f.size = 0;
        ++rows_del_;
        dirty_ = true;
        maybe_commit();
    }

    // ---- запечатывание и классы ----
    void release_old(File& f) {
        switch (f.cls) {
            case Cls::Inline:
                inline_bytes_ -= f.obj_len;
                --n_inline_;
                break;
            case Cls::Pack: {
                --n_pack_;
                Pack& pk = packs_[f.pack];
                if (pk.state == PackState::Done && f.applied) {
                    pk.live -= f.obj_len;
                    if (--pk.refcount == 0) {
                        delete_pack(pk);
                    }
                }
                break;
            }
            case Cls::Pos:
                --n_pos_;
                m_.deleted_msgs += f.nchunks;
                break;
            case Cls::None:
                break;
        }
        f.cls = Cls::None;
        f.applied = false;
        f.pack = kNone;
        f.nchunks = 0;
        f.last_item = kNone;
        f.obj_len = 0;
    }

    void delete_pack(Pack& pk) {
        pk.state = PackState::Deleted;
        ++m_.deleted_msgs;
    }

    void seal(uint32_t id) {
        File& f = files_[id];
        f.sealed_ver = f.ver;
        release_old(f);
        ++rows_new_;
        dirty_ = true;
        const int64_t size = f.size;
        if (size <= p_.inline_max && inline_bytes_ + size <= p_.inline_budget) {
            f.cls = Cls::Inline;
            f.obj_len = size;
            inline_bytes_ += size;
            rows_inline_bytes_ += size;
            ++n_inline_;
        } else if (size <= p_.pack_threshold) {
            append_to_pack(id, f, size);
        } else {
            f.cls = Cls::Pos;
            ++n_pos_;
            f.obj_len = size;
            const auto n = static_cast<uint32_t>((size + p_.chunk_size - 1) / p_.chunk_size);
            f.nchunks = n;
            for (uint32_t i = f.streamed; i < n; ++i) {
                const int64_t len = i + 1 < n ? p_.chunk_size : size - int64_t{i} * p_.chunk_size;
                f.last_item = enqueue_item(Kind::Chunk, len, kNone, false);
                staged_ += len;
            }
            f.streamed = 0;
            try_dispatch();
        }
        maybe_commit();
    }

    void append_to_pack(uint32_t id, File& f, int64_t size) {
        if (open_pack_ == kNone) {
            open_pack_ = static_cast<uint32_t>(packs_.size());
            packs_.emplace_back();
            push(now_ + p_.pack_max_age_s, Ev::PackAge, open_pack_);
        }
        Pack& pk = packs_[open_pack_];
        pk.plain += size;
        pk.objs.push_back(Obj{id, f.ver, size});
        staged_ += size;
        f.cls = Cls::Pack;
        f.pack = open_pack_;
        f.obj_len = size;
        f.applied = false;
        ++n_pack_;
        if (pk.plain >= p_.pack_target) {
            close_open_pack(Reason::Size);
        }
    }

    void close_open_pack(Reason why) {
        const uint32_t id = open_pack_;
        Pack& pk = packs_[id];
        pk.state = PackState::Uploading;
        open_pack_ = kNone;
        for (const Obj& o : pk.objs) {
            const File& f = files_[o.id];
            if (f.ver != o.ver || f.cls != Cls::Pack || f.pack != id) {
                m_.dead_at_close_bytes += o.len;
            }
        }
        ++m_.packs;
        switch (why) {
            case Reason::Size:
                ++m_.packs_by_size;
                break;
            case Reason::Age:
                ++m_.packs_by_age;
                break;
            case Reason::Fsync:
                ++m_.packs_by_fsync;
                break;
            case Reason::Unmount:
                ++m_.packs_by_unmount;
                break;
        }
        pk.item = enqueue_item(Kind::Pack, pk.plain, id, false);
        try_dispatch();
    }

    // ---- fsync ----
    void start_fsync(uint32_t id) {
        ++m_.fsyncs;
        const File& f = files_[id];
        uint32_t barrier = kNone;
        if (f.cls == Cls::Pack) {
            const Pack& pk = packs_[f.pack];
            if (pk.state == PackState::Open) {
                close_open_pack(Reason::Fsync);
            }
            if (pk.state == PackState::Uploading) {
                barrier = pk.item;
            }
        } else if (f.cls == Cls::Pos && f.last_item != kNone && !items_[f.last_item].done) {
            barrier = f.last_item;
        }
        waiting_ = true;
        waiter_item_ = barrier;
        waiter_ready_ = barrier == kNone ? now_ : kInf;
        dirty_ = true;
        maybe_commit();
    }

    // ---- очередь отправки и лимиты ----
    uint32_t enqueue_item(Kind kind, int64_t bytes, uint32_t pack, bool high) {
        const auto id = static_cast<uint32_t>(items_.size());
        items_.push_back(Item{kind, bytes, pack, false});
        (high ? hi_ : lo_).push_back(id);
        if (kind == Kind::Chunk || kind == Kind::Pack) {
            ++pending_data_;
        }
        return id;
    }

    void refill() {
        const double dt = now_ - last_refill_;
        last_refill_ = now_;
        tok_a_ = std::min(p_.send_burst, tok_a_ + dt * p_.send_rate_per_min / 60.0);
        tok_b_ = std::min(p_.send_bytes_burst, tok_b_ + dt * p_.send_bytes_per_min / 60.0);
    }

    void try_dispatch() {
        for (;;) {
            std::deque<uint32_t>& q = !hi_.empty() ? hi_ : lo_;
            if (q.empty()) {
                return;
            }
            const uint32_t id = q.front();
            const Item& it = items_[id];
            const bool doc = it.kind != Kind::Pin && it.kind != Kind::Unpin;
            if (doc && active_ >= p_.upload_concurrency) {
                return;  // продолжит on_item_done
            }
            if (p_.limits) {
                refill();
                const double need_b =
                    doc ? std::min<double>(static_cast<double>(it.bytes), p_.send_bytes_burst)
                        : 0.0;
                double wait = 0;
                if (tok_a_ < 1.0 - kEps) {
                    wait = std::max(wait, (1.0 - tok_a_) / (p_.send_rate_per_min / 60.0));
                }
                if (tok_b_ < need_b - kEps) {
                    wait = std::max(wait, (need_b - tok_b_) / (p_.send_bytes_per_min / 60.0));
                }
                if (wait > 0) {
                    ++dispatch_gen_;
                    push(now_ + wait + kEps, Ev::Dispatch, dispatch_gen_);
                    return;
                }
                tok_a_ -= 1.0;
                if (doc) {
                    tok_b_ -= static_cast<double>(it.bytes);
                }
            }
            q.pop_front();
            start_item(id, doc);
        }
    }

    void start_item(uint32_t id, bool doc) {
        const Item& it = items_[id];
        double dur = p_.action_latency_s;
        if (doc) {
            const double rate = active_ == 0 ? p_.rate_alone : p_.rate_shared;
            dur = p_.doc_latency_s + static_cast<double>(it.bytes) / rate;
            ++active_;
        }
        switch (it.kind) {
            case Kind::Chunk:
                ++m_.docs_chunk;
                m_.bytes_chunks += it.bytes;
                break;
            case Kind::Pack:
                ++m_.docs_pack;
                m_.bytes_packs += it.bytes;
                break;
            case Kind::BlobDelta:
                ++m_.docs_blob_delta;
                m_.bytes_blobs += it.bytes;
                break;
            case Kind::BlobSnapshot:
                ++m_.docs_blob_snapshot;
                m_.bytes_blobs += it.bytes;
                break;
            case Kind::Pin:
                ++m_.pins;
                break;
            case Kind::Unpin:
                ++m_.unpins;
                break;
        }
        push(now_ + dur, Ev::UploadDone, id);
    }

    void on_item_done(uint32_t id) {
        Item& it = items_[id];
        it.done = true;
        switch (it.kind) {
            case Kind::Chunk:
                --active_;
                --pending_data_;
                staged_ -= it.bytes;
                dirty_ = true;
                ++chunks_since_commit_;
                break;
            case Kind::Pack:
                --active_;
                --pending_data_;
                staged_ -= it.bytes;
                apply_pack(it.pack);
                dirty_ = true;
                ++chunks_since_commit_;
                break;
            case Kind::BlobDelta:
            case Kind::BlobSnapshot:
                --active_;
                enqueue_item(Kind::Pin, 0, kNone, true);
                break;
            case Kind::Pin:
                enqueue_item(Kind::Unpin, 0, kNone, true);
                break;
            case Kind::Unpin:
                commit_finished();
                break;
        }
        if (waiting_ && waiter_item_ == id) {
            waiter_ready_ = now_;
        }
        if (staging_blocked_ && staged_ < p_.staging_limit) {
            staging_blocked_ = false;
            piece_ready_ = now_;
            schedule_piece();
        }
        try_dispatch();
        maybe_commit();
    }

    void apply_pack(uint32_t id) {
        Pack& pk = packs_[id];
        pk.state = PackState::Done;
        for (const Obj& o : pk.objs) {
            File& f = files_[o.id];
            // Объект, чей файл изменился за время заливки, не применяется.
            if (f.ver == o.ver && f.cls == Cls::Pack && f.pack == id && !f.applied) {
                f.applied = true;
                pk.live += o.len;
                ++pk.refcount;
            }
        }
        pk.objs.clear();
        pk.objs.shrink_to_fit();
        if (pk.refcount == 0) {
            ++m_.packs_all_dead;
            delete_pack(pk);
        }
    }

    // ---- коммит ----
    // NOLINTNEXTLINE(misc-no-recursion)
    void maybe_commit() {
        if (commit_busy_) {
            return;
        }
        const bool waiter_ready = waiting_ && waiter_ready_ <= now_;
        if (!dirty_) {
            if (waiter_ready) {
                waiting_ = false;  // индекс чистый: всё уже закоммичено
                writer_next_op();
            }
            return;
        }
        const bool flush_ready = finalizing_ && pending_data_ == 0 && open_pack_ == kNone;
        double due = last_commit_start_ + p_.commit_interval_s;
        if (p_.commit_defer_factor > 0 && pending_data_ > 0) {
            due = std::max(due, last_commit_start_ + p_.commit_defer_factor * p_.commit_interval_s);
        }
        if (waiter_ready || flush_ready || chunks_since_commit_ >= p_.commit_every_chunks ||
            now_ >= due) {
            start_commit();
            return;
        }
        if (!timer_scheduled_) {
            timer_scheduled_ = true;
            ++timer_gen_;
            push(due, Ev::CommitTimer, timer_gen_);
        }
    }

    void start_commit() {
        commit_busy_ = true;
        timer_scheduled_ = false;  // таймер прошлого коммита (в том числе отсрочки) недействителен
        ++timer_gen_;
        commit_start_ = now_;
        last_commit_start_ = now_;
        dirty_ = false;
        chunks_since_commit_ = 0;
        ++m_.commits;
        const double delta = p_.delta_base + static_cast<double>(rows_new_) * p_.delta_per_row +
                             static_cast<double>(rows_del_) * p_.delta_per_delete +
                             static_cast<double>(rows_inline_bytes_) * p_.inline_compress;
        rows_new_ = 0;
        rows_del_ = 0;
        rows_inline_bytes_ = 0;
        if (chain_len_ >= p_.max_chain ||
            chain_bytes_ + delta > p_.checkpoint_ratio * snap_bytes_) {
            snap_bytes_ =
                p_.delta_base +
                static_cast<double>(n_inline_ + n_pack_ + n_pos_) * p_.snapshot_per_inode +
                static_cast<double>(inline_bytes_) * p_.inline_compress;
            chain_len_ = 0;
            chain_bytes_ = 0;
            commit_blob_kind_ = Kind::BlobSnapshot;
            commit_blob_bytes_ = static_cast<int64_t>(snap_bytes_);
        } else {
            ++chain_len_;
            chain_bytes_ += delta;
            commit_blob_kind_ = Kind::BlobDelta;
            commit_blob_bytes_ = static_cast<int64_t>(delta);
        }
        push(now_ + p_.cas_latency_s, Ev::CommitStep);
    }

    void commit_finished() {
        commit_busy_ = false;
        if (waiting_ && waiter_ready_ <= commit_start_) {
            waiting_ = false;
            writer_next_op();
        }
    }

    // ---- итоги ----
    void collect() {
        m_.files_inline = n_inline_;
        m_.files_pack = n_pack_;
        m_.files_pos = n_pos_;
        m_.inline_bytes = inline_bytes_;
        for (const Pack& pk : packs_) {
            if (pk.state != PackState::Done) {
                continue;
            }
            ++m_.packs_alive;
            m_.pack_plain_alive += pk.plain;
            m_.pack_live_alive += pk.live;
            if (static_cast<double>(pk.live) < 0.25 * static_cast<double>(pk.plain)) {
                ++m_.packs_compactable;
            }
        }
    }

    const std::vector<Op>& ops_;
    const Params& p_;
    Rng rng_;
    Metrics m_;

    std::priority_queue<Event, std::vector<Event>, std::greater<>> heap_;
    uint64_t seq_ = 0;
    double now_ = 0;

    std::vector<File> files_;
    std::vector<Pack> packs_;
    std::vector<Item> items_;
    uint32_t open_pack_ = kNone;
    int64_t inline_bytes_ = 0;
    int64_t n_inline_ = 0;
    int64_t n_pack_ = 0;
    int64_t n_pos_ = 0;
    int64_t staged_ = 0;

    // писатель
    size_t pos_ = 0;
    uint32_t cur_step_ = 0;
    uint32_t sub_ = 0;
    uint32_t nsub_ = 1;
    double piece_cost_ = 0;
    double piece_ready_ = 0;
    bool staging_blocked_ = false;
    bool finalizing_ = false;
    bool waiting_ = false;
    uint32_t waiter_item_ = kNone;
    double waiter_ready_ = kInf;

    // очередь отправки
    std::deque<uint32_t> hi_;
    std::deque<uint32_t> lo_;
    int active_ = 0;
    int64_t pending_data_ = 0;
    double tok_a_ = 0;
    double tok_b_ = 0;
    double last_refill_ = 0;
    uint64_t dispatch_gen_ = 0;

    // коммит
    bool dirty_ = false;
    bool commit_busy_ = false;
    double commit_start_ = 0;
    double last_commit_start_ = 0;
    int chunks_since_commit_ = 0;
    int64_t rows_new_ = 0;
    int64_t rows_del_ = 0;
    int64_t rows_inline_bytes_ = 0;
    int chain_len_ = 0;
    double chain_bytes_ = 0;
    double snap_bytes_ = 2048;
    Kind commit_blob_kind_ = Kind::BlobDelta;
    int64_t commit_blob_bytes_ = 0;
    bool timer_scheduled_ = false;
    uint64_t timer_gen_ = 0;
};

}  // namespace

std::vector<std::pair<std::string, double>> Metrics::columns() const {
    return {
        {"docs_total", static_cast<double>(docs_total())},
        {"actions_total", static_cast<double>(actions_total())},
        {"docs_chunk", static_cast<double>(docs_chunk)},
        {"docs_pack", static_cast<double>(docs_pack)},
        {"docs_blob_delta", static_cast<double>(docs_blob_delta)},
        {"docs_blob_snapshot", static_cast<double>(docs_blob_snapshot)},
        {"pins", static_cast<double>(pins)},
        {"unpins", static_cast<double>(unpins)},
        {"deleted_msgs", static_cast<double>(deleted_msgs)},
        {"commits", static_cast<double>(commits)},
        {"files_inline", static_cast<double>(files_inline)},
        {"files_pack", static_cast<double>(files_pack)},
        {"files_pos", static_cast<double>(files_pos)},
        {"inline_bytes", static_cast<double>(inline_bytes)},
        {"writes", static_cast<double>(writes)},
        {"deletes", static_cast<double>(deletes)},
        {"fsyncs", static_cast<double>(fsyncs)},
        {"packs", static_cast<double>(packs)},
        {"packs_by_size", static_cast<double>(packs_by_size)},
        {"packs_by_age", static_cast<double>(packs_by_age)},
        {"packs_by_fsync", static_cast<double>(packs_by_fsync)},
        {"packs_by_unmount", static_cast<double>(packs_by_unmount)},
        {"packs_all_dead", static_cast<double>(packs_all_dead)},
        {"packs_alive", static_cast<double>(packs_alive)},
        {"pack_plain_alive", static_cast<double>(pack_plain_alive)},
        {"pack_live_alive", static_cast<double>(pack_live_alive)},
        {"packs_compactable", static_cast<double>(packs_compactable)},
        {"dead_at_close_bytes", static_cast<double>(dead_at_close_bytes)},
        {"dead_share", dead_share()},
        {"undersized_share", undersized_share()},
        {"undersized_share_with_unmount", undersized_share_with_unmount()},
        {"bytes_chunks", static_cast<double>(bytes_chunks)},
        {"bytes_packs", static_cast<double>(bytes_packs)},
        {"bytes_blobs", static_cast<double>(bytes_blobs)},
        {"t_writer_end_s", t_writer_end_s},
        {"t_done_s", t_done_s},
    };
}

Metrics simulate(const std::vector<Op>& ops, const Params& params) {
    Sim sim(ops, params);
    return sim.run();
}

}  // namespace telega::bench::sim
