#pragma once

// tgfs-sim: офлайн-модель записи мелких файлов (эксперимент E9).
// Модель дискретно-событийная: писатель (файловая нагрузка), запечатывание с выбором класса
// хранения, открытый пакет, очередь отправки с токен-бакетами лимитов, коммит индекса. Сети и
// диска нет, время — модельные секунды. Допущения по времени собраны в Params и в отчёте E9.

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace telega::bench::sim {

constexpr int64_t kKiB = 1024;
constexpr int64_t kMiB = 1024 * kKiB;
constexpr int64_t kGiB = 1024 * kMiB;

// Одно событие нагрузки. `step` — номер переключения (между шагами писатель ждёт step_gap_s).
// 'w' — файл записан целиком (создан или перезаписан) и закрыт, 'd' — удалён.
struct Op {
    uint32_t step = 0;
    char kind = 'w';
    uint32_t id = 0;
    int64_t size = 0;
};

struct Params {
    // Параметры ФС.
    int64_t chunk_size = 16 * kMiB;
    int64_t inline_max = 4 * kKiB;
    int64_t inline_budget = 16 * kMiB;
    int64_t pack_threshold = kMiB;  // 0 — пакетов нет (файл либо инлайн, либо позиционный)
    int64_t pack_target = 16 * kMiB;
    double pack_max_age_s = 30;
    double commit_interval_s = 60;
    // Пока очередь заливки данных не пуста, коммит по таймеру откладывается, но не
    // дольше чем на commit_defer_factor · commit_interval_s. 0 — без отсрочки (как в E9).
    double commit_defer_factor = 0;
    int commit_every_chunks = 64;
    int max_chain = 16;
    double checkpoint_ratio = 0.25;
    double seal_delay_s = 5;
    int64_t staging_limit = 8 * kGiB;

    // Лимиты отправки (E5). limits = false — без лимитов (для сравнения).
    bool limits = true;
    double send_rate_per_min = 12;
    double send_burst = 5;
    double send_bytes_per_min = 224.0 * kMiB;
    double send_bytes_burst = 64.0 * kMiB;
    int upload_concurrency = 2;

    // Модель сети (по E5): задержка документа и скорость на поток.
    double doc_latency_s = 0.2;
    double rate_alone = 14.0 * kMiB;  // байт/с, одна идущая загрузка
    double rate_shared = 8.5 * kMiB;  // байт/с на поток, когда идёт ещё одна
    double action_latency_s = 0.3;  // pin, unpin
    double cas_latency_s = 0.3;  // проверка головы перед заливкой блоба

    // Модель блоба индекса (по E1): байт на строку после zstd и шифрования.
    double delta_base = 300;
    double delta_per_row = 120;  // строка inodes + extents
    double delta_per_delete = 8;
    double snapshot_per_inode = 100;  // E1: 68-71 байт на inode в C4, 257 на C1 с инлайном
    double inline_compress = 0.3;  // инлайн-данные после zstd (текст исходников)

    // Модель времени писателя.
    double files_per_s = 200;  // создание файла с данными: 247/с без FUSE (E1)
    double write_mib_per_s = 50;  // скорость записи данных
    double unlink_per_s = 2000;
    double step_gap_s = 60;  // пауза между переключениями
    double fsync_prob = 0;  // доля файлов, после записи которых вызывается fsync
    bool seal_lag = false;  // файл не закрыт: запечатывание через seal_delay_s после записи
    uint64_t seed = 1;
};

struct Metrics {
    // Сообщения (отправки), по видам.
    int64_t docs_chunk = 0;
    int64_t docs_pack = 0;
    int64_t docs_blob_delta = 0;
    int64_t docs_blob_snapshot = 0;
    int64_t pins = 0;
    int64_t unpins = 0;
    int64_t deleted_msgs = 0;  // убрано сообщений (чанки и пакеты), не считается в лимите
    int64_t commits = 0;
    // Классы файлов в конце.
    int64_t files_inline = 0;
    int64_t files_pack = 0;
    int64_t files_pos = 0;
    int64_t inline_bytes = 0;
    int64_t writes = 0;
    int64_t deletes = 0;
    int64_t fsyncs = 0;
    // Пакеты.
    int64_t packs = 0;
    int64_t packs_by_size = 0;
    int64_t packs_by_age = 0;
    int64_t packs_by_fsync = 0;
    int64_t packs_by_unmount = 0;
    int64_t packs_all_dead = 0;
    int64_t packs_alive = 0;
    int64_t pack_plain_alive = 0;
    int64_t pack_live_alive = 0;
    int64_t packs_compactable = 0;  // live/plain < 0,25
    int64_t dead_at_close_bytes = 0;  // мёртвые объекты открытого пакета на момент закрытия
    // Байты и время.
    int64_t bytes_chunks = 0;
    int64_t bytes_packs = 0;
    int64_t bytes_blobs = 0;
    double t_writer_end_s = 0;
    double t_done_s = 0;

    [[nodiscard]] int64_t docs_total() const {
        return docs_chunk + docs_pack + docs_blob_delta + docs_blob_snapshot;
    }
    [[nodiscard]] int64_t actions_total() const { return docs_total() + pins + unpins; }
    [[nodiscard]] double dead_share() const {
        return pack_plain_alive > 0 ? static_cast<double>(pack_plain_alive - pack_live_alive) /
                                          static_cast<double>(pack_plain_alive)
                                    : 0.0;
    }
    // Доля пакетов, закрытых по fsync или pack_max_age, и то же с размонтированием.
    [[nodiscard]] double undersized_share() const {
        return packs > 0
                   ? static_cast<double>(packs_by_age + packs_by_fsync) / static_cast<double>(packs)
                   : 0.0;
    }
    [[nodiscard]] double undersized_share_with_unmount() const {
        return packs > 0 ? static_cast<double>(packs_by_age + packs_by_fsync + packs_by_unmount) /
                               static_cast<double>(packs)
                         : 0.0;
    }

    // Пары (имя, значение) для CSV: порядок и имена фиксированы.
    [[nodiscard]] std::vector<std::pair<std::string, double>> columns() const;
};

Metrics simulate(const std::vector<Op>& ops, const Params& params);

// Загрузка нагрузки. events: CSV `step,op,id,size` (tools/bench/corpora/c1*.csv).
std::vector<Op> load_events(const std::string& path);
// hist: CSV `size,count` (c2-sizes.csv): одна запись (step 0) на файл, порядок перемешан по seed.
std::vector<Op> load_hist(const std::string& path, uint64_t seed);

}  // namespace telega::bench::sim
