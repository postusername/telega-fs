#pragma once

// PackWriter — класс хранения «в пакете»: мелкие файлы собираются в один чанк
// kind = 2 обычного формата и заливаются одним сообщением.
//
// Открытый пакет — это строки таблицы open_pack (ino, version, len, added_ns) и их образ в
// памяти; отдельного файла у него нет: данные объектов до заливки лежат в staging-файлах
// позиции 0 своих файлов (overlay). Объект попадает в пакет, когда Vfs запечатывает файл
// (add). Пакет закрывается по размеру, возрасту (от самого старого объекта), fsync файла из
// пакета, размонтированию и давлению на staging (close_open, close_for, tick). Закрытый пакет
// живёт в памяти как набор (ino, version) и заливается задачей run() в пуле загрузчиков Vfs:
//
//   1. сборка: из живых объектов (version совпадает с overlay) в tmp/pack-<id>, под
//      разделяемой полосой каждого inode; объект, чей файл успел измениться, в пакет не
//      попадает;
//   2. шифрование в tmp/chunk-<id> обычным форматом чанка, запись в upload_journal, put_chunk
//      (транзиентные ошибки повторяются бесконечно, постоянная — пакет отброшен, объекты
//      остаются в overlay с ошибкой);
//   3. применение: под исключительными полосами всех объектов (берутся по возрастанию адреса:
//      любой другой поток держит не больше одной полосы, поэтому взаимной блокировки нет)
//      одна транзакция index.sqlite добавляет чанк и переставляет экстенты тех объектов, чья
//      version не изменилась; затем одна транзакция state.sqlite очищает overlay и строки
//      open_pack. Если не применён ни один объект, сообщение удаляется.
//
// Блокировка mu_ — лист: под ней не берутся ни полосы, ни MetaStore, ни StateStore. Строки
// open_pack пишутся в state.sqlite до обновления образа, а удаляются Overlay в тех же
// транзакциях, что и грязные чанки (pack_hook возвращает вызов сюда).

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <map>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "common/bytes.h"
#include "crypto/keys.h"
#include "meta/index_db.h"
#include "meta/state_db.h"
#include "store/store.h"
#include "vfs/overlay.h"
#include "vfs/staging_budget.h"

namespace telega::vfs {

struct PackConfig {
    int64_t chunk_size = 0;
    int64_t pack_threshold = 0;
    int64_t pack_target = 0;
    std::chrono::milliseconds max_age{30'000};
    std::chrono::milliseconds retry_initial{1'000};
    std::chrono::milliseconds retry_max{60'000};
};

// Что PackWriter берёт у Vfs. Все функции обязательны.
struct PackHooks {
    // Полоса inode.
    std::function<std::shared_mutex&(int64_t ino)> stripe;
    std::function<int64_t()> base_gen;
    std::function<bool()> read_only;
    std::function<bool()> stopping;
    // Пауза повтора транзиентной ошибки с удвоением; false — идёт остановка.
    std::function<bool(std::chrono::milliseconds& delay)> backoff;
    std::function<void(const std::string& what)> note_error;
    // Поставить run(seq) в очередь пула загрузчиков.
    std::function<void(uint64_t seq)> submit;
    // Запечатать файл явно и закрыть пакет: объект явного пакета изменился за время заливки.
    std::function<void(int64_t ino)> reseal_explicit;
    // Пустой staging-файл позиции не заливается (Vfs::resolve_empty_position).
    std::function<void(const ChunkKey& key, uint64_t incarnation)> resolve_empty;
    // Пакет залит и применён (счётчик commit_every_chunks).
    std::function<void()> chunk_uploaded;
    // Занять место в tmp/ под сборку и шифрование пакета: ждёт, пока не поместится
    // до staging_limit, вне полос inode. nullopt — идёт остановка. Может быть пустой: тогда
    // место не считается.
    std::function<std::optional<StagingBudget::Lease>(uint64_t bytes)> acquire_tmp;
    // Только для тестов (инъекция сбоев): вызывается на шагах заливки и применения пакета —
    // "run" (до сборки), "assembled", "encrypted", "journal_started", "uploaded",
    // "journal_finished", "index_applied", "overlay_cleared" — исключение из неё моделирует падение
    // процесса. Может быть пустой.
    std::function<void(std::string_view step)> probe;
};

struct PackStatus {
    size_t open_objects = 0;  // объекты открытого пакета (без закрытых)
    int64_t open_bytes = 0;  // сумма их len
    size_t closing_packs = 0;  // закрытые пакеты, которые ещё заливаются
    uint64_t packs_uploaded = 0;  // применённые пакеты
    uint64_t objects_applied = 0;
};

class PackWriter {
  public:
    PackWriter(meta::MetaStore& meta, meta::StateStore& state, store::Store& store,
               Overlay& overlay, crypto::SubKeys keys, const FsUuid& fs_uuid,
               std::filesystem::path tmp_dir, PackConfig config, PackHooks hooks);
    PackWriter(const PackWriter&) = delete;
    PackWriter& operator=(const PackWriter&) = delete;

    // Поднять open_pack из state.sqlite после восстановления overlay: строки без грязного
    // чанка позиции 0 удаляются (persist = false — только в памяти, для --ro).
    void load(bool persist);

    // Запечатать объект: вставить или заменить строку. Вызывать под исключительной
    // полосой ino. Может закрыть пакет по размеру.
    void add(int64_t ino, int64_t version, int64_t len);
    // Файл больше не пакетный (стал позиционным): убрать его строку, если она есть. Под полосой.
    void drop(int64_t ino);
    // Позиция 0 ушла из overlay (вызывается Overlay после транзакции, строка уже удалена).
    void on_dropped(int64_t ino);

    // Закрыть открытый пакет и поставить его заливку. false — открытый пакет пуст.
    bool close_open(bool explicit_request);
    // fsync файла: если его объект в открытом пакете — закрыть пакет; если он уже в закрытом
    // и заливаемом пакете — ничего (fsync просто ждёт его заливку). true — пакет закрыт.
    bool close_for(int64_t ino);
    // Таймер: закрыть пакет, если самый старый объект старше max_age.
    void tick();
    // Залить закрытый пакет seq (рабочий поток пула).
    void run(uint64_t seq);

    [[nodiscard]] PackStatus status() const;
    // Объект ino сейчас в открытом пакете (не закрыт).
    [[nodiscard]] bool in_open_pack(int64_t ino) const;

  private:
    struct Row {
        int64_t version = 0;
        int64_t len = 0;
        int64_t added_ns = 0;
    };
    struct Member {
        int64_t ino = 0;
        int64_t version = 0;
    };
    struct Closing {
        std::vector<Member> members;
        // Пакет закрыт явным глобальным запросом (flush_all, sync): спрос у всех объектов.
        bool explicit_all = false;
        // Файлы, чей fsync закрыл пакет (или чьи объекты остались в нём после прошлого пакета
        // с таким спросом): спрос только у них.
        std::set<int64_t> explicit_inos;
        // Значение explicit_seq_ при закрытии: если к концу заливки метка глобального запроса
        // или метка fsync объекта стала больше, во время заливки кто-то ждёт этот объект.
        uint64_t explicit_seq_at_close = 0;
        // Сумма len строк при закрытии: верхняя оценка открытого текста пакета.
        int64_t total_len = 0;
    };
    // Объект, попавший в собранный файл пакета.
    struct Placed {
        int64_t ino = 0;
        int64_t version = 0;
        uint64_t incarnation = 0;
        int64_t off = 0;
        int64_t len = 0;
    };
    enum class Outcome : uint8_t {
        applied,  // экстент переставлен, overlay очищен
        dead,  // файл удалён или обрезан до нуля: чанка позиции 0 нет
        changed,  // version изменилась: объект уйдёт в следующий пакет
        reseal,  // файл перестал быть пакетным: запечатывается заново
        kept,  // остался в открытом пакете (не влез, остановка)
        failed,  // постоянная ошибка заливки
    };

    // Под mu_.
    [[nodiscard]] bool is_open_locked(int64_t ino, const Row& row) const;
    void account_add_locked(int64_t ino, const Row& row);
    void account_remove_locked(int64_t ino, const Row& row);
    void set_row_locked(int64_t ino, const Row& row);
    void erase_row_locked(int64_t ino);
    // Закрыть открытый пакет; 0 — пакета нет. После вызова вне mu_ нужен hooks_.submit(seq).
    // explicit_all — глобальный явный запрос, explicit_inos — файлы со спросом.
    uint64_t close_locked(bool explicit_all, std::set<int64_t> explicit_inos = {});
    // Объект m пакета ждёт кто-то явный (под mu_).
    [[nodiscard]] bool demanded_locked(const Closing& pack, int64_t ino) const;

    struct Assembled {
        std::vector<Placed> placed;
        std::vector<ChunkKey> empties;
        std::map<int64_t, Outcome> outcomes;  // ino → исход для не попавших в пакет
        int64_t total = 0;
    };
    Assembled assemble(const Closing& pack, const std::filesystem::path& plain, bool& stop);
    // Заливка с повторами. false — пакет не нужен (остановка, все объекты умерли) либо
    // постоянная ошибка (failed = true).
    bool upload(int64_t chunk_id, const std::filesystem::path& enc, const Assembled& as,
                store::ChunkRef& ref, bool& failed);
    // Применить к индексу; возвращает ино применённых объектов в outcomes.
    void apply(int64_t chunk_id, const store::ChunkRef& ref, int64_t plain_size, int64_t enc_size,
               ByteSpan mac, Assembled& as);
    void finalize(const Closing& pack, const std::map<int64_t, Outcome>& outcomes);
    [[nodiscard]] bool any_live(const std::vector<Placed>& placed) const;
    void probe(std::string_view step) const {
        if (hooks_.probe) {
            hooks_.probe(step);
        }
    }

    meta::MetaStore& meta_;
    meta::StateStore& state_;
    store::Store& store_;
    Overlay& overlay_;
    crypto::SubKeys keys_;
    FsUuid fs_uuid_;
    std::filesystem::path tmp_;
    PackConfig cfg_;
    PackHooks hooks_;

    // Сборка и шифрование пакета идут по одному: в этот момент в tmp/ лежат и открытый текст
    // (до chunk_size), и зашифрованный файл, а резерв заливки рассчитан на один такой
    // пакет сверх обычных заливок. Берётся до всех блокировок, держится без полос.
    std::mutex stage_mu_;

    mutable std::mutex mu_;        // лист
    std::map<int64_t, Row> rows_;  // образ open_pack
    std::unordered_map<int64_t, int64_t> marks_;  // ino → version, закрытая в заливаемый пакет
    int64_t open_bytes_ = 0;
    size_t open_count_ = 0;
    std::set<std::pair<int64_t, int64_t>> ages_;  // (added_ns, ino) объектов открытого пакета
    std::map<uint64_t, Closing> closing_;
    uint64_t next_seq_ = 1;
    // Метки явных запросов на закрытие: explicit_seq_ растёт с каждым запросом, flush_stamp_ —
    // метка последнего глобального (flush_all), fsync_stamp_ — метки fsync по файлам. Спрос
    // привязан к ino: fsync постороннего файла не закрывает чужой пакет и не метит других.
    uint64_t explicit_seq_ = 0;
    uint64_t flush_stamp_ = 0;
    std::map<int64_t, uint64_t> fsync_stamp_;
    uint64_t packs_uploaded_ = 0;
    uint64_t objects_applied_ = 0;
};

}  // namespace telega::vfs
