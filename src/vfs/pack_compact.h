#pragma once

// Уплотнение пакетов (SPEC 6.5): библиотечная функция, которую в M4 вызовет `gc --compact`.
//
// Берутся пакеты с live_bytes / plain_size < ratio, их живые объекты читаются (через
// ChunkReader, с проверкой тегов), собираются в новые пакеты (новый chunk_id и nonce), заливаются
// обычным put_chunk, после чего одной транзакцией index.sqlite на каждый новый пакет
// переставляются экстенты. Транзакция сначала сверяет каждый экстент с тем, каким он был при
// чтении: если файл за это время изменился (запись, truncate, unlink), объект не переставляется,
// его байты в новом пакете с самого начала мёртвые. У старых пакетов refcount падает до нуля,
// и они уходят в обычную отложенную уборку (SPEC 7.4). В конце один коммит.
//
// Функция не зависит от Vfs и overlay, поэтому годится и для смонтированной ФС (тесты), и для
// офлайн-команды: нужны только MetaStore, StateStore (журнал загрузок), Store и Committer.
// Блокировок Vfs она не берёт: согласованность дают транзакции MetaStore.

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <optional>

#include "crypto/keys.h"
#include "meta/committer.h"
#include "meta/index_db.h"
#include "meta/state_db.h"
#include "store/store.h"
#include "vfs/staging_budget.h"

namespace telega::vfs {

struct CompactContext {
    meta::MetaStore& meta;
    meta::StateStore& state;
    store::Store& store;
    meta::Committer& committer;
    const crypto::SubKeys& keys;
    std::filesystem::path tmp_dir;  // каталог для собираемых файлов (<data>/tmp)
    // Учёт staging (SPEC 8.3): если уплотнение идёт в процессе с монтированием, файлы tmp/
    // (открытый текст и зашифрованный пакет) берутся арендой этого бюджета. Офлайн — nullptr.
    StagingBudget* budget = nullptr;
    // Прервать ожидание места (остановка); нужен только вместе с budget.
    std::function<bool()> abort;
};

struct CompactOptions {
    double ratio = 0.25;  // compact_ratio профиля (SPEC 12.2)
    uint32_t download_concurrency = 4;
    std::chrono::milliseconds read_timeout{60'000};
    std::chrono::milliseconds retry_initial{1'000};
    std::chrono::milliseconds retry_max{60'000};
    // Сколько раз повторяется put_chunk при транзиентной ошибке, затем StoreError выходит
    // наружу (офлайн-команда не должна висеть вечно). Чтение повторяется до read_timeout.
    uint32_t max_upload_attempts = 8;
};

struct CompactResult {
    size_t packs_selected = 0;  // пакетов под порогом
    size_t objects_moved = 0;   // экстентов переставлено
    size_t objects_skipped = 0;  // экстент изменился с начала перепаковки
    uint64_t bytes_moved = 0;
    size_t new_packs = 0;  // залито и применено новых пакетов
    bool committed = false;
    bool aborted = false;  // ожидание места в staging прервано: уплотнение не закончено
};

// Уплотнить пакеты. Ошибка Store или диска — исключение; уже применённые пакеты остаются
// применёнными (каждый — одна транзакция), коммит тогда не делается.
CompactResult compact_packs(const CompactContext& ctx, const CompactOptions& options = {});

}  // namespace telega::vfs
