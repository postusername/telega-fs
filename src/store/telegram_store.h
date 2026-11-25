#pragma once

// TelegramStore: Store поверх TdClient. Канал задаётся chat_id,
// клиент уже авторизован. Библиотека не линкуется с TDLib (только telega_td), поэтому
// тестируется на фейковом транспорте.
//
// Ошибки:
// - FLOOD_WAIT (любой код с retry_after) не выходит наружу: лимитер ставит паузу, вызов ждёт и
//   повторяет. Наружу — только если истёк таймаут вызова (transient).
// - Сеть, таймаут TDLib, 5xx — StoreError transient (повторяет вызывающий: Vfs, CommitManager).
// - Сообщения нет (getMessages вернул null) — not_found. Остальные 4xx — permanent.
//
// Подробности реализации:
// - Документ в канале называется `<16 hex>.bin|.idx`: рядом с файлом чанка создаётся
//   жёсткая ссылка с таким именем (копия, если ссылки не поддерживаются) и убирается после итога
//   отправки. Каталог файла поэтому должен быть доступен на запись.
// - file_id для чтения берётся из кэша процесса, затем getRemoteFile (если вернулся размер > 0 и,
//   когда он известен процессу, равный размеру отправленного файла), иначе getMessages. Обновлённый
//   file_id хранится только в кэше процесса: ChunkRef приходит по const-ссылке.
// - Перед удалением и unpin сообщения, которых процесс не видел, подгружаются getMessages (E2, E5).
// - Если open() не звали, первый же метод по каналу сам делает getChat.
// - Скачивания идут через планировщик (download_scheduler.h): по файлу одно активное
//   окно, приоритеты пользовательского чтения, головы и readahead, readahead по
//   подсказкам Store::prefetch. read_timeout отсчитывается от входа в read_range, включая
//   ожидание в очереди; read_range_until берёт ближайший из своего read_timeout и срока вызывающего
//   (ChunkReader держит один срок на всё чтение чанка).
// - Кэш чтения (read_cache.h): учёт файлов tdfiles/ и вытеснение по cache_limit, если в
//   опциях заданы state_db и tdfiles_dir и вызван start_read_cache().
// - Секреты и пути в тексте исключений и логах не появляются.
//
// Потокобезопасен. Блокирующие вызовы нельзя делать из потока приёма TdRuntime.

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <string>

#include "store/download_scheduler.h"
#include "store/rate_limiter.h"
#include "store/read_cache.h"
#include "store/store.h"

namespace telega::td {
class TdClient;
}

namespace telega::store {

struct TelegramStoreOptions {
    int64_t chat_id = 0;
    uint32_t upload_concurrency = 2;  // одновременных отправок (документ или альбом), E5
    uint32_t download_concurrency = 4;  // одновременных downloadFile
    // Документы с открытым текстом не больше этого уходят альбомами по 10 (E6). Store видит
    // зашифрованный файл, поэтому сравнивает его размер с порогом плюс накладные расходы
    // формата чанка: 40 + 16 · ⌈порог / 64 КиБ⌉ байт.
    uint64_t album_max_doc_size = 8ULL << 20;
    // Сколько put_chunk ждёт попутчиков для альбома, прежде чем отправить то, что набралось.
    std::chrono::milliseconds album_gather{300};
    // Таймаут одного блокирующего вызова Store целиком (включая ожидание лимитера и повторы
    // после FLOOD_WAIT). put_chunk/put_index без таймаута ждут лимитер бесконечно, но сама
    // отправка (от sendMessage до итогового апдейта) ограничена send_timeout.
    std::chrono::milliseconds read_timeout{60'000};  // read_range, read_head, read_index
    // Итог одной отправки после выхода из лимитера: send_timeout плюс время на заливку документа
    // со скоростью не ниже send_min_rate байт в секунду. Не пришёл за это время:
    // временные сообщения удаляются (deleteMessages), и вызов получает transient.
    std::chrono::milliseconds send_timeout{600'000};
    uint64_t send_min_rate = 1ULL << 20;
    std::chrono::milliseconds call_timeout{60'000};  // один запрос к TDLib
    // CAS-проверка перед коммитом (read_head после собственного pin может видеть кэш TDLib до
    // минуты) повторяет CommitManager: он сам знает время своего последнего set_head.

    // Планировщик скачиваний: остановка без прогресса и обрыв (значения по
    // умолчанию — для боевых серверов, тесты ставят меньшие).
    std::chrono::milliseconds download_stall{10'000};
    std::chrono::milliseconds download_failure_guard{1'000};
    // Подсказок readahead в очереди не больше стольких (старые отбрасываются) и не старше
    // prefetch_ttl.
    size_t prefetch_queue_max = 64;
    std::chrono::milliseconds prefetch_ttl{10'000};

    // Кэш чтения: файлы TDLib в tdfiles_dir, учёт в таблице read_cache state.sqlite.
    // Пустые пути — учёта и вытеснения нет. cache_limit 0 — без вытеснения.
    std::filesystem::path state_db;
    std::filesystem::path tdfiles_dir;
    uint64_t cache_limit = uint64_t{4} << 30U;
    std::chrono::milliseconds cache_flush{60'000};  // сброс учёта в таблицу
    // Только для тестов: вызывается перед удалением каждой жертвы вытеснения (исключение из него
    // проверяет, что пометки вытеснения снимаются на любом выходе).
    std::function<void(const std::string& path)> eviction_test_hook;
};

class TelegramStore final : public Store {
  public:
    // limiter общий на процесс: все отправки в этот канал идут через него.
    TelegramStore(td::TdClient& client, RateLimiter& limiter, TelegramStoreOptions options);
    ~TelegramStore() override;

    // Проверка канала перед монтированием и init : getChat(chat_id),
    // доступ есть, message_auto_delete_time == 0, у своего аккаунта есть права публиковать,
    // редактировать и удалять (для бота — по chatMemberStatusAdministrator; для владельца канала
    // проверка проходит). Ошибка — StoreError permanent с текстом для пользователя.
    void open();

    [[nodiscard]] bool gathers_albums() const noexcept override { return true; }
    ChunkRef put_chunk(const std::filesystem::path& encrypted_file) override;
    void read_range(const ChunkRef& ref, uint64_t offset, std::span<std::byte> out) override;
    // Срок вызывающего ограничивает все повторы, ожидание лимитера и очереди; не позже read_timeout
    // от входа в вызов.
    void read_range_until(const ChunkRef& ref, uint64_t offset, std::span<std::byte> out,
                          Deadline until) override;
    // Ставит подсказку в очередь фонового потока и возвращается сразу.
    void prefetch(const ChunkRef& ref, uint64_t offset, uint64_t len, uint32_t distance) override;
    void delete_messages(std::span<const int64_t> msg_ids) override;
    // getMessages пачками по kMessagesBatch (fsck); null в ответе — сообщения нет.
    std::vector<bool> messages_exist(std::span<const int64_t> msg_ids) override;
    std::optional<HeadInfo> read_head() override;
    Bytes read_index(int64_t msg_id) override;
    int64_t put_index(const std::filesystem::path& blob) override;
    void set_head(int64_t new_msg_id, std::optional<int64_t> old_msg_id) override;

    // Кэш чтения: сверка tdfiles/ с таблицей read_cache и запуск фонового потока,
    // который раз в cache_flush сбрасывает учёт в таблицу, а при превышении cache_limit вытесняет
    // давно не использованные файлы. Зовётся при монтировании, после того как state.sqlite готов.
    // Без state_db и tdfiles_dir ничего не делает. Повторный вызов безопасен.
    void start_read_cache();
    // Вытеснение (если сумма выше cache_limit) и сброс учёта в таблицу, то же, что делает
    // фоновый поток. Для размонтирования и тестов.
    void maintain_read_cache();

    [[nodiscard]] SchedulerStats download_stats() const;
    // Ждёт, пока очередь подсказок readahead не опустеет и фоновый поток не закончит работу
    // (для тестов). false — не дождались за timeout.
    bool wait_prefetch_idle(std::chrono::milliseconds timeout);
    // nullptr, если учёт кэша не настроен.
    [[nodiscard]] const ReadCache* read_cache() const noexcept;

  private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace telega::store
