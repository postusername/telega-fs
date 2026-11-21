#pragma once

// TelegramStore: Store поверх TdClient (SPEC 3, 4, 7.1, 8.2, 10). Канал задаётся chat_id,
// клиент уже авторизован. Библиотека не линкуется с TDLib (только telega_td), поэтому
// тестируется на фейковом транспорте.
//
// Ошибки (SPEC 3, 10.2):
// - FLOOD_WAIT (любой код с retry_after) не выходит наружу: лимитер ставит паузу, вызов ждёт и
//   повторяет. Наружу — только если истёк таймаут вызова (transient).
// - Сеть, таймаут TDLib, 5xx — StoreError transient (повторяет вызывающий: Vfs, CommitManager).
// - Сообщения нет (getMessages вернул null) — not_found. Остальные 4xx — permanent.
//
// Подробности реализации:
// - Документ в канале называется `<16 hex>.bin|.idx` (SPEC 4): рядом с файлом чанка создаётся
//   жёсткая ссылка с таким именем (копия, если ссылки не поддерживаются) и убирается после итога
//   отправки. Каталог файла поэтому должен быть доступен на запись.
// - file_id для чтения берётся из кэша процесса, затем getRemoteFile (если вернулся размер > 0 и,
//   когда он известен процессу, равный размеру отправленного файла), иначе getMessages. Обновлённый
//   file_id хранится только в кэше процесса: ChunkRef приходит по const-ссылке.
// - Перед удалением и unpin сообщения, которых процесс не видел, подгружаются getMessages (E2, E5).
// - Если open() не звали, первый же метод по каналу сам делает getChat (SPEC 2).
// - Секреты и пути в тексте исключений и логах не появляются (SPEC 11).
//
// Потокобезопасен. Блокирующие вызовы нельзя делать из потока приёма TdRuntime (SPEC 10.1).

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>

#include "store/rate_limiter.h"
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
    // формата чанка: 40 + 16 · ⌈порог / 64 КиБ⌉ байт (SPEC 5).
    uint64_t album_max_doc_size = 8ULL << 20;
    // Сколько put_chunk ждёт попутчиков для альбома, прежде чем отправить то, что набралось.
    std::chrono::milliseconds album_gather{300};
    // Таймаут одного блокирующего вызова Store целиком (включая ожидание лимитера и повторы
    // после FLOOD_WAIT). put_chunk/put_index без таймаута ждут лимитер бесконечно, но сама
    // отправка (от sendMessage до итогового апдейта) ограничена send_timeout.
    std::chrono::milliseconds read_timeout{60'000};  // read_range, read_head, read_index
    // Итог одной отправки после выхода из лимитера: send_timeout плюс время на заливку документа
    // со скоростью не ниже send_min_rate байт в секунду (SPEC 10.2). Не пришёл за это время:
    // временные сообщения удаляются (deleteMessages), и вызов получает transient.
    std::chrono::milliseconds send_timeout{600'000};
    uint64_t send_min_rate = 1ULL << 20;
    std::chrono::milliseconds call_timeout{60'000};  // один запрос к TDLib
    // CAS в шаге 4 SPEC 7.1 (read_head после собственного pin может видеть кэш TDLib до минуты)
    // повторяет CommitManager: он сам знает время своего последнего set_head.
};

class TelegramStore final : public Store {
  public:
    // limiter общий на процесс: все отправки в этот канал идут через него.
    TelegramStore(td::TdClient& client, RateLimiter& limiter, TelegramStoreOptions options);
    ~TelegramStore() override;

    // Проверка канала перед монтированием и init (SPEC 7.2 шаг 3, 10.1): getChat(chat_id),
    // доступ есть, message_auto_delete_time == 0, у своего аккаунта есть права публиковать,
    // редактировать и удалять (для бота — по chatMemberStatusAdministrator; для владельца канала
    // проверка проходит). Ошибка — StoreError permanent с текстом для пользователя.
    void open();

    [[nodiscard]] bool gathers_albums() const noexcept override { return true; }
    ChunkRef put_chunk(const std::filesystem::path& encrypted_file) override;
    void read_range(const ChunkRef& ref, uint64_t offset, std::span<std::byte> out) override;
    void delete_messages(std::span<const int64_t> msg_ids) override;
    std::optional<HeadInfo> read_head() override;
    Bytes read_index(int64_t msg_id) override;
    int64_t put_index(const std::filesystem::path& blob) override;
    void set_head(int64_t new_msg_id, std::optional<int64_t> old_msg_id) override;

  private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace telega::store
