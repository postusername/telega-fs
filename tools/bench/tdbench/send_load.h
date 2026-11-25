#pragma once

// Нагрузочная отправка документов ботом (E5; E6). Задание — один
// документ (sendMessage) или альбом до 10 документов (sendMessageAlbum). Каждое задание
// выполняется в своём потоке, число одновременно идущих заданий ограничено параллельностью.
// Итог каждого документа берётся из своего журнала апдейтов updateMessageSendSucceeded/Failed
// (с моментом прихода каждого апдейта), поэтому у альбома итоги документов раздельные и видно,
// пришли ли они порознь.
//
// Секреты не печатаются: в ошибки попадают только код и текст ошибки TDLib.

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <list>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "exp_common.h"
#include "tdbench.h"

namespace telega::bench {

enum class SendKind : uint8_t {
    kOk,     // доставлено, есть id сообщения
    kFlood,  // 429 / FLOOD_WAIT (retry_after_s)
    kNet,  // сетевая ошибка, 5xx или иная локальная ошибка TDLib
    kTimeout,  // итог неизвестен за отведённое время (сообщение могло уйти)
    kAccount,  // 401, бот заблокирован или сессия снята: серия останавливается
    kFatal,  // другая ошибка 4xx: повторять бессмысленно, решает человек
    kAborted,  // прервано сигналом или закрытием клиента
};
[[nodiscard]] const char* send_kind_name(SendKind kind);

// Итог одного документа задания.
struct DocOutcome {
    SendKind kind = SendKind::kAborted;
    int64_t message_id = 0;  // id сообщения TDLib (серверный id << 20), 0 если не доставлено
    int64_t retry_after_s = 0;
    std::string error;  // код и текст ошибки, без секретов
    double done_ms = 0;  // когда пришёл апдейт с итогом, от начала задания
    int64_t album_id = 0;  // media_album_id итогового сообщения (0, если нет)
};

struct SendJob {
    uint64_t id = 0;
    std::string tag;  // произвольная пометка вызывающего (ступень, серия), возвращается как есть
    // Файлы: если prepare задан, он вызывается в потоке задания до замера и возвращает файлы
    // (создаёт их). Один файл — sendMessage, больше — sendMessageAlbum (до 10).
    std::vector<std::filesystem::path> files;
    std::function<std::vector<std::filesystem::path>()> prepare;
    bool remove_files = true;  // удалить файлы после отправки
    // Размер документа, если файла к моменту отправки может уже не быть (для CSV).
    uint64_t nominal_size = 0;
    // Вызывается в потоке задания сразу после ответа на sendMessage/sendMessageAlbum, до ожидания
    // итогов: сюда E6 вставляет порчу файла после отправки (проверка сбоя одного документа).
    std::function<void(const std::vector<std::filesystem::path>&)> after_call;
};

struct SendResult {
    SendJob job;
    std::vector<uint64_t> sizes;  // размеры документов, байт
    std::vector<DocOutcome> docs;  // по одному на документ, в порядке файлов
    SendKind kind = SendKind::kAborted;  // худший исход документов (ok только если все ok)
    int64_t retry_after_s = 0;  // наибольший retry_after среди документов
    std::string error;          // первая ошибка
    std::chrono::system_clock::time_point start_wall;
    std::chrono::system_clock::time_point end_wall;
    double prepare_ms = 0;
    double call_ms = 0;      // время ответа на sendMessage/sendMessageAlbum
    std::string call_error;  // ошибка самого вызова (тогда ничего не отправлено), без секретов
    double duration_ms = 0;  // от начала sendMessage до последнего итога
    int in_flight_at_start = 0;  // заданий, уже идущих в момент начала (без этого)
    int in_flight_at_end = 0;  // заданий, ещё идущих в момент конца (без этого)
};

class SendLoad {
  public:
    struct Options {
        int parallelism = 1;
        std::chrono::milliseconds call_timeout{60'000};  // ответ на sendMessage
        std::chrono::minutes send_timeout{30};  // итог отправки (загрузка), от ответа
    };

    SendLoad(BenchAccount& bot, int64_t chat_id, Options options);
    SendLoad(const SendLoad&) = delete;
    SendLoad& operator=(const SendLoad&) = delete;
    // Прерывает ожидание, дожидается потоков.
    ~SendLoad();

    [[nodiscard]] int parallelism() const { return options_.parallelism; }
    [[nodiscard]] int in_flight() const;
    [[nodiscard]] bool has_free_slot() const { return in_flight() < options_.parallelism; }

    // Запускает задание в новом потоке. Вызывающий сам следит за слотами (has_free_slot).
    void launch(SendJob job);
    // Забирает завершённые задания (в порядке завершения), потоки присоединяются.
    std::vector<SendResult> take_finished();
    // Ждёт, пока завершится любое задание, но не дольше timeout. true — есть что забрать.
    bool wait_event(std::chrono::milliseconds timeout);
    // Заставляет идущие задания завершиться исходом kAborted при ближайшей проверке.
    void abort();

  private:
    // Итог отправки одного сообщения из апдейтов TDLib.
    struct Event {
        bool failed = false;
        int64_t message_id = 0;
        int64_t album_id = 0;
        std::optional<td::TdError> error;
        ExpClock::time_point at;
    };
    using Key = std::pair<int64_t, int64_t>;  // (chat_id, id временного сообщения)

    void record(const td::Json& update, bool failed);
    // Ждёт апдейт с итогом не дольше slice и забирает его.
    std::optional<Event> take_event(int64_t old_message_id, std::chrono::milliseconds slice);

    struct Worker {
        std::thread thread;
        bool finished = false;
        SendResult result;
    };

    void run(Worker* worker, SendJob job, int in_flight_at_start);
    void execute(SendResult& result, SendJob& job);

    BenchAccount& bot_;
    const int64_t chat_id_;
    const Options options_;
    std::atomic<bool> abort_{false};

    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::list<Worker> workers_;
    int running_ = 0;

    std::mutex events_mu_;
    std::condition_variable events_cv_;
    std::map<Key, Event> events_;
    // Последними: отписка раньше остальных членов.
    td::Subscription succeeded_;
    td::Subscription failed_;
};

// Запрос sendMessage с документом как в telega-fs (`send_document_request`).
[[nodiscard]] td::Json single_document_request(int64_t chat_id, const std::filesystem::path& file);
// Запрос sendMessageAlbum с документами (до 10) с теми же флагами.
[[nodiscard]] td::Json album_document_request(int64_t chat_id,
                                              const std::vector<std::filesystem::path>& files);

// Документ из детерминированных данных: файл `<dir>/<16 hex>.bin` размером `size` байт,
// содержимое определяется только (seed, позиция).
[[nodiscard]] std::filesystem::path make_seeded_document(const std::filesystem::path& dir,
                                                         uint64_t seed, uint64_t size);

}  // namespace telega::bench
