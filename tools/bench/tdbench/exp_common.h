#pragma once

// Общий код подкоманд экспериментов E2 и E4 (SPEC 17.5, 17.7): CSV, статистика, ожидание
// FLOOD_WAIT, детерминированные данные, состояние эксперимента в state-dir. Секреты не
// печатаются, id каналов и пользователей уходят только в state-dir, а не в отчёты.

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iosfwd>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "td/td_error.h"
#include "tdbench.h"

namespace telega::bench {

using ExpClock = std::chrono::steady_clock;

[[nodiscard]] inline double ms_since(ExpClock::time_point t0) {
    return std::chrono::duration<double, std::milli>(ExpClock::now() - t0).count();
}

// Серия остановлена: FLOOD_WAIT дольше часа или бот заблокирован (SPEC 17.2). Решение о
// продолжении принимает пользователь.
class StopExperiment : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

// Ожидание ограничений Telegram. Первый FLOOD_WAIT: серия останавливается на время ожидания
// (с запасом 20 % и 5 с) и продолжается. Больше часа или ошибка 401 (бот заблокирован,
// сессия снята) бросают StopExperiment. Все события пишутся в `flood.csv` рядом с данными.
class FloodGuard {
  public:
    FloodGuard(std::ostream& log, std::filesystem::path csv, std::string experiment);

    template <class F>
    auto run(const std::string& what, F&& f) -> decltype(f()) {
        for (;;) {
            try {
                return f();
            } catch (const td::TdException& e) {
                on_error(what, e.error());  // бросает, если повторять нельзя
            }
        }
    }

    [[nodiscard]] int events() const { return events_; }
    [[nodiscard]] double waited_seconds() const { return waited_s_; }

  private:
    void on_error(const std::string& what, const td::TdError& error);

    std::ostream& log_;
    std::filesystem::path csv_;
    std::string experiment_;
    int events_ = 0;
    double waited_s_ = 0;
};

// Вызов, ошибку TDLib которого можно игнорировать: true — успех.
template <class F>
bool attempt(F&& f) {
    try {
        f();
        return true;
    } catch (const td::TdException&) {
        return false;
    }
}

// CSV с заголовком; дописывает в существующий файл. Строка сбрасывается сразу.
class Csv {
  public:
    Csv(const std::filesystem::path& path, const std::vector<std::string>& header);
    void row(const std::vector<std::string>& fields);

  private:
    std::ofstream out_;
};

[[nodiscard]] std::string fmt(double v, int digits = 1);
[[nodiscard]] std::string iso_utc_now();
// Время с миллисекундами: `2026-11-20T08:30:04.123Z`.
[[nodiscard]] std::string iso_utc_ms(std::chrono::system_clock::time_point t);

struct Summary {
    size_t n = 0;
    double median = 0;
    double min = 0;
    double max = 0;
};
[[nodiscard]] Summary summarize(std::vector<double> values);

// Детерминированные «случайные» данные с произвольным доступом: байт p файла определяется
// только (seed, p), поэтому ожидаемое содержимое любого диапазона получается без файла.
void fill_seeded(uint64_t seed, uint64_t offset, std::string& out, size_t count);
// Файл заданного размера из тех же данных (атомарно: tmp + rename).
void write_seeded_file(const std::filesystem::path& path, uint64_t seed, uint64_t size);

[[nodiscard]] std::string base64_decode(const std::string& text);
[[nodiscard]] std::string random_hex(size_t digits);  // не секрет: имя документа

// Минимальный интервал между отправками сообщений ботом (SPEC 17.2, не больше 20 в минуту).
class SendPacer {
  public:
    explicit SendPacer(std::chrono::milliseconds interval) : interval_(interval) {}
    void wait();

  private:
    std::chrono::milliseconds interval_;
    std::optional<ExpClock::time_point> last_;
};

// Состояние эксперимента (канал, id сообщений) лежит в <state-dir>/<name>.json, 0600.
[[nodiscard]] td::Json load_state(const BenchOptions& options, const std::string& name);
void save_state(const BenchOptions& options, const std::string& name, const td::Json& state);
[[nodiscard]] bool has_state(const BenchOptions& options, const std::string& name);

// Счётчики действий за все эксперименты: <state-dir>/counters.json.
void bump_counter(const BenchOptions& options, const std::string& key, int64_t n = 1);

// Каталог сырых данных по умолчанию: docs/experiments/<name> в репозитории.
[[nodiscard]] std::filesystem::path default_out_dir(const std::string& name);

// Права администратора канала для setChatMemberStatus. Имя набора: `full`, `no:<право>` (полный
// набор без одного права) или `only:<право>,<право>` (только перечисленные).
td::Json admin_rights_for(const std::string& set_name);
// Список true-прав объекта chatAdministratorRights через запятую (для отчёта).
[[nodiscard]] std::string rights_summary(const td::Json& rights);

[[nodiscard]] td::Json sender_user(int64_t user_id);

// Создаёт приватный канал пользователем (только канал, без бота). Возвращает id чата.
int64_t create_channel(BenchAccount& user, const std::string& title, FloodGuard& guard,
                       std::ostream& log);
// Добавляет бота администратором канала (права по имени набора, см. admin_rights_for).
// Бот находится по username через searchPublicChat, id бота пользователю заранее не нужен.
void add_bot_admin(BenchAccount& user, BenchAccount& bot, int64_t chat_id,
                   const std::string& rights_set, FloodGuard& guard, std::ostream& log);

// Создаёт приватный канал пользователем и добавляет бота администратором (права по имени
// набора). Возвращает id чата. Бот находится по username через searchPublicChat.
int64_t create_channel_with_bot(BenchAccount& user, BenchAccount& bot, const std::string& title,
                                const std::string& rights_set, FloodGuard& guard,
                                std::ostream& log);

// Ждёт, пока клиент узнает чат (getChat проходит). Возвращает время ожидания в мс или nullopt.
std::optional<double> wait_chat_known(td::TdClient& client, int64_t chat_id,
                                      std::chrono::seconds timeout, std::string* last_error);

// Временный каталог бота для «перезапуска с пустой БД»: удаляет старое содержимое.
std::filesystem::path fresh_bot_dir(const BenchOptions& options);
void remove_fresh_bot_dir(const std::filesystem::path& dir);

// Запрос отправки документа по SPEC 4: <16 hex>.bin, disable_content_type_detection,
// disable_notification, без подписи. Файл уже лежит под нужным именем.
td::Json send_document_request(int64_t chat_id, const std::filesystem::path& file);

}  // namespace telega::bench
