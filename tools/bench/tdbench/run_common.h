#pragma once

// Общая обвязка долгих возобновляемых прогонов экспериментов (E6 и далее): остановка по
// сигналу, пауза с проверкой остановки, flock на сессию, чтение CSV, состояние серий в
// <state-dir>/<имя>.json и ожидание ограничений Telegram вне измеряемых ступеней. Код
// повторяет решения cmd_e5.cpp; E5 на него не переведён (его прогон закончен).

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#include "exp_common.h"

namespace telega::bench::run {

// Флаг остановки по SIGINT/SIGTERM: текущие загрузки доделываются, процесс сохраняет состояние.
[[nodiscard]] bool stopping();
void install_signal_handlers();

class Interrupted : public std::runtime_error {
  public:
    Interrupted() : std::runtime_error("interrupted by signal") {}
};

// Клиент TDLib закрыт или рантайм остановлен: сеанс надо открыть заново.
class SessionError : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

// Спит, проверяя флаг остановки; бросает Interrupted.
void sleep_interruptible(std::chrono::milliseconds duration);
void log_line(const std::string& text);
[[nodiscard]] int64_t epoch_now();
[[nodiscard]] double seconds_between(std::chrono::system_clock::time_point a,
                                     std::chrono::system_clock::time_point b);
// Ждёт момента epoch (секунды Unix), раз в 5 минут пишет в лог, сколько осталось.
void wait_until_epoch(int64_t epoch, const std::string& why);

// Один процесс на каталог сессии бота: две копии TDLib на одной БД нельзя.
class SessionLock {
  public:
    SessionLock(const std::filesystem::path& path, const std::string& experiment);
    SessionLock(const SessionLock&) = delete;
    SessionLock& operator=(const SessionLock&) = delete;
    ~SessionLock();

  private:
    int fd_ = -1;
};

[[nodiscard]] std::vector<std::string> parse_csv_line(const std::string& line);
// CSV с заголовком в строки-словари (для status и возобновления).
[[nodiscard]] std::vector<std::map<std::string, std::string>> read_csv(
    const std::filesystem::path& path);

// Состояние серий: <state-dir>/<name>.json. Номера документов после перезапуска пропускают 1000,
// чтобы документы не повторялись.
class RunState {
  public:
    RunState(const BenchOptions& options, std::string name);

    td::Json& series(const std::string& id) { return j_["series"][id]; }
    [[nodiscard]] bool is_done(const std::string& id) const;
    td::Json& root() { return j_; }
    uint64_t take_doc_no();
    void save();

  private:
    const BenchOptions& options_;
    std::string name_;
    td::Json j_;
};

// Ожидание FLOOD_WAIT и повтор при сетевых ошибках для вызовов вне измеряемых ступеней
// (удаление, getMessages, getChat). Прочие ошибки возвращаются вызывающему.
class Guard {
  public:
    // Файлы <prefix>_flood.csv и <prefix>_net_errors.csv в каталоге out.
    Guard(const std::filesystem::path& out, const std::string& prefix,
          std::vector<std::string> flood_header);

    void set_series(const std::string& id) { series_ = id; }
    Csv& flood() { return flood_; }
    Csv& net() { return net_; }

    template <class F>
    auto call(const std::string& what, F&& f) -> decltype(f()) {
        for (int transient = 0;;) {
            if (stopping()) {
                throw Interrupted();
            }
            try {
                return f();
            } catch (const td::TdException& e) {
                on_error(what, e.error(), &transient);
            }
        }
    }

  private:
    // Возвращается, если вызов надо повторить; иначе бросает.
    void on_error(const std::string& what, const td::TdError& error, int* transient);

    Csv flood_;
    Csv net_;
    std::string series_;
    size_t flood_columns_;
};

}  // namespace telega::bench::run
