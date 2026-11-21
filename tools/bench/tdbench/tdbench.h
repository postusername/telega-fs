#pragma once

// Общая обвязка tgfs-bench (SPEC 17.2): учётные данные, каталог состояния, вход тестового
// аккаунта. Подкоманды экспериментов делают так:
//
//     BenchEnv env(options);
//     auto user = env.open(Account::kUser);   // вошёл и готов
//     auto bot = env.open(Account::kBot);     // можно оба в одном процессе
//     user->client().call(...);
//
// По умолчанию боевые серверы (тестовый DC недоступен, SPEC 17.2), --test-dc включает тестовый.
// Секреты (api_id, api_hash, токен, номер телефона) не печатаются и в лог не попадают.

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iosfwd>
#include <memory>
#include <optional>
#include <string>

#include "td/auth.h"
#include "td/send_tracker.h"
#include "td/td_client.h"
#include "td/td_runtime.h"

namespace telega::bench {

namespace fs = std::filesystem;

enum class Account { kUser, kBot };
const char* account_name(Account a);  // "user" | "bot"

// Файл --creds, формат key=value (пустые строки и строки с # пропускаются).
struct Creds {
    int32_t api_id = 0;
    std::string api_hash;
    std::optional<std::string> bot_token;  // bot-token: бот на боевых серверах
    std::optional<std::string> test_bot_token;  // test-bot-token: бот на тестовом DC
};
// Читает файл. Бросает std::runtime_error с именем ключа (не значением), если нет api-id или
// api-hash или файл не читается.
Creds read_creds(const fs::path& path);
// Дописывает `key=value` в файл атомарно (tmp, fsync, rename, права 0600), остальные строки
// не трогает. Если ключ уже есть, бросает.
void append_cred(const fs::path& path, const std::string& key, const std::string& value);

struct BenchOptions {
    fs::path creds;      // по умолчанию cred.secret в корне репозитория
    fs::path state_dir;  // по умолчанию $XDG_DATA_HOME/telega-fs-bench/
    int test_dc = 2;     // DC для нового тестового номера 99966XYYYY (1..3)
    // Номер, код и пароль 2FA спрашиваются в терминале, а не по правилам 99966XYYYY (SPEC 17.2).
    bool interactive = false;
    bool qr = false;  // вход пользователя по QR-коду (с --interactive)
    bool use_test_dc = false;  // тестовый DC вместо боевых серверов
    // Лог TDLib процесса. Пусто — <state-dir>/tdlib.log. Долгим процессам, которые работают
    // параллельно с другими (E3), нужен свой файл: лог один на процесс и ротируется по размеру.
    fs::path tdlib_log;

    // Значения по умолчанию.
    static BenchOptions defaults();
};

// Каталоги аккаунта: <state-dir>/<user|bot>/{tdlib,files}.
fs::path account_dir(const BenchOptions& options, Account account);

class BenchEnv;

// Вошедший аккаунт. Деструктор закрывает клиента TDLib (иначе данные не сбрасываются).
class BenchAccount {
  public:
    BenchAccount(const BenchAccount&) = delete;
    BenchAccount& operator=(const BenchAccount&) = delete;
    ~BenchAccount();

    [[nodiscard]] Account kind() const { return kind_; }
    [[nodiscard]] td::TdClient& client() { return *client_; }
    // Трекер отправки, созданный вместе с аккаунтом (подписан с самого начала).
    [[nodiscard]] td::SendTracker& tracker() { return *tracker_; }
    // getMe (запрос делается при каждом вызове).
    [[nodiscard]] td::Json me();

  private:
    friend class BenchEnv;
    BenchAccount(Account kind, std::shared_ptr<td::TdClient> client);

    Account kind_;
    std::shared_ptr<td::TdClient> client_;
    std::unique_ptr<td::SendTracker> tracker_;
};

class BenchEnv {
  public:
    explicit BenchEnv(BenchOptions options);
    BenchEnv(const BenchEnv&) = delete;
    BenchEnv& operator=(const BenchEnv&) = delete;
    ~BenchEnv();

    [[nodiscard]] const BenchOptions& options() const { return options_; }
    // Читает creds при первом обращении.
    [[nodiscard]] const Creds& creds();
    // Рантайм процесса. При первом вызове задаёт лог TDLib <state-dir>/tdlib.log (лог TDLib один
    // на процесс, поэтому лежит в корне state-dir, а не в каталоге аккаунта).
    td::TdRuntime& runtime();

    // Поднимает клиента аккаунта и ждёт authorizationStateReady. Для пользователя вход
    // автоматический (номер сохраняется в state-dir, код — цифра DC пять раз, регистрация при
    // необходимости), для бота нужен test-bot-token в creds. Ход входа пишется в `progress`.
    // Ошибки — TdException или std::runtime_error без секретов.
    std::unique_ptr<BenchAccount> open(Account account, std::ostream& progress);
    // То же, но база TDLib и файлы лежат в `dir` (<dir>/{tdlib,files}), а не в каталоге аккаунта.
    // Нужно экспериментам, которым нужен «перезапуск с пустой БД»: временный каталог бота
    // создаётся заново (свежий вход по токену), удаляется после замеров.
    std::unique_ptr<BenchAccount> open_at(Account account, const fs::path& dir,
                                          std::ostream& progress);

  private:
    BenchOptions options_;
    std::optional<Creds> creds_;
    std::unique_ptr<td::TdRuntime> runtime_;
};

// Номер тестового пользователя из state-dir: читает сохранённый или создаёт новый 99966XYYYY
// и сохраняет (права 0600).
std::string load_or_create_phone(const BenchOptions& options);

}  // namespace telega::bench
