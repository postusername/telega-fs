#include "tdbench.h"

#include <sodium.h>
#include <sys/stat.h>
#include <termios.h>
#include <unistd.h>

#include <algorithm>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <random>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <utility>

#include "app/version.h"
#include "common/bytes.h"
#include "common/files.h"
#include "td/tdjson_transport.h"

namespace telega::bench {

namespace {

constexpr std::chrono::seconds kMaxFloodWait{30 * 60};

std::string trim(const std::string& s) {
    size_t b = 0;
    size_t e = s.size();
    while (b < e && std::isspace(static_cast<unsigned char>(s[b])) != 0) {
        ++b;
    }
    while (e > b && std::isspace(static_cast<unsigned char>(s[e - 1])) != 0) {
        --e;
    }
    return s.substr(b, e - b);
}

void make_private_dir(const fs::path& dir) {
    fs::create_directories(dir);
    fs::permissions(dir, fs::perms::owner_all, fs::perm_options::replace);
}

// Уровень лога TDLib: 1 (SPEC 10.1), для отладки TGFS_BENCH_TDLIB_LOG=0..5.
int tdlib_log_level() {
    if (const char* v = std::getenv("TGFS_BENCH_TDLIB_LOG"); v != nullptr && *v != '\0') {
        return std::clamp(std::atoi(v), 0, 5);
    }
    return 1;
}

std::string read_text(const fs::path& path) {
    const std::ifstream in(path, std::ios::binary);
    if (!in) {
        throw std::runtime_error("cannot read " + path.string());
    }
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

// Строка из терминала; echo выключается для паролей. Ввод не печатается и не логируется.
std::string ask(const std::string& prompt, bool secret = false) {
    std::cerr << prompt << std::flush;
    termios old_attrs{};
    const bool tty =
        secret && ::isatty(STDIN_FILENO) != 0 && ::tcgetattr(STDIN_FILENO, &old_attrs) == 0;
    if (tty) {
        termios no_echo = old_attrs;
        no_echo.c_lflag &= ~static_cast<tcflag_t>(ECHO);
        ::tcsetattr(STDIN_FILENO, TCSANOW, &no_echo);
    }
    std::string line;
    const bool ok = static_cast<bool>(std::getline(std::cin, line));
    if (tty) {
        ::tcsetattr(STDIN_FILENO, TCSANOW, &old_attrs);
        std::cerr << "\n";
    }
    if (!ok) {
        throw std::runtime_error("no input on stdin");
    }
    return trim(line);
}

// Подсказчик входа по правилам тестового DC (SPEC 17.2).
class AutoPrompter final : public td::AuthPrompter {
  public:
    AutoPrompter(const BenchOptions& options, Creds creds, std::ostream& progress)
        : options_(options), creds_(std::move(creds)), progress_(progress) {}

    std::string phone_number() override {
        if (options_.interactive || options_.qr) {
            return ask("Phone number (international format): ");
        }
        if (!options_.use_test_dc) {
            throw std::runtime_error("the user account is not logged in; run login --interactive");
        }
        phone_ = load_or_create_phone(options_);
        return phone_;
    }
    std::string code(const td::Json& code_info) override {
        if (options_.interactive || options_.qr) {
            // Куда ушёл код: в приложение Telegram, SMS, звонок и т.д. Пустой ввод — отправить
            // заново.
            const auto type_of = [&](const char* key) -> std::string {
                if (code_info.contains(key) && code_info[key].is_object()) {
                    return code_info[key].value("@type", std::string{"?"});
                }
                return "-";
            };
            std::cerr << "code sent via " << type_of("type") << ", next: " << type_of("next_type")
                      << ", timeout " << code_info.value("timeout", 0) << " s\n";
            return ask("Login code (empty line = resend): ");
        }
        if (!options_.use_test_dc) {
            throw std::runtime_error("a login code is needed; run login --interactive");
        }
        // Код тестового аккаунта — цифра DC, повторённая length раз (обычно пять); DC — шестая
        // цифра номера 99966X.... Длину берём из code_info: Telegram её уже менял.
        if (phone_.empty()) {
            phone_ = load_or_create_phone(options_);  // TDLib мог вернуться в WaitCode из сессии
        }
        const char dc = phone_.size() > 5 ? phone_[5] : '2';
        int length = 5;
        if (code_info.contains("type") && code_info["type"].is_object()) {
            const int announced = code_info["type"].value("length", 0);
            length = announced > 0 && announced <= 12 ? announced : 5;
        }
        std::string digits(static_cast<size_t>(length), dc);
        return digits;
    }
    bool use_qr() override { return options_.qr; }
    void show_qr_link(const std::string& link) override {
        // QR рисует python3-qrcode; ссылка уходит через stdin, а не в argv (не видна в ps).
        std::cerr << "\nScan with the Telegram app on your phone: Settings > Devices > "
                     "Link Desktop Device\n"
                  << std::flush;
        FILE* py = ::popen(
            "python3 -I -c 'import sys,qrcode; q=qrcode.QRCode(border=2); "
            "q.add_data(sys.stdin.read().strip()); q.print_ascii(out=sys.stderr, invert=True)'",
            "w");
        if (py != nullptr) {
            std::fputs(link.c_str(), py);
            if (::pclose(py) == 0) {
                return;
            }
        }
        std::cerr << "cannot draw the QR code (pip install qrcode); link: " << link << "\n";
    }
    std::string password(const std::string& /*hint*/) override {
        if (options_.interactive || options_.qr) {
            return ask("2FA password: ", /*secret=*/true);
        }
        throw std::runtime_error(
            "the test account asks for a 2FA password; bench accounts have none");
    }
    td::RegistrationName registration() override {
        progress_ << "registering a new test user\n";
        return {"tgfs", "bench"};
    }
    std::string bot_token() override {
        const auto& token = options_.use_test_dc ? creds_.test_bot_token : creds_.bot_token;
        if (!token) {
            throw std::runtime_error(options_.use_test_dc
                                         ? "test-bot-token is not set in the creds file"
                                         : "bot-token is not set in the creds file");
        }
        return *token;
    }
    bool retry_after_error(td::AuthStep step, const td::TdError& error) override {
        // FLOOD_WAIT на входе: выждать с запасом и повторить тот же шаг, но не больше 30 минут.
        if (!error.retry_after) {
            return false;
        }
        if (*error.retry_after > kMaxFloodWait) {
            progress_ << "FLOOD_WAIT of " << error.retry_after->count()
                      << " s is longer than 30 minutes, giving up\n";
            return false;
        }
        const auto wait = *error.retry_after + *error.retry_after / 10 + std::chrono::seconds{5};
        progress_ << "FLOOD_WAIT at login step " << static_cast<int>(step) << ": waiting "
                  << wait.count() << " s\n"
                  << std::flush;
        std::this_thread::sleep_for(wait);
        return true;
    }

  private:
    const BenchOptions& options_;
    Creds creds_;
    std::ostream& progress_;
    std::string phone_;
};

}  // namespace

const char* account_name(Account a) { return a == Account::kUser ? "user" : "bot"; }

Creds read_creds(const fs::path& path) {
    if (!fs::exists(path)) {
        throw std::runtime_error("creds file not found: " + path.string());
    }
    std::istringstream in(read_text(path));
    Creds creds;
    bool have_id = false;
    std::string line;
    while (std::getline(in, line)) {
        line = trim(line);
        if (line.empty() || line[0] == '#') {
            continue;
        }
        const size_t eq = line.find('=');
        if (eq == std::string::npos) {
            continue;
        }
        const std::string key = trim(line.substr(0, eq));
        std::string value = trim(line.substr(eq + 1));
        if (value.size() >= 2 && (value.front() == '"' || value.front() == '\'') &&
            value.back() == value.front()) {
            value = value.substr(1, value.size() - 2);
        }
        if (key == "api-id") {
            try {
                creds.api_id = static_cast<int32_t>(std::stol(value));
                have_id = true;
            } catch (const std::exception&) {
                throw std::runtime_error("creds: api-id is not a number");
            }
        } else if (key == "api-hash") {
            creds.api_hash = value;
        } else if (key == "test-bot-token" && !value.empty()) {
            creds.test_bot_token = value;
        } else if (key == "bot-token" && !value.empty()) {
            creds.bot_token = value;
        }
    }
    if (!have_id) {
        throw std::runtime_error("creds: api-id is missing in " + path.string());
    }
    if (creds.api_hash.empty()) {
        throw std::runtime_error("creds: api-hash is missing in " + path.string());
    }
    return creds;
}

void append_cred(const fs::path& path, const std::string& key, const std::string& value) {
    std::string content = fs::exists(path) ? read_text(path) : std::string{};
    std::istringstream in(content);
    std::string line;
    while (std::getline(in, line)) {
        line = trim(line);
        if (line.starts_with(key) && trim(line.substr(key.size())).starts_with('=')) {
            throw std::runtime_error("creds: " + key + " is already set");
        }
    }
    if (!content.empty() && content.back() != '\n') {
        content += '\n';
    }
    content += key + "=" + value + "\n";
    write_file_atomic(path, as_bytes(content), 0600);
}

BenchOptions BenchOptions::defaults() {
    BenchOptions o;
#ifdef TGFS_REPO_ROOT
    o.creds = fs::path{TGFS_REPO_ROOT} / "cred.secret";
#else
    o.creds = "cred.secret";
#endif
    fs::path data;
    if (const char* xdg = std::getenv("XDG_DATA_HOME"); xdg != nullptr && *xdg != '\0') {
        data = xdg;
    } else if (const char* home = std::getenv("HOME"); home != nullptr && *home != '\0') {
        data = fs::path{home} / ".local" / "share";
    } else {
        data = ".";
    }
    o.state_dir = data / "telega-fs-bench";
    return o;
}

fs::path account_dir(const BenchOptions& options, Account account) {
    // Сессии тестового DC и боевых серверов не смешиваются.
    return options.state_dir /
           (std::string{options.use_test_dc ? "test-" : ""} + account_name(account));
}

// Ключ БД TDLib для боевых серверов: 32 случайных байта в <state-dir>/db.key (0600, SPEC 17.2).
static std::string load_or_create_db_key(const BenchOptions& options) {
    const fs::path file = options.state_dir / "db.key";
    if (fs::exists(file)) {
        std::string key = read_text(file);
        if (key.size() != 32) {
            throw std::runtime_error("unexpected size of " + file.string());
        }
        return key;
    }
    if (sodium_init() < 0) {
        throw std::runtime_error("sodium_init failed");
    }
    std::string key(32, '\0');
    randombytes_buf(key.data(), key.size());
    make_private_dir(options.state_dir);
    write_file_atomic(file, as_bytes(key), 0600);
    return key;
}

std::string load_or_create_phone(const BenchOptions& options) {
    const fs::path file = account_dir(options, Account::kUser) / "phone";
    if (fs::exists(file)) {
        const std::string phone = trim(read_text(file));
        if (phone.size() == 10 && phone.starts_with("99966")) {
            return phone;
        }
        throw std::runtime_error("unexpected content in " + file.string() +
                                 ", remove the file to get a new test number");
    }
    if (options.test_dc < 1 || options.test_dc > 3) {
        throw std::runtime_error("test DC must be 1, 2 or 3");
    }
    std::random_device rd;
    std::uniform_int_distribution<int> digit(0, 9);
    std::string phone = "99966" + std::to_string(options.test_dc);
    for (int i = 0; i < 4; ++i) {
        phone += static_cast<char>('0' + digit(rd));
    }
    make_private_dir(account_dir(options, Account::kUser));
    write_file_atomic(file, as_bytes(phone), 0600);
    return phone;
}

// ---- BenchAccount ------------------------------------------------------------------------

BenchAccount::BenchAccount(Account kind, std::shared_ptr<td::TdClient> client)
    : kind_(kind),
      client_(std::move(client)),
      tracker_(std::make_unique<td::SendTracker>(*client_)) {}

BenchAccount::~BenchAccount() {
    tracker_.reset();
    if (client_ && !client_->close(std::chrono::seconds{30})) {
        std::cerr << "warning: " << account_name(kind_) << " client did not close in 30 s\n";
    }
}

td::Json BenchAccount::me() { return client_->call(td::Json{{"@type", "getMe"}}); }

// ---- BenchEnv ----------------------------------------------------------------------------

BenchEnv::BenchEnv(BenchOptions options) : options_(std::move(options)) {}

BenchEnv::~BenchEnv() {
    if (runtime_) {
        runtime_->stop();
    }
}

const Creds& BenchEnv::creds() {
    if (!creds_) {
        creds_ = read_creds(options_.creds);
    }
    return *creds_;
}

td::TdRuntime& BenchEnv::runtime() {
    if (!runtime_) {
        make_private_dir(options_.state_dir);
        auto transport = std::make_shared<td::TdJsonTransport>();
        std::string error;
        if (!td::set_tdlib_log_file(
                *transport,
                options_.tdlib_log.empty() ? options_.state_dir / "tdlib.log" : options_.tdlib_log,
                16LL * 1024 * 1024, tdlib_log_level(), &error)) {
            throw std::runtime_error("setLogStream failed: " + error);
        }
        runtime_ = std::make_unique<td::TdRuntime>(std::move(transport));
    }
    return *runtime_;
}

std::unique_ptr<BenchAccount> BenchEnv::open(Account account, std::ostream& progress) {
    return open_at(account, account_dir(options_, account), progress);
}

std::unique_ptr<BenchAccount> BenchEnv::open_at(Account account, const fs::path& dir,
                                                std::ostream& progress) {
    const Creds& c = creds();
    if (account == Account::kBot && !(options_.use_test_dc ? c.test_bot_token : c.bot_token)) {
        throw std::runtime_error(
            options_.use_test_dc ? "test-bot-token is not set in the creds file (run create-bot)"
                                 : "bot-token is not set in the creds file");
    }
    make_private_dir(dir);
    make_private_dir(dir / "tdlib");
    make_private_dir(dir / "files");

    td::TdlibParameters params;
    params.use_test_dc = options_.use_test_dc;
    if (!options_.use_test_dc) {
        params.database_encryption_key = load_or_create_db_key(options_);
    }
    params.database_directory = (dir / "tdlib").string();
    params.files_directory = (dir / "files").string();
    params.api_id = c.api_id;
    params.api_hash = c.api_hash;
    params.application_version = std::string{telega::app::version()};

    td::AuthOptions auth;
    auth.mode = account == Account::kBot ? td::AuthMode::kBot : td::AuthMode::kUser;
    AutoPrompter prompter(options_, c, progress);

    progress << "logging in " << account_name(account)
             << (options_.use_test_dc ? " on the test DC\n" : "\n") << std::flush;
    auto account_ptr =
        std::unique_ptr<BenchAccount>(new BenchAccount(account, runtime().create_client()));
    td::authorize(account_ptr->client(), params, prompter, auth);
    return account_ptr;
}

}  // namespace telega::bench
