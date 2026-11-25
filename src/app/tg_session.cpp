#include "app/tg_session.h"

#include <sodium.h>
#include <spdlog/spdlog.h>
#include <sys/stat.h>

#include <mutex>
#include <string_view>
#include <utility>

#include "app/version.h"

namespace telega::app {

namespace fs = std::filesystem;

namespace {

std::mutex g_factory_mu;
TransportFactory g_factory;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

TransportFactory current_factory() {
    const std::lock_guard<std::mutex> lock(g_factory_mu);
    return g_factory;
}

void make_private_dir(const fs::path& dir) {
    fs::create_directories(dir);
    fs::permissions(dir, fs::perms::owner_all, fs::perm_options::replace);
}

void wipe(std::string& s) {
    if (!s.empty()) {
        sodium_memzero(s.data(), s.size());
    }
    s.clear();
}

}  // namespace

void set_transport_factory(TransportFactory factory) {
    const std::lock_guard<std::mutex> lock(g_factory_mu);
    g_factory = std::move(factory);
}

ApiCredentials api_credentials(const Config& config) {
    if (!config.api_id || !config.api_hash || config.api_hash->empty()) {
        throw ConfigError(
            "api_id and api_hash are required for Telegram: set them in config.toml or in "
            "TELEGA_API_ID and TELEGA_API_HASH (get them at https://my.telegram.org)");
    }
    if (*config.api_id <= 0 || *config.api_id > INT32_MAX) {
        throw ConfigError("api_id is out of range");
    }
    return ApiCredentials{.api_id = static_cast<int32_t>(*config.api_id),
                          .api_hash = *config.api_hash};
}

TgSession::TgSession(const ProfilePaths& paths, bool use_test_dc, const ApiCredentials& api,
                     const crypto::Key& tdlib_db_key) {
    const TransportFactory factory = current_factory();
    if (!factory) {
        throw std::logic_error("no TDLib transport is configured");
    }
    make_private_dir(paths.data_dir);
    make_private_dir(paths.tdlib());
    make_private_dir(paths.tdfiles());

    // Параметры TDLib (setTdlibParameters).
    params_.use_test_dc = use_test_dc;
    params_.database_directory = paths.tdlib().string();
    params_.files_directory = paths.tdfiles().string();
    params_.database_encryption_key.assign(reinterpret_cast<const char*>(tdlib_db_key.data()),
                                           crypto::Key::kSize);
    params_.use_message_database = false;
    params_.use_secret_chats = false;
    params_.api_id = api.api_id;
    params_.api_hash = api.api_hash;
    params_.device_model = "telega-fs";
    params_.application_version = std::string{version()};

    // Лог один на процесс и ставится до первого клиента: транспорт создаёт фабрика.
    auto transport = factory(paths.td_log());
    runtime_ = std::make_unique<td::TdRuntime>(std::move(transport));
    client_ = runtime_->create_client();
}

TgSession::~TgSession() {
    close();
    wipe(params_.database_encryption_key);
    wipe(params_.api_hash);
}

void TgSession::authorize(td::AuthPrompter& prompter, td::AuthMode mode,
                          std::chrono::milliseconds timeout) {
    td::AuthOptions options;
    options.mode = mode;
    options.timeout = timeout;
    td::authorize(*client_, params_, prompter, options);
}

void TgSession::authorize_existing(std::chrono::milliseconds timeout) {
    NoInteractionPrompter prompter;
    authorize(prompter, td::AuthMode::kUser, timeout);
}

td::Json TgSession::me() { return client_->call(td::Json{{"@type", "getMe"}}); }

bool TgSession::log_out(std::chrono::milliseconds timeout) { return client_->log_out(timeout); }

void TgSession::close() noexcept {
    if (closed_) {
        return;
    }
    closed_ = true;
    try {
        if (client_ && !client_->close(std::chrono::seconds{30})) {
            spdlog::warn("the TDLib client did not close in 30 s");
        }
    } catch (const std::exception& e) {
        spdlog::warn("closing the TDLib client: {}", e.what());
    }
    try {
        if (runtime_) {
            runtime_->stop();  // после close(): клиенты не закрываются при stop
        }
    } catch (const std::exception& e) {
        spdlog::warn("stopping the TDLib runtime: {}", e.what());
    }
}

}  // namespace telega::app
