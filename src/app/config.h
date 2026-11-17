#pragma once

// config.toml, профили и пути на диске (SPEC 12.1, 12.2).

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

#include "common/files.h"

namespace telega::app {

// Ошибка конфигурации: неверный файл, неизвестный ключ, неверное значение. Текст для пользователя.
class ConfigError : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

enum class StoreKind : uint8_t { telegram, local };
enum class AuthMode : uint8_t { bot, user };

struct Profile {
    std::string name;

    StoreKind store = StoreKind::telegram;
    std::filesystem::path local_dir;  // только при store = local
    AuthMode auth = AuthMode::bot;
    std::optional<int64_t> chat_id;
    bool use_test_dc = false;
    std::filesystem::path keyfile;  // пусто — спрашивать пароль

    uint64_t cache_limit = uint64_t{4} << 30U;
    uint64_t staging_limit = uint64_t{8} << 30U;
    std::chrono::milliseconds commit_interval{60'000};
    uint64_t commit_every_chunks = 64;
    uint32_t max_chain = 16;
    uint64_t patch_take_rows = 2000;
    double checkpoint_ratio = 0.25;
    uint32_t retain_generations = 8;
    uint32_t send_rate = 20;
    uint32_t upload_concurrency = 2;
    uint32_t download_concurrency = 4;
    std::chrono::milliseconds seal_delay{5'000};
    std::chrono::milliseconds staging_full_timeout{300'000};
    std::chrono::milliseconds read_timeout{60'000};
    std::chrono::milliseconds sync_timeout{300'000};
    uint64_t plain_cache = uint64_t{64} << 20U;

    // Задаются при init и дальше берутся из index.sqlite (meta), а не из конфига.
    uint64_t chunk_size = uint64_t{16} << 20U;
    uint64_t inline_max = uint64_t{4} << 10U;
    uint64_t inline_budget = uint64_t{16} << 20U;
    uint64_t pack_threshold = uint64_t{1} << 20U;
    uint64_t pack_target = uint64_t{16} << 20U;
    std::chrono::milliseconds pack_max_age{30'000};
};

struct Config {
    std::optional<int64_t> api_id;        // или TELEGA_API_ID
    std::optional<std::string> api_hash;  // или TELEGA_API_HASH; в логи не пишется
    std::map<std::string, Profile, std::less<>> profiles;

    // Профиль по имени; ConfigError, если его нет.
    [[nodiscard]] const Profile& profile(std::string_view name) const;
};

// "16MiB", "4KiB", "1GiB", "512", "8M" (M = MiB) → байты. ConfigError при ошибке.
uint64_t parse_size(std::string_view text);
// "60s", "500ms", "5m", "1h" → миллисекунды. ConfigError при ошибке.
std::chrono::milliseconds parse_duration(std::string_view text);

// Разобрать TOML. Неизвестные ключи — ошибка (опечатки не должны молча игнорироваться).
// Переменные окружения TELEGA_API_ID и TELEGA_API_HASH перекрывают файл.
Config parse_config(std::string_view toml_text, std::string_view source_name = "config.toml");
// Прочитать файл. Если файла нет — пустой Config (профилей нет). Файл, доступный группе
// или остальным, — ConfigError (в нём api_hash, SPEC 12.1).
Config load_config(const std::filesystem::path& path);

// Каталоги XDG (SPEC 12.1): XDG_CONFIG_HOME, XDG_DATA_HOME, XDG_CACHE_HOME, иначе от $HOME.
std::filesystem::path default_config_path();

struct ProfilePaths {
    std::filesystem::path data_dir;   // ~/.local/share/telega-fs/<profile>
    std::filesystem::path cache_dir;  // ~/.cache/telega-fs/<profile>

    [[nodiscard]] std::filesystem::path keyring() const { return data_dir / "keyring"; }
    [[nodiscard]] std::filesystem::path index_db() const { return data_dir / "index.sqlite"; }
    [[nodiscard]] std::filesystem::path state_db() const { return data_dir / "state.sqlite"; }
    [[nodiscard]] std::filesystem::path staging() const { return data_dir / "staging"; }
    [[nodiscard]] std::filesystem::path tmp() const { return data_dir / "tmp"; }
    [[nodiscard]] std::filesystem::path tdlib() const { return data_dir / "tdlib"; }
    [[nodiscard]] std::filesystem::path td_log() const { return data_dir / "td.log"; }
    [[nodiscard]] std::filesystem::path lock() const { return data_dir / "lock"; }
    [[nodiscard]] std::filesystem::path ctl_sock() const { return data_dir / "ctl.sock"; }
    [[nodiscard]] std::filesystem::path tdfiles() const { return cache_dir / "tdfiles"; }
};

ProfilePaths profile_paths(std::string_view profile);

// Создать data_dir (0700), staging/, tmp/ и cache_dir, если их нет. Права существующего
// data_dir приводятся к 0700.
void ensure_profile_dirs(const ProfilePaths& paths);

// flock(LOCK_EX | LOCK_NB) на файл lock на всё время монтирования или обслуживающей команды
// (SPEC 12.1). Занят — ConfigError «профиль уже используется».
class ProfileLock {
  public:
    explicit ProfileLock(const std::filesystem::path& lock_file);

  private:
    Fd fd_;
};

}  // namespace telega::app
