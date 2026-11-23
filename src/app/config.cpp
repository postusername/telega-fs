#include "app/config.h"

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <toml++/toml.hpp>

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <cmath>
#include <cstdlib>
#include <functional>
#include <limits>
#include <sstream>
#include <system_error>
#include <unordered_map>
#include <utility>

namespace telega::app {

namespace fs = std::filesystem;

namespace {

std::string_view trim(std::string_view s) {
    while (!s.empty() && (s.front() == ' ' || s.front() == '\t')) {
        s.remove_prefix(1);
    }
    while (!s.empty() && (s.back() == ' ' || s.back() == '\t')) {
        s.remove_suffix(1);
    }
    return s;
}

// Число и суффикс: "16MiB" → {16, "MiB"}.
std::pair<uint64_t, std::string_view> split_number(std::string_view text, std::string_view what) {
    text = trim(text);
    uint64_t value = 0;
    const auto* begin = text.data();
    const auto* end = text.data() + text.size();
    const auto [ptr, ec] = std::from_chars(begin, end, value);
    if (ec != std::errc{} || ptr == begin) {
        throw ConfigError("invalid " + std::string(what) + ": '" + std::string(text) + "'");
    }
    return {value, trim(std::string_view(ptr, static_cast<size_t>(end - ptr)))};
}

uint64_t checked_mul(uint64_t value, uint64_t factor, std::string_view text) {
    if (factor != 0 && value > UINT64_MAX / factor) {
        throw ConfigError("value is too large: '" + std::string(text) + "'");
    }
    return value * factor;
}

std::string env_or_empty(const char* name) {
    const char* v = std::getenv(name);  // NOLINT(concurrency-mt-unsafe): читается при старте
    return v != nullptr ? std::string(v) : std::string();
}

fs::path xdg_dir(const char* var, const char* fallback_under_home) {
    if (auto v = env_or_empty(var); !v.empty() && fs::path(v).is_absolute()) {
        return v;
    }
    const auto home = env_or_empty("HOME");
    if (home.empty()) {
        throw ConfigError(std::string("neither ") + var + " nor HOME is set");
    }
    return fs::path(home) / fallback_under_home;
}

// Значение ключа как размер: строка "16MiB" или целое число байт.
uint64_t node_size(const toml::node& node, std::string_view key) {
    if (const auto* s = node.as_string()) {
        return parse_size(s->get());
    }
    if (const auto* i = node.as_integer(); i != nullptr && i->get() >= 0) {
        return static_cast<uint64_t>(i->get());
    }
    throw ConfigError("'" + std::string(key) + "' must be a size like \"16MiB\"");
}

std::chrono::milliseconds node_duration(const toml::node& node, std::string_view key) {
    if (const auto* s = node.as_string()) {
        return parse_duration(s->get());
    }
    throw ConfigError("'" + std::string(key) + "' must be a duration like \"60s\"");
}

int64_t node_int(const toml::node& node, std::string_view key) {
    if (const auto* i = node.as_integer()) {
        return i->get();
    }
    throw ConfigError("'" + std::string(key) + "' must be an integer");
}

template <typename T>
T node_uint(const toml::node& node, std::string_view key) {
    const int64_t v = node_int(node, key);
    if (v < 0 || static_cast<uint64_t>(v) > std::numeric_limits<T>::max()) {
        throw ConfigError("'" + std::string(key) + "' is out of range");
    }
    return static_cast<T>(v);
}

std::string node_string(const toml::node& node, std::string_view key) {
    if (const auto* s = node.as_string()) {
        return s->get();
    }
    throw ConfigError("'" + std::string(key) + "' must be a string");
}

bool node_bool(const toml::node& node, std::string_view key) {
    if (const auto* b = node.as_boolean()) {
        return b->get();
    }
    throw ConfigError("'" + std::string(key) + "' must be true or false");
}

double node_double(const toml::node& node, std::string_view key) {
    if (const auto* d = node.as_floating_point()) {
        return d->get();
    }
    if (const auto* i = node.as_integer()) {
        return static_cast<double>(i->get());
    }
    throw ConfigError("'" + std::string(key) + "' must be a number");
}

using ProfileSetter = std::function<void(Profile&, const toml::node&, std::string_view)>;

const std::unordered_map<std::string_view, ProfileSetter>& profile_setters() {
    static const std::unordered_map<std::string_view, ProfileSetter> setters = {
        {"store",
         [](Profile& p, const toml::node& n, std::string_view k) {
             const auto v = node_string(n, k);
             if (v == "telegram") {
                 p.store = StoreKind::telegram;
             } else if (v == "local") {
                 p.store = StoreKind::local;
             } else {
                 throw ConfigError(R"('store' must be "telegram" or "local")");
             }
         }},
        {"local_dir", [](Profile& p, const toml::node& n,
                         std::string_view k) { p.local_dir = node_string(n, k); }},
        {"auth",
         [](Profile& p, const toml::node& n, std::string_view k) {
             const auto v = node_string(n, k);
             if (v == "bot") {
                 p.auth = AuthMode::bot;
             } else if (v == "user") {
                 p.auth = AuthMode::user;
             } else {
                 throw ConfigError(R"('auth' must be "bot" or "user")");
             }
         }},
        {"chat_id",
         [](Profile& p, const toml::node& n, std::string_view k) { p.chat_id = node_int(n, k); }},
        {"use_test_dc", [](Profile& p, const toml::node& n,
                           std::string_view k) { p.use_test_dc = node_bool(n, k); }},
        {"keyfile", [](Profile& p, const toml::node& n,
                       std::string_view k) { p.keyfile = node_string(n, k); }},
        {"cache_limit", [](Profile& p, const toml::node& n,
                           std::string_view k) { p.cache_limit = node_size(n, k); }},
        {"staging_limit", [](Profile& p, const toml::node& n,
                             std::string_view k) { p.staging_limit = node_size(n, k); }},
        {"staging_high_watermark",
         [](Profile& p, const toml::node& n, std::string_view k) {
             // Пустая строка — значение по умолчанию (75 % от staging_limit).
             if (const auto* str = n.as_string(); str != nullptr && trim(str->get()).empty()) {
                 p.staging_high_watermark.reset();
             } else {
                 p.staging_high_watermark = node_size(n, k);
             }
         }},
        {"commit_interval", [](Profile& p, const toml::node& n,
                               std::string_view k) { p.commit_interval = node_duration(n, k); }},
        {"commit_every_chunks",
         [](Profile& p, const toml::node& n, std::string_view k) {
             p.commit_every_chunks = node_uint<uint64_t>(n, k);
         }},
        {"patch_take_rows",
         [](Profile& p, const toml::node& n, std::string_view k) {
             p.patch_take_rows = node_uint<uint64_t>(n, k);
         }},
        {"max_chain", [](Profile& p, const toml::node& n,
                         std::string_view k) { p.max_chain = node_uint<uint32_t>(n, k); }},
        {"checkpoint_ratio", [](Profile& p, const toml::node& n,
                                std::string_view k) { p.checkpoint_ratio = node_double(n, k); }},
        {"retain_generations",
         [](Profile& p, const toml::node& n, std::string_view k) {
             p.retain_generations = node_uint<uint32_t>(n, k);
         }},
        {"send_rate", [](Profile& p, const toml::node& n,
                         std::string_view k) { p.send_rate = node_uint<uint32_t>(n, k); }},
        {"send_bytes_rate", [](Profile& p, const toml::node& n,
                               std::string_view k) { p.send_bytes_rate = node_size(n, k); }},
        {"album_max_doc_size", [](Profile& p, const toml::node& n,
                                  std::string_view k) { p.album_max_doc_size = node_size(n, k); }},
        {"read_pause", [](Profile& p, const toml::node& n,
                          std::string_view k) { p.read_pause = node_duration(n, k); }},
        {"upload_concurrency",
         [](Profile& p, const toml::node& n, std::string_view k) {
             p.upload_concurrency = node_uint<uint32_t>(n, k);
         }},
        {"download_concurrency",
         [](Profile& p, const toml::node& n, std::string_view k) {
             p.download_concurrency = node_uint<uint32_t>(n, k);
         }},
        {"seal_delay", [](Profile& p, const toml::node& n,
                          std::string_view k) { p.seal_delay = node_duration(n, k); }},
        {"staging_full_timeout",
         [](Profile& p, const toml::node& n, std::string_view k) {
             p.staging_full_timeout = node_duration(n, k);
         }},
        {"sync_timeout", [](Profile& p, const toml::node& n,
                            std::string_view k) { p.sync_timeout = node_duration(n, k); }},
        {"read_timeout", [](Profile& p, const toml::node& n,
                            std::string_view k) { p.read_timeout = node_duration(n, k); }},
        {"plain_cache", [](Profile& p, const toml::node& n,
                           std::string_view k) { p.plain_cache = node_size(n, k); }},
        {"readahead", [](Profile& p, const toml::node& n,
                         std::string_view k) { p.readahead = node_size(n, k); }},
        {"readahead_max", [](Profile& p, const toml::node& n,
                             std::string_view k) { p.readahead_max = node_size(n, k); }},
        {"compact_ratio", [](Profile& p, const toml::node& n,
                             std::string_view k) { p.compact_ratio = node_double(n, k); }},
        {"chunk_size", [](Profile& p, const toml::node& n,
                          std::string_view k) { p.chunk_size = node_size(n, k); }},
        {"inline_max", [](Profile& p, const toml::node& n,
                          std::string_view k) { p.inline_max = node_size(n, k); }},
        {"inline_budget", [](Profile& p, const toml::node& n,
                             std::string_view k) { p.inline_budget = node_size(n, k); }},
        {"pack_threshold", [](Profile& p, const toml::node& n,
                              std::string_view k) { p.pack_threshold = node_size(n, k); }},
        {"pack_target", [](Profile& p, const toml::node& n,
                           std::string_view k) { p.pack_target = node_size(n, k); }},
        {"pack_max_age", [](Profile& p, const toml::node& n,
                            std::string_view k) { p.pack_max_age = node_duration(n, k); }},
    };
    return setters;
}

void validate(const Profile& p) {
    const auto where = "profile '" + p.name + "': ";
    if (p.store == StoreKind::local && p.local_dir.empty()) {
        throw ConfigError(where + "store = \"local\" requires local_dir");
    }
    if (p.store == StoreKind::local && !p.local_dir.is_absolute()) {
        throw ConfigError(where + "local_dir must be an absolute path");
    }
    const uint64_t mib = uint64_t{1} << 20U;
    if (p.chunk_size < mib || p.chunk_size > 256 * mib ||
        (p.chunk_size & (p.chunk_size - 1)) != 0) {
        throw ConfigError(where + "chunk_size must be a power of two from 1MiB to 256MiB");
    }
    if (p.pack_threshold > p.chunk_size) {
        throw ConfigError(where + "pack_threshold must not exceed chunk_size");
    }
    if (p.pack_target > p.chunk_size) {
        throw ConfigError(where + "pack_target must not exceed chunk_size");
    }
    if (p.pack_target < 2 * p.pack_threshold) {
        throw ConfigError(where + "pack_target must be at least 2 * pack_threshold");
    }
    if (p.inline_max > p.pack_threshold) {
        throw ConfigError(where + "inline_max must not exceed pack_threshold");
    }
    if (p.checkpoint_ratio <= 0.0) {
        throw ConfigError(where + "checkpoint_ratio must be positive");
    }
    if (std::isnan(p.compact_ratio) || p.compact_ratio <= 0.0 || p.compact_ratio > 1.0) {
        throw ConfigError(where + "compact_ratio must be in (0, 1]");
    }
    if (p.readahead > p.readahead_max) {
        throw ConfigError(where + "readahead must not exceed readahead_max");
    }
    if (p.staging_limit == 0) {
        throw ConfigError(where + "staging_limit must be positive");
    }
    if (p.staging_high_watermark &&
        (*p.staging_high_watermark == 0 || *p.staging_high_watermark > p.staging_limit)) {
        throw ConfigError(where +
                          "staging_high_watermark must be positive and not exceed staging_limit");
    }
    if (p.staging_full_timeout <= std::chrono::milliseconds{0}) {
        throw ConfigError(where + "staging_full_timeout must be positive");
    }
    if (p.upload_concurrency == 0 || p.download_concurrency == 0) {
        throw ConfigError(where + "concurrency must be at least 1");
    }
    if (p.send_rate == 0) {
        throw ConfigError(where + "send_rate must be at least 1");
    }
    if (p.send_bytes_rate == 0) {
        throw ConfigError(where + "send_bytes_rate must be positive");
    }
    if (p.read_pause <= std::chrono::milliseconds{0}) {
        throw ConfigError(where + "read_pause must be positive");
    }
}

}  // namespace

const Profile& Config::profile(std::string_view name) const {
    const auto it = profiles.find(name);
    if (it == profiles.end()) {
        throw ConfigError("profile '" + std::string(name) + "' is not defined in config.toml");
    }
    return it->second;
}

uint64_t parse_size(std::string_view text) {
    const auto [value, suffix] = split_number(text, "size");
    static constexpr std::array<std::pair<std::string_view, unsigned>, 10> kUnits = {{
        {"", 0},
        {"B", 0},
        {"K", 10},
        {"KiB", 10},
        {"M", 20},
        {"MiB", 20},
        {"G", 30},
        {"GiB", 30},
        {"T", 40},
        {"TiB", 40},
    }};
    for (const auto& [name, shift] : kUnits) {
        if (suffix == name) {
            return checked_mul(value, uint64_t{1} << shift, text);
        }
    }
    throw ConfigError("unknown size unit in '" + std::string(text) + "' (use KiB, MiB, GiB)");
}

std::chrono::milliseconds parse_duration(std::string_view text) {
    const auto [value, suffix] = split_number(text, "duration");
    static constexpr std::array<std::pair<std::string_view, uint64_t>, 4> kUnits = {{
        {"ms", 1},
        {"s", 1000},
        {"m", 60'000},
        {"h", 3'600'000},
    }};
    for (const auto& [name, factor] : kUnits) {
        if (suffix == name) {
            const uint64_t ms = checked_mul(value, factor, text);
            if (ms > static_cast<uint64_t>(std::chrono::milliseconds::max().count())) {
                throw ConfigError("duration is too large: '" + std::string(text) + "'");
            }
            return std::chrono::milliseconds(static_cast<int64_t>(ms));
        }
    }
    throw ConfigError("unknown duration unit in '" + std::string(text) + "' (use ms, s, m, h)");
}

Config parse_config(std::string_view toml_text, std::string_view source_name) {
    toml::table root;
    try {
        root = toml::parse(toml_text, source_name);
    } catch (const toml::parse_error& e) {
        std::ostringstream msg;
        msg << source_name << ":" << e.source().begin.line << ": " << e.description();
        throw ConfigError(msg.str());
    }

    Config config;
    for (const auto& [key, node] : root) {
        const std::string_view k = key.str();
        if (k == "api_id") {
            config.api_id = node_int(node, k);
        } else if (k == "api_hash") {
            config.api_hash = node_string(node, k);
        } else if (k == "profiles") {
            const auto* profiles = node.as_table();
            if (profiles == nullptr) {
                throw ConfigError("'profiles' must be a table");
            }
            for (const auto& [pname, pnode] : *profiles) {
                const auto* ptable = pnode.as_table();
                if (ptable == nullptr) {
                    throw ConfigError("profile '" + std::string(pname.str()) + "' must be a table");
                }
                Profile profile;
                profile.name = std::string(pname.str());
                for (const auto& [okey, onode] : *ptable) {
                    const auto& setters = profile_setters();
                    const auto it = setters.find(okey.str());
                    if (it == setters.end()) {
                        throw ConfigError("profile '" + profile.name + "': unknown key '" +
                                          std::string(okey.str()) + "'");
                    }
                    it->second(profile, onode, okey.str());
                }
                // Значения по умолчанию pack_* зависят от chunk_size (SPEC 6.5: pack_threshold
                // не больше chunk_size, pack_target не больше chunk_size и не меньше
                // 2 * pack_threshold). При chunk_size 8MiB это ровно 4MiB и 8MiB из SPEC 12.2.
                if (!ptable->contains("pack_threshold")) {
                    profile.pack_threshold =
                        std::min(profile.pack_threshold, profile.chunk_size / 2);
                }
                if (!ptable->contains("pack_target")) {
                    profile.pack_target = std::min(profile.pack_target, profile.chunk_size);
                }
                validate(profile);
                config.profiles.emplace(profile.name, std::move(profile));
            }
        } else {
            throw ConfigError("unknown key '" + std::string(k) + "' in " +
                              std::string(source_name));
        }
    }

    if (const auto id = env_or_empty("TELEGA_API_ID"); !id.empty()) {
        int64_t value = 0;
        const auto [ptr, ec] = std::from_chars(id.data(), id.data() + id.size(), value);
        if (ec != std::errc{} || ptr != id.data() + id.size()) {
            throw ConfigError("TELEGA_API_ID must be an integer");
        }
        config.api_id = value;
    }
    if (auto hash = env_or_empty("TELEGA_API_HASH"); !hash.empty()) {
        config.api_hash = std::move(hash);
    }
    return config;
}

Config load_config(const fs::path& path) {
    std::error_code ec;
    const auto status = fs::status(path, ec);
    if (ec || !fs::exists(status)) {
        return parse_config("", path.string());
    }
    const auto perms = status.permissions();
    if ((perms & (fs::perms::group_all | fs::perms::others_all)) != fs::perms::none) {
        throw ConfigError(path.string() + " must not be accessible by group or others (chmod 600)");
    }
    const Bytes raw = read_file(path);
    const std::string text(reinterpret_cast<const char*>(raw.data()), raw.size());
    return parse_config(text, path.string());
}

fs::path default_config_path() {
    return xdg_dir("XDG_CONFIG_HOME", ".config") / "telega-fs" / "config.toml";
}

ProfilePaths profile_paths(std::string_view profile) {
    if (profile.empty() || profile.find('/') != std::string_view::npos || profile == "." ||
        profile == "..") {
        throw ConfigError("invalid profile name '" + std::string(profile) + "'");
    }
    return ProfilePaths{
        .data_dir = xdg_dir("XDG_DATA_HOME", ".local/share") / "telega-fs" / profile,
        .cache_dir = xdg_dir("XDG_CACHE_HOME", ".cache") / "telega-fs" / profile,
    };
}

void ensure_profile_dirs(const ProfilePaths& paths) {
    fs::create_directories(paths.data_dir);
    fs::permissions(paths.data_dir, fs::perms::owner_all, fs::perm_options::replace);
    fs::create_directories(paths.staging());
    fs::create_directories(paths.tmp());
    fs::create_directories(paths.cache_dir);
}

ProfileLock::ProfileLock(const fs::path& lock_file)
    : fd_(Fd::open(lock_file, O_RDWR | O_CREAT, 0600)) {
    if (::flock(fd_.get(), LOCK_EX | LOCK_NB) != 0) {
        if (errno == EWOULDBLOCK) {
            throw ConfigError("profile is in use by another telega-fs process (" +
                              lock_file.string() + " is locked)");
        }
        throw SysError(errno, "flock " + lock_file.string());
    }
}

}  // namespace telega::app
