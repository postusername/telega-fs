// config.toml, размеры и интервалы, XDG-пути, flock профиля (SPEC 12).

#include "app/config.h"

#include <gtest/gtest.h>
#include <stdlib.h>  // NOLINT(modernize-deprecated-headers): setenv/unsetenv только в <stdlib.h>
#include <unistd.h>

#include <filesystem>
#include <string>

#include "common/files.h"

namespace telega::app {
namespace {

namespace fs = std::filesystem;
using namespace std::chrono_literals;

// Тесты однопоточные, поэтому setenv/unsetenv здесь безопасны.
void set_env(const char* name, const std::string& value) {
    ::setenv(name, value.c_str(), 1);  // NOLINT(concurrency-mt-unsafe)
}
void unset_env(const char* name) {
    ::unsetenv(name);  // NOLINT(concurrency-mt-unsafe)
}

TEST(ConfigParse, Sizes) {
    EXPECT_EQ(parse_size("16MiB"), uint64_t{16} << 20U);
    EXPECT_EQ(parse_size("4KiB"), 4096U);
    EXPECT_EQ(parse_size("8M"), uint64_t{8} << 20U);
    EXPECT_EQ(parse_size("512"), 512U);
    EXPECT_EQ(parse_size(" 1 GiB "), uint64_t{1} << 30U);
    EXPECT_THROW((void)parse_size("MiB"), ConfigError);
    EXPECT_THROW((void)parse_size("5XB"), ConfigError);
    EXPECT_THROW((void)parse_size("99999999999999999999TiB"), ConfigError);
    EXPECT_THROW((void)parse_size("17000000TiB"), ConfigError);
}

TEST(ConfigParse, Durations) {
    EXPECT_EQ(parse_duration("60s"), 60s);
    EXPECT_EQ(parse_duration("500ms"), 500ms);
    EXPECT_EQ(parse_duration("5m"), 5min);
    EXPECT_EQ(parse_duration("1h"), 1h);
    EXPECT_THROW((void)parse_duration("10"), ConfigError);
    EXPECT_THROW((void)parse_duration("s"), ConfigError);
}

TEST(ConfigParse, FullProfile) {
    unset_env("TELEGA_API_ID");
    unset_env("TELEGA_API_HASH");
    const auto config = parse_config(R"(
api_id   = 1234567
api_hash = "0123456789abcdef0123456789abcdef"

[profiles.main]
auth           = "user"
chat_id        = -1001234567890
use_test_dc    = true
cache_limit    = "1GiB"
commit_interval = "30s"
sync_timeout   = "45s"
max_chain      = 16
patch_take_rows = 500
checkpoint_ratio = 0.5
chunk_size     = "8MiB"

[profiles.dev]
store     = "local"
local_dir = "/tmp/telega-local"
)");
    EXPECT_EQ(config.api_id, 1234567);
    ASSERT_EQ(config.profiles.size(), 2U);
    const auto& main = config.profile("main");
    EXPECT_EQ(main.store, StoreKind::telegram);
    EXPECT_EQ(main.auth, AuthMode::user);
    EXPECT_EQ(main.chat_id, -1001234567890);
    EXPECT_TRUE(main.use_test_dc);
    EXPECT_EQ(main.cache_limit, uint64_t{1} << 30U);
    EXPECT_EQ(main.commit_interval, 30s);
    EXPECT_EQ(main.sync_timeout, 45s);
    EXPECT_EQ(main.max_chain, 16U);
    EXPECT_EQ(main.patch_take_rows, 500U);
    EXPECT_DOUBLE_EQ(main.checkpoint_ratio, 0.5);
    EXPECT_EQ(main.chunk_size, uint64_t{8} << 20U);
    EXPECT_EQ(main.staging_limit, uint64_t{8} << 30U);  // значение по умолчанию

    const auto& dev = config.profile("dev");
    EXPECT_EQ(dev.store, StoreKind::local);
    EXPECT_EQ(dev.patch_take_rows, 2000U);  // значение по умолчанию
    EXPECT_EQ(dev.local_dir, fs::path("/tmp/telega-local"));
    EXPECT_THROW((void)config.profile("missing"), ConfigError);
}

TEST(ConfigParse, Rejects) {
    EXPECT_THROW((void)parse_config("bogus = 1"), ConfigError);
    EXPECT_THROW((void)parse_config("[profiles.a]\nunknown_key = 1"), ConfigError);
    EXPECT_THROW((void)parse_config("[profiles.a]\nstore = \"local\""), ConfigError);
    EXPECT_THROW((void)parse_config("[profiles.a]\nstore = \"local\"\nlocal_dir = \"rel\""),
                 ConfigError);
    EXPECT_THROW((void)parse_config("[profiles.a]\nchunk_size = \"3MiB\""), ConfigError);
    EXPECT_THROW((void)parse_config("[profiles.a]\nchunk_size = \"512KiB\""), ConfigError);
    EXPECT_THROW((void)parse_config("[profiles.a]\nauth = \"robot\""), ConfigError);
    EXPECT_THROW((void)parse_config("[profiles.a]\nmax_chain = -1"), ConfigError);
    EXPECT_THROW((void)parse_config("api_id = "), ConfigError);
}

TEST(ConfigParse, EnvOverridesApiCredentials) {
    set_env("TELEGA_API_ID", "42");
    set_env("TELEGA_API_HASH", "abc");
    const auto config = parse_config("api_id = 1\napi_hash = \"zzz\"");
    EXPECT_EQ(config.api_id, 42);
    EXPECT_EQ(config.api_hash, "abc");
    set_env("TELEGA_API_ID", "x42");
    EXPECT_THROW((void)parse_config(""), ConfigError);
    unset_env("TELEGA_API_ID");
    unset_env("TELEGA_API_HASH");
}

class ConfigFiles : public ::testing::Test {
  protected:
    void SetUp() override {
        root_ = fs::temp_directory_path() / ("telega-config-" + std::to_string(::getpid()));
        fs::remove_all(root_);
        fs::create_directories(root_);
    }
    void TearDown() override {
        unset_env("XDG_CONFIG_HOME");
        unset_env("XDG_DATA_HOME");
        unset_env("XDG_CACHE_HOME");
        fs::remove_all(root_);
    }
    fs::path root_;
};

TEST_F(ConfigFiles, MissingFileIsEmptyConfig) {
    const auto config = load_config(root_ / "nope.toml");
    EXPECT_TRUE(config.profiles.empty());
}

TEST_F(ConfigFiles, RejectsWorldReadableFile) {
    const auto path = root_ / "config.toml";
    write_file_atomic(path, as_bytes(std::string("[profiles.a]\n")), 0644);
    EXPECT_THROW((void)load_config(path), ConfigError);
    fs::permissions(path, fs::perms::owner_read | fs::perms::owner_write,
                    fs::perm_options::replace);
    EXPECT_EQ(load_config(path).profiles.size(), 1U);
}

TEST_F(ConfigFiles, XdgPaths) {
    set_env("XDG_CONFIG_HOME", (root_ / "cfg").string());
    set_env("XDG_DATA_HOME", (root_ / "data").string());
    set_env("XDG_CACHE_HOME", (root_ / "cache").string());
    EXPECT_EQ(default_config_path(), root_ / "cfg" / "telega-fs" / "config.toml");
    const auto paths = profile_paths("main");
    EXPECT_EQ(paths.data_dir, root_ / "data" / "telega-fs" / "main");
    EXPECT_EQ(paths.tdfiles(), root_ / "cache" / "telega-fs" / "main" / "tdfiles");
    EXPECT_THROW((void)profile_paths("../x"), ConfigError);
    EXPECT_THROW((void)profile_paths(""), ConfigError);

    ensure_profile_dirs(paths);
    EXPECT_TRUE(fs::is_directory(paths.staging()));
    EXPECT_TRUE(fs::is_directory(paths.tmp()));
    EXPECT_EQ(fs::status(paths.data_dir).permissions() & fs::perms::all, fs::perms::owner_all);
}

TEST_F(ConfigFiles, ProfileLockIsExclusive) {
    const auto lock = root_ / "lock";
    {
        const ProfileLock first(lock);
        EXPECT_THROW(const ProfileLock second(lock), ConfigError);
    }
    const ProfileLock again(lock);  // освобождён после разрушения первого
}

}  // namespace
}  // namespace telega::app
