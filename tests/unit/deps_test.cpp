// Дымовые проверки остальных зависимостей ядра: подключаются и работают.

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <sodium.h>
#include <toml++/toml.hpp>
#include <zstd.h>

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "app/version.h"

TEST(Deps, SodiumInit) { EXPECT_GE(sodium_init(), 0); }

TEST(Deps, ZstdRoundTrip) {
    const std::string input(100000, 'x');
    std::vector<char> compressed(ZSTD_compressBound(input.size()));
    const size_t csize =
        ZSTD_compress(compressed.data(), compressed.size(), input.data(), input.size(), 3);
    ASSERT_EQ(ZSTD_isError(csize), 0U);
    ASSERT_LT(csize, input.size());

    std::string output(input.size(), '\0');
    const size_t dsize = ZSTD_decompress(output.data(), output.size(), compressed.data(), csize);
    ASSERT_EQ(ZSTD_isError(dsize), 0U);
    EXPECT_EQ(output, input);
}

TEST(Deps, TomlParse) {
    const auto table = toml::parse(R"(
api_id = 1234567
[profiles.main]
auth = "bot"
)");
    EXPECT_EQ(table["api_id"].value<int64_t>(), 1234567);
    EXPECT_EQ(table["profiles"]["main"]["auth"].value<std::string>(), "bot");
}

TEST(Deps, JsonParse) {
    const auto reply = nlohmann::json::parse(R"({"proto": 1, "ok": true})");
    EXPECT_EQ(reply.at("proto").get<int>(), 1);
    EXPECT_TRUE(reply.at("ok").get<bool>());
}

TEST(Deps, Version) {
    EXPECT_EQ(telega::app::version(), TELEGA_FS_EXPECTED_VERSION);
    EXPECT_NE(telega::app::core_library_versions().find("SQLite 3."), std::string::npos);
}
