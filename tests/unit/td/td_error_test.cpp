// TdError: разбор ответа error и retry_after.

#include "td/td_error.h"

#include <gtest/gtest.h>

#include <optional>

namespace telega::td {
namespace {

using std::chrono::seconds;

TEST(TdError, ParsesTooManyRequests) {
    const auto e = TdError::from_json(Json::parse(
        R"({"@type":"error","code":429,"message":"Too Many Requests: retry after 17"})"));
    EXPECT_EQ(e.code, 429);
    EXPECT_EQ(e.message, "Too Many Requests: retry after 17");
    ASSERT_TRUE(e.retry_after.has_value());
    EXPECT_EQ(e.retry_after, std::optional<seconds>{seconds{17}});
    EXPECT_TRUE(e.is_rate_limit());
    EXPECT_FALSE(e.is_local());
}

TEST(TdError, ParsesFloodWait) {
    const auto e = TdError::make(420, "FLOOD_WAIT_300");
    ASSERT_TRUE(e.retry_after.has_value());
    EXPECT_EQ(e.retry_after, std::optional<seconds>{seconds{300}});
    EXPECT_TRUE(e.is_rate_limit());

    const auto embedded = TdError::make(400, "Failed: FLOOD_WAIT_5 (while sending)");
    ASSERT_TRUE(embedded.retry_after.has_value());
    EXPECT_EQ(embedded.retry_after, std::optional<seconds>{seconds{5}});

    const auto premium = TdError::make(420, "FLOOD_PREMIUM_WAIT_12");
    ASSERT_TRUE(premium.retry_after.has_value());
    EXPECT_EQ(premium.retry_after, std::optional<seconds>{seconds{12}});
}

TEST(TdError, OrdinaryErrorsHaveNoRetryAfter) {
    const auto e = TdError::make(400, "CHAT_ADMIN_REQUIRED");
    EXPECT_FALSE(e.retry_after.has_value());
    EXPECT_FALSE(e.is_rate_limit());
    EXPECT_FALSE(TdError::make(400, "FLOOD_WAIT_").retry_after.has_value());
    EXPECT_FALSE(TdError::make(400, "retry after soon").retry_after.has_value());
}

TEST(TdError, RateLimitWithoutNumberIsStillRateLimit) {
    const auto e = TdError::make(429, "Too Many Requests");
    EXPECT_TRUE(e.is_rate_limit());
    EXPECT_FALSE(e.retry_after.has_value());
}

TEST(TdError, HugeNumberIsCapped) {
    const auto e = TdError::make(429, "FLOOD_WAIT_99999999999999999999");
    ASSERT_TRUE(e.retry_after.has_value());
    EXPECT_EQ(e.retry_after, std::optional<seconds>{seconds{10'000'000}});
}

TEST(TdError, MalformedObject) {
    EXPECT_EQ(TdError::from_json(Json::parse(R"({"@type":"error"})")).code, TdError::kUnexpected);
    EXPECT_EQ(TdError::from_json(Json::parse("[1]")).code, TdError::kUnexpected);
    EXPECT_EQ(TdError::from_json(Json::parse(R"({"code":99999999999,"message":"x"})")).code,
              TdError::kUnexpected);
    // Нет message: ошибка есть, текст пустой.
    const auto e = TdError::from_json(Json::parse(R"({"code":400})"));
    EXPECT_EQ(e.code, 400);
    EXPECT_TRUE(e.message.empty());
}

TEST(TdError, ExceptionCarriesError) {
    try {
        throw TdException(TdError::make(429, "retry after 3"));
    } catch (const TdException& e) {
        EXPECT_EQ(e.error().code, 429);
        EXPECT_EQ(e.error().retry_after, seconds{3});
        EXPECT_NE(std::string{e.what()}.find("429"), std::string::npos);
    }
}

TEST(TdError, LocalCodes) {
    EXPECT_TRUE(TdError::make(TdError::kTimeout, "t").is_timeout());
    EXPECT_TRUE(TdError::make(TdError::kStopped, "s").is_local());
    EXPECT_TRUE(TdError::make(TdError::kClosed, "c").is_local());
}

}  // namespace
}  // namespace telega::td
