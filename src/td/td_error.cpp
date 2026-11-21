#include "td/td_error.h"

#include <cctype>
#include <limits>
#include <utility>

namespace telega::td {

namespace {

// Число из цифр, начиная с pos. nullopt, если цифр нет. Слишком большие значения обрезаются.
std::optional<int64_t> digits_at(std::string_view s, size_t pos) {
    if (pos >= s.size() || std::isdigit(static_cast<unsigned char>(s[pos])) == 0) {
        return std::nullopt;
    }
    int64_t value = 0;
    constexpr int64_t kCap = 10'000'000;  // сутки с запасом; защита от переполнения
    while (pos < s.size() && std::isdigit(static_cast<unsigned char>(s[pos])) != 0) {
        value = value * 10 + (s[pos] - '0');
        if (value > kCap) {
            return kCap;
        }
        ++pos;
    }
    return value;
}

std::optional<int64_t> number_after(std::string_view message, std::string_view marker) {
    const size_t at = message.find(marker);
    if (at == std::string_view::npos) {
        return std::nullopt;
    }
    return digits_at(message, at + marker.size());
}

}  // namespace

std::optional<std::chrono::seconds> parse_retry_after(std::string_view message) {
    for (const std::string_view marker : {"FLOOD_WAIT_", "FLOOD_PREMIUM_WAIT_", "retry after "}) {
        if (const auto n = number_after(message, marker)) {
            return std::chrono::seconds{*n};
        }
    }
    return std::nullopt;
}

TdError TdError::make(int code, std::string message) {
    TdError e;
    e.code = code;
    e.retry_after = parse_retry_after(message);
    e.message = std::move(message);
    return e;
}

TdError TdError::from_json(const Json& error_object) {
    if (!error_object.is_object() || !error_object.contains("code") ||
        !error_object["code"].is_number_integer()) {
        return make(kUnexpected, "malformed error object");
    }
    const auto code = error_object["code"].get<int64_t>();
    const bool fits =
        code >= std::numeric_limits<int>::min() && code <= std::numeric_limits<int>::max();
    std::string message;
    if (error_object.contains("message") && error_object["message"].is_string()) {
        message = error_object["message"].get<std::string>();
    }
    return make(fits ? static_cast<int>(code) : kUnexpected, std::move(message));
}

std::string TdError::to_string() const {
    std::string s = std::to_string(code) + ": " + message;
    if (retry_after) {
        s += " (retry after " + std::to_string(retry_after->count()) + " s)";
    }
    return s;
}

TdException::TdException(TdError error)
    : std::runtime_error("TDLib error " + error.to_string()), error_(std::move(error)) {}

}  // namespace telega::td
