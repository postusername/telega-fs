#include "common/bytes.h"

namespace telega {

std::string to_hex(ByteSpan data) {
    static constexpr std::string_view kDigits = "0123456789abcdef";
    std::string out;
    out.reserve(data.size() * 2);
    for (const std::byte b : data) {
        const auto v = static_cast<unsigned>(b);
        out.push_back(kDigits[v >> 4U]);
        out.push_back(kDigits[v & 0xFU]);
    }
    return out;
}

}  // namespace telega
