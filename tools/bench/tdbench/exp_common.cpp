#include "exp_common.h"

#include <algorithm>
#include <array>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <random>
#include <set>
#include <sstream>

#include "common/bytes.h"
#include "common/files.h"

namespace telega::bench {

namespace {

constexpr int64_t kMaxFloodWaitSeconds = 3600;  // SPEC 17.2: больше часа — остановиться
constexpr int kMaxFloodEvents = 5;  // не молотить по лимитам: дальше решает человек

std::string csv_field(const std::string& f) {
    if (f.find_first_of(",\"\n") == std::string::npos) {
        return f;
    }
    std::string out = "\"";
    for (const char c : f) {
        if (c == '"') {
            out += '"';
        }
        out += c;
    }
    out += '"';
    return out;
}

uint64_t splitmix64(uint64_t x) {
    x += 0x9E3779B97F4A7C15ULL;
    x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
    x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;
    return x ^ (x >> 31);
}

std::string read_whole(const std::filesystem::path& path) {
    const std::ifstream in(path, std::ios::binary);
    if (!in) {
        throw std::runtime_error("cannot read " + path.string());
    }
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

void write_text_atomic(const std::filesystem::path& path, const std::string& text) {
    write_file_atomic(path, as_bytes(text), 0600);
}

const std::map<std::string, std::vector<std::string>>& right_names() {
    static const std::map<std::string, std::vector<std::string>> names{
        {"manage_chat", {"can_manage_chat"}},
        {"change_info", {"can_change_info"}},
        {"post", {"can_post_messages"}},
        {"edit", {"can_edit_messages"}},
        {"delete", {"can_delete_messages"}},
        {"invite", {"can_invite_users"}},
        {"restrict", {"can_restrict_members"}},
        {"pin", {"can_pin_messages"}},
        {"promote", {"can_promote_members"}},
        {"video_chats", {"can_manage_video_chats"}},
        {"stories", {"can_post_stories", "can_edit_stories", "can_delete_stories"}},
        {"topics", {"can_manage_topics"}},
    };
    return names;
}

std::vector<std::string> split(const std::string& s, char sep) {
    std::vector<std::string> out;
    std::stringstream ss(s);
    std::string part;
    while (std::getline(ss, part, sep)) {
        if (!part.empty()) {
            out.push_back(part);
        }
    }
    return out;
}

}  // namespace

FloodGuard::FloodGuard(std::ostream& log, std::filesystem::path csv, std::string experiment)
    : log_(log), csv_(std::move(csv)), experiment_(std::move(experiment)) {}

void FloodGuard::on_error(const std::string& what, const td::TdError& error) {
    if (error.code == 401 || error.message.find("USER_DEACTIVATED") != std::string::npos ||
        error.message.find("AUTH_KEY") != std::string::npos) {
        throw StopExperiment("account problem at '" + what + "': " + error.to_string());
    }
    if (!error.is_rate_limit() || !error.retry_after) {
        throw td::TdException(error);
    }
    ++events_;
    const int64_t seconds = error.retry_after->count();
    Csv flood(csv_, {"time_utc", "experiment", "what", "retry_after_s", "event_no"});
    flood.row({iso_utc_now(), experiment_, what, std::to_string(seconds), std::to_string(events_)});
    if (seconds > kMaxFloodWaitSeconds || events_ > kMaxFloodEvents) {
        throw StopExperiment("FLOOD_WAIT of " + std::to_string(seconds) + " s at '" + what +
                             "' (event " + std::to_string(events_) + "): stopping the series");
    }
    const auto wait = std::chrono::seconds{seconds + seconds / 5 + 5};
    log_ << "FLOOD_WAIT " << seconds << " s at '" << what << "': waiting " << wait.count() << " s\n"
         << std::flush;
    waited_s_ += static_cast<double>(wait.count());
    std::this_thread::sleep_for(wait);
}

Csv::Csv(const std::filesystem::path& path, const std::vector<std::string>& header) {
    std::filesystem::create_directories(path.parent_path());
    const bool fresh = !std::filesystem::exists(path) || std::filesystem::file_size(path) == 0;
    out_.open(path, std::ios::app);
    if (!out_) {
        throw std::runtime_error("cannot open " + path.string());
    }
    if (fresh) {
        row(header);
    }
}

void Csv::row(const std::vector<std::string>& fields) {
    bool first = true;
    for (const auto& f : fields) {
        if (!first) {
            out_ << ',';
        }
        first = false;
        out_ << csv_field(f);
    }
    out_ << '\n';
    out_.flush();
}

std::string fmt(double v, int digits) {
    std::ostringstream ss;
    ss << std::fixed << std::setprecision(digits) << v;
    return ss.str();
}

std::string iso_utc_now() {
    const std::time_t t = std::time(nullptr);
    std::tm tm{};
    gmtime_r(&t, &tm);
    std::ostringstream ss;
    ss << std::put_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
    return ss.str();
}

std::string iso_utc_ms(std::chrono::system_clock::time_point t) {
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(t.time_since_epoch());
    const std::time_t secs = static_cast<std::time_t>(ms.count() / 1000);
    std::tm tm{};
    gmtime_r(&secs, &tm);
    std::ostringstream ss;
    ss << std::put_time(&tm, "%Y-%m-%dT%H:%M:%S") << '.' << std::setfill('0') << std::setw(3)
       << (ms.count() % 1000) << 'Z';
    return ss.str();
}

Summary summarize(std::vector<double> values) {
    Summary s;
    s.n = values.size();
    if (values.empty()) {
        return s;
    }
    std::ranges::sort(values);
    s.min = values.front();
    s.max = values.back();
    const size_t mid = values.size() / 2;
    s.median = values.size() % 2 == 1 ? values[mid] : (values[mid - 1] + values[mid]) / 2;
    return s;
}

void fill_seeded(uint64_t seed, uint64_t offset, std::string& out, size_t count) {
    out.resize(count);
    uint64_t word_index = offset / 8;
    uint64_t word = splitmix64(seed ^ (word_index * 0xD6E8FEB86659FD93ULL));
    for (size_t i = 0; i < count; ++i) {
        const uint64_t pos = offset + i;
        if (pos / 8 != word_index) {
            word_index = pos / 8;
            word = splitmix64(seed ^ (word_index * 0xD6E8FEB86659FD93ULL));
        }
        out[i] = static_cast<char>((word >> (8 * (pos % 8))) & 0xFFU);
    }
}

void write_seeded_file(const std::filesystem::path& path, uint64_t seed, uint64_t size) {
    std::filesystem::create_directories(path.parent_path());
    const std::filesystem::path tmp = path.string() + ".tmp";
    {
        std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
        if (!out) {
            throw std::runtime_error("cannot write " + tmp.string());
        }
        constexpr size_t kBlock = 1 << 20;
        std::string block;
        for (uint64_t off = 0; off < size; off += kBlock) {
            const size_t n = static_cast<size_t>(std::min<uint64_t>(kBlock, size - off));
            fill_seeded(seed, off, block, n);
            out.write(block.data(), static_cast<std::streamsize>(n));
        }
        out.flush();
        if (!out) {
            throw std::runtime_error("write failed: " + tmp.string());
        }
    }
    std::filesystem::rename(tmp, path);
}

std::string base64_decode(const std::string& text) {
    static const std::string kAlphabet =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::array<int, 256> table{};
    table.fill(-1);
    for (size_t i = 0; i < kAlphabet.size(); ++i) {
        table[static_cast<unsigned char>(kAlphabet[i])] = static_cast<int>(i);
    }
    std::string out;
    out.reserve(text.size() / 4 * 3);
    uint32_t acc = 0;
    int bits = 0;
    for (const char c : text) {
        const int v = table[static_cast<unsigned char>(c)];
        if (v < 0) {
            continue;  // '=' и пробелы
        }
        acc = (acc << 6) | static_cast<uint32_t>(v);
        bits += 6;
        if (bits >= 8) {
            bits -= 8;
            out += static_cast<char>((acc >> bits) & 0xFFU);
        }
    }
    return out;
}

std::string random_hex(size_t digits) {
    std::random_device rd;
    std::uniform_int_distribution<int> nibble(0, 15);
    std::string s;
    for (size_t i = 0; i < digits; ++i) {
        s += "0123456789abcdef"[nibble(rd)];
    }
    return s;
}

void SendPacer::wait() {
    if (last_) {
        const auto next = *last_ + interval_;
        if (ExpClock::now() < next) {
            std::this_thread::sleep_until(next);
        }
    }
    last_ = ExpClock::now();
}

td::Json load_state(const BenchOptions& options, const std::string& name) {
    const auto path = options.state_dir / (name + ".json");
    if (!std::filesystem::exists(path)) {
        throw std::runtime_error("no state " + path.string() + ", run `" + name + " setup` first");
    }
    return td::Json::parse(read_whole(path));
}

void save_state(const BenchOptions& options, const std::string& name, const td::Json& state) {
    write_text_atomic(options.state_dir / (name + ".json"), state.dump(2) + "\n");
}

bool has_state(const BenchOptions& options, const std::string& name) {
    return std::filesystem::exists(options.state_dir / (name + ".json"));
}

void bump_counter(const BenchOptions& options, const std::string& key, int64_t n) {
    const auto path = options.state_dir / "counters.json";
    td::Json counters =
        std::filesystem::exists(path) ? td::Json::parse(read_whole(path)) : td::Json::object();
    counters[key] = counters.value(key, int64_t{0}) + n;
    write_text_atomic(path, counters.dump(2) + "\n");
}

std::filesystem::path default_out_dir(const std::string& name) {
#ifdef TGFS_REPO_ROOT
    return std::filesystem::path{TGFS_REPO_ROOT} / "docs" / "experiments" / name;
#else
    return std::filesystem::path{"docs"} / "experiments" / name;
#endif
}

td::Json admin_rights_for(const std::string& set_name) {
    std::set<std::string> enabled;  // короткие имена
    const auto& names = right_names();
    if (set_name == "full" || set_name.starts_with("no:")) {
        for (const auto& [short_name, flags] : names) {
            enabled.insert(short_name);
        }
        if (set_name != "full") {
            for (const auto& drop : split(set_name.substr(3), ',')) {
                if (!names.contains(drop)) {
                    throw std::runtime_error("unknown right: " + drop);
                }
                enabled.erase(drop);
            }
        }
    } else if (set_name.starts_with("only:")) {
        for (const auto& keep : split(set_name.substr(5), ',')) {
            if (!names.contains(keep)) {
                throw std::runtime_error("unknown right: " + keep);
            }
            enabled.insert(keep);
        }
    } else {
        throw std::runtime_error("unknown rights set: " + set_name);
    }
    td::Json rights{{"@type", "chatAdministratorRights"}, {"is_anonymous", false}};
    for (const auto& [short_name, flags] : names) {
        for (const auto& flag : flags) {
            rights[flag] = enabled.contains(short_name);
        }
    }
    return rights;
}

std::string rights_summary(const td::Json& rights) {
    std::string out;
    for (const auto& [key, value] : rights.items()) {
        if (key.starts_with("can_") && value.is_boolean() && value.get<bool>()) {
            out += (out.empty() ? "" : ",") + key.substr(4);
        }
    }
    return out.empty() ? "-" : out;
}

td::Json sender_user(int64_t user_id) {
    return td::Json{{"@type", "messageSenderUser"}, {"user_id", user_id}};
}

int64_t create_channel(BenchAccount& user, const std::string& title, FloodGuard& guard,
                       std::ostream& log) {
    const td::Json chat = guard.run("createNewSupergroupChat", [&] {
        return user.client().call(td::Json{{"@type", "createNewSupergroupChat"},
                                           {"title", title},
                                           {"is_forum", false},
                                           {"is_channel", true},
                                           {"description", ""},
                                           {"message_auto_delete_time", 0},
                                           {"for_import", false}});
    });
    const int64_t chat_id = chat.value("id", int64_t{0});
    if (chat_id == 0) {
        throw std::runtime_error("createNewSupergroupChat returned no chat");
    }
    log << "channel created: " << title << "\n";
    return chat_id;
}

void add_bot_admin(BenchAccount& user, BenchAccount& bot, int64_t chat_id,
                   const std::string& rights_set, FloodGuard& guard, std::ostream& log) {
    const td::Json me = bot.me();
    std::string username;
    if (me.contains("usernames") && me["usernames"].contains("active_usernames") &&
        !me["usernames"]["active_usernames"].empty()) {
        username = me["usernames"]["active_usernames"][0].get<std::string>();
    }
    if (username.empty()) {
        throw std::runtime_error("the bot has no username");
    }
    const td::Json bot_chat = guard.run("searchPublicChat", [&] {
        return user.client().call(td::Json{{"@type", "searchPublicChat"}, {"username", username}});
    });
    const int64_t bot_user_id = bot_chat["type"].value("user_id", int64_t{0});
    if (bot_user_id == 0) {
        throw std::runtime_error("cannot resolve the bot for the user account");
    }
    guard.run("setChatMemberStatus", [&] {
        return user.client().call(td::Json{{"@type", "setChatMemberStatus"},
                                           {"chat_id", chat_id},
                                           {"member_id", sender_user(bot_user_id)},
                                           {"status",
                                            {{"@type", "chatMemberStatusAdministrator"},
                                             {"custom_title", ""},
                                             {"can_be_edited", true},
                                             {"rights", admin_rights_for(rights_set)}}}});
    });
    log << "bot added as administrator, rights set: " << rights_set << "\n";
}

int64_t create_channel_with_bot(BenchAccount& user, BenchAccount& bot, const std::string& title,
                                const std::string& rights_set, FloodGuard& guard,
                                std::ostream& log) {
    const int64_t chat_id = create_channel(user, title, guard, log);
    add_bot_admin(user, bot, chat_id, rights_set, guard, log);
    return chat_id;
}

std::optional<double> wait_chat_known(td::TdClient& client, int64_t chat_id,
                                      std::chrono::seconds timeout, std::string* last_error) {
    const auto t0 = ExpClock::now();
    bool loaded_chats = false;
    for (;;) {
        try {
            client.call(td::Json{{"@type", "getChat"}, {"chat_id", chat_id}},
                        std::chrono::seconds{20});
            return ms_since(t0);
        } catch (const td::TdException& e) {
            if (last_error != nullptr) {
                *last_error = e.error().to_string();
            }
            if (e.error().retry_after || ExpClock::now() - t0 > timeout) {
                return std::nullopt;
            }
            // Пользователю после старта процесса чат может быть неизвестен, пока не загружен
            // список чатов (для бота метод недоступен, ошибка игнорируется).
            if (!loaded_chats) {
                loaded_chats = true;
                attempt([&] {
                    client.call(
                        td::Json{{"@type", "loadChats"}, {"chat_list", nullptr}, {"limit", 100}},
                        std::chrono::seconds{30});
                });
            }
        }
        std::this_thread::sleep_for(std::chrono::seconds{1});
    }
}

std::filesystem::path fresh_bot_dir(const BenchOptions& options) {
    const auto dir = options.state_dir / "bot-fresh";
    remove_fresh_bot_dir(dir);
    return dir;
}

void remove_fresh_bot_dir(const std::filesystem::path& dir) {
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

td::Json send_document_request(int64_t chat_id, const std::filesystem::path& file) {
    return td::Json{{"@type", "sendMessage"},
                    {"chat_id", chat_id},
                    {"options", {{"@type", "messageSendOptions"}, {"disable_notification", true}}},
                    {"input_message_content",
                     {{"@type", "inputMessageDocument"},
                      {"document",
                       {{"@type", "inputDocument"},
                        {"document", {{"@type", "inputFileLocal"}, {"path", file.string()}}},
                        {"disable_content_type_detection", true}}}}}};
}

}  // namespace telega::bench
