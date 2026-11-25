// e2: закрепление в режиме бота. Канал создаёт и настраивает пользователь (только
// настройка, сообщений с него нет), цикл «отправить документ, pin, getChatPinnedMessage, unpin
// предыдущего» выполняет бот.
//
//   tgfs-bench e2 setup                  создать канал, добавить бота администратором (полный
//   набор) tgfs-bench e2 probe empty            что отвечает getChatPinnedMessage без закреплённых
//   tgfs-bench e2 run --sets full,...    циклы на наборах прав (--fresh: ещё и с пустой БД)
//   tgfs-bench e2 probe scan             перебор id через getMessages: служебные сообщения
//   tgfs-bench e2 probe fresh-chat       как бот с пустой БД узнаёт чат
//   tgfs-bench e2 teardown               удалить канал

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "commands.h"
#include "exp_common.h"

namespace telega::bench {

namespace {

namespace fs = std::filesystem;
using std::chrono::milliseconds;
using std::chrono::seconds;

constexpr const char* kState = "e2";
constexpr int64_t kServerIdShift = 20;

const char* const kDefaultSets =
    "full,no:manage_chat,no:change_info,no:post,no:edit,no:delete,no:invite,no:restrict,"
    "no:promote,no:video_chats,no:stories,only:post,edit,delete,only:post,edit,only:post,"
    "only:edit,only:post,delete";

struct E2Options {
    std::string sets;
    int cycles = 20;
    bool fresh = false;
    bool fresh_only = false;
    int min_interval_ms = 3100;
    int settle_s = 90;
    int lag_max_s = 75;
    fs::path out;
};

struct CycleRow {
    bool send_ok = false;
    bool pin_ok = false;
    bool get_ok = false;
    bool seen_immediately = false;
    bool unpin_ok = false;
    bool unpin_tried = false;
    double send_ms = 0;
    double pin_ms = 0;
    double get_ms = 0;
    double lag_ms = -1;
    double unpin_ms = 0;
    double total_ms = 0;
    bool flood = false;
    std::string error_step;
    std::string error;
    int64_t message_id = 0;
};

std::string yn(bool v) { return v ? "1" : "0"; }

std::string date_stamp() {
    const std::string iso = iso_utc_now();  // 2026-11-19T...
    return iso.substr(0, 4) + iso.substr(5, 2) + iso.substr(8, 2);
}

int64_t server_id(int64_t message_id) { return message_id >> kServerIdShift; }

// Сплит имён наборов по `;` не нужен: имена содержат запятые внутри only:, поэтому набор
// разделяется знаком `/` (только в командной строке).
std::vector<std::string> parse_sets(const std::string& text) {
    std::vector<std::string> out;
    std::string cur;
    // Разделитель между наборами: запятая, после которой идёт `full`, `no:` или `only:`.
    size_t i = 0;
    while (i < text.size()) {
        const size_t comma = text.find(',', i);
        const std::string piece = text.substr(i, comma == std::string::npos ? comma : comma - i);
        const bool starts_set =
            piece == "full" || piece.starts_with("no:") || piece.starts_with("only:");
        if (starts_set && !cur.empty()) {
            out.push_back(cur);
            cur.clear();
        }
        cur += (cur.empty() ? "" : ",") + piece;
        if (comma == std::string::npos) {
            break;
        }
        i = comma + 1;
    }
    if (!cur.empty()) {
        out.push_back(cur);
    }
    return out;
}

class E2Runner {
  public:
    E2Runner(BenchEnv& env, const BenchOptions& options, const E2Options& e2, int64_t chat_id)
        : env_(env),
          options_(options),
          e2_(e2),
          chat_id_(chat_id),
          guard_(std::cerr, e2.out / "flood.csv", "e2"),
          pacer_(milliseconds{e2.min_interval_ms}),
          cycles_csv_(e2.out / "e2_cycles.csv",
                      {"set", "phase", "cycle", "head_ok", "full_ok", "send_ok", "pin_ok", "get_ok",
                       "seen_immediately", "unpin_ok", "send_ms", "pin_ms", "get_ms", "lag_ms",
                       "unpin_ms", "total_ms", "flood", "error_step", "error"}),
          rights_csv_(e2.out / "e2_rights.csv",
                      {"set", "requested_rights", "server_rights", "bot_view_settle_ms"}),
          fresh_csv_(e2.out / "e2_fresh_lookup.csv",
                     {"set", "chat_known_ms", "first_get_ms", "first_get_is_expected_head",
                      "login_ms", "error"}) {}

    void run(const std::vector<std::string>& sets) {
        td::Json state = load_state(options_, kState);
        prev_pinned_ = state.contains("last_pinned") && state["last_pinned"].is_number_integer()
                           ? std::optional<int64_t>{state["last_pinned"].get<int64_t>()}
                           : std::nullopt;
        user_ = env_.open(Account::kUser, std::cerr);
        bot_ = env_.open(Account::kBot, std::cerr);
        know_chat(*user_);
        know_chat(*bot_);
        // Текущая голова по данным сервера: состояние могло отстать после прерванного прогона.
        attempt([&] {
            prev_pinned_ =
                bot_->client()
                    .call(td::Json{{"@type", "getChatPinnedMessage"}, {"chat_id", chat_id_}})
                    .value("id", int64_t{0});
        });
        bot_user_id_ = bot_->me().value("id", int64_t{0});
        for (const auto& set : sets) {
            std::cerr << "=== rights set: " << set << "\n";
            apply_rights(set);
            if (!e2_.fresh_only) {
                run_set(set, "main");
            }
            if (e2_.fresh || e2_.fresh_only) {
                run_fresh(set);
            }
            state["last_pinned"] = prev_pinned_ ? td::Json(*prev_pinned_) : td::Json(nullptr);
            state["max_message_id"] = max_message_id_;
            save_state(options_, kState, state);
        }
        // Права возвращаются к полному набору: канал остаётся рабочим для следующих проверок.
        apply_rights("full");
        bump_counter(options_, "e2.bot_messages_sent", bot_messages_);
        bump_counter(options_, "e2.user_actions", user_actions_);
        std::cerr << "flood events: " << guard_.events() << "\n";
    }

  private:
    // После старта процесса TDLib не держит чат в памяти: без getChat методы отвечают
    // «Chat not found», даже если чат есть в БД.
    void know_chat(BenchAccount& account) const {
        std::string error;
        if (!wait_chat_known(account.client(), chat_id_, seconds{60}, &error)) {
            throw std::runtime_error(std::string{account_name(account.kind())} +
                                     " cannot get the chat: " + error);
        }
    }

    // Пользователь меняет права бота, потом ждём, пока бот увидит те же права у себя.
    void apply_rights(const std::string& set) {
        const td::Json rights = admin_rights_for(set);
        guard_.run("setChatMemberStatus", [&] {
            return user_->client().call(td::Json{{"@type", "setChatMemberStatus"},
                                                 {"chat_id", chat_id_},
                                                 {"member_id", sender_user(bot_user_id_)},
                                                 {"status",
                                                  {{"@type", "chatMemberStatusAdministrator"},
                                                   {"custom_title", ""},
                                                   {"can_be_edited", true},
                                                   {"rights", rights}}}});
        });
        ++user_actions_;
        const td::Json server = guard_.run("getChatMember(user)", [&] {
            return user_->client().call(td::Json{{"@type", "getChatMember"},
                                                 {"chat_id", chat_id_},
                                                 {"member_id", sender_user(bot_user_id_)}});
        });
        ++user_actions_;
        const td::Json server_rights =
            server["status"].contains("rights") ? server["status"]["rights"] : td::Json::object();
        const auto t0 = ExpClock::now();
        double settle = -1;
        if (bot_) {
            settle = wait_bot_view(server_rights, seconds{e2_.settle_s});
        }
        std::cerr << "  server rights: " << rights_summary(server_rights)
                  << "; bot sees them after " << fmt(settle) << " ms (" << fmt(ms_since(t0))
                  << ")\n";
        rights_csv_.row({set, rights_summary(rights), rights_summary(server_rights), fmt(settle)});
    }

    // Бот перечитывает свой статус (getChatMember на себя), пока права не совпадут.
    double wait_bot_view(const td::Json& want, seconds timeout) {
        const auto t0 = ExpClock::now();
        const td::Json me_sender = sender_user(bot_user_id_);
        while (ExpClock::now() - t0 < timeout) {
            try {
                const td::Json m = bot_->client().call(td::Json{
                    {"@type", "getChatMember"}, {"chat_id", chat_id_}, {"member_id", me_sender}});
                if (m["status"].contains("rights") &&
                    rights_summary(m["status"]["rights"]) == rights_summary(want)) {
                    return ms_since(t0);
                }
            } catch (const td::TdException& e) {
                last_view_error_ = e.error().to_string();
            }
            std::this_thread::sleep_for(seconds{2});
        }
        return -1;
    }

    void run_set(const std::string& set, const std::string& phase) {
        std::vector<double> total;
        std::vector<double> send;
        std::vector<double> pin;
        std::vector<double> get;
        std::vector<double> unpin;
        int head_ok = 0;
        int full_ok = 0;
        int seen = 0;
        int done = 0;
        int same_error_streak = 0;
        std::string last_error;
        for (int i = 1; i <= e2_.cycles; ++i) {
            const CycleRow r = cycle();
            ++done;
            const bool head = r.send_ok && r.pin_ok && r.get_ok && r.seen_immediately;
            const bool head_lagged = r.send_ok && r.pin_ok && r.get_ok;
            const bool full = head && (!r.unpin_tried || r.unpin_ok);
            head_ok += head_lagged ? 1 : 0;
            full_ok += full ? 1 : 0;
            seen += r.seen_immediately ? 1 : 0;
            if (!r.flood && r.send_ok) {
                total.push_back(r.total_ms);
                send.push_back(r.send_ms);
            }
            if (r.pin_ok && !r.flood) {
                pin.push_back(r.pin_ms);
            }
            if (r.get_ok && !r.flood) {
                get.push_back(r.get_ms);
            }
            if (r.unpin_ok && !r.flood) {
                unpin.push_back(r.unpin_ms);
            }
            cycles_csv_.row({set, phase, std::to_string(i), yn(head_lagged), yn(full),
                             yn(r.send_ok), yn(r.pin_ok), yn(r.get_ok), yn(r.seen_immediately),
                             r.unpin_tried ? yn(r.unpin_ok) : "-", fmt(r.send_ms), fmt(r.pin_ms),
                             fmt(r.get_ms), fmt(r.lag_ms), fmt(r.unpin_ms), fmt(r.total_ms),
                             yn(r.flood), r.error_step, r.error});
            if (!head_lagged) {
                const std::string key = r.error_step + "|" + r.error;
                same_error_streak = key == last_error ? same_error_streak + 1 : 1;
                last_error = key;
                if (same_error_streak >= 3) {
                    std::cerr << "  the same failure 3 times in a row (" << r.error_step << ": "
                              << r.error << "), stopping this set after " << done << " cycles\n";
                    break;
                }
            } else {
                same_error_streak = 0;
                last_error.clear();
            }
        }
        const Summary t = summarize(total);
        std::cerr << "  [" << phase << "] cycles " << done << ", head ok " << head_ok
                  << ", full ok " << full_ok << ", seen immediately " << seen << "; total median "
                  << fmt(t.median) << " ms (min " << fmt(t.min) << ", max " << fmt(t.max)
                  << "); send " << fmt(summarize(send).median) << ", pin "
                  << fmt(summarize(pin).median) << ", get " << fmt(summarize(get).median)
                  << ", unpin " << fmt(summarize(unpin).median) << "\n";
    }

    CycleRow cycle() {
        CycleRow r;
        const auto t_cycle = ExpClock::now();
        const int floods_before = guard_.events();
        const std::string name = random_hex(16) + ".bin";
        const fs::path file = options_.state_dir / "e2-docs" / name;
        std::string content;
        fill_seeded(0xE2E2E2E2ULL + static_cast<uint64_t>(++doc_counter_), 0, content, 1024);
        fs::create_directories(file.parent_path());
        {
            std::ofstream out(file, std::ios::binary);
            out.write(content.data(), static_cast<std::streamsize>(content.size()));
        }
        // 1. отправка
        try {
            auto t0 = ExpClock::now();
            const td::Json msg = guard_.run("send", [&] {
                pacer_.wait();
                t0 = ExpClock::now();
                return bot_->tracker().send(send_document_request(chat_id_, file), seconds{30},
                                            seconds{180});
            });
            r.send_ms = ms_since(t0);
            r.message_id = msg.value("id", int64_t{0});
            r.send_ok = r.message_id != 0;
            ++bot_messages_;
            max_message_id_ = std::max(max_message_id_, r.message_id);
        } catch (const td::TdException& e) {
            fail(r, "send", e.error());
        }
        std::error_code ec;
        fs::remove(file, ec);
        if (r.send_ok) {
            // 2. pin
            try {
                const auto t0 = ExpClock::now();
                guard_.run("pin", [&] {
                    return bot_->client().call(td::Json{{"@type", "pinChatMessage"},
                                                        {"chat_id", chat_id_},
                                                        {"message_id", r.message_id},
                                                        {"disable_notification", true},
                                                        {"only_for_self", false}});
                });
                r.pin_ms = ms_since(t0);
                r.pin_ok = true;
            } catch (const td::TdException& e) {
                fail(r, "pin", e.error());
            }
        }
        if (r.pin_ok) {
            // 3. getChatPinnedMessage; если вернул не то (или 404), опрашиваем до lag_max_s
            try {
                const auto t0 = ExpClock::now();
                int64_t got = 0;
                std::string first_error;
                try {
                    const td::Json m = guard_.run("getChatPinnedMessage", [&] {
                        return bot_->client().call(
                            td::Json{{"@type", "getChatPinnedMessage"}, {"chat_id", chat_id_}});
                    });
                    got = m.value("id", int64_t{0});
                } catch (const td::TdException& e) {
                    first_error = e.error().to_string();
                }
                r.get_ms = ms_since(t0);
                r.seen_immediately = got == r.message_id;
                r.get_ok = r.seen_immediately;
                if (r.seen_immediately) {
                    r.lag_ms = 0;
                } else {
                    const auto t_lag = ExpClock::now();
                    while (ms_since(t_lag) < e2_.lag_max_s * 1000.0) {
                        std::this_thread::sleep_for(milliseconds{1000});
                        attempt([&] {
                            const td::Json m = bot_->client().call(
                                td::Json{{"@type", "getChatPinnedMessage"}, {"chat_id", chat_id_}});
                            r.get_ok = m.value("id", int64_t{0}) == r.message_id;
                        });
                        if (r.get_ok) {
                            r.lag_ms = ms_since(t_lag);
                            break;
                        }
                    }
                    if (!r.get_ok) {
                        r.error_step = "get";
                        r.error = (first_error.empty() ? "returned another message" : first_error) +
                                  "; not the new head after " + std::to_string(e2_.lag_max_s) +
                                  " s";
                    }
                }
            } catch (const td::TdException& e) {
                fail(r, "get", e.error());
            }
        }
        // 4. unpin предыдущего
        if (r.pin_ok) {
            if (prev_pinned_) {
                r.unpin_tried = true;
                try {
                    const auto t0 = ExpClock::now();
                    guard_.run("unpin", [&] {
                        return bot_->client().call(td::Json{{"@type", "unpinChatMessage"},
                                                            {"chat_id", chat_id_},
                                                            {"message_id", *prev_pinned_}});
                    });
                    r.unpin_ms = ms_since(t0);
                    r.unpin_ok = true;
                } catch (const td::TdException& e) {
                    if (r.error_step.empty()) {
                        fail(r, "unpin", e.error());
                    }
                }
            }
            prev_pinned_ = r.message_id;
        }
        r.total_ms = ms_since(t_cycle);
        r.flood = guard_.events() != floods_before;
        return r;
    }

    static void fail(CycleRow& r, const std::string& step, const td::TdError& e) {
        if (r.error_step.empty()) {
            r.error_step = step;
            r.error = e.to_string();
        }
    }

    // Перезапуск бота с пустой БД TDLib: новый каталог, свежий вход по токену.
    void run_fresh(const std::string& set) {
        bot_.reset();  // закрывает основной сеанс
        const fs::path dir = fresh_bot_dir(options_);
        const auto t_login = ExpClock::now();
        std::string error;
        double login_ms = 0;
        double known_ms = -1;
        double first_get_ms = -1;
        bool head_ok = false;
        try {
            bot_ = env_.open_at(Account::kBot, dir, std::cerr);
            login_ms = ms_since(t_login);
            const auto known = wait_chat_known(bot_->client(), chat_id_, seconds{90}, &error);
            if (known) {
                known_ms = *known;
                const auto t0 = ExpClock::now();
                const td::Json m = guard_.run("getChatPinnedMessage(fresh)", [&] {
                    return bot_->client().call(
                        td::Json{{"@type", "getChatPinnedMessage"}, {"chat_id", chat_id_}});
                });
                first_get_ms = ms_since(t0);
                head_ok = prev_pinned_ && m.value("id", int64_t{0}) == *prev_pinned_;
            }
        } catch (const td::TdException& e) {
            error = e.error().to_string();
        }
        fresh_csv_.row({set, fmt(known_ms), fmt(first_get_ms), yn(head_ok), fmt(login_ms), error});
        std::cerr << "  fresh DB: login " << fmt(login_ms) << " ms, chat known after "
                  << fmt(known_ms) << " ms, first getChatPinnedMessage " << fmt(first_get_ms)
                  << " ms, equals the head: " << yn(head_ok) << (error.empty() ? "" : "; ") << error
                  << "\n";
        if (known_ms >= 0) {
            run_set(set, "fresh");
        }
        bot_.reset();
        remove_fresh_bot_dir(dir);
        bot_ = env_.open(Account::kBot, std::cerr);
        know_chat(*bot_);
    }

    BenchEnv& env_;
    const BenchOptions& options_;
    const E2Options& e2_;
    const int64_t chat_id_;
    FloodGuard guard_;
    SendPacer pacer_;
    Csv cycles_csv_;
    Csv rights_csv_;
    Csv fresh_csv_;
    std::unique_ptr<BenchAccount> user_;
    std::unique_ptr<BenchAccount> bot_;
    int64_t bot_user_id_ = 0;
    std::optional<int64_t> prev_pinned_;
    int64_t max_message_id_ = 0;
    int64_t bot_messages_ = 0;
    int64_t user_actions_ = 0;
    int doc_counter_ = 0;
    std::string last_view_error_;
};

void cmd_setup(BenchEnv& env, const BenchOptions& options, const E2Options& e2) {
    if (has_state(options, kState)) {
        throw std::runtime_error("e2 channel already exists (state file present); run teardown");
    }
    FloodGuard guard(std::cerr, e2.out / "flood.csv", "e2");
    auto user = env.open(Account::kUser, std::cerr);
    auto bot = env.open(Account::kBot, std::cerr);
    const std::string title = "tgfs-bench-e2-" + date_stamp();
    const int64_t chat_id = create_channel_with_bot(*user, *bot, title, "full", guard, std::cerr);
    bump_counter(options, "e2.user_actions", 3);
    std::string error;
    const auto known = wait_chat_known(bot->client(), chat_id, seconds{90}, &error);
    std::cerr << "bot knows the channel: " << (known ? fmt(*known) + " ms" : "NO (" + error + ")")
              << "\n";
    save_state(options, kState,
               td::Json{{"chat_id", chat_id},
                        {"title", title},
                        {"created", iso_utc_now()},
                        {"last_pinned", nullptr}});
}

void cmd_probe(BenchEnv& env, const BenchOptions& options, const E2Options& e2,
               const std::string& what) {
    const td::Json state = load_state(options, kState);
    const int64_t chat_id = state.at("chat_id").get<int64_t>();
    Csv csv(e2.out / "e2_probe.csv", {"probe", "result"});
    if (what == "empty") {
        auto bot = env.open(Account::kBot, std::cerr);
        for (const bool known : {false, true}) {
            if (known) {
                std::string err;
                wait_chat_known(bot->client(), chat_id, seconds{30}, &err);
            }
            std::string result;
            try {
                const td::Json m = bot->client().call(
                    td::Json{{"@type", "getChatPinnedMessage"}, {"chat_id", chat_id}});
                result = "message id " + std::to_string(server_id(m.value("id", int64_t{0})));
            } catch (const td::TdException& e) {
                result = "error " + e.error().to_string();
            }
            const std::string name = std::string{"getChatPinnedMessage, no pinned messages, "} +
                                     (known ? "after getChat" : "right after start, no getChat");
            std::cout << name << ": " << result << "\n";
            csv.row({name, result});
        }
    } else if (what == "scan") {
        auto bot = env.open(Account::kBot, std::cerr);
        std::string err;
        wait_chat_known(bot->client(), chat_id, seconds{30}, &err);
        const int64_t max_sid = server_id(state.value("max_message_id", int64_t{0})) + 30;
        FloodGuard scan_guard(std::cerr, e2.out / "flood.csv", "e2");
        Csv rows(e2.out / "e2_scan.csv", {"server_id", "found", "content_type", "is_outgoing"});
        std::map<std::string, int> counts;
        for (int64_t from = 1; from <= max_sid; from += 100) {
            td::Json ids = td::Json::array();
            for (int64_t sid = from; sid < from + 100 && sid <= max_sid; ++sid) {
                ids.push_back(sid << kServerIdShift);
            }
            const td::Json res = scan_guard.run("getMessages", [&] {
                return bot->client().call(
                    td::Json{{"@type", "getMessages"}, {"chat_id", chat_id}, {"message_ids", ids}});
            });
            std::this_thread::sleep_for(seconds{2});
            for (size_t i = 0; i < ids.size(); ++i) {
                const td::Json& m = res["messages"][i];
                const int64_t sid = server_id(ids[i].get<int64_t>());
                if (m.is_null()) {
                    rows.row({std::to_string(sid), "0", "-", "-"});
                    ++counts["(missing)"];
                } else {
                    const std::string type = m["content"].value("@type", std::string{"?"});
                    rows.row({std::to_string(sid), "1", type,
                              m.value("is_outgoing", false) ? "1" : "0"});
                    ++counts[type];
                }
            }
        }
        for (const auto& [type, n] : counts) {
            std::cout << type << ": " << n << "\n";
            csv.row({"scan 1.." + std::to_string(max_sid) + " " + type, std::to_string(n)});
        }
    } else if (what == "pinned") {
        // Что видит getChatPinnedMessage у бота и у пользователя в уже закреплённом канале.
        auto bot = env.open(Account::kBot, std::cerr);
        auto user = env.open(Account::kUser, std::cerr);
        std::string err;
        wait_chat_known(user->client(), chat_id, seconds{30}, &err);
        const td::Json chat = [&] {
            wait_chat_known(bot->client(), chat_id, seconds{30}, &err);
            return bot->client().call(td::Json{{"@type", "getChat"}, {"chat_id", chat_id}});
        }();
        const int64_t supergroup = chat["type"].value("supergroup_id", int64_t{0});
        const auto get = [&](BenchAccount& who, const std::string& name) {
            std::string res;
            try {
                const td::Json m = who.client().call(
                    td::Json{{"@type", "getChatPinnedMessage"}, {"chat_id", chat_id}});
                res = "message " + std::to_string(server_id(m.value("id", int64_t{0})));
            } catch (const td::TdException& ex) {
                res = "error " + ex.error().to_string();
            }
            std::cout << name << ": " << res << "\n";
            csv.row({name, res});
        };
        get(*bot, "bot getChatPinnedMessage (after getChat)");
        get(*user, "user getChatPinnedMessage (after getChat)");
        try {
            const td::Json full = bot->client().call(
                td::Json{{"@type", "getSupergroupFullInfo"}, {"supergroup_id", supergroup}});
            std::cout << "bot getSupergroupFullInfo ok, keys: " << full.dump().substr(0, 400)
                      << "\n";
        } catch (const td::TdException& ex) {
            std::cout << "bot getSupergroupFullInfo: " << ex.error().to_string() << "\n";
        }
        get(*bot, "bot getChatPinnedMessage (after getSupergroupFullInfo)");
        std::this_thread::sleep_for(seconds{5});
        get(*bot, "bot getChatPinnedMessage (5 s later)");
    } else if (what == "delete") {
        // Может ли бот без права удаления убрать собственное сообщение (нужно для GC).
        auto user = env.open(Account::kUser, std::cerr);
        auto bot = env.open(Account::kBot, std::cerr);
        std::string err;
        wait_chat_known(user->client(), chat_id, seconds{60}, &err);
        wait_chat_known(bot->client(), chat_id, seconds{30}, &err);
        const int64_t bot_id = bot->me().value("id", int64_t{0});
        FloodGuard guard(std::cerr, e2.out / "flood.csv", "e2");
        SendPacer pacer(milliseconds{e2.min_interval_ms});
        for (const std::string set : {"only:post,edit", "only:post,edit,delete"}) {
            guard.run("setChatMemberStatus", [&] {
                return user->client().call(td::Json{{"@type", "setChatMemberStatus"},
                                                    {"chat_id", chat_id},
                                                    {"member_id", sender_user(bot_id)},
                                                    {"status",
                                                     {{"@type", "chatMemberStatusAdministrator"},
                                                      {"custom_title", ""},
                                                      {"can_be_edited", true},
                                                      {"rights", admin_rights_for(set)}}}});
            });
            bump_counter(options, "e2.user_actions", 1);
            std::this_thread::sleep_for(seconds{5});
            const fs::path file = options.state_dir / "e2-docs" / (random_hex(16) + ".bin");
            fs::create_directories(file.parent_path());
            std::string content;
            fill_seeded(0xDE1ULL, 0, content, 1024);
            std::ofstream(file, std::ios::binary).write(content.data(), 1024);
            pacer.wait();
            const td::Json msg = guard.run("send", [&] {
                return bot->tracker().send(send_document_request(chat_id, file), seconds{30},
                                           seconds{180});
            });
            fs::remove(file);
            bump_counter(options, "e2.bot_messages_sent", 1);
            std::string res = "deleted";
            try {
                bot->client().call(td::Json{{"@type", "deleteMessages"},
                                            {"chat_id", chat_id},
                                            {"message_ids", td::Json::array({msg["id"]})},
                                            {"revoke", true}});
                const td::Json after =
                    bot->client().call(td::Json{{"@type", "getMessages"},
                                                {"chat_id", chat_id},
                                                {"message_ids", td::Json::array({msg["id"]})}});
                res += after["messages"][0].is_null() ? ", getMessages returns null"
                                                      : ", but getMessages still returns it";
            } catch (const td::TdException& e) {
                res = "error " + e.error().to_string();
            }
            std::cout << "bot deletes its own message, rights " << set << ": " << res << "\n";
            csv.row({"bot deletes its own message, rights " + set, res});
        }
        guard.run("setChatMemberStatus", [&] {
            return user->client().call(td::Json{{"@type", "setChatMemberStatus"},
                                                {"chat_id", chat_id},
                                                {"member_id", sender_user(bot_id)},
                                                {"status",
                                                 {{"@type", "chatMemberStatusAdministrator"},
                                                  {"custom_title", ""},
                                                  {"can_be_edited", true},
                                                  {"rights", admin_rights_for("full")}}}});
        });
        bump_counter(options, "e2.user_actions", 1);
    } else if (what == "firstpin" || what == "firstpin-full") {
        // Первый pin в канале, где закреплённых сообщений ещё не было: когда getChatPinnedMessage
        // бота увидит его и помогают ли getMessages, getSupergroupFullInfo, getChat.
        auto bot = env.open(Account::kBot, std::cerr);
        std::string err;
        wait_chat_known(bot->client(), chat_id, seconds{30}, &err);
        FloodGuard guard(std::cerr, e2.out / "flood.csv", "e2");
        Csv events(e2.out / "e2_first_pin_probe.csv", {"variant", "t_s", "action", "result"});
        const int64_t supergroup =
            bot->client().call(td::Json{{"@type", "getChat"}, {"chat_id", chat_id}})["type"].value(
                "supergroup_id", int64_t{0});
        if (what == "firstpin-full") {
            // ChannelFull загружается до первого pin (так делает любой код, читающий настройки).
            bot->client().call(
                td::Json{{"@type", "getSupergroupFullInfo"}, {"supergroup_id", supergroup}});
        }
        const fs::path file = options.state_dir / "e2-docs" / (random_hex(16) + ".bin");
        fs::create_directories(file.parent_path());
        std::string content;
        fill_seeded(0xF1257ULL, 0, content, 1024);
        std::ofstream(file, std::ios::binary).write(content.data(), 1024);
        const td::Json msg = guard.run("send", [&] {
            return bot->tracker().send(send_document_request(chat_id, file), seconds{30},
                                       seconds{180});
        });
        fs::remove(file);
        bump_counter(options, "e2.bot_messages_sent", 1);
        const int64_t id = msg.value("id", int64_t{0});
        bot->client().call(td::Json{{"@type", "pinChatMessage"},
                                    {"chat_id", chat_id},
                                    {"message_id", id},
                                    {"disable_notification", true},
                                    {"only_for_self", false}});
        const auto t0 = ExpClock::now();
        const auto log = [&](const std::string& action, const std::string& result) {
            events.row({what, fmt(ms_since(t0) / 1000.0), action, result});
            std::cout << fmt(ms_since(t0) / 1000.0) << " s: " << action << ": " << result << "\n";
        };
        const auto probe_get = [&]() -> bool {
            try {
                const td::Json m = bot->client().call(
                    td::Json{{"@type", "getChatPinnedMessage"}, {"chat_id", chat_id}});
                const bool is_new = m.value("id", int64_t{0}) == id;
                log("getChatPinnedMessage", is_new ? "the new message" : "another message");
                return is_new;
            } catch (const td::TdException& e) {
                log("getChatPinnedMessage", "error " + e.error().to_string());
                return false;
            }
        };
        probe_get();
        try {
            const td::Json m = bot->client().call(td::Json{{"@type", "getMessages"},
                                                           {"chat_id", chat_id},
                                                           {"message_ids", td::Json::array({id})}});
            std::string pinned = "null";
            if (!m["messages"][0].is_null()) {
                pinned = m["messages"][0].value("is_pinned", false) ? "true" : "false";
            }
            log("getMessages(new message).is_pinned", pinned);
        } catch (const td::TdException& e) {
            log("getMessages", "error " + e.error().to_string());
        }
        bool done = false;
        for (int step = 1; step <= 40 && !done; ++step) {
            std::this_thread::sleep_for(seconds{3});
            if (step == 5) {
                attempt([&] {
                    bot->client().call(td::Json{{"@type", "getSupergroupFullInfo"},
                                                {"supergroup_id", supergroup}});
                });
                log("getSupergroupFullInfo", "called");
            }
            if (step == 10) {
                attempt([&] {
                    bot->client().call(td::Json{{"@type", "getChat"}, {"chat_id", chat_id}});
                });
                log("getChat", "called");
            }
            done = probe_get();
        }
        td::Json updated = load_state(options, kState);
        updated["last_pinned"] = id;
        updated["max_message_id"] = id;
        save_state(options, kState, updated);
    } else if (what == "lag") {
        // Когда getChatPinnedMessage бота увидит свой же pin: опрос раз в 2 с до 150 с, в раундах
        // с getSupergroupFullInfo ещё и принудительное обновление каждые 6 с.
        auto bot = env.open(Account::kBot, std::cerr);
        std::string err;
        wait_chat_known(bot->client(), chat_id, seconds{30}, &err);
        FloodGuard guard(std::cerr, e2.out / "flood.csv", "e2");
        SendPacer pacer(milliseconds{e2.min_interval_ms});
        Csv lag(e2.out / "e2_lag.csv",
                {"round", "mode", "immediate_result", "seen_after_s", "updates_seen", "error"});
        const int64_t supergroup = [&] {
            return bot->client()
                .call(td::Json{{"@type", "getChat"}, {"chat_id", chat_id}})["type"]
                .value("supergroup_id", int64_t{0});
        }();
        std::atomic<int> pin_updates{0};
        auto sub1 = bot->client().subscribe("updateMessageIsPinned",
                                            [&](const td::Json&) { ++pin_updates; });
        int64_t prev = 0;
        for (int round = 1; round <= 6; ++round) {
            const bool force = round > 3;
            const std::string name = random_hex(16) + ".bin";
            const fs::path file = options.state_dir / "e2-docs" / name;
            fs::create_directories(file.parent_path());
            std::string content;
            fill_seeded(0x1A6ULL + static_cast<uint64_t>(round), 0, content, 1024);
            std::ofstream(file, std::ios::binary).write(content.data(), 1024);
            pacer.wait();
            const td::Json msg = guard.run("send", [&] {
                return bot->tracker().send(send_document_request(chat_id, file), seconds{30},
                                           seconds{180});
            });
            fs::remove(file);
            const int64_t id = msg.value("id", int64_t{0});
            bump_counter(options, "e2.bot_messages_sent", 1);
            const int before = pin_updates;
            bot->client().call(td::Json{{"@type", "pinChatMessage"},
                                        {"chat_id", chat_id},
                                        {"message_id", id},
                                        {"disable_notification", true},
                                        {"only_for_self", false}});
            if (prev != 0) {
                bot->client().call(td::Json{
                    {"@type", "unpinChatMessage"}, {"chat_id", chat_id}, {"message_id", prev}});
            }
            prev = id;
            const auto t0 = ExpClock::now();
            std::string first;
            double seen = -1;
            std::string last_error;
            double last_force = -100000;
            while (ms_since(t0) < 150000) {
                if (force && ms_since(t0) - last_force >= 6000) {
                    last_force = ms_since(t0);
                    attempt([&] {
                        bot->client().call(td::Json{{"@type", "getSupergroupFullInfo"},
                                                    {"supergroup_id", supergroup}});
                    });
                }
                try {
                    const td::Json m = bot->client().call(
                        td::Json{{"@type", "getChatPinnedMessage"}, {"chat_id", chat_id}});
                    const bool is_new = m.value("id", int64_t{0}) == id;
                    if (first.empty()) {
                        first = is_new ? "new" : "other message";
                    }
                    if (is_new) {
                        seen = ms_since(t0) / 1000.0;
                        break;
                    }
                } catch (const td::TdException& e) {
                    last_error = e.error().to_string();
                    if (first.empty()) {
                        first = "error " + last_error;
                    }
                }
                std::this_thread::sleep_for(seconds{2});
            }
            std::cout << "round " << round << (force ? " +fullinfo" : " plain") << ": first "
                      << first << ", seen after " << fmt(seen, 1) << " s, updateMessageIsPinned "
                      << (pin_updates - before) << "\n";
            lag.row({std::to_string(round), force ? "poll+getSupergroupFullInfo" : "poll", first,
                     fmt(seen, 1), std::to_string(pin_updates - before), last_error});
            const td::Json state_now = load_state(options, kState);
            td::Json updated = state_now;
            updated["last_pinned"] = id;
            updated["max_message_id"] = id;
            save_state(options, kState, updated);
        }
    } else if (what == "stale") {
        // Голову сменил другой сеанс бота (как вторая машина или перезапуск с пустой БД), пока
        // основной сеанс был закрыт. Что увидит основной сеанс при следующем старте.
        FloodGuard guard(std::cerr, e2.out / "flood.csv", "e2");
        Csv stale(e2.out / "e2_stale_head.csv", {"round", "mode", "first_result",
                                                 "first_is_new_head", "seen_new_after_s", "error"});
        for (int round = 1; round <= 4; ++round) {
            const bool with_full = round % 2 == 0;
            int64_t old_head = 0;
            {
                auto a = env.open(Account::kBot, std::cerr);
                std::string err;
                wait_chat_known(a->client(), chat_id, seconds{30}, &err);
                attempt([&] {
                    old_head =
                        a->client()
                            .call(td::Json{{"@type", "getChatPinnedMessage"}, {"chat_id", chat_id}})
                            .value("id", int64_t{0});
                });
            }
            int64_t new_head = 0;
            {
                const fs::path dir = fresh_bot_dir(options);
                {
                    auto b = env.open_at(Account::kBot, dir, std::cerr);
                    std::string err;
                    wait_chat_known(b->client(), chat_id, seconds{30}, &err);
                    const std::string name = random_hex(16) + ".bin";
                    const fs::path file = options.state_dir / "e2-docs" / name;
                    fs::create_directories(file.parent_path());
                    std::string content;
                    fill_seeded(0x57A1EULL + static_cast<uint64_t>(round), 0, content, 1024);
                    std::ofstream(file, std::ios::binary).write(content.data(), 1024);
                    const td::Json msg = guard.run("send", [&] {
                        return b->tracker().send(send_document_request(chat_id, file), seconds{30},
                                                 seconds{180});
                    });
                    fs::remove(file);
                    new_head = msg.value("id", int64_t{0});
                    bump_counter(options, "e2.bot_messages_sent", 1);
                    b->client().call(td::Json{{"@type", "pinChatMessage"},
                                              {"chat_id", chat_id},
                                              {"message_id", new_head},
                                              {"disable_notification", true},
                                              {"only_for_self", false}});
                    if (old_head != 0) {
                        const auto unpin = [&] {
                            b->client().call(td::Json{{"@type", "unpinChatMessage"},
                                                      {"chat_id", chat_id},
                                                      {"message_id", old_head}});
                        };
                        std::string res = "ok";
                        try {
                            unpin();
                        } catch (const td::TdException& e) {
                            res = "error " + e.error().to_string();
                            const td::Json loaded = b->client().call(
                                td::Json{{"@type", "getMessages"},
                                         {"chat_id", chat_id},
                                         {"message_ids", td::Json::array({old_head})}});
                            res += "; getMessages returned " +
                                   std::string{loaded["messages"][0].is_null() ? "null"
                                                                               : "the message"};
                            try {
                                unpin();
                                res += "; unpin after getMessages ok";
                            } catch (const td::TdException& retry_error) {
                                res +=
                                    "; unpin after getMessages: " + retry_error.error().to_string();
                            }
                        }
                        std::cout << "round " << round
                                  << ": unpin of an unseen message in a fresh DB: " << res << "\n";
                        csv.row({"fresh DB, unpin of a message never loaded (round " +
                                     std::to_string(round) + ")",
                                 res});
                    }
                }
                remove_fresh_bot_dir(dir);
            }
            auto a = env.open(Account::kBot, std::cerr);
            std::string err;
            wait_chat_known(a->client(), chat_id, seconds{30}, &err);
            if (with_full) {
                const int64_t supergroup =
                    a->client()
                        .call(td::Json{{"@type", "getChat"}, {"chat_id", chat_id}})["type"]
                        .value("supergroup_id", int64_t{0});
                a->client().call(
                    td::Json{{"@type", "getSupergroupFullInfo"}, {"supergroup_id", supergroup}});
            }
            std::string first = "-";
            bool first_new = false;
            double seen = -1;
            std::string last_error;
            const auto t0 = ExpClock::now();
            bool first_done = false;
            while (ms_since(t0) < 120000) {
                try {
                    const td::Json m = a->client().call(
                        td::Json{{"@type", "getChatPinnedMessage"}, {"chat_id", chat_id}});
                    const bool is_new = m.value("id", int64_t{0}) == new_head;
                    if (!first_done) {
                        first = "other";
                        if (is_new) {
                            first = "new head";
                        } else if (m.value("id", int64_t{0}) == old_head) {
                            first = "old head";
                        }
                        first_new = is_new;
                        first_done = true;
                    }
                    if (is_new) {
                        seen = ms_since(t0) / 1000.0;
                        break;
                    }
                } catch (const td::TdException& e) {
                    last_error = e.error().to_string();
                    if (!first_done) {
                        first = "error " + last_error;
                        first_done = true;
                    }
                }
                std::this_thread::sleep_for(seconds{2});
            }
            std::cout << "round " << round << (with_full ? " +getSupergroupFullInfo" : " plain")
                      << ": first " << first << ", new head seen after " << fmt(seen, 1) << " s\n";
            stale.row({std::to_string(round), with_full ? "getSupergroupFullInfo, then get" : "get",
                       first, yn(first_new), fmt(seen, 1), last_error});
            td::Json updated = load_state(options, kState);
            updated["last_pinned"] = new_head;
            updated["max_message_id"] = new_head;
            save_state(options, kState, updated);
        }
    } else if (what == "fresh-chat") {
        const fs::path dir = fresh_bot_dir(options);
        {
            auto bot = env.open_at(Account::kBot, dir, std::cerr);
            int updates = 0;
            auto sub =
                bot->client().subscribe("updateNewChat", [&](const td::Json&) { ++updates; });
            const auto attempt = [&](const std::string& name, td::Json req) {
                std::string res;
                try {
                    const td::Json r = bot->client().call(std::move(req), seconds{30});
                    res = "ok " + r.value("@type", std::string{"?"});
                    if (r.contains("chat_ids")) {
                        res += ", chats: " + std::to_string(r["chat_ids"].size());
                    }
                } catch (const td::TdException& ex) {
                    res = "error " + ex.error().to_string();
                }
                std::cout << name << ": " << res << "\n";
                csv.row({"fresh DB, " + name, res});
            };
            attempt("getChat", td::Json{{"@type", "getChat"}, {"chat_id", chat_id}});
            attempt("loadChats",
                    td::Json{{"@type", "loadChats"}, {"chat_list", nullptr}, {"limit", 100}});
            attempt("getChats",
                    td::Json{{"@type", "getChats"}, {"chat_list", nullptr}, {"limit", 100}});
            std::this_thread::sleep_for(seconds{20});
            attempt("getChat after 20 s", td::Json{{"@type", "getChat"}, {"chat_id", chat_id}});
            std::cout << "updateNewChat received: " << updates << "\n";
            csv.row({"fresh DB, updateNewChat received", std::to_string(updates)});
        }
        remove_fresh_bot_dir(dir);
    } else {
        throw std::runtime_error("probe: empty | scan | fresh-chat");
    }
}

void cmd_teardown(BenchEnv& env, const BenchOptions& options, const E2Options& e2) {
    const td::Json state = load_state(options, kState);
    FloodGuard guard(std::cerr, e2.out / "flood.csv", "e2");
    auto user = env.open(Account::kUser, std::cerr);
    std::string error;
    wait_chat_known(user->client(), state.at("chat_id").get<int64_t>(), seconds{30}, &error);
    for (int attempt_no = 1;; ++attempt_no) {
        try {
            guard.run("deleteChat", [&] {
                return user->client().call(td::Json{
                    {"@type", "deleteChat"}, {"chat_id", state.at("chat_id").get<int64_t>()}});
            });
            break;
        } catch (const td::TdException& e) {
            // После старта процесса чат бывает неизвестен пользователю: подождать и повторить.
            if (attempt_no >= 5 || e.error().code != 400) {
                throw;
            }
            std::this_thread::sleep_for(seconds{3});
            wait_chat_known(user->client(), state.at("chat_id").get<int64_t>(), seconds{20},
                            &error);
        }
    }
    bump_counter(options, "e2.user_actions", 1);
    std::filesystem::remove(options.state_dir / (std::string{kState} + ".json"));
    std::cout << "channel deleted\n";
}

}  // namespace

void add_e2_command(CLI::App& app, const BenchOptions& options, int* exit_code) {
    auto* cmd = app.add_subcommand("e2", "Experiment E2: pinning in bot mode");
    cmd->require_subcommand(1);
    auto e2 = std::make_shared<E2Options>();
    e2->out = default_out_dir("02-e2-bot-pinning");
    e2->sets = kDefaultSets;
    cmd->add_option("--out", e2->out, "Directory for CSV data")->capture_default_str();

    cmd->add_subcommand("setup", "User creates the channel and adds the bot as administrator")
        ->callback([&options, exit_code, e2] {
            BenchEnv env(options);
            cmd_setup(env, options, *e2);
            *exit_code = 0;
        });
    auto* run = cmd->add_subcommand("run", "Cycles on rights sets: full, no:<right>, only:<r>,<r>");
    run->add_option("--sets", e2->sets, "Comma-separated rights sets")->capture_default_str();
    run->add_option("--cycles", e2->cycles, "Cycles per set")->capture_default_str();
    run->add_flag("--fresh", e2->fresh, "Also repeat the cycles after a restart with an empty DB");
    run->add_flag("--fresh-only", e2->fresh_only, "Only the restart-with-empty-DB phase");
    run->add_option("--min-interval-ms", e2->min_interval_ms, "Minimum interval between sends")
        ->capture_default_str();
    run->add_option("--lag-max", e2->lag_max_s,
                    "Seconds to wait until getChatPinnedMessage shows the new head")
        ->capture_default_str();
    run->add_option("--settle", e2->settle_s, "Seconds to wait until the bot sees new rights")
        ->capture_default_str();
    run->callback([&options, exit_code, e2] {
        BenchEnv env(options);
        const td::Json state = load_state(options, kState);
        E2Runner runner(env, options, *e2, state.at("chat_id").get<int64_t>());
        try {
            runner.run(parse_sets(e2->sets));
            *exit_code = 0;
        } catch (const StopExperiment& e) {
            std::cerr << "STOPPED: " << e.what() << "\n";
            *exit_code = 2;
        }
    });
    auto what = std::make_shared<std::string>();
    auto* probe = cmd->add_subcommand("probe", "empty | scan | fresh-chat");
    probe->add_option("what", *what)->required();
    probe->callback([&options, exit_code, e2, what] {
        BenchEnv env(options);
        cmd_probe(env, options, *e2, *what);
        *exit_code = 0;
    });
    cmd->add_subcommand("teardown", "Delete the E2 channel (user account)")
        ->callback([&options, exit_code, e2] {
            BenchEnv env(options);
            cmd_teardown(env, options, *e2);
            *exit_code = 0;
        });
}

}  // namespace telega::bench
