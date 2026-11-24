// e2e: настройка и уборка канала для E2E-прогона telega-fs в режиме бота (SPEC 14, 17.2).
// Аккаунт пользователя здесь только настраивает: создаёт приватный канал, добавляет бота
// администратором (post, edit, delete) и удаляет канал в конце. Ни одного сообщения с него.
// Нагрузку несёт сам telega-fs под ботом. id канала лежит только в файле --chat-file (0600),
// вне репозитория.
//
//   tgfs-bench e2e setup    --chat-file FILE   создать канал `tgfs-e2e-<дата>` и записать его id
//   tgfs-bench e2e inspect  --chat-file FILE   что бот видит в канале: типы сообщений, имена
//                                              документов (SPEC 4), подписи; без id и имён
//   tgfs-bench e2e drop-head --chat-file FILE  БОТ снимает все закрепы и удаляет сообщение головы
//                                              (SPEC 7.2, 16: сценарий M4 «потеря головы»)
//   tgfs-bench e2e teardown --chat-file FILE   удалить канал и файл

#include <sys/stat.h>

#include <algorithm>
#include <chrono>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <regex>
#include <string>
#include <thread>

#include "commands.h"
#include "exp_common.h"

namespace telega::bench {

namespace {

using std::chrono::seconds;
constexpr int64_t kServerIdShift = 20;

struct E2eOptions {
    fs::path chat_file;
    std::string rights = "only:post,edit,delete";
};

std::string e2e_date_stamp() {
    const std::time_t now = std::time(nullptr);
    std::tm tm{};
    gmtime_r(&now, &tm);
    char buf[16];
    std::strftime(buf, sizeof(buf), "%Y%m%d", &tm);
    return buf;
}

void write_private_file(const fs::path& path, const std::string& text) {
    fs::create_directories(path.parent_path());
    const fs::path tmp = path.string() + ".tmp";
    {
        std::ofstream out(tmp, std::ios::trunc);
        if (!out) {
            throw std::runtime_error("cannot write " + tmp.string());
        }
        fs::permissions(tmp, fs::perms::owner_read | fs::perms::owner_write,
                        fs::perm_options::replace);
        out << text << '\n';
    }
    fs::rename(tmp, path);
}

int64_t read_chat_file(const fs::path& path) {
    std::ifstream in(path);
    int64_t id = 0;
    if (!(in >> id) || id == 0) {
        throw std::runtime_error("cannot read the chat id from the --chat-file");
    }
    return id;
}

// Голова канала с учётом кэша TDLib (SPEC 2): ChannelFull у бота обновляется раз в 60 с, и свежий
// процесс может до минуты видеть старую голову (после unpin старой «закреплённого нет», 404).
// Повторяем до `window`, раз в 10 с, с принудительным getSupergroupFullInfo. 0 — головы нет.
int64_t find_head_id(td::TdClient& client, int64_t chat_id, seconds window, std::ostream& log) {
    const auto deadline = std::chrono::steady_clock::now() + window;
    for (;;) {
        try {
            const td::Json pinned =
                client.call(td::Json{{"@type", "getChatPinnedMessage"}, {"chat_id", chat_id}});
            return pinned.value("id", int64_t{0});
        } catch (const td::TdException& e) {
            if (e.error().code != 404 || std::chrono::steady_clock::now() >= deadline) {
                log << "getChatPinnedMessage: " << e.error().to_string() << "\n";
                return 0;
            }
        }
        try {
            const td::Json chat = client.call(td::Json{{"@type", "getChat"}, {"chat_id", chat_id}});
            client.call(
                td::Json{{"@type", "getSupergroupFullInfo"},
                         {"supergroup_id", chat["type"].value("supergroup_id", int64_t{0})}});
        } catch (const td::TdException& e) {
            // Обновление кэша — только подсказка: следующая попытка getChatPinnedMessage всё решит.
            log << "refresh before retry: " << e.error().to_string() << "\n";
        }
        std::this_thread::sleep_for(seconds{10});
    }
}

void cmd_setup(BenchEnv& env, const BenchOptions& options, const E2eOptions& e2e) {
    if (fs::exists(e2e.chat_file)) {
        throw std::runtime_error("--chat-file already exists: run `e2e teardown` first");
    }
    FloodGuard guard(std::cerr, options.state_dir / "e2e-flood.csv", "e2e");
    auto bot = env.open(Account::kBot, std::cerr);
    auto user = env.open(Account::kUser, std::cerr);
    const std::string title = "tgfs-e2e-" + e2e_date_stamp();
    const int64_t chat_id =
        create_channel_with_bot(*user, *bot, title, e2e.rights, guard, std::cerr);
    bump_counter(options, "e2e.user_actions", 3);
    // id сразу на диск: если дальше что-то упадёт, teardown всё равно найдёт канал.
    write_private_file(e2e.chat_file, std::to_string(chat_id));
    std::string error;
    if (!wait_chat_known(bot->client(), chat_id, seconds{90}, &error)) {
        throw std::runtime_error("the bot does not see the channel: " + error);
    }
    std::cout << "channel " << title << " ready, chat id saved to the --chat-file\n";
}

void cmd_teardown(BenchEnv& env, const BenchOptions& options, const E2eOptions& e2e) {
    const int64_t chat_id = read_chat_file(e2e.chat_file);
    FloodGuard guard(std::cerr, options.state_dir / "e2e-flood.csv", "e2e");
    auto user = env.open(Account::kUser, std::cerr);
    std::string error;
    wait_chat_known(user->client(), chat_id, seconds{30}, &error);
    // Право удалить канал приходит с полной информацией о нём (E3).
    const auto load_full_info = [&] {
        try {
            const td::Json chat =
                user->client().call(td::Json{{"@type", "getChat"}, {"chat_id", chat_id}});
            const td::Json full = user->client().call(
                td::Json{{"@type", "getSupergroupFullInfo"},
                         {"supergroup_id", chat["type"].value("supergroup_id", int64_t{0})}});
            std::cerr << "can_be_deleted: " << (full.value("can_be_deleted", false) ? "yes" : "no")
                      << "\n";
        } catch (const td::TdException& e) {
            std::cerr << "getSupergroupFullInfo: " << e.error().to_string() << "\n";
        }
    };
    load_full_info();
    for (int attempt_no = 1;; ++attempt_no) {
        try {
            guard.run("deleteChat", [&] {
                return user->client().call(td::Json{{"@type", "deleteChat"}, {"chat_id", chat_id}});
            });
            break;
        } catch (const td::TdException& e) {
            if (attempt_no >= 5 || e.error().code != 400) {
                throw;
            }
            std::this_thread::sleep_for(seconds{3});
            wait_chat_known(user->client(), chat_id, seconds{20}, &error);
            load_full_info();
        }
    }
    bump_counter(options, "e2e.user_actions", 1);
    std::error_code ec;
    fs::remove(e2e.chat_file, ec);
    std::cout << "channel deleted\n";
}

// Перебор id 1..голова+3 через getMessages пачками по 100 с паузой 10 с (SPEC 17.2) и проверка
// SPEC 4: документы называются `<16 hex>.bin` или `<16 hex>.idx`, подписей нет. Печатает только
// счётчики и размеры.
int cmd_inspect(BenchEnv& env, const E2eOptions& e2e) {
    const int64_t chat_id = read_chat_file(e2e.chat_file);
    auto bot = env.open(Account::kBot, std::cerr);
    std::string error;
    if (!wait_chat_known(bot->client(), chat_id, seconds{90}, &error)) {
        throw std::runtime_error("the bot does not see the channel: " + error);
    }
    const int64_t head_id = find_head_id(bot->client(), chat_id, seconds{150}, std::cerr);
    if (head_id == 0) {
        throw std::runtime_error("the channel has no pinned message (the head)");
    }
    const int64_t last = (head_id >> kServerIdShift) + 3;
    std::map<std::string, int> types;
    std::map<std::string, int> doc_kinds;
    int bad_names = 0;
    int captions = 0;
    int64_t doc_bytes = 0;
    const std::regex name_re("^[0-9a-f]{16}\\.(bin|idx)$");
    for (int64_t first = 1; first <= last; first += 100) {
        td::Json ids = td::Json::array();
        for (int64_t sid = first; sid < first + 100 && sid <= last; ++sid) {
            ids.push_back(sid << kServerIdShift);
        }
        const td::Json reply = bot->client().call(
            td::Json{{"@type", "getMessages"}, {"chat_id", chat_id}, {"message_ids", ids}});
        for (const td::Json& m : reply["messages"]) {
            if (m.is_null()) {
                continue;
            }
            const std::string type = m["content"].value("@type", std::string{"?"});
            ++types[type];
            if (type != "messageDocument") {
                continue;
            }
            const std::string name = m["content"]["document"].value("file_name", std::string{});
            const std::string ext = name.size() > 4 ? name.substr(name.size() - 4) : "?";
            if (std::regex_match(name, name_re)) {
                ++doc_kinds[ext];
            } else {
                ++bad_names;
            }
            if (!m["content"]["caption"].value("text", std::string{}).empty()) {
                ++captions;
            }
            doc_bytes += m["content"]["document"]["document"].value("size", int64_t{0});
        }
        if (first + 100 <= last) {
            std::this_thread::sleep_for(seconds{10});
        }
    }
    std::cout << "head server id: " << (last - 3) << "\n";
    for (const auto& [type, n] : types) {
        std::cout << "messages " << type << ": " << n << "\n";
    }
    for (const auto& [ext, n] : doc_kinds) {
        std::cout << "documents " << ext << ": " << n << "\n";
    }
    std::cout << "documents bytes: " << doc_bytes << "\n"
              << "documents with a name outside <16 hex>.bin|.idx: " << bad_names << "\n"
              << "documents with a caption: " << captions << "\n"
              << (bad_names == 0 && captions == 0 ? "INSPECT OK\n" : "INSPECT FAILED\n");
    return bad_names == 0 && captions == 0 ? 0 : 1;
}

// Потеря головы (SPEC 7.2, веха M4): бот снимает ВСЕ закрепы (в том числе старые, иначе вместо
// «головы нет» получился бы откат) и удаляет сообщение головы. Аккаунт пользователя не участвует.
// Печатает, что закреплено после (404 — головы нет); код 0, если головы нет и сообщения нет.
int cmd_drop_head(BenchEnv& env, const BenchOptions& options, const E2eOptions& e2e) {
    const int64_t chat_id = read_chat_file(e2e.chat_file);
    FloodGuard guard(std::cerr, options.state_dir / "e2e-flood.csv", "e2e");
    auto bot = env.open(Account::kBot, std::cerr);
    std::string error;
    if (!wait_chat_known(bot->client(), chat_id, seconds{90}, &error)) {
        throw std::runtime_error("the bot does not see the channel: " + error);
    }
    const int64_t head = find_head_id(bot->client(), chat_id, seconds{150}, std::cout);
    if (head == 0) {
        std::cout << "there is no head to drop\n";
        return 1;
    }
    std::cout << "head message server id " << (head >> kServerIdShift) << "\n";
    guard.run("unpinAllChatMessages", [&] {
        return bot->client().call(
            td::Json{{"@type", "unpinAllChatMessages"}, {"chat_id", chat_id}});
    });
    bump_counter(options, "e2e.bot_actions", 1);
    guard.run("deleteMessages", [&] {
        return bot->client().call(td::Json{{"@type", "deleteMessages"},
                                           {"chat_id", chat_id},
                                           {"message_ids", td::Json::array({head})},
                                           {"revoke", true}});
    });
    bump_counter(options, "e2e.bot_actions", 1);
    // Кэш TDLib может ещё минуту показывать старую голову (SPEC 2): ждём, пока бот увидит
    // «закрепа нет» и «сообщения нет», но не дольше 90 с.
    bool no_pin = false;
    bool no_msg = false;
    for (int i = 0; i < 18 && !(no_pin && no_msg); ++i) {
        try {
            const td::Json pinned = bot->client().call(
                td::Json{{"@type", "getChatPinnedMessage"}, {"chat_id", chat_id}});
            no_pin = pinned.value("id", int64_t{0}) == 0;
        } catch (const td::TdException& e) {
            no_pin = e.error().code == 404;
        }
        try {
            const td::Json msgs =
                bot->client().call(td::Json{{"@type", "getMessages"},
                                            {"chat_id", chat_id},
                                            {"message_ids", td::Json::array({head})}});
            no_msg = msgs["messages"][0].is_null();
        } catch (const td::TdException&) {
            no_msg = true;
        }
        if (!(no_pin && no_msg)) {
            std::this_thread::sleep_for(seconds{5});
        }
    }
    std::cout << "after: pinned message " << (no_pin ? "none" : "STILL PRESENT")
              << ", head message " << (no_msg ? "deleted" : "STILL PRESENT") << "\n";
    return no_pin && no_msg ? 0 : 1;
}

}  // namespace

void add_e2e_command(CLI::App& app, const BenchOptions& options, int* exit_code) {
    auto* cmd = app.add_subcommand(
        "e2e", "Channel setup and teardown for the telega-fs E2E run, bot mode (SPEC 14)");
    cmd->require_subcommand(1);
    auto e2e = std::make_shared<E2eOptions>();
    cmd->add_option("--chat-file", e2e->chat_file,
                    "File with the channel id (outside the repository)")
        ->required()
        ->type_name("FILE");

    auto* setup =
        cmd->add_subcommand("setup", "Create the channel and add the bot as administrator");
    setup->add_option("--rights", e2e->rights, "Bot rights set (see e2 run --sets)")
        ->capture_default_str();
    setup->callback([&options, exit_code, e2e] {
        BenchEnv env(options);
        try {
            cmd_setup(env, options, *e2e);
            *exit_code = 0;
        } catch (const StopExperiment& ex) {
            std::cerr << "STOPPED: " << ex.what() << "\n";
            *exit_code = 2;
        }
    });

    cmd->add_subcommand("inspect", "Check what the bot sees: document names, captions, counts")
        ->callback([&options, exit_code, e2e] {
            BenchEnv env(options);
            *exit_code = cmd_inspect(env, *e2e);
        });

    cmd->add_subcommand("drop-head",
                        "Bot only: unpin everything and delete the head message (SPEC 7.2, M4)")
        ->callback([&options, exit_code, e2e] {
            BenchEnv env(options);
            try {
                *exit_code = cmd_drop_head(env, options, *e2e);
            } catch (const StopExperiment& ex) {
                std::cerr << "STOPPED: " << ex.what() << "\n";
                *exit_code = 2;
            }
        });

    cmd->add_subcommand("teardown", "Delete the channel and the --chat-file")
        ->callback([&options, exit_code, e2e] {
            BenchEnv env(options);
            try {
                cmd_teardown(env, options, *e2e);
                *exit_code = 0;
            } catch (const StopExperiment& ex) {
                std::cerr << "STOPPED: " << ex.what() << "\n";
                *exit_code = 2;
            }
        });
}

}  // namespace telega::bench
