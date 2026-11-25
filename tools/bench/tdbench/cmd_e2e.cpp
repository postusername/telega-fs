// e2e: подготовка и уборка канала для E2E-прогона telega-fs в режиме бота. Аккаунт пользователя
// здесь только настраивает: создаёт приватный канал, добавляет бота администратором (публиковать,
// редактировать, удалять сообщения) и удаляет канал в конце. Ни одного сообщения с него. Нагрузку
// несёт сам telega-fs под ботом. id канала лежит только в файле --chat-file (0600), вне
// репозитория.
//
// Если у контрибьютора уже есть канал, где бот администратор, аккаунт пользователя не нужен
// совсем: tests/e2e/*.sh принимают E2E_CHAT_ID и вызывают только подкоманды, которые работают
// под ботом (check-empty, inspect, drop-head, wipe).
//
//   tgfs-bench e2e setup    --chat-file FILE   создать канал `tgfs-e2e-<дата>` и записать его id
//                                              (аккаунт пользователя)
//   tgfs-bench e2e inspect  --chat-file FILE   что бот видит в канале: типы сообщений, имена
//                                              документов, подписи; без id и имён
//   tgfs-bench e2e check-empty --chat-file FILE  БОТ проверяет, что в канале нет закрепа и
//                                              документов (текст и прочее не считаются); для
//                                              верхней границы id шлёт и удаляет текст-зонд;
//                                              код 3 — не пуст, 2 — проверить не удалось
//   tgfs-bench e2e drop-head --chat-file FILE  БОТ снимает закрепы и удаляет сообщения голов
//                                              (<16 hex>.idx), чужой закреп не трогает
//                                              (сценарий «потеря головы»)
//   tgfs-bench e2e wipe     --chat-file FILE   БОТ снимает закреп головы (<16 hex>.idx) и удаляет
//                                              документы telega-fs (<16 hex>.bin|.idx) и
//                                              служебные сообщения об их закрепе; чужое закреплено
//                                              (код 3) или потолок перебора (код 2): ничего не
//                                              удаляется; чужие сообщения не трогает
//   tgfs-bench e2e teardown --chat-file FILE   удалить канал и файл (аккаунт пользователя)

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
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "commands.h"
#include "exp_common.h"

namespace telega::bench {

namespace {

using std::chrono::seconds;
constexpr int64_t kServerIdShift = 20;

struct E2eOptions {
    fs::path chat_file;
    std::string rights = "only:post,edit,delete";
    seconds scan_pause{
        10};  // пауза между пачками getMessages (как read_pause в E3); тесты ставят 0
    int64_t scan_max_id = 100000;  // check-empty, wipe: потолок серверного id зонда (и перебора)
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

// Закреплённое сообщение канала (голова) с учётом кэша TDLib: ChannelFull у бота обновляется раз в
// 60 с, и свежий процесс может до минуты видеть старую голову (после unpin старой «закреплённого
// нет», 404; после удаления закреплённого сообщения ещё какое-то время «закреплено» уже
// несуществующее). Повторяем до `window`, раз в 10 с, с принудительным getSupergroupFullInfo.
// Пустой Json — закрепа нет: 404 держался всё окно, или закреплённого сообщения уже нет. Любая
// другая ошибка TDLib пробрасывается: «не знаю» нельзя выдавать за «нет».
td::Json find_pinned(td::TdClient& client, int64_t chat_id, seconds window, std::ostream& log) {
    const auto deadline = std::chrono::steady_clock::now() + window;
    for (;;) {
        try {
            td::Json pinned =
                client.call(td::Json{{"@type", "getChatPinnedMessage"}, {"chat_id", chat_id}});
            const int64_t id = pinned.value("id", int64_t{0});
            if (id == 0) {
                return {};
            }
            const td::Json check = client.call(td::Json{{"@type", "getMessages"},
                                                        {"chat_id", chat_id},
                                                        {"message_ids", td::Json::array({id})}});
            if (!check["messages"][0].is_null()) {
                return pinned;
            }
        } catch (const td::TdException& e) {
            if (e.error().code != 404) {
                throw;
            }
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            return {};
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

// Id головы или 0, если закрепа нет.
int64_t find_head_id(td::TdClient& client, int64_t chat_id, seconds window, std::ostream& log) {
    const td::Json pinned = find_pinned(client, chat_id, window, log);
    return pinned.is_null() ? 0 : pinned.value("id", int64_t{0});
}

constexpr seconds kHeadWindow{
    60};  // окно ожидания закрепа у check-empty, wipe, drop-head (повторы)

// Бот работает только с каналами: тип чата supergroup с is_channel. Иначе ошибка (код 2 у
// вызывающего): чужой id группы или личного чата не должен попасть под уборку.
void require_channel(td::TdClient& client, int64_t chat_id) {
    const td::Json chat = client.call(td::Json{{"@type", "getChat"}, {"chat_id", chat_id}});
    const td::Json& type = chat["type"];
    if (type.value("@type", std::string{}) != "chatTypeSupergroup" ||
        !type.value("is_channel", false)) {
        throw std::runtime_error(
            "the chat is not a channel (E2E_CHAT_ID must be the id of a channel, -100...)");
    }
}

// Закреплённое сообщение — голова telega-fs: документ `<16 hex>.idx`.
bool is_head_document(const td::Json& message) {
    static const std::regex head_re("^[0-9a-f]{16}\\.idx$");
    const td::Json& c = message["content"];
    return c.value("@type", std::string{}) == "messageDocument" &&
           std::regex_match(c["document"].value("file_name", std::string{}), head_re);
}

// Документ telega-fs: `<16 hex>.bin` или `<16 hex>.idx`.
bool is_telega_document(const td::Json& message) {
    static const std::regex name_re("^[0-9a-f]{16}\\.(bin|idx)$");
    const td::Json& c = message["content"];
    return c.value("@type", std::string{}) == "messageDocument" &&
           std::regex_match(c["document"].value("file_name", std::string{}), name_re);
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

// Перебор id 1..голова+3 через getMessages пачками по 100 с паузой 10 с (чтобы не упереться в
// лимиты запросов бота) и проверка формата: документы называются `<16 hex>.bin` или `<16 hex>.idx`,
// подписей нет. Печатает только счётчики и размеры.
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

// Потеря головы: бот по одной снимает закрепы и удаляет сообщения голов telega-fs (документы
// `<16 hex>.idx`), пока что-то закреплено: старые головы тоже закреплены, и без них вместо
// «головы нет» получился бы откат. Закреплено что-то другое: отказ (код 3), чужое не трогаем.
// Аккаунт пользователя не участвует. Печатает итог `after: pinned message none, head message
// deleted`; код 0, если закрепа больше нет и сообщения голов удалены, 1 — головы не было или
// удаление не подтвердилось.
int cmd_drop_head(BenchEnv& env, const BenchOptions& options, const E2eOptions& e2e) {
    const int64_t chat_id = read_chat_file(e2e.chat_file);
    FloodGuard guard(std::cerr, options.state_dir / "e2e-flood.csv", "e2e");
    auto bot = env.open(Account::kBot, std::cerr);
    std::string error;
    if (!wait_chat_known(bot->client(), chat_id, seconds{90}, &error)) {
        throw std::runtime_error("the bot does not see the channel: " + error);
    }
    require_channel(bot->client(), chat_id);
    std::vector<int64_t> dropped;
    seconds window{150};
    bool pin_left = true;
    for (int round = 0; round < 100; ++round) {
        const td::Json pinned = find_pinned(bot->client(), chat_id, window, std::cout);
        window = kHeadWindow;
        if (pinned.is_null()) {
            pin_left = false;
            break;
        }
        if (!is_head_document(pinned)) {
            std::cout << "REFUSED: the pinned message is not a telega-fs head (<16 hex>.idx); "
                      << dropped.size() << " head(s) dropped before it\n";
            return 3;
        }
        const int64_t head = pinned.value("id", int64_t{0});
        std::cout << "head message server id " << (head >> kServerIdShift) << "\n";
        guard.run("unpinChatMessage", [&] {
            return bot->client().call(td::Json{
                {"@type", "unpinChatMessage"}, {"chat_id", chat_id}, {"message_id", head}});
        });
        bump_counter(options, "e2e.bot_actions", 1);
        guard.run("deleteMessages", [&] {
            return bot->client().call(td::Json{{"@type", "deleteMessages"},
                                               {"chat_id", chat_id},
                                               {"message_ids", td::Json::array({head})},
                                               {"revoke", true}});
        });
        bump_counter(options, "e2e.bot_actions", 1);
        dropped.push_back(head);
    }
    if (dropped.empty()) {
        std::cout << "there is no head to drop\n";
        return 1;
    }
    // Кэш TDLib может ещё минуту показывать старые сообщения: ждём, пока бот увидит «сообщений
    // нет», но не дольше 90 с.
    bool no_msg = false;
    for (int i = 0; i < 18 && !no_msg; ++i) {
        try {
            td::Json ids = td::Json::array();
            for (const int64_t id : dropped) {
                ids.push_back(id);
            }
            const td::Json msgs = bot->client().call(
                td::Json{{"@type", "getMessages"}, {"chat_id", chat_id}, {"message_ids", ids}});
            no_msg = true;
            for (const td::Json& m : msgs["messages"]) {
                no_msg = no_msg && m.is_null();
            }
        } catch (const td::TdException&) {
            no_msg = true;
        }
        if (!no_msg) {
            std::this_thread::sleep_for(seconds{5});
        }
    }
    std::cout << dropped.size() << " head(s) dropped\n"
              << "after: pinned message " << (pin_left ? "STILL PRESENT" : "none")
              << ", head message " << (no_msg ? "deleted" : "STILL PRESENT") << "\n";
    return !pin_left && no_msg ? 0 : 1;
}

constexpr const char* kProbeText = "telega-fs e2e probe";
// Бот не удаляет в канале сообщения старше двух суток (TDLib), и одно такое сообщение отклоняет
// всю пачку deleteMessages. Берём запас в 10 минут.
constexpr int64_t kBotDeleteMaxAgeS = 2 * 24 * 3600 - 600;

struct ScanSummary {
    bool ceiling = false;  // id зонда выше --scan-max-id: ничего не проверено
    bool stopped_early = false;  // visit попросил остановиться (найден документ)
    bool probe_deleted = false;
    int64_t probe_id = 0;  // серверный id зонда: верхняя граница перебора
    int64_t checked_up_to = 0;
    int batches = 0;
};

// Текст-зонд: его серверный id U выше id любого сообщения канала, отправленного до него. Это
// настоящая верхняя граница перебора: ни дыра из удалённых id, ни голова, ни getChat.last_message
// (бот его не получает) границей служить не могут. Возвращает итоговое сообщение (после
// updateMessageSendSucceeded).
template <typename Bot>
td::Json send_probe(Bot& bot, FloodGuard& guard, const BenchOptions& options, int64_t chat_id) {
    const td::Json request{
        {"@type", "sendMessage"},
        {"chat_id", chat_id},
        {"options", {{"@type", "messageSendOptions"}, {"disable_notification", true}}},
        {"input_message_content",
         {{"@type", "inputMessageText"},
          {"text", {{"@type", "formattedText"}, {"text", kProbeText}}}}}};
    td::Json message = guard.run(
        "sendMessage", [&] { return bot.tracker().send(request, seconds{30}, seconds{120}); });
    bump_counter(options, "e2e.bot_actions", 1);
    return message;
}

// Удаляет зонд (своё свежее сообщение). false — не вышло: это не ошибка проверки.
template <typename Bot>
bool delete_probe(Bot& bot, FloodGuard& guard, const BenchOptions& options, int64_t chat_id,
                  int64_t message_id) {
    try {
        guard.run("deleteMessages", [&] {
            return bot.client().call(td::Json{{"@type", "deleteMessages"},
                                              {"chat_id", chat_id},
                                              {"message_ids", td::Json::array({message_id})},
                                              {"revoke", true}});
        });
        bump_counter(options, "e2e.bot_actions", 1);
        return true;
    } catch (const std::exception& e) {
        std::cerr << "deleting the probe message: " << e.what() << "\n";
        return false;
    }
}

// Перебор серверных id 1..U пачками по 100 через getMessages, где U — id зонда. Потолок
// `scan_max_id`: U выше него — ceiling, перебора нет. Между пачками пауза 10 с, растущая (до 2
// раз) после FLOOD_WAIT. Ответ getMessages обязан быть массивом длины запроса, иначе
// runtime_error (а не «пустая пачка»). Сам зонд из getMessages обязан вернуться (иначе бот не
// видит сообщений канала): runtime_error. `visit(message)` (зонд ему не передаётся) возвращает
// true, чтобы остановить перебор. Зонд удаляется на любом выходе.
template <typename Bot, typename Visit>
ScanSummary scan_channel(Bot& bot, FloodGuard& guard, const BenchOptions& options, int64_t chat_id,
                         const E2eOptions& e2e, Visit&& visit) {
    ScanSummary r;
    const td::Json probe = send_probe(bot, guard, options, chat_id);
    const int64_t probe_message_id = probe.value("id", int64_t{0});
    r.probe_id = probe_message_id >> kServerIdShift;
    const auto drop_probe = [&] {
        r.probe_deleted = delete_probe(bot, guard, options, chat_id, probe_message_id);
    };
    try {
        if (r.probe_id == 0) {
            throw std::runtime_error("the probe message has no final id");
        }
        if (r.probe_id > e2e.scan_max_id) {
            r.ceiling = true;
            drop_probe();
            return r;
        }
        bool probe_seen = false;
        double factor = 1.0;
        int events_seen = guard.events();
        for (int64_t first = 1; first <= r.probe_id; first += 100) {
            const int64_t last = std::min(first + 99, r.probe_id);
            td::Json ids = td::Json::array();
            for (int64_t sid = first; sid <= last; ++sid) {
                ids.push_back(sid << kServerIdShift);
            }
            const td::Json reply = guard.run("getMessages", [&] {
                return bot.client().call(
                    td::Json{{"@type", "getMessages"}, {"chat_id", chat_id}, {"message_ids", ids}});
            });
            if (!reply.contains("messages") || !reply["messages"].is_array() ||
                reply["messages"].size() != ids.size()) {
                throw std::runtime_error(
                    "getMessages returned an unexpected reply (not an array "
                    "of the requested length)");
            }
            bool stop = false;
            for (size_t i = 0; i < ids.size(); ++i) {
                const td::Json& m = reply["messages"][i];
                if (m.is_null()) {
                    continue;
                }
                const int64_t id = m.value("id", int64_t{0});
                if (id != ids[i].get<int64_t>()) {
                    throw std::runtime_error("getMessages returned a message with another id");
                }
                if (id == probe_message_id) {
                    probe_seen = true;
                    continue;
                }
                stop = visit(m) || stop;
            }
            r.checked_up_to = last;
            ++r.batches;
            if (stop) {
                r.stopped_early = true;
                break;
            }
            if (last >= r.probe_id) {
                break;
            }
            if (guard.events() > events_seen) {
                factor = std::min(2.0, factor * 1.15);
                events_seen = guard.events();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds{static_cast<int64_t>(
                static_cast<double>(e2e.scan_pause.count()) * 1000.0 * factor)});
        }
        if (!r.stopped_early && !probe_seen) {
            throw std::runtime_error(
                "the bot cannot read the messages of the channel: the probe message is not "
                "returned by getMessages");
        }
    } catch (...) {
        drop_probe();
        throw;
    }
    drop_probe();
    return r;
}

void print_scan(const ScanSummary& r, const E2eOptions& e2e) {
    if (r.ceiling) {
        std::cout << "scan: SCAN LIMIT: the probe message has server id " << r.probe_id
                  << ", above --scan-max-id " << e2e.scan_max_id << "; nothing was checked\n";
    } else {
        std::cout << "scan: checked server ids 1.." << r.checked_up_to << " of " << r.probe_id
                  << " (the probe message is the upper bound) in " << r.batches << " batch(es), "
                  << (r.stopped_early ? "stopped early (a document was found)"
                                      : "the whole channel up to the probe")
                  << "\n";
    }
    std::cout << (r.probe_deleted
                      ? "probe: deleted\n"
                      : "probe: the text message \"" + std::string{kProbeText} + "\" (server id " +
                            std::to_string(r.probe_id) + ") was NOT deleted: delete it by hand\n");
}

// Проверка перед прогоном в чужом (уже готовом) канале. «Пусто» здесь значит: нет закрепа и нет
// документов; текст, фото, служебные сообщения и прочее не считаются. Иначе прогон мог бы
// затереть чужую файловую систему. Перебор 1..U, где U — id зонда (см. send_probe). Коды: 0 —
// пусто, 3 — не пусто, 2 — проверить не удалось (ошибка TDLib, не канал, канал не виден боту,
// потолок --scan-max-id, неожиданный ответ getMessages).
int cmd_check_empty(BenchEnv& env, const BenchOptions& options, const E2eOptions& e2e) {
    const int64_t chat_id = read_chat_file(e2e.chat_file);
    FloodGuard guard(std::cerr, options.state_dir / "e2e-flood.csv", "e2e");
    auto bot = env.open(Account::kBot, std::cerr);
    std::string error;
    if (!wait_chat_known(bot->client(), chat_id, seconds{90}, &error)) {
        throw std::runtime_error("the bot does not see the channel (is it an administrator?): " +
                                 error);
    }
    require_channel(bot->client(), chat_id);
    const td::Json pinned = find_pinned(bot->client(), chat_id, kHeadWindow, std::cerr);
    if (!pinned.is_null()) {
        std::cout << "NOT EMPTY: the channel has a pinned message (the head of a telega-fs "
                     "filesystem or another pin)\n";
        return 3;
    }
    int documents = 0;
    const ScanSummary scan =
        scan_channel(*bot, guard, options, chat_id, e2e, [&](const td::Json& m) {
            if (m["content"].value("@type", std::string{}) == "messageDocument") {
                ++documents;
                return true;
            }
            return false;
        });
    print_scan(scan, e2e);
    if (scan.ceiling) {
        std::cout << "CANNOT VERIFY: the scan limit was reached\n";
        return 2;
    }
    if (documents != 0) {
        std::cout << "NOT EMPTY: the channel has documents, no pinned message\n";
        return 3;
    }
    std::cout << "CHANNEL EMPTY: no pinned message and no documents (text, photos and other "
                 "messages are not counted)\n";
    return 0;
}

struct Victim {
    int64_t id;
    int64_t date;  // unix-время сообщения, 0 — неизвестно
};

struct DeleteResult {
    size_t deleted = 0;
    size_t too_old = 0;  // бот не удаляет сообщения старше двух суток: удалять вручную
    size_t failed = 0;   // остальные сбои
};

// Удаляет сообщения пачками по 100. Слишком старые (бот их удалить не может, а одно такое
// отклоняет всю пачку) пропускаются и считаются в too_old. Пачка отклонена: повтор по одному id,
// неудавшиеся считаются в too_old (если по дате старые) или failed. `pause` между пачками.
template <typename Bot>
DeleteResult delete_victims(Bot& bot, FloodGuard& guard, const BenchOptions& options,
                            int64_t chat_id, const std::vector<Victim>& victims, int64_t now,
                            seconds pause) {
    DeleteResult r;
    const auto too_old = [&](const Victim& v) {
        return v.date > 0 && now - v.date >= kBotDeleteMaxAgeS;
    };
    const auto delete_ids = [&](const std::vector<int64_t>& ids_vec) {
        td::Json ids = td::Json::array();
        for (const int64_t id : ids_vec) {
            ids.push_back(id);
        }
        guard.run("deleteMessages", [&] {
            return bot.client().call(td::Json{{"@type", "deleteMessages"},
                                              {"chat_id", chat_id},
                                              {"message_ids", ids},
                                              {"revoke", true}});
        });
        bump_counter(options, "e2e.bot_actions", 1);
    };
    std::vector<Victim> deletable;
    for (const Victim& v : victims) {
        if (too_old(v)) {
            ++r.too_old;
        } else {
            deletable.push_back(v);
        }
    }
    for (size_t i = 0; i < deletable.size(); i += 100) {
        const size_t end = std::min(deletable.size(), i + 100);
        std::vector<int64_t> batch;
        for (size_t j = i; j < end; ++j) {
            batch.push_back(deletable[j].id);
        }
        try {
            delete_ids(batch);
            r.deleted += batch.size();
        } catch (const td::TdException& e) {
            std::cerr << "deleteMessages (batch): " << e.error().to_string()
                      << "; retrying one by one\n";
            for (size_t j = i; j < end; ++j) {
                try {
                    delete_ids({deletable[j].id});
                    ++r.deleted;
                } catch (const td::TdException& e1) {
                    std::cerr << "deleteMessages (one): " << e1.error().to_string() << "\n";
                    if (too_old(deletable[j])) {
                        ++r.too_old;
                    } else {
                        ++r.failed;
                    }
                }
            }
        }
        if (end < deletable.size()) {
            std::this_thread::sleep_for(pause);
        }
    }
    return r;
}

// Уборка в чужом канале без удаления самого канала: бот снимает закреп головы и удаляет документы
// telega-fs (имя `<16 hex>.bin|.idx`) и служебные сообщения о закрепе таких документов. Всё
// остальное остаётся. Если закреплено что-то кроме `<16 hex>.idx`, отказ и ничего не удаляется.
// Сообщения старше двух суток бот удалить не может: их не трогаем и печатаем число (удалять
// вручную); при ошибке пачки удаляем по одному id. Коды: 0 — удалено, 1 — не всё удалено (слишком
// старые или другие сбои), 2 — ошибка или потолок (ничего не удалено), 3 — закреплено чужое.
int cmd_wipe(BenchEnv& env, const BenchOptions& options, const E2eOptions& e2e) {
    const int64_t chat_id = read_chat_file(e2e.chat_file);
    FloodGuard guard(std::cerr, options.state_dir / "e2e-flood.csv", "e2e");
    auto bot = env.open(Account::kBot, std::cerr);
    std::string error;
    if (!wait_chat_known(bot->client(), chat_id, seconds{90}, &error)) {
        throw std::runtime_error("the bot does not see the channel: " + error);
    }
    require_channel(bot->client(), chat_id);
    const td::Json pinned = find_pinned(bot->client(), chat_id, kHeadWindow, std::cerr);
    int64_t pinned_id = 0;
    if (!pinned.is_null()) {
        if (!is_head_document(pinned)) {
            std::cout << "REFUSED: the pinned message is not a telega-fs head (<16 hex>.idx); "
                         "nothing was deleted\n";
            return 3;
        }
        pinned_id = pinned.value("id", int64_t{0});
    }
    std::map<int64_t, int64_t> docs;  // id -> date
    struct PinMessage {
        int64_t id;
        int64_t target;
        int64_t date;
    };
    std::vector<PinMessage> pin_messages;
    int kept = 0;
    const ScanSummary scan =
        scan_channel(*bot, guard, options, chat_id, e2e, [&](const td::Json& m) {
            const std::string type = m["content"].value("@type", std::string{});
            const int64_t id = m.value("id", int64_t{0});
            const int64_t date = m.value("date", int64_t{0});
            if (is_telega_document(m)) {
                docs[id] = date;
            } else if (type == "messagePinMessage") {
                pin_messages.push_back({id, m["content"].value("message_id", int64_t{0}), date});
            } else if (type == "messageDocument" || type == "messageText") {
                ++kept;  // чужое содержимое; служебные сообщения (создание канала) не считаем
            }
            return false;
        });
    print_scan(scan, e2e);
    if (scan.ceiling) {
        std::cout << "CANNOT WIPE: the scan limit was reached; nothing was deleted\n";
        return 2;
    }
    std::vector<Victim> victims;
    victims.reserve(docs.size() + pin_messages.size());
    for (const auto& [id, date] : docs) {
        victims.push_back({id, date});
    }
    for (const PinMessage& pm : pin_messages) {
        if (docs.contains(pm.target)) {
            victims.push_back({pm.id, pm.date});
        }
    }
    // Закреп снимаем только у головы telega-fs; старые закрепы наших документов уходят вместе с
    // самими сообщениями.
    if (pinned_id != 0) {
        try {
            guard.run("unpinChatMessage", [&] {
                return bot->client().call(td::Json{{"@type", "unpinChatMessage"},
                                                   {"chat_id", chat_id},
                                                   {"message_id", pinned_id}});
            });
            bump_counter(options, "e2e.bot_actions", 1);
        } catch (const td::TdException& e) {
            std::cerr << "unpinChatMessage: " << e.error().to_string() << "\n";
        }
    }
    const DeleteResult dr = delete_victims(*bot, guard, options, chat_id, victims,
                                           static_cast<int64_t>(std::time(nullptr)), seconds{5});
    const size_t deleted = dr.deleted;
    const size_t old_count = dr.too_old;
    const size_t failed = dr.failed;
    std::cout << "wipe: head " << (pinned_id != 0 ? "unpinned" : "not found") << ", deleted "
              << deleted << " of " << victims.size() << " telega-fs message(s), left " << kept
              << " other document/text message(s)\n";
    if (old_count != 0) {
        std::cout << "wipe: " << old_count
                  << " telega-fs message(s) are too old for the bot to delete (the limit is two "
                     "days): delete them by hand\n";
    }
    if (failed != 0) {
        std::cout << "wipe: " << failed << " telega-fs message(s) could not be deleted\n";
    }
    return old_count == 0 && failed == 0 ? 0 : 1;
}

}  // namespace

void add_e2e_command(CLI::App& app, const BenchOptions& options, int* exit_code) {
    auto* cmd = app.add_subcommand(
        "e2e", "Channel setup, checks and cleanup for the telega-fs E2E run, bot mode");
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
                        "Bot only: unpin and delete the telega-fs head message(s), nothing else")
        ->callback([&options, exit_code, e2e] {
            BenchEnv env(options);
            try {
                *exit_code = cmd_drop_head(env, options, *e2e);
            } catch (const StopExperiment& ex) {
                std::cerr << "STOPPED: " << ex.what() << "\n";
                *exit_code = 2;
            } catch (const std::exception& ex) {
                std::cerr << "ERROR: " << ex.what() << "\n";
                *exit_code = 2;
            }
        });

    const auto add_scan_options = [e2e](CLI::App* sub) {
        sub->add_option(
               "--scan-max-id", e2e->scan_max_id,
               "Ceiling of the probe message server id (above it: error, nothing is checked)")
            ->capture_default_str();
    };

    auto* check_empty = cmd->add_subcommand("check-empty",
                                            "Bot only: no pinned message and no documents in the "
                                            "channel (exit 3 if not, 2 if unknown)");
    add_scan_options(check_empty);
    check_empty->callback([&options, exit_code, e2e] {
        BenchEnv env(options);
        try {
            *exit_code = cmd_check_empty(env, options, *e2e);
        } catch (const std::exception& ex) {
            std::cerr << "ERROR: " << ex.what() << "\n";
            *exit_code = 2;
        }
    });

    auto* wipe = cmd->add_subcommand(
        "wipe",
        "Bot only: unpin the telega-fs head and delete telega-fs documents; keeps the channel");
    add_scan_options(wipe);
    wipe->callback([&options, exit_code, e2e] {
        BenchEnv env(options);
        try {
            *exit_code = cmd_wipe(env, options, *e2e);
        } catch (const StopExperiment& ex) {
            std::cerr << "STOPPED: " << ex.what() << "\n";
            *exit_code = 2;
        } catch (const std::exception& ex) {
            std::cerr << "ERROR: " << ex.what() << "\n";
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
