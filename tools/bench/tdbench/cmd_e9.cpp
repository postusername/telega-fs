// e9: холодное чтение мелких файлов из пакетов (SPEC 17.12), только в режиме бота. Канал создаёт
// пользователь (только настройка), документы заливает и скачивает бот в своей сессии
// <state-dir>/bot-e9. Сообщений от бота немного: по одному документу на пакет и на размер файла.
//
//   tgfs-bench e9 setup      канал, бот-администратор, пакеты и отдельные документы (seed)
//   tgfs-bench e9 run        холодные чтения: основной сеанс бота и сеанс с пустой БД
//   tgfs-bench e9 teardown   удалить канал и состояние
//
// Что меряется. Пакет — документ из seed-данных длиной как зашифрованный чанк (SPEC 5: 40 байт
// заголовка, сегменты по 64 КиБ плюс 16 байт тега). Файл размером `len` лежит в пакете по
// смещению `off` открытого текста, читается диапазон зашифрованных сегментов (SPEC 5, 8.2):
// `downloadFile(offset, limit, synchronous)` после `deleteFile`, затем `pread` по local.path.
// Сравнивается с отдельным документом такого же размера (чанк из одного маленького файла),
// который читается целиком. Время = скачивание + чтение; данные сверяются с seed.

#include <sys/stat.h>

#include <algorithm>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <vector>

#include "commands.h"
#include "exp_common.h"
#include "run_common.h"

namespace telega::bench {

namespace {

namespace fs = std::filesystem;
using std::chrono::seconds;

constexpr const char* kState = "e9";
constexpr int64_t kKiB = 1024;
constexpr int64_t kMiB = 1024 * kKiB;
constexpr int64_t kSegment = 64 * kKiB;
constexpr int64_t kSegmentFull = kSegment + 16;  // сегмент с тегом
constexpr int64_t kHeader = 40;

struct E9Options {
    std::string file_kib = "2,32,64,256,512,1024,4096";
    std::string pack_mib = "4,16,64";
    int repeats = 5;
    uint64_t seed = 20261121;
    std::string rights = "only:post,edit,delete";  // как в E5 и E6
    std::string db = "both";                       // main | fresh | both
    fs::path out;
};

std::vector<int64_t> parse_list(const std::string& text) {
    std::vector<int64_t> out;
    std::istringstream in(text);
    std::string part;
    while (std::getline(in, part, ',')) {
        if (!part.empty()) {
            out.push_back(std::stoll(part));
        }
    }
    return out;
}

// Размер зашифрованного чанка с открытым текстом `plain` байт (SPEC 5).
int64_t encrypted_size(int64_t plain) {
    return kHeader + plain + ((plain + kSegment - 1) / kSegment) * 16;
}

struct Target {
    std::string kind;  // pack | single
    int64_t pack_mib = 0;
    int64_t file_kib = 0;
    std::string pos;  // start | middle | end (для single — whole)
    int64_t plain_offset = 0;
    int64_t enc_offset = 0;
    int64_t enc_len = 0;
};

// Диапазон шифртекста для открытого текста [off, off + len) в чанке длиной plain байт.
void segment_range(int64_t plain, int64_t off, int64_t len, int64_t* enc_off, int64_t* enc_len) {
    const int64_t first = off / kSegment;
    const int64_t last = (off + len - 1) / kSegment;
    *enc_off = kHeader + first * kSegmentFull;
    *enc_len = std::min(encrypted_size(plain), kHeader + (last + 1) * kSegmentFull) - *enc_off;
}

std::string date_stamp() {
    const std::string iso = iso_utc_now();
    return iso.substr(0, 4) + iso.substr(5, 2) + iso.substr(8, 2);
}

std::string yn(bool v) { return v ? "1" : "0"; }

struct Measurement {
    bool match = false;
    double download_ms = 0;
    double first_update_ms = -1;
    double read_ms = 0;
    int64_t downloaded_size = 0;
    int64_t prefix_size = 0;
    int64_t disk_alloc = -1;
    std::string error;
    [[nodiscard]] double total_ms() const { return download_ms + read_ms; }
};

using Key = std::tuple<std::string, std::string, int64_t, int64_t>;

class E9Bench {
  public:
    E9Bench(BenchEnv& env, const BenchOptions& options, const E9Options& e9, td::Json state)
        : env_(env),
          options_(options),
          e9_(e9),
          state_(std::move(state)),
          chat_id_(state_.at("chat_id").get<int64_t>()),
          guard_(std::cerr, e9.out / "flood.csv", "e9") {}

    void run() {
        Csv reads(
            e9_.out / "e9_reads.csv",
            {"time_utc", "variant", "repeat", "kind", "pack_mib", "file_kib", "pos", "plain_offset",
             "enc_offset", "enc_len", "download_ms", "first_update_ms", "read_ms", "total_ms",
             "match", "downloaded_size", "prefix_size", "disk_alloc", "error"});
        Csv lookup(e9_.out / "e9_lookup.csv", {"time_utc", "variant", "doc", "ok", "ms", "error"});
        if (e9_.db == "main" || e9_.db == "both") {
            const run::SessionLock lock(options_.state_dir / "bot-e9.lock", "e9");
            run_variant("main", env_.open_at(Account::kBot, session_dir(), std::cerr), reads,
                        lookup);
        }
        if (e9_.db == "fresh" || e9_.db == "both") {
            const fs::path dir = options_.state_dir / "bot-e9-fresh";
            remove_fresh_bot_dir(dir);
            run_variant("fresh", env_.open_at(Account::kBot, dir, std::cerr), reads, lookup);
            remove_fresh_bot_dir(dir);
        }
        write_summary();
    }

    [[nodiscard]] fs::path session_dir() const { return options_.state_dir / "bot-e9"; }

  private:
    struct Doc {
        std::string name;
        std::string kind;
        int64_t pack_mib = 0;
        int64_t file_kib = 0;
        int64_t plain = 0;
        uint64_t seed = 0;
        int64_t message_id = 0;
    };

    [[nodiscard]] std::vector<Doc> docs() const {
        std::vector<Doc> out;
        for (const td::Json& d : state_.at("docs")) {
            Doc x;
            x.name = d.at("name").get<std::string>();
            x.kind = d.at("kind").get<std::string>();
            x.pack_mib = d.at("pack_mib").get<int64_t>();
            x.file_kib = d.at("file_kib").get<int64_t>();
            x.plain = d.at("plain").get<int64_t>();
            x.seed = d.at("seed").get<uint64_t>();
            x.message_id = d.at("message_id").get<int64_t>();
            out.push_back(x);
        }
        return out;
    }

    Measurement measure(td::TdClient& c, int32_t file_id, const Doc& doc, const Target& t) {
        Measurement m;
        try {
            // файла на диске могло не быть
            attempt([&] { c.call(td::Json{{"@type", "deleteFile"}, {"file_id", file_id}}); });
            std::mutex mu;
            double first_ms = -1;
            const auto t0 = ExpClock::now();
            auto sub = c.subscribe("updateFile", [&](const td::Json& u) {
                if (!u.contains("file") || u["file"].value("id", int32_t{-1}) != file_id) {
                    return;
                }
                const td::Json& l = u["file"]["local"];
                const std::lock_guard<std::mutex> lock(mu);
                if (first_ms < 0 && l.value("downloaded_size", int64_t{0}) > 0) {
                    first_ms = ms_since(t0);
                }
            });
            const td::Json f = guard_.run("downloadFile", [&] {
                return c.call(td::Json{{"@type", "downloadFile"},
                                       {"file_id", file_id},
                                       {"priority", 1},
                                       {"offset", t.enc_offset},
                                       {"limit", t.enc_len},
                                       {"synchronous", true}},
                              seconds{300});
            });
            m.download_ms = ms_since(t0);
            sub.reset();
            {
                const std::lock_guard<std::mutex> lock(mu);
                m.first_update_ms = first_ms;
            }
            const std::string path = f["local"].value("path", std::string{});
            m.downloaded_size = f["local"].value("downloaded_size", int64_t{0});
            m.prefix_size = f["local"].value("downloaded_prefix_size", int64_t{0});
            struct stat st {};
            if (!path.empty() && ::stat(path.c_str(), &st) == 0) {
                m.disk_alloc = static_cast<int64_t>(st.st_blocks) * 512;
            }
            const auto t1 = ExpClock::now();
            std::string got(static_cast<size_t>(t.enc_len), '\0');
            {
                std::ifstream in(path, std::ios::binary);
                in.seekg(t.enc_offset);
                in.read(got.data(), static_cast<std::streamsize>(got.size()));
                if (in.gcount() != t.enc_len) {
                    m.error = "short read " + std::to_string(in.gcount());
                }
            }
            m.read_ms = ms_since(t1);
            std::string want;
            fill_seeded(doc.seed, static_cast<uint64_t>(t.enc_offset), want,
                        static_cast<size_t>(t.enc_len));
            m.match = m.error.empty() && got == want;
            if (m.error.empty() && !m.match) {
                m.error = "data mismatch";
            }
        } catch (const td::TdException& e) {
            m.error = e.error().to_string();
        }
        return m;
    }

    void run_variant(const std::string& variant, std::unique_ptr<BenchAccount> bot, Csv& reads,
                     Csv& lookup) {
        td::TdClient& c = bot->client();
        std::cerr << "=== variant: " << variant << "\n";
        std::string err;
        if (!wait_chat_known(c, chat_id_, seconds{90}, &err)) {
            throw std::runtime_error("the bot does not see the channel: " + err);
        }
        // file_id документов: getMessages (SPEC 8.2, основной путь по итогам E4).
        std::map<std::string, int32_t> file_ids;
        for (const Doc& d : docs()) {
            const auto t0 = ExpClock::now();
            try {
                const td::Json m = guard_.run("getMessages", [&] {
                    return c.call(td::Json{{"@type", "getMessages"},
                                           {"chat_id", chat_id_},
                                           {"message_ids", td::Json::array({d.message_id})}});
                });
                if (m["messages"].empty() || m["messages"][0].is_null()) {
                    lookup.row({iso_utc_now(), variant, d.name, "0", fmt(ms_since(t0)),
                                "message not returned"});
                    continue;
                }
                file_ids[d.name] =
                    m["messages"][0]["content"]["document"]["document"].value("id", int32_t{0});
                lookup.row({iso_utc_now(), variant, d.name, "1", fmt(ms_since(t0)), ""});
            } catch (const td::TdException& e) {
                lookup.row({iso_utc_now(), variant, d.name, "0", fmt(ms_since(t0)),
                            e.error().to_string()});
            }
        }
        const std::vector<Doc> all = docs();
        const std::vector<int64_t> sizes = parse_list(e9_.file_kib);
        for (int rep = 1; rep <= e9_.repeats; ++rep) {
            // Порядок внутри повтора: размер файла, затем пакеты и положения, затем отдельный
            // документ, чтобы дрейф сети не накапливался на одной ветке сравнения.
            for (const int64_t kib : sizes) {
                const int64_t len = kib * kKiB;
                for (const Doc& d : all) {
                    if (!file_ids.contains(d.name)) {
                        continue;
                    }
                    if (d.kind == "pack") {
                        for (const std::string_view pos : {"start", "middle", "end"}) {
                            Target t;
                            t.kind = "pack";
                            t.pack_mib = d.pack_mib;
                            t.file_kib = kib;
                            t.pos = std::string{pos};
                            // Середина — не на границе сегмента (файл лежит где угодно).
                            t.plain_offset = d.plain - len;  // end
                            if (pos == "start") {
                                t.plain_offset = 0;
                            } else if (pos == "middle") {
                                t.plain_offset = d.plain / 2 + 1000;
                            }
                            if (t.plain_offset < 0 || t.plain_offset + len > d.plain ||
                                (pos == "end" && t.plain_offset == 0)) {
                                continue;  // файл не помещается в пакет или конец совпал с началом
                            }
                            segment_range(d.plain, t.plain_offset, len, &t.enc_offset, &t.enc_len);
                            record(variant, rep, c, file_ids[d.name], d, t, reads);
                        }
                    } else if (d.file_kib == kib) {
                        Target t;
                        t.kind = "single";
                        t.file_kib = kib;
                        t.pos = "whole";
                        t.enc_offset = 0;
                        t.enc_len = encrypted_size(d.plain);
                        record(variant, rep, c, file_ids[d.name], d, t, reads);
                    }
                }
            }
        }
        for (const Doc& d : all) {
            if (file_ids.contains(d.name)) {
                attempt([&] {
                    c.call(td::Json{{"@type", "deleteFile"}, {"file_id", file_ids[d.name]}});
                });
            }
        }
    }

    void record(const std::string& variant, int rep, td::TdClient& c, int32_t file_id, const Doc& d,
                const Target& t, Csv& reads) {
        const Measurement m = measure(c, file_id, d, t);
        reads.row({iso_utc_now(), variant, std::to_string(rep), t.kind, std::to_string(t.pack_mib),
                   std::to_string(t.file_kib), t.pos, std::to_string(t.plain_offset),
                   std::to_string(t.enc_offset), std::to_string(t.enc_len), fmt(m.download_ms),
                   fmt(m.first_update_ms), fmt(m.read_ms), fmt(m.total_ms()), yn(m.match),
                   std::to_string(m.downloaded_size), std::to_string(m.prefix_size),
                   std::to_string(m.disk_alloc), m.error});
        if (m.match) {
            samples_[{variant, t.kind, t.pack_mib, t.file_kib}].push_back(m.total_ms());
        }
        std::cerr << variant << " #" << rep << " " << t.kind << " pack " << t.pack_mib
                  << " MiB, file " << t.file_kib << " KiB, " << t.pos << ": " << fmt(m.total_ms())
                  << " ms (download " << fmt(m.download_ms) << ", read " << fmt(m.read_ms)
                  << "), match " << yn(m.match) << " " << m.error << "\n";
        std::this_thread::sleep_for(std::chrono::milliseconds{200});
    }

    // Медиана по повторам и положениям, отношение «из пакета / отдельный документ».
    void write_summary() {
        Csv csv(e9_.out / "e9_summary.csv", {"variant", "pack_mib", "file_kib", "n", "median_ms",
                                             "min_ms", "max_ms", "single_median_ms", "ratio"});
        for (const auto& [key, values] : samples_) {
            const auto& [variant, kind, pack_mib, file_kib] = key;
            if (kind != "pack") {
                continue;
            }
            const Summary s = summarize(values);
            const auto single = samples_.find(Key{variant, "single", 0, file_kib});
            const double base = single == samples_.end() ? 0.0 : summarize(single->second).median;
            csv.row({variant, std::to_string(pack_mib), std::to_string(file_kib),
                     std::to_string(s.n), fmt(s.median), fmt(s.min), fmt(s.max), fmt(base),
                     base > 0 ? fmt(s.median / base, 2) : ""});
        }
    }

    BenchEnv& env_;
    const BenchOptions& options_;
    const E9Options& e9_;
    td::Json state_;
    const int64_t chat_id_;
    FloodGuard guard_;
    std::map<Key, std::vector<double>> samples_;
};

bool is_no_write_access(const td::TdException& e) {
    return e.error().code == 400 && e.error().message.find("write access") != std::string::npos;
}

// Setup возобновляем: если состояние уже есть (канал создан, часть документов залита), канал
// переиспользуется. Бот может ещё какое-то время не иметь права писать после добавления
// администратором («400: Have no write access to the chat»): отправка повторяется с паузой, после
// шести неудач права выставляются заново (одно действие пользователя) и сеанс бота открывается
// заново.
void cmd_setup(BenchEnv& env, const BenchOptions& options, const E9Options& e9) {
    FloodGuard guard(std::cerr, e9.out / "flood.csv", "e9");
    const fs::path session = options.state_dir / "bot-e9";
    const run::SessionLock lock(options.state_dir / "bot-e9.lock", "e9");
    auto bot = env.open_at(Account::kBot, session, std::cerr);
    td::Json state;
    int64_t chat_id = 0;
    std::string error;
    if (has_state(options, kState)) {
        state = load_state(options, kState);
        chat_id = state.at("chat_id").get<int64_t>();
        std::cerr << "resuming: channel exists, " << state.at("docs").size()
                  << " documents already uploaded\n";
    } else {
        auto user = env.open(Account::kUser, std::cerr);
        const std::string title = "tgfs-bench-e9-" + date_stamp();
        chat_id = create_channel_with_bot(*user, *bot, title, e9.rights, guard, std::cerr);
        bump_counter(options, "e9.user_actions", 3);
        state = td::Json{{"chat_id", chat_id},
                         {"title", title},
                         {"seed", e9.seed},
                         {"docs", td::Json::array()},
                         {"stage", "uploading"}};
        save_state(options, kState, state);
    }
    if (!wait_chat_known(bot->client(), chat_id, seconds{90}, &error)) {
        throw std::runtime_error("the bot does not see the channel: " + error);
    }

    struct Plan {
        std::string kind;
        int64_t pack_mib;
        int64_t file_kib;
        int64_t plain;
    };
    std::vector<Plan> plans;
    for (const int64_t mib : parse_list(e9.pack_mib)) {
        plans.push_back({"pack", mib, 0, mib * kMiB});
    }
    for (const int64_t kib : parse_list(e9.file_kib)) {
        plans.push_back({"single", 0, kib, kib * kKiB});
    }
    const auto send_with_retry = [&](const fs::path& file) {
        bool rights_reset = false;
        for (int attempt_no = 1;; ++attempt_no) {
            try {
                return guard.run("send", [&] {
                    return bot->tracker().send(send_document_request(chat_id, file), seconds{60},
                                               std::chrono::minutes{30});
                });
            } catch (const td::TdException& e) {
                if (!is_no_write_access(e) || attempt_no > 14) {
                    throw;
                }
                std::cerr << "no write access yet (attempt " << attempt_no << "), waiting\n";
                std::this_thread::sleep_for(seconds{15});
                if (attempt_no == 6 && !rights_reset) {
                    // Права дошли не так: выставить заново минимальным набором, как в E5 и E6.
                    rights_reset = true;
                    auto user = env.open(Account::kUser, std::cerr);
                    wait_chat_known(user->client(), chat_id, seconds{60}, &error);
                    add_bot_admin(*user, *bot, chat_id, "only:post,edit,delete", guard, std::cerr);
                    bump_counter(options, "e9.user_actions", 1);
                }
                if (attempt_no % 3 == 0) {
                    bot.reset();
                    bot = env.open_at(Account::kBot, session, std::cerr);
                    wait_chat_known(bot->client(), chat_id, seconds{90}, &error);
                }
            }
        }
    };
    // Не больше 10 отправок в минуту (SPEC 17.2), крупные документы по лимиту байт (E5) безопасны.
    SendPacer pacer(std::chrono::milliseconds{6500});
    const fs::path dir = options.state_dir / "e9";
    uint64_t index = 0;
    for (const Plan& p : plans) {
        const uint64_t seed = e9.seed + (++index);
        if (index <= state.at("docs").size()) {
            continue;  // уже залит
        }
        const auto size = static_cast<uint64_t>(encrypted_size(p.plain));
        const fs::path file = dir / (random_hex(16) + ".bin");
        write_seeded_file(file, seed, size);
        pacer.wait();
        const auto t0 = ExpClock::now();
        const td::Json msg = send_with_retry(file);
        bump_counter(options, "e9.bot_messages_sent", 1);
        const td::Json d{{"name", file.filename().string()},
                         {"kind", p.kind},
                         {"pack_mib", p.pack_mib},
                         {"file_kib", p.file_kib},
                         {"plain", p.plain},
                         {"enc_size", static_cast<int64_t>(size)},
                         {"seed", seed},
                         {"message_id", msg.value("id", int64_t{0})},
                         {"upload_s", ms_since(t0) / 1000.0}};
        state["docs"].push_back(d);
        save_state(options, kState, state);
        std::cerr << p.kind << " " << (p.kind == "pack" ? p.pack_mib : p.file_kib)
                  << (p.kind == "pack" ? " MiB" : " KiB") << " uploaded in "
                  << fmt(ms_since(t0) / 1000.0) << " s\n";
        std::error_code ec;
        fs::remove(file, ec);  // данные известны по seed
    }
    state["stage"] = "uploaded";
    state["uploaded_at"] = iso_utc_now();
    save_state(options, kState, state);
}

void cmd_teardown(BenchEnv& env, const BenchOptions& options, const E9Options& e9) {
    const td::Json state = load_state(options, kState);
    FloodGuard guard(std::cerr, e9.out / "flood.csv", "e9");
    auto user = env.open(Account::kUser, std::cerr);
    std::string error;
    const int64_t chat_id = state.at("chat_id").get<int64_t>();
    wait_chat_known(user->client(), chat_id, seconds{30}, &error);
    // Право удалить канал приходит с полной информацией о нём: без неё deleteChat отвечает
    // «The chat can't be deleted» (E3).
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
            // После старта процесса чат бывает неизвестен пользователю: подождать и повторить.
            if (attempt_no >= 5 || e.error().code != 400) {
                throw;
            }
            std::this_thread::sleep_for(seconds{3});
            wait_chat_known(user->client(), chat_id, seconds{20}, &error);
            load_full_info();
        }
    }
    bump_counter(options, "e9.user_actions", 1);
    std::error_code ec;
    fs::remove(options.state_dir / (std::string{kState} + ".json"), ec);
    fs::remove_all(options.state_dir / "e9", ec);
    std::cout << "channel deleted\n";
}

}  // namespace

void add_e9_command(CLI::App& app, const BenchOptions& options, int* exit_code) {
    auto* cmd =
        app.add_subcommand("e9", "Experiment E9: cold reads from packs, bot mode (SPEC 17.12)");
    cmd->require_subcommand(1);
    auto e9 = std::make_shared<E9Options>();
    e9->out = default_out_dir("09-e9-packs");
    cmd->add_option("--out", e9->out, "Directory for CSV data")->capture_default_str();
    cmd->add_option("--file-kib", e9->file_kib, "File sizes, KiB, comma-separated")
        ->capture_default_str();
    cmd->add_option("--pack-mib", e9->pack_mib, "Pack sizes, MiB, comma-separated")
        ->capture_default_str();
    cmd->add_option("--seed", e9->seed, "Seed of the document data")->capture_default_str();

    auto* setup = cmd->add_subcommand("setup", "Create the channel and upload packs and singles");
    setup->add_option("--rights", e9->rights, "Bot rights set (see e2 run --sets)")
        ->capture_default_str();
    setup->callback([&options, exit_code, e9] {
        BenchEnv env(options);
        try {
            cmd_setup(env, options, *e9);
            *exit_code = 0;
        } catch (const StopExperiment& e) {
            std::cerr << "STOPPED: " << e.what() << "\n";
            *exit_code = 2;
        }
    });

    auto* run = cmd->add_subcommand("run", "Cold read measurements");
    run->add_option("--repeats", e9->repeats, "Repeats per target (>= 5, SPEC 17.2)")
        ->capture_default_str();
    run->add_option("--db", e9->db, "main | fresh | both")->capture_default_str();
    run->callback([&options, exit_code, e9] {
        if (e9->repeats < 5) {
            std::cerr << "repeats must be >= 5 (SPEC 17.2)\n";
            *exit_code = 2;
            return;
        }
        BenchEnv env(options);
        try {
            E9Bench(env, options, *e9, load_state(options, kState)).run();
            *exit_code = 0;
        } catch (const StopExperiment& e) {
            std::cerr << "STOPPED: " << e.what() << "\n";
            *exit_code = 2;
        }
    });

    cmd->add_subcommand("teardown", "Delete the E9 channel and the state")
        ->callback([&options, exit_code, e9] {
            BenchEnv env(options);
            cmd_teardown(env, options, *e9);
            *exit_code = 0;
        });
}

}  // namespace telega::bench
