#pragma once

// Имитация канала Telegram поверх FakeTransport для тестов TelegramStore: сообщения с
// документами, закрепления, getMessages / downloadFile / getRemoteFile, апдейты отправки и
// программируемые сбои. Повторяет особенности TDLib из SPEC 2: getChat первым запросом,
// getMessages возвращает null для отсутствующих, unpin и удаление требуют, чтобы TDLib «знал»
// сообщение (после forget_session() — нет), getChatPinnedMessage без закреплённых — 404,
// отправка приходит апдейтом (возможно, раньше ответа или с задержкой, и тогда deleteMessages по
// временному id отменяет заливку, как в TDLib), отправленный файл у TDLib «локальный», а его
// ссылку потом убирают: downloadFile сам это замечает и качает заново (FileManager::
// check_local_location), deleteFile для такого файла ничего не делает (путь вне files_directory).

#include <nlohmann/json.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "../td/fake_transport.h"

namespace telega::store::test {

using td::Json;
using td::test::FakeTransport;

inline constexpr int64_t kChatId = -1001234567890;
inline constexpr int64_t kShift = int64_t{1} << 20;

inline Json td_error(int code, const std::string& message) {
    return Json{{"@type", "error"}, {"code", code}, {"message", message}};
}

class FakeTelegram {  // NOLINT(clang-analyzer-optin.performance.Padding): тестовый фейк
    mutable std::mutex mu_;

    template <class F>
    auto with_lock(F&& f) const {
        const std::lock_guard<std::mutex> lock(mu_);
        return f();
    }

  public:
    struct Message {
        int64_t id = 0;
        std::string name;
        std::string content;
        int32_t file_id = 0;
        std::string remote_id;
        bool pinned = false;
    };

    // Решение об исходе документа отправки: ошибка из апдейта updateMessageSendFailed или nullopt.
    // index — позиция в альбоме (0 для одиночной отправки).
    using Verdict = std::function<std::optional<Json>(const std::string& name, size_t index)>;

    ~FakeTelegram() {
        std::vector<std::thread> threads;
        {
            const std::lock_guard<std::mutex> lock(mu_);
            threads.swap(delayed_);
        }
        for (auto& t : threads) {
            t.join();
        }
    }
    FakeTelegram(const FakeTelegram&) = delete;
    FakeTelegram& operator=(const FakeTelegram&) = delete;

    FakeTelegram(FakeTransport& transport, std::filesystem::path download_dir)
        : transport_(transport), download_dir_(std::move(download_dir)) {
        std::filesystem::create_directories(download_dir_);
        transport_.set_responder([this](FakeTransport& t, int client, const Json& request) {
            handle(t, client, request);
        });
    }

    // ---- Настройка ----

    void set_auto_delete(int32_t seconds) {
        with_lock([&] { auto_delete_ = seconds; });
    }
    void set_is_channel(bool v) {
        with_lock([&] { is_channel_ = v; });
    }
    void set_member_status(Json status) {
        with_lock([&] { member_status_ = std::move(status); });
    }
    void set_verdict(Verdict verdict) {
        with_lock([&] { verdict_ = std::move(verdict); });
    }
    void set_early_updates(bool v) {
        with_lock([&] { early_updates_ = v; });
    }
    // Не слать апдейты updateMessageSend* (итог отправки так и не придёт).
    void set_suppress_updates(bool v) {
        with_lock([&] { suppress_updates_ = v; });
    }
    void set_remote_size_zero(bool v) {
        with_lock([&] { remote_size_zero_ = v; });
    }
    void set_send_delay(std::chrono::milliseconds d) {
        with_lock([&] { send_delay_ = d; });
    }
    void set_download_delay(std::chrono::milliseconds d) {
        with_lock([&] { download_delay_ = d; });
    }
    // Как TDLib: новый downloadFile для файла, который уже скачивается (нужен download_delay),
    // отменяет прежний запрос: тот отвечает 200 «Canceled by another downloadFile request».
    void set_cancel_overlapping_downloads(bool v) {
        with_lock([&] { cancel_overlap_ = v; });
    }
    // Итог отправки (updateMessageSend*) приходит через d после ответа на sendMessage. Сообщение в
    // канале появляется только к этому моменту, если заливку не отменили (см.
    // set_cancel_on_delete).
    void set_update_delay(std::chrono::milliseconds d) {
        with_lock([&] { update_delay_ = d; });
    }
    // true (как в TDLib): deleteMessages по временному id ещё не завершённой отправки отменяет её.
    // false: удаление временного id ничего не меняет (худший случай: итог уже в пути).
    void set_cancel_on_delete(bool v) {
        with_lock([&] { cancel_on_delete_ = v; });
    }
    // Сообщение отправлено и итог приходит, а ответ на sendMessage теряется (таймаут вызова).
    void set_lose_send_response(bool v) {
        with_lock([&] { lose_send_response_ = v; });
    }
    // Отправленные этим процессом файлы «локальные» по версии TDLib, но самого файла уже нет
    // (ссылку убрали). Как в TDLib, первый downloadFile такого файла замечает это, забывает
    // локальное расположение и качает с сервера. deleteFile для него ничего не делает.
    void set_sent_files_stale(bool v) {
        with_lock([&] { stale_sends_ = v; });
    }
    // Гонка, которую TDLib не заметил: следующие n ответов downloadFile — «скачано» с путём в
    // никуда (локальный файл исчез после проверки). Чтение получает ENOENT и повторяет запрос.
    void vanish_next_downloads(int n) {
        with_lock([&] { vanish_downloads_ = n; });
    }
    // Следующие n downloadFile отменяются: 400 «File download has failed or was canceled» (или,
    // если another_request, 200 «Canceled by another downloadFile request»).
    void cancel_next_downloads(int n, bool another_request = false) {
        fail_next("downloadFile",
                  another_request ? td_error(200, "Canceled by another downloadFile request")
                                  : td_error(400, "File download has failed or was canceled"),
                  n);
    }
    // Ответить ошибкой на следующие n запросов этого типа (по порядку поступления).
    void fail_next(const std::string& type, Json error, int n = 1) {
        with_lock([&] {
            for (int i = 0; i < n; ++i) {
                faults_[type].push_back(error);
            }
        });
    }
    // Не отвечать на запросы этого типа вовсе (для таймаутов).
    void drop_requests(const std::string& type, bool drop = true) {
        with_lock([&] {
            if (drop) {
                dropped_.insert(type);
            } else {
                dropped_.erase(type);
            }
        });
    }
    // Как после перезапуска процесса: TDLib не знает старые сообщения и канал.
    void forget_session() {
        with_lock([&] {
            known_.clear();
            chat_known_ = false;
        });
    }

    // Сообщение, которое уже лежит в канале (не отправлено этим процессом, TDLib его не знает).
    int64_t seed_document(const std::string& content, const std::string& name = "seed.bin") {
        return with_lock([&] { return store_message(name, content, /*known=*/false).id; });
    }
    void seed_pin(int64_t id) {
        with_lock([&] { messages_.at(id).pinned = true; });
    }

    // ---- Наблюдение ----

    [[nodiscard]] std::vector<Json> requests(const std::string& type) const {
        return with_lock([&] {
            std::vector<Json> out;
            out.reserve(log_.size());
            for (const Json& r : log_) {
                if (r.value("@type", "") == type) {
                    out.push_back(r);
                }
            }
            return out;
        });
    }
    [[nodiscard]] size_t count(const std::string& type) const { return requests(type).size(); }
    [[nodiscard]] std::vector<std::string> sequence() const {
        return with_lock([&] {
            std::vector<std::string> out;
            out.reserve(log_.size());
            for (const Json& r : log_) {
                out.push_back(r.value("@type", ""));
            }
            return out;
        });
    }
    [[nodiscard]] std::optional<Message> message(int64_t id) const {
        return with_lock([&]() -> std::optional<Message> {
            const auto it = messages_.find(id);
            if (it == messages_.end()) {
                return std::nullopt;
            }
            return it->second;
        });
    }
    [[nodiscard]] std::vector<Message> messages() const {
        return with_lock([&] {
            std::vector<Message> out;
            out.reserve(messages_.size());
            for (const auto& [id, m] : messages_) {
                out.push_back(m);
            }
            return out;
        });
    }
    [[nodiscard]] std::vector<int64_t> pinned() const {
        return with_lock([&] {
            std::vector<int64_t> out;
            out.reserve(messages_.size());
            for (const auto& [id, m] : messages_) {
                if (m.pinned) {
                    out.push_back(id);
                }
            }
            return out;
        });
    }
    [[nodiscard]] int max_inflight_sends() const { return max_inflight_.load(); }
    [[nodiscard]] int max_inflight_downloads() const { return max_downloads_.load(); }

  private:
    Message& store_message(const std::string& name, const std::string& content, bool known) {
        Message m;
        m.id = next_server_id_++ * kShift;
        m.name = name;
        m.content = content;
        m.file_id = next_file_id_++;
        m.remote_id = "remote-" + std::to_string(m.id);
        if (known) {
            known_.insert(m.id);
        }
        return messages_.emplace(m.id, std::move(m)).first->second;
    }

    Json file_json(const Message& m, bool local, int64_t start = 0, int64_t count = 0,
                   bool zero_size = false) const {
        const auto size = static_cast<int64_t>(m.content.size());
        Json f{
            {"@type", "file"},
            {"id", m.file_id},
            {"size", zero_size ? 0 : size},
            {"expected_size", zero_size ? 0 : size},
            {"remote", {{"@type", "remoteFile"}, {"id", m.remote_id}}},
            {"local", {{"@type", "localFile"}, {"path", ""}, {"is_downloading_completed", false}}}};
        if (local) {
            f["local"]["path"] = (download_dir_ / std::to_string(m.file_id)).string();
            f["local"]["is_downloading_completed"] = start == 0 && (count == 0 || count >= size);
        }
        return f;
    }

    Json message_json(const Message& m) const {
        return Json{{"@type", "message"},
                    {"id", m.id},
                    {"chat_id", kChatId},
                    {"is_pinned", m.pinned},
                    {"content",
                     {{"@type", "messageDocument"},
                      {"document",
                       {{"@type", "document"},
                        {"file_name", m.name},
                        {"document", file_json(m, false)}}}}}};
    }

    static Json update_failed(int64_t temp, const Json& error) {
        return Json{{"@type", "updateMessageSendFailed"},
                    {"message", {{"@type", "message"}, {"id", temp}, {"chat_id", kChatId}}},
                    {"old_message_id", temp},
                    {"error", error}};
    }

    void handle(FakeTransport& t, int client, const Json& request) {
        const std::string type = request.value("@type", "");
        std::optional<std::chrono::milliseconds> delay;
        {
            const std::lock_guard<std::mutex> lock(mu_);
            log_.push_back(request);
            if (dropped_.contains(type)) {
                return;
            }
            if (auto it = faults_.find(type); it != faults_.end() && !it->second.empty()) {
                Json error = std::move(it->second.front());
                it->second.pop_front();
                t.respond(client, request, std::move(error));
                return;
            }
            const bool is_send = type == "sendMessage" || type == "sendMessageAlbum";
            if (is_send && send_delay_.count() > 0) {
                delay = send_delay_;
            }
            if (type == "downloadFile" && download_delay_.count() > 0) {
                delay = download_delay_;
            }
        }
        if (type == "downloadFile" && delay) {
            const auto fid = static_cast<int32_t>(request["file_id"].get<int64_t>());
            int64_t generation = 0;
            {
                const std::lock_guard<std::mutex> lock(mu_);
                generation = ++download_counter_;
                download_generation_[fid] = generation;
            }
            const int now = ++downloads_;
            int seen = max_downloads_.load();
            while (now > seen && !max_downloads_.compare_exchange_weak(seen, now)) {
            }
            std::this_thread::sleep_for(*delay);
            {
                const std::lock_guard<std::mutex> lock(mu_);
                if (cancel_overlap_ && download_generation_[fid] != generation) {
                    t.respond(client, request,
                              td_error(200, "Canceled by another downloadFile request"));
                } else {
                    handle_other(t, client, request, type);
                }
            }
            --downloads_;
            return;
        }
        if (type == "sendMessage" || type == "sendMessageAlbum") {
            const int now = ++inflight_;
            int seen = max_inflight_.load();
            while (now > seen && !max_inflight_.compare_exchange_weak(seen, now)) {
            }
            if (delay) {
                std::this_thread::sleep_for(*delay);
            }
            handle_send(t, client, request);
            --inflight_;
            return;
        }
        const std::lock_guard<std::mutex> lock(mu_);
        handle_other(t, client, request, type);
    }

    void handle_send(FakeTransport& t, int client, const Json& request) {
        const bool album = request["@type"] == "sendMessageAlbum";
        std::vector<std::filesystem::path> paths;
        if (album) {
            for (const Json& c : request["input_message_contents"]) {
                paths.emplace_back(c["document"]["document"]["path"].get<std::string>());
            }
        } else {
            paths.emplace_back(request["input_message_content"]["document"]["document"]["path"]
                                   .get<std::string>());
        }
        const std::lock_guard<std::mutex> lock(mu_);
        if (!chat_known_) {
            t.respond(client, request, td_error(400, "Chat not found"));
            return;
        }
        if (album) {
            // Как TDLib: непригодный файл — 400 на весь вызов, ничего не уходит и файл не назван.
            for (const auto& p : paths) {
                std::error_code ec;
                if (!std::filesystem::is_regular_file(p, ec) ||
                    std::filesystem::file_size(p, ec) == 0) {
                    t.respond(client, request, td_error(400, "InputFile is not specified"));
                    return;
                }
            }
        }
        std::vector<int64_t> temps;
        for (size_t i = 0; i < paths.size(); ++i) {
            temps.push_back(kTempBase + next_temp_++);
        }
        struct Pending {
            int64_t temp;
            std::string name;
            std::string content;
            std::optional<Json> verdict;
        };
        std::vector<Pending> updates;
        for (size_t i = 0; i < paths.size(); ++i) {
            const std::string name = paths[i].filename().string();
            std::ifstream in(paths[i], std::ios::binary);
            const std::string content{std::istreambuf_iterator<char>(in),
                                      std::istreambuf_iterator<char>()};
            std::optional<Json> verdict = verdict_ ? verdict_(name, i) : std::nullopt;
            if (!in.is_open() || content.empty()) {
                verdict = td_error(400, "Can't open file");
            }
            updates.push_back({temps[i], name, content, std::move(verdict)});
        }
        const auto temp_message = [](int64_t id) {
            return Json{{"@type", "message"},
                        {"id", id},
                        {"chat_id", kChatId},
                        {"sending_state", {{"@type", "messageSendingStatePending"}}}};
        };
        Json reply;
        if (album) {
            Json list = Json::array();
            for (const int64_t id : temps) {
                list.push_back(temp_message(id));
            }
            reply = Json{{"@type", "messages"}, {"total_count", temps.size()}, {"messages", list}};
        } else {
            reply = temp_message(temps[0]);
        }
        // Итоговое сообщение появляется в канале в момент итога.
        const auto result_update = [this](const Pending& p) {
            if (p.verdict) {
                return update_failed(p.temp, *p.verdict);
            }
            const Message& m = store_message(p.name, p.content, /*known=*/true);
            if (stale_sends_) {
                stale_files_.insert(m.file_id);
            }
            return Json{{"@type", "updateMessageSendSucceeded"},
                        {"message", message_json(m)},
                        {"old_message_id", p.temp}};
        };
        const auto respond = [&] {
            if (!lose_send_response_) {
                t.respond(client, request, reply);
            }
        };
        if (suppress_updates_) {
            respond();
        } else if (update_delay_.count() > 0) {
            for (const Pending& p : updates) {
                pending_temps_.insert(p.temp);
            }
            respond();
            const auto delay = update_delay_;
            // Поток держит только то, что нужно для итога; mu_ берёт сам.
            delayed_.emplace_back([this, &t, client, delay, updates, result_update] {
                std::this_thread::sleep_for(delay);
                for (const Pending& p : updates) {
                    const std::lock_guard<std::mutex> guard(mu_);
                    if (pending_temps_.erase(p.temp) == 0) {
                        continue;  // заливку отменили удалением временного сообщения
                    }
                    t.push(client, result_update(p));
                }
            });
        } else if (early_updates_) {
            for (const Pending& p : updates) {
                t.push(client, result_update(p));
            }
            respond();
        } else {
            respond();
            for (const Pending& p : updates) {
                t.push(client, result_update(p));
            }
        }
    }

    void handle_other(FakeTransport& t, int client, const Json& request, const std::string& type) {
        const auto reply = [&](Json response) { t.respond(client, request, std::move(response)); };
        const auto fail = [&](int code, const std::string& text) { reply(td_error(code, text)); };
        if (type == "getChat") {
            chat_known_ = true;
            reply(Json{{"@type", "chat"},
                       {"id", kChatId},
                       {"type",
                        {{"@type", "chatTypeSupergroup"},
                         {"supergroup_id", 1234567890},
                         {"is_channel", is_channel_}}},
                       {"message_auto_delete_time", auto_delete_}});
            return;
        }
        if (type == "getMe") {
            reply(Json{{"@type", "user"}, {"id", 4242}});
            return;
        }
        if (type == "getChatMember") {
            reply(Json{{"@type", "chatMember"}, {"status", member_status_}});
            return;
        }
        if (type == "getRemoteFile") {
            const std::string rid = request["remote_file_id"].get<std::string>();
            for (const auto& [id, m] : messages_) {
                if (m.remote_id == rid) {
                    Json f = file_json(m, false, 0, 0, remote_size_zero_);
                    if (remote_size_zero_) {
                        f["id"] =
                            m.file_id + kBogusFileOffset;  // файл без размера: скачать конец нельзя
                    }
                    reply(std::move(f));
                    return;
                }
            }
            fail(400, "Wrong remote file identifier specified");
            return;
        }
        if (type == "deleteFile") {
            const auto fid = static_cast<int32_t>(request["file_id"].get<int64_t>());
            std::error_code ec;
            std::filesystem::remove(download_dir_ / std::to_string(fid), ec);
            reply(Json{{"@type", "ok"}});
            return;
        }
        if (type == "downloadFile") {
            const auto fid = static_cast<int32_t>(request["file_id"].get<int64_t>());
            const Message* found = nullptr;
            for (const auto& [id, m] : messages_) {
                if (m.file_id == fid) {
                    found = &m;
                }
            }
            if (found == nullptr) {
                fail(400, "File not found");
                return;
            }
            // check_local_location: локального файла нет, TDLib забывает его и качает заново.
            stale_files_.erase(fid);
            if (vanish_downloads_ > 0) {
                --vanish_downloads_;
                Json f = file_json(*found, false);
                f["local"]["path"] = (download_dir_ / ("gone-" + std::to_string(fid))).string();
                f["local"]["is_downloading_completed"] = true;
                reply(std::move(f));
                return;
            }
            const auto offset = request["offset"].get<int64_t>();
            auto limit = request["limit"].get<int64_t>();
            const auto size = static_cast<int64_t>(found->content.size());
            if (limit == 0 || offset + limit > size) {
                limit = size - offset;
            }
            const auto path = download_dir_ / std::to_string(fid);
            std::error_code ec;
            if (!std::filesystem::exists(path)) {
                std::ofstream create(path, std::ios::binary);
                create.close();
                std::filesystem::resize_file(path, static_cast<uintmax_t>(size), ec);
            }
            {
                std::fstream out(path, std::ios::binary | std::ios::in | std::ios::out);
                out.seekp(offset);
                out.write(found->content.data() + offset, limit);
            }
            reply(file_json(*found, true, offset, request["limit"].get<int64_t>()));
            return;
        }
        if (!chat_known_) {
            fail(400, "Chat not found");
            return;
        }
        if (type == "getMessages") {
            Json list = Json::array();
            for (const Json& id : request["message_ids"]) {
                const auto it = messages_.find(id.get<int64_t>());
                if (it == messages_.end()) {
                    list.push_back(nullptr);
                } else {
                    known_.insert(it->first);
                    list.push_back(message_json(it->second));
                }
            }
            reply(Json{{"@type", "messages"}, {"total_count", list.size()}, {"messages", list}});
            return;
        }
        if (type == "getChatPinnedMessage") {
            const Message* newest = nullptr;
            for (const auto& [id, m] : messages_) {
                if (m.pinned) {
                    newest = &m;
                }
            }
            if (newest == nullptr) {
                fail(404, "Message not found");
                return;
            }
            known_.insert(newest->id);
            reply(message_json(*newest));
            return;
        }
        if (type == "pinChatMessage" || type == "unpinChatMessage") {
            const auto id = request["message_id"].get<int64_t>();
            const auto it = messages_.find(id);
            if (!known_.contains(id) || it == messages_.end()) {
                fail(400, "Message not found");
                return;
            }
            it->second.pinned = type == "pinChatMessage";
            reply(Json{{"@type", "ok"}});
            return;
        }
        if (type == "deleteMessages") {
            std::vector<int64_t> ids;
            for (const Json& id : request["message_ids"]) {
                ids.push_back(id.get<int64_t>());
            }
            for (const int64_t id : ids) {
                if (messages_.contains(id) && !known_.contains(id)) {
                    fail(400, "Message not found");
                    return;
                }
            }
            for (const int64_t id : ids) {
                messages_.erase(id);
                if (cancel_on_delete_) {
                    pending_temps_.erase(id);
                }
            }
            reply(Json{{"@type", "ok"}});
            return;
        }
        fail(400, "Unsupported request " + type);
    }

    static constexpr int64_t kTempBase = int64_t{1} << 40;
    static constexpr int32_t kBogusFileOffset = 1'000'000;

    FakeTransport& transport_;
    const std::filesystem::path download_dir_;

    std::vector<Json> log_;
    std::map<std::string, std::deque<Json>> faults_;
    std::set<std::string> dropped_;
    std::map<int64_t, Message> messages_;
    std::set<int64_t> known_;
    bool chat_known_ = false;
    int32_t auto_delete_ = 0;
    bool is_channel_ = true;
    Json member_status_ =
        Json{{"@type", "chatMemberStatusCreator"}, {"is_anonymous", false}, {"is_member", true}};
    Verdict verdict_;
    bool early_updates_ = false;
    bool suppress_updates_ = false;
    std::chrono::milliseconds update_delay_{0};
    bool cancel_on_delete_ = true;
    bool lose_send_response_ = false;
    bool stale_sends_ = false;
    std::set<int32_t> stale_files_;
    int vanish_downloads_ = 0;
    std::set<int64_t> pending_temps_;  // отправки, итог которых ещё в пути
    std::vector<std::thread> delayed_;
    bool remote_size_zero_ = false;
    std::chrono::milliseconds send_delay_{0};
    std::chrono::milliseconds download_delay_{0};
    bool cancel_overlap_ = false;
    int64_t download_counter_ = 0;
    std::map<int32_t, int64_t> download_generation_;
    int64_t next_server_id_ = 2;  // id 1 — создание канала (SPEC 4)
    int32_t next_file_id_ = 100;
    int64_t next_temp_ = 1;
    std::atomic<int> inflight_{0};
    std::atomic<int> max_inflight_{0};
    std::atomic<int> downloads_{0};
    std::atomic<int> max_downloads_{0};
};

}  // namespace telega::store::test
