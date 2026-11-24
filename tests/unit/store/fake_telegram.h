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
//
// Скачивания повторяют E10 (SPEC 17.13): downloadFile асинхронный, ход приходит апдейтами
// updateFile по частям; окна исполняются по одному, строго по приоритету (при равных — самый новый
// запрос первым); повторный вызов с теми же offset и limit меняет приоритет и не отменяет окно;
// тот же offset с большим limit продлевает окно без паузы; другой offset заменяет окно (скачанное
// сохраняется); пока файл качается, local.path — временный, при завершении файла он меняется
// (старый путь после этого даёт ENOENT); файл, удалённый с диска за спиной TDLib,
// getFileDownloadedPrefixSize считает нулём, а getFile и getRemoteFile — по-прежнему скачанным.
// Режим automatic крутит скачивание в своём потоке, manual — только по вызовам
// step()/run_downloads() (для детерминированных тестов порядка).

#include <nlohmann/json.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
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
#include "store/interval_set.h"

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
        {
            const std::lock_guard<std::mutex> lock(mu_);
            engine_stop_ = true;
        }
        engine_cv_.notify_all();
        if (engine_.joinable()) {
            engine_.join();
        }
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
        std::filesystem::create_directories(download_dir_ / "tmp");
        std::filesystem::create_directories(download_dir_ / "documents");
        std::filesystem::create_directories(download_dir_ / "sent");
        engine_ = std::thread([this] { engine_loop(); });
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

    // ---- Скачивания ----

    enum class DownloadMode : uint8_t { automatic, manual };
    struct Window {
        int64_t offset = 0;
        int64_t limit = 0;  // 0 — до конца файла
        int priority = 0;
    };

    void set_download_mode(DownloadMode m) {
        with_lock([&] { mode_ = m; });
        engine_cv_.notify_all();
    }
    void set_part_size(int64_t bytes) {
        with_lock([&] { part_size_ = bytes; });
    }
    // automatic: пауза между частями (скорость скачивания).
    void set_part_delay(std::chrono::milliseconds d) {
        with_lock([&] { part_delay_ = d; });
    }
    // manual: продвинуть выполняемое скачивание на n частей. Возвращает, сколько сделано.
    int step(int n = 1) {
        int done = 0;
        const std::lock_guard<std::mutex> lock(mu_);
        while (done < n && step_locked()) {
            ++done;
        }
        return done;
    }
    // manual: исполнить все окна до конца, как есть (в порядке приоритетов).
    void run_downloads() {
        const std::lock_guard<std::mutex> lock(mu_);
        while (step_locked()) {
        }
    }
    [[nodiscard]] std::optional<Window> window(int32_t file_id) const {
        return with_lock([&]() -> std::optional<Window> {
            const auto it = files_.find(file_id);
            if (it == files_.end() || !it->second.win) {
                return std::nullopt;
            }
            return Window{it->second.win->off, it->second.win->limit, it->second.win->prio};
        });
    }
    // file_id в порядке завершения окон.
    [[nodiscard]] std::vector<int32_t> completion_order() const {
        return with_lock([&] { return completed_; });
    }
    [[nodiscard]] int64_t downloaded_bytes(int32_t file_id) const {
        return with_lock([&]() -> int64_t {
            const auto it = files_.find(file_id);
            return it == files_.end() ? 0 : it->second.ready.total();
        });
    }
    [[nodiscard]] std::string local_path(int32_t file_id) const {
        return with_lock([&]() -> std::string {
            const auto it = files_.find(file_id);
            return it == files_.end() ? std::string{} : it->second.path;
        });
    }
    // Следующие n новых окон «не качаются»: TDLib не сообщает ошибки, только апдейт
    // is_downloading_active = false (E10, «Грабли»).
    void fail_next_windows(int n) {
        with_lock([&] { fail_windows_ = n; });
    }
    // Следующие n ответов getFileDownloadedPrefixSize лгут: «всё скачано». Файла при этом может не
    // быть (гонка, которую TDLib не заметил): чтение получает ENOENT.
    void set_probe_lies(int n) {
        with_lock([&] { probe_lies_ = n; });
    }
    // При завершении файла TDLib переименовывает временный файл: апдейт с прогрессом идёт ещё со
    // старым путём, а итоговый апдейт (с новым путём и completed) задерживается до
    // release_held_updates().
    void hold_final_update(bool v) {
        with_lock([&] { hold_final_ = v; });
    }
    void release_held_updates() {
        const std::lock_guard<std::mutex> lock(mu_);
        for (Json& u : held_) {
            transport_.push(client_id_, std::move(u));
        }
        held_.clear();
    }
    // Второй file_id того же файла (после getMessages TDLib может выдать новый): те же данные,
    // тот же локальный путь и окно, апдейты идут под обоими id.
    void add_alias(int32_t alias, int32_t real) {
        with_lock([&] { aliases_[alias] = real; });
    }
    // Произвольный апдейт клиенту (например, запоздалый апдейт о прежнем окне).
    void push_update(Json update) {
        const std::lock_guard<std::mutex> lock(mu_);
        transport_.push(client_id_, std::move(update));
    }
    // Файл «локальный и готовый целиком», а файла по пути нет (как у отправленного этим процессом
    // после удаления ссылки): prefix и getFile говорят «готово», downloadFile сам проверит путь.
    void set_stale_local(int32_t file_id) {
        const std::lock_guard<std::mutex> lock(mu_);
        const Message* m = message_by_file(file_id);
        FileState& st = files_[file_id];
        st.ready.add(0, static_cast<int64_t>(m->content.size()));
        st.sent_local = true;
        st.path = (download_dir_ / "sent" / std::to_string(file_id)).string();
    }
    // Удалить локальный файл за спиной TDLib (E10 п. 7).
    void remove_local_file(int32_t file_id) {
        const std::lock_guard<std::mutex> lock(mu_);
        const auto it = files_.find(file_id);
        if (it != files_.end() && !it->second.path.empty()) {
            std::error_code ec;
            std::filesystem::remove(it->second.path, ec);
        }
    }
    // Сколько окон скачивалось одновременно в пике.
    [[nodiscard]] int max_active_windows() const { return max_windows_.load(); }

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

    // Файл с локальным состоянием TDLib (local = true) или без него (как в сообщении).
    Json file_json(const Message& m, bool local, bool zero_size = false) const {
        const auto size = static_cast<int64_t>(m.content.size());
        Json f{{"@type", "file"},
               {"id", m.file_id},
               {"size", zero_size ? 0 : size},
               {"expected_size", zero_size ? 0 : size},
               {"remote", {{"@type", "remoteFile"}, {"id", m.remote_id}}},
               {"local",
                {{"@type", "localFile"},
                 {"path", ""},
                 {"is_downloading_active", false},
                 {"is_downloading_completed", false},
                 {"download_offset", 0},
                 {"downloaded_prefix_size", 0}}}};
        if (local) {
            const auto it = files_.find(m.file_id);
            if (it != files_.end()) {
                const FileState& st = it->second;
                Json& l = f["local"];
                l["path"] = st.path;
                l["is_downloading_completed"] = size > 0 && st.ready.covers(0, size);
                // После завершения окна апдейт всё ещё описывает его: готовый префикс от его
                // offset.
                const int64_t off = st.win ? st.win->off : st.last_off;
                l["is_downloading_active"] = st.win && st.win->active;
                l["download_offset"] = off;
                l["downloaded_prefix_size"] = st.ready.end_of(off) - off;
            }
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
        // Запрос попадает в журнал под той же блокировкой, что и его обработка (кроме отправки):
        // тест, увидевший count("downloadFile") > n, видит и окно этого вызова. Иначе между
        // записью в журнал и обработкой тест успевал run_downloads() при ещё не открытом окне.
        std::unique_lock<std::mutex> lock(mu_);
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
        if (is_send) {
            lock.unlock();
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
        client_id_ = client;
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
            std::string source;  // путь отправленного файла
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
            updates.push_back({temps[i], name, content, std::move(verdict), paths[i].string()});
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
            // Отправленный файл для TDLib локальный и готовый целиком, путь — исходный файл (у
            // Store это временная ссылка, которую он удаляет после итога отправки). stale_sends_:
            // путь сразу несуществующий.
            FileState& st = files_[m.file_id];
            st.ready.add(0, static_cast<int64_t>(m.content.size()));
            st.sent_local = true;
            st.path = stale_sends_ ? (download_dir_ / "sent" / std::to_string(m.file_id)).string()
                                   : p.source;
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
                    Json f = file_json(m, true, remote_size_zero_);
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
        if (type == "getFile") {
            const Message* m =
                message_by_file(static_cast<int32_t>(request["file_id"].get<int64_t>()));
            if (m == nullptr) {
                fail(400, "File not found");
                return;
            }
            reply(with_id(file_json(*m, true),
                          static_cast<int32_t>(request["file_id"].get<int64_t>())));
            return;
        }
        if (type == "getFileDownloadedPrefixSize") {
            const auto fid = canon(static_cast<int32_t>(request["file_id"].get<int64_t>()));
            const auto offset = request["offset"].get<int64_t>();
            const Message* m = message_by_file(fid);
            if (m == nullptr) {
                fail(400, "File not found");
                return;
            }
            const auto size = static_cast<int64_t>(m->content.size());
            int64_t prefix = 0;
            const auto it = files_.find(fid);
            std::error_code ec;
            if (probe_lies_ > 0) {
                --probe_lies_;
                prefix = size - offset;
            } else if (it != files_.end() && !it->second.path.empty() &&
                       (it->second.sent_local || std::filesystem::exists(it->second.path, ec))) {
                // Файла на диске нет — TDLib возвращает 0, хотя getFile считает его скачанным.
                prefix = it->second.ready.end_of(offset) - offset;
            }
            reply(Json{{"@type", "fileDownloadedPrefixSize"},
                       {"size", std::max<int64_t>(prefix, 0)}});
            return;
        }
        if (type == "deleteFile") {
            const auto fid = static_cast<int32_t>(request["file_id"].get<int64_t>());
            const auto it = files_.find(fid);
            // Файл, который этот процесс отправлял, лежит вне files_directory: TDLib его не
            // трогает.
            if (it != files_.end() && !it->second.sent_local &&
                !it->second.path.starts_with((download_dir_ / "sent").string())) {
                std::error_code ec;
                if (!it->second.path.empty()) {
                    std::filesystem::remove(it->second.path, ec);
                }
                it->second.ready.clear();
                it->second.path.clear();
                it->second.win.reset();
            }
            reply(Json{{"@type", "ok"}});
            return;
        }
        if (type == "downloadFile") {
            handle_download(t, client, request);
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

    // ---- Скачивания (под mu_) ----

    struct Win {
        int64_t off = 0;
        int64_t limit = 0;
        int prio = 0;
        uint64_t seq = 0;
        bool active = true;
    };
    struct FileState {
        IntervalSet ready;
        std::string path;
        // Файл отправлен этим процессом: TDLib считает его локальным и полностью готовым с путём
        // исходного файла и состояние диска не проверяет (getFileDownloadedPrefixSize, getFile),
        // пока downloadFile не вызовет check_local_location.
        bool sent_local = false;
        std::optional<Win> win;
        int64_t last_off = 0;  // offset последнего окна
    };

    [[nodiscard]] int32_t canon(int32_t fid) const {
        const auto it = aliases_.find(fid);
        return it == aliases_.end() ? fid : it->second;
    }
    static Json with_id(Json f, int32_t id) {
        f["id"] = id;
        return f;
    }

    const Message* message_by_file(int32_t fid_in) const {
        const int32_t fid = canon(fid_in);
        for (const auto& [id, m] : messages_) {
            if (m.file_id == fid) {
                return &m;
            }
        }
        return nullptr;
    }

    [[nodiscard]] std::filesystem::path tmp_path(int32_t fid) const {
        return download_dir_ / "tmp" / std::to_string(fid);
    }
    [[nodiscard]] std::filesystem::path final_path(int32_t fid) const {
        return download_dir_ / "documents" / std::to_string(fid);
    }

    void push_file_update(const Message& m, const std::string* path_override = nullptr) {
        Json f = file_json(m, true);
        if (path_override != nullptr) {
            f["local"]["path"] = *path_override;
        }
        for (const auto& [alias, real] : aliases_) {
            if (real == m.file_id) {
                transport_.push(client_id_,
                                Json{{"@type", "updateFile"}, {"file", with_id(f, alias)}});
            }
        }
        transport_.push(client_id_, Json{{"@type", "updateFile"}, {"file", std::move(f)}});
    }

    void update_max_windows() {
        int n = 0;
        for (const auto& [id, st] : files_) {
            n += st.win && st.win->active ? 1 : 0;
        }
        int seen = max_windows_.load();
        while (n > seen && !max_windows_.compare_exchange_weak(seen, n)) {
        }
    }

    void handle_download(FakeTransport& t, int client, const Json& request) {
        const auto reply = [&](Json response) { t.respond(client, request, std::move(response)); };
        const auto req_fid = static_cast<int32_t>(request["file_id"].get<int64_t>());
        const auto fid = canon(req_fid);
        const Message* m = message_by_file(fid);
        if (m == nullptr) {
            reply(td_error(400, "File not found"));
            return;
        }
        const auto offset = request["offset"].get<int64_t>();
        const auto limit = request["limit"].get<int64_t>();
        const int prio = request.value("priority", 1);
        const auto size = static_cast<int64_t>(m->content.size());
        const int64_t wend = limit == 0 ? size : std::min(size, offset + limit);
        FileState& st = files_[fid];
        // check_local_location: локального файла нет, TDLib забывает его и качает заново.
        std::error_code ec;
        bool dropped = false;
        if (!st.path.empty() && !std::filesystem::exists(st.path, ec)) {
            st.ready.clear();
            st.path.clear();
            st.win.reset();
            st.sent_local = false;
            dropped = true;
            // Как TDLib: сброс локального расположения даёт апдейт «не качается» до начала
            // скачивания (on_failed_check_local_location).
            push_file_update(*m);
        }
        st.last_off = offset;
        if (st.ready.covers(offset, wend)) {
            st.win.reset();
        } else if (st.win && st.win->active && st.win->off == offset) {
            st.win->limit = limit;  // тот же offset: приоритет и продление без паузы
            st.win->prio = prio;
            st.win->seq = ++seq_;
        } else if (fail_windows_ > 0) {
            --fail_windows_;
            st.win = Win{offset, limit, prio, ++seq_, false};  // обрыв без ошибки
        } else {
            st.win = Win{offset, limit, prio, ++seq_, true};
        }
        if (st.path.empty()) {
            st.path = tmp_path(fid).string();
            std::ofstream create(st.path, std::ios::binary);
            create.close();
            std::filesystem::resize_file(st.path, static_cast<uintmax_t>(size), ec);
        }
        update_max_windows();
        if (dropped && st.win && st.win->active) {
            push_file_update(*m);  // скачивание началось
        }
        reply(with_id(file_json(*m, true), req_fid));
        if (st.win && !st.win->active) {
            push_file_update(*m);  // «не качается»
            st.win.reset();
        }
        engine_cv_.notify_all();
    }

    // Продвигает выполняемое окно на одну часть. true — что-то сделано.
    bool step_locked() {
        FileState* best = nullptr;
        int32_t best_id = 0;
        std::optional<Win> best_win;
        for (auto& [id, st] : files_) {
            if (!st.win || !st.win->active) {
                continue;
            }
            const Win& cand = *st.win;
            if (!best_win || cand.prio > best_win->prio ||
                (cand.prio == best_win->prio && cand.seq > best_win->seq)) {
                best = &st;
                best_id = id;
                best_win = cand;
            }
        }
        if (best == nullptr || !best_win) {
            return false;
        }
        const Message& m = *message_by_file(best_id);
        const auto size = static_cast<int64_t>(m.content.size());
        const Win& w = *best_win;
        const int64_t wend = w.limit == 0 ? size : std::min(size, w.off + w.limit);
        const int64_t pos = best->ready.end_of(w.off);
        if (pos < wend) {
            const int64_t n = std::min(part_size_, wend - pos);
            std::fstream out(best->path, std::ios::binary | std::ios::in | std::ios::out);
            out.seekp(pos);
            out.write(m.content.data() + pos, n);
            out.close();
            best->ready.add(pos, pos + n);
        }
        if (best->ready.end_of(w.off) < wend) {
            push_file_update(m);
            return true;
        }
        // Окно завершено.
        completed_.push_back(best_id);
        const bool whole = size > 0 && best->ready.covers(0, size);
        const std::string old_path = best->path;
        best->win.reset();
        if (whole) {
            // Файл целиком: временный файл переименовывается в итоговый.
            std::error_code ec;
            std::filesystem::rename(old_path, final_path(best_id), ec);
            best->path = final_path(best_id).string();
            if (hold_final_) {
                push_file_update(m, &old_path);  // прогресс ещё по старому пути
                held_.push_back(Json{{"@type", "updateFile"}, {"file", file_json(m, true)}});
                return true;
            }
        }
        push_file_update(m);
        return true;
    }

    void engine_loop() {
        std::unique_lock<std::mutex> lock(mu_);
        while (!engine_stop_) {
            if (mode_ == DownloadMode::manual || !step_locked()) {
                engine_cv_.wait_for(lock, std::chrono::milliseconds{20});
                continue;
            }
            const auto delay = part_delay_;
            lock.unlock();
            std::this_thread::sleep_for(delay);
            lock.lock();
        }
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
    std::set<int64_t> pending_temps_;  // отправки, итог которых ещё в пути
    std::vector<std::thread> delayed_;
    bool remote_size_zero_ = false;
    std::chrono::milliseconds send_delay_{0};
    std::map<int32_t, FileState> files_;
    DownloadMode mode_ = DownloadMode::automatic;
    int64_t part_size_ = 65'536;
    std::chrono::milliseconds part_delay_{0};
    uint64_t seq_ = 0;
    int fail_windows_ = 0;
    int probe_lies_ = 0;
    bool hold_final_ = false;
    std::vector<Json> held_;
    std::vector<int32_t> completed_;
    int client_id_ = 0;
    bool engine_stop_ = false;
    std::condition_variable engine_cv_;
    std::thread engine_;
    std::atomic<int> max_windows_{0};
    int64_t next_server_id_ = 2;  // id 1 — создание канала (SPEC 4)
    int32_t next_file_id_ = 100;
    std::map<int32_t, int32_t> aliases_;  // второй file_id → настоящий
    int64_t next_temp_ = 1;
    std::atomic<int> inflight_{0};
    std::atomic<int> max_inflight_{0};
};

}  // namespace telega::store::test
