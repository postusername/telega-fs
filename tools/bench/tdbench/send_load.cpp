#include "send_load.h"

#include <algorithm>
#include <bit>
#include <cstring>
#include <fstream>

namespace telega::bench {

namespace {

using std::chrono::milliseconds;

constexpr int64_t kDefaultRetryAfterS = 60;  // 429 без retry_after

uint64_t splitmix64(uint64_t x) {
    x += 0x9E3779B97F4A7C15ULL;
    x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
    x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;
    return x ^ (x >> 31);
}

// То же содержимое, что у fill_seeded (байт p определяется только (seed, p)), но по 8 байт:
// документы по 64 МиБ в сборке Debug иначе генерируются заметную долю секунды.
void fill_seeded_words(uint64_t seed, uint64_t offset, std::string& out, size_t count) {
    static_assert(std::endian::native == std::endian::little);
    out.resize(count);
    size_t i = 0;
    while (i < count) {
        const uint64_t pos = offset + i;
        const uint64_t word = splitmix64(seed ^ ((pos / 8) * 0xD6E8FEB86659FD93ULL));
        const size_t skip = pos % 8;
        const size_t n = std::min<size_t>(8 - skip, count - i);
        std::memcpy(out.data() + i, reinterpret_cast<const char*>(&word) + skip, n);
        i += n;
    }
}

// Ошибки TDLib про файлы содержат полный локальный путь (в нём имя пользователя ОС): пути в
// кавычках заменяются на <path>, чтобы они не попали в CSV и отчёты.
std::string scrub(std::string text) {
    size_t pos = 0;
    while ((pos = text.find('"', pos)) != std::string::npos) {
        const size_t end = text.find('"', pos + 1);
        if (end == std::string::npos) {
            break;
        }
        if (text.find('/', pos) < end) {
            text.replace(pos + 1, end - pos - 1, "<path>");
            pos += std::string{"\"<path>\""}.size();
        } else {
            pos = end + 1;
        }
    }
    return text;
}

SendKind classify(const td::TdError& e, int64_t* retry_after_s) {
    if (e.code == 401 || e.message.find("USER_DEACTIVATED") != std::string::npos ||
        e.message.find("AUTH_KEY") != std::string::npos) {
        return SendKind::kAccount;
    }
    if (e.code == td::TdError::kClosed || e.code == td::TdError::kStopped) {
        return SendKind::kAborted;
    }
    if (e.is_rate_limit()) {
        *retry_after_s = e.retry_after ? e.retry_after->count() : kDefaultRetryAfterS;
        return SendKind::kFlood;
    }
    if (e.is_timeout()) {
        return SendKind::kTimeout;
    }
    if (e.is_local() || e.code >= 500) {
        return SendKind::kNet;
    }
    return SendKind::kFatal;
}

// Приоритет худшего исхода: чем больше, тем важнее вызывающему.
int severity(SendKind kind) {
    switch (kind) {
        case SendKind::kOk:
            return 0;
        case SendKind::kAborted:
            return 1;
        case SendKind::kTimeout:
            return 2;
        case SendKind::kNet:
            return 3;
        case SendKind::kFlood:
            return 4;
        case SendKind::kFatal:
            return 5;
        case SendKind::kAccount:
            return 6;
    }
    return 0;
}

td::Json send_options() {
    return td::Json{{"@type", "messageSendOptions"}, {"disable_notification", true}};
}

}  // namespace

const char* send_kind_name(SendKind kind) {
    switch (kind) {
        case SendKind::kOk:
            return "ok";
        case SendKind::kFlood:
            return "flood";
        case SendKind::kNet:
            return "net_error";
        case SendKind::kTimeout:
            return "timeout";
        case SendKind::kAccount:
            return "account_error";
        case SendKind::kFatal:
            return "error";
        case SendKind::kAborted:
            return "aborted";
    }
    return "?";
}

td::Json single_document_request(int64_t chat_id, const std::filesystem::path& file) {
    // Документ как в telega-fs: inputDocument с disable_content_type_detection, без подписи.
    return send_document_request(chat_id, file);
}

td::Json album_document_request(int64_t chat_id, const std::vector<std::filesystem::path>& files) {
    td::Json contents = td::Json::array();
    for (const auto& file : files) {
        // TDLib 1.8.70: inputMessageDocument.document — inputDocument (файл и флаг), как в
        // sendMessage.
        contents.push_back(
            td::Json{{"@type", "inputMessageDocument"},
                     {"document",
                      {{"@type", "inputDocument"},
                       {"document", {{"@type", "inputFileLocal"}, {"path", file.string()}}},
                       {"disable_content_type_detection", true}}}});
    }
    return td::Json{{"@type", "sendMessageAlbum"},
                    {"chat_id", chat_id},
                    {"options", send_options()},
                    {"input_message_contents", contents}};
}

std::filesystem::path make_seeded_document(const std::filesystem::path& dir, uint64_t seed,
                                           uint64_t size) {
    std::filesystem::create_directories(dir);
    const std::filesystem::path path = dir / (random_hex(16) + ".bin");
    const std::filesystem::path tmp = path.string() + ".tmp";
    {
        std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
        if (!out) {
            throw std::runtime_error("cannot write " + tmp.string());
        }
        constexpr uint64_t kBlock = 1U << 20;
        std::string block;
        for (uint64_t off = 0; off < size; off += kBlock) {
            const auto n = static_cast<size_t>(std::min<uint64_t>(kBlock, size - off));
            fill_seeded_words(seed, off, block, n);
            out.write(block.data(), static_cast<std::streamsize>(n));
        }
        out.flush();
        if (!out) {
            throw std::runtime_error("write failed: " + tmp.string());
        }
    }
    std::filesystem::rename(tmp, path);
    return path;
}

SendLoad::SendLoad(BenchAccount& bot, int64_t chat_id, Options options)
    : bot_(bot), chat_id_(chat_id), options_(options) {
    succeeded_ = bot_.client().subscribe("updateMessageSendSucceeded",
                                         [this](const td::Json& u) { record(u, false); });
    failed_ = bot_.client().subscribe("updateMessageSendFailed",
                                      [this](const td::Json& u) { record(u, true); });
}

void SendLoad::record(const td::Json& update, bool failed) {
    // Поток приёма TDLib: только раскладываем данные и будим ожидающих.
    if (!update.contains("message") || !update["message"].is_object() ||
        !update.contains("old_message_id") || !update["old_message_id"].is_number_integer()) {
        return;
    }
    const td::Json& message = update["message"];
    if (message.value("chat_id", int64_t{0}) != chat_id_) {
        return;
    }
    Event event;
    event.failed = failed;
    event.at = ExpClock::now();
    event.message_id = message.value("id", int64_t{0});
    if (message.contains("media_album_id") && message["media_album_id"].is_number_integer()) {
        event.album_id = message["media_album_id"].get<int64_t>();
    }
    if (failed) {
        event.error = update.contains("error") ? td::TdError::from_json(update["error"])
                                               : td::TdError::make(td::TdError::kUnexpected,
                                                                   "send failed without error");
    }
    {
        const std::lock_guard<std::mutex> lock(events_mu_);
        events_[{chat_id_, update["old_message_id"].get<int64_t>()}] = std::move(event);
    }
    events_cv_.notify_all();
}

std::optional<SendLoad::Event> SendLoad::take_event(int64_t old_message_id,
                                                    std::chrono::milliseconds slice) {
    std::unique_lock<std::mutex> lock(events_mu_);
    const Key key{chat_id_, old_message_id};
    events_cv_.wait_for(lock, slice, [&] { return events_.contains(key); });
    const auto it = events_.find(key);
    if (it == events_.end()) {
        return std::nullopt;
    }
    Event event = std::move(it->second);
    events_.erase(it);
    return event;
}

SendLoad::~SendLoad() {
    abort();
    std::list<Worker> workers;
    {
        const std::lock_guard<std::mutex> lock(mu_);
        workers.swap(workers_);
    }
    for (auto& w : workers) {
        if (w.thread.joinable()) {
            w.thread.join();
        }
    }
}

int SendLoad::in_flight() const {
    const std::lock_guard<std::mutex> lock(mu_);
    return running_;
}

void SendLoad::abort() { abort_.store(true); }

void SendLoad::launch(SendJob job) {
    const std::lock_guard<std::mutex> lock(mu_);
    const int before = running_;
    ++running_;
    Worker& w = workers_.emplace_back();
    w.thread = std::thread([this, worker = &w, job = std::move(job), before]() mutable {
        run(worker, std::move(job), before);
    });
}

std::vector<SendResult> SendLoad::take_finished() {
    std::vector<std::thread> threads;
    std::vector<SendResult> results;
    {
        const std::lock_guard<std::mutex> lock(mu_);
        for (auto it = workers_.begin(); it != workers_.end();) {
            if (it->finished) {
                threads.push_back(std::move(it->thread));
                results.push_back(std::move(it->result));
                it = workers_.erase(it);
            } else {
                ++it;
            }
        }
    }
    for (auto& t : threads) {
        t.join();
    }
    std::ranges::sort(results, {}, [](const SendResult& r) { return r.end_wall; });
    return results;
}

bool SendLoad::wait_event(milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mu_);
    const auto has_finished = [this] {
        return std::ranges::any_of(workers_, [](const Worker& w) { return w.finished; });
    };
    if (has_finished()) {
        return true;
    }
    cv_.wait_for(lock, timeout, has_finished);
    return has_finished();
}

void SendLoad::run(Worker* worker, SendJob job, int in_flight_at_start) {
    SendResult result;
    result.in_flight_at_start = in_flight_at_start;
    try {
        execute(result, job);
    } catch (const std::exception& e) {
        result.kind = SendKind::kFatal;
        result.error = std::string{"internal: "} + e.what();
        for (auto& d : result.docs) {
            if (d.kind != SendKind::kOk) {
                d.kind = SendKind::kFatal;
                d.error = result.error;
            }
        }
    }
    if (job.remove_files) {
        for (const auto& f : job.files) {
            std::error_code ec;
            std::filesystem::remove(f, ec);
        }
    }
    result.end_wall = std::chrono::system_clock::now();
    result.job = std::move(job);
    {
        const std::lock_guard<std::mutex> lock(mu_);
        --running_;
        result.in_flight_at_end = running_;
        worker->result = std::move(result);
        worker->finished = true;
    }
    cv_.notify_all();
}

void SendLoad::execute(SendResult& result, SendJob& job) {
    if (job.prepare) {
        const auto t_prepare = ExpClock::now();
        job.files = job.prepare();
        result.prepare_ms = ms_since(t_prepare);
    }
    if (job.files.empty() || job.files.size() > 10) {
        throw std::runtime_error("a send job needs 1..10 files");
    }
    for (const auto& f : job.files) {
        std::error_code size_error;
        const auto size = std::filesystem::file_size(f, size_error);
        result.sizes.push_back(size_error ? job.nominal_size : size);
    }
    result.docs.assign(job.files.size(), DocOutcome{});

    result.start_wall = std::chrono::system_clock::now();
    const auto t0 = ExpClock::now();
    const auto finish = [&] {
        SendKind worst = SendKind::kOk;
        for (const auto& d : result.docs) {
            if (severity(d.kind) > severity(worst)) {
                worst = d.kind;
            }
            result.retry_after_s = std::max(result.retry_after_s, d.retry_after_s);
            if (result.error.empty() && !d.error.empty()) {
                result.error = d.error;
            }
        }
        result.kind = worst;
        result.duration_ms = ms_since(t0);
    };
    const auto fail_all = [&](const td::TdError& e) {
        for (auto& d : result.docs) {
            d.kind = classify(e, &d.retry_after_s);
            d.error = scrub(e.to_string());
            d.done_ms = ms_since(t0);
        }
        finish();
    };

    td::Json reply;
    try {
        td::Json request = job.files.size() == 1 ? single_document_request(chat_id_, job.files[0])
                                                 : album_document_request(chat_id_, job.files);
        reply = bot_.client().call(std::move(request), options_.call_timeout);
    } catch (const td::TdException& e) {
        result.call_ms = ms_since(t0);
        result.call_error = scrub(e.error().to_string());
        fail_all(e.error());
        return;
    }
    result.call_ms = ms_since(t0);
    if (job.after_call) {
        job.after_call(job.files);
    }

    std::vector<int64_t> old_ids;
    if (job.files.size() == 1) {
        old_ids.push_back(reply.value("id", int64_t{0}));
    } else if (reply.contains("messages") && reply["messages"].is_array()) {
        for (const auto& m : reply["messages"]) {
            old_ids.push_back(m.is_object() ? m.value("id", int64_t{0}) : int64_t{0});
        }
    }
    old_ids.resize(job.files.size(), 0);

    const auto deadline = ExpClock::now() + options_.send_timeout;
    for (size_t i = 0; i < old_ids.size(); ++i) {
        DocOutcome& d = result.docs[i];
        if (old_ids[i] == 0) {
            d.kind = SendKind::kFatal;
            d.error = "sendMessage returned no message for the document";
            continue;
        }
        for (;;) {
            if (abort_.load()) {
                d.kind = SendKind::kAborted;
                d.error = "aborted";
                break;
            }
            const std::optional<Event> event = take_event(old_ids[i], milliseconds{1000});
            if (event) {
                d.done_ms = std::chrono::duration<double, std::milli>(event->at - t0).count();
                d.album_id = event->album_id;
                if (event->failed) {
                    d.kind = classify(*event->error, &d.retry_after_s);
                    d.error = scrub(event->error->to_string());
                } else {
                    d.kind = SendKind::kOk;
                    d.message_id = event->message_id;
                }
                break;
            }
            if (const auto dead = bot_.client().dead_reason()) {
                d.kind = SendKind::kAborted;
                d.error = scrub(dead->to_string());
                break;
            }
            if (ExpClock::now() > deadline) {
                d.kind = SendKind::kTimeout;
                d.error =
                    "no send result in " + std::to_string(options_.send_timeout.count()) + " min";
                break;
            }
        }
        if (d.done_ms == 0) {
            d.done_ms = ms_since(t0);
        }
    }
    finish();
}

}  // namespace telega::bench
