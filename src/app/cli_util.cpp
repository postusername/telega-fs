#include "app/cli_util.h"

#include <sodium.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/stdout_sinks.h>
#include <spdlog/spdlog.h>
#include <termios.h>
#include <unistd.h>

#include <iostream>
#include <memory>
#include <utility>

#include "crypto/keys.h"
#include "meta/mount.h"
#include "store/store.h"
#include "td/td_error.h"

namespace telega::app {

namespace fs = std::filesystem;

Secret::~Secret() { wipe(); }

void Secret::set(std::string value) {
    wipe();
    value_ = std::move(value);
}

void Secret::wipe() {
    if (!value_.empty()) {
        sodium_memzero(value_.data(), value_.size());
    }
    value_.clear();
}

void read_secret(const char* prompt, Secret& out) {
    const bool tty = ::isatty(STDIN_FILENO) != 0;
    termios saved{};
    bool echo_off = false;
    if (tty) {
        if (::tcgetattr(STDIN_FILENO, &saved) == 0) {
            termios quiet = saved;
            quiet.c_lflag &= ~static_cast<tcflag_t>(ECHO);
            echo_off = ::tcsetattr(STDIN_FILENO, TCSAFLUSH, &quiet) == 0;
        }
        std::cerr << prompt << std::flush;
    }
    std::string line;
    const bool ok = static_cast<bool>(std::getline(std::cin, line));
    if (echo_off) {
        ::tcsetattr(STDIN_FILENO, TCSAFLUSH, &saved);
    }
    if (tty) {
        std::cerr << '\n';
    }
    out.set(std::move(line));
    if (!ok && out.empty()) {
        throw CommandError("cannot read from standard input");
    }
}

std::string read_line(const std::string& prompt) {
    std::cerr << prompt << std::flush;
    std::string line;
    if (!std::getline(std::cin, line)) {
        throw CommandError("no input on standard input");
    }
    while (!line.empty() && (line.back() == '\r' || line.back() == ' ' || line.back() == '\t')) {
        line.pop_back();
    }
    size_t start = 0;
    while (start < line.size() && (line[start] == ' ' || line[start] == '\t')) {
        ++start;
    }
    return line.substr(start);
}

// Затереть копию пароля в std::optional (best effort).
void wipe(std::optional<std::string>& s) {
    if (s && !s->empty()) {
        sodium_memzero(s->data(), s->size());
    }
    s.reset();
}

fs::path absolute_path(const fs::path& p) { return fs::absolute(p).lexically_normal(); }

Loaded load_profile(const GlobalArgs& global) {
    Loaded l;
    l.config_path = global.config ? absolute_path(*global.config) : default_config_path();
    l.config = load_config(l.config_path);
    l.profile = l.config.profile(global.profile);
    if (l.profile.store == StoreKind::local) {
        if (l.profile.local_dir.empty()) {
            throw CommandError("profile '" + global.profile +
                               "' has store = \"local\" but no local_dir");
        }
        l.profile.local_dir = absolute_path(l.profile.local_dir);
    }
    if (!l.profile.keyfile.empty()) {
        l.profile.keyfile = absolute_path(l.profile.keyfile);
    }
    l.paths = profile_paths(global.profile);
    return l;
}

void setup_logging(bool verbose, bool color) {
    spdlog::drop("telega-fs");  // повторный вызов (в тестах) не должен падать на занятом имени
    std::shared_ptr<spdlog::logger> logger =
        color ? spdlog::stderr_color_mt("telega-fs") : spdlog::stderr_logger_mt("telega-fs");
    logger->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%l] %v");
    logger->set_level(verbose ? spdlog::level::debug : spdlog::level::info);
    logger->flush_on(spdlog::level::info);
    spdlog::set_default_logger(std::move(logger));
}

namespace {

std::string describe(const meta::MountError& e) {
    using meta::MountErrorKind;
    switch (e.kind()) {
        case MountErrorKind::wrong_password:
            return "wrong passphrase or keyfile";
        case MountErrorKind::bad_key_source:
            return std::string("key source mismatch: ") + e.what();
        case MountErrorKind::no_head:
            return std::string("the channel has no file system head: ") + e.what();
        case MountErrorKind::not_index_blob:
            return std::string("the channel head is not an index blob: ") + e.what();
        case MountErrorKind::head_lost:
            return std::string("the file system head was lost: ") + e.what();
        case MountErrorKind::corrupt:
            return std::string("the channel data is corrupted or was tampered with: ") + e.what();
        case MountErrorKind::foreign_fs:
            return std::string("the channel or local files belong to a different file system: ") +
                   e.what();
        case MountErrorKind::local_conflict:
            return std::string(
                       "the channel is newer than the local state and there are local "
                       "changes: ") +
                   e.what() +
                   "\n  use --ro to mount read-only without changes, or --discard-local to drop "
                   "the local changes";
        case MountErrorKind::rollback:
            return std::string("the channel head is older than the local state (rolled back?): ") +
                   e.what() +
                   "\n  use --ro to mount read-only, or --force-local to upload the local state "
                   "as the new head";
        case MountErrorKind::local_state:
            return std::string("the local state files are damaged or inconsistent: ") + e.what();
    }
    return e.what();
}

}  // namespace

std::string describe_exception(const std::exception& e) {
    if (const auto* me = dynamic_cast<const meta::MountError*>(&e)) {
        return describe(*me);
    }
    if (const auto* se = dynamic_cast<const store::StoreError*>(&e)) {
        return std::string("channel storage error: ") + se->what();
    }
    if (const auto* ce = dynamic_cast<const crypto::CorruptError*>(&e)) {
        return std::string("file system data is corrupted: ") + ce->what();
    }
    if (const auto* te = dynamic_cast<const td::TdException*>(&e)) {
        const td::TdError& err = te->error();
        switch (err.code) {
            case td::TdError::kTimeout:
                return "Telegram did not answer in time (no network?)";
            case td::TdError::kStopped:
            case td::TdError::kClosed:
                return "the Telegram client was closed";
            default: {
                std::string text = "Telegram error " + err.to_string();
                if (err.code == 401) {
                    text +=
                        "; if the session is damaged or was ended, run `telega-fs logout` and "
                        "`telega-fs login` again";
                }
                return text;
            }
        }
    }
    return e.what();
}

}  // namespace telega::app
