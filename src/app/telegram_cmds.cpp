#include "app/telegram_cmds.h"

#include <spdlog/spdlog.h>

#include <iostream>
#include <optional>
#include <string>
#include <utility>

#include "app/config_edit.h"
#include "app/terminal_prompter.h"
#include "common/bytes.h"
#include "crypto/keys.h"

namespace telega::app {

namespace fs = std::filesystem;

namespace {

std::string profile_hint(const GlobalArgs& global) {
    return global.profile == "main" ? std::string{} : " -p " + global.profile;
}

[[noreturn]] void not_logged_in(const GlobalArgs& global) {
    throw CommandError("the Telegram session is not logged in: run `telega-fs login" +
                       profile_hint(global) + "` first");
}

void require_telegram(const Loaded& l, const GlobalArgs& global, const char* command) {
    if (l.profile.store != StoreKind::telegram) {
        throw CommandError(std::string(command) + " is not needed for profile '" + global.profile +
                           "': it has store = \"local\"");
    }
}

crypto::KeyringFile read_keyring(const fs::path& path) {
    try {
        return crypto::load_keyring(path);
    } catch (const crypto::CorruptError& e) {
        throw CommandError(std::string("the local keyring ") + path.string() +
                           " is damaged: " + e.what());
    }
}

// Создать keyring без ФС (SPEC 11, «Keyring до init»). Возвращает tdlib_db_key.
crypto::Key create_bare_keyring(const Loaded& l) {
    crypto::KdfParams kdf;
    crypto::Key kek;
    if (!l.profile.keyfile.empty()) {
        kdf = crypto::keyfile_kdf_params();
        try {
            kek = crypto::read_keyfile(l.profile.keyfile);
        } catch (const std::exception& e) {
            throw CommandError(std::string("cannot use the keyfile: ") + e.what());
        }
    } else {
        Secret first;
        Secret second;
        read_secret("New passphrase: ", first);
        read_secret("Repeat passphrase: ", second);
        if (first.empty()) {
            throw CommandError("the passphrase must not be empty");
        }
        if (first.value() != second.value()) {
            throw CommandError("the passphrases do not match");
        }
        kdf = crypto::passphrase_kdf_params();
        kek = crypto::derive_kek(first.value(), kdf);
    }
    const crypto::Key tdlib_db = crypto::Key::random();
    crypto::save_keyring(l.paths.keyring(), crypto::make_keyring_without_fs(kdf, kek, tdlib_db));
    std::cout << "Created the local keyring " << l.paths.keyring().string() << '\n'
              << "WARNING: if you lose the "
              << (l.profile.keyfile.empty() ? "passphrase"
                                            : "keyfile " + l.profile.keyfile.string())
              << ", your data is lost for good: there is no recovery. Keep a copy.\n";
    return tdlib_db;
}

}  // namespace

crypto::KeyringFile load_keyring_for(const Loaded& l, const GlobalArgs& global) {
    if (!fs::exists(l.paths.keyring())) {
        throw CommandError("no keyring for profile '" + global.profile + "': run `telega-fs login" +
                           profile_hint(global) + "` first");
    }
    return read_keyring(l.paths.keyring());
}

meta::KeySource keyring_key_source(const crypto::KeyringFile& keyring, const fs::path& keyfile,
                                   Secret& passphrase, const char* prompt) {
    meta::KeySource source;
    if (keyring.kdf.alg == crypto::KdfAlg::keyfile) {
        if (keyfile.empty()) {
            throw CommandError(
                "the keyring is protected by a keyfile: set `keyfile` in the profile (or pass "
                "--keyfile)");
        }
        source.keyfile = keyfile;
        return source;
    }
    if (!keyfile.empty()) {
        throw CommandError(
            "the keyring is protected by a passphrase, but a keyfile is given: remove it from "
            "the profile (or --keyfile)");
    }
    read_secret(prompt, passphrase);
    source.passphrase = passphrase.value();
    return source;
}

crypto::Key unlock_tdlib_key_or_throw(const crypto::KeyringFile& keyring,
                                      const meta::KeySource& source) {
    crypto::init();
    crypto::Key kek;
    try {
        kek = meta::kek_from(source, keyring.kdf);
    } catch (const meta::MountError& e) {
        throw CommandError(describe_exception(e));
    }
    auto key = crypto::unlock_tdlib_key(keyring, kek);
    if (!key) {
        throw CommandError("wrong passphrase or keyfile");
    }
    return *key;
}

store::RateLimiterOptions limiter_options(const Profile& p) {
    store::RateLimiterOptions o;
    o.actions_per_minute = static_cast<double>(p.send_rate);
    o.bytes_per_minute = p.send_bytes_rate;
    return o;
}

store::TelegramStoreOptions store_options(const Profile& p, int64_t chat_id) {
    store::TelegramStoreOptions o;
    o.chat_id = chat_id;
    o.upload_concurrency = p.upload_concurrency;
    o.download_concurrency = p.download_concurrency;
    o.album_max_doc_size = p.album_max_doc_size;
    o.read_timeout = p.read_timeout;
    return o;
}

std::string describe_account(const td::Json& me) {
    std::string kind = "account";
    if (me.contains("type") && me["type"].is_object()) {
        const std::string t = me["type"].value("@type", std::string{});
        if (t == "userTypeBot") {
            kind = "bot";
        } else if (t == "userTypeRegular") {
            kind = "user";
        } else {
            kind = t;
        }
    }
    std::string name = "-";
    if (me.contains("usernames") && me["usernames"].is_object()) {
        const auto& names = me["usernames"];
        if (names.contains("active_usernames") && names["active_usernames"].is_array() &&
            !names["active_usernames"].empty() && names["active_usernames"][0].is_string()) {
            name = "@" + names["active_usernames"][0].get<std::string>();
        }
    }
    return kind + " " + name + " (id " + std::to_string(me.value("id", int64_t{0})) + ")";
}

// --- TelegramBackend ---

TelegramBackend::TelegramBackend(const Loaded& loaded, const crypto::Key& tdlib_db_key)
    : loaded_(loaded),
      session_(loaded.paths, loaded.profile.use_test_dc, api_credentials(loaded.config),
               tdlib_db_key) {}

TelegramBackend::~TelegramBackend() {
    // Порядок: хранилище, лимитер (члены уничтожаются в обратном порядке), затем клиент и
    // рантайм TDLib в деструкторе сессии.
    store_.reset();
    limiter_.reset();
    session_.close();
}

void TelegramBackend::connect() {
    try {
        session_.authorize_existing();
    } catch (const NotLoggedIn&) {
        // Профиль мог быть передан не как -p: подсказка берётся из имени в Profile.
        GlobalArgs g;
        g.profile = loaded_.profile.name;
        not_logged_in(g);
    }
}

store::TelegramStoreOptions mount_store_options(const Loaded& loaded, int64_t chat_id,
                                                bool read_only) {
    store::TelegramStoreOptions options = store_options(loaded.profile, chat_id);
    // Кэш чтения (SPEC 8.2): файлы TDLib в tdfiles/, учёт в state.sqlite профиля. В --ro
    // state.sqlite не меняется (SPEC 7.2): без state_db учёт ведётся только в памяти, таблица
    // read_cache не создаётся и не пишется.
    if (!read_only) {
        options.state_db = loaded.paths.state_db();
    }
    options.tdfiles_dir = loaded.paths.tdfiles();
    options.cache_limit = loaded.profile.cache_limit;
    return options;
}

void TelegramBackend::open_store(int64_t chat_id, bool read_only) {
    limiter_ = std::make_unique<store::RateLimiter>(limiter_options(loaded_.profile));
    store::TelegramStoreOptions options = mount_store_options(loaded_, chat_id, read_only);
    store_ =
        std::make_unique<store::TelegramStore>(session_.client(), *limiter_, std::move(options));
    store_->open();
}

void TelegramBackend::start_read_cache() {
    if (store_) {
        store_->start_read_cache();
    }
}

store::Store& TelegramBackend::store() {
    if (!store_) {
        throw std::logic_error("the Telegram store is not open");
    }
    return *store_;
}

// --- login ---

namespace {

int login_impl(const LoginArgs& args) {
    const Loaded l = load_profile(args.global);
    require_telegram(l, args.global, "login");
    if (args.bot && args.qr) {
        throw CommandError("--bot and --qr cannot be combined: a QR code is for user accounts");
    }
    const ApiCredentials api = api_credentials(l.config);
    crypto::init();

    // Режим: --bot и --qr сильнее профиля; без них — auth из профиля (SPEC 10.3).
    const bool bot = args.bot || (!args.qr && l.profile.auth == AuthMode::bot);
    const bool test_dc = args.test_dc || l.profile.use_test_dc;

    ensure_profile_dirs(l.paths);
    const ProfileLock lock{l.paths.lock()};

    if (args.test_dc && !l.profile.use_test_dc) {
        // mount и init читают use_test_dc из профиля: без записи сессия тестового DC не нашлась
        // бы на боевых серверах.
        ProfileEdit edit;
        edit.use_test_dc = true;
        edit_profile_file(l.config_path, args.global.profile, edit);
        std::cout << "Set use_test_dc = true in profile '" << args.global.profile << "' of "
                  << l.config_path.string() << '\n';
    }

    crypto::Key tdlib_key;
    if (fs::exists(l.paths.keyring())) {
        const crypto::KeyringFile keyring = read_keyring(l.paths.keyring());
        Secret passphrase;
        const meta::KeySource source =
            keyring_key_source(keyring, l.profile.keyfile, passphrase, "Passphrase: ");
        tdlib_key = unlock_tdlib_key_or_throw(keyring, source);
    } else {
        tdlib_key = create_bare_keyring(l);
    }

    TgSession session{l.paths, test_dc, api, tdlib_key};
    TerminalPrompter::Options popts;
    popts.qr = args.qr;
    TerminalPrompter prompter{std::move(popts)};
    std::cerr << "Logging in " << (bot ? "a bot" : "a user") << (test_dc ? " on the test DC" : "")
              << "...\n";
    session.authorize(prompter, bot ? td::AuthMode::kBot : td::AuthMode::kUser);
    const td::Json me = session.me();
    std::cout << "Logged in as " << describe_account(me) << '\n';

    const bool is_bot =
        me.contains("type") && me["type"].value("@type", std::string{}) == "userTypeBot";
    if (is_bot != (l.profile.auth == AuthMode::bot)) {
        std::cout << "Note: profile '" << args.global.profile << "' has auth = \""
                  << (l.profile.auth == AuthMode::bot ? "bot" : "user") << "\" but this is "
                  << (is_bot ? "a bot" : "a user")
                  << " account; set auth in config.toml accordingly.\n";
    }
    if (!l.profile.chat_id) {
        std::cout << "Next: telega-fs init" << profile_hint(args.global)
                  << (is_bot ? " --chat ID" : " (--chat ID | --title T)") << '\n';
    }
    session.close();
    return 0;
}

// --- logout ---

int logout_impl(const LogoutArgs& args) {
    const Loaded l = load_profile(args.global);
    require_telegram(l, args.global, "logout");
    crypto::init();
    ensure_profile_dirs(l.paths);
    const ProfileLock lock{l.paths.lock()};

    if (!fs::exists(l.paths.tdlib())) {
        std::cout << "Profile '" << args.global.profile << "' has no Telegram session.\n";
        return 0;
    }

    bool server_side_done = false;
    if (fs::exists(l.paths.keyring())) {
        const crypto::KeyringFile keyring = read_keyring(l.paths.keyring());
        Secret passphrase;
        const meta::KeySource source =
            keyring_key_source(keyring, l.profile.keyfile, passphrase, "Passphrase: ");
        const crypto::Key tdlib_key = unlock_tdlib_key_or_throw(keyring, source);
        TgSession session{l.paths, l.profile.use_test_dc, api_credentials(l.config), tdlib_key};
        try {
            session.authorize_existing();
            server_side_done = session.log_out();
            if (!server_side_done) {
                std::cerr << "Warning: Telegram did not confirm the logout in time (no "
                             "network?). The session stays active on the server: end it in "
                             "Telegram, Settings, Devices.\n";
            }
        } catch (const NotLoggedIn&) {
            std::cout << "The session was not logged in; removing its files.\n";
        } catch (const td::TdException& e) {
            std::cerr << "Warning: cannot log out on the server (" << describe_exception(e)
                      << "). The session may stay active: end it in Telegram, Settings, "
                         "Devices.\n";
        }
        session.close();
    } else {
        std::cerr << "Warning: there is no keyring, so the TDLib database cannot be opened to "
                     "log out on the server. Removing the local session only; end it in "
                     "Telegram, Settings, Devices.\n";
    }

    std::error_code ec;
    fs::remove_all(l.paths.tdlib(), ec);
    if (ec) {
        throw CommandError("cannot remove " + l.paths.tdlib().string() + ": " + ec.message());
    }
    fs::remove_all(l.paths.tdfiles(), ec);
    std::cout << (server_side_done ? "Logged out" : "Removed the local session")
              << " (keyring kept: " << l.paths.keyring().string() << ")\n";
    return 0;
}

}  // namespace

int run_login(const LoginArgs& args) {
    try {
        return login_impl(args);
    } catch (const std::exception& e) {
        std::cerr << "telega-fs: " << describe_exception(e) << '\n';
        return 1;
    }
}

int run_logout(const LogoutArgs& args) {
    try {
        return logout_impl(args);
    } catch (const std::exception& e) {
        std::cerr << "telega-fs: " << describe_exception(e) << '\n';
        return 1;
    }
}

// --- init ---

namespace {

bool is_bot(const td::Json& me) {
    return me.contains("type") && me["type"].is_object() &&
           me["type"].value("@type", std::string{}) == "userTypeBot";
}

// createNewSupergroupChat(is_channel = true): канал создаёт пользователь (SPEC 10.3).
int64_t create_channel(TgSession& session, const std::string& title) {
    const td::Json chat = session.client().call(td::Json{{"@type", "createNewSupergroupChat"},
                                                         {"title", title},
                                                         {"is_forum", false},
                                                         {"is_channel", true},
                                                         {"description", ""},
                                                         {"message_auto_delete_time", 0},
                                                         {"for_import", false}},
                                                std::chrono::seconds{60});
    const int64_t id = chat.value("id", int64_t{0});
    if (id == 0) {
        throw CommandError("Telegram created the channel but returned no chat id");
    }
    return id;
}

}  // namespace

int init_telegram(const Loaded& l, const InitArgs& args, const meta::InitOptions& base_opts) {
    if (static_cast<int>(args.chat.has_value()) + static_cast<int>(args.title.has_value()) != 1) {
        throw CommandError(
            "specify the channel: --chat ID (an empty existing channel; the only option for a "
            "bot) or --title T (create a new one; user accounts only)");
    }
    if (args.title && args.title->empty()) {
        throw CommandError("the channel title must not be empty");
    }
    // Поля init: keyring уже создан login, ключи берутся из него (SPEC 11).
    const crypto::KeyringFile keyring = load_keyring_for(l, args.global);
    // Профиль занимается до любой проверки его состояния: иначе параллельный init или mount
    // менял бы базы между проверкой и самим init.
    ensure_profile_dirs(l.paths);
    const ProfileLock lock{l.paths.lock()};
    try {
        (void)meta::inspect_profile_for_init(meta::FsPaths{l.paths.data_dir});
    } catch (const meta::InitError& e) {
        throw CommandError(std::string(e.what()) + "; init refuses to overwrite it");
    }
    // Keyring с ФС: если она уже закоммичена, init отказывает; если нет (прерванный init), он
    // повторяется (SPEC 12.3).
    fs::path keyfile = l.profile.keyfile;
    if (args.keyfile) {
        keyfile = absolute_path(*args.keyfile);
    }
    Secret passphrase;
    meta::KeySource source = keyring_key_source(keyring, keyfile, passphrase, "Passphrase: ");
    const crypto::Key tdlib_key = unlock_tdlib_key_or_throw(keyring, source);

    TelegramBackend backend{l, tdlib_key};
    backend.connect();
    const td::Json me = backend.session().me();
    std::cout << "Telegram account: " << describe_account(me) << '\n';

    int64_t chat_id = 0;
    bool created = false;
    if (args.chat) {
        chat_id = *args.chat;
    } else if (args.title) {
        if (is_bot(me)) {
            throw CommandError(
                "a bot cannot create channels: create a private channel with your own account, "
                "add the bot as an administrator (post, edit and delete messages) and run "
                "`telega-fs init --chat ID`");
        }
        chat_id = create_channel(backend.session(), *args.title);
        created = true;
        std::cout << "Created the channel '" << *args.title << "' (chat id " << chat_id << ")\n";
    }

    meta::InitOptions opts = base_opts;
    opts.key = source;
    // Кэш TDLib может до полутора минут скрывать свой pin (SPEC 2, 12.3).
    opts.stale_head_window = meta::CommitConfig{}.cas_retry_window;
    opts.stale_head_period = meta::CommitConfig{}.cas_retry_period;
    // Копии пароля затираются на любом выходе (Secret затирает свою сам).
    struct KeyWipe {
        meta::KeySource& key;
        ~KeyWipe() { wipe(key.passphrase); }
    };
    const KeyWipe wipe_source{source};
    const KeyWipe wipe_opts{opts.key};
    meta::InitResult result;
    try {
        // Проверки канала (SPEC 12.3): доступен, автоудаление выключено, права — в open();
        // «закреплённых нет» проверяет init_fs.
        backend.open_store(chat_id);
        result = meta::init_fs(backend.store(), meta::FsPaths{l.paths.data_dir}, opts);
    } catch (...) {
        if (created) {
            std::cerr << "The channel " << chat_id
                      << " stays in your account: retry with `telega-fs init"
                      << profile_hint(args.global) << " --chat " << chat_id << "`.\n";
        }
        throw;
    }

    std::cout << "Initialized file system " << to_hex(result.fs_uuid) << " in profile '"
              << args.global.profile << "' (chunk size " << opts.chunk_size << " bytes)\n"
              << "Channel:        " << chat_id << '\n'
              << "Data directory: " << l.paths.data_dir.string() << '\n';

    ProfileEdit edit;
    edit.chat_id = chat_id;
    try {
        edit_profile_file(l.config_path, args.global.profile, edit);
        std::cout << "Saved chat_id in profile '" << args.global.profile << "' of "
                  << l.config_path.string() << '\n';
    } catch (const std::exception& e) {
        std::cerr << "telega-fs: the file system is initialized, but chat_id could not be saved: "
                  << e.what() << "\nSet chat_id = " << chat_id << " in the profile by hand.\n";
        return 1;
    }
    std::cout << "\nWARNING: if you lose the "
              << (keyfile.empty() ? "passphrase" : "keyfile " + keyfile.string())
              << ", your data is lost for good: there is no recovery. Keep a copy.\n";
    return 0;
}

}  // namespace telega::app
