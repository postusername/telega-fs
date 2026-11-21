// Точечная правка config.toml: остальное содержимое сохраняется (SPEC 12.2).

#include "app/config_edit.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#include "common/files.h"

namespace telega::app {
namespace {

namespace fs = std::filesystem;

ProfileEdit chat(int64_t id) {
    ProfileEdit e;
    e.chat_id = id;
    return e;
}

TEST(ConfigEdit, ReplacesKeyInPlaceKeepingCommentsAndOtherProfiles) {
    const std::string text =
        "# top comment\n"
        "api_id = 1\n"
        "api_hash = \"abc\"  # secret\n"
        "\n"
        "[profiles.main]\n"
        "store = \"telegram\"\n"
        "  chat_id = 5   # filled by init\n"
        "use_test_dc = false\n"
        "\n"
        "[profiles.dev]\n"
        "chat_id = 7\n";
    const std::string got = apply_profile_edit(text, "main", chat(-1001234567890));
    const std::string want =
        "# top comment\n"
        "api_id = 1\n"
        "api_hash = \"abc\"  # secret\n"
        "\n"
        "[profiles.main]\n"
        "store = \"telegram\"\n"
        "  chat_id = -1001234567890   # filled by init\n"
        "use_test_dc = false\n"
        "\n"
        "[profiles.dev]\n"
        "chat_id = 7\n";
    EXPECT_EQ(got, want);
}

TEST(ConfigEdit, InsertsAfterLastKeyOfTheTable) {
    const std::string text =
        "[profiles.main]\n"
        "store = \"telegram\"\n"
        "auth = \"bot\"\n"
        "# trailing note\n"
        "\n"
        "[profiles.dev]\n"
        "auth = \"user\"\n";
    const std::string got = apply_profile_edit(text, "main", chat(-100));
    EXPECT_EQ(got,
              "[profiles.main]\n"
              "store = \"telegram\"\n"
              "auth = \"bot\"\n"
              "chat_id = -100\n"
              "# trailing note\n"
              "\n"
              "[profiles.dev]\n"
              "auth = \"user\"\n");
    // Чужой профиль не тронут, свой разобран.
    const Config cfg = parse_config(got);
    EXPECT_EQ(cfg.profile("main").chat_id, -100);
    EXPECT_FALSE(cfg.profile("dev").chat_id.has_value());
}

TEST(ConfigEdit, InsertsIntoEmptyTableAndLastTableWithoutNewline) {
    EXPECT_EQ(apply_profile_edit("[profiles.main]\n", "main", chat(-1)),
              "[profiles.main]\nchat_id = -1\n");
    EXPECT_EQ(apply_profile_edit("[profiles.main]\nstore = \"telegram\"", "main", chat(-1)),
              "[profiles.main]\nstore = \"telegram\"\nchat_id = -1\n");
}

TEST(ConfigEdit, AppendsMissingProfile) {
    EXPECT_EQ(apply_profile_edit("api_id = 1\n", "main", chat(-9)),
              "api_id = 1\n\n[profiles.main]\nchat_id = -9\n");
    EXPECT_EQ(apply_profile_edit("", "main", chat(-9)), "[profiles.main]\nchat_id = -9\n");
    EXPECT_EQ(apply_profile_edit("", "my profile", chat(-9)),
              "[profiles.\"my profile\"]\nchat_id = -9\n");
}

TEST(ConfigEdit, UnderstandsHeaderSpellings) {
    for (const std::string header : {"[profiles.\"main\"]", "[ profiles . main ]",
                                     "[profiles.'main'] # comment", "  [profiles.main]"}) {
        const std::string got = apply_profile_edit(header + "\nauth = \"user\"\n", "main", chat(3));
        EXPECT_EQ(parse_config(got).profile("main").chat_id, 3) << header;
        EXPECT_NE(got.find(header), std::string::npos) << header;  // заголовок сохранён
    }
}

TEST(ConfigEdit, KeepsCrLfLineEndings) {
    const std::string got =
        apply_profile_edit("[profiles.main]\r\nauth = \"bot\"\r\n", "main", chat(4));
    EXPECT_EQ(got, "[profiles.main]\r\nauth = \"bot\"\r\nchat_id = 4\r\n");
}

TEST(ConfigEdit, SetsBothKeys) {
    ProfileEdit e;
    e.chat_id = -5;
    e.use_test_dc = true;
    const std::string got = apply_profile_edit("[profiles.main]\nuse_test_dc = false\n", "main", e);
    const Config cfg = parse_config(got);
    EXPECT_EQ(cfg.profile("main").chat_id, -5);
    EXPECT_TRUE(cfg.profile("main").use_test_dc);
    EXPECT_EQ(got, "[profiles.main]\nuse_test_dc = true\nchat_id = -5\n");
}

TEST(ConfigEdit, RefusesFormsItCannotEdit) {
    // Inline-таблица: построчная правка не нашла бы таблицу.
    EXPECT_THROW(
        (void)apply_profile_edit("profiles = { main = { auth = \"bot\" } }\n", "main", chat(1)),
        ConfigError);
    // Файл, который уже не разбирается.
    EXPECT_THROW((void)apply_profile_edit("[profiles.main\nauth = \n", "main", chat(1)),
                 ConfigError);
    // Результат не должен ломать файл: неверное значение рядом не маскируется.
    try {
        (void)apply_profile_edit("profiles = { main = { auth = \"bot\" } }\n", "main", chat(-7));
        FAIL();
    } catch (const ConfigError& e) {
        EXPECT_NE(std::string(e.what()).find("chat_id = -7"), std::string::npos);  // подсказка
    }
}

class ConfigEditFile : public ::testing::Test {
  protected:
    void SetUp() override {
        root_ = fs::temp_directory_path() / ("telega-cfgedit-" + std::to_string(::getpid()));
        fs::remove_all(root_);
        fs::create_directories(root_);
    }
    void TearDown() override { fs::remove_all(root_); }
    fs::path root_;
};

TEST_F(ConfigEditFile, WritesAtomicallyWithMode0600) {
    const fs::path path = root_ / "config.toml";
    write_file_atomic(path, as_bytes(std::string("api_id = 1\n[profiles.main]\nauth = \"bot\"\n")),
                      0600);
    edit_profile_file(path, "main", chat(-100123));
    EXPECT_EQ(fs::status(path).permissions() & fs::perms::all,
              fs::perms::owner_read | fs::perms::owner_write);
    EXPECT_EQ(load_config(path).profile("main").chat_id, -100123);
    EXPECT_EQ(load_config(path).api_id, 1);
    // Каталог не замусорен временными файлами: остаются только config.toml и файл блокировки.
    std::vector<std::string> names;
    for (const auto& e : fs::directory_iterator(root_)) {
        names.push_back(e.path().filename().string());
    }
    std::sort(names.begin(), names.end());
    EXPECT_EQ(names, (std::vector<std::string>{"config.toml", "config.toml.lock"}));
}

TEST_F(ConfigEditFile, SymlinkIsKeptAndItsTargetIsEdited) {
    fs::create_directories(root_ / "dotfiles");
    const fs::path real = root_ / "dotfiles" / "telega.toml";
    const fs::path link = root_ / "config.toml";
    write_file_atomic(real, as_bytes(std::string("api_id = 1\n[profiles.main]\nauth = \"bot\"\n")),
                      0600);
    fs::create_symlink(real, link);
    edit_profile_file(link, "main", chat(-5));
    EXPECT_TRUE(fs::is_symlink(link));  // не заменена обычным файлом
    EXPECT_EQ(load_config(real).profile("main").chat_id, -5);
    EXPECT_EQ(load_config(link).profile("main").chat_id, -5);
}

// Висячая ссылка (цели нет, например dotfiles ещё не разложены): цель создаётся, ссылка остаётся.
TEST_F(ConfigEditFile, DanglingSymlinkCreatesItsTarget) {
    const fs::path real = root_ / "dotfiles" / "telega.toml";
    const fs::path link = root_ / "config.toml";
    fs::create_symlink(real, link);
    ASSERT_FALSE(fs::exists(link));
    edit_profile_file(link, "main", chat(-7));
    EXPECT_TRUE(fs::is_symlink(link));
    EXPECT_TRUE(fs::is_regular_file(real));
    EXPECT_EQ(load_config(real).profile("main").chat_id, -7);
}

// Цепочка ссылок, в конце которой висячая, и относительная ссылка.
TEST_F(ConfigEditFile, ChainOfSymlinksEndingInADanglingOneIsFollowed) {
    fs::create_directories(root_ / "a");
    fs::create_symlink("real.toml", root_ / "a" / "mid.toml");  // относительная, висячая
    const fs::path link = root_ / "config.toml";
    fs::create_symlink(root_ / "a" / "mid.toml", link);
    edit_profile_file(link, "main", chat(-8));
    EXPECT_TRUE(fs::is_symlink(link));
    EXPECT_TRUE(fs::is_symlink(root_ / "a" / "mid.toml"));
    EXPECT_EQ(load_config(root_ / "a" / "real.toml").profile("main").chat_id, -8);
}

TEST_F(ConfigEditFile, SymlinkLoopIsAConfigError) {
    const fs::path link = root_ / "config.toml";
    fs::create_symlink(link, link);
    EXPECT_THROW(edit_profile_file(link, "main", chat(-9)), ConfigError);
}

TEST_F(ConfigEditFile, ConcurrentEditsOfDifferentProfilesAreNotLost) {
    const fs::path path = root_ / "config.toml";
    std::string text = "api_id = 1\n";
    constexpr int kProfiles = 8;
    for (int i = 0; i < kProfiles; ++i) {
        text += "[profiles.p" + std::to_string(i) + "]\nauth = \"bot\"\n";
    }
    write_file_atomic(path, as_bytes(text), 0600);
    std::vector<std::thread> threads;
    threads.reserve(kProfiles);
    for (int i = 0; i < kProfiles; ++i) {
        threads.emplace_back(
            [&, i] { edit_profile_file(path, "p" + std::to_string(i), chat(-i - 1)); });
    }
    for (auto& t : threads) {
        t.join();
    }
    const Config cfg = load_config(path);
    for (int i = 0; i < kProfiles; ++i) {
        EXPECT_EQ(cfg.profile("p" + std::to_string(i)).chat_id, -i - 1) << "profile p" << i;
    }
}

TEST_F(ConfigEditFile, CreatesMissingFileAndDirectory) {
    const fs::path path = root_ / "sub" / "config.toml";
    edit_profile_file(path, "main", chat(-1));
    EXPECT_EQ(load_config(path).profile("main").chat_id, -1);
    EXPECT_EQ(fs::status(path).permissions() & fs::perms::all,
              fs::perms::owner_read | fs::perms::owner_write);
}

TEST_F(ConfigEditFile, FailedEditLeavesFileUntouched) {
    const fs::path path = root_ / "config.toml";
    const std::string text = "profiles = { main = { auth = \"bot\" } }\n";
    write_file_atomic(path, as_bytes(text), 0600);
    EXPECT_THROW(edit_profile_file(path, "main", chat(1)), ConfigError);
    const Bytes raw = read_file(path);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(raw.data()), raw.size()), text);
}

}  // namespace
}  // namespace telega::app
