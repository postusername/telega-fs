// fsx на смонтированной ФС не меньше 100 000 операций без расхождений и
// прогон с перемонтированием посередине. Длинные прогоны — в наборе FuseFsxLong (метка long).

#include <gtest/gtest.h>

#include <chrono>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

#include "fsx.h"
#include "harness.h"

namespace telega::itest {
namespace {

class FuseFsxTest : public ::testing::Test {
  protected:
    void SetUp() override {
        if (const std::string why = fuse_skip_reason(); !why.empty()) {
            GTEST_SKIP() << why;
        }
    }

    void TearDown() override {
        if (HasFailure() && env_) {
            std::cerr << "---- telega-fs.log ----\n" << env_->log_text() << '\n';
        }
    }

    // fsx; состояние и дампы лежат вне точки монтирования. С progress_log вывод fsx (прогресс
    // через -p) пишется без буферизации в fsx-<набор>.<тест>.log в рабочем каталоге теста
    // (build/<пресет>/tests/integration), чтобы за длинным прогоном можно было следить tail -f.
    // Первый вызов в тесте файл перезаписывает, следующие (продолжение после перемонтирования)
    // дописывают.
    int run(std::vector<std::string> args, const fs::path& file, bool progress_log = false) {
        args.emplace_back("--dump-dir");
        args.push_back((env_->scratch() / "fsx-dumps").string());
        args.emplace_back("--state");
        args.push_back((env_->scratch() / "fsx.state").string());
        args.push_back(file.string());
        fs::create_directories(env_->scratch() / "fsx-dumps");
        std::ostringstream out;
        std::ostringstream err;
        std::ofstream log;
        if (progress_log) {
            const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
            const std::string name =
                std::string("fsx-") + info->test_suite_name() + "." + info->name() + ".log";
            log.open(name, progress_log_started_ ? std::ios::app : std::ios::trunc);
            progress_log_started_ = true;
            log << std::unitbuf;
            std::cerr << "[fsx] progress: " << (fs::current_path() / name).string() << '\n';
        }
        std::ostream& sink = progress_log ? static_cast<std::ostream&>(log) : out;
        const auto t0 = std::chrono::steady_clock::now();
        const int rc = fsx::fsx_main(args, sink, err);
        const auto secs = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - t0)
                              .count();
        std::cerr << "[fsx] exit " << rc << " in " << static_cast<double>(secs) / 1000.0 << " s\n";
        if (rc != 0) {
            std::cerr << out.str() << err.str();
        } else {
            const std::string text = out.str();
            const auto tail = text.size() > 400 ? text.size() - 400 : 0;
            std::cerr << text.substr(tail);
        }
        return rc;
    }

    std::unique_ptr<Env> env_;
    bool progress_log_started_ = false;
};

// Короткий прогон на каждом запуске: чанки по 1 МиБ, файл до 3 МиБ пересекает границы.
TEST_F(FuseFsxTest, Fsx10kOps) {
    env_ = std::make_unique<Env>();
    env_->init();
    auto m = env_->mount();
    EXPECT_EQ(run({"-q", "-N", "10000", "-S", "1", "-l", "3m"}, m->path("fsx.dat")), 0);
    EXPECT_TRUE(m->unmount().ok());
}

// Длинные прогоны: метка long, большой таймаут (tests/integration/CMakeLists.txt).
class FuseFsxLong : public FuseFsxTest {};

// 100 000 операций без расхождений.
TEST_F(FuseFsxLong, Fsx100kOps) {
    env_ = std::make_unique<Env>();
    env_->init();
    auto m = env_->mount();
    EXPECT_EQ(
        run({"-p", "1000", "-N", "100000", "-S", "20260117", "-l", "16m", "--full-check", "5000"},
            m->path("fsx.dat"), /*progress_log=*/true),
        0);
    EXPECT_TRUE(m->unmount().ok());
}

// Перемонтирование посередине прогона сохраняет данные.
TEST_F(FuseFsxLong, Fsx50kOpsWithRemount) {
    env_ = std::make_unique<Env>();
    env_->init();
    const std::vector<std::string> common = {"-p",  "1000", "-N", "50000",        "-S",
                                             "777", "-l",   "8m", "--full-check", "5000"};
    {
        auto m = env_->mount();
        std::vector<std::string> args = common;
        args.insert(args.end(), {"--checkpoint", "25000"});
        ASSERT_EQ(run(args, m->path("fsx.dat"), /*progress_log=*/true), 0);
        ASSERT_TRUE(m->unmount().ok());
    }
    {
        // Продолжение на «другой машине»: пустой локальный каталог данных.
        env_->new_machine();
        auto m = env_->mount();
        std::vector<std::string> args = common;
        args.insert(args.end(), {"--resume"});
        EXPECT_EQ(run(args, m->path("fsx.dat"), /*progress_log=*/true), 0);
        EXPECT_TRUE(m->unmount().ok());
    }
}

}  // namespace
}  // namespace telega::itest
