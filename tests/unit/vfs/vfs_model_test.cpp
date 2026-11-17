// Vfs против модели в памяти: случайные операции (фиксированные seed) над несколькими
// файлами, фоновые заливки по таймеру, периодический «перезапуск» (Vfs и базы создаются
// заново на тех же файлах) и принудительная заливка. Содержимое всех файлов сверяется
// с моделью по ходу и в конце, в том числе по одному индексу без overlay.

#include <fcntl.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <map>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include "vfs_test_util.h"

namespace telega::vfs::test {
namespace {

class VfsModel : public ::testing::TestWithParam<uint32_t> {
  protected:
    struct File {
        int64_t ino = 0;
        Bytes data;
        std::optional<uint64_t> fh;  // открыт ли сейчас
    };

    void SetUp() override {
        VfsOptions o = fast_options();
        o.seal_delay = milliseconds(3);  // таймер заливает в фоне, вперемешку с операциями
        o.plain_cache = 2U << 20U;  // маленький кэш: чаще ходим в Store
        options = o;
        env.start(o);
        rng.seed(GetParam());
    }

    uint64_t rnd(uint64_t n) { return std::uniform_int_distribution<uint64_t>(0, n - 1)(rng); }

    static std::string name_of(size_t i) { return "f" + std::to_string(i); }

    // Смещение, тяготеющее к границам позиций.
    uint64_t pick_offset(uint64_t max) {
        if (rnd(3) == 0) {
            const uint64_t boundary = rnd(4) * kChunk;
            const uint64_t jitter = rnd(64);
            if (rnd(2) == 0) {
                return boundary + jitter;
            }
            return boundary > jitter ? boundary - jitter : 0;
        }
        return rnd(max);
    }

    void check_file(const std::string& name, const File& f) {
        const Attr a = env.vfs->getattr(f.ino);
        ASSERT_EQ(a.size, static_cast<int64_t>(f.data.size())) << name << " " << log.str();
        // Случайный диапазон и файл целиком.
        if (!f.data.empty()) {
            const uint64_t off = rnd(f.data.size());
            const auto len = static_cast<size_t>(rnd(300'000) + 1);
            const Bytes part = env.vfs->read(f.ino, off, len);
            const size_t expect = std::min<size_t>(len, f.data.size() - off);
            ASSERT_EQ(part.size(), expect) << name;
            ASSERT_TRUE(std::equal(part.begin(), part.end(),
                                   f.data.begin() + static_cast<std::ptrdiff_t>(off)))
                << name << " at " << off << "\n"
                << log.str();
        }
    }

    void check_all() {
        for (const auto& [name, f] : files) {
            const Bytes all = env.read_all(f.ino);
            ASSERT_TRUE(all == f.data) << name << " differs\n" << log.str();
        }
    }

    void do_write(File& f) {
        const uint64_t off = pick_offset(3 * kChunk + kChunk / 2);
        const auto len = static_cast<size_t>(rnd(4) == 0 ? rnd(200'000) + 1 : rnd(5000) + 1);
        const Bytes data = pattern(len, rng());
        log << "write " << f.ino << " " << off << " " << len << "\n";
        env.write(f.ino, off, data);
        if (f.data.size() < off + len) {
            f.data.resize(off + len, std::byte{0});
        }
        std::copy(data.begin(), data.end(), f.data.begin() + static_cast<std::ptrdiff_t>(off));
    }

    void do_truncate(File& f) {
        uint64_t size = 0;
        switch (rnd(4)) {
            case 0:
                size = 0;
                break;
            case 1:
                size = rnd(5000);  // кандидат в инлайн
                break;
            default:
                size = pick_offset(4 * kChunk);
                break;
        }
        log << "truncate " << f.ino << " " << size << "\n";
        SetAttr s;
        s.size = static_cast<int64_t>(size);
        env.vfs->setattr(f.ino, s);
        f.data.resize(size, std::byte{0});
    }

    void close_all() {
        for (auto& [name, f] : files) {
            if (f.fh) {
                env.vfs->release(*f.fh);
                f.fh.reset();
            }
        }
    }

    void run(size_t ops) {
        for (size_t i = 0; i < 4; ++i) {
            const Created c = env.vfs->create(kRoot, bytes_of(name_of(i)), 0644, O_RDWR, 0, 0);
            files[name_of(i)] = File{c.attr.ino, {}, c.fh};
        }
        size_t next_name = 4;
        for (size_t op = 0; op < ops; ++op) {
            auto it = files.begin();
            std::advance(it, static_cast<std::ptrdiff_t>(rnd(files.size())));
            File& f = it->second;
            const uint64_t r = rnd(100);
            if (r < 45) {
                do_write(f);
            } else if (r < 55) {
                do_truncate(f);
            } else if (r < 68) {
                check_file(it->first, f);
            } else if (r < 72) {
                log << "fsync " << f.ino << "\n";
                env.vfs->fsync(f.ino);
            } else if (r < 80) {
                if (f.fh) {
                    log << "release " << f.ino << "\n";
                    env.vfs->release(*f.fh);
                    f.fh.reset();
                } else {
                    log << "open " << f.ino << "\n";
                    f.fh = env.vfs->open(f.ino, O_RDWR);
                }
            } else if (r < 84) {
                // Удалить (может быть открыт — тогда отсоединение) и создать заново.
                log << "unlink " << it->first << " (" << f.ino << ")\n";
                const std::string name = it->first;
                if (f.fh) {
                    // Удалённый открытый файл читается до закрытия.
                    env.vfs->unlink(kRoot, bytes_of(name));
                    ASSERT_EQ(env.vfs->read(f.ino, 0, f.data.size() + 1), f.data);
                    env.vfs->release(*f.fh);
                } else {
                    env.vfs->unlink(kRoot, bytes_of(name));
                }
                env.vfs->forget(f.ino, 1);
                files.erase(it);
                const Created c = env.vfs->create(kRoot, bytes_of(name), 0644, O_RDWR, 0, 0);
                files[name] = File{c.attr.ino, {}, c.fh};
            } else if (r < 89) {
                // rename поверх другого файла или на новое имя.
                const std::string from = it->first;
                std::string to;
                if (rnd(2) == 0 && files.size() > 1) {
                    auto jt = files.begin();
                    std::advance(jt, static_cast<std::ptrdiff_t>(rnd(files.size())));
                    to = jt->first;
                } else {
                    to = "f" + std::to_string(next_name++);
                }
                if (to == from) {
                    continue;
                }
                log << "rename " << from << " " << to << "\n";
                env.vfs->rename(kRoot, bytes_of(from), kRoot, bytes_of(to), 0);
                File moved = f;
                files.erase(from);
                auto victim = files.find(to);
                if (victim != files.end()) {
                    if (const auto vfh = victim->second.fh) {
                        env.vfs->release(*vfh);
                    }
                    env.vfs->forget(victim->second.ino, 1);
                    files.erase(victim);
                }
                files[to] = std::move(moved);
                if (files.size() < 3) {
                    const std::string n = "f" + std::to_string(next_name++);
                    const Created c = env.vfs->create(kRoot, bytes_of(n), 0644, O_RDWR, 0, 0);
                    files[n] = File{c.attr.ino, {}, c.fh};
                }
            } else if (r < 93) {
                log << "flush_all\n";
                env.vfs->flush_all();
                ASSERT_EQ(env.vfs->status().dirty_chunks, 0U);
            } else if (r < 97) {
                log << "restart\n";
                for (auto& [name, file] : files) {
                    file.fh.reset();  // дескрипторы не переживают перезапуск
                }
                env.restart();
                check_all();
            } else {
                log << "check_all\n";
                check_all();
            }
            if (HasFatalFailure()) {
                return;
            }
        }
        // Финал: залить всё, сверить, затем сверить по одному индексу (без overlay).
        close_all();
        check_all();
        env.vfs->shutdown();
        env.restart();
        ASSERT_TRUE(env.state->list_dirty_chunks().empty());
        ASSERT_TRUE(env.state->list_dirty_attrs().empty());
        ASSERT_EQ(env.staging_files(), 0U);
        check_all();
        ASSERT_TRUE(env.accounting_ok());
        // Каждый файл в индексе: размер и класс хранения согласованы.
        for (const auto& [name, f] : files) {
            const meta::Inode inode = env.must_inode(f.ino);
            ASSERT_EQ(inode.size, static_cast<int64_t>(f.data.size()));
            const auto exts = env.extents(f.ino);
            if (inode.inline_len) {
                ASSERT_TRUE(exts.empty());
            }
            for (const meta::Extent& e : exts) {
                ASSERT_LE(e.idx * kChunk + e.len, inode.size) << name;
            }
        }
    }

    VfsEnv env;
    VfsOptions options;
    std::mt19937_64 rng;
    std::map<std::string, File> files;
    std::ostringstream log;
};

TEST_P(VfsModel, RandomOperationsMatchModel) { run(250); }

INSTANTIATE_TEST_SUITE_P(Seeds, VfsModel, ::testing::Values(1U, 2U, 3U, 4U, 5U));

}  // namespace
}  // namespace telega::vfs::test
