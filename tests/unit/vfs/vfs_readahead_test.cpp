// Readahead: определение последовательности по дескриптору, окно
// readahead → readahead_max с удвоением, сброс непоследовательным чтением, нарезка по чанкам,
// расстояние для приоритета, отсутствие расшифровки и RAM-кэша. Плюс проводка через Vfs.

#include <fcntl.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

#include "crypto/chunk.h"
#include "crypto/keys.h"
#include "vfs/chunk_reader.h"
#include "vfs_test_util.h"

namespace telega::vfs::test {
namespace {

constexpr uint64_t kKiB = 1024;
constexpr uint64_t kMiB = 1024 * kKiB;

// Store, который записывает вызовы. Если задан inner, чтение и запись идут в него.
class RecordingStore final : public store::Store {
  public:
    struct Prefetch {
        int64_t msg_id = 0;
        uint64_t offset = 0;
        uint64_t len = 0;
        uint32_t distance = 0;
        bool operator==(const Prefetch&) const = default;
    };
    struct Read {
        int64_t msg_id = 0;
        uint64_t offset = 0;
        uint64_t len = 0;
    };

    explicit RecordingStore(store::Store* inner = nullptr) : inner_(inner) {}

    store::ChunkRef put_chunk(const std::filesystem::path& f) override {
        return inner_->put_chunk(f);
    }
    void read_range(const store::ChunkRef& r, uint64_t off, std::span<std::byte> out) override {
        {
            const std::lock_guard lock(mu_);
            reads_.push_back({r.msg_id, off, out.size()});
        }
        if (inner_ == nullptr) {
            throw store::StoreError(store::StoreErrorKind::permanent, "no data");
        }
        inner_->read_range(r, off, out);
    }
    void prefetch(const store::ChunkRef& r, uint64_t off, uint64_t len,
                  uint32_t distance) override {
        const std::lock_guard lock(mu_);
        prefetches_.push_back({r.msg_id, off, len, distance});
    }
    void delete_messages(std::span<const int64_t> ids) override { inner_->delete_messages(ids); }
    std::optional<store::HeadInfo> read_head() override { return inner_->read_head(); }
    Bytes read_index(int64_t id) override { return inner_->read_index(id); }
    int64_t put_index(const std::filesystem::path& f) override { return inner_->put_index(f); }
    void set_head(int64_t n, std::optional<int64_t> o) override { inner_->set_head(n, o); }

    std::vector<Prefetch> take_prefetches() {
        const std::lock_guard lock(mu_);
        return std::exchange(prefetches_, {});
    }
    std::vector<Read> reads() const {
        const std::lock_guard lock(mu_);
        return reads_;
    }
    void clear() {
        const std::lock_guard lock(mu_);
        reads_.clear();
        prefetches_.clear();
    }

  private:
    store::Store* inner_;
    mutable std::mutex mu_;
    std::vector<Prefetch> prefetches_;
    std::vector<Read> reads_;
};

// Зашифрованный диапазон открытого текста [plain_off, plain_off + len) чанка plain_size, как
// его просит readahead: заголовок входит, если диапазон начинается в первом мегабайте.
std::pair<uint64_t, uint64_t> enc_range(uint64_t plain_size, uint64_t plain_off, uint64_t len) {
    const crypto::SegmentSpan span = crypto::chunk_segments_for(plain_size, plain_off, len);
    const uint64_t a = span.enc_offset <= kMiB ? 0 : span.enc_offset;
    return {a, span.enc_offset + span.enc_len - a};
}

class ReadaheadTest : public ::testing::Test {
  protected:
    static constexpr uint64_t kChunks = 8;
    static constexpr uint64_t kFileSize = kChunks * kMiB;

    void SetUp() override {
        crypto::init();
        key = crypto::Key::random();
    }

    ChunkReader make_reader(uint64_t readahead, uint64_t readahead_max) {
        ReaderOptions o;
        o.readahead = readahead;
        o.readahead_max = readahead_max;
        return ChunkReader(store, key, FsUuid{}, o);
    }
    // Чанк на каждую позицию файла, кроме тех, что перечислены в holes.
    ReadaheadResolver resolver() const {
        return [this](int64_t idx) -> std::optional<ReadaheadTarget> {
            if (idx < 0 || static_cast<uint64_t>(idx) >= kChunks ||
                std::find(holes.begin(), holes.end(), idx) != holes.end()) {
                return std::nullopt;
            }
            meta::Chunk c;
            c.chunk_id = idx + 1;
            c.msg_id = 100 + idx;
            c.remote_file_id = "remote-" + std::to_string(idx);
            c.plain_size = static_cast<int64_t>(kMiB);
            c.enc_size = static_cast<int64_t>(crypto::chunk_encrypted_size(kMiB));
            return ReadaheadTarget{c, 0, kMiB};
        };
    }
    void note(ChunkReader& r, uint64_t fh, uint64_t off, uint64_t len = 128 * kKiB) {
        r.readahead(fh, off, len, kFileSize, kMiB, resolver());
    }
    // Ожидаемая подсказка: кусок [plain_off, plain_off + len) чанка idx.
    static RecordingStore::Prefetch want(int64_t idx, uint64_t plain_off, uint64_t len,
                                         uint32_t distance) {
        const auto [a, n] = enc_range(kMiB, plain_off, len);
        return {100 + idx, a, n, distance};
    }

    RecordingStore store;
    crypto::Key key;
    std::vector<int64_t> holes;
};

TEST_F(ReadaheadTest, FirstReadAsksForNothingThenTheWindowDoublesOnEveryHit) {
    ChunkReader r = make_reader(512 * kKiB, 2 * kMiB);
    note(r, 1, 0);
    EXPECT_TRUE(store.take_prefetches().empty());

    // Попадание 1: окно 512 КиБ с конца прочитанного (256 КиБ).
    note(r, 1, 128 * kKiB);
    EXPECT_EQ(store.take_prefetches(),
              (std::vector<RecordingStore::Prefetch>{want(0, 256 * kKiB, 512 * kKiB, 0)}));

    // Попадание 2: окно 1 МиБ; запрошено до 768 КиБ, добавляется остальное. Окно режется по
    // чанкам: хвост чанка 0 и начало чанка 1 (расстояние 1).
    note(r, 1, 256 * kKiB);
    EXPECT_EQ(store.take_prefetches(),
              (std::vector<RecordingStore::Prefetch>{want(0, 768 * kKiB, 256 * kKiB, 0),
                                                     want(1, 0, 384 * kKiB, 1)}));

    // Попадание 3: окно 2 МиБ (предел).
    note(r, 1, 384 * kKiB);
    EXPECT_EQ(store.take_prefetches(),
              (std::vector<RecordingStore::Prefetch>{want(1, 384 * kKiB, 640 * kKiB, 1),
                                                     want(2, 0, 512 * kKiB, 2)}));

    // Дальше окно не растёт, а запрошенное раньше ещё далеко: подсказок нет.
    note(r, 1, 512 * kKiB);
    note(r, 1, 640 * kKiB);
    EXPECT_TRUE(store.take_prefetches().empty());
}

TEST_F(ReadaheadTest, NonSequentialReadResetsTheWindow) {
    ChunkReader r = make_reader(512 * kKiB, 2 * kMiB);
    note(r, 1, 0);
    note(r, 1, 128 * kKiB);
    note(r, 1, 256 * kKiB);
    note(r, 1, 384 * kKiB);
    (void)store.take_prefetches();

    note(r, 1, 6 * kMiB);  // переход: подсказок нет, окно сброшено
    EXPECT_TRUE(store.take_prefetches().empty());
    note(r, 1, 6 * kMiB + 128 * kKiB);  // снова с начального окна 512 КиБ, от нового места
    EXPECT_EQ(store.take_prefetches(), (std::vector<RecordingStore::Prefetch>{want(
                                           6, 6 * kMiB + 256 * kKiB - 6 * kMiB, 512 * kKiB, 0)}));
}

TEST_F(ReadaheadTest, ReadsOutOfOrderWithinTheWindowStaySequential) {
    ChunkReader r = make_reader(512 * kKiB, 2 * kMiB);
    note(r, 1, 0);
    note(r, 1, 256 * kKiB);  // ядро читает параллельно: порядок не соблюдён
    EXPECT_FALSE(store.take_prefetches().empty());
    const auto before = store.reads().size();
    note(r, 1, 128 * kKiB);
    EXPECT_EQ(store.reads().size(), before);  // ничего не читалось
}

TEST_F(ReadaheadTest, WindowSpanningSeveralChunksIsSlicedPerChunkWithDistance) {
    ChunkReader r = make_reader(3 * kMiB, 3 * kMiB);
    note(r, 1, 0);
    note(r, 1, 128 * kKiB);  // конец чтения 256 КиБ, окно [256 КиБ, 3,25 МиБ)
    EXPECT_EQ(store.take_prefetches(), (std::vector<RecordingStore::Prefetch>{
                                           want(0, 256 * kKiB, 768 * kKiB, 0), want(1, 0, kMiB, 1),
                                           want(2, 0, kMiB, 2), want(3, 0, 256 * kKiB, 3)}));
}

TEST_F(ReadaheadTest, PositionsWithoutAChunkAndTheEndOfFileAreSkipped) {
    holes = {1};
    ChunkReader r = make_reader(3 * kMiB, 3 * kMiB);
    note(r, 1, 0);
    note(r, 1, 128 * kKiB);
    const auto got = store.take_prefetches();
    ASSERT_EQ(got.size(), 3U);  // позиции 0, 2, 3 (позиции 1 нет: нули или overlay)
    EXPECT_EQ(got[0].msg_id, 100);
    EXPECT_EQ(got[1].msg_id, 102);
    EXPECT_EQ(got[2].msg_id, 103);
    EXPECT_EQ(got[1].distance, 2U);

    // Окно не выходит за конец файла.
    holes.clear();
    ChunkReader tail = make_reader(3 * kMiB, 3 * kMiB);
    note(tail, 2, kFileSize - 512 * kKiB);
    note(tail, 2, kFileSize - 384 * kKiB, 128 * kKiB);
    const auto end = store.take_prefetches();
    ASSERT_EQ(end.size(), 1U);
    EXPECT_EQ(end[0], want(7, 640 * kKiB, 384 * kKiB, 0));  // до конца файла, и только
}

TEST_F(ReadaheadTest, ReadaheadNeverReadsDecryptsOrCaches) {
    ChunkReader r = make_reader(kMiB, 4 * kMiB);
    for (uint64_t off = 0; off < 3 * kMiB; off += 128 * kKiB) {
        note(r, 1, off);
    }
    EXPECT_FALSE(store.take_prefetches().empty());
    EXPECT_TRUE(store.reads().empty());  // ни одного read_range
    EXPECT_EQ(r.cache_bytes(), 0U);  // RAM-кэш открытого текста пуст: память от окна не зависит
}

TEST_F(ReadaheadTest, NoHandleOrDisabledMeansNoReadahead) {
    ChunkReader r = make_reader(512 * kKiB, 2 * kMiB);
    for (uint64_t off = 0; off < kMiB; off += 128 * kKiB) {
        note(r, 0, off);  // fh == 0: дескриптор неизвестен
    }
    EXPECT_TRUE(store.take_prefetches().empty());
    ChunkReader off_reader = make_reader(0, 0);
    for (uint64_t off = 0; off < kMiB; off += 128 * kKiB) {
        note(off_reader, 1, off);
    }
    EXPECT_TRUE(store.take_prefetches().empty());
}

TEST_F(ReadaheadTest, HandlesAreIndependentAndForgetHandleResets) {
    ChunkReader r = make_reader(512 * kKiB, 2 * kMiB);
    note(r, 1, 0);
    note(r, 2, 4 * kMiB);  // другой дескриптор: своя история
    EXPECT_TRUE(store.take_prefetches().empty());
    note(r, 1, 128 * kKiB);
    EXPECT_EQ(store.take_prefetches().size(), 1U);
    r.forget_handle(1);
    note(r, 1, 256 * kKiB);  // как первое чтение
    EXPECT_TRUE(store.take_prefetches().empty());
}

// ---- Проводка через Vfs ---------------------------------------------------------------------

class VfsReadahead : public ::testing::Test {
  protected:
    void SetUp() override {
        rec = std::make_unique<RecordingStore>(env.store.get());
        env.override_store = rec.get();
        VfsOptions o = fast_options();
        o.readahead = 512 * kKiB;
        o.readahead_max = 2 * kMiB;
        env.start(o);
    }
    // Vfs пользуется rec: останавливается раньше него.
    void TearDown() override { env.vfs.reset(); }
    // Файл из четырёх позиций, залитый в Store; Vfs перезапущен (пустой RAM-кэш).
    int64_t cold_file(const Bytes& data) {
        const int64_t f = env.mkfile("movie");
        env.write(f, 0, data);
        env.vfs->fsync(f);
        env.restart();
        rec->clear();
        return f;
    }

    VfsEnv env;
    std::unique_ptr<RecordingStore> rec;
};

TEST_F(VfsReadahead, SequentialReadsByHandleTriggerPrefetchAndReturnRightData) {
    const Bytes data = pattern(4 * kMiB, 3);
    const int64_t f = cold_file(data);
    const uint64_t fh = env.vfs->open(f, O_RDONLY);
    Bytes got;
    for (uint64_t off = 0; off < 2 * kMiB; off += 128 * kKiB) {
        const Bytes part = env.vfs->read(f, off, 128 * kKiB, fh);
        got.insert(got.end(), part.begin(), part.end());
        if (off == 0) {
            EXPECT_TRUE(rec->take_prefetches().empty());  // первое чтение
        }
    }
    EXPECT_EQ(got, Bytes(data.begin(), data.begin() + static_cast<std::ptrdiff_t>(2 * kMiB)));
    const auto pf = rec->take_prefetches();
    ASSERT_FALSE(pf.empty());
    for (const auto& p : pf) {
        EXPECT_GT(p.len, 0U);
    }
    env.vfs->release(fh);
}

TEST_F(VfsReadahead, ReadWithoutHandleAndOverlayChunksDoNotReadAhead) {
    const Bytes data = pattern(4 * kMiB, 4);
    const int64_t f = cold_file(data);
    for (uint64_t off = 0; off < 2 * kMiB; off += 128 * kKiB) {
        (void)env.vfs->read(f, off, 128 * kKiB);  // fh == 0
    }
    EXPECT_TRUE(rec->take_prefetches().empty());

    // Файл, который ещё не залит (позиции в overlay): читать вперёд нечего.
    const int64_t dirty = env.mkfile("dirty");
    env.write(dirty, 0, pattern(3 * kMiB, 5));
    const uint64_t fh = env.vfs->open(dirty, O_RDONLY);
    for (uint64_t off = 0; off < 2 * kMiB; off += 128 * kKiB) {
        (void)env.vfs->read(dirty, off, 128 * kKiB, fh);
    }
    EXPECT_TRUE(rec->take_prefetches().empty());
    env.vfs->release(fh);
}

TEST_F(VfsReadahead, ColdReadAtTheStartOfAChunkFetchesHeaderAndDataInOneCall) {
    const Bytes data = pattern(2 * kMiB, 6);
    const int64_t f = cold_file(data);
    const Bytes part = env.vfs->read(f, 100 * kKiB, 64 * kKiB);
    EXPECT_EQ(part, Bytes(data.begin() + static_cast<std::ptrdiff_t>(100 * kKiB),
                          data.begin() + static_cast<std::ptrdiff_t>(164 * kKiB)));
    const auto reads = rec->reads();
    ASSERT_EQ(reads.size(), 1U);  // заголовок и сегменты одним вызовом, а не двумя
    EXPECT_EQ(reads[0].offset, 0U);
    EXPECT_GE(reads[0].len, 164 * kKiB + crypto::kChunkHeaderSize);
}

}  // namespace
}  // namespace telega::vfs::test
