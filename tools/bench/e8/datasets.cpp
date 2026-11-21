// E8: наборы для измерения индекса. Размеры C2 берутся тем же загрузчиком, что у tgfs-sim
// (sim::load_hist: порядок перемешан по seed), поэтому симуляция и индекс видят одни и те же файлы.

#include <algorithm>
#include <fstream>
#include <stdexcept>

#include "corpora/c4_gen.h"
#include "e8/e8.h"
#include "sim/sim.h"

namespace telega::bench::e8 {

namespace {

constexpr char kAlphabet[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
constexpr int64_t kFanout = 50;
constexpr size_t kTextFileMax = size_t{16} * 1024U;  // в «текст» идут только мелкие файлы C1

Bytes random_bytes(Rng& rng, int64_t size) {
    Bytes b(static_cast<size_t>(size));
    rng.fill(b.data(), b.size());
    return b;
}

Bytes base64_bytes(Rng& rng, int64_t size) {
    Bytes b(static_cast<size_t>(size));
    for (auto& x : b) {
        x = static_cast<std::byte>(kAlphabet[rng.below(64)]);
    }
    return b;
}

Bytes slice(const Bytes& corpus, Rng& rng, int64_t size) {
    const auto n = static_cast<uint64_t>(size);
    if (corpus.size() < n) {
        throw std::runtime_error("text corpus is shorter than the requested file");
    }
    const uint64_t at = rng.below(corpus.size() - n + 1);
    const auto first = corpus.begin() + static_cast<std::ptrdiff_t>(at);
    return {first, first + static_cast<std::ptrdiff_t>(n)};
}

Bytes read_whole(const fs::path& p) {
    std::ifstream in(p, std::ios::binary);
    Bytes data(static_cast<size_t>(fs::file_size(p)));
    in.read(reinterpret_cast<char*>(data.data()), static_cast<std::streamsize>(data.size()));
    if (!in && !data.empty()) {
        throw std::runtime_error("cannot read " + p.string());
    }
    return data;
}

// Файлы C1 по возрастанию относительного пути (порядок создания для набора c1).
void load_c1(Dataset& d, const fs::path& dir) {
    if (dir.empty() || !fs::is_directory(dir)) {
        throw std::runtime_error("--c1-dir is required for c1 and text content");
    }
    std::vector<std::pair<std::string, fs::path>> files;
    for (const auto& de : fs::recursive_directory_iterator(dir)) {
        if (de.is_regular_file()) {
            files.emplace_back(fs::relative(de.path(), dir).generic_string(), de.path());
        }
    }
    std::sort(files.begin(), files.end());
    for (const auto& [rel, path] : files) {
        Bytes data = read_whole(path);
        d.sizes.push_back(static_cast<int64_t>(data.size()));
        if (data.size() <= kTextFileMax) {
            d.corpus->insert(d.corpus->end(), data.begin(), data.end());
        }
        d.corpus_files->push_back(std::move(data));
    }
}

}  // namespace

const char* content_name(Content c) {
    switch (c) {
        case Content::real:
            return "real";
        case Content::random:
            return "random";
        case Content::base64:
            return "base64";
        case Content::text:
            return "text";
    }
    return "?";
}

std::vector<int64_t> c4_e8_sizes(int64_t files, int64_t pack_threshold, uint64_t seed) {
    Rng rng(seed ^ 0xC4C4C4C4ULL);
    std::vector<int64_t> sizes;
    sizes.reserve(static_cast<size_t>(files));
    for (int64_t i = 0; i < files; ++i) {
        if (rng.below(1000) < 50) {
            sizes.push_back(rng.log_uniform(pack_threshold + 1, int64_t{128} << 20U));
        } else {
            sizes.push_back(rng.log_uniform(16, pack_threshold));
        }
    }
    return sizes;
}

Dataset make_dataset(const std::string& name, const Options& o) {
    Dataset d;
    d.name = name;
    d.corpus = std::make_shared<Bytes>();
    d.corpus_files = std::make_shared<std::vector<Bytes>>();
    const bool want_text = o.content == "text";
    if (name == "c1") {
        load_c1(d, o.c1_dir);
        d.content = want_text ? Content::text : Content::real;
    } else if (name == "c2") {
        for (const sim::Op& op : sim::load_hist(o.c2_sizes.string(), o.seed)) {
            d.sizes.push_back(op.size);
        }
        d.content = want_text ? Content::text : Content::random;
    } else if (name.starts_with("c4-")) {
        const int64_t inodes = std::stoll(name.substr(3));
        const int64_t files = plan_c4_layout(inodes, kFanout).files;
        d.sizes = c4_e8_sizes(files, o.pack_threshold, o.seed);
        d.content = want_text ? Content::text : Content::base64;
    } else {
        throw std::invalid_argument("unknown dataset " + name);
    }
    if (want_text && name != "c1") {
        // Текст для C2 и C4 — куски исходников C1.
        Dataset c1;
        c1.corpus = d.corpus;
        c1.corpus_files = d.corpus_files;
        load_c1(c1, o.c1_dir);
    }
    const auto corpus = d.corpus;
    const auto files = d.corpus_files;
    switch (d.content) {
        case Content::real:
            d.initial = [files](Rng&, int64_t idx, int64_t) {
                return (*files)[static_cast<size_t>(idx)];
            };
            d.fresh = [corpus](Rng& rng, int64_t size) { return slice(*corpus, rng, size); };
            break;
        case Content::text:
            d.initial = [corpus](Rng& rng, int64_t, int64_t size) {
                return slice(*corpus, rng, size);
            };
            d.fresh = [corpus](Rng& rng, int64_t size) { return slice(*corpus, rng, size); };
            break;
        case Content::random:
            d.initial = [](Rng& rng, int64_t, int64_t size) { return random_bytes(rng, size); };
            d.fresh = random_bytes;
            break;
        case Content::base64:
            d.initial = [](Rng& rng, int64_t, int64_t size) { return base64_bytes(rng, size); };
            d.fresh = base64_bytes;
            break;
    }
    return d;
}

}  // namespace telega::bench::e8
