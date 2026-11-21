#include <fstream>
#include <sstream>
#include <stdexcept>

#include "common/bench_common.h"
#include "sim/sim.h"

namespace telega::bench::sim {

namespace {

std::ifstream open_csv(const std::string& path, const std::string& header) {
    std::ifstream in(path);
    if (!in) {
        throw std::runtime_error("cannot open " + path);
    }
    std::string line;
    std::getline(in, line);
    if (line != header) {
        throw std::runtime_error(path + ": expected header " + header);
    }
    return in;
}

}  // namespace

std::vector<Op> load_events(const std::string& path) {
    std::ifstream in = open_csv(path, "step,op,id,size");
    std::vector<Op> ops;
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) {
            continue;
        }
        std::istringstream row(line);
        std::string step;
        std::string kind;
        std::string id;
        std::string size;
        std::getline(row, step, ',');
        std::getline(row, kind, ',');
        std::getline(row, id, ',');
        std::getline(row, size, ',');
        if (kind != "w" && kind != "d") {
            // NOLINTNEXTLINE(performance-inefficient-string-concatenation)
            throw std::runtime_error(path + ": bad op " + kind);
        }
        ops.push_back(Op{static_cast<uint32_t>(std::stoul(step)), kind[0],
                         static_cast<uint32_t>(std::stoul(id)), std::stoll(size)});
    }
    return ops;
}

std::vector<Op> load_hist(const std::string& path, uint64_t seed) {
    std::ifstream in = open_csv(path, "size,count");
    std::vector<Op> ops;
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) {
            continue;
        }
        const size_t comma = line.find(',');
        if (comma == std::string::npos) {
            throw std::runtime_error(path + ": bad line");
        }
        const int64_t size = std::stoll(line.substr(0, comma));
        const int64_t count = std::stoll(line.substr(comma + 1));
        for (int64_t i = 0; i < count; ++i) {
            ops.push_back(Op{0, 'w', 0, size});
        }
    }
    // Порядок обхода каталогов неизвестен (и в CSV не сохранён): перемешать по seed.
    Rng rng(seed ^ 0xC2C2C2C2ULL);
    for (size_t i = ops.size(); i > 1; --i) {
        std::swap(ops[i - 1], ops[rng.below(i)]);
    }
    uint32_t id = 0;
    for (Op& op : ops) {
        op.id = ++id;
    }
    return ops;
}

}  // namespace telega::bench::sim
