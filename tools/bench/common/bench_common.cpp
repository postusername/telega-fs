#include "common/bench_common.h"

#include <sys/statfs.h>
#include <sys/utsname.h>
#include <unistd.h>

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <ctime>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <utility>

#include "common/files.h"
#include "crypto/keys.h"

namespace telega::bench {

uint64_t Rng::next() {
    state_ += 0x9E3779B97F4A7C15ULL;
    uint64_t z = state_;
    z = (z ^ (z >> 30U)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27U)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31U);
}

int64_t Rng::log_uniform(int64_t lo, int64_t hi) {
    const double u = static_cast<double>(next() >> 11U) / 9007199254740992.0;  // [0, 1)
    const double v =
        std::exp(std::log(static_cast<double>(lo)) +
                 u * (std::log(static_cast<double>(hi)) - std::log(static_cast<double>(lo))));
    return std::clamp(static_cast<int64_t>(v), lo, hi);
}

void Rng::fill(std::byte* out, size_t n) {
    for (size_t i = 0; i < n; i += 8) {
        const uint64_t x = next();
        for (size_t j = 0; j < 8 && i + j < n; ++j) {
            out[i + j] = static_cast<std::byte>((x >> (8U * j)) & 0xFFU);
        }
    }
}

std::vector<std::string> split_list(const std::string& text) {
    std::vector<std::string> out;
    std::istringstream in(text);
    std::string part;
    while (std::getline(in, part, ',')) {
        if (!part.empty()) {
            out.push_back(part);
        }
    }
    return out;
}

int64_t parse_bytes(const std::string& text) {
    size_t used = 0;
    const double value = std::stod(text, &used);
    const std::string unit = text.substr(used);
    double mul = 1;
    if (unit == "KiB") {
        mul = 1024.0;
    } else if (unit == "MiB") {
        mul = 1024.0 * 1024.0;
    } else if (unit == "GiB") {
        mul = 1024.0 * 1024.0 * 1024.0;
    } else if (!unit.empty()) {
        throw std::runtime_error("bad size: " + text);
    }
    return static_cast<int64_t>(value * mul);
}

Summary summarize(std::vector<double> values) {
    Summary s;
    s.n = values.size();
    if (values.empty()) {
        return s;
    }
    std::sort(values.begin(), values.end());
    s.min = values.front();
    s.max = values.back();
    const size_t mid = values.size() / 2;
    s.median = values.size() % 2 == 1 ? values[mid] : (values[mid - 1] + values[mid]) / 2.0;
    return s;
}

Csv::Csv(const fs::path& path, const std::vector<std::string>& header)
    : out_(path, std::ios::trunc), columns_(header.size()) {
    if (!out_) {
        throw std::runtime_error("cannot open " + path.string());
    }
    row(header);
}

void Csv::row(const std::vector<std::string>& cells) {
    if (cells.size() != columns_) {
        throw std::logic_error("csv: wrong number of columns");
    }
    for (size_t i = 0; i < cells.size(); ++i) {
        if (cells[i].find_first_of(",\"\n") != std::string::npos) {
            throw std::logic_error("csv: unsupported character in a cell");
        }
        out_ << (i == 0 ? "" : ",") << cells[i];
    }
    out_ << '\n';
}

std::string cell(int64_t v) { return std::to_string(v); }
std::string cell(uint64_t v) { return std::to_string(v); }
std::string cell(int v) { return std::to_string(v); }
std::string cell(double v) {
    char buf[64];
    std::snprintf(buf, sizeof buf, "%.6g", v);
    return buf;
}
std::string cell(const std::string& v) { return v; }
std::string cell(const char* v) { return v; }

uint64_t dir_bytes(const fs::path& dir) {
    uint64_t total = 0;
    std::error_code ec;
    for (fs::recursive_directory_iterator it(dir, ec), end; !ec && it != end; it.increment(ec)) {
        if (it->is_regular_file(ec)) {
            total += it->file_size(ec);
        }
    }
    return total;
}

ScratchFs::ScratchFs(fs::path root_dir, const meta::CommitConfig& commit,
                     const meta::InitOptions& init_options)
    : root(std::move(root_dir)) {
    std::error_code ec;
    fs::remove_all(root, ec);
    fs::create_directories(root);
    keyfile = root / "keyfile";
    crypto::init();
    write_file_atomic(keyfile, crypto::Key::random().span());
    store = std::make_unique<store::LocalDirStore>(root / "channel");
    paths = meta::FsPaths{root / "data"};
    meta::InitOptions init = init_options;
    init.key = key();
    meta::init_fs(*store, paths, init);
    meta::MountOptions mount;
    mount.key = key();
    mount.start_background = false;
    mount.commit = commit;
    mounted = meta::mount_fs(*store, paths, mount);
}

ScratchFs::~ScratchFs() {
    mounted.reset();
    store.reset();
}

uint64_t ScratchFs::message_size(int64_t msg_id) const {
    return fs::file_size(root / "channel" / "msgs" / std::to_string(msg_id));
}

meta::MountOptions fresh_mount_options(const meta::KeySource& key) {
    meta::MountOptions o;
    o.key = key;
    o.start_background = false;
    return o;
}

namespace {

std::string read_first_line_with(const char* path, const std::string& key) {
    std::ifstream in(path);
    for (std::string line; std::getline(in, line);) {
        if (line.starts_with(key)) {
            const auto colon = line.find(':');
            return colon == std::string::npos ? line : line.substr(colon + 2);
        }
    }
    return "?";
}

std::string run_capture(const std::string& cmd) {
    std::string out;
    if (FILE* p = ::popen(cmd.c_str(), "r")) {
        char buf[256];
        while (std::fgets(buf, sizeof buf, p) != nullptr) {
            out += buf;
        }
        ::pclose(p);
    }
    while (!out.empty() && (out.back() == '\n' || out.back() == ' ')) {
        out.pop_back();
    }
    return out;
}

}  // namespace

std::string environment_report(const fs::path& work_dir) {
    std::ostringstream o;
    const std::time_t now = std::time(nullptr);
    char stamp[64];
    std::strftime(stamp, sizeof stamp, "%Y-%m-%d %H:%M:%S %z", std::localtime(&now));
    o << "date: " << stamp << '\n';
    o << "cpu: " << read_first_line_with("/proc/cpuinfo", "model name") << '\n';
    o << "hardware_threads: " << std::thread::hardware_concurrency() << '\n';
    utsname u{};
    ::uname(&u);
    o << "kernel: " << u.sysname << ' ' << u.release << ' ' << u.machine << '\n';
    o << "mem_total_kib: " << read_first_line_with("/proc/meminfo", "MemTotal") << '\n';
    o << "work_dir_fs: "
      << run_capture("findmnt -no SOURCE,FSTYPE,OPTIONS -T '" + work_dir.string() + "'") << '\n';
#ifdef TGFS_BUILD_TYPE
    o << "build_type: " << TGFS_BUILD_TYPE << '\n';
#endif
    return o.str();
}

}  // namespace telega::bench
