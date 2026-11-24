#include "vfs/export_dirty.h"

#include <fcntl.h>
#include <spdlog/spdlog.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string_view>
#include <system_error>
#include <vector>

#include "common/files.h"
#include "meta/index_ops.h"
#include "meta/state_db.h"
#include "vfs/chunk_reader.h"
#include "vfs/overlay.h"

namespace telega::vfs {

namespace fs = std::filesystem;

namespace {

// Верхняя граница размера файла при выгрузке: защита от испорченного dirty_attrs.
constexpr int64_t kMaxExportSize = int64_t{1} << 50;
// Выгрузка идёт кусками, память не зависит от chunk_size.
constexpr uint64_t kPiece = uint64_t{4} << 20U;
// Запас на скачки часов при сравнении времени inode с committed_take_ns (SPEC 12.3).
constexpr int64_t kClockSlackNs = int64_t{5} * 60 * 1'000'000'000;

std::string printable(const std::string& name) {
    std::string out;
    for (const char ch : name) {
        const auto c = static_cast<unsigned char>(ch);
        if (c >= 0x20 && c < 0x7F && c != '\\') {
            out += ch;
        } else {
            constexpr std::string_view kHex = "0123456789abcdef";
            out += "\\x";
            out += kHex[c >> 4U];
            out += kHex[c & 0xFU];
        }
    }
    return out;
}

bool is_zero(ByteSpan data) {
    return std::all_of(data.begin(), data.end(), [](std::byte b) { return b == std::byte{0}; });
}

// Компоненты пути inode от корня. nullopt и причина — если пути нет (файл удалён, но открыт, имя
// не годится для файловой системы).
std::optional<std::vector<std::string>> inode_components(meta::Db& db, int64_t ino,
                                                         std::string& why) {
    std::vector<std::string> parts;
    int64_t cur = ino;
    for (int depth = 0; depth < 4096; ++depth) {
        if (cur == meta::kRootIno) {
            std::reverse(parts.begin(), parts.end());
            return parts;
        }
        const auto node = meta::get_inode(db, cur);
        if (!node) {
            why = "the path leads to a missing inode " + std::to_string(cur);
            return std::nullopt;
        }
        if (!node->parent || !node->name) {
            why = "the file was unlinked (it has no name in the index)";
            return std::nullopt;
        }
        // Предки обязаны быть каталогами: иначе при испорченном индексе выгрузка пошла бы через
        // уже созданный в DIR симлинк и записала бы файл за его пределами.
        if (cur != ino && node->kind != meta::InodeKind::dir) {
            why = "an ancestor of the path is not a directory (the index is damaged)";
            return std::nullopt;
        }
        std::string name(reinterpret_cast<const char*>(node->name->data()), node->name->size());
        if (name.empty() || name == "." || name == ".." || name.find('/') != std::string::npos ||
            name.find('\0') != std::string::npos) {
            why = "the name " + printable(name) + " is not valid";
            return std::nullopt;
        }
        parts.push_back(std::move(name));
        cur = *node->parent;
    }
    why = "the directory tree is too deep or has a loop";
    return std::nullopt;
}

std::string join_for_report(const std::vector<std::string>& parts) {
    std::string out;
    for (const std::string& p : parts) {
        out += '/';
        out += printable(p);
    }
    return out.empty() ? "/" : out;
}

// mkdir -p с правами 0700 для создаваемых каталогов.
void make_private_dirs(const fs::path& dir) {
    std::vector<fs::path> missing;  // от самого вложенного к корню
    std::error_code ec;
    for (fs::path cur = dir; !cur.empty() && !fs::is_directory(cur, ec); cur = cur.parent_path()) {
        missing.push_back(cur);
        if (cur.parent_path() == cur) {
            break;
        }
    }
    std::reverse(missing.begin(), missing.end());
    for (const fs::path& d : missing) {
        if (::mkdir(d.c_str(), 0700) != 0 && errno != EEXIST) {
            throw SysError(errno, "mkdir " + d.string());
        }
    }
}

fs::path staging_file(const meta::FsPaths& paths, const meta::DirtyChunk& row) {
    fs::path p{row.path};
    if (p.is_relative()) {
        p = paths.staging() / p;
    }
    return p;
}

// Читатели залитых чанков: обычный и «быстрый» после первой неудачи.
class RemoteReader {
  public:
    RemoteReader(store::Store* store, const crypto::SubKeys& keys, const FsUuid& fs_uuid,
                 const ExportOptions& options) {
        if (store == nullptr) {
            return;
        }
        ReaderOptions normal;
        normal.cache_bytes = uint64_t{8} << 20U;
        normal.concurrency = 1;
        normal.read_timeout = options.read_timeout;
        normal.retry_initial = std::min(normal.retry_initial, options.read_timeout);
        normal.readahead = 0;
        ReaderOptions fast = normal;
        fast.read_timeout = options.fast_timeout;
        fast.retry_initial = std::chrono::milliseconds{200};
        normal_ = std::make_unique<ChunkReader>(*store, keys.data, fs_uuid, normal);
        fast_ = std::make_unique<ChunkReader>(*store, keys.data, fs_uuid, fast);
    }

    // false — прочитать не удалось (dst содержимое неопределённо).
    bool read(const meta::Chunk& chunk, uint64_t off, MutableByteSpan dst, std::string& why) {
        if (!normal_) {
            why = "the channel store is not available";
            return false;
        }
        try {
            (degraded_ ? *fast_ : *normal_).read(chunk, off, dst);
            degraded_ = false;
            return true;
        } catch (const std::exception& e) {
            degraded_ = true;
            why = e.what();
            return false;
        }
    }

  private:
    std::unique_ptr<ChunkReader> normal_;
    std::unique_ptr<ChunkReader> fast_;
    bool degraded_ = false;
};

// Симлинк, изменённый после последнего коммита: создаётся тем же именем и с той же целью.
void export_symlink(meta::Db& db, const fs::path& out_dir, int64_t ino, const meta::Inode& node,
                    ExportReport& report) {
    std::string why;
    const auto parts = inode_components(db, ino, why);
    if (!parts || !node.target || parts->empty()) {
        report.warnings.push_back("inode " + std::to_string(ino) + ": symlink not exported" +
                                  (parts ? "" : ", " + why));
        return;
    }
    fs::path target = out_dir;
    for (size_t i = 0; i + 1 < parts->size(); ++i) {
        target /= (*parts)[i];
    }
    make_private_dirs(target);
    target /= parts->back();
    const std::string link(reinterpret_cast<const char*>(node.target->data()), node.target->size());
    if (::symlink(link.c_str(), target.c_str()) != 0) {
        throw SysError(errno, "symlink " + target.string());
    }
    ExportedFile file;
    file.path = join_for_report(*parts);
    file.size = node.target->size();
    file.symlink = true;
    report.files.push_back(std::move(file));
}

}  // namespace

size_t ExportReport::incomplete_count() const {
    return static_cast<size_t>(std::count_if(files.begin(), files.end(),
                                             [](const ExportedFile& f) { return !f.complete(); }));
}

ExportReport export_dirty(const meta::FsPaths& paths, store::Store* store,
                          const crypto::SubKeys& keys, const fs::path& out_dir,
                          const ExportOptions& options) {
    if (!fs::exists(paths.index_db())) {
        throw std::runtime_error(
            "there is no local index: the overlay cannot be exported without it");
    }
    if (!fs::exists(paths.state_db())) {
        throw std::runtime_error("there is no local state (state.sqlite): nothing to export");
    }
    {
        std::error_code ec;
        if (fs::exists(out_dir, ec)) {
            if (!fs::is_directory(out_dir, ec) || !fs::is_empty(out_dir, ec)) {
                throw std::runtime_error(out_dir.string() +
                                         " exists and is not an empty directory");
            }
        } else {
            make_private_dirs(out_dir);
        }
    }

    meta::OpenOptions ro;
    ro.read_only = true;
    meta::Db db = meta::Db::open(paths.index_db(), ro);
    const meta::IndexParams params = meta::read_params(db);
    meta::StateStore state{paths.state_db(), /*migrate=*/false};

    std::map<int64_t, std::vector<meta::DirtyChunk>> chunks_of;
    for (meta::DirtyChunk& row : state.list_dirty_chunks()) {
        chunks_of[row.ino].push_back(std::move(row));
    }
    std::map<int64_t, meta::DirtyAttrs> attrs_of;
    for (const meta::DirtyAttrs& a : state.list_dirty_attrs()) {
        attrs_of[a.ino] = a;
    }
    std::set<int64_t> overlay_inos;
    for (const auto& [ino, rows] : chunks_of) {
        overlay_inos.insert(ino);
    }
    for (const auto& [ino, a] : attrs_of) {
        overlay_inos.insert(ino);
    }
    std::set<int64_t> inos = overlay_inos;

    RemoteReader remote{store, keys, params.fs_uuid, options};
    const auto chunk_size = static_cast<uint64_t>(params.chunk_size);
    ExportReport report;

    // Залитое, но не закоммиченное: overlay у него уже очищен (SPEC 8.1, шаг 5), а в канале его
    // нет. По committed_take_ns находятся inode, изменённые после последнего забора патча.
    std::set<int64_t> changed_dirs;
    if (const auto take = state.get_int(meta::state_key::committed_take_ns)) {
        const int64_t threshold = *take - kClockSlackNs;
        meta::Stmt st{db,
                      "SELECT ino, kind FROM inodes WHERE ino != 1 AND parent IS NOT NULL AND "
                      "(ctime_ns >= ?1 OR mtime_ns >= ?1)"};
        st.bind(1, threshold);
        while (st.step()) {
            if (st.column_int64(1) == static_cast<int64_t>(meta::InodeKind::dir)) {
                changed_dirs.insert(st.column_int64(0));
            } else {
                inos.insert(st.column_int64(0));
            }
        }
    } else {
        // Состояние до M4: времени последнего коммита нет. Инлайн-данные живут только в индексе
        // и могли быть запечатаны после коммита, поэтому выгружаются все.
        report.warnings.emplace_back(
            "committed_take_ns is missing: only the overlay and all inline files are exported");
        meta::Stmt st{
            db, "SELECT ino FROM inodes WHERE inline_data IS NOT NULL AND parent IS NOT NULL"};
        while (st.step()) {
            inos.insert(st.column_int64(0));
        }
    }

    for (const int64_t ino : inos) {
        const std::string label = "inode " + std::to_string(ino);
        // Ошибка одного файла (EFBIG, ENOSPC, права) не прерывает выгрузку остальных.
        try {
            const auto node = meta::get_inode(db, ino);
            if (!node) {
                report.warnings.push_back(label +
                                          ": overlay rows of an inode that is not in the index");
                continue;
            }
            if (node->kind == meta::InodeKind::symlink) {
                export_symlink(db, out_dir, ino, *node, report);
                continue;
            }
            if (node->kind != meta::InodeKind::file) {
                report.warnings.push_back(label + ": overlay rows of something that is not a file");
                continue;
            }
            std::string why;
            const auto parts = inode_components(db, ino, why);
            if (!parts) {
                report.warnings.push_back(label + ": not exported, ");
                report.warnings.back() += why;
                continue;
            }
            const auto attr_it = attrs_of.find(ino);
            const int64_t size = attr_it != attrs_of.end() ? attr_it->second.size : node->size;
            const int64_t mtime_ns =
                attr_it != attrs_of.end() ? attr_it->second.mtime_ns : node->mtime_ns;
            if (size < 0 || size > kMaxExportSize) {
                report.warnings.push_back(label + ": not exported, the size " +
                                          std::to_string(size) + " is not valid");
                continue;
            }
            std::optional<Bytes> inline_data;
            if (node->inline_len) {
                inline_data = meta::get_inline(db, ino);
            }

            fs::path target = out_dir;
            for (size_t i = 0; i + 1 < parts->size(); ++i) {
                target /= (*parts)[i];
            }
            make_private_dirs(target);
            target /= parts->back();
            const Fd out = Fd::open(target, O_WRONLY | O_CREAT | O_EXCL, 0600);
            if (::ftruncate(out.get(), size) != 0) {
                throw SysError(errno, "ftruncate " + target.string());
            }

            ExportedFile file;
            file.path = join_for_report(*parts);
            file.size = static_cast<uint64_t>(size);
            std::map<int64_t, const meta::DirtyChunk*> dirty;
            if (const auto it = chunks_of.find(ino); it != chunks_of.end()) {
                for (const meta::DirtyChunk& row : it->second) {
                    dirty[row.idx] = &row;
                }
            }

            const uint64_t positions = (file.size + chunk_size - 1) / chunk_size;
            Bytes piece;
            for (uint64_t idx = 0; idx < positions; ++idx) {
                const uint64_t base = idx * chunk_size;
                const uint64_t n = std::min<uint64_t>(chunk_size, file.size - base);
                const auto write_piece = [&](uint64_t off, ByteSpan data) {
                    if (!is_zero(data)) {  // нули уже есть: файл разреженный
                        pwrite_full(out.get(), data, base + off);
                        report.bytes_written += data.size();
                    }
                };

                // 1. Позиция в overlay: staging-файл, дальше его длины нули (SPEC 8.2).
                if (const auto it = dirty.find(static_cast<int64_t>(idx)); it != dirty.end()) {
                    const fs::path sp = staging_file(paths, *it->second);
                    std::error_code ec;
                    if (fs::exists(sp, ec)) {
                        const Fd sfd = Fd::open(sp, O_RDONLY);
                        for (uint64_t off = 0; off < n; off += kPiece) {
                            piece.assign(static_cast<size_t>(std::min(kPiece, n - off)),
                                         std::byte{0});
                            pread_full(sfd.get(), piece, off);
                            write_piece(off, piece);
                        }
                        ++file.overlay_positions;
                        continue;
                    }
                    // Строка пережила свой файл (SPEC 8.1): при старте Vfs позиция снова читается
                    // из индекса, откат не глубже последнего коммита.
                    report.warnings.push_back(file.path + ": staging file of position " +
                                              std::to_string(idx) +
                                              " is missing, the committed data is used");
                }
                // 2. Позиция 0 инлайн-файла: данные в индексе, дальше их длины нули.
                if (idx == 0 && inline_data) {
                    const size_t k = std::min<size_t>(static_cast<size_t>(n), inline_data->size());
                    write_piece(0, ByteSpan{*inline_data}.first(k));
                    continue;
                }
                // 3. Залитый чанк по экстенту; без экстента позиция — дыра.
                const auto ext = meta::get_extent(db, ino, static_cast<int64_t>(idx));
                if (!ext || ext->len <= 0) {
                    continue;
                }
                const uint64_t valid = std::min<uint64_t>(n, static_cast<uint64_t>(ext->len));
                const auto chunk = meta::get_chunk(db, ext->chunk_id);
                if (!chunk) {
                    file.missing.push_back(static_cast<int64_t>(idx));
                    report.warnings.push_back(file.path + ": position " + std::to_string(idx) +
                                              " refers to a missing chunk row " +
                                              std::to_string(ext->chunk_id));
                    continue;
                }
                for (uint64_t off = 0; off < valid; off += kPiece) {
                    piece.assign(static_cast<size_t>(std::min(kPiece, valid - off)), std::byte{0});
                    std::string error;
                    if (!remote.read(*chunk, static_cast<uint64_t>(ext->chunk_off) + off, piece,
                                     error)) {
                        file.missing.push_back(static_cast<int64_t>(idx));
                        report.warnings.push_back(file.path + ": position " + std::to_string(idx) +
                                                  " (chunk " + std::to_string(chunk->chunk_id) +
                                                  ") cannot be read: " + error);
                        break;
                    }
                    write_piece(off, piece);
                }
            }

            timespec when{};
            when.tv_sec = static_cast<time_t>(mtime_ns / 1'000'000'000);
            when.tv_nsec = static_cast<long>(mtime_ns % 1'000'000'000);
            const std::array<timespec, 2> times{when, when};
            ::futimens(out.get(), times.data());  // время не критично: ошибка не мешает выгрузке
            ::fchmod(out.get(), (node->mode & 0777U) | 0600U);
            if (::fsync(out.get()) != 0) {
                throw SysError(errno, "fsync " + target.string());
            }
            report.files.push_back(std::move(file));
        } catch (const SysError& e) {
            report.warnings.push_back(label + ": not exported: " + e.what());
        }
    }

    // Каталоги, изменённые после последнего коммита, но без выгруженных файлов: создаются
    // пустыми и перечисляются (файлы внутри уже создали свои каталоги).
    for (const int64_t dir : changed_dirs) {
        std::string why;
        const auto parts = inode_components(db, dir, why);
        if (!parts || parts->empty()) {
            continue;
        }
        fs::path target = out_dir;
        for (const std::string& part : *parts) {
            target /= part;
        }
        std::error_code ec;
        if (!fs::exists(target, ec)) {
            make_private_dirs(target);
            report.empty_dirs.push_back(join_for_report(*parts));
        }
    }
    std::sort(report.empty_dirs.begin(), report.empty_dirs.end());
    return report;
}

}  // namespace telega::vfs
