#include "corpora/c4_gen.h"

#include <algorithm>
#include <cstdio>
#include <stdexcept>

#include "meta/index_ops.h"

namespace telega::bench {

namespace {

constexpr int64_t kBatchFiles = 20'000;  // файлов в одной транзакции индекса
constexpr int64_t kSegment = int64_t{64} * 1024;  // сегмент шифрования чанка
constexpr int64_t kChunkOverhead = 64;  // заголовок чанка, порядок величины
constexpr int64_t kSegmentTag = 17;  // +17 байт на сегмент
constexpr int64_t kBaseTimeNs = 1'700'000'000'000'000'000;
constexpr char kAlphabet[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

int64_t upper_dirs(int64_t leaves, int64_t fanout) {
    int64_t total = 0;
    int64_t level = leaves;
    while (level > 1) {
        level = (level + fanout - 1) / fanout;
        total += level;
    }
    return total;
}

int64_t encrypted_size(int64_t plain) {
    return plain + kChunkOverhead + kSegmentTag * ((plain + kSegment - 1) / kSegment);
}

std::string hex_name(char prefix, int64_t n, int width) {
    char buf[32];
    std::snprintf(buf, sizeof buf, "%c%0*llx", prefix, width, static_cast<unsigned long long>(n));
    return buf;
}

// Правдоподобная подсказка remote_file_id: base64-подобный текст длиной 70..90 символов.
std::string fake_file_id(Rng& rng) {
    const size_t len = 70 + rng.below(21);
    std::string s(len, 'A');
    for (char& c : s) {
        c = kAlphabet[rng.below(64)];
    }
    return s;
}

Bytes random_mac(Rng& rng) {
    Bytes b(32);
    rng.fill(b.data(), b.size());
    return b;
}

// Текст инлайн-файла: символы алфавита base64, то есть сжимается не лучше ~25 %.
Bytes inline_content(Rng& rng, int64_t len) {
    Bytes b(static_cast<size_t>(len));
    for (auto& x : b) {
        x = static_cast<std::byte>(kAlphabet[rng.below(64)]);
    }
    return b;
}

// Состояние генерации между транзакциями.
struct Gen {
    const C4Params& p;
    int64_t base_gen;
    Rng rng;
    C4Index out;
    int64_t pack_threshold = 0;
    int64_t pack_target = 0;
    int64_t chunk_size = 0;
    int64_t inline_max_param = 0;
    int64_t next_msg_id = 1'000'000;

    // Открытый пакет: экстенты ждут, пока чанк-пакет не будет вставлен.
    struct PendingExtent {
        int64_t ino;
        int64_t off;
        int64_t len;
    };
    std::vector<PendingExtent> pack;
    int64_t pack_used = 0;

    Gen(const C4Params& params, int64_t gen) : p(params), base_gen(gen), rng(params.seed) {}

    meta::NewChunk new_chunk(meta::Db& db, meta::ChunkKind kind, int64_t plain) {
        meta::NewChunk c;
        c.chunk_id = meta::alloc_chunk_id(db);
        c.kind = kind;
        c.msg_id = next_msg_id++;
        c.remote_file_id = fake_file_id(rng);
        c.plain_size = plain;
        c.enc_size = encrypted_size(plain);
        c.content_mac = random_mac(rng);
        return c;
    }

    void close_pack(meta::Db& db) {
        if (pack.empty()) {
            return;
        }
        const meta::NewChunk c = new_chunk(db, meta::ChunkKind::pack, pack_used);
        meta::insert_chunk(db, c, base_gen);
        for (const auto& e : pack) {
            meta::put_extent(db, meta::Extent{e.ino, 0, c.chunk_id, e.off, e.len}, base_gen);
            ++out.extents;
        }
        ++out.pack_chunks;
        pack.clear();
        pack_used = 0;
    }

    // Файл «в пакете»: один экстент idx = 0 на общий чанк kind = 2 с chunk_off.
    void add_to_pack(meta::Db& db, int64_t ino, int64_t given = -1) {
        const int64_t size =
            given >= 0 ? given : rng.log_uniform(inline_max_param + 1, pack_threshold);
        if (pack_used + size > pack_target) {
            close_pack(db);
        }
        pack.push_back({ino, pack_used, size});
        pack_used += size;
        set_size(db, ino, size);
        ++out.pack_files;
    }

    void add_positional(meta::Db& db, int64_t ino, int64_t given = -1) {
        const int64_t size =
            given >= 0 ? given : rng.log_uniform(pack_threshold + 1, p.positional_max);
        const int64_t positions = (size + chunk_size - 1) / chunk_size;
        for (int64_t idx = 0; idx < positions; ++idx) {
            const int64_t plain = std::min(chunk_size, size - idx * chunk_size);
            const meta::NewChunk c = new_chunk(db, meta::ChunkKind::positional, plain);
            meta::insert_chunk(db, c, base_gen);
            meta::put_extent(db, meta::Extent{ino, idx, c.chunk_id, 0, plain}, base_gen);
            ++out.extents;
            ++out.positional_chunks;
        }
        set_size(db, ino, size);
        ++out.positional_files;
    }

    // given >= 0 — размер задан (E8), idx — номер файла для источника содержимого.
    void add_inline(meta::Db& db, int64_t ino, int64_t given = -1, int64_t idx = 0) {
        const int64_t len =
            given >= 0 ? given
                       : p.inline_min + static_cast<int64_t>(rng.below(static_cast<uint64_t>(
                                            p.inline_max - p.inline_min + 1)));
        const Bytes data =
            given >= 0 && p.content ? p.content(rng, idx, len) : inline_content(rng, len);
        if (!meta::set_inline(db, ino, data, base_gen)) {
            ++out.inline_overflow;  // бюджет исчерпан: как при запечатывании, файл идёт в пакет
            add_to_pack(db, ino, given);
            return;
        }
        set_size(db, ino, len);
        out.inline_bytes += len;
        out.inline_inos.push_back(ino);
        out.inline_sizes.push_back(len);
        ++out.inline_files;
    }

    static void set_size(meta::Db& db, int64_t ino, int64_t size) {
        meta::AttrUpdate u;
        u.size = size;
        meta::update_attrs(db, ino, u);
    }

    static int64_t add_dir(meta::Db& db, int64_t parent, const std::string& name, int64_t seq) {
        meta::NewInode n;
        n.parent = parent;
        n.name.assign(reinterpret_cast<const std::byte*>(name.data()),
                      reinterpret_cast<const std::byte*>(name.data()) + name.size());
        n.kind = meta::InodeKind::dir;
        n.mode = 0755;
        n.uid = 1000;
        n.gid = 1000;
        n.now_ns = kBaseTimeNs + seq * 1000;
        return meta::insert_inode(db, n);
    }

    static int64_t add_file_inode(meta::Db& db, int64_t parent, const std::string& name,
                                  int64_t seq) {
        meta::NewInode n;
        n.parent = parent;
        n.name.assign(reinterpret_cast<const std::byte*>(name.data()),
                      reinterpret_cast<const std::byte*>(name.data()) + name.size());
        n.kind = meta::InodeKind::file;
        n.mode = 0644;
        n.uid = 1000;
        n.gid = 1000;
        n.now_ns = kBaseTimeNs + seq * 1000;
        return meta::insert_inode(db, n);
    }
};

const char* const kExtensions[] = {".dat", ".txt", ".bin", ".jpg", ".cpp", ".log", ".json", ".pdf"};

}  // namespace

C4Layout plan_c4_layout(int64_t inodes, int64_t fanout) {
    if (inodes < fanout * 4 || fanout < 2) {
        throw std::invalid_argument("c4: too few inodes for the fanout");
    }
    // Наименьшее число листьев L, при котором файлов F = N - L - выше(L) не больше fanout · L.
    for (int64_t leaves = 2;; ++leaves) {
        const int64_t dirs = leaves + upper_dirs(leaves, fanout);
        const int64_t files = inodes - dirs;
        if (files <= fanout * leaves) {
            C4Layout layout;
            for (int64_t level = leaves;; level = (level + fanout - 1) / fanout) {
                layout.dirs_per_level.push_back(level);
                if (level == 1) {
                    break;
                }
            }
            std::reverse(layout.dirs_per_level.begin(), layout.dirs_per_level.end());
            layout.files = files;
            return layout;
        }
    }
}

C4Layout plan_c4_layout_for_files(int64_t files, int64_t fanout) {
    if (files < fanout * 2 || fanout < 2) {
        throw std::invalid_argument("c4: too few files for the fanout");
    }
    int64_t leaves = (files + fanout - 1) / fanout;
    leaves = std::max<int64_t>(leaves, 2);
    const int64_t inodes = files + leaves + upper_dirs(leaves, fanout);
    C4Layout layout = plan_c4_layout(inodes, fanout);
    if (layout.files != files) {
        throw std::logic_error("c4: layout does not fit the requested number of files");
    }
    return layout;
}

C4Index generate_c4(meta::MetaStore& meta, const C4Params& params, int64_t base_gen,
                    const std::function<void(int64_t)>& progress) {
    const C4Layout layout =
        params.sizes != nullptr
            ? plan_c4_layout_for_files(static_cast<int64_t>(params.sizes->size()), params.fanout)
            : plan_c4_layout(params.inodes, params.fanout);
    Gen g{params, base_gen};
    const meta::IndexParams ip = meta.read([](meta::Db& db) { return meta::read_params(db); });
    g.pack_threshold = ip.pack_threshold;
    g.pack_target = ip.pack_target;
    g.chunk_size = ip.chunk_size;
    g.inline_max_param = ip.inline_max;
    g.out.params = params;
    g.out.depth = static_cast<int64_t>(layout.dirs_per_level.size());

    // Каталоги сверху вниз; уровень 0 — корень (ino 1, уже есть).
    std::vector<int64_t> prev_level{meta::kRootIno};
    int64_t seq = 0;
    g.out.dirs = 1;
    for (size_t level = 1; level < layout.dirs_per_level.size(); ++level) {
        const int64_t count = layout.dirs_per_level[level];
        std::vector<int64_t> this_level;
        this_level.reserve(static_cast<size_t>(count));
        const bool leaf = level + 1 == layout.dirs_per_level.size();
        for (int64_t at = 0; at < count; at += kBatchFiles) {
            const int64_t end = std::min(count, at + kBatchFiles);
            meta.write([&](meta::Db& db) {
                for (int64_t j = at; j < end; ++j) {
                    const int64_t parent = prev_level[static_cast<size_t>(j / params.fanout)];
                    const std::string name = hex_name('d', j, 6);
                    const int64_t ino = Gen::add_dir(db, parent, name, ++seq);
                    this_level.push_back(ino);
                    if (leaf) {
                        g.out.leaf_dirs.push_back({ino, parent, name});
                    }
                }
            });
            g.out.dirs += end - at;
        }
        prev_level = std::move(this_level);
    }
    if (progress) {
        progress(g.out.dirs);
    }

    // Файлы: i-й файл лежит в листе i / fanout.
    const auto& leaves = prev_level;
    g.out.file_inos.reserve(static_cast<size_t>(layout.files));
    for (int64_t at = 0; at < layout.files; at += kBatchFiles) {
        const int64_t end = std::min(layout.files, at + kBatchFiles);
        meta.write([&](meta::Db& db) {
            for (int64_t i = at; i < end; ++i) {
                const int64_t parent = leaves[static_cast<size_t>(i / params.fanout)];
                const std::string name =
                    hex_name('f', i, 8) + kExtensions[g.rng.below(std::size(kExtensions))];
                const int64_t ino = Gen::add_file_inode(db, parent, name, ++seq);
                g.out.file_inos.push_back(ino);
                if (params.sizes != nullptr) {
                    const int64_t size = (*params.sizes)[static_cast<size_t>(i)];
                    if (size == 0) {
                        ++g.out.empty_files;
                    } else if (size <= g.inline_max_param) {
                        g.add_inline(db, ino, size, i);
                    } else if (size <= g.pack_threshold) {
                        g.add_to_pack(db, ino, size);
                    } else {
                        g.add_positional(db, ino, size);
                    }
                    continue;
                }
                const auto draw = static_cast<int>(g.rng.below(1000));
                if (draw < params.pack_permille) {
                    g.add_to_pack(db, ino);
                } else if (draw < params.pack_permille + params.inline_permille) {
                    g.add_inline(db, ino);
                } else {
                    g.add_positional(db, ino);
                }
            }
            if (end == layout.files) {
                g.close_pack(db);
            }
        });
        g.out.files += end - at;
        if (progress) {
            progress(g.out.dirs + g.out.files);
        }
    }
    return std::move(g.out);
}

}  // namespace telega::bench
