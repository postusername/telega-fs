#include "meta/chain.h"

#include <algorithm>
#include <set>
#include <string>

#include "meta/commit.h"

namespace telega::meta {

namespace {

std::string where(int64_t msg_id) { return "index blob " + std::to_string(msg_id) + ": "; }

bool all_zero(const crypto::Hash32& h) {
    return std::all_of(h.begin(), h.end(), [](std::byte b) { return b == std::byte{0}; });
}

}  // namespace

ChainBlob open_chain_blob(const crypto::Key& k_index, const FsUuid& fs_uuid, int64_t msg_id,
                          ByteSpan blob) {
    ChainBlob out;
    out.msg_id = msg_id;
    try {
        auto opened = crypto::open_index_blob(k_index, blob);
        out.header = opened.header;
        if (out.header.fs_uuid != fs_uuid) {
            throw ChainCorrupt(where(msg_id) + "belongs to another file system");
        }
        Bytes body = crypto::zstd_decompress(opened.body);
        if (out.header.kind == crypto::BlobKind::delta) {
            auto delta = decode_delta_body(body);
            out.listed_chain = std::move(delta.chain);
            out.payload = std::move(delta.patchset);
        } else {
            out.payload = std::move(body);
        }
    } catch (const crypto::CorruptError& e) {
        throw ChainCorrupt(where(msg_id) + e.what());
    }
    out.hash = crypto::blake2b256(blob);
    out.blob_size = static_cast<int64_t>(blob.size());
    return out;
}

std::vector<ChainBlob> fetch_chain(store::Store& store, const crypto::Key& k_index,
                                   const FsUuid& fs_uuid, int64_t head_msg_id, ByteSpan head_blob) {
    ChainBlob head = open_chain_blob(k_index, fs_uuid, head_msg_id, head_blob);
    const auto& hh = head.header;
    if (hh.kind == crypto::BlobKind::snapshot) {
        if (hh.snapshot_gen != hh.generation || hh.snapshot_msg_id != 0 ||
            !all_zero(hh.prev_hash)) {
            throw ChainCorrupt(where(head_msg_id) + "inconsistent snapshot header");
        }
        std::vector<ChainBlob> out;
        out.push_back(std::move(head));
        return out;
    }

    const std::vector<int64_t> ids = head.listed_chain;
    const uint64_t n = ids.size();
    if (n == 0 || hh.generation < n || hh.generation - n != hh.snapshot_gen ||
        ids.front() != hh.snapshot_msg_id || ids.back() != hh.prev_msg_id) {
        throw ChainCorrupt(where(head_msg_id) + "chain list does not match the header");
    }
    std::set<int64_t> unique(ids.begin(), ids.end());
    unique.insert(head_msg_id);
    if (unique.size() != n + 1) {
        throw ChainCorrupt(where(head_msg_id) + "chain list has duplicates");
    }

    std::vector<ChainBlob> out;
    out.reserve(ids.size() + 1);
    for (size_t i = 0; i < ids.size(); ++i) {
        Bytes blob;
        try {
            blob = store.read_index(ids[i]);
        } catch (const store::StoreError& e) {
            if (e.kind() == store::StoreErrorKind::not_found) {
                throw ChainCorrupt(where(ids[i]) + "chain element is missing");
            }
            throw;
        }
        ChainBlob el = open_chain_blob(k_index, fs_uuid, ids[i], blob);
        const auto& h = el.header;
        const bool first = i == 0;
        const bool ok =
            h.kind == (first ? crypto::BlobKind::snapshot : crypto::BlobKind::delta) &&
            h.snapshot_gen == hh.snapshot_gen && h.generation == hh.snapshot_gen + i &&
            (first ? h.snapshot_msg_id == 0 && all_zero(h.prev_hash)
                   : h.snapshot_msg_id == ids.front() && h.prev_msg_id == ids[i - 1] &&
                         h.prev_hash == out.back().hash && el.listed_chain.size() == i &&
                         std::equal(el.listed_chain.begin(), el.listed_chain.end(), ids.begin()));
        if (!ok) {
            throw ChainCorrupt(where(ids[i]) + "chain element " + std::to_string(i) +
                               " is out of order, missing or substituted");
        }
        out.push_back(std::move(el));
    }
    if (hh.prev_hash != out.back().hash) {
        throw ChainCorrupt(where(head_msg_id) + "prev_hash does not match the previous element");
    }
    out.push_back(std::move(head));
    return out;
}

}  // namespace telega::meta
