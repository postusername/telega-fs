#pragma once

// Узкий интерфейс хранилища сообщений канала (SPEC 3). Реализации: LocalDirStore (тесты,
// эксперименты, отладка) и TelegramStore (M2).
//
// Все вызовы блокирующие и потокобезопасные. Вызывать их при удерживаемой блокировке MetaStore
// НЕ ДОПУСКАЕТСЯ (SPEC 9).

#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

#include "common/bytes.h"

namespace telega::store {

struct ChunkRef {
    int64_t msg_id = 0;
    std::string remote_file_id;  // подсказка, может быть пустой
};

inline constexpr size_t kHeadHeaderSize = 256;

struct HeadInfo {
    int64_t msg_id = 0;
    // Первые 256 байт блоба (открытый заголовок, SPEC 6.3). Короче, если сообщение короче.
    Bytes header;
};

enum class StoreErrorKind : uint8_t {
    transient,  // сеть, таймаут, 5xx, FLOOD_WAIT: можно повторить
    not_found,  // сообщения нет
    permanent,  // остальное: повтор не поможет
};

class StoreError : public std::runtime_error {
  public:
    StoreError(StoreErrorKind kind, const std::string& what)
        : std::runtime_error(what), kind_(kind) {}
    [[nodiscard]] StoreErrorKind kind() const noexcept { return kind_; }

  private:
    StoreErrorKind kind_;
};

class Store {
  public:
    Store() = default;
    Store(const Store&) = delete;
    Store& operator=(const Store&) = delete;
    virtual ~Store() = default;

    // Загрузить зашифрованный файл чанка как новое сообщение. Файл после возврата можно удалять.
    virtual ChunkRef put_chunk(const std::filesystem::path& encrypted_file) = 0;

    // Прочитать [offset, offset + out.size()) сообщения. Выход за конец — permanent.
    virtual void read_range(const ChunkRef& ref, uint64_t offset, std::span<std::byte> out) = 0;

    // Удалить сообщения. Отсутствующие id не ошибка.
    virtual void delete_messages(std::span<const int64_t> msg_ids) = 0;

    // Голова ФС: самое новое закреплённое сообщение. nullopt — закреплённых нет.
    virtual std::optional<HeadInfo> read_head() = 0;

    // Прочитать сообщение-блоб индекса целиком.
    virtual Bytes read_index(int64_t msg_id) = 0;

    // Загрузить блоб индекса, вернуть msg_id.
    virtual int64_t put_index(const std::filesystem::path& blob) = 0;

    // Закрепить new_msg_id (точка коммита, SPEC 7.1 шаг 6), затем открепить old_msg_id.
    // Если pin не удался — исключение, голова не изменилась. Ошибка unpin проглатывается
    // (пишется в лог): головой всё равно считается самое новое закреплённое.
    virtual void set_head(int64_t new_msg_id, std::optional<int64_t> old_msg_id) = 0;
};

}  // namespace telega::store
