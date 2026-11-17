#pragma once

// LocalDirStore: канал в локальном каталоге, с инъекцией сбоев (SPEC 3, 14).

#include <array>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "store/store.h"

namespace telega::store {

enum class StoreOp : uint8_t {
    put_chunk,
    read_range,
    delete_messages,
    read_head,
    read_index,
    put_index,
    set_head,
};

inline constexpr size_t kStoreOpCount = 7;

enum class FaultAction : uint8_t {
    transient,     // бросить StoreError(transient) вместо операции
    permanent,     // бросить StoreError(permanent) вместо операции
    crash_before,  // вызвать crash-обработчик до операции
    crash_after,  // выполнить операцию, затем вызвать crash-обработчик
    delay,  // подождать delay, затем выполнить операцию
};

struct FaultRule {
    StoreOp op = StoreOp::put_chunk;
    std::optional<uint64_t> nth;  // номер вызова этой операции с 1; nullopt — каждый вызов
    FaultAction action = FaultAction::transient;
    std::chrono::milliseconds delay{0};
};

// Переменная окружения с правилами сбоев для бинарника (SPEC 3).
inline constexpr const char* kFaultsEnvVar = "TELEGA_FS_FAULTS";

// Разобрать формат TELEGA_FS_FAULTS (SPEC 3). Ошибка формата — std::invalid_argument.
std::vector<FaultRule> parse_fault_rules(std::string_view spec);

// Правила из переменной TELEGA_FS_FAULTS; нет переменной — пустой список.
// Ошибка формата — std::invalid_argument.
std::vector<FaultRule> fault_rules_from_env();

class LocalDirStore final : public Store {
  public:
    // Каталог создаётся, если его нет. Состояние (next_id, pinned) читается с диска,
    // поэтому новый экземпляр на том же каталоге видит всё, что записал прежний.
    explicit LocalDirStore(std::filesystem::path dir);

    // Правила сбоев. Счётчики вызовов ведутся по каждой операции с момента создания объекта.
    void set_faults(std::vector<FaultRule> rules);
    // Что делать при crash_*: по умолчанию _exit(86). Тесты в одном процессе подменяют на
    // бросок исключения CrashInjected.
    void set_crash_handler(std::function<void()> handler);
    // Счётчик вызовов операции (для тестов).
    uint64_t call_count(StoreOp op) const;

    // Для тестов и fsck: список существующих сообщений по возрастанию.
    std::vector<int64_t> list_messages() const;
    std::vector<int64_t> pinned() const;
    const std::filesystem::path& dir() const noexcept { return dir_; }

    ChunkRef put_chunk(const std::filesystem::path& encrypted_file) override;
    void read_range(const ChunkRef& ref, uint64_t offset, std::span<std::byte> out) override;
    void delete_messages(std::span<const int64_t> msg_ids) override;
    std::optional<HeadInfo> read_head() override;
    Bytes read_index(int64_t msg_id) override;
    int64_t put_index(const std::filesystem::path& blob) override;
    void set_head(int64_t new_msg_id, std::optional<int64_t> old_msg_id) override;

  private:
    // Что сделать с текущим вызовом операции по правилам сбоев.
    struct FaultPlan {
        std::chrono::milliseconds delay{0};
        bool crash_before = false;
        bool crash_after = false;
        std::optional<StoreErrorKind> error;
        std::function<void()> crash_handler;
    };

    FaultPlan plan_faults(StoreOp op);
    // Задержка, crash_before, ошибка, затем тело операции, затем crash_after.
    template <class Body>
    auto run_op(StoreOp op, Body&& body);

    std::filesystem::path msg_path(int64_t msg_id) const;
    // Выделить id (долговечно сдвинуть next_id) и скопировать файл в msgs/<id>.
    int64_t put_message(const std::filesystem::path& src);

    // Под mu_: состояние на диске.
    int64_t load_next_id_locked() const;
    std::vector<int64_t> load_pinned_locked() const;
    void save_pinned_locked(const std::vector<int64_t>& pins) const;

    std::filesystem::path dir_;
    std::filesystem::path msgs_dir_;
    mutable std::mutex mu_;
    // Защищено mu_.
    std::vector<FaultRule> rules_;
    std::function<void()> crash_handler_;
    std::array<uint64_t, kStoreOpCount> calls_{};
};

// Исключение, которое бросает crash-обработчик в тестах внутри одного процесса.
struct CrashInjected : std::runtime_error {
    CrashInjected() : std::runtime_error("injected crash") {}
};

}  // namespace telega::store
