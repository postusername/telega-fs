#pragma once

// Настоящий транспорт поверх td_json_client (SPEC 10.1). Линкуется только к бинарникам
// (telega-fs, tgfs-bench), юнит-тесты его не используют (SPEC 15.4).

#include <chrono>
#include <filesystem>
#include <optional>
#include <string>

#include "td/td_transport.h"

namespace telega::td {

class TdJsonTransport final : public TdTransport {
  public:
    int create_client_id() override;
    void send(int client_id, const std::string& request_json) override;
    std::optional<std::string> receive(std::chrono::milliseconds timeout) override;
    std::string execute(const std::string& request_json) override;
};

// Лог TDLib в файл (SPEC 10.1): setLogStream(logStreamFile, max_file_size) и уровень. Лог один на
// процесс, вызывать надо до создания первого клиента. false — TDLib отказал (текст ошибки в
// error, если не nullptr).
bool set_tdlib_log_file(TdTransport& transport, const std::filesystem::path& path,
                        int64_t max_file_size = 16LL * 1024 * 1024, int verbosity = 1,
                        std::string* error = nullptr);

}  // namespace telega::td
