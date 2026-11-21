#include "td/tdjson_transport.h"

#include <nlohmann/json.hpp>
#include <td/telegram/td_json_client.h>

namespace telega::td {

int TdJsonTransport::create_client_id() { return td_create_client_id(); }

void TdJsonTransport::send(int client_id, const std::string& request_json) {
    td_send(client_id, request_json.c_str());
}

std::optional<std::string> TdJsonTransport::receive(std::chrono::milliseconds timeout) {
    const char* raw = td_receive(std::chrono::duration<double>(timeout).count());
    if (raw == nullptr) {
        return std::nullopt;
    }
    return std::string{raw};  // указатель действует до следующего td_receive: копируем
}

std::string TdJsonTransport::execute(const std::string& request_json) {
    const char* raw = td_execute(request_json.c_str());
    return raw == nullptr ? std::string{} : std::string{raw};
}

bool set_tdlib_log_file(TdTransport& transport, const std::filesystem::path& path,
                        int64_t max_file_size, int verbosity, std::string* error) {
    using nlohmann::json;
    const json stream{{"@type", "setLogStream"},
                      {"log_stream",
                       {{"@type", "logStreamFile"},
                        {"path", path.string()},
                        {"max_file_size", max_file_size},
                        {"redirect_stderr", false}}}};
    const json level{{"@type", "setLogVerbosityLevel"}, {"new_verbosity_level", verbosity}};
    for (const json& request : {stream, level}) {
        const json reply = json::parse(transport.execute(request.dump()), nullptr, false);
        if (!reply.is_object() || reply.value("@type", std::string{}) != "ok") {
            if (error != nullptr) {
                *error = reply.is_object() ? reply.value("message", std::string{"error"})
                                           : std::string{"no reply"};
            }
            return false;
        }
    }
    return true;
}

}  // namespace telega::td
