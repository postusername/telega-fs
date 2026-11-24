#include "app/ctl_proto.h"

#include <exception>
#include <string>
#include <utility>

namespace telega::app::ctl {

namespace {

using nlohmann::json;

std::string dump_line(const json& j) {
    // Тексты ошибок могут содержать имена файлов не в UTF-8: заменяем, а не бросаем.
    return j.dump(-1, ' ', false, json::error_handler_t::replace);
}

}  // namespace

std::string make_ok(const json& result) {
    return dump_line({{"proto", kProtoVersion}, {"ok", true}, {"result", result}});
}

std::string make_error(std::string_view code, std::string_view message) {
    return dump_line({{"proto", kProtoVersion},
                      {"ok", false},
                      {"error", {{"code", std::string(code)}, {"message", std::string(message)}}}});
}

std::string dispatch(std::string_view line, const Handlers& handlers) {
    const json req = json::parse(line, nullptr, /*allow_exceptions=*/false);
    if (!req.is_object()) {
        return make_error(code::bad_request, "the request must be a JSON object on one line");
    }
    const auto proto = req.find("proto");
    if (proto == req.end()) {
        return make_error(code::bad_request, "the request has no \"proto\" field");
    }
    // Версия не целое или не 1 — «неизвестная версия» (SPEC 12.4).
    if (!proto->is_number_integer() || proto->get<int64_t>() != kProtoVersion) {
        return make_error(code::unsupported, "unsupported protocol version, this server speaks " +
                                                 std::to_string(kProtoVersion));
    }
    const auto cmd = req.find("cmd");
    if (cmd == req.end() || !cmd->is_string()) {
        return make_error(code::bad_request, "the request has no string \"cmd\" field");
    }
    const auto args = req.find("args");
    if (args != req.end() && !args->is_object() && !args->is_null()) {
        return make_error(code::bad_request, "\"args\" must be an object");
    }

    const std::string name = cmd->get<std::string>();
    const std::function<json()>* handler = nullptr;
    if (name == "status") {
        handler = &handlers.status;
    } else if (name == "sync") {
        handler = &handlers.sync;
    } else if (name == "snapshot") {
        handler = &handlers.snapshot;
    }
    if (handler == nullptr || !*handler) {
        return make_error(code::unsupported, "unknown command \"" + name + "\"");
    }
    try {
        return make_ok((*handler)());
    } catch (const Error& e) {
        return make_error(e.code(), e.what());
    } catch (const std::exception& e) {
        return make_error(code::internal, e.what());
    } catch (...) {
        return make_error(code::internal, "unknown error");
    }
}

}  // namespace telega::app::ctl
