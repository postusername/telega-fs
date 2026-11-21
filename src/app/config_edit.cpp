#include "app/config_edit.h"

#include <fcntl.h>
#include <sys/file.h>

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <system_error>
#include <vector>

#include "common/bytes.h"
#include "common/files.h"

namespace telega::app {

namespace fs = std::filesystem;

namespace {

struct Line {
    std::string text;  // без терминатора
    std::string eol;  // "\n", "\r\n" или "" у последней строки без перевода
};

std::vector<Line> split_lines(std::string_view text) {
    std::vector<Line> lines;
    size_t pos = 0;
    while (pos < text.size()) {
        const size_t nl = text.find('\n', pos);
        Line line;
        if (nl == std::string_view::npos) {
            line.text = std::string(text.substr(pos));
            pos = text.size();
        } else {
            line.text = std::string(text.substr(pos, nl - pos));
            line.eol = "\n";
            pos = nl + 1;
            if (!line.text.empty() && line.text.back() == '\r') {
                line.text.pop_back();
                line.eol = "\r\n";
            }
        }
        lines.push_back(std::move(line));
    }
    return lines;
}

std::string join_lines(const std::vector<Line>& lines) {
    std::string out;
    for (const Line& l : lines) {
        out += l.text;
        out += l.eol;
    }
    return out;
}

std::string_view trim(std::string_view s) {
    while (!s.empty() && std::isspace(static_cast<unsigned char>(s.front())) != 0) {
        s.remove_prefix(1);
    }
    while (!s.empty() && std::isspace(static_cast<unsigned char>(s.back())) != 0) {
        s.remove_suffix(1);
    }
    return s;
}

bool is_bare_key(std::string_view name) {
    return !name.empty() && std::all_of(name.begin(), name.end(), [](char c) {
        return std::isalnum(static_cast<unsigned char>(c)) != 0 || c == '_' || c == '-';
    });
}

// Имя таблицы профиля в заголовке: `[profiles.main]`, `[ profiles . "main" ]`. Не `[[...]]`.
bool is_profile_header(std::string_view line, std::string_view profile) {
    const std::string_view s = trim(line);
    if (s.size() < 2 || s.front() != '[' || s[1] == '[') {
        return false;
    }
    const size_t close = s.find(']');
    if (close == std::string_view::npos) {
        return false;
    }
    const std::string_view rest = trim(s.substr(close + 1));
    if (!rest.empty() && rest.front() != '#') {
        return false;
    }
    std::string_view inner = trim(s.substr(1, close - 1));
    if (!inner.starts_with("profiles")) {
        return false;
    }
    inner = trim(inner.substr(std::string_view{"profiles"}.size()));
    if (inner.empty() || inner.front() != '.') {
        return false;
    }
    inner = trim(inner.substr(1));
    if (inner.size() >= 2 && (inner.front() == '"' || inner.front() == '\'') &&
        inner.back() == inner.front()) {
        inner = inner.substr(1, inner.size() - 2);
    }
    return inner == profile;
}

bool is_table_header(std::string_view line) {
    const std::string_view s = trim(line);
    return !s.empty() && s.front() == '[';
}

// Строка `key = ...` ключа key (bare или в кавычках)?
bool is_key_line(std::string_view line, std::string_view key) {
    std::string_view s = trim(line);
    if (s.empty() || s.front() == '#') {
        return false;
    }
    if (s.front() == '"' || s.front() == '\'') {
        const char q = s.front();
        if (s.size() < key.size() + 2 || s.substr(1, key.size()) != key || s[key.size() + 1] != q) {
            return false;
        }
        s.remove_prefix(key.size() + 2);
    } else {
        if (!s.starts_with(key)) {
            return false;
        }
        s.remove_prefix(key.size());
    }
    s = trim(s);
    return !s.empty() && s.front() == '=';
}

// Хвостовой комментарий строки (вместе с пробелами перед ним), если он есть вне кавычек.
std::string trailing_comment(std::string_view line) {
    const size_t eq = line.find('=');
    if (eq == std::string_view::npos) {
        return {};
    }
    char quote = 0;
    for (size_t i = eq + 1; i < line.size(); ++i) {
        const char c = line[i];
        if (quote != 0) {
            if (c == quote) {
                quote = 0;
            }
        } else if (c == '"' || c == '\'') {
            quote = c;
        } else if (c == '#') {
            size_t start = i;
            while (start > eq + 1 && (line[start - 1] == ' ' || line[start - 1] == '\t')) {
                --start;
            }
            return std::string(line.substr(start));
        }
    }
    return {};
}

std::string indent_of(std::string_view line) {
    size_t n = 0;
    while (n < line.size() && (line[n] == ' ' || line[n] == '\t')) {
        ++n;
    }
    return std::string(line.substr(0, n));
}

struct KeyValue {
    std::string key;
    std::string value;
};

std::vector<KeyValue> to_pairs(const ProfileEdit& edit) {
    std::vector<KeyValue> out;
    if (edit.chat_id) {
        out.push_back({"chat_id", std::to_string(*edit.chat_id)});
    }
    if (edit.use_test_dc) {
        out.push_back({"use_test_dc", *edit.use_test_dc ? "true" : "false"});
    }
    return out;
}

[[noreturn]] void cannot_edit(const std::string& what, const std::vector<KeyValue>& pairs) {
    std::string lines;
    for (const KeyValue& kv : pairs) {
        lines += "  " + kv.key + " = " + kv.value + "\n";
    }
    throw ConfigError(what + "; add to the profile by hand:\n" + lines);
}

void verify(const std::string& text, std::string_view profile, const ProfileEdit& edit,
            const std::vector<KeyValue>& pairs) {
    try {
        const Config parsed = parse_config(text, "config.toml");
        const Profile& p = parsed.profile(profile);
        if ((edit.chat_id && p.chat_id != edit.chat_id) ||
            (edit.use_test_dc && p.use_test_dc != *edit.use_test_dc)) {
            cannot_edit("the edit did not take effect in config.toml", pairs);
        }
    } catch (const ConfigError& e) {
        if (std::string_view{e.what()}.starts_with("the edit did not")) {
            throw;
        }
        cannot_edit(std::string("the edited config.toml would not be valid (") + e.what() + ")",
                    pairs);
    }
}

}  // namespace

std::string apply_profile_edit(std::string_view text, std::string_view profile,
                               const ProfileEdit& edit) {
    const std::vector<KeyValue> pairs = to_pairs(edit);
    std::vector<Line> lines = split_lines(text);
    const std::string eol = !lines.empty() && !lines.front().eol.empty() ? lines.front().eol : "\n";

    size_t header = lines.size();
    for (size_t i = 0; i < lines.size(); ++i) {
        if (is_profile_header(lines[i].text, profile)) {
            header = i;
            break;
        }
    }

    if (header == lines.size()) {
        // Таблицы нет. Если профиль всё же есть (inline-таблица, dotted keys), правка не наша.
        bool exists = false;
        try {
            exists = parse_config(text, "config.toml").profiles.contains(profile);
        } catch (const ConfigError&) {
            exists = true;  // файл и так не разбирается: ничего не трогаем
        }
        if (exists) {
            cannot_edit("the profile '" + std::string(profile) +
                            "' is written in a form this tool cannot edit",
                        pairs);
        }
        if (!lines.empty() && lines.back().eol.empty()) {
            lines.back().eol = eol;
        }
        if (!lines.empty() && !trim(lines.back().text).empty()) {
            lines.push_back({"", eol});
        }
        const std::string name =
            is_bare_key(profile) ? std::string(profile) : "\"" + std::string(profile) + "\"";
        lines.push_back({"[profiles." + name + "]", eol});
        for (const KeyValue& kv : pairs) {
            lines.push_back({kv.key + " = " + kv.value, eol});
        }
    } else {
        size_t end = header + 1;
        while (end < lines.size() && !is_table_header(lines[end].text)) {
            ++end;
        }
        size_t last_content = header;  // последняя строка со значением (не пустая и не комментарий)
        for (size_t i = header + 1; i < end; ++i) {
            const std::string_view t = trim(lines[i].text);
            if (!t.empty() && t.front() != '#') {
                last_content = i;
            }
        }
        size_t insert_at = last_content + 1;
        for (const KeyValue& kv : pairs) {
            bool replaced = false;
            for (size_t i = header + 1; i < end; ++i) {
                if (is_key_line(lines[i].text, kv.key)) {
                    const std::string comment = trailing_comment(lines[i].text);
                    lines[i].text = indent_of(lines[i].text) + kv.key + " = " + kv.value + comment;
                    replaced = true;
                    break;
                }
            }
            if (!replaced) {
                if (insert_at > 0 && lines[insert_at - 1].eol.empty()) {
                    lines[insert_at - 1].eol = eol;
                }
                lines.insert(lines.begin() + static_cast<std::ptrdiff_t>(insert_at),
                             Line{kv.key + " = " + kv.value, eol});
                ++insert_at;
                ++end;
            }
        }
    }

    std::string result = join_lines(lines);
    verify(result, profile, edit, pairs);
    return result;
}

void edit_profile_file(const fs::path& path, std::string_view profile, const ProfileEdit& edit) {
    // config.toml может быть символической ссылкой (например, в dotfiles): правка идёт в её
    // цель, иначе атомарная замена через rename превратила бы ссылку в обычный файл.
    // Висячая ссылка (цели ещё нет) тоже ведёт в свою цель: она будет создана. Цепочку ссылок
    // разворачиваем вручную: canonical() на висячей ссылке падает.
    fs::path target = path;
    std::error_code ec;
    constexpr int kMaxLinkHops = 40;  // как SYMLOOP_MAX у ядра
    for (int hops = 0; fs::is_symlink(target, ec); ++hops) {
        if (hops == kMaxLinkHops) {
            throw ConfigError("too many levels of symbolic links: " + path.string());
        }
        const fs::path link = fs::read_symlink(target, ec);
        if (ec) {
            throw ConfigError("cannot follow the symbolic link " + target.string() + ": " +
                              ec.message());
        }
        target = link.is_absolute() ? link : target.parent_path() / link;
    }
    if (target.has_parent_path()) {
        fs::create_directories(target.parent_path());
    }
    // Чтение, правка и запись — под блокировкой соседнего файла: замена через rename меняет
    // inode, поэтому flock на самом config.toml не работал бы. Две правки подряд (например, два
    // профиля) не теряют друг друга.
    fs::path lock_path = target;
    lock_path += ".lock";
    const Fd lock = Fd::open(lock_path, O_RDWR | O_CREAT, 0600);
    if (::flock(lock.get(), LOCK_EX) != 0) {
        throw ConfigError("cannot lock " + lock_path.string() + ": " +
                          std::generic_category().message(errno));
    }
    std::string text;
    if (fs::exists(target, ec)) {
        const Bytes raw = read_file(target);
        text.assign(reinterpret_cast<const char*>(raw.data()), raw.size());
    }
    const std::string edited = apply_profile_edit(text, profile, edit);
    write_file_atomic(target, as_bytes(edited), 0600);
}

}  // namespace telega::app
