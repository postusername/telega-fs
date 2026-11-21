#pragma once

// Точечная правка config.toml (SPEC 12.2): init записывает chat_id, login --test-dc записывает
// use_test_dc. Остальное содержимое файла (комментарии, порядок, чужие профили) сохраняется
// байт в байт: текст правится построчно, а не переписывается из разобранного дерева.

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>

#include "app/config.h"

namespace telega::app {

struct ProfileEdit {
    std::optional<int64_t> chat_id;
    std::optional<bool> use_test_dc;
};

// Новый текст файла. Ключ заменяется на месте (с сохранением отступа и хвостового комментария),
// иначе добавляется в конец таблицы профиля; профиля нет — в конец файла добавляется таблица.
// Результат разбирается заново и сверяется с правкой; если профиль записан способом, который
// построчная правка не понимает (inline-таблица, dotted keys), или проверка не прошла —
// ConfigError, ничего не записывается.
std::string apply_profile_edit(std::string_view text, std::string_view profile,
                               const ProfileEdit& edit);

// То же для файла: чтение, apply_profile_edit, атомарная запись с правами 0600. Файла нет —
// создаётся (вместе с каталогом).
void edit_profile_file(const std::filesystem::path& path, std::string_view profile,
                       const ProfileEdit& edit);

}  // namespace telega::app
