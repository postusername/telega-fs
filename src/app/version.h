#pragma once

#include <string>
#include <string_view>

namespace telega::app {

// Версия telega-fs из project() в CMakeLists.txt.
std::string_view version() noexcept;

// Версии библиотек, с которыми собрано ядро (без TDLib: её версию знает только бинарник).
std::string core_library_versions();

}  // namespace telega::app
