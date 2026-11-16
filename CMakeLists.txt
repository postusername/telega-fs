cmake_minimum_required(VERSION 3.25)

project(telega-fs VERSION 0.0.1 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(TELEGA_FS_BUILD_TESTS "Build unit and integration tests" ON)
option(TELEGA_FS_E2E "Build E2E tests (need a test DC account and network)" OFF)
option(TELEGA_FS_BUILD_TOOLS "Build experiment tools from tools/bench" OFF)
option(TELEGA_FS_WERROR "Treat warnings as errors in project code" OFF)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

# Системные зависимости (SPEC 15.1). Динамически линкуемся только с ними.
find_package(PkgConfig REQUIRED)
pkg_check_modules(FUSE3 REQUIRED IMPORTED_TARGET fuse3>=3.10)
pkg_check_modules(SODIUM REQUIRED IMPORTED_TARGET libsodium>=1.0.18)
pkg_check_modules(ZSTD REQUIRED IMPORTED_TARGET libzstd>=1.5)
find_package(Threads REQUIRED)

# TDLib собирается отдельно (SPEC 15.3), путь задаётся через Td_DIR.
find_package(Td 1.8.70 EXACT REQUIRED)

include(deps)
include(sqlite)

# Предупреждения и определения только для собственного кода, не для зависимостей.
add_library(telega_options INTERFACE)
target_compile_options(telega_options INTERFACE
    -Wall -Wextra -Wpedantic -Wshadow -Wconversion
    $<$<BOOL:${TELEGA_FS_WERROR}>:-Werror>)
target_compile_definitions(telega_options INTERFACE
    _FILE_OFFSET_BITS=64
    FUSE_USE_VERSION=310)

add_subdirectory(src)

if(TELEGA_FS_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
