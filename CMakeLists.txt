cmake_minimum_required(VERSION 3.25)

project(telega-fs VERSION 1.0.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(TELEGA_FS_BUILD_TESTS "Build unit and integration tests" ON)
option(TELEGA_FS_WITH_TDLIB "Build with TDLib (OFF: only store = \"local\", for quick core work)" ON)
option(TELEGA_FS_E2E "Build live E2E tests (need a bot, a channel and network)" OFF)
option(TELEGA_FS_BUILD_TOOLS "Build experiment tools from tools/bench" OFF)
option(TELEGA_FS_WERROR "Treat warnings as errors in project code" OFF)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

# Системные зависимости. Динамически линкуемся только с ними и с libc/libstdc++.
find_package(PkgConfig REQUIRED)
pkg_check_modules(FUSE3 REQUIRED IMPORTED_TARGET fuse3>=3.10)
pkg_check_modules(SODIUM REQUIRED IMPORTED_TARGET libsodium>=1.0.18)
pkg_check_modules(ZSTD REQUIRED IMPORTED_TARGET libzstd>=1.5)
find_package(Threads REQUIRED)

# TDLib собирается отдельно (docs/build.md), путь задаётся через Td_DIR. Без неё собираются
# ядро, юнит- и FUSE-тесты на LocalDirStore, а telega-fs работает только с store = "local".
if(TELEGA_FS_WITH_TDLIB)
    find_package(Td 1.8.70 EXACT REQUIRED)
elseif(TELEGA_FS_BUILD_TOOLS OR TELEGA_FS_E2E)
    message(FATAL_ERROR "TELEGA_FS_BUILD_TOOLS and TELEGA_FS_E2E need TELEGA_FS_WITH_TDLIB=ON")
endif()

include(deps)
include(sqlite)

# Предупреждения и определения только для собственного кода, не для зависимостей.
add_library(telega_options INTERFACE)
target_compile_options(telega_options INTERFACE
    -Wall -Wextra -Wpedantic -Wshadow -Wconversion
    $<$<BOOL:${TELEGA_FS_WERROR}>:-Werror>)
# FUSE_USE_VERSION: с libfuse 3.12 и новее берём API конфигурации цикла через
# fuse_loop_cfg_* (можно поднять число рабочих потоков); на 3.10-3.11 остаётся прежний
# интерфейс, в котором число потоков не настраивается.
if(FUSE3_VERSION VERSION_GREATER_EQUAL 3.12)
    set(TELEGA_FUSE_USE_VERSION 312)
else()
    set(TELEGA_FUSE_USE_VERSION 310)
endif()
target_compile_definitions(telega_options INTERFACE
    _FILE_OFFSET_BITS=64
    FUSE_USE_VERSION=${TELEGA_FUSE_USE_VERSION})

add_subdirectory(src)

if(TELEGA_FS_BUILD_TOOLS)
    add_subdirectory(tools/bench)
endif()

if(TELEGA_FS_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
