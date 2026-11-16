# Зависимости, которые собираются из исходников (SPEC 15.1).
# Системные копии этих библиотек намеренно не используются: сборка одинаковая везде.
# Обновление любой зависимости — отдельный коммит с новым URL и SHA256.
# Офлайн-сборка: -DFETCHCONTENT_FULLY_DISCONNECTED=ON и заполненный FETCHCONTENT_BASE_DIR.

include(FetchContent)

# Общий каталог для скачанных архивов (SPEC 15.6). Архив с совпавшим SHA256 повторно не качается,
# поэтому все пресеты и задачи CI делят одну копию. Пусто — каждый build-каталог качает сам.
# У каждого архива своё DOWNLOAD_NAME: теги GitHub (v1.17.0.tar.gz) у разных проектов совпадают.
set(TELEGA_FS_DOWNLOAD_DIR "" CACHE PATH "Shared directory for FetchContent archives")

set(_telega_download_args "")
if(TELEGA_FS_DOWNLOAD_DIR)
    file(MAKE_DIRECTORY "${TELEGA_FS_DOWNLOAD_DIR}")
    set(_telega_download_args DOWNLOAD_DIR "${TELEGA_FS_DOWNLOAD_DIR}")
endif()

FetchContent_Declare(sqlite_amalgamation
    URL https://sqlite.org/2026/sqlite-amalgamation-3540100.zip
    DOWNLOAD_NAME sqlite-amalgamation-3540100.zip
    URL_HASH SHA256=0a9c8e34352dad46006aa82836d5383bff9dc6bc9ce5a60553c05aa30e6ad1f9
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    ${_telega_download_args})

FetchContent_Declare(nlohmann_json
    URL https://github.com/nlohmann/json/releases/download/v3.12.0/json.tar.xz
    DOWNLOAD_NAME nlohmann_json-3.12.0.tar.xz
    URL_HASH SHA256=42f6e95cad6ec532fd372391373363b62a14af6d771056dbfc86160e6dfff7aa
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    ${_telega_download_args}
    SYSTEM)

FetchContent_Declare(tomlplusplus
    URL https://github.com/marzer/tomlplusplus/archive/refs/tags/v3.4.0.tar.gz
    DOWNLOAD_NAME tomlplusplus-3.4.0.tar.gz
    URL_HASH SHA256=8517f65938a4faae9ccf8ebb36631a38c1cadfb5efa85d9a72e15b9e97d25155
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    ${_telega_download_args}
    SYSTEM)

FetchContent_Declare(cli11
    URL https://github.com/CLIUtils/CLI11/archive/refs/tags/v2.7.2.tar.gz
    DOWNLOAD_NAME cli11-2.7.2.tar.gz
    URL_HASH SHA256=46eef3101da70852ec7af026e09d485ccee81813331c8c6052d39344443b83da
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    ${_telega_download_args}
    SYSTEM)

FetchContent_Declare(spdlog
    URL https://github.com/gabime/spdlog/archive/refs/tags/v1.17.0.tar.gz
    DOWNLOAD_NAME spdlog-1.17.0.tar.gz
    URL_HASH SHA256=d8862955c6d74e5846b3f580b1605d2428b11d97a410d86e2fb13e857cd3a744
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    ${_telega_download_args}
    SYSTEM)

FetchContent_Declare(googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.18.0.tar.gz
    DOWNLOAD_NAME googletest-1.18.0.tar.gz
    URL_HASH SHA256=6e3191c1455468b3fc35a417fb565c1c5071aee1b7e7f85e30cf48a98d37d8b5
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    ${_telega_download_args}
    SYSTEM)

set(JSON_BuildTests OFF CACHE INTERNAL "")
set(JSON_Install OFF CACHE INTERNAL "")

set(CLI11_BUILD_TESTS OFF CACHE INTERNAL "")
set(CLI11_BUILD_EXAMPLES OFF CACHE INTERNAL "")
set(CLI11_BUILD_DOCS OFF CACHE INTERNAL "")
set(CLI11_INSTALL OFF CACHE INTERNAL "")
set(CLI11_PRECOMPILED OFF CACHE INTERNAL "")

set(SPDLOG_BUILD_EXAMPLE OFF CACHE INTERNAL "")
set(SPDLOG_BUILD_TESTS OFF CACHE INTERNAL "")
set(SPDLOG_BUILD_BENCH OFF CACHE INTERNAL "")
set(SPDLOG_INSTALL OFF CACHE INTERNAL "")
set(SPDLOG_FMT_EXTERNAL OFF CACHE INTERNAL "")

set(INSTALL_GTEST OFF CACHE INTERNAL "")
set(BUILD_GMOCK ON CACHE INTERNAL "")

FetchContent_MakeAvailable(nlohmann_json tomlplusplus cli11 spdlog)

if(TELEGA_FS_BUILD_TESTS)
    FetchContent_MakeAvailable(googletest)
endif()
