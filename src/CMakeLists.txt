configure_file(app/build_info.h.in "${CMAKE_CURRENT_BINARY_DIR}/generated/telega/build_info.h" @ONLY)

# Ядро без TDLib: всё, что тестируется на LocalDirStore (SPEC 14, 15.4).
add_library(telega_core STATIC
    app/version.cpp)
target_include_directories(telega_core PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}"
    "${CMAKE_CURRENT_BINARY_DIR}/generated")
target_link_libraries(telega_core
    PUBLIC
        telega_options
        telega::sqlite
        PkgConfig::SODIUM
        PkgConfig::ZSTD
        PkgConfig::FUSE3
        spdlog::spdlog
        nlohmann_json::nlohmann_json
        tomlplusplus::tomlplusplus
        Threads::Threads)

add_executable(telega-fs app/main.cpp)
target_link_libraries(telega-fs PRIVATE telega_core CLI11::CLI11 Td::TdJsonStatic)
