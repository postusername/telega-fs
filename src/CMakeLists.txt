configure_file(app/build_info.h.in "${CMAKE_CURRENT_BINARY_DIR}/generated/telega/build_info.h" @ONLY)

# Ядро без TDLib: всё, что тестируется на LocalDirStore и на фейковом транспорте TDLib.
# Каждый модуль — своя статическая библиотека, telega_core собирает их вместе.

add_library(telega_common STATIC
    common/bytes.cpp
    common/files.cpp)
target_include_directories(telega_common PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(telega_common PUBLIC telega_options spdlog::spdlog Threads::Threads)

# crypto: форматы чанка и блобов индекса, ключи, keyring
add_library(telega_crypto STATIC
    crypto/keys.cpp
    crypto/chunk.cpp
    crypto/index_blob.cpp
    crypto/keyring.cpp)
target_link_libraries(telega_crypto PUBLIC telega_common PkgConfig::SODIUM PkgConfig::ZSTD)

# store: Store, LocalDirStore
add_library(telega_store STATIC
    store/local_dir_store.cpp)
target_link_libraries(telega_store PUBLIC telega_common)

# tgstore: TelegramStore, лимитер, планировщик скачиваний, кэш чтения. Без TDLib: только telega_td.
add_library(telega_tgstore STATIC
    store/rate_limiter.cpp
    store/download_scheduler.cpp
    store/read_cache.cpp
    store/telegram_store.cpp)
target_link_libraries(telega_tgstore PUBLIC telega_store telega_td telega_meta)

# meta: MetaStore, state.sqlite, дельта-журнал, снимки
add_library(telega_meta STATIC
    meta/sqlite.cpp
    meta/index_ops.cpp
    meta/index_db.cpp
    meta/snapshot.cpp
    meta/state_db.cpp)
target_link_libraries(telega_meta PUBLIC telega_common telega::sqlite)

# commit: CommitManager, init, монтирование, fsck
add_library(telega_commit STATIC
    meta/commit.cpp
    meta/chain.cpp
    meta/mount.cpp
    meta/init.cpp
    meta/fsck.cpp)
target_link_libraries(telega_commit PUBLIC telega_meta telega_crypto telega_store)

# vfs: Overlay, пути данных, пакеты, staging, операции ФС по inode
add_library(telega_vfs STATIC
    vfs/vfs.cpp
    vfs/vfs_data.cpp
    vfs/vfs_upload.cpp
    vfs/overlay.cpp
    vfs/staging_budget.cpp
    vfs/pack_writer.cpp
    vfs/pack_compact.cpp
    vfs/chunk_reader.cpp
    vfs/export_dirty.cpp)
target_link_libraries(telega_vfs PUBLIC telega_meta telega_crypto telega_store)

# fuse: libfuse3 low-level поверх Vfs
add_library(telega_fuse STATIC
    fuse/frontend.cpp)
target_link_libraries(telega_fuse PUBLIC telega_vfs PkgConfig::FUSE3 spdlog::spdlog)

# app: config.toml, профили, XDG-пути, flock
add_library(telega_config STATIC
    app/config.cpp
    app/config_edit.cpp)
target_link_libraries(telega_config PUBLIC telega_common tomlplusplus::tomlplusplus)

# td: TdTransport, TdRuntime, TdClient, ошибки, трекер отправки, авторизация.
# С TDLib не линкуется: тестируется на фейковом транспорте.
add_library(telega_td STATIC
    td/td_error.cpp
    td/td_client.cpp
    td/td_runtime.cpp
    td/send_tracker.cpp
    td/auth.cpp)
target_link_libraries(telega_td PUBLIC telega_common nlohmann_json::nlohmann_json)

# tdjson: транспорт поверх td_json_client. Линкуется только к бинарникам (telega-fs, tgfs-bench).
if(TELEGA_FS_WITH_TDLIB)
    add_library(telega_tdjson STATIC
        td/tdjson_transport.cpp)
    target_link_libraries(telega_tdjson PUBLIC telega_td Td::TdJsonStatic)
endif()

add_library(telega_core STATIC
    app/version.cpp
    app/cli_util.cpp
    app/tg_session.cpp
    app/terminal_prompter.cpp
    app/telegram_cmds.cpp
    app/maint_session.cpp
    app/cmd_fsck.cpp
    app/cmd_recover.cpp
    app/commands.cpp
    app/ctl_proto.cpp
    app/ctl_socket.cpp
    app/ctl_client.cpp
    app/ctl_handlers.cpp
    app/status.cpp
    app/cmd_maint.cpp)
target_include_directories(telega_core PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}"
    "${CMAKE_CURRENT_BINARY_DIR}/generated")
target_link_libraries(telega_core
    PUBLIC
        telega_options
        telega_common
        telega_crypto
        telega_store
        telega_meta
        telega_commit
        telega_vfs
        telega_config
        telega_fuse
        telega_td
        telega_tgstore
        telega::sqlite
        PkgConfig::SODIUM
        PkgConfig::ZSTD
        PkgConfig::FUSE3
        spdlog::spdlog
        nlohmann_json::nlohmann_json
        tomlplusplus::tomlplusplus
        Threads::Threads)

add_executable(telega-fs app/main.cpp)
target_link_libraries(telega-fs PRIVATE telega_core telega_td CLI11::CLI11)
if(TELEGA_FS_WITH_TDLIB)
    target_link_libraries(telega-fs PRIVATE telega_tdjson Td::TdJsonStatic)
    target_compile_definitions(telega-fs PRIVATE TELEGA_FS_HAVE_TDLIB=1)
else()
    target_compile_definitions(telega-fs PRIVATE TELEGA_FS_HAVE_TDLIB=0)
endif()
