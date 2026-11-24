configure_file(app/build_info.h.in "${CMAKE_CURRENT_BINARY_DIR}/generated/telega/build_info.h" @ONLY)

# Ядро без TDLib: всё, что тестируется на LocalDirStore (SPEC 14, 15.4).
# Каждый модуль (SPEC 15.7) — своя статическая библиотека, telega_core собирает их вместе.

add_library(telega_common STATIC
    common/bytes.cpp
    common/files.cpp)
target_include_directories(telega_common PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(telega_common PUBLIC telega_options spdlog::spdlog Threads::Threads)

# crypto: форматы чанка и блобов индекса, ключи, keyring (SPEC 5, 6.3, 11)
add_library(telega_crypto STATIC
    crypto/keys.cpp
    crypto/chunk.cpp
    crypto/index_blob.cpp
    crypto/keyring.cpp)
target_link_libraries(telega_crypto PUBLIC telega_common PkgConfig::SODIUM PkgConfig::ZSTD)

# store: Store, LocalDirStore (SPEC 3)
add_library(telega_store STATIC
    store/local_dir_store.cpp)
target_link_libraries(telega_store PUBLIC telega_common)

# tgstore: TelegramStore и лимитер (SPEC 3, 10.2). Без TDLib: только telega_td.
add_library(telega_tgstore STATIC
    store/rate_limiter.cpp
    store/download_scheduler.cpp
    store/read_cache.cpp
    store/telegram_store.cpp)
target_link_libraries(telega_tgstore PUBLIC telega_store telega_td telega_meta)

# meta: MetaStore, state.sqlite, дельта-журнал (SPEC 6)
add_library(telega_meta STATIC
    meta/sqlite.cpp
    meta/index_ops.cpp
    meta/index_db.cpp
    meta/snapshot.cpp
    meta/state_db.cpp)
target_link_libraries(telega_meta PUBLIC telega_common telega::sqlite)

# commit: CommitManager, init и монтирование (SPEC 7). Блок ведёт исполнитель commit.
add_library(telega_commit STATIC
    meta/commit.cpp
    meta/chain.cpp
    meta/mount.cpp
    meta/init.cpp
    meta/fsck.cpp)
target_link_libraries(telega_commit PUBLIC telega_meta telega_crypto telega_store)

# vfs: Overlay, пути данных, операции ФС по inode (SPEC 8, 9). Блок ведёт исполнитель vfs.
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

# fuse: libfuse3 low-level поверх Vfs (SPEC 9)
add_library(telega_fuse STATIC
    fuse/frontend.cpp)
target_link_libraries(telega_fuse PUBLIC telega_vfs PkgConfig::FUSE3 spdlog::spdlog)

# app: config.toml, профили, XDG-пути, flock (SPEC 12)
add_library(telega_config STATIC
    app/config.cpp
    app/config_edit.cpp)
target_link_libraries(telega_config PUBLIC telega_common tomlplusplus::tomlplusplus)

# td: TdTransport, TdRuntime, TdClient, ошибки, трекер отправки, авторизация (SPEC 10.1).
# С TDLib не линкуется: тестируется на фейковом транспорте (SPEC 15.4).
add_library(telega_td STATIC
    td/td_error.cpp
    td/td_client.cpp
    td/td_runtime.cpp
    td/send_tracker.cpp
    td/auth.cpp)
target_link_libraries(telega_td PUBLIC telega_common nlohmann_json::nlohmann_json)

# tdjson: транспорт поверх td_json_client. Линкуется только к бинарникам (telega-fs, tgfs-bench).
add_library(telega_tdjson STATIC
    td/tdjson_transport.cpp)
target_link_libraries(telega_tdjson PUBLIC telega_td Td::TdJsonStatic)

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
target_link_libraries(telega-fs PRIVATE telega_core telega_td telega_tdjson CLI11::CLI11
                      Td::TdJsonStatic)
