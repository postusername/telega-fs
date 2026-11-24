include(GoogleTest)

add_executable(telega_unit_tests
    unit/sqlite_test.cpp
    unit/deps_test.cpp
    unit/common_test.cpp)
target_link_libraries(telega_unit_tests PRIVATE telega_core GTest::gtest_main)
gtest_discover_tests(telega_unit_tests DISCOVERY_MODE PRE_TEST)

target_compile_definitions(telega_unit_tests PRIVATE
    TELEGA_FS_EXPECTED_VERSION="${PROJECT_VERSION}")

# Тесты модулей — отдельные исполняемые файлы, чтобы модуль собирался и проверялся сам по себе.
function(telega_module_tests name lib)
    add_executable(${name} ${ARGN})
    target_link_libraries(${name} PRIVATE ${lib} GTest::gtest_main GTest::gmock)
    gtest_discover_tests(${name} DISCOVERY_MODE PRE_TEST)
endfunction()

telega_module_tests(telega_app_tests telega_config
    unit/app/config_test.cpp
    unit/app/config_edit_test.cpp)

# Команды login, logout, init, mount для store = "telegram" на фейковом транспорте TDLib.
telega_module_tests(telega_app_cmd_tests telega_core
    unit/app/telegram_cmds_test.cpp)

# Управляющий сокет и status (SPEC 12.3, 12.4).
telega_module_tests(telega_ctl_tests telega_core
    unit/app/ctl_test.cpp)

telega_module_tests(telega_crypto_tests telega_crypto
    unit/crypto/keys_test.cpp
    unit/crypto/chunk_test.cpp
    unit/crypto/index_blob_test.cpp
    unit/crypto/keyring_test.cpp)

# td: TdClient, TdRuntime, трекер отправки, авторизация на фейковом транспорте, без TDLib.
telega_module_tests(telega_td_tests telega_td
    unit/td/td_error_test.cpp
    unit/td/td_client_test.cpp
    unit/td/send_tracker_test.cpp
    unit/td/auth_test.cpp)

telega_module_tests(telega_store_tests telega_store
    unit/store/local_dir_store_test.cpp)

# Блок ведёт исполнитель TelegramStore.
telega_module_tests(telega_tgstore_tests telega_tgstore
    unit/store/rate_limiter_test.cpp
    unit/store/read_cache_test.cpp
    unit/store/telegram_store_test.cpp)

telega_module_tests(telega_meta_tests telega_meta
    unit/meta/sqlite_wrapper_test.cpp
    unit/meta/index_ops_test.cpp
    unit/meta/meta_store_test.cpp
    unit/meta/delta_journal_test.cpp
    unit/meta/fair_mutex_test.cpp
    unit/meta/snapshot_test.cpp
    unit/meta/state_db_test.cpp)

# Блок ведёт исполнитель commit.
telega_module_tests(telega_commit_tests telega_commit
    unit/commit/commit_test.cpp
    unit/commit/mount_test.cpp
    unit/commit/head_loss_test.cpp
    unit/commit/keyring_flow_test.cpp
    unit/commit/commit_timing_test.cpp
    unit/commit/fault_test.cpp
    unit/commit/snapshot_commit_test.cpp)

# Блок ведёт исполнитель vfs.
telega_module_tests(telega_vfs_tests telega_vfs
    unit/vfs/vfs_test.cpp
    unit/vfs/vfs_data_test.cpp
    unit/vfs/vfs_fault_test.cpp
    unit/vfs/vfs_model_test.cpp
    unit/vfs/vfs_concurrency_test.cpp
    unit/vfs/vfs_pack_test.cpp
    unit/vfs/chunk_reader_deadline_test.cpp
    unit/vfs/vfs_readahead_test.cpp
    unit/vfs/vfs_staging_test.cpp
    unit/vfs/vfs_dedup_test.cpp)

# Падение на каждом шаге заливки и применения пакета: настоящее монтирование и Vfs поверх него.
telega_module_tests(telega_pack_fault_tests telega_vfs
    unit/vfs/pack_fault_test.cpp)
target_link_libraries(telega_pack_fault_tests PRIVATE telega_commit)

# Восстановление: fsck и recover --export-dirty. Настоящее монтирование и Vfs поверх него.
telega_module_tests(telega_recovery_tests telega_vfs
    unit/commit/fsck_test.cpp
    unit/commit/export_dirty_test.cpp)
target_link_libraries(telega_recovery_tests PRIVATE telega_commit)

# Дедупликация чанков и отложенная уборка: настоящее монтирование и Vfs поверх него.
telega_module_tests(telega_dedup_commit_tests telega_vfs
    unit/vfs/dedup_commit_test.cpp)
target_link_libraries(telega_dedup_commit_tests PRIVATE telega_commit)

# Vfs поверх TelegramStore на фейковом транспорте TDLib.
telega_module_tests(telega_vfs_tg_tests telega_vfs
    unit/vfs/vfs_telegram_test.cpp)
target_link_libraries(telega_vfs_tg_tests PRIVATE telega_tgstore)

# Бинарник собирается и запускается во всех пресетах. Это единственный тест, который вызывает
# TDLib (синхронный td_execute, без потоков TDLib), в том числе под санитайзерами.
add_test(NAME telega_fs.version
    COMMAND "${PROJECT_SOURCE_DIR}/tools/ci/check-version.sh" $<TARGET_FILE:telega-fs>
            "${Td_VERSION}")

# SPEC 15.4: динамически линкуемся только с разрешённым списком библиотек.
set(_linkage_args "")
string(TOUPPER "${CMAKE_BUILD_TYPE}" _build_type)
if("${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${_build_type}}" MATCHES "-fsanitize=")
    set(_linkage_args --sanitizer)
endif()
add_test(NAME telega_fs.linkage
    COMMAND "${PROJECT_SOURCE_DIR}/tools/ci/check-linkage.sh" ${_linkage_args}
            $<TARGET_FILE:telega-fs>)

add_subdirectory(integration)

if(TELEGA_FS_E2E)
    add_subdirectory(e2e)
endif()
