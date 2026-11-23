# Интеграционные тесты (SPEC 14).

# fsx: библиотека отдельно, чтобы её могли вызывать GoogleTest-обёртки FUSE-тестов.
add_library(tgfs_fsx STATIC fsx/fsx_lib.cpp)
target_include_directories(tgfs_fsx PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/fsx")
target_link_libraries(tgfs_fsx PRIVATE telega_options)

add_executable(tgfs-fsx fsx/fsx.cpp)
target_link_libraries(tgfs-fsx PRIVATE tgfs_fsx telega_options)

# Самопроверка инструмента на обычной ФС в каталоге сборки: ложных расхождений нет, настоящие
# ловятся, продолжение после контрольной точки совпадает с прогоном без неё.
foreach(mode small large determinism resume detects usage)
    add_test(NAME fsx.selftest.${mode}
        COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/fsx/selftest.sh" ${mode} $<TARGET_FILE:tgfs-fsx>
                "${CMAKE_CURRENT_BINARY_DIR}/fsx-selftest")
endforeach()

# FUSE-тесты: настоящий бинарник telega-fs на LocalDirStore во временном каталоге. Без
# /dev/fuse и fusermount3 пропускаются через GTEST_SKIP (SPEC 14). Бинарник линкуется с TDLib,
# сам тестовый исполняемый файл — нет.
add_executable(telega_integration_tests
    harness.cpp
    fuse_test.cpp
    fsx_fuse_test.cpp
    resources_test.cpp)
# telega_store — чтение файла stats LocalDirStore (read_store_stats), telega_crypto — sha256 из
# libsodium. Оба без TDLib.
target_link_libraries(telega_integration_tests PRIVATE
    tgfs_fsx telega_store telega_crypto telega_options GTest::gtest_main)
# Тяжёлые данные приёмки M3 — в каталоге сборки (на большом диске), а не в /tmp.
target_compile_definitions(telega_integration_tests PRIVATE
    TELEGA_FS_BINARY="$<TARGET_FILE:telega-fs>"
    TELEGA_IT_HEAVY_DIR="${CMAKE_BINARY_DIR}/it-tmp")
add_dependencies(telega_integration_tests telega-fs)

# Длинные прогоны (fsx и приёмка M3 на 10 ГиБ) — метка long и большой таймаут. Остальное идёт в
# обычном ctest.
gtest_discover_tests(telega_integration_tests
    TEST_FILTER "-FuseFsxLong.*:ResourcesLong.*"
    DISCOVERY_MODE PRE_TEST
    PROPERTIES TIMEOUT 900)
gtest_discover_tests(telega_integration_tests
    TEST_FILTER "FuseFsxLong.*:ResourcesLong.*"
    DISCOVERY_MODE PRE_TEST
    PROPERTIES LABELS long TIMEOUT 7200)
