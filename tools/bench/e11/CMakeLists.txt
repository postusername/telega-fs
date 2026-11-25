# E11: снимок индекса без блокировки записи. Гоняется на MetaStore с LocalDirStore,
# TDLib не нужен.
#
# Вариант (c) требует SQLITE_ENABLE_SNAPSHOT. Глобальный флаг в cmake/sqlite.cmake не нужен:
# при -DTELEGA_FS_E11_SNAPSHOT=ON флаг добавляется к telega_sqlite только в этой сборке
# (продуктовая сборка остаётся без него).

option(TELEGA_FS_E11_SNAPSHOT "E11: build SQLite with SQLITE_ENABLE_SNAPSHOT (variant c)" OFF)

add_executable(tgfs-e11
    main.cpp
    corpus.cpp
    run.cpp
    probe.cpp)
target_link_libraries(tgfs-e11 PRIVATE tgfs_bench_common tgfs_bench_corpora CLI11::CLI11)
target_compile_definitions(tgfs-e11 PRIVATE TGFS_BUILD_TYPE="$<CONFIG>")
if(TELEGA_FS_E11_SNAPSHOT)
    target_compile_definitions(telega_sqlite PUBLIC SQLITE_ENABLE_SNAPSHOT)
endif()
