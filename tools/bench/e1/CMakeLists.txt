# E1: размеры патчей дельта-журнала. Гоняется на самом telega-fs с LocalDirStore,
# TDLib не нужен.

add_executable(tgfs-e1
    main.cpp
    c1.cpp
    c4.cpp
    stress.cpp)
target_link_libraries(tgfs-e1 PRIVATE tgfs_bench_common tgfs_bench_corpora CLI11::CLI11)
target_compile_definitions(tgfs-e1 PRIVATE TGFS_BUILD_TYPE="$<CONFIG>")
