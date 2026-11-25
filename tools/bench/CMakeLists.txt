# Инструменты экспериментов. Собираются только с -DTELEGA_FS_BUILD_TOOLS=ON.
# Предупреждения и -Werror наследуются от telega_options, как в основном коде.

add_library(tgfs_bench_common STATIC
    common/bench_common.cpp)
target_include_directories(tgfs_bench_common PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(tgfs_bench_common PUBLIC telega_core)

# Синтетический набор C4 «Большое дерево»: только индекс, без данных.
add_library(tgfs_bench_corpora STATIC
    corpora/c4_gen.cpp)
target_include_directories(tgfs_bench_corpora PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(tgfs_bench_corpora PUBLIC tgfs_bench_common)

add_subdirectory(e1)
add_subdirectory(e11)
add_subdirectory(e8)
add_subdirectory(sim)
add_subdirectory(tdbench)
