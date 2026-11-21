# tgfs-sim: офлайн-симуляция пакетов мелких файлов (SPEC 17.2, 17.12). Сети и TDLib не нужны.

add_executable(tgfs-sim
    main.cpp
    sim.cpp
    workload.cpp)
target_link_libraries(tgfs-sim PRIVATE tgfs_bench_common CLI11::CLI11)
