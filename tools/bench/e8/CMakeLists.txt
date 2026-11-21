# E8: инлайн-файлы (SPEC 17.11). Размеры снимков и дельт индекса на настоящем telega-fs с
# LocalDirStore, TDLib не нужен. Сообщения экономит tgfs-sim (run-e8.sh), расчёт времени
# заливки и решения — analyze-e8.py.

add_executable(tgfs-e8
    main.cpp
    datasets.cpp
    index_run.cpp
    ../sim/workload.cpp)
target_link_libraries(tgfs-e8 PRIVATE tgfs_bench_common tgfs_bench_corpora CLI11::CLI11)
target_compile_definitions(tgfs-e8 PRIVATE TGFS_BUILD_TYPE="$<CONFIG>")
