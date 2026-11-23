# tgfs-bench: замеры через TdClient против тестового DC (SPEC 17.2). Линкуется с TDLib.
# Общая обвязка (учётные данные, state-dir, вход аккаунта) — в tgfs_bench_td, подкоманды
# экспериментов добавляются отдельными файлами в executable tgfs-bench.

add_library(tgfs_bench_td STATIC
    tdbench.cpp
    exp_common.cpp
    send_load.cpp
    run_common.cpp)
target_include_directories(tgfs_bench_td PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(tgfs_bench_td PUBLIC telega_core telega_td telega_tdjson)
target_compile_definitions(tgfs_bench_td PRIVATE TGFS_REPO_ROOT="${PROJECT_SOURCE_DIR}")

add_executable(tgfs-bench
    main.cpp
    cmd_login.cpp
    cmd_create_bot.cpp
    cmd_e2.cpp
    cmd_e3.cpp
    cmd_e4.cpp
    cmd_e5.cpp
    cmd_e6.cpp
    cmd_e7.cpp
    cmd_e9.cpp
    cmd_e10.cpp
    cmd_e2e.cpp)
target_link_libraries(tgfs-bench PRIVATE tgfs_bench_td CLI11::CLI11)
