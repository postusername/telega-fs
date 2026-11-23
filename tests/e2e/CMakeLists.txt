# E2E на живом Telegram в режиме бота (SPEC 14). Подключается только с -DTELEGA_FS_E2E=ON.
# Тест ходит в сеть и нужен ему бот и канал (см. шапку e2e_bot.sh), поэтому без
# TELEGA_FS_E2E_RUN=1 он пропускается (код 77) и `ctest -LE long` в сеть не ходит.
# Запуск: TELEGA_FS_E2E_RUN=1 ctest --test-dir <build> -L e2e --output-on-failure

if(NOT TARGET tgfs-bench)
    message(FATAL_ERROR "TELEGA_FS_E2E needs tgfs-bench: configure with -DTELEGA_FS_BUILD_TOOLS=ON")
endif()

add_test(NAME telega_fs.e2e_bot
    COMMAND bash "${CMAKE_CURRENT_SOURCE_DIR}/e2e_bot.sh")
set_tests_properties(telega_fs.e2e_bot PROPERTIES
    LABELS e2e
    TIMEOUT 7200
    SKIP_RETURN_CODE 77
    ENVIRONMENT "E2E_CTEST=1;TGFS_BIN=$<TARGET_FILE:telega-fs>;BENCH_BIN=$<TARGET_FILE:tgfs-bench>")

# Расширенный сценарий M3 (холодное чтение, видео, файл 10 ГиБ, кэш чтения): 1,5-2 часа, возобновляемый.
# Прогонять лучше напрямую (tests/e2e/e2e_m3.sh, см. шапку), ctest — для единообразия.
add_test(NAME telega_fs.e2e_m3
    COMMAND bash "${CMAKE_CURRENT_SOURCE_DIR}/e2e_m3.sh")
set_tests_properties(telega_fs.e2e_m3 PROPERTIES
    LABELS "e2e;long"
    TIMEOUT 14400
    SKIP_RETURN_CODE 77
    ENVIRONMENT "E2E_CTEST=1;TGFS_BIN=$<TARGET_FILE:telega-fs>;BENCH_BIN=$<TARGET_FILE:tgfs-bench>")
