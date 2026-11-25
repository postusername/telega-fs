# E2E на живом Telegram в режиме бота. Подключается только с -DTELEGA_FS_E2E=ON.
# Тесты ходят в сеть, им нужны бот и канал (креды, E2E_CHAT_ID и остальное: шапки скриптов),
# поэтому без TELEGA_FS_E2E_RUN=1 они пропускаются (код 77) и `ctest -LE long` в сеть не ходит.
# Запуск: TELEGA_FS_E2E_RUN=1 ctest --test-dir <build> -L e2e --output-on-failure
# Один тест: добавьте -R e2e_basic, -R e2e_resources или -R e2e_maint.
# Канал можно подставить свой: E2E_CHAT_ID=-100... (бот в нём администратор), тогда аккаунт
# пользователя не нужен.

if(NOT TARGET tgfs-bench)
    message(FATAL_ERROR "TELEGA_FS_E2E needs tgfs-bench: configure with -DTELEGA_FS_BUILD_TOOLS=ON")
endif()

# Базовый сценарий: запись, fsync, повторное монтирование, SIGTERM, чтение с другой «машины», logout.
# Около 15 минут.
add_test(NAME telega_fs.e2e_basic
    COMMAND bash "${CMAKE_CURRENT_SOURCE_DIR}/e2e_basic.sh")
set_tests_properties(telega_fs.e2e_basic PROPERTIES
    LABELS e2e
    TIMEOUT 7200
    SKIP_RETURN_CODE 77
    ENVIRONMENT "E2E_CTEST=1;TGFS_BIN=$<TARGET_FILE:telega-fs>;BENCH_BIN=$<TARGET_FILE:tgfs-bench>")

# Ресурсный сценарий (холодное чтение, видео, файл 10 ГиБ, кэш чтения): 1,5-2 часа, возобновляемый.
# Прогонять лучше напрямую (tests/e2e/e2e_resources.sh, см. шапку), ctest нужен для единообразия.
add_test(NAME telega_fs.e2e_resources
    COMMAND bash "${CMAKE_CURRENT_SOURCE_DIR}/e2e_resources.sh")
set_tests_properties(telega_fs.e2e_resources PROPERTIES
    LABELS "e2e;long"
    TIMEOUT 14400
    SKIP_RETURN_CODE 77
    ENVIRONMENT "E2E_CTEST=1;TGFS_BIN=$<TARGET_FILE:telega-fs>;BENCH_BIN=$<TARGET_FILE:tgfs-bench>")

# Сценарий обслуживания (status и sync на смонтированной ФС, дедупликация, потеря головы и
# --force-local, fsck, gc, gc --compact): около часа, возобновляемый, тоже лучше напрямую
# (tests/e2e/e2e_maint.sh).
add_test(NAME telega_fs.e2e_maint
    COMMAND bash "${CMAKE_CURRENT_SOURCE_DIR}/e2e_maint.sh")
set_tests_properties(telega_fs.e2e_maint PROPERTIES
    LABELS "e2e;long"
    TIMEOUT 10800
    SKIP_RETURN_CODE 77
    ENVIRONMENT "E2E_CTEST=1;TGFS_BIN=$<TARGET_FILE:telega-fs>;BENCH_BIN=$<TARGET_FILE:tgfs-bench>")
