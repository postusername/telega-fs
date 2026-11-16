include(GoogleTest)

add_executable(telega_unit_tests
    unit/sqlite_test.cpp
    unit/deps_test.cpp)
target_link_libraries(telega_unit_tests PRIVATE telega_core GTest::gtest_main)
gtest_discover_tests(telega_unit_tests DISCOVERY_MODE PRE_TEST)

target_compile_definitions(telega_unit_tests PRIVATE
    TELEGA_FS_EXPECTED_VERSION="${PROJECT_VERSION}")

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
