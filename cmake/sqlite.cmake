# Своя статическая SQLite из amalgamation (SPEC 15.2): session extension для дельта-журнала
# и одна версия формата патчей и VACUUM INTO на всех машинах. TDLib несёт свою копию
# с символами tdsqlite3_*, поэтому конфликтов при линковке нет.

# В архиве нет CMakeLists.txt, поэтому MakeAvailable только распаковывает его.
FetchContent_MakeAvailable(sqlite_amalgamation)

add_library(telega_sqlite STATIC "${sqlite_amalgamation_SOURCE_DIR}/sqlite3.c")
add_library(telega::sqlite ALIAS telega_sqlite)

target_include_directories(telega_sqlite SYSTEM PUBLIC "${sqlite_amalgamation_SOURCE_DIR}")

# Эти флаги меняют видимый API sqlite3.h, поэтому они PUBLIC.
target_compile_definitions(telega_sqlite PUBLIC
    SQLITE_ENABLE_SESSION
    SQLITE_ENABLE_PREUPDATE_HOOK
    SQLITE_OMIT_DEPRECATED)

target_compile_definitions(telega_sqlite PRIVATE
    SQLITE_THREADSAFE=2
    SQLITE_DQS=0
    SQLITE_DEFAULT_MEMSTATUS=0
    SQLITE_OMIT_LOAD_EXTENSION
    SQLITE_LIKE_DOESNT_MATCH_BLOBS)

set_target_properties(telega_sqlite PROPERTIES
    POSITION_INDEPENDENT_CODE ON
    C_STANDARD 11)

target_link_libraries(telega_sqlite PUBLIC Threads::Threads m)
