# Сборка

telega-fs собирается под Linux (x86_64 и aarch64) из исходников. Нужны компилятор C++20 (GCC 13+ или Clang 17+), CMake 3.25+ и Ninja. Остальные библиотеки ставятся из пакетов, кроме TDLib, которую собирают отдельно.

## Зависимости

Ubuntu 24.04 и Debian:

```sh
sudo apt install build-essential clang cmake ninja-build pkg-config gperf git \
    libfuse3-dev fuse3 libsodium-dev libzstd-dev libssl-dev zlib1g-dev
```

Для разработки ещё `clang-format-18` и `clang-tidy-18`.

SQLite (с session extension), nlohmann_json, toml++, CLI11, spdlog и GoogleTest CMake скачивает сам при первом configure. Версии и SHA256 архивов зафиксированы в `cmake/deps.cmake`, архивы складываются в `.cache/deps`. Для сборки без сети заполните этот каталог заранее и добавьте `-DFETCHCONTENT_FULLY_DISCONNECTED=ON`.

## TDLib

Нужна ровно TDLib 1.8.70, коммит `ba976cb4aba53a82eff425db2f85e2741774c944`. Пресеты ищут её в `./td/tdlib`, каталог `td/` игнорируется git'ом:

```sh
git clone https://github.com/tdlib/td.git td && cd td
git checkout ba976cb4aba53a82eff425db2f85e2741774c944
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release -DTD_ENABLE_LTO=OFF \
    -DBUILD_TESTING=OFF -DTD_INSTALL_SHARED_LIBRARIES=OFF \
    -DCMAKE_INSTALL_PREFIX:PATH="$PWD/tdlib"
cmake --build build --target install -j4
cd ..
```

- TDLib собирается 20–40 минут и берёт 1,5–3 ГБ памяти на поток компиляции. `-j` выбирайте по свободной памяти, а не по числу ядер: при 6 ГБ свободных это `-j3`.
- Не включайте LTO и `-stdlib=libc++`. Такие архивы содержат биткод LLVM, GCC их не линкует, а libc++ добавляет лишнюю динамическую зависимость.
- Проверить архив: распакуйте `ar x td/tdlib/lib/libtdjson_static.a` во временном каталоге и посмотрите `file *.o`. Должно быть `ELF 64-bit relocatable`, а не `LLVM IR bitcode`.
- TDLib в другом месте (или вы в git worktree, где нет `td/`): `-DTd_DIR=<prefix>/lib/cmake/Td`.

## telega-fs

Бинарник для установки:

```sh
cmake --preset release && cmake --build --preset release
install -m 755 build/release/src/telega-fs ~/.local/bin/
```

Получается один файл `telega-fs`. TDLib в нём статически, динамически он зависит только от libfuse3, libsodium, libzstd, libssl, libcrypto, libz и стандартных библиотек.

## Пресеты

| Пресет | Сборка | Для чего |
|---|---|---|
| `dev` | Debug | разработка, предупреждения не ломают сборку |
| `core` | Debug, без TDLib | быстрый старт: ядро, юнит- и FUSE-тесты на локальном хранилище, без сборки TDLib |
| `ci` | RelWithDebInfo, `-Werror` | как основная сборка CI |
| `asan` | Debug, ASan + UBSan | собирать Clang'ом |
| `tsan` | Debug, TSan | собирать Clang'ом |
| `release` | Release, LTO | установка |

```sh
cmake --preset dev && cmake --build --preset dev && ctest --preset dev -LE long
```

- Компилятор берётся из `CC`/`CXX` и запоминается при первом configure каталога `build/<пресет>`. Сменить: `CC=clang CXX=clang++ cmake --preset dev --fresh` или отдельный каталог `cmake --preset ci -B build/ci-clang`.
- В пресете `core` бинарник `telega-fs` собирается, но работает только с профилями `store = "local"`, а `telega-fs --version` печатает `TDLib none`.
- Опции CMake: `TELEGA_FS_WITH_TDLIB` (ON), `TELEGA_FS_BUILD_TESTS` (ON), `TELEGA_FS_BUILD_TOOLS` (OFF, инструменты экспериментов `tgfs-sim` и `tgfs-bench`), `TELEGA_FS_E2E` (OFF, живые E2E-тесты), `TELEGA_FS_WERROR` (включает пресет `ci`). Инструменты и E2E требуют TDLib: вместе с `TELEGA_FS_WITH_TDLIB=OFF` configure откажет.

## Тесты

| Метка ctest | Что | Сколько идёт |
|---|---|---|
| без метки | юнит-тесты и FUSE-тесты на локальном хранилище | минуты |
| `long` | fsx на 100 тысяч операций, тест ресурсов на 10 ГиБ | 25+ минут, только на свободной машине |
| `e2e` | живой Telegram, нужны `-DTELEGA_FS_E2E=ON`, бот и канал | от 20 минут до 2 часов |

FUSE-тестам нужны `/dev/fuse` и `fusermount3`. Без них тесты пропускаются (`GTEST_SKIP`), а не засчитываются как пройденные.

TSan на ядрах с `vm.mmap_rnd_bits=32` падает при старте с `unexpected memory mapping`. Без root запускайте тесты через `setarch $(uname -m) -R ctest --preset tsan`.
