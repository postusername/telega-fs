# telega-fs

FUSE-файловая система для Linux поверх приватного Telegram-канала. Чанки шифруются на клиенте, индекс хранится в закреплённом сообщении.

**Статус:** веха M2 — работа с Telegram. ФС монтируется поверх канала в режиме бота: вход, `init`, запись и чтение, повторное монтирование на другой машине проверены вживую (E2E). Параметры (размер чанка, лимиты отправки, пакеты мелких файлов) выбраны по экспериментам E1–E9. Чтение пока медленное (около 1 МиБ/с, без readahead) — это веха M3. Всё устройство описано в [docs/SPEC.md](docs/SPEC.md).

> Telegram не гарантирует хранение. Канал или аккаунт могут пропасть, поэтому telega-fs не заменяет резервную копию. Рекомендуемый режим — бот-аккаунт в канале, который принадлежит вам, со своим `api_id` (см. SPEC, разделы 10.3 и 13).

## Зависимости для разработки

Ubuntu и Debian:

```sh
sudo apt install build-essential clang cmake ninja-build pkg-config gperf \
    libfuse3-dev fuse3 libsodium-dev libzstd-dev libssl-dev zlib1g-dev \
    clang-format-18 clang-tidy-18
```

SQLite (с session extension), nlohmann_json, toml++, CLI11, spdlog и GoogleTest CMake скачивает сам через `FetchContent`, версии зафиксированы в `cmake/deps.cmake` (SPEC, раздел 15).

### TDLib

Используется TDLib 1.8.70 (коммит `ba976cb4aba53a82eff425db2f85e2741774c944`). Собирается отдельно, в репозиторий не входит:

```sh
git clone https://github.com/tdlib/td.git && cd td
git checkout ba976cb4aba53a82eff425db2f85e2741774c944
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release -DTD_ENABLE_LTO=OFF \
    -DBUILD_TESTING=OFF -DTD_INSTALL_SHARED_LIBRARIES=OFF \
    -DCMAKE_INSTALL_PREFIX:PATH="$PWD/tdlib"
# TDLib прожорлив: 1,5–3 ГБ RAM на поток, -j выбирайте по свободной памяти, а не по ядрам
cmake --build build --target install -j4
```

Не включайте LTO и `-stdlib=libc++`: такие архивы не линкуются GCC (SPEC 15.3).

Пресеты ищут TDLib в `./td/tdlib` (клон TDLib прямо в `./td`, каталог игнорируется git'ом). Другой путь задаётся через `-DTd_DIR=<prefix>/lib/cmake/Td`.

## Сборка

```sh
cmake --preset dev && cmake --build --preset dev && ctest --preset dev
```

| Пресет | Сборка |
|---|---|
| `dev` | Debug, для разработки |
| `ci` | RelWithDebInfo, `-Werror` |
| `asan` | Debug, ASan + UBSan |
| `tsan` | Debug, TSan |
| `release` | Release, LTO |

Компилятор выбирается через `CC`/`CXX` и запоминается при первом configure каталога `build/<пресет>`. Чтобы сменить его, пересоздайте каталог (`CC=clang CXX=clang++ cmake --preset dev --fresh`) или соберите в отдельный: `cmake --preset dev -B build/dev-clang`. Проверки как в CI: `tools/ci/check-format.sh` и `tools/ci/run-clang-tidy.sh build/dev`.

## Быстрый старт (режим бота)

1. Создайте бота у @BotFather, приватный канал и добавьте бота в канал администратором с правами публиковать, редактировать и удалять сообщения.
2. Пропишите `api_id`, `api_hash` и профиль в `~/.config/telega-fs/config.toml` (SPEC 12.2).
3. Дальше:

```sh
telega-fs login --bot            # токен бота; при первом входе — пароль (или keyfile) для ключей
telega-fs init --chat <id канала>
telega-fs mount ~/tg             # размонтирование: fusermount3 -u ~/tg
```

Потеря пароля или keyfile означает потерю данных. Вход пользователем: `telega-fs login` (код или `--qr`), только с отдельным аккаунтом (SPEC 10.3).

## Эксперименты

Параметры, которые нельзя выбрать без замеров, фиксируются экспериментами (SPEC, раздел 17). Эксперименты E1–E9 проведены, отчёты и сырые данные лежат в [docs/experiments/](docs/experiments/), инструменты — в `tools/bench/` (сборка с `-DTELEGA_FS_BUILD_TOOLS=ON`). Сетевые эксперименты и E2E идут на боевых серверах с ботом (тестовый DC недоступен), E2E запускается вручную: `tests/e2e/e2e_bot.sh`.

## Учётные данные

`api_id` и `api_hash` получаются на <https://my.telegram.org> и хранятся в `~/.config/telega-fs/config.toml` (или в `TELEGA_API_ID` / `TELEGA_API_HASH`). В репозиторий не коммитятся.
