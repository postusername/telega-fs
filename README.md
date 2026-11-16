# telega-fs

FUSE-файловая система для Linux поверх приватного Telegram-канала. Чанки шифруются на клиенте, индекс хранится в закреплённом сообщении.

**Статус:** только спецификация, кода пока нет. Всё устройство описано в [docs/SPEC.md](docs/SPEC.md).

> Telegram не гарантирует хранение. Канал или аккаунт могут пропасть, поэтому telega-fs не заменяет резервную копию. Рекомендуемый режим — бот-аккаунт в канале, который принадлежит вам, со своим `api_id` (см. SPEC, разделы 10.3 и 13).

## Зависимости для разработки

Ubuntu и Debian:

```sh
sudo apt install build-essential clang cmake ninja-build pkg-config gperf \
    libfuse3-dev fuse3 libsodium-dev libzstd-dev libssl-dev zlib1g-dev
```

SQLite (с session extension), nlohmann_json, toml++, CLI11, spdlog и GoogleTest CMake скачивает сам через `FetchContent`, версии зафиксированы в `cmake/deps.cmake` (SPEC, раздел 15).

### TDLib

Используется TDLib 1.8.70 (коммит `ba976cb4a`). Собирается отдельно, в репозиторий не входит:

```sh
git clone https://github.com/tdlib/td.git && cd td
git checkout ba976cb4a
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX:PATH=../tdlib
cmake --build build --target install -j"$(nproc)"   # TDLib прожорлив: около 2–3 ГБ RAM на поток
```

При сборке telega-fs указать `-DTd_DIR=<путь>/tdlib/lib/cmake/Td`. Локальная копия в `./td/` игнорируется git'ом.

## Учётные данные

`api_id` и `api_hash` получаются на <https://my.telegram.org> и хранятся в `~/.config/telega-fs/config.toml` (или в `TELEGA_API_ID` / `TELEGA_API_HASH`). В репозиторий не коммитятся.
