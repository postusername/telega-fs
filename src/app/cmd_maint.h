#pragma once

// Команды status, sync и gc (SPEC 12.3). `status` и `sync` говорят со смонтированной ФС через
// управляющий сокет (SPEC 12.4), `gc` работает без монтирования под `lock`. Эта библиотека не
// линкуется с TDLib (SPEC 15.4): для store = "telegram" транспорт приходит из main через
// set_transport_factory (app/tg_session.h).

#include "app/commands.h"

namespace telega::app {

struct StatusArgs {
    GlobalArgs global;
    bool json = false;  // --json: ответ управляющего сокета как есть
};

struct SyncArgs {
    GlobalArgs global;
};

struct GcArgs {
    GlobalArgs global;
    bool compact = false;  // --compact: ещё и уплотнение пакетов (SPEC 6.5)
};

// Возвращают код выхода процесса: 0 — успех. Сообщения об ошибках — в stderr.
int run_status(const StatusArgs& args);
int run_sync(const SyncArgs& args);
int run_gc(const GcArgs& args);

}  // namespace telega::app
