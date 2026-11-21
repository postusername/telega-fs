#pragma once

// Подкоманды tgfs-bench. Каждая регистрируется на корневом CLI::App; общие опции (--creds,
// --state-dir) лежат в BenchOptions, который заполняет main. Подкоманды экспериментов (E2 и
// далее) добавляются так же: свой файл, add_<имя>_command и строка в main.cpp.

#include <CLI/CLI.hpp>

#include "tdbench.h"

namespace telega::bench {

// Код возврата подкоманды кладётся в *exit_code.
void add_login_command(CLI::App& app, const BenchOptions& options, int* exit_code);
void add_whoami_command(CLI::App& app, const BenchOptions& options, int* exit_code);
void add_logout_command(CLI::App& app, const BenchOptions& options, int* exit_code);
void add_create_bot_command(CLI::App& app, const BenchOptions& options, int* exit_code);
// Эксперименты E2 (закрепление в режиме бота), E3 (чтение старых сообщений по id) и E4
// (скачивание диапазонов), SPEC 17.5-17.7.
void add_e2_command(CLI::App& app, const BenchOptions& options, int* exit_code);
void add_e3_command(CLI::App& app, const BenchOptions& options, int* exit_code);
void add_e4_command(CLI::App& app, const BenchOptions& options, int* exit_code);
// Эксперимент E5 (лимиты отправки, SPEC 17.8).
void add_e5_command(CLI::App& app, const BenchOptions& options, int* exit_code);
// Эксперимент E6 (альбомы, SPEC 17.9).
void add_e6_command(CLI::App& app, const BenchOptions& options, int* exit_code);
// Эксперимент E7 (размер чанка, SPEC 17.10).
void add_e7_command(CLI::App& app, const BenchOptions& options, int* exit_code);
// Эксперимент E9 (холодное чтение из пакетов, SPEC 17.12): сетевая часть, офлайн-часть — tgfs-sim.
void add_e9_command(CLI::App& app, const BenchOptions& options, int* exit_code);

// Настройка и уборка канала для E2E telega-fs в режиме бота (SPEC 14).
void add_e2e_command(CLI::App& app, const BenchOptions& options, int* exit_code);

// Общий разбор `--as user|bot`.
CLI::Option* add_account_option(CLI::App& cmd, Account* account);

}  // namespace telega::bench
