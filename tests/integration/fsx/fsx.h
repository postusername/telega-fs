#pragma once

// Собственная реализация fsx по модели fsx из xfstests.
//
// Над одним файлом выполняется случайная, но воспроизводимая по seed последовательность операций:
// read, write, truncate, mmap-чтение, mmap-запись, fsync, закрытие и повторное открытие. Рядом
// в памяти живёт модель файла; каждое чтение и периодическая полная сверка сравнивают файл с
// моделью. При расхождении печатаются окно байтов, журнал последних операций и seed, а модель и
// фактическое содержимое сохраняются в FILE.fsxgood и FILE.fsxbad.
//
// Режим --checkpoint/--resume нужен для прогона с перемонтированием: первый запуск после K
// операций сохраняет состояние (генератор, номер операции, модель, журнал) и выходит, второй
// запуск сверяет файл целиком и продолжает с той же операции.
//
// Логика вынесена в библиотеку, чтобы её можно было вызывать и из GoogleTest-обёрток.

#include <array>
#include <cstddef>
#include <cstdint>
#include <iosfwd>
#include <string>
#include <vector>

namespace telega::fsx {

enum class OpType : std::uint8_t {
    kRead,
    kWrite,
    kMapRead,
    kMapWrite,
    kTruncate,
    kFsync,
    kReopen,
};
inline constexpr std::size_t kOpTypeCount = 7;

// Коды выхода: 0 — без расхождений, 1 — расхождение с моделью или сбой проверяемой ФС,
// 2 — ошибка использования (аргументы, файл состояния, не удалось создать файл).
inline constexpr int kExitOk = 0;
inline constexpr int kExitFailure = 1;
inline constexpr int kExitUsage = 2;

// Параметры, от которых зависит последовательность операций. При --resume они берутся из файла
// состояния; явно заданное в командной строке значение обязано совпадать с сохранённым.
enum ParamBit : std::uint8_t {
    kParamSeed = 1U << 0U,
    kParamMaxLen = 1U << 1U,
    kParamMaxOp = 1U << 2U,
    kParamOps = 1U << 3U,
    kParamNumOps = 1U << 4U,
};

struct Options {
    std::string path;
    std::uint64_t num_ops = 10000;
    std::uint64_t seed = 1;
    std::uint64_t max_len = 256ULL * 1024;
    std::uint64_t max_op = 64ULL * 1024;
    std::array<bool, kOpTypeCount> enabled{true, true, true, true, true, true, true};
    std::uint64_t progress_every = 0;  // 0 — без прогресса
    bool quiet = false;
    bool verbose = false;
    bool help = false;
    std::size_t log_len = 100;
    std::uint64_t full_check_every = 1000;  // 0 — только в конце
    std::uint64_t checkpoint_at = 0;        // 0 — без контрольной точки
    bool resume = false;
    std::string state_path;  // пусто — FILE.fsxstate
    std::string dump_dir;    // пусто — рядом с FILE
    std::uint64_t inject_corruption_at = 0;  // 0 — не портить (самопроверка инструмента)
    std::uint32_t explicit_params = 0;  // ParamBit, заданные в командной строке
};

// Разбор аргументов без имени программы. При ошибке бросает std::invalid_argument.
Options parse_args(const std::vector<std::string>& args);

void print_usage(std::ostream& out);

// Прогон с готовыми параметрами. Возвращает код выхода.
int run(const Options& opts, std::ostream& out, std::ostream& err);

// Разбор аргументов и прогон, как в main(). Исключения не выпускает.
int fsx_main(const std::vector<std::string>& args, std::ostream& out, std::ostream& err);

}  // namespace telega::fsx
