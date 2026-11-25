#pragma once

// FUSE-фронтенд: тонкий слой libfuse3 low-level над Vfs. Каждая операция ядра
// превращается в один вызов Vfs, любое исключение — в fuse_reply_err(to_errno(e)).
//
// Порядок жизни (его выбирает вызывающий, см. app/commands.cpp):
//   Frontend f{vfs, options, on_destroy};
//   f.mount();   // fuse_session_new + обработчики сигналов + монтирование; после возврата
//                // ФС смонтирована, но запросы начнут обслуживаться только в serve()
//   f.serve();   // многопоточный цикл до размонтирования (fusermount3 -u) или SIGINT/SIGTERM
//   f.close();   // размонтирование, если ещё не снято, и уничтожение сессии; вызывает
//                // on_destroy, если ядро не прислало DESTROY само
//
// on_destroy вызывается ровно один раз: из операции destroy (ядро размонтирует — тогда
// fusermount3 -u возвращается только после финального коммита) либо из close().
// Он обязан остановить Vfs и сделать финальный коммит; исключения он не выпускает.

#include <filesystem>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>

#include "vfs/vfs.h"

struct fuse_session;

namespace telega::fuse {

class FuseError : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

struct Options {
    std::filesystem::path mountpoint;
    // Пользовательские опции монтирования (-o), через запятую. Добавляются к умолчаниям
    // default_permissions, fsname=telega-fs, subtype=telega-fs.
    std::string mount_options;
    bool read_only = false;
    // Время жизни записей каталога и атрибутов в ядре, секунды.
    double entry_timeout = 1.0;
    double attr_timeout = 1.0;
};

class Frontend {
  public:
    Frontend(vfs::Vfs& vfs, Options options, std::function<void()> on_destroy);
    Frontend(const Frontend&) = delete;
    Frontend& operator=(const Frontend&) = delete;
    Frontend(Frontend&&) = delete;
    Frontend& operator=(Frontend&&) = delete;
    ~Frontend();

    // Бросает FuseError с текстом libfuse. Повторный вызов — std::logic_error.
    void mount();
    // Цикл запросов. Возвращается после размонтирования или сигнала. Ошибка цикла — FuseError.
    void serve();
    // Размонтировать (ленивое, если занято) и уничтожить сессию. Повторный вызов безопасен.
    void close() noexcept;

    struct Impl;  // непрозрачно; публичен только для C-обратных вызовов в frontend.cpp

  private:
    std::unique_ptr<Impl> impl_;
};

// Последнее сообщение libfuse (для текста ошибки монтирования).
std::string last_libfuse_message();

}  // namespace telega::fuse
