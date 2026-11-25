#pragma once

// Сервер управляющего сокета `<data>/ctl.sock` и общие помощники для клиента.
//
// Устройство:
// - сокет создаёт процесс монтирования ПОСЛЕ захвата `lock`: оставшийся от упавшего
//   процесса файл сокета при этом безопасно удаляется. Права сокета 0600, каталог data уже 0700
//   (ensure_profile_dirs). Файл удаляется в begin_stop (начало размонтирования) и в деструкторе;
// - один поток принимает соединения (poll по слушающему сокету и каналу пробуждения), для
//   каждого клиента — свой короткоживущий поток: `sync` может ждать минуты, а ни FUSE-потоки,
//   ни другие клиенты ждать его не должны. Число одновременных клиентов ограничено;
// - при каждом подключении проверяется SO_PEERCRED: uid собеседника должен совпасть с uid
//   процесса telega-fs, иначе соединение закрывается без ответа;
// - размонтирование сокет не вешает: begin_stop() закрывает слушающий сокет и сразу отвечает
//   `shutting_down` на новые запросы, ждущие запроса клиенты уходят за ≤100 мс. Обработчик,
//   уже выполняющий `sync`, прерывается остановкой Vfs (Vfs::shutdown будит ожидание заливки),
//   а деструктор после этого присоединяет его поток. Деструктор ДОЛЖЕН быть вызван раньше
//   уничтожения объектов, на которые ссылаются обработчики.
//
// Ничего не знает ни про Vfs, ни про MetaStore: команды приходят обработчиками (ctl_proto.h).

#include <sys/types.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <filesystem>
#include <list>
#include <memory>
#include <mutex>
#include <thread>

#include "app/ctl_proto.h"
#include "common/files.h"

namespace telega::app {

// Принимаем собеседника только с тем же uid, что у процесса.
[[nodiscard]] bool peer_uid_allowed(uid_t peer, uid_t allowed) noexcept;

struct CtlOptions {
    // Чей uid допускается; по умолчанию — uid процесса. Тесты ставят чужой, чтобы проверить
    // закрытие соединения.
    uid_t allowed_uid;
    // Сколько клиентов обслуживается одновременно; лишние получают `busy`.
    size_t max_clients = 16;
    // Сколько клиент может молчать, не присылая строку запроса.
    std::chrono::milliseconds request_timeout{10'000};

    CtlOptions();
};

class CtlServer {
  public:
    CtlServer(std::filesystem::path path, ctl::Handlers handlers, CtlOptions options = {});
    CtlServer(const CtlServer&) = delete;
    CtlServer& operator=(const CtlServer&) = delete;
    CtlServer(CtlServer&&) = delete;
    CtlServer& operator=(CtlServer&&) = delete;
    // begin_stop() и присоединение потоков клиентов.
    ~CtlServer();

    // Удалить оставшийся сокет (только если это сокет), создать новый с правами 0600 и начать
    // принимать соединения. Ошибка — SysError. Зовётся один раз.
    void start();
    // Начало размонтирования: больше не принимать, убрать файл сокета, новые запросы получают
    // `shutting_down`. Идемпотентно, не ждёт идущие команды.
    void begin_stop() noexcept;

    [[nodiscard]] const std::filesystem::path& path() const { return path_; }

  private:
    struct Client {
        std::thread thread;
        std::atomic<bool> done{false};
    };

    void accept_loop();
    void serve_client(Fd fd);
    void reap_finished();
    [[nodiscard]] bool stopping() const { return stopping_.load(); }

    std::filesystem::path path_;
    ctl::Handlers handlers_;
    CtlOptions opts_;

    Fd listen_fd_;
    Fd wake_read_;
    Fd wake_write_;
    std::thread accept_thread_;
    std::atomic<bool> stopping_{false};
    bool bound_ = false;  // файл сокета создан нами (start прошёл); пишет start, читает begin_stop
    std::once_flag stop_once_;

    std::mutex mu_;  // clients_
    std::list<std::unique_ptr<Client>> clients_;
};

// Подключиться к сокету path. Ошибка — SysError (err() = ENOENT, ECONNREFUSED и т. д.).
// Пути длиннее sockaddr_un::sun_path (108 байт) обслуживаются через /proc/self/fd.
Fd connect_unix(const std::filesystem::path& path);

}  // namespace telega::app
