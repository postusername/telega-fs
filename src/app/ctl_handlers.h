#pragma once

// Команды управляющего сокета `status`, `sync`, `snapshot` поверх смонтированной
// ФС. Отдельно от ctl_socket.h: сервер ничего не знает про Vfs и коммит.

#include "app/config.h"
#include "app/ctl_proto.h"
#include "meta/mount.h"
#include "vfs/vfs.h"

namespace telega::app {

// vfs и mounted должны пережить обработчики (сервер уничтожается раньше них). read_only_mount —
// монтирование с --ro: `sync` там успешен, а `snapshot` отказывает.
ctl::Handlers make_mount_handlers(vfs::Vfs& vfs, meta::MountedFs& mounted,
                                  const ProfilePaths& paths, bool read_only_mount);

}  // namespace telega::app
