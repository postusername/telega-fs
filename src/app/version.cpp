#include "app/version.h"

#include <fuse_lowlevel.h>
#include <sodium.h>
#include <spdlog/fmt/fmt.h>
#include <spdlog/version.h>
#include <sqlite3.h>
#include <zstd.h>

#include "telega/build_info.h"

namespace telega::app {

std::string_view version() noexcept { return TELEGA_FS_VERSION; }

std::string core_library_versions() {
    return fmt::format("SQLite {}\nlibsodium {}\nzstd {}\nlibfuse {}\nspdlog {}.{}.{}\n",
                       sqlite3_libversion(), sodium_version_string(), ZSTD_versionString(),
                       fuse_pkgversion(), SPDLOG_VER_MAJOR, SPDLOG_VER_MINOR, SPDLOG_VER_PATCH);
}

}  // namespace telega::app
