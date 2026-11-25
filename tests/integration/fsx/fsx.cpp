// tgfs-fsx: стресс-тест файловой системы по модели fsx из xfstests, см. fsx.h.

#include "fsx.h"

#include <exception>
#include <iostream>
#include <span>
#include <string>
#include <vector>

int main(int argc, char** argv) {
    try {
        const std::span<char*> raw(argv, static_cast<std::size_t>(argc));
        const std::vector<std::string> args(raw.begin() + 1, raw.end());
        return telega::fsx::fsx_main(args, std::cout, std::cerr);
    } catch (const std::exception& e) {
        std::cerr << "tgfs-fsx: " << e.what() << '\n';
        return telega::fsx::kExitUsage;
    }
}
