#!/usr/bin/env python3

from conans import ConanFile
from conans.model.version import Version


class KatanaConan(ConanFile):
    settings = ("os", "compiler", "build_type", "arch")

    # Several packages are installed via APT:
    #  - arrow
    #  - llvm
    requires = (
        "backward-cpp/1.5",
        "benchmark/1.5.0",
        "boost/1.74.0",
        "eigen/3.3.7",
        "fmt/6.2.1",
        "libcurl/7.74.0",
        "nlohmann_json/3.7.3",
        "openssl/1.1.1h",
    )

    default_options = {
        "libcurl:shared": False,
    }

    generators = ("cmake_find_package", "cmake_paths", "pkg_config")

    def configure(self):
        if self.settings.os == "Macos":
            self.options["backward-cpp"].stack_details = "backtrace_symbol"

        # Hack to support older distributions that do not have gcc-7
        compiler_version = Version(str(self.settings.compiler.version))
        if self.settings.compiler != "gcc" or compiler_version >= Version("5.3"):
            self.requires.add("libmysqlclient/8.0.17")

    def imports(self):
        self.copy("*.dylib*", dst="/usr/local/lib", src="lib")
