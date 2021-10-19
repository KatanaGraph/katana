#!/usr/bin/env python3
import os
import sys

from conans import ConanFile
from conans.model.version import Version

# TODO(amp): Replace with normal import once we have a script utilities python project in this repo.
# Add the project root to the python path just in case.
conan_file_path = os.path.dirname(os.path.dirname(__file__))
sys.path.append(conan_file_path)

from scripts import katana_requirements


class KatanaConan(ConanFile):
    settings = ("os", "compiler", "build_type", "arch")

    requires = tuple(katana_requirements.package_list(["conan"], katana_requirements.OutputFormat.CONAN))

    default_options = {
        "jemalloc:enable_prof": True,
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
