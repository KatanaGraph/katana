#!/usr/bin/env python3

from conans import ConanFile
from conans.model.version import Version


class KatanaConan(ConanFile):
    settings = ("os", "compiler", "build_type", "arch")

    requires = (
        "arrow/2.0.0",
        "backward-cpp/1.5",
        "benchmark/1.5.0",
        "boost/1.71.0",
        "brotli/1.0.9",
        "date/2.4.1",
        "eigen/3.3.7",
        "fmt/6.2.1",
        "libcurl/7.74.0",
        "lz4/1.9.3",
        "nlohmann_json/3.7.3",
        "openssl/1.1.1h",
        "snappy/1.1.8",
        "utf8proc/2.6.0",
        "zstd/1.4.5",
    )

    default_options = {
        "arrow:filesystem_layer": True,
        "arrow:parquet": True,
        "arrow:shared": False,
        "arrow:with_brotli": True,
        "arrow:with_snappy": True,
        "arrow:with_zlib": True,
        # In general, we prefer to support as many compression algorithms as
        # possible to tolerate files created by others. bz2, lz4 and zstd cause
        # some issues with arrow/2.0.0 because arrow's cmake package doesn't
        # find conan's libraries.
        #
        # According to [1], only snappy and gzip are expected to be present.
        #
        # [1] https://arrow.apache.org/docs/r/reference/write_parquet.html
        #
        # "arrow:with_bz2": True,
        # "arrow:with_lz4": True,
        # "arrow:with_zstd": True,
        "date:header_only": True,
        "libcurl:shared": False,
    }

    generators = ("cmake_find_package", "cmake_paths")

    def configure(self):
        if self.settings.os == "Macos":
            self.options["backward-cpp"].stack_details = "backtrace_symbol"

        # Hack to support older distributions that do not have gcc-7
        compiler_version = Version(str(self.settings.compiler.version))
        if self.settings.compiler != "gcc" or compiler_version >= Version("5.3"):
            self.requires.add("libmysqlclient/8.0.17")

    def imports(self):
        self.copy("*.dylib*", dst="/usr/local/lib", src="lib")
