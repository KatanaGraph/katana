#!/usr/bin/python

import argparse
import os
import subprocess

from katana_traversal.traversal import FileVisitor, Launcher, Pivot

# Hard-coded constants
prune_names = {"build*", ".#*"}


class CppCudaVisitor(FileVisitor):
    file_suffixes = {".cpp", ".h", ".cu", ".cuh"}

    def __init__(self, fix: bool, clang_format: str):
        super().__init__()
        self.fix = fix
        self.clang_format = clang_format

    def visit(self, file_path: str) -> bool:
        if self.fix:
            print("fixing ", file_path)
            cmd = [self.clang_format, " -style=file -i ", file_path]
        else:
            cmd = [self.clang_format, "-style=file", "-output-replacements-xml", file_path]

        completed = subprocess.run(cmd, capture_output=True, check=False)
        if not self.fix and (completed.returncode != 0 or ("<replacement " in str(completed.stdout))):
            print(file_path, " NOT OK")
            return False  # Check failed
        return True

    def needs_visiting(self, file_name: str) -> bool:
        for suffix in CppCudaVisitor.file_suffixes:
            if file_name.endswith(suffix):
                return True
        return False


if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Check or fix C++/CUDA code style.")
    parser.add_argument(
        "-fix",
        dest="fix",
        default=False,
        action="store_true",
        help="fix the sources instead of just checking their formatting",
    )
    parser.add_argument("roots", type=str, nargs="+", help="the root paths to search from")
    args = parser.parse_args()

    # Init and launch traversal
    visitor = CppCudaVisitor(args.fix, os.environ.get("CLANG_FORMAT", "clang-format"))
    pivot = Pivot(visitor, args.roots[:], prune_names)
    launcher = Launcher(pivot)
    launcher.run_to_exit()
