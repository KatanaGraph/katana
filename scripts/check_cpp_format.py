#!/usr/bin/env python3

import argparse
import multiprocessing
import os
import subprocess
import sys
from concurrent import futures
from pathlib import Path


def check_file(file_path: Path, fix: bool, clang_format: str, verbose: bool):
    """
    Run `clang_format` on `file_path`.
    :param file_path: The path to the file.
    :param fix: Should we fix the file if it's wrong.
    :param clang_format: The name of the ``clang-format`` command.
    :return: True if there were no errors, or all errors were fixed.
    """
    if fix:
        print("fixing ", file_path)
        cmd = [clang_format, "-style=file", "-i", file_path]
    else:
        cmd = [clang_format, "-style=file", "-output-replacements-xml", file_path]

    if verbose:
        print(f"Running: {cmd}")
    completed = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
    if not fix and (completed.returncode != 0 or ("<replacement " in str(completed.stdout))):
        print(file_path, " NOT OK")
        return False
    return True


if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Check or fix C++/CUDA code style.")
    parser.add_argument(
        "--fix",
        "-fix",
        "-f",
        default=False,
        action="store_true",
        help="Fix the sources instead of just checking their formatting.",
    )
    parser.add_argument(
        "--verbose", "-v", default=False, action="store_true", help="Verbosely print what I am doing.",
    )
    parser.add_argument(
        "--exclude",
        default=[],
        action="append",
        help="Add a pattern to exclude from processing. The pattern can be a path or a glob.",
    )
    parser.add_argument("roots", type=str, nargs="+", help="The root paths to search from, or files to check.")
    args = parser.parse_args()

    clang_format = os.environ.get("CLANG_FORMAT")

    if not clang_format:
        # If the user doesn't specify, try clang-format-12 and then clang-format.
        # Trying the specific version first makes sure we get the correct version even if clang-format points to a
        # newer version.
        clang_format_names = ["clang-format-12", "clang-format"]
        for candidate in clang_format_names:
            try:
                subprocess.check_call([candidate, "--version"])
                clang_format = candidate
                break
            except subprocess.CalledProcessError:
                continue

    exclude_paths = set(args.exclude)
    # For backwards compat check an old environment variable.
    exclude_paths.update(os.environ.get("PRUNE_PATHS", "").split(":"))
    exclude_paths.update({"build*", ".#*", "*-build-*"})

    # Empty strings can come from user input. Drop them.
    exclude_paths.discard("")

    def is_excluded(f):
        # Check the patterns against the path, the absolute, path and every prefix of the absolute path.
        paths = [f, f.resolve()] + list(f.resolve().parents)
        for path_to_check in paths:
            for pp in exclude_paths:
                if path_to_check.match(pp):
                    if args.verbose:
                        print(f"Excluding path {f} ({path_to_check}) with pattern {pp}")
                    return True
        return False

    file_suffixes = {"cpp", "h", "cu", "cuh"}

    tasks = []

    # We are definitely IO bound so use double the number of CPUs.
    with futures.ThreadPoolExecutor(max_workers=multiprocessing.cpu_count() * 2) as executor:
        for root_str in args.roots:
            root = Path(root_str)
            # If the root is excluded, don't even look in it.
            if not is_excluded(root):
                for suffix in file_suffixes:
                    suffix_glob = f"*.{suffix}"
                    # Check if the root IS a file that matches this suffix
                    if root.is_file() and root.match(suffix_glob):
                        tasks.append(executor.submit(check_file, root, args.fix, clang_format, args.verbose))
                    else:
                        # Otherwise, iterate over all files matching the suffix in the subtree.
                        for file in root.rglob(suffix_glob):
                            if file.is_file() and not is_excluded(file):
                                tasks.append(executor.submit(check_file, file, args.fix, clang_format, args.verbose))
        results = futures.as_completed(tasks)
        all_files_ok = all(f.result() for f in results)
    sys.exit(0 if all_files_ok else 1)
