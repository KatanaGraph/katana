#!/usr/bin/env python3

import argparse
import multiprocessing
import os
import subprocess
import sys
from concurrent import futures
from pathlib import Path
from typing import List


def run_cmd(cmd, verbose):
    if verbose:
        print(f"Running: {cmd}")
    completed = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False, encoding="UTF-8")
    return completed


if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Check or fix Python like code.")
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
        "--pydocstyle", default=False, action="store_true", help="Run pydocstyle and other explicitly requested commands.",
    )
    parser.add_argument(
        "--pylint", default=False, action="store_true", help="Run pylint and other explicitly requested commands.",
    )
    parser.add_argument(
        "--black", default=False, action="store_true", help="Run black and other explicitly requested commands.",
    )
    parser.add_argument(
        "--isort", default=False, action="store_true", help="Run isort and other explicitly requested commands.",
    )
    parser.add_argument(
        "--no-versions", default=False, action="store_true", help="Don't print tool versions.",
    )
    parser.add_argument(
        "--exclude",
        default=[],
        action="append",
        help="Add a pattern to exclude from processing. The pattern can be a path or a glob.",
    )
    parser.add_argument("roots", type=str, nargs="+", help="The root paths to search from, or files to check.")
    args = parser.parse_args()

    if not args.pydocstyle and not args.pylint and not args.black and not args.isort:
        args.pydocstyle = True
        args.pylint = True
        args.black = True
        args.isort = True

    exclude_paths = set(args.exclude)
    # For backwards compat check an old environment variable.
    exclude_paths.update(os.environ.get("PRUNE_PATHS", "").split(":"))
    exclude_paths.update({"build*", ".#*", "*-build-*", ".git"})

    # Empty strings can come from user input. Drop them.
    exclude_paths.discard("")

    file_suffixes = {"py", "pyx", "pxd", "pyi"}
    black_unsupported_suffixes = {"pyx", "pxd"}
    pylint_unsupported_suffixes = {"pyx", "pxd"}

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

    git_root = str(os.environ.get("GIT_ROOT", Path(__file__).parent.parent))
    black_cmd = [os.environ.get("BLACK", "black"), f"--config={git_root}/pyproject.toml"]
    black_check_cmd = black_cmd + ["--check"]
    isort_cmd = [os.environ.get("ISORT", "isort"), f"--settings-file={git_root}/pyproject.toml"]
    isort_check_cmd = isort_cmd + ["--diff", "--check"]
    lint_cmd = [os.environ.get("PYLINT", "pylint"), "-j", "0", f"--rcfile={git_root}/pyproject.toml"]
    pydocstyle_cmd = [os.environ.get("PYDOCSTYLE", "pydocstyle"), f"--config={git_root}/pyproject.toml"]

    if args.black and not args.no_versions:
        subprocess.check_call([os.environ.get("BLACK", "black"), "--version"])
    if args.isort and not args.no_versions:
        subprocess.check_call([os.environ.get("ISORT", "isort"), "--version"])
    if args.pylint and not args.no_versions:
        subprocess.check_call([os.environ.get("PYLINT", "pylint"), "--version"])
    if args.pydocstyle and not args.no_versions:
        subprocess.check_call([os.environ.get("PYDOCSTYLE", "pydocstyle"), "--version"])

    def check_file(file_path: Path, cmd_prefix: List[str], fix: bool, verbose: bool):
        if fix:
            print("fixing ", file_path)

        completed = run_cmd(cmd_prefix + [str(file_path)], verbose)
        if not fix and completed.returncode != 0:
            print(file_path, f" NOT OK\n{completed.stderr}\n{completed.stdout}")
            return False
        return True

    tasks = []

    def add_tasks_for_file(tasks, executor, file_name):
        use_black = file_name.suffix[1:] not in black_unsupported_suffixes
        if args.fix:
            if use_black and args.black:
                tasks.append(executor.submit(check_file, file_name, black_cmd, args.fix, args.verbose))
            if args.isort:
                tasks.append(executor.submit(check_file, file_name, isort_cmd, args.fix, args.verbose))
        else:
            if use_black and args.black:
                tasks.append(executor.submit(check_file, file_name, black_check_cmd, args.fix, args.verbose))
            if args.isort:
                tasks.append(executor.submit(check_file, file_name, isort_check_cmd, args.fix, args.verbose))
        if file_name.suffix[1:] not in pylint_unsupported_suffixes and args.pylint:
            tasks.append(executor.submit(check_file, file_name, lint_cmd, args.fix, args.verbose))
        if args.pydocstyle:
            tasks.append(executor.submit(check_file, file_name, pydocstyle_cmd, args.fix, args.verbose))

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
                        add_tasks_for_file(tasks, executor, root)
                    else:
                        # Otherwise, iterate over all files matching the suffix in the subtree.
                        for file in root.rglob(suffix_glob):
                            if file.is_file() and not is_excluded(file):
                                add_tasks_for_file(tasks, executor, file)
        results = futures.as_completed(tasks)
        all_files_ok = all(f.result() for f in results)
    sys.exit(0 if all_files_ok else 1)
