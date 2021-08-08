#!/usr/bin/python

import argparse
import fnmatch
import io
import multiprocessing
import os
import queue
import subprocess
import sys
import threading

# Default values to be changed by the script
fix: bool = False
prune_paths = set()
clang_format: str = None

# Hard-coded constants
prune_names = {"build*", ".#*"}
file_suffixes = {".cpp", ".h", ".cu", ".cuh"}

# Processing variables
tasks = queue.Queue()


class Consumer:
    def __init__(self):
        self.thread = threading.Thread(target=self.entry)
        self.check_failed = False

    def entry(self):
        while True:
            item = tasks.get()
            if item is None:
                tasks.put(None)
                return
            if fix:
                print("fixing ", item)
                cmd = [clang_format, " -style=file -i ", item]
            else:
                cmd = [clang_format, "-style=file", "-output-replacements-xml", item]

            completed = subprocess.run(cmd, capture_output=True, check=False)
            if not fix and (completed.returncode != 0 or ("<replacement " in str(completed.stdout))):
                self.check_failed = True
                print(item, " NOT OK")


def needs_formatting(file_name: str) -> bool:
    for suffix in file_suffixes:
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
    fix = args.fix
    roots = args.roots[:]

    # Parse environment variables
    clang_format = os.environ.get("CLANG_FORMAT", "clang-format")
    if "PRUNE_PATHS" in os.environ:
        last_token = io.StringIO()
        in_quotes = False
        for ch in os.environ["PRUNE_PATHS"]:
            if ch in ["'", '"']:
                if last_token.tell() != 0:
                    prune_paths.add(last_token.getvalue())
                    last_token = io.StringIO()
                in_quotes = not in_quotes
            elif ch == " ":
                if in_quotes:
                    last_token.write(ch)
                elif last_token.tell() != 0:
                    prune_paths.add(last_token.getvalue())
                    last_token = io.StringIO()
                # else tokens are separated by multiple spaces
            else:
                last_token.write(ch)
        if last_token.tell() != 0:
            prune_paths.add(last_token.getvalue())
            last_token = io.StringIO()

    # Launch consumers
    consumers = []
    for i in range(multiprocessing.cpu_count()):
        cur_cons = Consumer()
        consumers.append(cur_cons)
        cur_cons.thread.start()

    # Traverse the directory trees
    for root in roots:
        for dir_path, dir_names, file_names in os.walk(root):
            to_remove = []
            for dir_name in dir_names:
                removing = False
                for prune_name in prune_names:
                    if fnmatch.fnmatch(dir_name, prune_name):
                        to_remove.append(dir_name)
                        removing = True
                        break
                if removing:
                    continue
                full_path = os.path.join(dir_path, dir_name)
                for prune_path in prune_paths:
                    if fnmatch.fnmatch(full_path, prune_path):
                        to_remove.append(dir_name)
                        break
            for dir_name in to_remove:
                dir_names.remove(dir_name)
            for file_name in file_names:
                pruned = False
                for prune_name in prune_names:
                    if fnmatch.fnmatch(file_name, prune_name):
                        pruned = True
                        break
                if pruned or not needs_formatting(file_name):
                    continue
                tasks.put(os.path.join(dir_path, file_name))

    # Finalize the processing
    tasks.put(None)
    check_failed = False
    for consumer in consumers:
        consumer.thread.join()
        check_failed = check_failed or consumer.check_failed

    # Return proper exit code
    sys.exit(1 if check_failed else 0)
