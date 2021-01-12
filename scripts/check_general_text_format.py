#!/usr/bin/env python
#
# check_general_text_format.py [-fix] <file or directory>...
#
# Check and optionally fix general text formatting in files. Currently just whitespace
# at end of lines and new lines at end of file.

from __future__ import print_function

import argparse
import os
import re
import sys
import tempfile
import shutil

FILENAME_RE = re.compile(
    r"""
    (
        \.(
            c|cxx|cc|C|cpp|cu|  # C/C++ source
            h|cuh|             # C/C++ headers
            py|pyx|pxd|jinja|  # Python/Cython and related
            yaml|              # Config
            txt|md|rst|        # Mark up (and CMakeLists.txt)
            tf|                # Terraform
            sh|                # Shell scripts
            cmake              # CMake modules
        )
        (\.in)?                # Allow .in files
        $
    )
    |
    (
        ^README                # README with odd suffix
    )
    """,
    re.VERBOSE,
)

TRAILING_WHITESPACE_RE = re.compile(r"[\t ]+$", re.MULTILINE)
NO_TRAILING_NEWLINE_RE = re.compile(r"(?<=[^\n])\Z", re.MULTILINE)


def run_check(filename):
    try:
        with open(filename, "rt") as f:
            contents = f.read()
            has_trailing_whitespace = TRAILING_WHITESPACE_RE.search(contents)
            missing_trailing_newline = NO_TRAILING_NEWLINE_RE.search(contents)
            if has_trailing_whitespace:
                print("{filename}: Trailing white space.".format(filename=filename), file=sys.stderr)
            if missing_trailing_newline:
                print("{filename}: Missing newline at end of file.".format(filename=filename), file=sys.stderr)

            return has_trailing_whitespace or missing_trailing_newline
    except UnicodeDecodeError:
        # Ignore any binary files.
        return False
    except FileNotFoundError:
        print("{filename} does not exist.".format(filename=filename), file=sys.stderr)
        return False


def run_fix(filename):
    try:
        with open(filename, "rt") as f:
            contents = f.read()
            contents, n_trailing_whitespace_fixes = TRAILING_WHITESPACE_RE.subn("", contents)
            contents, n_trailing_newline_fixes = NO_TRAILING_NEWLINE_RE.subn("\n", contents, count=1)

            if n_trailing_whitespace_fixes:
                print(
                    "{filename}: Fixed {n_trailing_whitespace_fixes} trailing white space error(s).".format(
                        filename=filename, n_trailing_whitespace_fixes=n_trailing_whitespace_fixes
                    ),
                    file=sys.stderr,
                )
            if n_trailing_newline_fixes:
                print("{filename}: Fixed missing newline at end of file.".format(filename=filename), file=sys.stderr)

            if not n_trailing_whitespace_fixes and not n_trailing_newline_fixes:
                return False
    except UnicodeDecodeError:
        # Ignore any binary files.
        return False
    except FileNotFoundError:
        print("{filename} does not exist.".format(filename=filename), file=sys.stderr)
        return False

    with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
        assert contents, "It should not be possible to write out and empty file."
        f.write(contents)
    shutil.copystat(filename, f.name)
    shutil.move(f.name, filename)

    return False


def main(files, fix):
    if fix:
        process = run_fix
    else:
        process = run_check

    any_errors = False
    for name in files:
        if not os.path.isdir(name):
            if FILENAME_RE.search(name):
                any_errors = process(name) or any_errors
        for dir, _, subfiles in os.walk(name):
            for subname in (os.path.join(dir, s) for s in subfiles):
                if FILENAME_RE.search(subname):
                    any_errors = process(subname) or any_errors

    if any_errors:
        return 1
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="check or fix ifndef guards in files")
    parser.add_argument("files", nargs="+", help="files or directories to examine")
    parser.add_argument(
        "-fix", help="fix files instead of checking them", action="store_true", default=False,
    )
    args = parser.parse_args()
    sys.exit(main(**vars(args)))
