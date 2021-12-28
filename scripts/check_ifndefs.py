#!/usr/bin/env python
#
# check_ifndefs.py [-fix] <file or directory>...
#
# Check and optionally fix ifndef guards in files.
#
# The standard define pattern is:
#
#   KATANA_<PATH_WITH_UNDERSCORES>_H_
#
# where path is the path to the header file without its extension and with
# the components "include", "src", "test", "tests" removed.

from __future__ import print_function

import argparse
import os
import re
import shutil
import sys
import tempfile

guard_pattern = re.compile(
    r"""^\#ifndef \s* (.*)$ \n
        ^\#define \s* (.*)$""",
    re.MULTILINE | re.VERBOSE,
)

pragma_pattern = re.compile(r"^#pragma[^\S\r\n]+once[^\S\r\n]*$", re.MULTILINE)


def no_ext(path):
    last_sep = path.rfind(os.path.sep)
    if last_sep < 0:
        return path
    # Plus one for after the separator. Plus one again to discount filenames
    # that are all extension, i.e., dotfiles.
    first_dot = path.find(".", last_sep + 1 + 1)
    if first_dot < 0:
        return path
    return path[:first_dot]


def make_guard(root, filename):
    p = os.path.relpath(filename, root)
    # We can't use os.path.splitexp directly because files may have multiple
    # extensions (e.g., config.h.in).
    p = no_ext(p)
    p = p.upper()
    p = p.replace("/INCLUDE/", "/", 1)
    p = p.replace("/SRC/", "/", 1)
    p = p.replace("/TESTS/", "/", 1)
    p = p.replace("/TEST/", "/", 1)
    # Just in case, remove characters that can't be part of macros
    p = re.sub(r"[+\-*%=<>?~&^|#:;{}.[\]]", "", p)
    # Differentiate between snake_case file names and directories
    p = p.replace("_", "", -1)
    p = p.replace("/", "_")
    return "KATANA_{p}_H_".format(p=p)


def run_check(root, filename):
    expected = make_guard(root, filename)
    with open(filename, "r") as f:
        contents = f.read()
        m = pragma_pattern.search(contents)
        if m:
            print(
                "{filename}: found:\n\t#pragma once\nshould be #ifndef guard with:\n\t{expected}".format(
                    filename=filename, expected=expected
                ),
                file=sys.stderr,
            )
            return True

        m = guard_pattern.search(contents)
        if not m:
            return False
        g1 = m.group(1)
        g2 = m.group(2)
        # Python2 is still kicking in some of our build environments. Minimize
        # the difference between Python3.6 f-strings and Python2 string format.
        d = {
            "g1": g1,
            "g2": g2,
            "filename": filename,
            "expected": expected,
        }
        if g1 != g2:
            print("{filename}: ifndef {g1} not equal define {g2}".format(**d), file=sys.stderr)
            return True
        if g1 != expected:
            print("{filename}: expected {expected} but found {g1}".format(**d), file=sys.stderr)
            return True
    return False


def run_fix(root, filename):
    with open(filename, "r") as f:
        contents = f.read()
        expected = make_guard(root, filename)
        replacement = "#ifndef {expected}\n#define {expected}".format(expected=expected)
        contents, num_subs = guard_pattern.subn(replacement, contents, count=1)

        if num_subs == 0:
            contents, num_subs = pragma_pattern.subn(replacement, contents, count=1)
            if num_subs == 0:
                return False
            contents += "\n#endif\n"

    with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
        f.write(contents)
    shutil.move(f.name, filename)

    return False


def main(files, root, fix):
    if fix:
        process = run_fix
    else:
        process = run_check

    any_errors = False
    for name in files:
        if not os.path.isdir(name):
            any_errors = process(root, name) or any_errors
        for dir, _, subfiles in os.walk(name):
            for subname in [os.path.join(dir, s) for s in subfiles]:
                any_errors = process(root, subname) or any_errors

    if any_errors:
        return 1
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="check or fix ifndef guards in files")
    parser.add_argument("files", nargs="+", help="files or directories to examine")
    parser.add_argument("--root", help="root directory to determine import name", required=True)
    parser.add_argument("-fix", help="fix files instead of checking them", action="store_true", default=False)
    args = parser.parse_args()
    sys.exit(main(**vars(args)))
