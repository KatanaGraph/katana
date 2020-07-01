#!/usr/bin/env python
#
# Script to check the output of algorithms:
# Author: Gurbinder Gill (gurbinder533@gmail.com)
# Author: Roshan Dathathri (roshan@cs.utexas.edu)
# Modified to calculate error + take tolerance as an error by Loc Hoang
#
# Expects files in the following format:
#
#   <nodeID> <nodeFieldVal>
#   <nodeID> <nodeFieldVal>
#   ...

from __future__ import print_function

import argparse
import os
import subprocess
import sys
import tempfile


if sys.version_info >= (3, 0):

    def convert(x):
        return x


else:

    def convert(x):
        return long(x)


def check_results(
    master_file,
    other_files,
    tolerance,
    offset,
    errors,
    mrows,
    global_error_squared,
    num_nodes,
):

    with open(master_file) as mfile, open(other_files) as ofile:
        mfile.seek(offset)

        for line2 in ofile:
            line1 = mfile.readline()
            offset = offset + len(line1)

            split_line1 = line1.split(" ")
            split_line2 = line2.split(" ")

            if split_line1[0] == "":
                print("ERROR: output longer than input")
                return (0, errors, mrows, None, None)

            while convert(split_line1[0]) < convert(split_line2[0]):
                print("MISSING ROW: ", split_line1[0])
                mrows = mrows + 1
                line1 = mfile.readline()
                offset = offset + len(line1)
                split_line1 = line1.split(" ")

            # forces failure if missings rows exist
            # if mrows > 0:
            #  return (-1, errors, mrows)

            if convert(split_line1[0]) == convert(split_line2[0]):
                # absolute value of difference in fields
                field_difference = abs(float(split_line1[1]) - float(split_line2[1]))

                global_error_squared += field_difference ** 2
                num_nodes += 1

                if field_difference > tolerance:
                    print("NOT MATCHED \n")
                    print(line1)
                    print(line2)
                    errors = errors + 1
                # TODO (Loc) make more general: deals with 2 fields in output (should
                # optimally deal with arbitrary # of fields
                elif len(split_line1) == 3:
                    field_difference2 = abs(
                        float(split_line1[2]) - float(split_line2[2])
                    )
                    if field_difference2 > tolerance:
                        print("NOT MATCHED \n")
                        print(line1)
                        print(line2)
                        errors = errors + 1
            else:
                print("OFFSET MISMATCH: ", split_line1[0], split_line2[0])
                return (-1, errors, mrows, global_error_squared, num_nodes)

    return (offset, errors, mrows, global_error_squared, num_nodes)


def main(master_file, all_files, tolerance, mean_tolerance):
    offset = 0
    errors = 0
    mrows = 0
    global_error_squared = 0
    num_nodes = 0

    for f in all_files:
        print("Checking", f, "offset =", offset)
        offset, errors, mrows, global_error_squared, num_nodes = check_results(
            master_file,
            f,
            tolerance,
            offset,
            errors,
            mrows,
            global_error_squared,
            num_nodes,
        )
        if offset == -1:
            break

    rmse = (global_error_squared / num_nodes) ** 0.5
    if rmse > mean_tolerance:
        print("\nRoot mean square error (for first field): ", rmse)

    if offset != -1:
        mfile = open(master_file)
        mfile.seek(offset)
        old_mrows = mrows
        for line in mfile:
            mrows = mrows + 1
        if mrows > old_mrows:
            mrows = mrows - old_mrows
            print("\nNo of offsets/rows missing: ", mrows)

    if offset == -1:
        print("\nOffset not correct")

    if errors > 0:
        print("\nNo. of mismatches: ", errors)

    if (errors > 0) or (offset == -1) or (mrows > 0) or (rmse > mean_tolerance):
        print("\nFAILED\n")
        return 1
    else:
        print("\nSUCCESS\n")
        return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check graph output results")

    # parse files and an optional tolerance
    parser.add_argument("files", type=str, nargs="+", help="input + output files")

    parser.add_argument(
        "-tolerance",
        "-t",
        type=float,
        default=0.0001,
        help="tolerance for difference in fields (error)",
    )
    parser.add_argument(
        "-sort", "-s", type=bool, default=False, help="sort the generated output files",
    )
    parser.add_argument(
        "-delete",
        "-d",
        type=bool,
        nargs=1,
        default=False,
        help="delete the generated output files",
    )
    parser.add_argument(
        "-mean_tolerance",
        "-m",
        type=float,
        default=0.0001,
        help="tolerance for root mean square error",
    )

    parsed_arguments = parser.parse_args()

    master_file = parsed_arguments.files[0]

    all_files = []
    for f in parsed_arguments.files[1:]:
        if os.path.isdir(f):
            all_files.extend([os.path.join(f, x) for x in os.listdir(f)])
        else:
            all_files.append(f)

    if not all_files:
        print("no files to verify")
        sys.exit(1)

    if parsed_arguments.sort:
        temp_file = tempfile.NamedTemporaryFile(delete=True)
        cmd = ["sort", "-nu", "-o", temp_file.name]
        cmd += all_files
        subprocess.check_call(cmd)
        all_files = [temp_file.name]

    if parsed_arguments.delete:
        for f in all_files:
            os.remove(f)

    tolerance = parsed_arguments.tolerance
    mean_tolerance = parsed_arguments.mean_tolerance

    ret = main(master_file, all_files, tolerance, mean_tolerance)

    if ret:
        sys.exit(1)
