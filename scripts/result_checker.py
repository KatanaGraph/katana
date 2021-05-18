#!/usr/bin/env python3
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

import argparse
import os
import subprocess
import sys
import tempfile

mismatch_printed = 0


def print_mismatch(line1, line2):
    # pylint: disable=global-statement
    global mismatch_printed
    if mismatch_printed < 20:
        print("ERROR: NOT MATCHED:\n\tExpected ({})\n\tFound ({})".format(line1.strip(), line2.strip()))
    mismatch_printed += 1


def check_results(
    master_file, other_files, tolerance, offset, errors, mrows, global_error_squared, num_nodes,
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

            while split_line1[0] < split_line2[0]:
                print("ERROR: MISSING ROW: ", split_line1[0])
                mrows = mrows + 1
                line1 = mfile.readline()
                offset = offset + len(line1)
                split_line1 = line1.split(" ")

            # forces failure if missings rows exist
            # if mrows > 0:
            #  return (-1, errors, mrows)

            if split_line1[0] == split_line2[0]:
                # absolute value of difference in fields
                field_difference = abs(float(split_line1[1]) - float(split_line2[1]))

                global_error_squared += field_difference ** 2
                num_nodes += 1

                if field_difference > tolerance:
                    print_mismatch(line1, line2)
                    errors = errors + 1
                # TODO (Loc) make more general: deals with 2 fields in output (should
                # optimally deal with arbitrary # of fields
                elif len(split_line1) == 3:
                    field_difference2 = abs(float(split_line1[2]) - float(split_line2[2]))
                    if field_difference2 > tolerance:
                        print_mismatch(line1, line2)
                        errors = errors + 1
            else:
                print("ERROR: OFFSET MISMATCH: ", split_line1[0], split_line2[0])
                return (-1, errors, mrows, global_error_squared, num_nodes)

    return (offset, errors, mrows, global_error_squared, num_nodes)


def check_results_string_column(
    masterFile, otherFiles, tolerance, offset, errors, mrows, global_error_squared, num_nodes,
):
    """
    Called if first column is a string (other version casts into a long which
    is errorneous if string
    """
    with open(masterFile) as mfile, open(otherFiles) as ofile:
        mfile.seek(offset)

        for line2 in ofile:
            line1 = mfile.readline()
            offset = offset + len(line1)

            split_line1 = line1.split(" ")
            split_line2 = line2.split(" ")

            if split_line1[0] == "":
                print("ERROR: output longer than input")
                return (0, errors, mrows, global_error_squared, num_nodes)

            if len(split_line1) != len(split_line2):
                print_mismatch(line1, line2)
                errors = errors + 1

            if len(split_line1) == 2:
                exact_cols = [0]
                numeric_col = 1
            elif len(split_line1) == 3:
                exact_cols = [0, 1]
                numeric_col = 2
            else:
                print("FIELD COUNT ISSUE:", split_line1)

            # check to make sure row matches exactly between the files
            if all(split_line1[i] == split_line2[i] for i in exact_cols):
                # absolute value of difference in fields
                field_difference = abs(float(split_line1[numeric_col]) - float(split_line2[numeric_col]))
                global_error_squared += field_difference ** 2
                num_nodes += 1

                if field_difference > tolerance:
                    print_mismatch(line1, line2)
                    errors = errors + 1
            else:
                print("ERROR: OFFSET MISMATCH: ", split_line1[0], split_line2[0])
                return (-1, errors, mrows, global_error_squared, num_nodes)

    return (offset, errors, mrows, global_error_squared, num_nodes)


def main(master_file, all_files, tolerance, mean_tolerance, stringcolumn):
    offset = 0
    errors = 0
    mrows = 0
    global_error_squared = 0
    num_nodes = 0

    for f in all_files:
        print("Checking", f, "offset =", offset)
        if not stringcolumn:
            offset, errors, mrows, global_error_squared, num_nodes = check_results(
                master_file, f, tolerance, offset, errors, mrows, global_error_squared, num_nodes,
            )
        else:
            # first column is a string
            (offset, errors, mrows, global_error_squared, num_nodes,) = check_results_string_column(
                master_file, f, tolerance, offset, errors, mrows, global_error_squared, num_nodes,
            )

        if offset == -1:
            break

    rmse = (global_error_squared / num_nodes) ** 0.5
    if rmse > mean_tolerance:
        print("\nRoot mean square error (for first field): ", rmse)

    if offset != -1:
        with open(master_file) as mfile:
            mfile.seek(offset)
            old_mrows = mrows
            for _ in mfile:
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
    print("\nSUCCESS\n")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check graph output results")

    # parse files and an optional tolerance
    parser.add_argument("files", type=str, nargs="+", help="input + output files")

    parser.add_argument(
        "-tolerance", "-t", type=float, default=0.0001, help="tolerance for difference in fields (error)",
    )
    parser.add_argument(
        "-sort", "-s", type=bool, default=False, help="sort the generated output files",
    )
    parser.add_argument(
        "-delete", "-d", type=bool, nargs=1, default=False, help="delete the generated output files",
    )
    parser.add_argument(
        "-mean_tolerance", "-m", type=float, default=0.0001, help="tolerance for root mean square error",
    )
    parser.add_argument(
        "-stringcolumn", "-c", type=bool, nargs=1, default=False, help="true if first column is a string",
    )

    parsed_arguments = parser.parse_args()

    master_file = parsed_arguments.files[0]

    all_files = []
    for f in parsed_arguments.files[1:]:
        if os.path.isdir(f):
            all_files.extend([x.path for x in os.scandir(f) if x.is_file()])
        else:
            all_files.append(f)

    if not all_files:
        print("no files to verify")
        sys.exit(1)

    if parsed_arguments.sort:
        # pylint: disable=consider-using-with
        temp_file = tempfile.NamedTemporaryFile(delete=True)
        cmd = ["sort", "-nu", "-o", temp_file.name]
        cmd += all_files
        subprocess.check_call(cmd)
        all_files = [temp_file.name]

        # only delete if sorted; else you delete the thing you were going to
        # compare with
        if parsed_arguments.delete:
            for f in all_files:
                os.remove(f)

    tolerance = parsed_arguments.tolerance
    mean_tolerance = parsed_arguments.mean_tolerance
    stringcolumn = parsed_arguments.stringcolumn

    ret = main(master_file, all_files, tolerance, mean_tolerance, stringcolumn)

    if not parsed_arguments.sort:
        if parsed_arguments.delete:
            for f in all_files:
                os.remove(f)

    if ret:
        sys.exit(1)
