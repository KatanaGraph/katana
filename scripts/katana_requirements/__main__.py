import argparse
import os
import subprocess
import sys
import textwrap
from enum import Enum
from functools import partial
from pathlib import Path

from katana_requirements.data import KATANA_REQUIREMENTS_FILE_NAME, load
from katana_requirements.model import OutputFormat, Requirements


class OutputSeparation(Enum):
    LINE = "line"
    QUOTE = "quote"
    COMMA = "comma"
    YAML_LIST = "yamllist"

    @property
    def prefix(self):
        return OUTPUT_SEPARATION_DEFINITIONS[self][0]

    @property
    def infix(self):
        return OUTPUT_SEPARATION_DEFINITIONS[self][1]

    @property
    def suffix(self):
        return OUTPUT_SEPARATION_DEFINITIONS[self][2]


# This table makes amp sad, but I couldn't figure out a better way to attach values to each element while still having
# construction by name work correctly.
OUTPUT_SEPARATION_DEFINITIONS = {
    OutputSeparation.LINE: ("", "\n", "\n"),
    OutputSeparation.YAML_LIST: (" - ", "\n - ", "\n"),
    OutputSeparation.QUOTE: ("'", "' '", "'"),
    OutputSeparation.COMMA: ("", ", ", ""),
}


def print_str_table(table: dict):
    terminal_width = os.get_terminal_size().columns
    continuation_indent = " " * 12
    for name, description in table.items():
        description_lines = textwrap.wrap(description, width=terminal_width - len(continuation_indent))
        print(f"{name:>10}: {description_lines[0]}")
        if len(description_lines) > 1:
            print(continuation_indent + ("\n" + continuation_indent).join(description_lines[1:]))


def setup_general_arguments(parser):
    parser.add_argument(
        "--label",
        "-l",
        help="A label that is required for all packages to output.",
        type=str,
        default=[],
        action="append",
    )
    parser.add_argument(
        "--input",
        "-i",
        help=f"Input YAML file. By default the file {KATANA_REQUIREMENTS_FILE_NAME} in the root of the repository.",
        type=Path,
        default=[],
        action="append",
    )


def labels_subcommand(args, inputs, data):
    # pylint: disable=unused-argument

    print("Loaded data from files: " + ", ".join(str(f) for f in inputs))

    print("Labels listed in data files:")
    print_str_table(data.labels)


def packaging_systems_subcommand(args, inputs, data):
    # pylint: disable=unused-argument

    print("Loaded data from files: " + ", ".join(str(f) for f in inputs))

    print("Packaging systems listed in data files:")
    print_str_table(data.packaging_systems)


def select_packages(args, data: Requirements):
    return data.select_packages(args.label, args.format.value)


def list_subcommand(args, inputs, data):
    # pylint: disable=unused-argument

    print(args.separation.prefix, end="")
    is_first = True
    for p in select_packages(args, data):
        if not is_first:
            print(args.separation.infix, end="")
        is_first = False
        print(p.format(args.format), end="")
    print(args.separation.suffix, end="")


def setup_install_arguments(parser):
    comma = ", "

    parser.add_argument(
        "--format",
        "-f",
        help=f"The output format: {comma.join(v.value for v in OutputFormat)}",
        type=OutputFormat,
        default=OutputFormat.YAML,
    )
    parser.add_argument(
        "--command",
        "-c",
        help="The install command to use. This is split on whitespace before use. (Default based on --format.)",
        type=str,
        default=None,
    )
    parser.add_argument(
        "--argument",
        "--arg",
        "-a",
        help="Additional arguments to the package installer. Useful for --yes and --quiet and similar arguments.",
        type=str,
        default=[],
        action="append",
    )
    setup_general_arguments(parser)


def install_package_list(args, packages, silent=False):
    # TODO(amp): Remove special cases for these throughout the system. This should probably be configurable.
    if args.command:
        command = args.command.split()
    elif args.format == OutputFormat.PIP:
        command = [sys.executable, "-m", "pip", "install"]
    elif args.format == OutputFormat.APT:
        command = ["apt-get", "satisfy"]
    elif args.format == OutputFormat.CONDA:
        command = ["conda", "install"]
    else:
        raise ValueError(f"{args.format.value} installation not supported from this command.")

    command += args.argument

    full_command = command + [p.format(args.format) for p in packages]
    space = " "
    if not silent:
        print(f"Executing: {space.join(full_command)}")
    return subprocess.check_call(
        full_command, stdout=subprocess.DEVNULL if silent else None, stderr=subprocess.DEVNULL if silent else None
    )


def bisect_list_for_working(packages, func):
    def step(lower_bound, upper_bound):
        pivot = (lower_bound + upper_bound) // 2
        print(
            f"({lower_bound:>3}-{upper_bound:>3}) Trying with {len(packages[:pivot]):>2} elements...",
            end="",
            flush=True,
        )
        try:
            func(packages[:pivot])
            print("Success")
            if lower_bound == upper_bound:
                print("Done!")
                return pivot
            return step(pivot + 1, upper_bound)
        except subprocess.SubprocessError:
            print("Fail")
            if lower_bound == upper_bound:
                print("Done!")
                return pivot - 1
            return step(lower_bound, pivot - 1)

    return step(0, len(packages))


def install_subcommand(args, inputs, data):
    # pylint: disable=unused-argument

    packages = list(select_packages(args, data))
    install_package_list(args, packages)


def bisect_install_subcommand(args, inputs, data):
    # pylint: disable=unused-argument

    packages = list(select_packages(args, data))
    i = bisect_list_for_working(packages, partial(install_package_list, args, silent=True))

    succeeding_packages = packages[:i]
    print()
    print(f"Prefix of {len(succeeding_packages)} packages works:")
    print("\n".join(p.format(args.format) for p in succeeding_packages))

    failing_packages = packages[i:]
    print()
    print(f"The adding packages from this suffix of {len(failing_packages)} packages causes failure:")
    print("\n".join(p.format(args.format) for p in failing_packages))

    if failing_packages:
        print()
        print("Rerunning smallest failing prefix:")
        install_package_list(args, succeeding_packages + [failing_packages[0]])


def main():
    parser = argparse.ArgumentParser(
        prog="katana_requirements",
        description="""
        A tool to extract and format information from katana requirements YAML files.
        """,
    )
    setup_general_arguments(parser)
    parser.set_defaults(cmd=None)

    subparsers = parser.add_subparsers()

    # Subcommand: labels
    labels_parser = subparsers.add_parser("labels", help="List the labels documented in the YAML files.")
    setup_general_arguments(labels_parser)
    labels_parser.set_defaults(cmd=labels_subcommand)

    # Subcommand: packaging_systems
    packaging_systems_parser = subparsers.add_parser(
        "packaging-systems", help="List the packaging systems documented in the YAML files."
    )
    setup_general_arguments(packaging_systems_parser)
    packaging_systems_parser.set_defaults(cmd=packaging_systems_subcommand)

    # Subcommand: list
    list_parser = subparsers.add_parser("list", help="List packages.")

    comma = ", "
    list_parser.add_argument(
        "--format",
        "-f",
        help=f"The output format: {comma.join(v.value for v in OutputFormat)}",
        type=OutputFormat,
        default=OutputFormat.YAML,
    )
    list_parser.add_argument(
        "--separation",
        "-s",
        help=f"The separation to use for the output: {comma.join(v.value for v in OutputSeparation)}",
        type=OutputSeparation,
        default=OutputSeparation.LINE,
    )
    setup_general_arguments(list_parser)
    list_parser.set_defaults(cmd=list_subcommand)

    # Subcommand: install
    install_parser = subparsers.add_parser("install", help="Install packages using a known packaging system")

    setup_install_arguments(install_parser)
    install_parser.set_defaults(cmd=install_subcommand)

    # Subcommand: bisect_install
    bisect_install_parser = subparsers.add_parser(
        "bisect_install",
        help="Try to install subsets of packages to determine a set of working dependencies and a set of non-working.",
    )

    setup_install_arguments(bisect_install_parser)
    bisect_install_parser.set_defaults(cmd=bisect_install_subcommand)

    args = parser.parse_args()

    data, inputs = load(args.input)

    if args.cmd:
        args.cmd(args, inputs, data)
    else:
        parser.print_help()


if __name__ == "__main__":
    sys.exit(main())
