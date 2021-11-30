import argparse
import os
import re
import subprocess
import sys
import textwrap
from enum import Enum
from functools import partial
from pathlib import Path
from typing import List, Sequence

from katana_requirements.data import KATANA_REQUIREMENTS_FILE_NAME, load
from katana_requirements.model import OutputFormat, Package, PackagingSystem, Requirements
from packaging.version import Version


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
        "--packaging-system",
        "-p",
        help="The packaging system to use. Only print packages which support this system and use defaults appropriate "
        "for this system. To list the available packaging systems, run `requirements packaging-systems`.",
        type=str,
    )
    parser.add_argument(
        "--input",
        "-i",
        help=f"Input YAML file. By default the file {KATANA_REQUIREMENTS_FILE_NAME} in the root of the repository.",
        type=Path,
        default=[],
        action="append",
    )


def labels_subcommand(args, inputs, data: Requirements):
    # pylint: disable=unused-argument

    print("Loaded data from files: " + ", ".join(str(f) for f in inputs))

    print("Labels listed in data files:")
    print_str_table({k: f"{v.description} (inherits from: {list(v.inherits)})" for k, v in data.labels.items()})


def packaging_systems_subcommand(args, inputs, data: Requirements):
    # pylint: disable=unused-argument

    print("Loaded data from files: " + ", ".join(str(f) for f in inputs))

    print("Packaging systems listed in data files:")
    print_str_table(
        {
            k: f"{v.description} (inherits from: {list(v.inherits)})\nChannels: {v.channels}"
            for k, v in data.packaging_systems.items()
        }
    )


def select_packages(args, data: Requirements):
    return data.select_packages(args.label, args.packaging_system)


def list_subcommand(args, inputs, data: Requirements):
    # pylint: disable=unused-argument
    _, ps = get_format(args, data)

    print(args.separation.prefix, end="")
    is_first = True
    for p in select_packages(args, data):
        if not is_first:
            print(args.separation.infix, end="")
        is_first = False
        print(p.format(ps), end="")
    print(args.separation.suffix, end="")


def print_markdown_table(table: dict):
    print(
        """
Name       | Description
---------- | -----------------"""
    )
    for name, description in sorted(table.items()):
        description = description.replace("\n", "")
        print(f"{name:>10} | {description}")


def markdown_subcommand(args, inputs, data: Requirements):
    # pylint: disable=unused-argument

    print("Loaded data from files: " + ", ".join(str(f) for f in inputs))
    print()
    print("Packages")
    print("========")
    print()

    print("Name | Labels " + "".join(f"| {ps} " for ps in data.packaging_systems.keys()))
    print("------- | ------- " + "| ------------ " * len(data.packaging_systems))

    for p in sorted(data.select_packages(labels=args.label), key=lambda p: p.name):
        labels = ", ".join(p.labels)
        print(
            f"{p.name} | {labels} "
            + "".join(f"| {p.format(data._sub_packaging_systems(ps))} " for ps in data.packaging_systems.values())
        )

    print()
    print("Packaging systems")
    print("=================")
    print()
    print("Name | Description | Channels")
    print("------- | ------- | ------------")
    comma = ", "
    for ps in data.packaging_systems.values():
        print(f"{ps.name} | {ps.description} | {comma.join(ps.channels)}")
    print()
    print("Labels")
    print("======")
    print()
    print_markdown_table({k: v.description for k, v in data.labels.items()})


def setup_install_arguments(parser):
    comma = ", "

    parser.add_argument(
        "--format",
        "-f",
        help=f"The output format: {comma.join(v.value for v in OutputFormat)}. (Default based on --packaging-system.)",
        type=OutputFormat,
        default=None,
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


def get_apt_version():
    version_information_str = str(subprocess.check_output(["apt-get", "-v"], stderr=subprocess.DEVNULL), "UTF-8")
    version_match = re.match(r"apt ([0-9.]+) \(.*\)", version_information_str)
    if not version_match:
        raise RuntimeError(f"apt-get returned unexpected version information:\n{version_information_str}")
    return Version(version_match.group(1))


def install_package_list(args, packages: List[Package], data: Requirements, silent=False):
    format, ps = get_format(args, data)
    if format == OutputFormat.APT:
        # Because APT is unable to handle bounds correctly, we need a special case to install different packages
        # differently.
        pinned_package_arguments = []
        specified_package_arguments = []
        # The same packages as specified_package_arguments, but without versions (used for old APT)
        unpinned_package_arguments = []
        for p in packages:
            version = p.version_for(ps)
            if version[0] == "=":
                # This is pinned for APT
                pinned_package_arguments.append(p.name_for(ps) + version)
            else:
                specified_package_arguments.append(p.format(ps))
                unpinned_package_arguments.append(p.name_for(ps))
        install_command = ["apt-get", "install"] + args.argument
        satisfy_command = ["apt-get", "satisfy"] + args.argument
        if get_apt_version() >= Version("2.0.0"):
            execute_subprocess(install_command + pinned_package_arguments, silent)
            execute_subprocess(satisfy_command + specified_package_arguments, silent)
        else:
            warning_msg = (
                "WARNING: You are using apt < 2.0. This means package versions will not be specified "
                "correctly due to lack of support for the satisfy subcommand. If you have any problems, "
                "manually check the installed package versions."
            )
            print(warning_msg, file=sys.stderr)
            execute_subprocess(install_command + pinned_package_arguments + unpinned_package_arguments, silent)
            print(warning_msg, file=sys.stderr)
        return

    if args.command:
        command = args.command.split()
    elif format == OutputFormat.PIP:
        command = [sys.executable, "-m", "pip", "install"]
    elif format == OutputFormat.CONDA:
        command = ["conda", "install"] + [v for c in ps.channels for v in ["-c", c]]
    else:
        raise ValueError(f"{format.value} installation not supported from this command.")

    command += args.argument

    execute_subprocess(command + [p.format(ps) for p in packages], silent)


def execute_subprocess(full_command, silent):
    command_str = " ".join(f"'{v}'" for v in full_command)
    if not silent:
        print(f"Executing: {command_str}")
    try:
        return subprocess.check_call(
            full_command, stdout=subprocess.DEVNULL if silent else None, stderr=subprocess.DEVNULL if silent else None
        )
    except subprocess.SubprocessError:
        print(f"Failed to execute:\n{command_str}")
        print("You might be able to modify the command and run it yourself to make it work.")
        raise


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
            if lower_bound >= upper_bound:
                print("Done!")
                return pivot
            return step(min(pivot + 1, upper_bound), upper_bound)
        except subprocess.SubprocessError:
            print("Fail")
            if lower_bound >= upper_bound or pivot == 0:
                print("Done!")
                return max(pivot - 1, 0)
            return step(lower_bound, max(lower_bound, pivot - 1))

    return step(0, len(packages))


def install_subcommand(args, inputs, data):
    # pylint: disable=unused-argument

    packages = list(select_packages(args, data))
    install_package_list(args, packages, data)


def bisect_install_subcommand(args, inputs, data: Requirements):
    # pylint: disable=unused-argument
    _, ps = get_format(args, data)
    packages = list(select_packages(args, data))
    i = bisect_list_for_working(packages, partial(install_package_list, args, silent=True, data=data))

    succeeding_packages = packages[:i]
    print()
    print(f"Prefix of {len(succeeding_packages)} packages works:")
    print("\n".join(p.format(ps) for p in succeeding_packages))

    failing_packages = packages[i:]
    print()
    print(f"The adding packages from this suffix of {len(failing_packages)} packages causes failure:")
    print("\n".join(p.format(ps) for p in failing_packages))

    if failing_packages:
        print()
        print("Rerunning smallest failing prefix:")
        install_package_list(args, succeeding_packages + [failing_packages[0]], data)


def get_format(args, data) -> (OutputFormat, Sequence[PackagingSystem]):
    if not args.packaging_system:
        raise ValueError("--packaging-system/-p is required.")

    ps = data.packaging_systems[args.packaging_system]
    return args.format or ps.format, ps


def main():
    parser = argparse.ArgumentParser(
        prog="scripts/requirements",
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
        default=None,
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

    # Subcommand: markdown
    markdown_parser = subparsers.add_parser("markdown", help="Output data in markdown for humans to read.")
    setup_general_arguments(markdown_parser)
    markdown_parser.set_defaults(cmd=markdown_subcommand)

    # Subcommand: install
    install_parser = subparsers.add_parser(
        "install",
        help="Install packages using a packaging system based on the format it uses. "
        "(There are special cases for some packaging systems. Beware.)",
    )

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

    return 0


if __name__ == "__main__":
    sys.exit(main())
