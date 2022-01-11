import argparse
import datetime
import os
import platform
import re
import sys
from os import environ
from subprocess import CalledProcessError

from katana_version import SUBMODULE_PATH, Configuration, git
from katana_version.commands import capture_command
from katana_version.release_workflow_commands import setup_global_log_arguments, setup_global_repo_arguments
from katana_version.version import format_version_debian, format_version_pep440, format_version_semantic, get_version
from packaging import version


def show_subcommand(args):
    ver = get_version(
        args.configuration,
        commit=args.commit,
        variant=args.variant,
        pretend_master=args.master,
        pretend_clean=args.clean,
    )
    if args.component == "full":
        print(args.format(ver))
    else:
        print(getattr(ver, args.component))


def setup_show_subcommand(subparsers):
    parser = subparsers.add_parser(
        "show", help="Computes and prints the version (or components of it) in the current tree."
    )

    group_component = parser.add_mutually_exclusive_group()
    group_component.add_argument(
        "--major", help="Output the major version", dest="component", action="store_const", const="major"
    )
    group_component.add_argument(
        "--minor", help="Output the minor version", dest="component", action="store_const", const="minor"
    )
    group_component.add_argument(
        "--patch",
        "--micro",
        help="Output the patch (a.k.a. micro) version",
        dest="component",
        action="store_const",
        const="micro",
    )
    group_component.add_argument(
        "--local",
        help="Output the local version (variant and development tag)",
        dest="component",
        action="store_const",
        const="local",
    )
    group_component.add_argument(
        "--full",
        help="Output the full version including all components [default]",
        dest="component",
        action="store_const",
        const="full",
    )
    parser.set_defaults(component="full")

    group_format = parser.add_mutually_exclusive_group()
    group_format.add_argument(
        "--debian",
        help="Output a Debian-compatible version number (with ~)",
        dest="format",
        action="store_const",
        const=format_version_debian,
    )
    group_format.add_argument(
        "--conda",
        "--pep440",
        help="Output a Conda-compatible version number (with .) [default]",
        dest="format",
        action="store_const",
        const=format_version_pep440,
    )
    parser.set_defaults(format=format_version_pep440)

    parser.add_argument(
        "--pretend-master",
        help="Pretend that the working tree or commit is committed to master and clean.",
        action="store_true",
        dest="master",
    )
    parser.add_argument(
        "--override-variant",
        help="Specify the variant tag. (environment variable: BUILD_VARIANT)",
        type=str,
        dest="variant",
        default=environ.get("BUILD_VARIANT"),
    )
    parser.add_argument(
        "commit",
        type=str,
        nargs="?",
        help="Get the version for a clean checkout of the given commit in the repo of the current directory. (Note: "
        "Works best from the enterprise repo or in open only mode.)",
        default=None,
    )

    setup_global_log_arguments(parser)
    setup_global_repo_arguments(parser)

    parser.set_defaults(subcommand_impl=show_subcommand)


def parse_subcommand(args):
    if not args.version:
        version_str = sys.stdin.read()
    else:
        version_str = args.version

    # Coerce debian versions into pep440 versions
    version_str = version_str.replace("~", ".")

    v = version.Version(version_str)

    if v.local:
        local_components = v.local.split(".")
    else:
        local_components = []

    if args.component == "semantic":
        print(format_version_semantic(v))
    elif args.component == "all":
        print(format_version_pep440(v))
    elif args.component == "open":
        if len(local_components) >= 4:
            print(local_components[3])
    elif args.component == "enterprise":
        if len(local_components) >= 3:
            print(local_components[2])
    else:
        print(getattr(v, args.component))


def setup_parse_subcommand(subparsers):
    parser = subparsers.add_parser("parse", help="Parses a katana version and prints information about it.")

    group_component = parser.add_mutually_exclusive_group()
    group_component.add_argument(
        "--semantic", help="Output the full semantic version", dest="component", action="store_const", const="semantic"
    )
    group_component.add_argument(
        "--major", help="Output the major version", dest="component", action="store_const", const="major"
    )
    group_component.add_argument(
        "--minor", help="Output the minor version", dest="component", action="store_const", const="minor"
    )
    group_component.add_argument(
        "--patch",
        "--micro",
        help="Output the patch (a.k.a. micro) version",
        dest="component",
        action="store_const",
        const="micro",
    )
    group_component.add_argument(
        "--local",
        help="Output the local version (variant and development tag)",
        dest="component",
        action="store_const",
        const="local",
    )
    group_component.add_argument(
        "--open", help="Output open repository commit hash", dest="component", action="store_const", const="open",
    )
    group_component.add_argument(
        "--enterprise",
        help="Output enterprise repository commit hash",
        dest="component",
        action="store_const",
        const="enterprise",
    )
    parser.set_defaults(component="all")

    parser.add_argument(
        "version",
        type=str,
        nargs="?",
        help="A version number to parse. If not provided, read a version from stdin.",
        default=None,
    )

    parser.set_defaults(subcommand_impl=parse_subcommand)


def provenance_subcommand(args):
    values = dict()
    if platform.node():
        values.update(hostname=platform.node())

    if hasattr(os, "getlogin"):
        values.update(user=os.getlogin())

    if platform.platform():
        values.update(platform=platform.platform())

    try:
        values.update(lsb_release=capture_command("lsb_release", "-ds"))
    except CalledProcessError:
        pass

    try:
        values.update(lsb_codename=capture_command("lsb_release", "-cs"))
    except CalledProcessError:
        pass

    environment_to_capture = {
        re.compile(r"CONDA_.*"),
        "CXX",
        "CC",
        "LD",
        "CXXFLAGS",
        "CFLAGS",
        "LDFLAGS",
        "SHELL",
        re.compile(r"CMAKE_.*"),
        re.compile(r".*PATH"),
    }

    environment_to_exclude = re.compile(r"([^a-z0-9]|^)(key|token|password|secret)", re.IGNORECASE)

    for var in environment_to_capture:
        if isinstance(var, re.Pattern):
            vars = [k for k in environ if var.fullmatch(k)]
        else:
            vars = [var]
        for var in vars:
            if environment_to_exclude.search(var):
                # Don't capture variables which might have secrets.
                continue
            values["ENV_" + var] = environ.get(var, "<unset>")

    config: Configuration = args.configuration

    values.update(cwd=os.getcwd())
    values.update(date=datetime.datetime.now().astimezone())
    values.update(
        python_exe=sys.executable,
        python_version=sys.version,
        python_api_version=sys.api_version,
        python_path=":".join(sys.path),
    )
    values.update(katana_version_file=config.version_file)
    values.update(katana_version=get_version(args.configuration))

    if config.open:
        katana_repo_root = config.open.dir
        values.update(katana_repo_root=katana_repo_root.absolute())
        values.update(katana_branch=git.get_branch_checked_out(katana_repo_root))
        values.update(katana_upstream=config.open.upstream_url)
        values.update(katana_origin=config.open.origin_url)
        values.update(katana_hash=git.get_hash(git.HEAD, katana_repo_root))
        values.update(katana_head=git.get_hash(git.HEAD, katana_repo_root, pretend_clean=True))

    if config.has_enterprise:
        katana_enterprise_repo_path = config.enterprise.dir
        values.update(katana_enterprise_repo_path=katana_enterprise_repo_path.absolute())
        values.update(katana_enterprise_branch=git.get_branch_checked_out(katana_enterprise_repo_path))
        values.update(katana_enterprise_upstream=config.enterprise.upstream_url)
        values.update(katana_enterprise_origin=config.enterprise.origin_url)
        values.update(
            katana_enterprise_hash=git.get_hash(git.HEAD, katana_enterprise_repo_path, exclude_dirty=(SUBMODULE_PATH,))
        )
        values.update(katana_enterprise_head=git.get_hash(git.HEAD, katana_enterprise_repo_path, pretend_clean=True))

    # Sort by the key for easier reading later
    values = dict(sorted(values.items(), key=lambda kv: kv[0]))

    # Convert all values to strings and quote universally problematic characters
    values = {k: str(v).replace("\n", "\\n") for k, v in values.items()}

    def escape_double_quotes(s):
        return s.replace('"', '\\"')

    def escape_single_quotes(s):
        return s.replace("'", "\\'")

    format_str = args.format
    format_str = format_str.replace("\\n", "\n").replace("\\t", "\t")
    print(
        args.prefix
        + args.separator.join(
            format_str.format(
                k,
                v,
                k=k,
                K=k.upper(),
                v=v,
                v_double_quoted=f'"{escape_double_quotes(v)}"',
                v_single_quoted=f"'{escape_single_quotes(v)}'",
            )
            for k, v in values.items()
        )
        + args.suffix,
        end="",
    )


def setup_provenance_subcommand(subparsers):
    parser = subparsers.add_parser(
        "provenance", help="Prints a provenance description for inclusion in artifacts. This is not a version.",
    )

    class SetFormatAction(argparse.Action):
        def __init__(self, option_strings, dest=None, nargs=None, **kwargs):
            assert not nargs
            self.format = None
            self.prefix = ""
            self.suffix = ""
            self.separator = ""
            self.__dict__.update(kwargs)
            super().__init__(option_strings, dest, nargs=0)

        def __call__(self, parser, namespace, values, option_string=None):
            assert not values
            setattr(namespace, "format", self.format)
            setattr(namespace, "prefix", self.prefix)
            setattr(namespace, "suffix", self.suffix)
            setattr(namespace, "separator", self.separator)

    group_format = parser.add_mutually_exclusive_group()
    group_format.add_argument(
        "--define", help="Format as #defines.", action=SetFormatAction, format="#define {K} {v_double_quoted}\n",
    )
    group_format.add_argument(
        "--yaml", help="Format as YAML.", action=SetFormatAction, format="{k}: {v_double_quoted}\n"
    )
    group_format.add_argument(
        "--python", help="Format as Python.", action=SetFormatAction, format="{k} = {v_double_quoted}\n"
    )
    group_format.add_argument(
        "--json",
        help="Format as JSON.",
        action=SetFormatAction,
        format='  "{k}": {v_double_quoted}',
        prefix="{\n",
        suffix="\n}\n",
        separator=",\n",
    )

    group_format.add_argument(
        "--format",
        "-f",
        help="Provide a format string for each key-value pair. Use the source, Luke.",
        dest="format",
        type=str,
    )
    parser.add_argument(
        "--separator",
        "-j",
        help="The separator to print between key-value pairs. Use the source, Luke.",
        dest="separator",
        type=str,
    )
    parser.add_argument(
        "--prefix",
        "-p",
        help="Prefix to print before the first key-value pair. Use the source, Luke.",
        dest="prefix",
        type=str,
    )
    parser.add_argument(
        "--suffix",
        "-s",
        help="Suffix to print after the last key-value pair. Use the source, Luke.",
        dest="suffix",
        type=str,
    )

    setup_global_log_arguments(parser)
    setup_global_repo_arguments(parser)

    parser.set_defaults(
        subcommand_impl=provenance_subcommand, format="{k}: {v_double_quoted}\n", prefix="", suffix="", separator=""
    )
