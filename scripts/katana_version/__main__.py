import argparse
import datetime
import logging
import os
import platform
import re
import sys
import threading
from enum import Enum
from os import environ
from subprocess import CalledProcessError
from typing import Iterable, Optional

from packaging import version
from packaging.version import InvalidVersion

from . import CONFIG_VERSION_PATH, SUBMODULE_PATH, Configuration, Repo, StateError, git
from .commands import CommandError, capture_command
from .github import GithubFacade
from .version import add_dev_to_version, format_version_debian, format_version_pep440, get_explicit_version, get_version

logger = logging.getLogger(__name__)


class BranchKind(Enum):
    MASTER = "master"
    RELEASE = "release/v.*"
    VARIANT = "variant/.*"


def check_remotes(config: Configuration):
    def check_remote(repo, remote, remote_name):
        if not remote:
            raise StateError(f"{repo}: Repository must have an {remote_name} remote. Your work flow is not supported.")

    check_remote(config.open, config.open.origin_remote, "origin")
    check_remote(config.open, config.open.upstream_remote, "upstream")
    check_remote(config.enterprise, config.enterprise.origin_remote, "origin")
    check_remote(config.enterprise, config.enterprise.upstream_remote, "upstream")


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
    # group_component.add_argument(
    #     "--dev-tag",
    #     help="Output the development tag (the commit counts and hashes)",
    #     dest="component",
    #     action="store_const",
    #     const="dev_tag",
    # )
    # group_component.add_argument(
    #     "--variant", help="Output the variant", dest="component", action="store_const", const="variant"
    # )
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


def bump_checks(args):
    config: Configuration = args.configuration
    check_clean(args, config)

    current_branch = get_current_branch_from_either_repository(config)
    kind = get_branch_kind(current_branch, (BranchKind.MASTER, BranchKind.RELEASE, BranchKind.VARIANT))
    check_at_branch(current_branch, config)
    git.switch(current_branch, config.enterprise, config.dry_run)
    git.switch(current_branch, config.open, config.dry_run)

    prev_version, _ = get_explicit_version(git.HEAD, True, config.open, config.version_file, no_dev=False)
    next_version = version.Version(args.next_version)

    check_branch_version(current_branch, kind, next_version, prev_version)


def get_current_branch_from_either_repository(config: Configuration):
    current_branch = git.get_branch_checked_out(config.open, ref_only=True)
    if config.has_enterprise:
        current_branch = current_branch or git.get_branch_checked_out(config.enterprise, ref_only=True)
    if not current_branch:
        raise StateError("Operation is not supported without a branch checked out (currently HEAD is detached).")
    return current_branch


def check_branch_version(
    current_branch: str, kind: BranchKind, next_version: version.Version, prev_version: Optional[version.Version]
):
    if prev_version and kind == BranchKind.RELEASE and prev_version.release != next_version.release:
        raise ValueError("The semantic version cannot be changed on a release branch.")
    expected_release_branch_name = "release/v" + ".".join(str(i) for i in next_version.release)
    if kind == BranchKind.RELEASE and current_branch != expected_release_branch_name:
        raise StateError(
            "The semantic version does not match the release branch name: "
            f"{expected_release_branch_name} != {current_branch}"
        )
    if prev_version and kind != BranchKind.VARIANT and prev_version.local:
        raise StateError(
            f"The non-variant branch {current_branch} has a variant. This should not happen. Somebody broke the rules."
        )
    if kind != BranchKind.VARIANT and next_version.local:
        raise ValueError(f"The variant cannot be set on the non-variant branch {current_branch}.")
    if kind == BranchKind.VARIANT and next_version.local != current_branch.split("/", maxsplit=1)[1]:
        branch_variant = current_branch.split("/", maxsplit=1)[1]
        raise StateError(
            "The variant in the version and the variant in the branch name must be the same: "
            f"{next_version.local} != {branch_variant}"
        )
    if prev_version and next_version <= prev_version:
        raise ValueError(f"The next version ({next_version}) must be greater than the current one ({prev_version})")
    if prev_version and kind == BranchKind.VARIANT and next_version.release != prev_version.release:
        raise ValueError(
            "To change the version of a variant branch, merge master into the variant branch. "
            "Bumping the version directly on the variant branch is not allowed."
        )


def get_branch_kind(current_branch, kinds: Iterable[BranchKind]):
    for kind in kinds:
        if re.match(kind.value, current_branch):
            return kind
    kinds_str = ", ".join(k.value for k in kinds)
    raise StateError(f"The current branch ({current_branch}) should be one of: {kinds_str}")


def check_at_branch(branch, config):
    check_remotes(config)
    if git.get_hash(f"{config.open.upstream_remote}/{branch}", config.open) != git.get_hash(git.HEAD, config.open):
        raise StateError(f"{config.open.dir} HEAD is up to date with {branch}")

    if config.has_enterprise and git.get_hash(
        f"{config.enterprise.upstream_remote}/{branch}", config.enterprise
    ) != git.get_hash(git.HEAD, config.enterprise):
        raise StateError(f"{config.enterprise.dir} HEAD is up to date with {branch}")


def bump_subcommand(args):
    bump_checks(args)

    config: Configuration = args.configuration

    g = GithubFacade(config)

    prev_version, _ = get_explicit_version(git.HEAD, True, config.open, config.version_file, no_dev=True)
    next_version = version.Version(args.next_version)

    current_branch = git.get_branch_checked_out(config.open)
    todos = bump_both_repos(config, g, prev_version, next_version, current_branch)

    warn_dry_run(args)
    return todos


def check_branch_not_exist(config: Configuration, branch_name):
    check_remotes(config)
    if git.ref_exists(branch_name, config.open):
        raise StateError(f"Branch {branch_name} already exists in {config.open.dir.name}")
    if git.ref_exists(branch_name, config.enterprise):
        raise StateError(f"Branch {branch_name} already exists in {config.enterprise.dir.name}")


def bump_both_repos(config: Configuration, g: GithubFacade, prev_version, next_version, base):
    check_remotes(config)
    next_version_str = format_version_pep440(next_version)
    if config.has_enterprise:
        enterprise_commit = git.get_hash(git.HEAD, config.enterprise, pretend_clean=True)
    open_commit = git.get_hash(git.HEAD, config.open, pretend_clean=True)
    if config.dry_run:
        print(f"WRITE: {next_version_str} to {config.open.dir / CONFIG_VERSION_PATH}")
    else:
        with open(config.open.dir / CONFIG_VERSION_PATH, "wt", encoding="utf-8") as fi:
            fi.write(next_version_str)
            fi.write("\n")
    title = f"Bump version to {next_version} on {base}"
    main_body = f"Previous version {prev_version}.\n(Automatically generated with `scripts/version`)"
    branch_name = f"bump/v{next_version_str}"

    check_branch_not_exist(config, branch_name)

    def bump_create_branch_and_pr(repo: Repo, files, pr_body) -> "PullRequest":
        git.create_branch(
            branch_name, dir=repo, dry_run=config.dry_run,
        )
        git.switch(
            branch_name, dir=repo, dry_run=config.dry_run,
        )
        git.commit(
            msg=f"{title}\n\n{main_body}", files=files, dir=repo, dry_run=config.dry_run,
        )
        git.push(repo.origin_remote, branch_name, dir=repo, dry_run=config.dry_run)

        return g.create_pr(repo.upstream_url, repo.origin_url, branch_name, base, title, pr_body)

    open_pr = bump_create_branch_and_pr(config.open, files=[config.open.dir / CONFIG_VERSION_PATH], pr_body=main_body,)
    enterprise_pr = None
    if config.has_enterprise:
        enterprise_pr = bump_create_branch_and_pr(
            config.enterprise,
            files=[config.enterprise.dir / SUBMODULE_PATH],
            pr_body=f"After: {open_pr.base.repo.full_name}#{open_pr.number}\n\n{main_body}",
        )

    if config.has_enterprise:
        git.switch(enterprise_commit, config.enterprise, dry_run=config.dry_run)
    git.switch(open_commit, config.open, dry_run=config.dry_run)
    print("WARNING: Your local git repository will be left in a detached head state. Checkout any branch to fix this.")

    todos = [f"TODO: Review and merge {open_pr.html_url} as soon as possible."]
    if enterprise_pr:
        todos.append(
            f"""
TODO: Review {enterprise_pr.html_url} as soon as possible. Once the above PR is
    merged run 'scripts/version update_dependent_pr {enterprise_pr.number}' to update
    the PR based on the open commit hash and then merge it.""".strip()
        )
    return todos


def setup_bump_subcommand(subparsers):
    parser = subparsers.add_parser("bump", help="Bump the version.",)

    parser.add_argument("next_version", type=str)

    setup_global_log_arguments(parser)
    setup_global_repo_arguments(parser)
    setup_global_action_arguments(parser)

    parser.set_defaults(subcommand_impl=bump_subcommand)


PR_AFTER_RE = re.compile(
    r"""
After:\s*(
(?P<username>[\w.-]+)/(?P<repository>[\w.-]+)\#(?P<external_number>[0-9]+)|
\#(?P<internal_number>[0-9]+)
)
""",
    re.IGNORECASE | re.VERBOSE,
)


def update_dependent_pr_subcommand(args):
    config: Configuration = args.configuration

    check_remotes(config)
    check_clean(args, config)

    g = GithubFacade(config)

    enterprise_pr = g.get_pr(config.enterprise.upstream_url, number=args.number)
    if enterprise_pr.commits > 1:
        raise NotImplementedError(
            "update_dependent_pr only supports single commit PRs. (It could be implemented if needed.)"
        )
    after_match = PR_AFTER_RE.search(enterprise_pr.body)
    if not after_match:
        raise ValueError(
            f"PR {enterprise_pr.base.repo.full_name}#{enterprise_pr.number} does not have an 'After:' annotation."
        )
    if after_match.group("external_number"):
        repo_full_name = "{username}/{repository}".format(**after_match.groupdict())
        open_repo = g.github.get_repo(repo_full_name)
        open_pr = open_repo.get_pull(int(after_match.group("external_number")))
        if not open_pr.merged:
            raise StateError(f"The dependency {open_repo.full_name}#{open_pr.number} is not merged.")

        enterprise_original_branch = git.get_branch_checked_out(config.enterprise)
        open_original_branch = git.get_branch_checked_out(config.open)

        git.switch(enterprise_pr.head.ref, config.enterprise, config.dry_run)
        git.switch(open_pr.merge_commit_sha, config.open, config.dry_run)

        git.commit_amend([SUBMODULE_PATH], config.enterprise, config.dry_run)
        git.push(config.enterprise.origin_remote, enterprise_pr.head.ref, config.enterprise, config.dry_run, force=True)

        git.switch(enterprise_original_branch, config.enterprise, config.dry_run)
        git.switch(open_original_branch, config.open, config.dry_run)

        warn_dry_run(args)

        return [f"TODO: Merge {enterprise_pr.html_url} as soon as possible."]
    raise StateError(
        "PR does not have an acceptable 'After:' annotation. Only external PR references are supported. "
        f"(Was '{after_match.group(0)}')"
    )


def setup_update_dependent_pr_subcommand(subparsers):
    parser = subparsers.add_parser(
        "update_dependent_pr", help="Update an enterprise PR to match a merged PR in a submodule.",
    )

    parser.add_argument("number", help="The PR number in Github.", type=int)

    setup_global_log_arguments(parser)
    setup_global_repo_arguments(parser)
    setup_global_action_arguments(parser)

    parser.set_defaults(subcommand_impl=update_dependent_pr_subcommand)


def tag_stable_subcommand(args):
    print(args)
    config: Configuration = args.configuration
    ke_commit = args.commit

    check_remotes(config)
    check_clean(args, config)

    version = get_version(config, commit=ke_commit)
    tag = f"stable-{version}"

    # Tag both repos.
    git.tag_commit(tag, ke_commit, config.enterprise)
    k_commit = git.submodule_commit_at(SUBMODULE_PATH, ke_commit, config.enterprise)
    git.tag_commit(tag, k_commit, config.open)

    # Push both if explicitly requested.
    git.push(config.enterprise.origin_remote, tag, dir=config.enterprise, dry_run=config.dry_run)
    git.push(config.open.origin_remote, tag, dir=config.open, dry_run=config.dry_run)


def tag_subcommand(args):
    config: Configuration = args.configuration

    check_remotes(config)
    check_clean(args, config)

    commit = git.HEAD

    current_branch = get_current_branch_from_either_repository(config)
    kind = get_branch_kind(current_branch, (BranchKind.RELEASE, BranchKind.VARIANT))

    if (
        not git.is_ancestor_of(commit, f"{config.open.upstream_remote}/{current_branch}", config.open)
        and args.require_upstream
        and not args.pretend_upstream
    ):
        raise StateError(f"HEAD of {current_branch} is not upstream")

    if (
        not git.is_ancestor_of(commit, f"{config.enterprise.upstream_remote}/{current_branch}", config.enterprise)
        and args.require_upstream
        and not args.pretend_upstream
    ):
        raise StateError(f"HEAD of {current_branch} is not upstream")

    next_version = version.Version(args.version)

    check_branch_version(current_branch, kind, next_version, prev_version=None)

    tag_name = f"v{format_version_pep440(next_version)}"
    title = f"Version {format_version_pep440(next_version)}"

    g = GithubFacade(config)

    def tag_repo(repo: Repo):
        if git.is_ancestor_of(commit, f"{repo.upstream_remote}/{current_branch}", repo) or args.pretend_upstream:
            g.create_tag(repo.upstream_url, git.get_hash(commit, repo, pretend_clean=True), tag_name, message=title)
        else:
            raise NotImplementedError("To tag a release commit, the commit must already be in the upstream branch")
            # TODO(amp): This is complex and requires additional support. It's not clear we need it, so just disable it
            #  for now.
            # message_suffix = f"\n\nTag: {tag_name}"
            # git.amend_commit_message(
            #     git.get_commit_message(commit, repo_path).rstrip() + message_suffix, repo_path, dry_run=config.dry_run
            # )
            # branch_name = git.get_branch_checked_out(repo_path)
            # git.push(config.upstream_remote, branch_name, repo_path, dry_run=config.dry_run)
            # pr = g.get_pr(upstream_url, branch_name)
            # if pr:
            #     print(f"Review and merge {pr.url} as soon as possible.")
            # else:
            #     print(f"Create a PR for {repo_path.name}:{branch_name} and merge it as soon as possible.")

    tag_repo(config.open)
    if config.has_enterprise:
        tag_repo(config.enterprise)
    fetch_upstream(config)

    warn_dry_run(args)


def setup_tag_stable_subcommand(subparsers):
    parser = subparsers.add_parser("tag_stable", help="Tag stable commits.")

    parser.add_argument("commit", type=str)

    parser.set_defaults(subcommand_impl=tag_stable_subcommand)


def setup_tag_subcommand(subparsers):
    parser = subparsers.add_parser("tag", help="Tag HEAD as a version.",)

    parser.add_argument("version", type=str)

    parser.add_argument(
        "--require-upstream", help="Fail if the commit to be tagged isn't already upstream.", action="store_true"
    )

    setup_global_log_arguments(parser)
    setup_global_repo_arguments(parser)
    setup_global_action_arguments(parser)

    parser.set_defaults(subcommand_impl=tag_subcommand)


def release_subcommand(args):
    config: Configuration = args.configuration
    # Perform the checks that bump will do first. That way we will fail before tagging if possible.
    bump_checks(args)
    # Set some arguments for tag. This is a bit of a hack, but not worth the engineering to fix.
    ver, _ = get_explicit_version(git.HEAD, False, config.open, config.version_file, no_dev=True)
    args.version = str(ver)
    args.require_upstream = True
    tag_subcommand(args)

    warn_dry_run(args)
    return bump_subcommand(args)


def setup_release_subcommand(subparsers):
    parser = subparsers.add_parser("release", help="Tag HEAD as the current version and bump the version.",)

    parser.add_argument("next_version", type=str)

    setup_global_log_arguments(parser)
    setup_global_repo_arguments(parser)
    setup_global_action_arguments(parser)

    parser.set_defaults(subcommand_impl=release_subcommand)


def release_branch_subcommand(args):
    config: Configuration = args.configuration
    check_clean(args, config)
    if not args.allow_arbitrary_branch:
        current_branch = get_current_branch_from_either_repository(config)
        get_branch_kind(current_branch, [BranchKind.MASTER])
        check_at_branch("master", config)
        if config.has_enterprise:
            git.switch("master", config.enterprise, config.dry_run)
        else:
            # Do not switch branches in open if we did so in enterprise. This allows external/katana to lag
            # at branch time.
            git.switch("master", config.open, config.dry_run)
    else:
        print(
            "WARNING: Branching from HEAD instead of upstream/master. Be careful! This will create an out of date "
            "release branch."
        )

    # Check if HEAD is on the upstream master branch.
    if (
        not git.is_ancestor_of(git.HEAD, f"{config.open.upstream_remote}/master", config.open)
        and not args.pretend_upstream
    ):
        raise StateError(f"{config.open.dir} HEAD is not on upstream master")

    if (
        config.has_enterprise
        and not git.is_ancestor_of(git.HEAD, f"{config.enterprise.upstream_remote}/master", config.enterprise)
        and not args.pretend_upstream
    ):
        raise StateError(f"{config.enterprise.dir} HEAD is not on upstream master")

    prev_version, _ = get_explicit_version(git.HEAD, True, config.open, config.version_file, no_dev=True)
    next_version = version.Version(args.next_version)
    rc_version = version.Version(f"{prev_version}rc1")

    # Always pretend we are on master. We either actually are, or the user has overridden things.
    check_branch_version("master", BranchKind.MASTER, next_version, prev_version)

    g = GithubFacade(config)

    # Create release branches.
    release_branch_name = f"release/v{format_version_pep440(prev_version)}"
    check_branch_version(release_branch_name, BranchKind.RELEASE, rc_version, add_dev_to_version(prev_version))
    g.create_branch(
        config.open.upstream_url, git.get_hash(git.HEAD, config.open, pretend_clean=True), release_branch_name,
    )
    if config.has_enterprise:
        g.create_branch(
            config.enterprise.upstream_url,
            git.get_hash(git.HEAD, config.enterprise, pretend_clean=True),
            release_branch_name,
        )

    # Create a PR on master which updates the version.txt to {next version}.
    todos = bump_both_repos(config, g, prev_version, next_version, "master")
    # Create a PR on the release branch which updates the version.txt to {version}rc1.
    todos.extend(bump_both_repos(config, g, prev_version, rc_version, release_branch_name))

    warn_dry_run(args)
    return todos


def check_clean(args, config):
    if not config.open:
        raise StateError("Action cannot run in a source tree that is not a git clone.")

    is_dirty = git.is_dirty(config.open) or (config.has_enterprise and git.is_dirty(config.enterprise))
    if not args.clean and is_dirty:
        raise StateError("Action only supported in clean repositories. (Stash your changes.)")


def setup_release_branch_subcommand(subparsers):
    parser = subparsers.add_parser(
        "release_branch",
        help="Create the release branch for an upcoming release and create the versioning commits around it.",
    )

    parser.add_argument(
        "--allow-arbitrary-branch",
        help="Allow creating a release branch from any commit instead of just from master. "
        "This should be used with care.",
        action="store_true",
    )
    parser.add_argument("next_version", type=str)

    setup_global_log_arguments(parser)
    setup_global_repo_arguments(parser)
    setup_global_action_arguments(parser)

    parser.set_defaults(subcommand_impl=release_branch_subcommand)


def setup_global_repo_arguments(parser, *, top_level=False):
    parser.add_argument(
        "--fetch",
        help="Fetch upstream before doing other operations (default)",
        action="store_true",
        default=None if top_level else argparse.SUPPRESS,
    )
    parser.add_argument(
        "--no-fetch",
        help="Do not fetch upstream before doing other operations",
        action="store_false",
        dest="fetch",
        default=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--pretend-upstream",
        help="Pretend the commit is already on upstream master. (For testing only!)"
        if top_level
        else argparse.SUPPRESS,
        action="store_true",
    )
    parser.add_argument(
        "--pretend-clean",
        help="Pretend that the working tree is clean. (For testing only!)" if top_level else argparse.SUPPRESS,
        dest="clean",
        action="store_true",
    )
    parser.add_argument(
        "--open-only",
        "--open",
        help="Ignore any enclosing enterprise repository." if top_level else argparse.SUPPRESS,
        dest="open",
        action="store_true",
        default=False if top_level else argparse.SUPPRESS,
    )
    parser.add_argument(
        "--katana",
        help="Explicit path to katana repository checkout." if top_level else argparse.SUPPRESS,
        type=str,
        default=None if top_level else argparse.SUPPRESS,
    )
    parser.add_argument(
        "--katana-enterprise",
        help="Explicit path to katana-enterprise repository checkout." if top_level else argparse.SUPPRESS,
        type=str,
        default=None if top_level else argparse.SUPPRESS,
    )


def setup_global_action_arguments(parser, *, top_level=False):
    parser.add_argument(
        "--really",
        help="Perform the real operations on local and remote repositories." if top_level else argparse.SUPPRESS,
        action="store_false",
        dest="dry_run",
        default=True if top_level else argparse.SUPPRESS,
    )
    parser.add_argument(
        "--dry-run",
        "-n",
        help="Don't actually do anything, just print what would have been done. [default]"
        if top_level
        else argparse.SUPPRESS,
        action="store_true",
        default=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--github-token",
        help="Github access token (environment variable: GITHUB_TOKEN)" if top_level else argparse.SUPPRESS,
        dest="access_token",
        type=str,
        default=environ.get("GITHUB_TOKEN", None) if top_level else argparse.SUPPRESS,
    )
    parser.add_argument(
        "--github-username",
        help="Github username (environment variable: GITHUB_USERNAME)" if top_level else argparse.SUPPRESS,
        dest="username",
        type=str,
        default=environ.get("c", None) if top_level else argparse.SUPPRESS,
    )
    parser.add_argument(
        "--github-password",
        help="Github password (environment variable: GITHUB_PASSWORD)" if top_level else argparse.SUPPRESS,
        dest="password",
        type=str,
        default=environ.get("GITHUB_PASSWORD", None) if top_level else argparse.SUPPRESS,
    )


def setup_global_log_arguments(parser, top_level=False):
    parser.add_argument(
        "--log",
        help="Set the python log level." if top_level else argparse.SUPPRESS,
        default="WARNING" if top_level else argparse.SUPPRESS,
    )
    parser.add_argument(
        "--verbose",
        "-v",
        help="Increase the logging verbosity." if top_level else argparse.SUPPRESS,
        action="count",
        default=0 if top_level else argparse.SUPPRESS,
    )


def main():
    parser = argparse.ArgumentParser(
        prog="scripts/version",
        description="""
Manage version numbers of Katana packages.
This program assumes that your checkouts have the same name as the github repository they are clones of.
""",
    )
    setup_global_log_arguments(parser, top_level=True)
    setup_global_repo_arguments(parser, top_level=True)
    setup_global_action_arguments(parser, top_level=True)

    subparsers = parser.add_subparsers(title="subcommands")

    setup_show_subcommand(subparsers)
    setup_provenance_subcommand(subparsers)
    setup_bump_subcommand(subparsers)
    setup_tag_subcommand(subparsers)
    setup_tag_stable_subcommand(subparsers)
    setup_release_subcommand(subparsers)
    setup_release_branch_subcommand(subparsers)
    setup_update_dependent_pr_subcommand(subparsers)

    args = parser.parse_args()

    logging.basicConfig(level=str(args.log).upper().strip())
    logging.root.setLevel(logging.root.level - args.verbose * 10)

    args.configuration = Configuration(args)

    if args.fetch or not args.dry_run:
        fetch_upstream(args.configuration)

    if hasattr(args, "subcommand_impl"):
        try:
            execute_subcommand(args)
        except (RuntimeError, ValueError, NotImplementedError, CommandError, InvalidVersion) as e:
            # If at first we don't succeed, fetch the upstream remote and try again.
            logger.debug("Exception", exc_info=True)
            print("INFO: Fetching upstream remote.")
            fetch_upstream(args.configuration)
            try:
                execute_subcommand(args)
            except (RuntimeError, ValueError, NotImplementedError, CommandError, InvalidVersion):
                logger.debug("Exception", exc_info=True)
                print(f"ERROR({type(e).__name__}): {str(e)}")
    else:
        parser.print_help()


def execute_subcommand(args):
    todos = args.subcommand_impl(args)
    if todos:
        print("=========== TODOS FOR THE DEVELOPER ===========")
        print("\n".join(todos))


def warn_dry_run(args):
    if args.dry_run:
        print(
            "WARNING: This was a dry-run. Nothing was actually done. Once you are comfortable with the actions this "
            "script will take, call it with --really."
        )


def fetch_upstream(config: Configuration):
    if not config.has_git:
        return
    # Do fetches in parallel since they run at the start of many commands and are totally separate.
    thread = None
    if config.has_enterprise and config.enterprise.upstream_remote:
        thread = threading.Thread(
            target=lambda: git.fetch(
                config.enterprise.upstream_remote, dir=config.enterprise, tags=True, dry_run=False, log=False,
            )
        )
        thread.start()
    if config.open.upstream_remote:
        git.fetch(config.open.upstream_remote, dir=config.open, tags=True, dry_run=False, log=False)
    if thread:
        thread.join()


if __name__ == "__main__":
    main()
