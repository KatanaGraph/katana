import argparse
import logging
import os
import platform
import re
import threading
from enum import Enum
from os import environ
from pathlib import Path
from subprocess import CalledProcessError
from typing import Iterable, Optional

from packaging import version
from packaging.version import InvalidVersion

from .github import GithubFacade
from . import CONFIG_VERSION_PATH, Configuration, git, GitURL, StateError, SUBMODULE_PATH
from .commands import capture_command, CommandError
from .version import (
    add_dev_to_version,
    format_version_debian,
    format_version_pep440,
    get_explicit_version,
    get_version,
)

logger = logging.getLogger(__name__)


class BranchKind(Enum):
    MASTER = "master"
    RELEASE = "release/v.*"
    VARIANT = "variant/.*"


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

    environment_to_capture = {"CONDA_PREFIX"}

    for var in environment_to_capture:
        if var in environ:
            values[var.lower()] = environ[var]

    config = args.configuration
    katana_repo_root = config.katana_repo_path
    katana_enterprise_repo_path = config.katana_enterprise_repo_path

    values.update(katana_repo_root=katana_repo_root.absolute())
    values.update(katana_branch=git.get_branch_checked_out(katana_repo_root))
    values.update(katana_upstream=config.upstream_url)
    values.update(katana_origin=config.origin_url)
    values.update(katana_hash=git.get_hash(git.HEAD, katana_repo_root))

    if config.has_enterprise:
        values.update(katana_repo_root=katana_enterprise_repo_path.absolute())
        values.update(katana_enterprise_branch=git.get_branch_checked_out(katana_enterprise_repo_path))
        values.update(katana_enterprise_upstream=config.enterprise_upstream_url)
        values.update(katana_enterprise_origin=config.enterprise_origin_url)
        values.update(
            katana_enterprise_hash=git.get_hash(git.HEAD, katana_enterprise_repo_path, exclude_dirty=(SUBMODULE_PATH,))
        )

    format_str = args.format
    format_str = format_str.replace("\\n", "\n").replace("\\t", "\t")
    print("".join(format_str.format(k, v, k=k, K=k.upper(), v=v) for k, v in values.items()), end="")


def setup_provenance_subcommand(subparsers):
    parser = subparsers.add_parser(
        "provenance", help="Prints a provenance description for inclusion in artifacts. This is not a version.",
    )

    group_format = parser.add_mutually_exclusive_group()
    group_format.add_argument(
        "--define", help="Format as #defines.", dest="format", action="store_const", const='#define {K} "{v}" \n'
    )
    group_format.add_argument(
        "--yaml", help="Format as YAML.", dest="format", action="store_const", const='{k}: "{v}"\n'
    )
    group_format.add_argument(
        "--python", help="Format as Python.", dest="format", action="store_const", const='{k} = "{v}"\n'
    )
    group_format.add_argument(
        "--format", "-f", help="Provide a format string for each value. Use the source luck.", dest="format", type=str
    )

    setup_global_repo_arguments(parser)

    parser.set_defaults(subcommand_impl=provenance_subcommand, format='{k}: "{v}"\n')


def bump_checks(args):
    config: Configuration = args.configuration
    check_clean(args, config)

    current_branch = get_current_branch_from_either_repository(config)
    kind = get_branch_kind(current_branch, (BranchKind.MASTER, BranchKind.RELEASE, BranchKind.VARIANT))
    check_at_branch(f"{config.upstream_remote}/{current_branch}", config)
    git.switch(current_branch, config.katana_enterprise_repo_path, config.dry_run)
    git.switch(current_branch, config.katana_repo_path, config.dry_run)

    prev_version, variant = get_explicit_version(config, git.HEAD, True, config.katana_repo_path, no_dev=False)
    next_version = version.Version(args.next_version)

    check_branch_version(current_branch, kind, next_version, prev_version)


def get_current_branch_from_either_repository(config):
    current_branch = git.get_branch_checked_out(config.katana_repo_path, ref_only=True)
    if config.has_enterprise:
        current_branch = current_branch or git.get_branch_checked_out(config.katana_enterprise_repo_path, ref_only=True)
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
            f"The semantic version does not match the release branch name: {expected_release_branch_name} != {current_branch}"
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
            f"The variant in the version and the variant in the branch name must be the same: {next_version.local} != {branch_variant}"
        )
    if prev_version and next_version <= prev_version:
        raise ValueError(f"The next version ({next_version}) must be greater than the current one ({prev_version})")
    if prev_version and kind == BranchKind.VARIANT and next_version.release != prev_version.release:
        raise ValueError(
            "To change the version of a variant branch, merge master into the variant branch. Bumping the version directly on the variant branch is not allowed."
        )


def get_branch_kind(current_branch, kinds: Iterable[BranchKind]):
    for kind in kinds:
        if re.match(kind.value, current_branch):
            return kind
    kinds_str = ", ".join(k.value for k in kinds)
    raise StateError(f"The current branch ({current_branch}) should be one of: {kinds_str}")


def check_at_branch(branch, config):
    if git.get_hash(branch, config.katana_repo_path) != git.get_hash(git.HEAD, config.katana_repo_path):
        raise StateError(f"{config.katana_repo_path} HEAD is up to date with {branch}")

    if config.has_enterprise and git.get_hash(branch, config.katana_enterprise_repo_path) != git.get_hash(
        git.HEAD, config.katana_enterprise_repo_path
    ):
        raise StateError(f"{config.katana_enterprise_repo_path} HEAD is up to date with {branch}")


def bump_subcommand(args):
    bump_checks(args)

    config: Configuration = args.configuration

    g = GithubFacade(config)

    prev_version, variant = get_explicit_version(config, git.HEAD, True, config.katana_repo_path, no_dev=True)
    next_version = version.Version(args.next_version)

    current_branch = git.get_branch_checked_out(config.katana_repo_path)
    return bump_both_repos(config, g, prev_version, next_version, current_branch)


def check_branch_not_exist(config: Configuration, branch_name):
    if git.ref_exists(branch_name, config.katana_repo_path):
        raise StateError(f"Branch {branch_name} already exists in {config.katana_repo_path}")
    if git.ref_exists(branch_name, config.katana_enterprise_repo_path):
        raise StateError(f"Branch {branch_name} already exists in {config.katana_enterprise_repo_path}")


def bump_both_repos(config: Configuration, g: GithubFacade, prev_version, next_version, base):
    next_version_str = format_version_pep440(next_version)
    current_branch = git.get_branch_checked_out(config.katana_repo_path)
    if config.dry_run:
        print(next_version_str)
    else:
        with open(config.katana_repo_path / CONFIG_VERSION_PATH, "wt", encoding="utf-8") as fi:
            fi.write(next_version_str)
            fi.write("\n")
    title = f"Bump version to {next_version} on {base}"
    main_body = f"Previous version {prev_version}.\n(Automatically generated with `scripts/version`)"
    branch_name = f"bump/v{next_version_str}"

    check_branch_not_exist(config, branch_name)

    def bump_create_branch_and_pr(
        repo_root: Path, upstream_url: GitURL, origin_url: GitURL, files, pr_body
    ) -> "PullRequest":
        git.create_branch(
            branch_name, dir=repo_root, dry_run=config.dry_run,
        )
        git.switch(
            branch_name, dir=repo_root, dry_run=config.dry_run,
        )
        git.commit(
            msg=f"{title}\n\n{main_body}", files=files, dir=repo_root, dry_run=config.dry_run,
        )
        git.push(config.origin_remote, branch_name, dir=repo_root, dry_run=config.dry_run)

        return g.create_pr(upstream_url, origin_url, branch_name, base, title, pr_body)

    open_pr = bump_create_branch_and_pr(
        config.katana_repo_path,
        config.upstream_url,
        config.origin_url,
        files=[config.katana_repo_path / CONFIG_VERSION_PATH],
        pr_body=main_body,
    )
    enterprise_pr = None
    if config.has_enterprise:
        enterprise_pr = bump_create_branch_and_pr(
            config.katana_enterprise_repo_path,
            config.enterprise_upstream_url,
            config.enterprise_origin_url,
            files=[config.katana_enterprise_repo_path / SUBMODULE_PATH],
            pr_body=f"After: {open_pr.base.repo.full_name}#{open_pr.number}\n\n{main_body}",
        )

    git.switch(current_branch, config.katana_enterprise_repo_path, dry_run=config.dry_run)
    git.switch(current_branch, config.katana_repo_path, dry_run=config.dry_run)

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

    check_clean(args, config)

    g = GithubFacade(config)

    enterprise_pr = g.get_pr(config.enterprise_upstream_url, number=args.number)
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

        enterprise_original_branch = git.get_branch_checked_out(config.katana_enterprise_repo_path)
        open_original_branch = git.get_branch_checked_out(config.katana_enterprise_repo_path)

        git.switch(enterprise_pr.head.ref, config.katana_enterprise_repo_path, config.dry_run)
        git.switch(open_pr.merge_commit_sha, config.katana_repo_path, config.dry_run)

        git.commit_amend([SUBMODULE_PATH], config.katana_enterprise_repo_path, config.dry_run)
        git.push(
            config.origin_remote, enterprise_pr.head.ref, config.katana_enterprise_repo_path, config.dry_run, force=True
        )

        git.switch(enterprise_original_branch, config.katana_enterprise_repo_path, config.dry_run)
        git.switch(open_original_branch, config.katana_repo_path, config.dry_run)

        return [f"TODO: Merge {enterprise_pr.html_url} as soon as possible."]
    else:
        raise StateError(
            "PR does not have an acceptable 'After:' annotation. Only external PR references are supported. "
            f"(Was '{after_match.group(0)}')"
        )


def setup_update_dependent_pr_subcommand(subparsers):
    parser = subparsers.add_parser(
        "update_dependent_pr", help="Update an enterprise PR to match a merged PR in a submodule.",
    )

    parser.add_argument("number", help="The PR number in Github.", type=int)

    setup_global_repo_arguments(parser)
    setup_global_action_arguments(parser)

    parser.set_defaults(subcommand_impl=update_dependent_pr_subcommand)


def tag_subcommand(args):
    config: Configuration = args.configuration
    check_clean(args, config)

    commit = git.HEAD

    current_branch = get_current_branch_from_either_repository(config)
    kind = get_branch_kind(current_branch, (BranchKind.RELEASE, BranchKind.VARIANT))

    if (
        not git.is_ancestor_of(commit, f"{config.upstream_remote}/{current_branch}", config.katana_repo_path)
        and args.require_upstream
        and not args.pretend_upstream
    ):
        raise StateError(f"HEAD of {current_branch} is not upstream")

    if (
        not git.is_ancestor_of(commit, f"{config.upstream_remote}/{current_branch}", config.katana_enterprise_repo_path)
        and args.require_upstream
        and not args.pretend_upstream
    ):
        raise StateError(f"HEAD of {current_branch} is not upstream")

    next_version = version.Version(args.version)

    check_branch_version(current_branch, kind, next_version, prev_version=None)

    tag_name = f"v{format_version_pep440(next_version)}"
    title = f"Version {format_version_pep440(next_version)}"

    g = GithubFacade(config)

    def tag_repo(repo_path: Path, upstream_url: GitURL):
        if git.is_ancestor_of(commit, f"{config.upstream_remote}/{current_branch}", repo_path) or args.pretend_upstream:
            g.create_tag(upstream_url, git.get_hash(commit, repo_path, pretend_clean=True), tag_name, message=title)
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

    tag_repo(config.katana_repo_path, config.upstream_url)
    if config.has_enterprise:
        tag_repo(config.katana_enterprise_repo_path, config.enterprise_upstream_url)
    fetch_upstream(config)


def setup_tag_subcommand(subparsers):
    parser = subparsers.add_parser("tag", help="Tag HEAD as a version.",)

    parser.add_argument("version", type=str)

    parser.add_argument(
        "--pretend-upstream",
        help=argparse.SUPPRESS,  # Pretend the commit is already up stream. (for testing)
        action="store_true",
    )
    parser.add_argument(
        "--require-upstream", help="Fail if the commit to be tagged isn't already upstream.", action="store_true"
    )

    setup_global_repo_arguments(parser)
    setup_global_action_arguments(parser)

    parser.set_defaults(subcommand_impl=tag_subcommand)


def release_subcommand(args):
    # Perform the checks that bump will do first. That way we will fail before tagging if possible.
    bump_checks(args)
    # Set some arguments for tag. This is a bit of a hack, but not worth the engineering to fix.
    ver, variant = get_explicit_version(git.HEAD, False, args.configuration.katana_repo_path, no_dev=True)
    args.version = str(ver)
    args.require_upstream = True
    tag_subcommand(args)
    return bump_subcommand(args)


def setup_release_subcommand(subparsers):
    parser = subparsers.add_parser("release", help="Tag HEAD as the current version and bump the version.",)

    parser.add_argument("next_version", type=str)

    parser.add_argument(
        "--pretend-upstream",
        help=argparse.SUPPRESS,  # Pretend the commit is already up stream. (for testing)
        action="store_true",
    )

    setup_global_repo_arguments(parser)
    setup_global_action_arguments(parser)

    parser.set_defaults(subcommand_impl=release_subcommand)


def release_branch_subcommand(args):
    config: Configuration = args.configuration
    check_clean(args, config)
    current_branch = get_current_branch_from_either_repository(config)
    get_branch_kind(current_branch, [BranchKind.MASTER])
    check_at_branch(f"{config.upstream_remote}/master", config)
    git.switch("master", config.katana_enterprise_repo_path, config.dry_run)
    git.switch("master", config.katana_repo_path, config.dry_run)

    prev_version, variant = get_explicit_version(config, git.HEAD, True, config.katana_repo_path, no_dev=True)
    next_version = version.Version(args.next_version)
    rc_version = version.Version(f"{prev_version}rc1")

    check_branch_version("master", BranchKind.MASTER, next_version, prev_version)

    g = GithubFacade(config)

    # Create release branches.
    release_branch_name = f"release/v{format_version_pep440(prev_version)}"
    check_branch_version(release_branch_name, BranchKind.RELEASE, rc_version, add_dev_to_version(prev_version))
    g.create_branch(
        config.upstream_url, git.get_hash(git.HEAD, config.katana_repo_path, pretend_clean=True), release_branch_name,
    )
    g.create_branch(
        config.enterprise_upstream_url,
        git.get_hash(git.HEAD, config.katana_enterprise_repo_path, pretend_clean=True),
        release_branch_name,
    )

    # Create a PR on master which updates the version.txt to {next version}.
    todos = bump_both_repos(config, g, prev_version, next_version, "master")
    # Create a PR on the release branch which updates the version.txt to {version}rc1.
    todos.extend(bump_both_repos(config, g, prev_version, rc_version, release_branch_name))
    return todos


def check_clean(args, config):
    is_dirty = git.is_dirty(config.katana_repo_path) or (
        config.has_enterprise and git.is_dirty(config.katana_enterprise_repo_path)
    )
    if not args.clean and is_dirty:
        raise StateError("Action only supported in clean repositories. (Stash your changes.)")


def setup_release_branch_subcommand(subparsers):
    parser = subparsers.add_parser(
        "release_branch",
        help="Create the release branch for an upcoming release and create the versioning commits around it.",
    )

    parser.add_argument("next_version", type=str)

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
        "--pretend-clean",
        help="Pretend that the working tree is clean." if top_level else argparse.SUPPRESS,
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


def main():
    parser = argparse.ArgumentParser(
        prog="scripts/version",
        description="""
Manage version numbers of Katana packages.
This program assumes a number of things:
Your checkouts have the same name as the github repository they are clones of;
Your "working" fork remote (the remove used to create PRs) is called 'origin' and the upstream remote (if different from origin) is called 'upstream'.
""",
    )
    parser.add_argument("--log", help="Set the python log level.", default="WARNING")
    setup_global_repo_arguments(parser, top_level=True)
    setup_global_action_arguments(parser, top_level=True)

    subparsers = parser.add_subparsers(title="subcommands")

    setup_show_subcommand(subparsers)
    setup_provenance_subcommand(subparsers)
    setup_bump_subcommand(subparsers)
    setup_tag_subcommand(subparsers)
    setup_release_subcommand(subparsers)
    setup_release_branch_subcommand(subparsers)
    setup_update_dependent_pr_subcommand(subparsers)

    args = parser.parse_args()

    logging.basicConfig(level=args.log)

    args.configuration = Configuration(args)

    if args.fetch:
        fetch_upstream(args.configuration)

    if hasattr(args, "subcommand_impl"):
        try:
            todos = args.subcommand_impl(args)
            if todos:
                print("=========== TODOS FOR THE DEVELOPER ===========")
                print("\n".join(todos))
        except (RuntimeError, ValueError, NotImplementedError, CommandError, InvalidVersion) as e:
            logger.debug("Exception", exc_info=True)
            print(f"ERROR({type(e).__name__}): {str(e)}")
    else:
        parser.print_help()


def fetch_upstream(config):
    if not config.has_git:
        return
    # Do fetches in parallel since they run at the start of many commands and are totally separate.
    thread = None
    if config.has_enterprise:
        thread = threading.Thread(
            target=lambda: git.fetch(
                config.upstream_remote, dir=config.katana_enterprise_repo_path, tags=True, dry_run=False, log=False,
            )
        )
        thread.start()
    git.fetch(config.upstream_remote, dir=config.katana_repo_path, tags=True, dry_run=False, log=False)
    if thread:
        thread.join()


if __name__ == "__main__":
    main()
