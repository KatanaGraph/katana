import argparse
import re
import threading
from enum import Enum
from os import environ
from typing import Iterable, Optional

from katana_version import CONFIG_VERSION_PATH, SUBMODULE_PATH, Configuration, Repo, StateError, git
from katana_version.github import GithubFacade
from katana_version.version import add_dev_to_version, format_version_pep440, get_explicit_version, get_version
from packaging import version


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
