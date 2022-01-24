import logging
import re
import warnings
from pathlib import Path
from typing import Optional, Tuple, Union

from packaging import version
from packaging.version import Version

from . import CONFIG_VERSION_PATH, SUBMODULE_PATH, Configuration, Repo, git
from .commands import CommandError, capture_command

__all__ = [
    "get_version",
    "format_version_pep440",
    "format_version_debian",
    "format_version_semantic",
    "add_dev_to_version",
]

logger = logging.getLogger(__name__)

VERSION_TAG_RE = re.compile(r"v(?P<version>[^a-zA-Z][.0-9+a-zA-Z]+)")


def get_version(
    config: Configuration = None, commit=None, variant=None, *, pretend_master=False, pretend_clean=False
) -> Version:
    """
    Get the version of the code in the repositories specified in config. This will always return a version (even if
    it's incomplete) for the working copy (`commit` = None). It may throw an exception if the version of a specific
    commit is requested.
    :param config: A description of the repositories to analyze.
    :param commit: The commit to get the version of, or None to get the version of the working copy.
    :param variant: The build variant. This may only be used if the source code does not specify a variant.
    :return: The version of the Katana package.
    """
    config = config or Configuration()

    # Find commits in both repos.
    if commit is None:
        use_working_copy = True
        k_commit = None
        ke_commit = None
    else:
        use_working_copy = False
        here = git.get_working_tree(Path.cwd())
        if here == config.open.dir:
            k_commit = git.get_hash(commit, config.open, pretend_clean=True)
            if config.has_enterprise:
                logger.warning(
                    f"Computing historic versions based on an {SUBMODULE_PATH} commit is limited. Producing "
                    "open-source build version."
                )
                config.enterprise = None
            ke_commit = None
        elif here == config.enterprise.dir:
            ke_commit = commit
            k_commit = git.submodule_commit_at(SUBMODULE_PATH, ke_commit, config.enterprise)
        else:
            raise ValueError(
                "To specify a commit you must be in either katana or katana-enterprise to tell me which repo you want "
                "to use to find the commit."
            )

    k_commit = k_commit or "HEAD"
    ke_commit = ke_commit or "HEAD"

    original_k_commit = k_commit
    original_ke_commit = ke_commit

    if config.has_git:
        k_commit = git.simplify_merge_commit(k_commit, config.open)
        if config.has_enterprise:
            ke_commit = git.simplify_merge_commit(ke_commit, config.enterprise)

    k_explicit_version, variant = get_explicit_version(
        k_commit, use_working_copy, config.open, config.version_file, variant, other_commits=[original_k_commit]
    )
    ke_tag_version = None
    if config.has_enterprise and (not git.is_dirty(config.enterprise) or pretend_clean):
        ke_tag_version = get_tag_version(ke_commit or "HEAD", config.enterprise)
        ke_tag_version = ke_tag_version or get_tag_version(original_ke_commit or "HEAD", config.enterprise)

    if k_explicit_version.is_devrelease:
        if not ke_tag_version:
            explicit_version = add_dev_to_version(k_explicit_version)
        else:
            warnings.warn(
                "The enterprise repo is tagged, but the open repo is a dev version. Using the enterprise "
                "tag, but please check the repo state."
            )
            explicit_version = ke_tag_version
    else:
        explicit_version = k_explicit_version

    if pretend_master or not use_working_copy:
        pretend_clean = True

    if pretend_master:
        open_core_branch = Repo.remote_branch(config.open.upstream_remote, "master")
        enterprise_core_branch = None
        if config.has_enterprise:
            enterprise_core_branch = Repo.remote_branch(config.enterprise.upstream_remote, "master")
        is_merged = True
    else:
        is_enterprise_merged = True
        enterprise_core_branch = None
        if config.has_enterprise:
            enterprise_core_branch = git_find_closest_core_branch(ke_commit, config.enterprise)
            is_enterprise_merged = enterprise_core_branch and git.is_ancestor_of(
                ke_commit, enterprise_core_branch, dir=config.enterprise
            )
        open_core_branch = git_find_closest_core_branch(k_commit, config.open)
        is_merged = open_core_branch and git.is_ancestor_of(k_commit, open_core_branch, dir=config.open)
        is_merged = is_enterprise_merged and is_merged

    k_count = None
    k_hash = None
    ke_count = None
    ke_hash = None
    if config.has_git:
        k_last_version_commit = git.find_change(config.open.dir / CONFIG_VERSION_PATH, k_commit, config.open)
        k_count = compute_commit_count(k_commit, k_last_version_commit, config.open, pretend_master, open_core_branch)
        k_hash = git.get_hash(k_commit, config.open, pretend_clean=pretend_clean, abbrev=6)

        if config.has_enterprise:
            ke_last_version_commit = git_find_super_commit(
                k_last_version_commit, ke_commit, config.enterprise, config.open.dir
            )
            ke_count = (
                compute_commit_count(
                    ke_commit, ke_last_version_commit, config.enterprise, pretend_master, enterprise_core_branch,
                )
                if ke_last_version_commit
                else "xxx"
            )
            ke_hash = git.get_hash(
                ke_commit, config.enterprise, pretend_clean=pretend_clean, exclude_dirty=(SUBMODULE_PATH,), abbrev=6,
            )

    if len(explicit_version.release) != 3:
        raise ValueError(
            f"Versions must have 3 main components (x.y.z). The version specified {explicit_version} "
            f"(in a git tag, version.txt, or environment variable) has {len(explicit_version.release)} "
            f"main components."
        )

    computed_version = katana_version(
        *explicit_version.release,
        k_count,
        ke_count,
        k_hash,
        ke_hash,
        variant=variant,
        dev=explicit_version.is_devrelease,
        pre=explicit_version.pre,
        post=explicit_version.post,
        is_merged=is_merged and ke_hash != "DIRTY" and k_hash != "DIRTY",
    )
    if config.version_from_environment_variable:
        env_version = config.version_from_environment_variable
        if env_version.release != computed_version.release:
            logger.warning(
                "The KATANA_VERSION environment variable does not match the version in the source code: "
                f"{env_version} does not match {computed_version}"
            )
        return env_version
    return computed_version


def git_find_closest_core_branch(commit, repo: Repo):
    if not repo:
        return None

    branch_patterns = [
        Repo.remote_branch(repo.upstream_remote, "master"),
        Repo.remote_branch(repo.upstream_remote, "release/v*"),
        Repo.remote_branch(repo.upstream_remote, "variant/*"),
    ]
    branches = [
        b
        for pat in branch_patterns
        for b in git.find_branches(
            pat, dir=repo, prefix="remotes" if repo.upstream_remote else "heads", sort="-creatordate"
        )
    ]

    if not branches:
        return None

    def branch_ahead_count(branch):
        return git.get_commit_count(git.merge_base(commit, branch, repo), commit, dir=repo)

    nearest_branch = min(branches, key=branch_ahead_count)
    return nearest_branch


def get_explicit_version(
    k_commit: str, use_working_copy: bool, repo, version_file, variant=None, no_dev=False, *, other_commits=()
):
    # A generator for versions produced from tags on commits.
    all_tag_versions = (get_tag_version(c, repo) for c in (k_commit,) + tuple(other_commits))
    # Remove all Nones (filter(None, ...)) and get the first one, returning None if there are no versions found.
    tag_version = next(filter(None, all_tag_versions), None)
    explicit_version = tag_version or get_config_version(
        None if use_working_copy else k_commit, repo, version_file, no_dev=no_dev
    )
    if explicit_version.local and variant and variant != explicit_version.local:
        logger.warning(
            f"You are overriding the repository variant {explicit_version.local} with build-time variant {variant}."
        )
    variant = variant or explicit_version.local
    return explicit_version, variant


def get_config_version(k_commit, repo: Repo, version_file, no_dev=False) -> version.Version:
    if repo:
        if k_commit:
            version_str = capture_command("git", *git.dir_arg(repo), "show", "-p", f"{k_commit}:{CONFIG_VERSION_PATH}")
        else:
            with open(repo.dir / CONFIG_VERSION_PATH, "rt") as version_fi:
                version_str = version_fi.read()
    elif version_file:
        # We have no git information. Wing it.
        with open(version_file, "rt") as version_fi:
            version_str = version_fi.read()
    else:
        # We have no information. Something is really broken. Still don't crash to allow builds.
        version_str = "0.0.0"
    ver = version.Version(version_str.strip())

    if no_dev:
        return ver

    return add_dev_to_version(ver)


def get_tag_version(commit, repo: Repo):
    if not repo or not commit:
        return None
    tag_version = None
    version_tags = [m for m in (VERSION_TAG_RE.match(t) for t in git.get_tags_of(commit, repo)) if m]
    if len(version_tags) > 1:
        logger.warning("There is more than one version tag at the given commit. Picking one arbitrarily.")
    if version_tags:
        tag_version = version.Version(version_tags[0].group("version"))
    return tag_version


def compute_commit_count(commit, last_version_commit, repo_path, pretend_master, core_branch):
    if not pretend_master:
        if not core_branch:
            logger.warning(
                f"Cannot determine the commit count at {commit} (replacing with 'x'). Make sure you have git history "
                f"on master, release, and variant branches back to the last change to 'config/version.txt' to avoid "
                f"this issue."
            )
            return "x"
        last_core_commit = git.merge_base(commit, core_branch, repo_path)
    else:
        last_core_commit = commit
    return git.get_commit_count(last_version_commit, last_core_commit, repo_path)


def git_find_super_commit(
    submodule_commit_to_find, super_commit, super_repo_path: Repo, sub_repo_path: Union[Path, str]
):
    """
    Find the super module commit which introduced the provided submodule commit.

    :return: The super commit hash.
    """
    submodule_changes = git.find_changes(sub_repo_path, super_commit, super_repo_path, n=None)

    for i, commit in enumerate(submodule_changes):
        submodule_commit = git.submodule_commit_at(SUBMODULE_PATH, commit, dir=super_repo_path)
        try:
            if git.is_ancestor_of(submodule_commit_to_find, submodule_commit, dir=sub_repo_path):
                continue
        except CommandError as e:
            if "is a tree" in str(e):
                logger.warning(
                    f"Reached repository restructure commit ({submodule_commit}). Picking that commit as version "
                    f"change commit. This is weird, but provides semi-useful versions for commits before the "
                    f"versioning system was introduced fully."
                )
                return commit
            logger.info(f"Encountered bad commit in {super_repo_path.dir.name}. Skipping. Error: {e}")
            continue
        # submodule_commit_to_find is not an ancestor of commit
        return submodule_changes[i - 1]

    return None


def add_dev_to_version(ver):
    # Now a terrible hack to add .dev, No there is no way to just set it or create a version without a string.
    parts = []
    # Epoch
    if ver.epoch != 0:
        parts.append("{0}:".format(ver.epoch))
    # Release segment
    parts.append(".".join(str(x) for x in ver.release))
    # Pre-release
    if ver.pre is not None:
        parts.append("".join(str(x) for x in ver.pre))
    # Post-release
    if ver.post is not None:
        parts.append(".post{0}".format(ver.post))
    parts.append(".dev0")
    # Local version segment
    if ver.local is not None:
        parts.append("+{0}".format(ver.local))
    return version.Version("".join(parts))


def katana_version(
    major: int,
    minor: int,
    micro: int,
    k_count: int,
    ke_count: Optional[int],
    k_hash,
    ke_hash,
    *,
    variant: Optional[str] = None,
    dev: bool = False,
    pre: Optional[Tuple[str, int]] = None,
    post: Optional[int] = None,
    is_merged: bool,
):
    s = f"{major}.{minor}.{micro}"
    if pre is not None:
        s += f"{pre[0]}{pre[1]}"
    if post is not None:
        assert isinstance(post, int), post
        s += f".post{post}"
    assert isinstance(dev, bool), dev
    if dev:
        s += ".dev"
    k_count = k_count if k_count is not None else "x"
    k_hash = k_hash or "xxxxxx"
    if ke_count is not None and ke_hash:
        dev_tag = f"{k_count}.{ke_count}.{k_hash}.{ke_hash}"
    else:
        dev_tag = f"{k_count}.0.{k_hash}"
    if not is_merged:
        dev_tag += ".unmerged"
    if variant is not None or dev:
        s += "+"
        if variant is not None:
            assert len(variant) > 0
            s += f"{variant}"
        if dev:
            if variant is not None:
                s += "."
            s += dev_tag
    v = version.Version(s)
    return v


def format_version_semantic(ver: version.Version, pre_separator: str = "", dev_separator: str = ".") -> str:
    parts = []

    # Epoch
    if ver.epoch != 0:
        parts.append("{0}:".format(ver.epoch))

    # Release segment
    parts.append(".".join(str(x) for x in ver.release))

    # Pre-release
    if ver.pre is not None:
        parts.append(pre_separator + "".join(str(x) for x in ver.pre))

    # Post-release
    if ver.post is not None:
        parts.append(".post{0}".format(ver.post))

    # Development release
    if ver.dev is not None:
        parts.append("{0}dev{1}".format(dev_separator, ver.dev or ""))

    return "".join(parts)


def format_version_pep440(ver: version.Version) -> str:
    parts = [format_version_semantic(ver)]

    # Local version segment
    if ver.local is not None:
        parts.append("+{0}".format(ver.local))

    return "".join(parts)


def format_version_debian(ver: version.Version) -> str:
    parts = [format_version_semantic(ver, "~", "~")]

    # Local version segment
    if ver.local is not None:
        parts.append("+{0}".format(ver.local))

    return "".join(parts)
