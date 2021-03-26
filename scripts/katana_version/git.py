import re
import logging
from datetime import datetime
from functools import cmp_to_key, lru_cache
from pathlib import Path
from typing import Tuple, Union

from .commands import action_command, capture_command, CommandError, predicate_command

logger = logging.getLogger(__name__)


class GitURL(str):
    protocol: str
    username: str
    hostname: str
    repository: str

    # LIMITATION: These are only a subset of the URL formats supported by git and they make assumptions about how
    # Github uses the paths. If this module is to be used as a general git library this should be corrected.
    URL_REGEXS = [
        re.compile(
            r"(?P<protocol>ssh)://git@(?P<hostname>[\w.-]+)/(?P<username>[\w.-]+)/(?P<repository>[\w-]+)(\.git)?"
        ),
        re.compile(r"git@(?P<hostname>[\w.-]+):(?P<username>[\w.-]+)/(?P<repository>[\w-]+)(\.git)?"),
        re.compile(
            r"(?P<protocol>https?)://(?P<hostname>[\w.-]+)/(?P<username>[\w.-]+)/(?P<repository>[\w-]+)(\.git)?"
        ),
    ]

    def __init__(self, s):
        super().__init__()
        for regex in self.URL_REGEXS:
            m = regex.match(s)
            if m:
                self.__dict__.update(m.groupdict())
                return
        self.protocol = "unsupported"
        self.username = "unknown"
        self.hostname = "unknown"
        self.repository = s
        logger.debug(f"Not a known git URL: {s} (Version management actions will probably not work.)")


class Repo:
    dir: Path
    origin_url: GitURL
    origin_remote: str
    upstream_url: GitURL
    upstream_remote: str

    def __init__(
        self,
        dir: Union[str, Path],
        origin_remote: str,
        origin_url: Union[GitURL, str],
        upstream_remote: str,
        upstream_url: Union[GitURL, str],
    ):
        self.dir = dir and Path(dir)
        self.origin_url = origin_url and GitURL(str(origin_url))
        self.origin_remote = origin_remote
        self.upstream_url = upstream_url and GitURL(str(upstream_url))
        self.upstream_remote = upstream_remote

    def __repr__(self):
        return (
            f"Repo({str(self.dir)!r}, {self.origin_remote!r}, {str(self.origin_url)!r}, "
            f"{self.upstream_remote!r}, {str(self.upstream_url)!r})"
        )

    @staticmethod
    def remote_branch(remote, branch):
        if remote:
            return f"{remote}/{branch}"
        else:
            return branch


def dir_arg(dir):
    if hasattr(dir, "dir"):
        dir = dir.dir
    if dir is not None:
        return "-C", str(dir)
    else:
        return ()


def get_working_tree(dir):
    try:
        p = capture_command("git", *dir_arg(dir), "rev-parse", "--show-toplevel")
    except CommandError:
        return None
    if p:
        return Path(p)
    else:
        return None


def is_working_tree(dir):
    if dir is None:
        return False
    dir = Path(dir).absolute()
    return dir == get_working_tree(dir)


def get_super_working_tree(dir):
    if dir is None:
        return None
    p = capture_command("git", *dir_arg(dir), "rev-parse", "--show-superproject-working-tree")
    if p:
        return Path(p)
    else:
        # An empty result means there is no super working tree
        return None


def is_dirty(dir, exclude: Tuple[str] = ()):
    changes = capture_command("git", *dir_arg(dir), "diff-index", "HEAD", "--").splitlines(keepends=False)
    for i, l in enumerate(list(changes)):
        if not any(bool(re.search(str(excl), l)) for excl in exclude):
            return True
    return False


def find_changes(filename, commit, dir, n=1):
    return capture_command(
        "git", *dir_arg(dir), "rev-list", *(("-n", str(n)) if n is not None else ()), commit, "--", str(filename),
    ).splitlines(keepends=False)


def find_change(filename, commit, dir):
    ret = find_changes(filename, commit, dir, n=1)
    return ret[0] if ret else None


def submodule_commit_at(submodule, commit, dir):
    output = capture_command("git", *dir_arg(dir), "ls-tree", commit, "--", str(submodule))
    return output.split()[2]


def get_commit_count(frm, to, dir):
    return int(capture_command("git", *dir_arg(dir), "rev-list", "--count", to, "^" + frm, "--"))


def get_date_of_commit(commit, dir) -> datetime:
    date_str = capture_command("git", *dir_arg(dir), "log", "-n1", "--format=%cd", "--date=iso-strict", commit)
    return datetime.fromisoformat(date_str)


def get_branch_checked_out(dir, ref_only=False):
    branch = capture_command("git", *dir_arg(dir), "branch", "--show-current")
    if not branch and ref_only:
        return None
    elif not branch:
        return get_hash("HEAD", dir)
    else:
        return branch


def get_commit_parents(commit, dir):
    return capture_command("git", *dir_arg(dir), "rev-parse", f"{commit}^@", "--").splitlines(keepends=False)[:-1]


@lru_cache(maxsize=128)
def is_ancestor_of(a, b, dir):
    """
    True if a is reachable from b.
    """
    return predicate_command("git", *dir_arg(dir), "merge-base", "--is-ancestor", a, b)


def is_same_tree(a, b, dir):
    return not bool(capture_command("git", *dir_arg(dir), "diff-tree", a, b, "--"))


def simplify_merge_commit(commit, dir):
    """
    Remove no-op merges (those that could have been a fast-forward) from commit.
    This removes the forced merge commits that github uses during CI runs.
    :param commit: A commit ref or hash.
    :return: A commit hash which removes any no-op merge.
    """
    parents = get_commit_parents(commit, dir)
    parents.sort(key=cmp_to_key(lambda a, b: is_ancestor_of(a, b, dir=dir)))
    potential_simplification = parents[-1]
    if is_same_tree(commit, potential_simplification, dir) and all(
        is_ancestor_of(p, potential_simplification, dir) for p in parents
    ):
        return potential_simplification
    else:
        return commit


def merge_base(a, b, dir):
    """
    True if commit is reachable from branch.
    """
    return capture_command("git", *dir_arg(dir), "merge-base", a, b)


def get_hash(commit, dir, *, exclude_dirty: Tuple[str] = (), pretend_clean=False, abbrev=False):
    if not pretend_clean and is_dirty(dir, exclude=exclude_dirty):
        return "DIRTY"
    if abbrev:
        abbrev_args = ("--abbrev-commit", f"--abbrev={abbrev}")
    else:
        abbrev_args = ()
    return capture_command("git", *dir_arg(dir), "rev-list", "-n", "1", *abbrev_args, commit, "--")


def get_remotes(dir):
    return capture_command("git", *dir_arg(dir), "remote").splitlines(keepends=False)


def get_remote_url(remote, dir) -> GitURL:
    return GitURL(capture_command("git", *dir_arg(dir), "remote", "get-url", remote))


def get_commit_message(commit, dir):
    return capture_command("git", *dir_arg(dir), "show", "--format=%s%n%n%b", "--quiet", commit)


def ref_exists(ref, dir):
    return predicate_command("git", *dir_arg(dir), "rev-parse", "--verify", ref, ignore_error=True)


def get_tags_of(ref, dir):
    return capture_command("git", *dir_arg(dir), "tag", f"--points-at={ref}").splitlines(keepends=False)


def get_refs_containing(ref, dir):
    return capture_command(
        "git", *dir_arg(dir), "for-each-ref", "--format=%(refname:short)", f"--contains={ref}"
    ).splitlines(keepends=False)


def commit(files, msg, dir, dry_run):
    action_command(
        "git", *dir_arg(dir), "commit", "--message", str(msg), "--", *[str(f) for f in files], dry_run=dry_run
    )


def commit_amend(files, dir, dry_run):
    action_command(
        "git",
        *dir_arg(dir),
        "commit",
        "--only",
        "--amend",
        "--message",
        get_commit_message("HEAD", dir),
        "--",
        *[str(f) for f in files],
        dry_run=dry_run,
    )


def create_branch(branch_name, dir, dry_run):
    action_command("git", *dir_arg(dir), "branch", branch_name, dry_run=dry_run)


def push(remote, branch_name, dir, dry_run, force=False):
    action_command(
        "git", *dir_arg(dir), "push", *(("--force-with-lease",) if force else ()), remote, branch_name, dry_run=dry_run
    )


def fetch(remote, dir, tags, dry_run, log=True):
    action_command("git", *dir_arg(dir), "fetch", *(("--tags",) if tags else ()), remote, dry_run=dry_run, log=log)


def amend_commit_message(message, dir, dry_run):
    action_command("git", *dir_arg(dir), "commit", "--only", "--amend", "--message", message, dry_run=dry_run)


def switch(branch_name, dir, dry_run):
    action_command("git", *dir_arg(dir), "checkout", branch_name, "--", dry_run=dry_run)


def find_branches(pattern, dir, sort=None, prefix="heads"):
    return capture_command(
        "git",
        *dir_arg(dir),
        "for-each-ref",
        "--format=%(refname:short)",
        *((f"--sort={sort}",) if sort else ()),
        f"refs/{prefix}/{pattern}",
    ).splitlines(keepends=False)


HEAD = "HEAD"
