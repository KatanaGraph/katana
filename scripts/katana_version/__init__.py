import logging
from argparse import Namespace
from os import environ
from pathlib import Path
from typing import Optional, Tuple, Union

from packaging.version import InvalidVersion, Version

from . import git
from .commands import CommandError
from .git import GitURL, Repo

UPSTREAM_USERNAME = "KatanaGraph"

logger = logging.getLogger(__name__)


def _maybe_path(s: Optional[Union[str, Path]]):
    if s is None:
        return None
    return Path(s).absolute()


class ConfigurationError(RuntimeError):
    pass


class StateError(RuntimeError):
    pass


CONFIG_VERSION_PATH = Path("config") / "version.txt"
SUBMODULE_PATH = Path("external") / "katana"


class Configuration:
    version_from_environment_variable: Optional[Version]
    github_access: Tuple[str, ...]
    enterprise: Optional[Repo]
    open: Optional[Repo]
    version_file: Optional[Path]
    dry_run: bool

    def __init__(self, args=Namespace()):
        self.github_access = ()
        self.dry_run = getattr(args, "dry_run", True)

        katana_repo_path, katana_enterprise_repo_path = Configuration._find_katana_repo_paths(args)

        if not katana_repo_path:
            raise ConfigurationError("The katana git repository must be available. Specify with --katana.")

        if getattr(args, "open", False):
            katana_enterprise_repo_path = None

        if getattr(args, "username", None) and getattr(args, "password", None):
            self.github_access = (args.username, args.password)

        if getattr(args, "access_token", None):
            self.github_access = (args.access_token,)

        self.version_from_environment_variable = None
        try:
            if "KATANA_VERSION" in environ:
                self.version_from_environment_variable = Version(environ["KATANA_VERSION"])
        except InvalidVersion:
            logger.warning(
                "Failed to parse version provided in environment variable "
                f"KATANA_VERSION: {environ.get('KATANA_VERSION')}"
            )

        self.version_file = None
        if (katana_repo_path / CONFIG_VERSION_PATH).is_file():
            self.version_file = katana_repo_path / CONFIG_VERSION_PATH
        else:
            logger.error(
                f"Version file does not exist in source: {katana_repo_path / CONFIG_VERSION_PATH}. "
                "Your Katana source is incomplete. Using 0.0.0 as a stand-in for the missing version."
            )

        self.open = None
        self.enterprise = None
        try:
            self.open = self._find_katana_remotes(katana_repo_path)

            if katana_enterprise_repo_path:
                self.enterprise = self._find_katana_remotes(katana_enterprise_repo_path)
        except CommandError:
            if not self.version_from_environment_variable:
                logger.warning(
                    f"Failed to find git repository at {katana_repo_path}. "
                    f"Version information will be very limited."
                )
        except ConfigurationError as e:
            if not self.version_from_environment_variable:
                logger.warning(
                    f"Failed to determine the repo structure. Ignoring git information. "
                    f"Version information will be very limited.\n{e}",
                )

    @staticmethod
    def _find_katana_remotes(dir):
        name = Path(dir).name
        remotes = set(git.get_remotes(dir))
        upstream_remote = None
        origin_remote = None
        for remote in remotes:
            # If we find a remote with an obvious name, override anything we already found
            if remote in {"upstream"}:
                upstream_remote = remote
            elif remote in {"origin"}:
                origin_remote = remote
            # If the remote URL has user "KatanaGraph" then it's our upstream (as long as we have not already found an
            # upstream).
            try:
                url = git.get_remote_url(remote, dir)
                if not upstream_remote and url.username == UPSTREAM_USERNAME:
                    upstream_remote = remote
            except ValueError as e:
                logger.info(f"Unsupported URL for git remote '{remote}'. Assuming it's not the upstream. Error: {e}")

        if not origin_remote:
            # If there is a single remote that is not the upstream, assume that is origin
            non_upstream_remotes = remotes - {upstream_remote}
            if len(non_upstream_remotes) == 1:
                origin_remote = list(non_upstream_remotes)[0]

        # If we have no upstream, use origin.
        if not upstream_remote:
            upstream_remote = origin_remote

        # Post-conditions of the above code:
        # If origin is set, so is upstream. Both may be None, or only upstream may be None.

        if not origin_remote and not upstream_remote:
            logger.warning(
                f"{name}: Workflow could not be determined, because this script did not find any remotes it "
                f"understands. Commit counts and merged flag may be wrong in versions."
            )
        elif not origin_remote:
            logger.info(
                f"{name}: Workflow could not be determined, because no origin remote was found, "
                f"however we assume pulls are coming from upstream (remote: {upstream_remote})."
            )
        elif upstream_remote != origin_remote:
            logger.info(
                f"{name}: Assuming a triangular workflow with pushes going to a personal fork "
                f"(remote: {origin_remote}) and pulls coming from upstream (remote: {upstream_remote})."
            )
        else:
            logger.info(
                f"{name}: Assuming an in-repository workflow with pushes going to personal branches in "
                f"the main repository (remote: {origin_remote})."
            )
        upstream_url = upstream_remote and git.get_remote_url(upstream_remote, dir)
        origin_url = origin_remote and git.get_remote_url(origin_remote, dir)
        return Repo(dir, origin_remote, origin_url, upstream_remote, upstream_url)

    @staticmethod
    def _find_katana_repo_paths(args) -> Tuple[Optional[Path], Optional[Path]]:
        # Get command line paths if available
        katana_repo_path = _maybe_path(getattr(args, "katana", None))
        katana_enterprise_repo_path = _maybe_path(getattr(args, "katana_enterprise", None))

        # Next try: Compute open path from enterprise
        if not katana_repo_path and katana_enterprise_repo_path:
            katana_repo_path = katana_enterprise_repo_path / SUBMODULE_PATH

        # Next try: Compute open path based on current working directory
        cwd_katana_repo_path = Configuration._find_cwd_repo_path()
        if not katana_repo_path and cwd_katana_repo_path:
            katana_repo_path = cwd_katana_repo_path

        # Next try: Compute open path based on the location of this script
        if not katana_repo_path:
            katana_repo_path = Path(__file__).resolve().parent.parent.parent

        # Compute enterprise path if needed based on open path
        if not katana_enterprise_repo_path:
            katana_enterprise_repo_path = katana_repo_path.parent.parent

        # Check the enterprise path and discard it if it's not valid
        if not (katana_enterprise_repo_path / SUBMODULE_PATH) == katana_repo_path:
            katana_enterprise_repo_path = None

        return katana_repo_path, katana_enterprise_repo_path

    @staticmethod
    def _find_cwd_repo_path():
        cwd_repo_path = git.get_working_tree(Path.cwd().resolve()) or Path.cwd()
        if cwd_repo_path:
            katana_repo_path = None
            if (cwd_repo_path / SUBMODULE_PATH).is_dir():
                # We are in the enterprise repo
                katana_repo_path = cwd_repo_path / SUBMODULE_PATH
            elif (cwd_repo_path / CONFIG_VERSION_PATH).is_file():
                # We are in the open repo
                katana_repo_path = cwd_repo_path
            if katana_repo_path and (katana_repo_path / CONFIG_VERSION_PATH).is_file():
                return katana_repo_path
        return None

    @property
    def has_enterprise(self):
        return self.enterprise is not None

    @property
    def has_git(self):
        return self.open is not None
