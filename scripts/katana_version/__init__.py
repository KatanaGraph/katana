import logging
from argparse import Namespace
from os import environ
from pathlib import Path
from typing import Optional, Tuple, Union

from packaging.version import InvalidVersion, Version

from . import git
from .commands import CommandError
from .git import GitURL, Repo

logger = logging.getLogger(__name__)


def _maybe_path(s: Optional[Union[str, Path]]):
    if s is None:
        return None
    else:
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
                f"Failed to parse version provided in environment variable KATANA_VERSION: {environ.get('KATANA_VERSION')}"
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
        remotes = git.get_remotes(dir)
        if "origin" not in remotes:
            raise ConfigurationError(
                f"{name}: Missing origin remote. Why?! This script does not support your workflow. "
                "I'm sorry and confused."
            )
        if "KatanaGraph" in remotes or "katanagraph" in remotes:
            logger.warning(
                f"{name}: You have a remote that references the KatanaGraph organization. If that remote is your "
                "upstream, then rename it to 'upstream'."
            )
        if "upstream" in remotes:
            logger.info(
                f"{name}: Assuming a triangular workflow with pushes going to a personal fork and pulls coming "
                "from upstream."
            )
            upstream_remote = "upstream"
            origin_remote = "origin"
        else:
            logger.info(
                f"{name}: Assuming an in-repo workflow with pushes going to personal branches in " "the main repo."
            )
            upstream_remote = "origin"
            origin_remote = "origin"
        upstream_url = git.get_remote_url(upstream_remote, dir)
        origin_url = git.get_remote_url(origin_remote, dir)
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
