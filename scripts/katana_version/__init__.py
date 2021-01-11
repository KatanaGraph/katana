import logging
from os import environ
from pathlib import Path
from typing import Optional, Tuple, Union
from argparse import Namespace

from packaging.version import InvalidVersion, Version

from . import git
from .commands import CommandError
from .git import GitURL

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
    enterprise_origin_url: Optional[GitURL]
    enterprise_upstream_url: Optional[GitURL]
    dry_run: bool
    katana_enterprise_repo_path: Optional[Path]
    katana_repo_path: Path
    origin_remote: str
    origin_url: GitURL
    upstream_remote: str
    upstream_url: GitURL

    def __init__(self, args=Namespace()):
        self.github_access = ()
        self.dry_run = getattr(args, "dry_run", True)

        self.katana_repo_path, self.katana_enterprise_repo_path = Configuration._find_katana_repo_paths(args)

        if not self.katana_repo_path:
            raise ConfigurationError("The katana git repository must be available. Specify with --katana.")

        if getattr(args, "open", False):
            self.katana_enterprise_repo_path = None
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

        self.upstream_remote = "upstream"
        self.origin_remote = "origin"
        self.upstream_url = None
        self.origin_url = None
        self.enterprise_upstream_url = None
        self.enterprise_origin_url = None
        try:
            remotes = git.get_remotes(self.katana_repo_path)
            if "KatanaGraph" in remotes or "katanagraph" in remotes:
                logger.warning(
                    "You have a remote that references the KatanaGraph organization. If that remote is your "
                    "upstream, then rename it to 'upstream'."
                )
            if "upstream" in remotes:
                logger.info(
                    "Assuming a triangular workflow with pushes going to a personal fork and pulls coming from upstream."
                )
                self.upstream_remote = "upstream"
                if "origin" not in remotes:
                    raise ConfigurationError(
                        "Missing origin remote. Why?! This script does not support your workflow. "
                        "I'm sorry and confused."
                    )
                self.origin_remote = "origin"
            elif "origin" in remotes:
                logger.info("Assuming an in-repo workflow with pushes going to personal branches in the main repo.")
                self.upstream_remote = "origin"
                self.origin_remote = "origin"
            self.upstream_url = git.get_remote_url(self.upstream_remote, self.katana_repo_path)
            self.origin_url = git.get_remote_url(self.origin_remote, self.katana_repo_path)

            if self.has_enterprise:
                enterprise_remotes = git.get_remotes(self.katana_enterprise_repo_path)
                if self.origin_remote not in enterprise_remotes or self.upstream_remote not in enterprise_remotes:
                    raise ConfigurationError("Missing remotes in enterprise, they should match open.")
                url = git.get_remote_url(self.upstream_remote, self.katana_enterprise_repo_path)
                if url.username != self.upstream_url.username:
                    raise ConfigurationError("Upstream remotes are not in the same user")

                self.enterprise_upstream_url = git.get_remote_url(
                    self.upstream_remote, self.katana_enterprise_repo_path
                )
                self.enterprise_origin_url = git.get_remote_url(self.origin_remote, self.katana_enterprise_repo_path)
            else:
                self.enterprise_upstream_url = None
                self.enterprise_origin_url = None
        except CommandError:
            if not self.version_from_environment_variable:
                logger.warning(
                    f"Failed to find git repository at {self.katana_repo_path}. "
                    f"Version information will be very limited."
                )

    @staticmethod
    def _find_katana_repo_paths(args) -> Tuple[Optional[Path], Optional[Path]]:
        cwd_katana_repo_path = Configuration._find_cwd_repo_path()

        katana_repo_path = (
            _maybe_path(getattr(args, "katana", None))
            or cwd_katana_repo_path
            or Path(__file__).resolve().parent.parent.parent
        )
        katana_enterprise_repo_path = (
            _maybe_path(getattr(args, "katana_enterprise", None)) or katana_repo_path.parent.parent
        )

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
            if (cwd_repo_path / CONFIG_VERSION_PATH).is_file():
                return katana_repo_path
        return None

    @property
    def has_enterprise(self):
        return self.katana_enterprise_repo_path is not None

    @property
    def has_git(self):
        return self.origin_url is not None
