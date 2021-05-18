from argparse import Namespace
from typing import Optional

from . import Configuration
from .git import GitURL


class GithubFacade:
    github: "Github"
    config: Configuration

    def __init__(self, config: Configuration):
        from github import Github

        self.github = Github(*config.github_access)
        self.config = config

    def _get_repo(self, url: GitURL) -> "Repository":
        from github import GithubException

        try:
            return self.github.get_repo(f"{url.username}/{url.repository}")
        except GithubException as e:
            raise RuntimeError(
                f"Failed to access Github repository {url.username}/{url.repository} "
                f"(Did you provide a correct Github token or username/password?): {e.data}"
            ) from e

    def create_pr(
        self, upstream_url: GitURL, origin_url: GitURL, branch: str, base: str, title: str, body: str = ""
    ) -> "PullRequest":
        from github import GithubException

        upstream_repo = self._get_repo(upstream_url)
        print(
            f"GITHUB: Creating PR on {upstream_repo.full_name} to merge "
            f"{origin_url.username}/{origin_url.repository}:{branch} into {base}: {title}\n{body}"
        )
        if self.config.dry_run:
            return Namespace(html_url="[Github PR URL]", base=Namespace(repo=self._get_repo(upstream_url)), number="NN")
        try:
            return upstream_repo.create_pull(head=f"{origin_url.username}:{branch}", base=base, title=title, body=body)
        except GithubException as e:
            raise RuntimeError(f"Failed to create PR: {e.data}") from e
        # pylint: disable=inconsistent-return-statements

    def create_tag(self, upstream_url: GitURL, commit: str, tag_name: str, message: str) -> Optional["GitRef"]:
        from github import GithubException

        upstream_repo = self._get_repo(upstream_url)
        print(f"GITHUB: Creating tag {tag_name} at {commit} on {upstream_repo.full_name}: {repr(message)}")
        if self.config.dry_run:
            return
        try:
            ref = upstream_repo.create_git_tag(tag_name, message, object=commit, type="commit")
            return upstream_repo.create_git_ref(f"refs/tags/{tag_name}", ref.sha)
        except GithubException as e:
            raise RuntimeError(
                f"Failed to create tag {tag_name} at {commit} on {upstream_repo.full_name}: {e.data}"
            ) from e
        # pylint: disable=inconsistent-return-statements

    def create_branch(self, url: GitURL, commit, branch_name: str) -> Optional["GitRef"]:
        from github import GithubException

        repo = self._get_repo(url)
        print(f"GITHUB: Creating branch {branch_name} at {commit} on {repo.full_name}")
        if self.config.dry_run:
            return None
        try:
            return repo.create_git_ref(f"refs/heads/{branch_name}", commit)
        except GithubException as e:
            raise RuntimeError(
                f"Failed to create branch {branch_name} at {commit} on {repo.full_name}: {e.data}"
            ) from e

    def get_pr(
        self, url: GitURL, *, branch: Optional[str] = None, number: Optional[int] = None
    ) -> Optional["PullRequest"]:
        from github import GithubException

        upstream_repo = self._get_repo(url)
        if branch is not None:
            username = self.github.get_user().login
            try:
                pulls = upstream_repo.get_pulls(head=f"{username}:{branch}")
            except GithubException as e:
                raise RuntimeError(f"Failed to get PR for {upstream_repo.full_name}:{branch}: {e.data}") from e
            if pulls.totalCount == 0:
                return None
            return pulls[0]
        if number is not None:
            try:
                return upstream_repo.get_pull(number=number)
            except GithubException as e:
                raise RuntimeError(f"Failed to get PR {upstream_repo.full_name}#{number}: {e.data}") from e
        raise TypeError("branch or number is required")
