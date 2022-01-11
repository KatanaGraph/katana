import argparse
import logging

from katana_version.information_subcommands import (
    setup_parse_subcommand,
    setup_provenance_subcommand,
    setup_show_subcommand,
)
from katana_version.release_workflow_commands import (
    fetch_upstream,
    setup_bump_subcommand,
    setup_global_action_arguments,
    setup_global_log_arguments,
    setup_global_repo_arguments,
    setup_release_branch_subcommand,
    setup_release_subcommand,
    setup_tag_stable_subcommand,
    setup_tag_subcommand,
    setup_update_dependent_pr_subcommand,
)
from packaging.version import InvalidVersion

from . import Configuration
from .commands import CommandError

logger = logging.getLogger(__name__)


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
    setup_parse_subcommand(subparsers)
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


if __name__ == "__main__":
    main()
