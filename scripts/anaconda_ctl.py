import argparse

from binstar_client import Binstar
from binstar_client.utils import get_server_api


def remove_old_func(args, api: Binstar):
    args.package = args.package or ["katana-cpp", "katana-python", "katana-tools"]
    for package_name in set(args.package):
        package = api.package("KatanaGraph", package_name)

        latest_versions = package["versions"][-args.recent :]
        files = package["files"]
        for f in files:
            basename = f["basename"]
            labels = f["labels"]
            version = f["version"]
            ndownloads = f["ndownloads"]
            if version in latest_versions:
                # Skip the most recent versions always.
                continue
            if args.label not in labels:
                # Skip if it doesn't have the label
                continue
            if ndownloads > args.downloads:
                # Skip if it has been downloaded enough
                continue
            info_str = f"{basename} (labels: {labels}, ndownloads: {ndownloads})"
            if args.really:
                print(f"Deleting {info_str}")
                api.remove_dist("KatanaGraph", package_name, version, basename=basename)
            else:
                print(f"Would delete {info_str}")


def main():
    parser = argparse.ArgumentParser(description="Manage anaconda.org packages.")
    parser.add_argument("--token", "-t", help="The anaconda.org token.", default=None)
    parser.set_defaults(func=None)

    subparsers = parser.add_subparsers()

    clean_parser = subparsers.add_parser("remove-old", help="Remove old anaconda.org packages with a specific label.")
    clean_parser.add_argument(
        "--package",
        "-p",
        help="The package to clean. Can be provided multiple times. Default: katana-cpp, katana-python, katana-tools",
        type=str,
        action="append",
        default=[],
    )
    clean_parser.add_argument(
        "--really", "-r", help="Really remove instead of just printing names.", action="store_true"
    )
    clean_parser.add_argument("--label", "-l", help="The label to clean. Default: dev", default="dev")
    clean_parser.add_argument(
        "--downloads",
        "-n",
        help="The maximum number of downloads the package can have and still be deleted. Default: 0",
        type=int,
        default=0,
    )
    clean_parser.add_argument("--recent", help="The number of recent versions to save. Default: 5", type=int, default=5)
    clean_parser.set_defaults(func=remove_old_func, really=False)

    args = parser.parse_args()

    api = get_server_api(args.token)

    if args.func:
        args.func(args, api)


if __name__ == "__main__":
    main()
