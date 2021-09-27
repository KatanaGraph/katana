#! /usr/bin/env python
import argparse
import logging
import subprocess
import sys
import tempfile
from os import environ
from pathlib import Path

import yaml
from katana_version.version import format_version_pep440, get_version

# Setup logging so libraries (such as katana_version) are not logging, but we are.
logger = logging.getLogger("conda_build")
logging.basicConfig()
logger.setLevel(logging.INFO)


def main():
    repo_root = Path(__file__).parent.parent

    version = get_version()
    environ["KATANA_VERSION"] = format_version_pep440(version)

    parser = argparse.ArgumentParser(
        prog="conda_build.py",
        description="""
      Build Katana conda packages
      """,
    )

    parser.add_argument(
        "--python-version", "-p", help="Build only for one python version", type=str,
    )

    parser.add_argument("--no-mamba", help="Do not use mambabuild even if it is available.", action="store_true")

    parser.add_argument(
        "--build-command", help="Use a custom build command instead of build or mambabuild.", type=str,
    )

    args, extra_args = parser.parse_known_args()

    build_config = None
    if args.python_version:
        # Force a single version in conda_build_config.yaml
        with open(repo_root / "conda_recipe" / "conda_build_config.yaml", mode="rt") as yaml_file:
            build_config = yaml.load(yaml_file, Loader=yaml.SafeLoader)
        original_python_versions = build_config["python"]
        build_config["python"] = list(filter(lambda s: args.python_version in s, build_config["python"]))
        if not build_config["python"]:
            raise ValueError(
                f"--python-version {args.python_version} is invalid because it does not match any "
                f"supported python version: {original_python_versions}"
            )

    has_mamba = (
        subprocess.call(["conda", "mambabuild", "-V"], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL) == 0
    )
    use_mamba = has_mamba and not args.no_mamba
    build_subcommand = args.build_command or ("mambabuild" if use_mamba else "build")
    if build_subcommand == "mambabuild":
        logger.info("Using mambabuild (because it's much faster and available). You can disable this with --no-mamba.")

    with tempfile.NamedTemporaryFile(suffix="_conda_build_config.yaml", mode="w+t") as build_config_file:
        if build_config:
            yaml.dump(build_config, build_config_file)
            extra_args = ["--variant-config-files", build_config_file.name]
        logger.info(
            "Running command: %s",
            " ".join(["conda", build_subcommand, "-c", "conda-forge", str(repo_root / "conda_recipe")] + extra_args),
        )
        result = subprocess.call(
            ["conda", build_subcommand, "-c", "conda-forge", repo_root / "conda_recipe"] + extra_args
        )

    if result != 0:
        logger.error("Package build failed!!")

    return result


if __name__ == "__main__":
    sys.exit(main())
