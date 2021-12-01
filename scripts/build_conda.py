#!/usr/bin/env python3

import os
import subprocess
import sys
import tempfile
from pathlib import Path

import click
import yaml

# TODO(amp): Replace with normal import once we have a script utilities python project in this repo.
SCRIPTS_DIR_PATH = Path(__file__).absolute().parent
sys.path.append(str(SCRIPTS_DIR_PATH))

import katana_requirements
import katana_version.version
from katana_requirements import OutputFormat, Requirements


def find_default_repo_root():
    open_repo_root = SCRIPTS_DIR_PATH.parent
    enterprise_repo_root = open_repo_root.parent.parent
    if (enterprise_repo_root / "conda_recipe").is_dir():
        return enterprise_repo_root
    return open_repo_root


def has_mambabuild():
    r = subprocess.call(["conda", "mambabuild"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return r != 127


@click.command()
@click.option("--force-conda", is_flag=True, help="Force the user of conda build instead of conda mambabuild.")
@click.option("--render", is_flag=True, help="Render the recipe instead of building.")
@click.option(
    "--use-pythonpath",
    is_flag=True,
    help="Use PYTHONPATH from the environment. By default, PYTHONPATH is "
    "cleared to prevent it from affecting the conda package build.",
)
@click.option(
    "--repo", type=Path, default=find_default_repo_root(), help="Override the automatically detected repository root."
)
@click.option("--output-folder", type=Path, default=None, help="Specify output directory for packages.")
def build_conda(force_conda, render, repo, use_pythonpath, output_folder):
    """Build the conda package for the current source tree."""
    build_subcommand = "mambabuild"
    if force_conda or not has_mambabuild():
        build_subcommand = "build"
    if render:
        build_subcommand = "render"

    requirements_data, _ = katana_requirements.load()

    # Approach: Generate a variant file with the versions we need in it and explicitly reference the versions from the
    # meta.yaml file. This will make adding dependencies less clean that I would like, but see failed approaches.

    # Failed approaches:
    # 1. Use an append or clobber file to override parts of meta.yaml. This either does not override enough (the former
    #    doesn't allow making version requirement stricter), or overrides too much (replaces whole lists, so jinja
    #    expression would need to be rerun, but cannot be.)
    # 2. Patch meta.yaml itself. This does not work easily because pyyaml outputs yaml in a format that conda doesn't
    #    support. Conda does some regex parsing on the yaml, so it doesn't support yaml in full generality, and requires
    #    white space in cases where pyyaml doesn't put it.
    # 3. Call the conda build pipeline directly using Python. This does not work because conda does not provide a hook
    #    (that I could find) where I need it: between yaml loading and dependency resolution. I think this is partly
    #    because the two phases are weirdly intertwined with several cases where jinja is reexecuted based on
    #    information from a solve done using a previous parse.
    # Basically, this is a total mess, and I spent way too much time on it. I hate everything.

    variant_config = {}
    for p in requirements_data.select_packages(["conda", "conda/dev"], OutputFormat.CONDA):
        variant_config[p.name_for(requirements_data.packaging_systems["conda"]).replace("-", "_")] = [
            p.version_for(requirements_data.packaging_systems["conda"])
        ]

    # Several variant variables have special handling. Remove them. They are set manually as needed.
    for k in ("python", "numpy", "cxx_compiler"):
        del variant_config[k]

    with tempfile.NamedTemporaryFile(mode="wt", prefix="variant-file-", suffix=".yaml") as variant_file:
        yaml.dump(variant_config, variant_file, Dumper=yaml.SafeDumper)
        variant_file.flush()

        build_command_line = [
            "conda",
            build_subcommand,
            "--channel",
            "conda-forge",
            "--channel",
            "katanagraph",
            "--variant-config-files",
            variant_file.name,
            repo / "conda_recipe",
        ]
        if output_folder:
            build_command_line += ["--output-folder", output_folder]
        os.environ["KATANA_VERSION"] = katana_version.version.format_version_pep440(
            katana_version.version.get_version()
        )
        if not use_pythonpath:
            # Clear python path because if it is set, it will leak into conda build potentially causing issues.
            os.environ["PYTHONPATH"] = ""

        try:
            subprocess.check_call(build_command_line, cwd=SCRIPTS_DIR_PATH.parent.parent)
        except subprocess.CalledProcessError:
            print(open(variant_file.name).read())
            raise


if __name__ == "__main__":
    build_conda()
