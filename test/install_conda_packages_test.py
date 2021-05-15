#! /usr/bin/env python3

import argparse
import tarfile
from pathlib import Path
from subprocess import PIPE, Popen
from sys import stderr


def main():
    parser = argparse.ArgumentParser(
        description="Install katana packages in a docker image and test them. " "Only runs a trivial test."
    )
    parser.add_argument("package_dir", type=Path, help="The directory containing the conda packages to install.")
    parser.add_argument("docker_image", type=str, help="The docker image to use.")

    parser.add_argument("--python-package", default="katana-python", help="Name of python conda package to test")
    parser.add_argument("--tools-package", default="katana-tools", help="Name of tools conda package to test")
    parser.add_argument(
        "--tools-test-command", default="graph-convert --version", help="Command to use to test tools package"
    )

    args = parser.parse_args()

    build_args = [
        f"BASE_IMAGE={args.docker_image}",
        f"PYTHON_PACKAGE={args.python_package}",
        f"TOOLS_PACKAGE={args.tools_package}",
        f"TOOLS_TEST_COMMAND={args.tools_test_command}",
    ]
    command = ["docker", "build", "-"]
    for b in build_args:
        command.append("--build-arg")
        command.append(b)

    with Popen(command, stdin=PIPE) as docker_proc, tarfile.open(fileobj=docker_proc.stdin, mode="w:gz") as context_tar:
        context_tar.add(Path(args.package_dir).absolute(), arcname="packages")
        context_tar.add(Path(__file__).parent / "install_conda_packages_test.Dockerfile", arcname="Dockerfile")
        context_tar.add(
            Path(__file__).parent.parent / ".github" / "workflows" / "download_miniconda.sh",
            arcname="download_miniconda.sh",
        )
        context_tar.add(
            Path(__file__).parent.parent / ".github" / "workflows" / "activate_miniconda.sh",
            arcname="activate_miniconda.sh",
        )
    err_code = docker_proc.wait()
    if err_code != 0:
        print(f"Failed to install and test Conda packages from {args.package_dir} in {args.docker_image}", file=stderr)


if __name__ == "__main__":
    main()
