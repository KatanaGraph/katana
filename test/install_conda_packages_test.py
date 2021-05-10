#! /bin/python

import argparse
from pathlib import Path
from subprocess import Popen, PIPE
from sys import stderr
import tarfile


def main():
    parser = argparse.ArgumentParser(
        description="Install katana packages in a docker image and test them. " "Only runs a trivial test."
    )
    parser.add_argument("package_dir", type=Path, help="The directory containing the conda packages to install.")
    parser.add_argument("docker_image", type=str, help="The docker image to use.")

    args = parser.parse_args()

    with Popen(
        ["docker", "build", "-", "--build-arg", f"BASE_IMAGE={args.docker_image}",], stdin=PIPE
    ) as docker_proc, tarfile.open(fileobj=docker_proc.stdin, mode="w:gz") as context_tar:
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
