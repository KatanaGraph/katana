#! /usr/bin/env python3

import argparse
import os
import tarfile
from pathlib import Path
from subprocess import PIPE, Popen, check_call, check_output
from sys import stderr

build_configs = dict(
    conda=dict(
        dockerfile="build_in_container_conda.Dockerfile",
        context_files=[
            # name in context, name in source directory
            ("environment.yml", "conda_recipe/environment.yml"),
            ("open_environment.yml", "external/katana/conda_recipe/environment.yml"),
        ],
    )
)


def main():
    parser = argparse.ArgumentParser(description="Build katana in a container using the current source tree.")
    parser.add_argument(
        "--tag",
        type=str,
        help="Specify the tag for the generated docker image. (default: katana/build-in-container/<type>)",
        default=None,
    )
    parser.add_argument("--build", "-B", type=Path, help="The build directory in the container host.", required=True)
    parser.add_argument(
        "--source", "-S", type=Path, help="The root of the source tree to use. Defaults to auto detect.", default=None
    )
    parser.add_argument("--type", "-t", type=str, help="The type of build to perform.", default="conda")
    parser.add_argument("--cmake-generator", "-G", type=str, help="CMake generator to use", default="Unix Makefiles")
    parser.add_argument("targets", nargs="*", help="The targets to build")
    parser.add_argument(
        "--reuse", help="Rebuild the build image even if it already exists.", default=False, action="store_true"
    )
    parser.add_argument(
        "--image-only",
        help="Only generate the docker image, do not build Katana within the image.",
        default=False,
        action="store_true",
    )

    args = parser.parse_args()

    build_config = build_configs[args.type]
    if not args.source:
        args.source = Path(__file__).parent.parent
    source = Path(args.source).absolute()
    build = Path(args.build).absolute()

    build.mkdir(exist_ok=True)

    image_tag = args.tag or f"katana/build-in-container/{args.type}"

    has_image = False
    if image_tag:
        has_image = len(check_output(["docker", "image", "ls", "--quiet", image_tag])) > 0

    if not has_image or not args.reuse:
        with Popen(["docker", "build", "-t", image_tag, "-"], stdin=PIPE) as docker_proc:
            with docker_proc.stdin, tarfile.open(fileobj=docker_proc.stdin, mode="w:gz") as context_tar:
                context_tar.add((Path(__file__).parent / build_config["dockerfile"]).resolve(), arcname="Dockerfile")
                for target_file, source_file in build_config["context_files"]:
                    if (source / source_file).exists():
                        context_tar.add(source / source_file, arcname=target_file)
            err_code = docker_proc.wait()
            if err_code != 0:
                print("Failed to build docker container.", file=stderr)
                return err_code

    print(f"Docker image tag: {image_tag}")

    targets_str = " ".join(args.targets)

    user = os.getuid()
    group = os.getgid()

    def make_docker_run_command(katana_version):
        return [
            "docker",
            "run",
            "--rm",
            "--user",
            f"{user}:{group}",
            "--env",
            f"KATANA_VERSION={katana_version}",
            "--env",
            f"TARGETS={targets_str}",
            "--env",
            f"CMAKE_GENERATOR={args.cmake_generator}",
            "--mount",
            f"type=bind,src={build},dst=/build",
            "--mount",
            f"type=bind,src={source},dst=/source",
            image_tag,
        ]

    if not args.image_only:
        build_command = make_docker_run_command(
            check_output([f"{source}/scripts/version", "show"], cwd=source, encoding="ascii").strip()
        )
        check_call(build_command)
        print("Built Katana in docker using: " + " ".join(build_command))
        print(f"/build in the container is {build} in the host")
        print(f"/source in the container is {source} in the host")
    else:
        build_command = make_docker_run_command(f"$({source}/scripts/version show)")
        print(
            "You can build Katana in docker using:\n\n"
            + " ".join((f'"{s}"' if " " in s else s) for s in build_command)
            + "\n"
        )
        print("or modify this command to access the build environment in other ways.")


if __name__ == "__main__":
    main()
