import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parent.parent.resolve()


def run_docker_test(image, script):
    cmdline = ["docker", "run", "--rm", "-i", "--mount", f"type=bind,source={REPO_ROOT},target=/source", image, "bash"]
    print(cmdline)
    with subprocess.Popen(cmdline, stdin=subprocess.PIPE, encoding="utf-8") as cmd:
        cmd.communicate("cd /source; set -xeuo pipefail; " + script)
        r = cmd.wait()
    print(r)
    assert r == 0


# @pytest.mark.skip("Slow and tested as a side effect of other tests.")
@pytest.mark.parametrize("image", ["ubuntu:20.04", "ubuntu:18.04"])
def test_setup_ubuntu(image):
    run_docker_test(
        image,
        """
    apt-get update
    scripts/setup_ubuntu.sh
    """,
    )


@pytest.mark.parametrize("image", ["condaforge/mambaforge"])
def test_list(image):
    run_docker_test(
        image,
        """
    mamba install --quiet --yes pyyaml packaging
    scripts/requirements list -l conda/dev -p conda
    """,
    )


@pytest.mark.parametrize("image", ["condaforge/mambaforge", "condaforge/miniforge3"])
def test_requirements_install_conda(image):
    run_docker_test(
        image,
        """
    conda install --quiet --yes pyyaml packaging
    scripts/requirements install --arg=--yes --arg=--quiet -l conda/dev -p conda
    """,
    )
