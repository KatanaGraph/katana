#! /usr/bin/env python3
import os
import subprocess
import textwrap
from pathlib import Path

import click

OPEN_ROOT = Path(__file__).resolve().parent.parent
if (OPEN_ROOT.parent.parent / "conda_recipe").is_dir():
    SRC_DIR = OPEN_ROOT.parent.parent
else:
    SRC_DIR = OPEN_ROOT


def show_problem(msg, instructions, exc=None):
    exc_str = f"({exc})" if exc else ""
    print(f"PROBLEM: {msg} {exc_str}")
    print(textwrap.indent(textwrap.dedent(instructions), "  "))
    print()


check_functions = []


def check(f):
    check_functions.append(f)
    return f


CONDA_ENV_UPDATE_INSTRUCTIONS = """
    Install the required conda packages with:
        # The external file if you have it.
        conda env update -f external/katana/conda_recipe/environment.yml
        # The main file
        conda env update -f conda_recipe/environment.yml
    """


@check
def avoid_go_nocgo(**_kwargs):
    has_problem = False
    try:
        go_env_lines = subprocess.check_output(["go", "env"], encoding="UTF-8").splitlines()
        if 'CGO_ENABLED="0"' not in go_env_lines:
            show_problem(
                "go is installed in no-cgo mode",
                """
                Remove the go-nocgo package with:
                  conda remove go-nocgo
                """,
            )
            has_problem = True
    except subprocess.SubprocessError as e:
        show_problem("go executable did not run correctly", CONDA_ENV_UPDATE_INSTRUCTIONS, exc=e)
        has_problem = True
    if has_problem:
        # Only check this if we see an issue above. It may help people with workarounds. However,
        # it has false positives, so don't check in the normal case.
        cgo_enabled_env_var = os.environ.get("CGO_ENABLED")
        if cgo_enabled_env_var != "1":
            show_problem(
                f"CGO_ENABLED environment variable is set to {cgo_enabled_env_var}",
                """Remove no cgo go install, or set CGO_ENABLED=1 in your environment.""",
            )
            has_problem = True
    return has_problem


def parse_env_output(s: str):
    env = {}
    for l in s.splitlines():
        if not l:
            continue
        k, v = l.split("=", 1)
        env[k] = v
    return env


def strip_str(s):
    if hasattr(s, "strip"):
        return s.strip()
    return s


@check
def conda_needs_reactivate(**_kwargs):
    has_problem = False
    if "CONDA_PREFIX" not in os.environ:
        show_problem(
            "conda environment not activated",
            """
            Run:
              conda activate katana-dev""",
        )
        return True
    current_env = dict(os.environ)
    conda_run_env = parse_env_output(
        subprocess.check_output(["conda", "run", "-p", os.environ["CONDA_PREFIX"], "env"], encoding="UTF-8")
    )
    conda_env_name = Path(os.environ["CONDA_PREFIX"]).name
    for k in set(current_env.keys()) | set(conda_run_env.keys()):
        if k in ("PS1", "PS2", "SHLVL", "PATH", "CONDA_ROOT", "_"):
            continue
        if strip_str(current_env.get(k)) != strip_str(conda_run_env.get(k)):
            show_problem(
                f"environment does not match installed conda packages "
                f"(Environment variable {k} does not match: {current_env.get(k)} != {conda_run_env.get(k)})",
                f"""
                Reactivate your environment with:
                  conda deactivate; conda activate {conda_env_name}
                """,
            )
            has_problem = True
    return has_problem


@check
def conda_environment_packages_wrong(src_dir):
    has_problem = False
    environment_yml = src_dir / "conda_recipe" / "environment.yml"
    external_katana_environment_yml = src_dir / "external" / "katana" / "conda_recipe" / "environment.yml"
    if not environment_yml.exists():
        raise RuntimeError(f"Checkout is missing {external_katana_environment_yml}")
    for yml in (external_katana_environment_yml, environment_yml):
        if not yml.exists():
            continue

        with subprocess.Popen(
            ["conda", "compare", yml], stderr=subprocess.STDOUT, stdout=subprocess.PIPE, encoding="UTF-8"
        ) as p:
            output = p.stdout.read()
            p.wait()
            if p.returncode != 0:
                show_problem(f"Conda environment needs update with {yml}\n{output}", CONDA_ENV_UPDATE_INSTRUCTIONS)
    return has_problem


check_function_names = [f.__name__ for f in check_functions]


@click.command()
@click.option(
    "--check",
    multiple=True,
    type=click.Choice(check_function_names, case_sensitive=False),
    help="Checks to run. Default: all checks.",
)
@click.option("--src-dir", type=Path, default=SRC_DIR, help="The explicit source directory.")
@click.option("-v", "--verbose", count=True)
def check_build_environment(check, src_dir, verbose):
    if verbose > 0:
        click.echo(f"Using source directory: {src_dir}")
    has_problem = False
    for f in check_functions:
        if not check or f.__name__ in check:
            click.echo(f"Running check {f.__name__}...")
            has_problem = f(src_dir=src_dir) or has_problem
    if has_problem:
        click.echo("There are potential problems. You may need to address them.")
    return 1 if has_problem else 0


if __name__ == "__main__":
    check_build_environment()
