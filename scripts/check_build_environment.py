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


def conda_env_update_instructions():
    return """"
    Install the required conda packages with:
        # The external file if you have it.
        conda env update -f external/katana/conda_recipe/environment.yml
        # The main file
        conda env update -f conda_recipe/environment.yml
    """


def conda_env_reactivate_instructions():
    conda_env_name = Path(os.environ["CONDA_PREFIX"]).name
    return f"""
        Reactivate your environment by starting a new shell and activating your environment:
          [start new shell]
          conda activate {conda_env_name}
        (running conda deactivate followed by activate does not correctly reset Go environment variables, so you
        need a new clean shell. Logout and back in, or open a clean shell some other way.)
        """


@check
def avoid_go_nocgo(**_kwargs):
    has_problem = False
    cgo_enabled_env_var = os.environ.get("CGO_ENABLED")
    if cgo_enabled_env_var is not None and cgo_enabled_env_var != "1":
        show_problem(
            f"CGO_ENABLED environment variable is set to {cgo_enabled_env_var}",
            conda_env_reactivate_instructions()
            + """\n        and make sure CGO_ENABLED=1 is set in your environment.""",
        )
        has_problem = True
    try:
        go_env_lines = subprocess.check_output(["go", "env"], encoding="UTF-8").splitlines()
        if 'CGO_ENABLED="1"' not in go_env_lines and cgo_enabled_env_var in (None, "1"):
            show_problem(
                "go is installed in no-cgo mode",
                """
                Remove the go-nocgo package with:
                  conda remove go-nocgo
                """,
            )
            has_problem = True
    except subprocess.SubprocessError as e:
        show_problem("go executable did not run correctly", conda_env_update_instructions, exc=e)
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

    for k in set(current_env.keys()) | set(conda_run_env.keys()):
        if k in ("PS1", "PS2", "SHLVL", "PATH", "CONDA_ROOT", "_"):
            continue
        if strip_str(current_env.get(k)) != strip_str(conda_run_env.get(k)):
            show_problem(
                f"environment does not match installed conda packages "
                f"(Environment variable {k} does not match: {current_env.get(k)} != {conda_run_env.get(k)})",
                conda_env_reactivate_instructions(),
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
                show_problem(f"Conda environment needs update with {yml}\n{output}", conda_env_update_instructions)
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
    click.echo("Make sure you ran this script from the exact shell you use for builds.")
    if has_problem:
        click.echo("There are potential problems. You may need to address them.")
    else:
        click.echo("No obvious problems found.")
    return 1 if has_problem else 0


if __name__ == "__main__":
    check_build_environment()
