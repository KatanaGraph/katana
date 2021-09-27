import datetime
import os
import re
import socket
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path
from typing import Any, Iterable, Optional, Union

_environment_capture_routines = []


def register_capture_routine(f):
    _environment_capture_routines.append(f)
    return f


@register_capture_routine
def _capture_system(zipout: zipfile.ZipFile):
    capture_string(
        zipout,
        "os.txt",
        f"""
            host: {socket.gethostname()}
            uname: {os.uname()}
            date: {datetime.datetime.now()}

            {capture_command('lsb_release', '-a')}
            """,
    )

    import katana  # Imported here to avoid cyclical import

    capture_string(
        zipout,
        "katana.txt",
        f"""
            __file__: {__file__}
            version: {katana.__version__}
            """,
    )

    capture_string(zipout, "apt.txt", capture_command("apt", "list", "--installed"))

    capture_string(
        zipout,
        "python.txt",
        f"""
            python: {capture_command('which', 'python')} {capture_command('python', '--version')}
            python3: {capture_command('which', 'python3')} {capture_command('python3', '--version')}
            this_python: {sys.executable} {sys.version}

            pip:
            {capture_command('pip', 'list', '--format=columns')}

            pip3:
            {capture_command('pip3', 'list', '--format=columns')}
            """.strip(),
    )

    capture_string(zipout, "env.txt", "\n".join(f"{k}={v}" for k, v in get_filtered_environ().items()))

    capture_string(
        zipout,
        "conda.txt",
        f"""
            CONDA_PREFIX: {os.environ.get('CONDA_PREFIX', '')}

            {os.environ.get('CONDA_EXE', 'no CONDA_EXE env var')}:
            {capture_command(os.environ['CONDA_EXE'], 'info')}
            {capture_command(os.environ['CONDA_EXE'], 'list')}

            {capture_command('which', 'conda')}:
            {capture_command('conda', 'info')}
            {capture_command('conda', 'list')}
            """,
    )

    capture_string(
        zipout,
        "cmake.txt",
        f"""
            {capture_command('which', 'cmake')}:
            {capture_command('cmake', '--version')}
                """,
    )

    capture_files(zipout, ["/etc/ld.so.conf", "/etc/ld.so.conf.d/"])


@register_capture_routine
def _capture_build(zipout: zipfile.ZipFile):
    for root in [Path(__file__).parents[3], Path(__file__).parents[5], Path.cwd()]:
        for target_filename in ["CMakeCache.txt", "CMakeError.log", "CMakeOutput.log", "graph-worker*/link.txt"]:
            capture_files(zipout, root.rglob(target_filename))


def capture_command(*args, **kwargs) -> str:
    # pylint: disable=subprocess-run-check
    # Not using check=True because I want to capture both success and failure the same way.
    res = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, **kwargs)
    out = res.stdout.decode("utf-8").strip("\n")
    return out


def capture_files(zipout: zipfile.ZipFile, paths: Iterable[Union[Path, str]]):
    for p in paths:
        p = Path(p)
        if p.is_file():
            zipout.write(p)
        elif p.is_dir():
            capture_files(zipout, p.iterdir())
        else:
            # ignore other kinds of files
            pass


def capture_string(zipout: zipfile.ZipFile, filename: str, s: str):
    zipout.writestr(f"info/{filename}", "\n".join(l.strip() for l in s.strip().splitlines()))


def get_filtered_environ():
    environment_to_capture = {
        re.compile(r"CONDA_.*"),
        re.compile(r"KATANA.*"),
        "CXX",
        "CC",
        "LD",
        "CXXFLAGS",
        "CFLAGS",
        "LDFLAGS",
        "SHELL",
        re.compile(r"CMAKE_.*"),
        re.compile(r".*PATH"),
    }

    environment_to_exclude = re.compile(r"([^a-z0-9]|^)(key|token|password|secret)", re.IGNORECASE)

    result = {}

    for var in environment_to_capture:
        if isinstance(var, re.Pattern):
            vars = [k for k in os.environ if var.fullmatch(k)]
        else:
            vars = [var]
        for var in vars:
            if environment_to_exclude.search(var):
                # Don't capture variables which might have secrets.
                continue
            result[var] = os.environ.get(var, "<unset>")

    return result


def capture_environment(filename: Optional[Union[str, Path, Any]] = None):
    """
    Capture the execution and build environment in as much detail as reasonably possible
    and store it to a file. This is used for bug reporting.

    :param filename: The file name for the captured environment information.
        (Default: an auto generated file in the system temporary directory)
    :type filename: str or Path or a file-like object or None
    :return: A file path where the captured environment information is stored.
    """
    # pylint: disable=consider-using-with
    # with is used, but not on the same line, and pylint does do flow analysis.
    if filename is None:
        file = tempfile.NamedTemporaryFile(delete=False, mode="wb", prefix="environment_information_", suffix=".zip")
        filename = file.name
    elif isinstance(filename, (str, Path)):
        file = open(filename, mode="wb")
    else:
        # Assume it's file-like
        file = filename
        filename = None

    try:
        with zipfile.ZipFile(file=file, mode="w", compression=zipfile.ZIP_BZIP2) as zipout:
            for f in _environment_capture_routines:
                f(zipout)
    finally:
        if filename is not None:
            file.close()

    return filename


def main():
    print("Captured environment to:", capture_environment())


if __name__ == "__main__":
    sys.exit(main())
