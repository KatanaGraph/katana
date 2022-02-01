import datetime
import os
import re
import socket
import subprocess
import sys
import tempfile
import warnings
import zipfile
from pathlib import Path
from typing import Any, Iterable, Optional, Union

_environment_capture_routines = []


def register_capture_routine(f):
    _environment_capture_routines.append(f)
    return f


@register_capture_routine
def _capture_system(zipout: zipfile.ZipFile, **kwargs):
    # pylint: disable=unused-argument
    import katana  # Imported here to avoid cyclical import

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
    conda_exist = "CONDA_EXE" in os.environ
    capture_string(
        zipout,
        "conda.txt",
        f"""
            CONDA_PREFIX: {os.environ.get('CONDA_PREFIX', '')}

            {os.environ['CONDA_EXE'] if conda_exist else 'no CONDA_EXE env var'}:
            {capture_command(os.environ['CONDA_EXE'], 'info') if conda_exist else ''}
            {capture_command(os.environ['CONDA_EXE'], 'list') if conda_exist else ''}

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
def _capture_build(zipout: zipfile.ZipFile, **kwargs):
    # pylint: disable=unused-argument
    """
    Capture build related files into zip.
    """
    # The number of parent steps needed to go from __file__ to the root of the open repo.
    PARENT_STEPS_TO_ROOT_FOR_OPEN = 3
    # Similarly, for the root of the enterprise repo.
    PARENT_STEPS_TO_ROOT_FOR_ENTERPRISE = 5

    already_added = set()

    for root in [
        Path(__file__).parents[PARENT_STEPS_TO_ROOT_FOR_OPEN],
        Path(__file__).parents[PARENT_STEPS_TO_ROOT_FOR_ENTERPRISE],
        Path.cwd(),
    ]:
        for target_filename in [
            "CMakeCache.txt",
            "CMakeError.log",
            "CMakeOutput.log",
            "graph-convert.dir/link.txt",
            "graph-worker.dir/link.txt",
        ]:
            files = list(root.rglob(target_filename))
            capture_files(zipout, [f for f in files if f not in already_added])
            already_added.update(files)


def capture_command(*args, **kwargs) -> str:
    # pylint: disable=subprocess-run-check
    # Not using check=True because I want to capture both success and failure the same way.
    out = ""

    try:
        res = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, **kwargs)
        out = res.stdout.decode("utf-8").strip("\n")
    except OSError as err:
        out = f"error executing {args}: {err}"

    return out


def capture_files(zipout: zipfile.ZipFile, paths: Iterable[Union[Path, str]]) -> None:
    """
    Add files and/or directory trees to the zip file.

    :param zipout: The zip file to add files to.
    :param paths: The paths to add to the zip file. And directories are added along with all their contents.
    """
    for p in paths:
        # Convert the path to an actual Path instance in case it was a str representation of the path.
        p = Path(p)
        if p.is_file() or p.is_symlink():
            zipout.write(p)
        elif p.is_dir():
            capture_files(zipout, p.iterdir())
        else:
            # ignore other kinds of files
            pass


def capture_string(zipout: zipfile.ZipFile, filename: str, content: str) -> None:
    """
    Write content to a file in the zip file.

    :param zipout: The zip file to write to.
    :param filename: The file name to create in the archive. This will be prefixed with ``info/`` to separate it from
        directly added files.
    :param content: The string to write into the file. All white space at the beginning and ends of lines will be
        stripped. This makes triple-quoted strings easier to use.
    """
    zipout.writestr(f"info/{filename}", "\n".join(l.strip() for l in content.strip().splitlines()))


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


def is_interactive():
    return hasattr(sys, "ps1")


def capture_environment(filename: Optional[Union[str, Path, Any]] = None, **kwargs):
    """
    Capture the execution and build environment in as much detail as reasonably possible
    and store it to a file. This is used for bug reporting.

    :param filename: The file name for the captured environment information.
        (Default: an auto generated file in the system temporary directory)
    :type filename: str or Path or a file-like object or None
    :key client: Required if using remote enviroment. Expects katana.remote.Client
    :return: A file path where the captured environment information is stored.
    """
    # pylint: disable=consider-using-with
    # with is used, but not on the same line, and pylint does not do flow analysis.

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
                f(zipout, kwargs)
    finally:
        if filename is not None:
            file.close()

    if is_interactive() and filename is not None:
        # This is an interactive shell, but might not be IPython
        path = Path(filename)
        try:
            from IPython import get_ipython
            from IPython.display import HTML, display

            if get_ipython():
                # If IPython is importable and actually loaded, display the link.
                try:
                    # Path.readlink doesn't exist before Python 3.9, so that cannot be used quite yet.
                    ipython_root_directory = os.readlink(f"/proc/{os.environ['JPY_PARENT_PID']}/cwd")
                except OSError:
                    warnings.warn(
                        "Could not determine Jupyter root path. Guessing that it is the current working "
                        "directory. The download link may be incorrect."
                    )
                    ipython_root_directory = Path.cwd()

                environment_information_directory = Path.cwd() / "environment_information"
                environment_information_directory.mkdir(parents=True, exist_ok=True)

                download_path = environment_information_directory / path.name
                download_path.symlink_to(path)

                # IPython.display.FileLink cannot be used since it creates a link that tries to open the file as text.
                display(
                    HTML(
                        f"""
                    Environment information file download link:
                    <a href="/files/{download_path.relative_to(ipython_root_directory)}" target="_blank">
                        {download_path.name}
                    </a>
                    """
                    )
                )
        except ImportError:
            pass  # Fail silently if we don't have IPython. The output of the function is still useful.

    return filename
