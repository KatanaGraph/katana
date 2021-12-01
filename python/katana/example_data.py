"""
Utilities which download example data for testing and experimentation.
"""

import os
import shutil
import tarfile
import urllib.request
from pathlib import Path

__all__ = ["get_input", "get_input_as_url"]


def get_inputs_directory(*, invalidate=False, rel_path=None) -> Path:
    inputs_dir = None
    if rel_path == "csv-datasets":
        paths_to_check = list(Path(__file__).parents) + list(Path.cwd().parents)
        for path in paths_to_check:
            csv_path = (path / "katana-enterprise" / "external").resolve()
            if csv_path.exists():
                return csv_path

    # Use the build paths if they exist.
    if "KATANA_BUILD_DIR" in os.environ:
        # If KATANA_BUILD_DIR environment is set, just use it
        paths_to_check = [Path(os.environ["KATANA_BUILD_DIR"])]
    else:
        paths_to_check = list(Path(__file__).parents) + list(Path.cwd().parents)
    for path in paths_to_check:
        # TODO(amp): If we can abstract the input version info a shared file, this should look for
        #  specifically that version.
        ci_inputs_path = (path / "inputs" / "current").resolve()
        if ci_inputs_path.is_dir():
            inputs_dir = ci_inputs_path
    # Otherwise use a cache directory
    if not inputs_dir:
        inputs_dir = Path(os.environ["HOME"]) / ".cache" / "katana" / "inputs"
    if inputs_dir.is_dir() and (inputs_dir / "propertygraphs" / "ldbc_003").is_dir():
        if not invalidate:
            return inputs_dir
        try:
            shutil.rmtree(inputs_dir)
        except OSError:
            inputs_dir.unlink()
    inputs_dir.mkdir(parents=True, exist_ok=True)
    fn, _headers = urllib.request.urlretrieve(
        "https://katana-ci-public.s3.us-east-1.amazonaws.com/inputs/katana-inputs-v30.tar.gz"
    )
    try:
        with tarfile.open(fn) as tar:
            tar.extractall(inputs_dir)
    finally:
        os.unlink(fn)
    return inputs_dir


def get_input(rel_path) -> Path:
    """
    Download the standard Galois inputs (with local caching on disk) and return a path to a file in that archive.

    >>> from katana.local import Graph
    ... graph = Graph(get_input("propertygraphs/ldbc_003"))
    """
    path = get_inputs_directory(rel_path=rel_path) / rel_path
    if path.exists():
        return path
    return get_inputs_directory(invalidate=True) / rel_path


def get_input_as_url(rel_path) -> str:
    """
    Similar to get_input, but return the graph as file:// URL.
    """
    path = get_input(rel_path).resolve()
    return f"file://{path}"
