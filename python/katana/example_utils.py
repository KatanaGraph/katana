import os
import shutil
import tarfile
import urllib.request
from pathlib import Path

__all__ = ["get_input"]


def get_inputs_directory(*, invalidate=False):
    inputs_dir = None
    # Use the build paths if they exist.
    paths_to_check = list(Path(__file__).parents) + list(Path.cwd().parents)
    for path in paths_to_check:
        # TODO(amp): If we can abstract the input version info a shared file, this should look for
        #  specifically that version.
        ci_inputs_path = path / "inputs" / "current"
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
        "https://katana-ci-public.s3.us-east-1.amazonaws.com/inputs/katana-inputs-v19.tar.gz"
    )
    try:
        with tarfile.open(fn) as tar:
            tar.extractall(inputs_dir)
    finally:
        os.unlink(fn)
    return inputs_dir


def get_input(rel_path):
    """
    Download the standard Galois inputs (with local caching on disk) and return a path to a file in that archive.

    >>> from katana.property_graph import PropertyGraph
    ... graph = PropertyGraph(get_input("propertygraphs/ldbc_003"))
    """
    path = get_inputs_directory() / rel_path
    if path.exists():
        return path
    return get_inputs_directory(invalidate=True) / rel_path
