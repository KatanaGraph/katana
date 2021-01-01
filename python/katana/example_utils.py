import os
import tarfile
import urllib.request

__all__ = ["get_input"]


def get_cache_directory():
    # Use the CI paths if they exist, just to make CI integration easier.
    ci_inputs_path = os.environ["HOME"] + "/.cache/graph"
    if os.path.isdir(ci_inputs_path):
        return ci_inputs_path
    # Otherwise use a more specific path name
    return os.environ["HOME"] + "/.cache/galois"


def get_inputs_directory():
    cache_dir = get_cache_directory()
    inputs_dir = cache_dir + "/inputs"
    if os.path.isdir(inputs_dir) and os.path.isdir(inputs_dir + "/propertygraphs/ldbc_003"):
        return inputs_dir
    fn, _headers = urllib.request.urlretrieve(
        "https://katana-ci-public.s3.us-east-1.amazonaws.com/inputs/katana-inputs-v16.tar.gz"
    )
    try:
        with tarfile.open(fn) as tar:
            tar.extractall(inputs_dir)
    finally:
        os.unlink(fn)
    return inputs_dir


def get_input(path):
    """
    Download the standard Galois inputs (with local caching on disk) and return a path to a file in that archive.

    >>> from galois.property_graph import PropertyGraph
    ... graph = PropertyGraph(get_input("propertygraphs/ldbc_003"))
    """
    return get_inputs_directory() + "/" + path
