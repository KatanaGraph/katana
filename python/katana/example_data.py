"""
Utilities which download example data for testing and experimentation.
"""

import os
import shutil
import subprocess
import urllib.request
import zipfile
from pathlib import Path

from katana import url
from katana.local.rdg_storage_format_version import get_latest_storage_format_version

__all__ = ["get_rdg_dataset_at_version", "get_rdg_dataset", "get_csv_dataset", "get_misc_dataset"]

# git sha of the datasets repo to download/cache if it is not available locally in the source
# TODO(emcginnis) it would be really really nice if this got updated automatically
# when the submodule ref held by open katana is updated
DATASETS_SHA = "650f3e92e6880adab9c0a7afe9b3d0a41306fa99"


def get_rdg_dataset_at_version(rdg_name, storage_format_version, as_url=False):
    """
    Get the path to the specified rdg dataset in external/test-datasets/rdg_datasets
    at the specified storage_format_version

    If you don't think you need a specific storage_format_version of the rdg
    Then use "get_rdg_dataset()" to get the latest storage_format_version of the rdg

    >>> from katana.local import Graph
    ... graph = Graph(get_rdg_dataset_at_version("ldbc_003", 1,))


    >>> from katana.local import Graph
    ... graph = Graph(get_rdg_dataset_at_version("ldbc_003", 1, as_url=True))
    """
    rel_path = Path("rdg_datasets") / rdg_name / f"storage_format_version_{storage_format_version}"
    rdg_path = _get_dataset(rel_path)
    if not rdg_path.is_dir():
        raise ValueError(f"rdg {rdg_name} at version {storage_format_version} is not available at {rdg_path}")
    rdg_path.resolve()
    if as_url:
        return _local_file_url(rdg_path)

    return rdg_path


def get_rdg_dataset(rdg_name, as_url=False):
    """
    Get the path to the specified rdg dataset in external/test-datasets/rdg_datasets
    at the latest supported storage_format_version


    >>> from katana.local import Graph
    ... graph = Graph(get_rdg_dataset("ldbc_003"))


    >>> from katana.local import Graph
    ... graph = Graph(get_rdg_dataset("ldbc_003", as_url=True))
    """
    latest_rdg_storage_format_version = get_latest_storage_format_version()
    return get_rdg_dataset_at_version(rdg_name, latest_rdg_storage_format_version, as_url)


def get_csv_dataset(csv_name, as_url=False):
    """
    Get the path to the specified csv dataset in external/test-datasets/csv_datasets
    """

    rel_path = Path("csv_datasets") / csv_name
    csv_path = _get_dataset(rel_path)
    if not csv_path.exists():
        raise ValueError("csv {} is not available at {}".format(csv_name, csv_path))

    csv_path.resolve()
    if as_url:
        return _local_file_url(csv_path)

    return csv_path


def get_misc_dataset(misc_rel_path, as_url=False):
    """
    Get the path to the specified misc dataset in external/test-datasets/misc_datasets
    """

    rel_path = Path("misc_datasets") / misc_rel_path
    misc_path = _get_dataset(rel_path)
    if not misc_path.exists():
        raise ValueError("misc dataset {} is not available at {}".format(misc_rel_path, misc_path))

    misc_path.resolve()
    if as_url:
        return _local_file_url(misc_path)

    return misc_path


## functions to access test datasets git repo, not for users of example_data
def _get_dataset(rel_path):
    """
    *** Don't use this, use get_[rdg,csv,misc]_dataset() ***
    try to get the dataset at rel_path
    since we might have downloaded the datasets, and our cache could be invalid
    try re-downloading the datasets if we are unable to locate rel_path
    """
    try:
        datasets_path = _get_test_datasets_directory()
    except ValueError:
        # try to re-download the datasets
        datasets_path = _get_test_datasets_directory(invalidate_cache=True)

    path = datasets_path / rel_path
    if not path.exists():
        raise ValueError(f"Dataset does not exist at {path}")

    return path


def _get_test_datasets_directory(invalidate_cache=False) -> Path:
    """
    *** Don't use this, use get_[rdg,csv,misc]_dataset() ***
    Locate the test datasets directory
    if invalidate_cache is passed, and the datasets are not available from the source
    then the datasets cache will be deleted and re-downloaded
    """
    test_datasets_path = None

    def validate_path(path: Path, validate_sha):
        if path.is_dir() and (path / "rdg_datasets").is_dir() and (path / "csv_datasets").is_dir():
            if validate_sha:
                res = subprocess.run(
                    ["git", "-C", str(path), "rev-list", "-n", "1", "HEAD"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    check=True,
                )
                out = res.stdout.decode("utf-8").strip("\n")
                if out != DATASETS_SHA:
                    # fail early if we are checking the git sha, and it is not what we expect
                    # If you have made changes to the "test-datasets" repo and are seeing this, make sure to update
                    # DATASETS_SHA
                    raise ValueError(f"git sha {out} does not match the expected: {DATASETS_SHA}")
            return True

        return False

    def find_path() -> Path:
        found_path = None
        # If SRC_DIR or KATANA_SOURCE_DIR environment is set, just use it
        env_path = []
        if "SRC_DIR" in os.environ:
            env_path = [Path(os.environ["SRC_DIR"])]
        elif "KATANA_SOURCE_DIR" in os.environ:
            env_path = [Path(os.environ["KATANA_SOURCE_DIR"])]

        paths_to_check = list(Path(__file__).parents) + list(Path.cwd().parents) + env_path
        for path in paths_to_check:
            katana_path = path / "external" / "test-datasets"
            enterprise_path = path / "external" / "katana" / "external" / "test-datasets"
            if validate_path(katana_path, validate_sha=True):
                found_path = katana_path
            elif validate_path(enterprise_path, validate_sha=True):
                found_path = enterprise_path

        return found_path

    def download_datasets(invalidate_cache=False) -> Path:
        base_cache_path = Path(os.environ["HOME"]) / ".cache" / "katana"
        cache_path = base_cache_path / f"test-datasets-{DATASETS_SHA}"
        # the downloaded cache doesn't have a git sha, so can't validate it
        if validate_path(cache_path, validate_sha=False):
            if not invalidate_cache:
                return cache_path
            # cleanup anything leftover
            if cache_path.exists():
                try:
                    shutil.rmtree(cache_path)
                except OSError:
                    cache_path.unlink()

        # download a fresh copy
        base_cache_path.mkdir(parents=True, exist_ok=True)
        fn, _headers = urllib.request.urlretrieve(
            f"https://github.com/KatanaGraph/test-datasets/archive/{DATASETS_SHA}.zip"
        )
        try:
            with zipfile.ZipFile(fn, "r") as zip:
                zip.extractall(str(base_cache_path))
        finally:
            os.unlink(fn)

        if not validate_path(cache_path, validate_sha=False):
            raise ValueError("test-dataset repo was downloaded but is not available")
        return cache_path

    # try to use the env var
    if "KATANA_TEST_DATASETS" in os.environ:
        test_datasets_path = Path(os.environ["KATANA_TEST_DATASETS"])
        if not validate_path(test_datasets_path, validate_sha=True):
            test_datasets_path = None

    # try to locate the datasets repo in the source directory structure
    if test_datasets_path is None:
        test_datasets_path = find_path()

    # finally, try downloading the datasets for cases where we haven't built katana
    # and the source is not available
    if test_datasets_path is None and "KATANA_SOURCE_DIR" not in os.environ:
        test_datasets_path = download_datasets(invalidate_cache)

    if test_datasets_path is None:
        raise ValueError(f"unable to locate or download test-datasets repo, env_dump = {os.environ}")

    return test_datasets_path.resolve()


def _local_file_url(path: Path) -> url.URL:
    url_str = f"file://{path}"
    return url.URL(url_str)
