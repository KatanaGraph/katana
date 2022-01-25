import filecmp
import json
import pathlib
import shutil
import tempfile
from collections.abc import MutableMapping

import click
import pytest

from katana.example_data import get_rdg_dataset
from katana.local import Graph

### Tests Functions ###


def test_storage_format_unchanged_local():
    """
    load up a known good rdg, store a copy
    compare the storage format of the known good rdg to our stored copy
    Attempts to catch the following situations:
    1) the storage_format_version was changed but the rdg test datasets have not been updated
    2) the on disk storage format was changed, but the storage_format_version was not
    3) unstable storage format changes that are in use without the unstable storage format flag

    TODO(emcginnis): this test would be best if we had some way to create a 'maximal' RDG, aka one with as many
        optional features present as possible
        the current 'maximal' input requires the developer to be aware of all optional storage format features
        which is not realistic/sustainable
    """
    orig_rdg = get_rdg_dataset("ldbc_003_maximal")
    orig_graph = Graph(orig_rdg)
    new_rdg = tempfile.mkdtemp()
    orig_graph.write(new_rdg)
    # ensure we can load it, so we can say it is sort of sane
    Graph(new_rdg)

    orig_rdg_path = pathlib.Path(orig_rdg)
    new_rdg_path = pathlib.Path(new_rdg)

    assert get_storage_format_version(orig_rdg_path) == get_storage_format_version(new_rdg_path), (
        "storage_format_version mismatch between the known good rdg and the generated rdg. Ensure that the rdgs in"
        "test-datasets/rdg_datasets have been updated to use the newest supported storage_format_version."
    )

    assert validate_rdg_storage_format_match(orig_rdg_path, new_rdg_path), (
        "storage format mismatch between the known good rdg and the generated rdg."
        "This usually is due to one of the following: \n"
        "1) The storage format was changed, but the storage_format_version was not bumped up \n"
        "2) An unstable feature is not properly gated behind the unstable storage format flag,"
        "resulting in the unstable feature getting added to stable RDGs."
    )

    # only cleanup the temp rdg on success to make debugging failures easier
    shutil.rmtree(new_rdg)


### Support Functions ####

# json file filename substrings
manifest_filename_substring = "_rdg.manifest"
part_header_filename_substring = "part_vers"


def list_diff(li1, li2):
    """
    returns the difference of the two lists
    """
    return list(set(li1) - set(li2)) + list(set(li2) - set(li1))


def remove_rand_string(orig):
    """
    removes the random string appended to RDG file names
    """
    return orig.split("-")[0]


def get_storage_format_version(rdg_path):
    latest_part_header = None
    for part_header in rdg_path.glob("part_vers*_rdg_node00000"):
        if latest_part_header is None:
            latest_part_header = part_header
        elif part_header > latest_part_header:
            latest_part_header = part_header

    with open(latest_part_header) as f:
        part_header_json = json.load(f)
        return part_header_json.get("kg.v1.storage_format_version")


def flatten_and_sanitize(dictionary, parent_key=False, separator="::", verbose=False):
    """
    Turn a nested dictionary into a flattened dictionary,
    while also removing the random appended string from
    the dictionary value
    :param dictionary: The dictionary to flatten
    :param parent_key: The string to prepend to dictionary's keys
    :param separator: The string used to separate flattened keys
    :return: A flattened dictionary
    """

    items = []
    for key, value in dictionary.items():
        if verbose:
            print("checking:", key)
        new_key = str(parent_key) + separator + key if parent_key else key
        if isinstance(value, MutableMapping):
            if verbose:
                print(new_key, ": dict found")
            if not value.items():
                if verbose:
                    print(f"Adding key-value pair: {new_key}, {None}")
                items.append((new_key, None))
            else:
                items.extend(flatten_and_sanitize(value, new_key, separator, verbose).items())
        elif isinstance(value, list):
            if verbose:
                print(new_key, ": list found")
            if len(value):
                for k, v in enumerate(value):
                    items.extend(flatten_and_sanitize({str(k): v}, new_key, separator, verbose).items())
            else:
                if verbose:
                    print(f"Adding key-value pair: {new_key}, {None}")
                items.append((new_key, None))
        else:
            # remove the appended random string so we can actually compare two json files deterministically
            if isinstance(value, str):
                value = remove_rand_string(value)
            if verbose:
                print(f"Adding key-value pair: {new_key}, {value}")
            items.append((new_key, value))
    return dict(items)


def json_match(first_file, second_file, verbose=False):
    """
    Returns True iff the structure and values of the
    first_file are all identical to the superset_file
    """
    with open(first_file) as first_f:
        with open(second_file) as second_f:
            first_json = json.load(first_f)
            second_json = json.load(second_f)

            # flatten the json files.
            # this also remove the random strings appended to some of the json value strings
            # to make comparing the two possible
            # TODO(emcginnis): if/when we no longer put random strings at the end of
            # file names, the flatten/sanitize step would no longer be needed
            # instead use == and only flatten when == fails, so we can provide debug info
            first_flat = flatten_and_sanitize(first_json, verbose)
            second_flat = flatten_and_sanitize(second_json, verbose)

            if sorted(first_flat) != sorted(second_flat):
                print(f"{first_file} json does not match {second_file}," f"diff = {list_diff(first_flat, second_flat)}")
                return False

            if verbose:
                print(f"json files {first_file} and {second_file} match")
            return True


def validate_rdg_storage_format_subset(subset_path, superset_path, verbose=False):
    """
    uses the RDG at subset_path to validate the structure of the RDG at superset_path
    ensures that all files in the subset are available in the superset, and are identical
    This does not prove the two RDGs are identical, only that the RDG at superset_path is at
    least a superset of the RDG at subset_path
    """
    manifest_files = []
    part_header_files = []
    compare_paths = []

    file_paths = [x for x in subset_path.glob("**/*") if x.is_file()]

    # sort out the json files
    for path in file_paths:
        if manifest_filename_substring in path.name:
            manifest_files.append(path)
        elif part_header_filename_substring in path.name:
            part_header_files.append(path)
        else:
            # all json files that we do not recognize will end up getting compared as normal files
            compare_paths.append(path)

    # compare all parquet and data files
    filecmp.clear_cache()
    for subset_file in compare_paths:
        subset_no_rand = remove_rand_string(subset_file.name)
        found = False
        for superset_file in superset_path.glob(f"{subset_no_rand}*"):
            if filecmp.cmp(subset_file, superset_file, shallow=False):
                if verbose:
                    print(f"{subset_file} in {subset_path} is equal to {superset_file} in {superset_path}")
                found = True

        if not found:
            print(f"Failed to find matching file for {subset_no_rand}-* in {superset_path}")
            return False

    # compare the json files
    for manifest in manifest_files:
        if verbose:
            print(f"checking json manifest files {manifest}, {superset_path / manifest.name}")
        if not json_match(manifest, superset_path / manifest.name, verbose):
            print(f"json manifest file {manifest} does not match {superset_path / manifest.name}")
            return False

    for part_header in part_header_files:
        if verbose:
            print(f"checking json part_header files {part_header}, {superset_path / part_header.name}")
        if not json_match(part_header, superset_path / part_header.name, verbose):
            print(f"json part_header file {part_header} does not match {superset_path / part_header.name}")
            return False

    return True


def validate_rdg_storage_format_match(first_path, second_path, verbose=False):
    """
    prove the storage format of the two RDGs match by proving that the second is at least a superset
    of the first, and then that the first is at least a superset of the second.
    """

    if get_storage_format_version(first_path) != get_storage_format_version(second_path):
        raise RuntimeError(
            "the storage_format_version of the two RDGs does not match. The storage_format_version must match to"
            "compare the storage format of the two RDGs"
        )

    # first ensure all files available in the first rdg are available and identical to the second
    if not validate_rdg_storage_format_subset(first_path, second_path, verbose):
        return False
    if verbose:
        print(f"all files in {first_path} match the files in {second_path}. Checking the inverse next...")

    # next, flip it. Ensure all files available in the second rdg are available and identical to the first
    if not validate_rdg_storage_format_subset(second_path, first_path, verbose):
        return False

    if verbose:
        print(f"all files in {second_path} match the files in {first_path}.")

    return True


### cli support ###


@click.group()
def cli():
    """

    Validates that the storage format of the two provided RDGs matches
    If the two RDGs are the same exact graph, stored in the same storage_format_version,
    then this script returns true
    If the two RDGs differ, this script returns false

    to run this from the command line, first run a full build then
    bash build/python_env.sh python3 python/test/test_storage_format.py rdgs -V -O <rdg_1> -N <rdg_2>
    """


@cli.command(name="rdgs")
@click.option("--first_rdg", "-O", type=str, required=True)
@click.option("--second_rdg", "-N", type=str, required=True)
@click.option("--verbose", "-V", default=False, is_flag=True)
def cli_rdgs(first_rdg: str, second_rdg: str, verbose: bool):
    first_path = pathlib.Path(first_rdg)
    second_path = pathlib.Path(second_rdg)

    return validate_rdg_storage_format_match(first_path, second_path, verbose)


if __name__ == "__main__":
    cli.main(prog_name="validate_storage_format")
