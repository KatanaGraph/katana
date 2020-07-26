import pytest

import os.path
import os
import tempfile
import tarfile
import urllib.request

@pytest.fixture(scope="session")
def small_inputs_dir(request):
    small_inputs_dir = request.config.cache.get("small_inputs_dir", None)
    if small_inputs_dir is not None and os.path.isdir(small_inputs_dir) and os.path.isfile(small_inputs_dir + "/propertygraphs/ldbc_003/meta"):
        return small_inputs_dir
    else:
        # Check for CI cached graphs
        ci_inputs_path = os.environ["HOME"] + "/.cache/graph/inputs"
        if os.path.isdir(ci_inputs_path):
            small_inputs_dir = ci_inputs_path
        else:
            # If there is no CI cache then download the data ourselves.
            small_inputs_dir = tempfile.mkdtemp()
            fn, headers = urllib.request.urlretrieve("https://katana-ci-public.s3.us-east-1.amazonaws.com/inputs/katana-inputs-v2.1.tar.gz")
            with tarfile.open(fn) as tar:
                tar.extractall(small_inputs_dir)
        request.config.cache.set("small_inputs_dir", small_inputs_dir)
    return small_inputs_dir

@pytest.fixture
def property_graph(request, small_inputs_dir):
    from galois.property_graph import PropertyGraph
    g = PropertyGraph(small_inputs_dir + "/propertygraphs/ldbc_003/meta")
    return g

@pytest.fixture
def threads_1():
    from galois.shmem import setActiveThreads
    setActiveThreads(1)
    return True

@pytest.fixture(autouse=True)
def threads_default():
    from galois.shmem import setActiveThreads
    setActiveThreads(4)
    return True

@pytest.fixture
def threads_many():
    from galois.shmem import setActiveThreads
    setActiveThreads(16)
    return True
