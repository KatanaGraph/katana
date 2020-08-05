import pytest

from galois.example_utils import get_input


@pytest.fixture
def property_graph():
    from galois.property_graph import PropertyGraph

    g = PropertyGraph(get_input("propertygraphs/ldbc_003/meta"))
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
