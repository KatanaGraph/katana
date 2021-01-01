import pytest

from katana.example_utils import get_input
from katana.property_graph import PropertyGraph
from katana.galois import setActiveThreads


@pytest.fixture
def property_graph():
    g = PropertyGraph(get_input("propertygraphs/ldbc_003"))
    return g


@pytest.fixture
def threads_1():
    setActiveThreads(1)
    return True


@pytest.fixture(autouse=True)
def threads_default():
    setActiveThreads(4)
    return True


@pytest.fixture
def threads_many():
    setActiveThreads(16)
    return True
