import pytest

import katana.local
from katana.example_utils import get_input
from katana.galois import set_active_threads
from katana.property_graph import PropertyGraph

katana.local.initialize()


@pytest.fixture
def property_graph():
    g = PropertyGraph(get_input("propertygraphs/ldbc_003"))
    return g


@pytest.fixture
def threads_1():
    set_active_threads(1)
    return True


@pytest.fixture(autouse=True)
def threads_default():
    set_active_threads(4)
    return True


@pytest.fixture
def threads_many():
    set_active_threads(16)
    return True
