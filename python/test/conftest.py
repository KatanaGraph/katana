import os

import pytest

import katana.local
from katana import set_active_threads
from katana.example_data import get_input
from katana.local import Graph

katana.local.initialize()


def pytest_configure(config):
    config.addinivalue_line("markers", "required_env(name): mark test to run only if environment variable is set")


def pytest_runtest_setup(item: "pytest.hookspec.Item"):
    envs = [mark.args[0] for mark in item.iter_markers(name="required_env")]
    all_set = all(os.getenv(env) is not None for env in envs)
    if not all_set:
        pytest.skip("test requires environment variables {!r}".format(envs))


@pytest.fixture
def graph():
    g = Graph(get_input("propertygraphs/ldbc_003"))
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
