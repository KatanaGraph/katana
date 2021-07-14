# Fixture scopes
# Fixtures are created when first requested by a test, and are destroyed based on their scope:

# function: the default scope, the fixture is destroyed at the end of the test.

# class: the fixture is destroyed during teardown of the last test in the class.

# module: the fixture is destroyed during teardown of the last test in the module.

# package: the fixture is destroyed during teardown of the last test in the package.

# session: the fixture is destroyed at the end of the test session.

# @pytest.fixture(scope='module')
# def smtp_connection():
#     return smtplib.SMTP("smtp.gmail.com", 587, timeout=5)


# content of tests/conftest.py
import pytest
import pandas as pd
import metagraph as mg

from katana.property_graph import PropertyGraph
import katana.local
from katana.example_utils import get_input

# Currently PropertyGraph does not support undirected graphs
# we are using directed graphs with symmetric edges to denote undirected graphs.
@pytest.fixture(autouse=True)
def pg_rmat15_cleaned_symmetric():
    katana.local.initialize()
    pg = PropertyGraph(get_input('propertygraphs/rmat15_cleaned_symmetric'))
    return pg

@pytest.fixture(autouse=True)
def kg_rmat15_cleaned_di(pg_rmat15_cleaned_symmetric):
    katana_graph = mg.wrappers.Graph.KatanaGraph(pg_rmat15_cleaned_symmetric)
    return katana_graph


@pytest.fixture(autouse=True)
def kg_rmat15_cleaned_ud(pg_rmat15_cleaned_symmetric):
    katana_graph = mg.wrappers.Graph.KatanaGraph(pg_rmat15_cleaned_symmetric, is_weighted=True, edge_weight_prop_name='value', is_directed=False)
    return katana_graph

# "nx" short for NetworkXGraph which is the major type of metagraph
@pytest.fixture(autouse=True)
def nx_weighted_undirected_8_12():
    df = pd.read_csv('metagraph_katana/data/edge1.csv')
    em = mg.wrappers.EdgeMap.PandasEdgeMap(df, 'Source', 'Destination', 'Weight', is_directed=False)
    graph1 = mg.algos.util.graph.build(em)
    return graph1


@pytest.fixture(autouse=True)
def nx_weighted_directed_8_12():
    df = pd.read_csv('metagraph_katana/data/edge1.csv')
    em = mg.wrappers.EdgeMap.PandasEdgeMap(df, 'Source', 'Destination', 'Weight', is_directed=True)
    graph1 = mg.algos.util.graph.build(em)
    return graph1


@pytest.fixture
def order():
    return []

@pytest.fixture
def top(order, innermost):
    order.append("top")


# tests/
#     __init__.py

#     conftest.py
#         # content of tests/conftest.py
#         import pytest

#         @pytest.fixture
#         def order():
#             return []

#         @pytest.fixture
#         def top(order, innermost):
#             order.append("top")

#     test_top.py
#         # content of tests/test_top.py
#         import pytest

#         @pytest.fixture
#         def innermost(order):
#             order.append("innermost top")

#         def test_order(order, top):
#             assert order == ["innermost top", "top"]

#     subpackage/
#         __init__.py

#         conftest.py
#             # content of tests/subpackage/conftest.py
#             import pytest

#             @pytest.fixture
#             def mid(order):
#                 order.append("mid subpackage")

#         test_subpackage.py
#             # content of tests/subpackage/test_subpackage.py
#             import pytest

#             @pytest.fixture
#             def innermost(order, mid):
#                 order.append("innermost subpackage")

#             def test_order(order, top):
#                 assert order == ["mid subpackage", "innermost subpackage", "top"]






# import katana.local
# from katana.example_utils import get_input
# from katana.galois import set_active_threads
# from katana.property_graph import PropertyGraph

# katana.local.initialize()


# @pytest.fixture
# def property_graph():
#     g = PropertyGraph(get_input("propertygraphs/ldbc_003"))
#     return g


# @pytest.fixture
# def threads_1():
#     set_active_threads(1)
#     return True


# @pytest.fixture(autouse=True)
# def threads_default():
#     set_active_threads(4)
#     return True


# @pytest.fixture
# def threads_many():
#     set_active_threads(16)
#     return True