import metagraph as mg
import pandas as pd
import pytest

import katana.local
from katana.example_data import get_input
from katana.local import Graph


# Currently PropertyGraph does not support undirected graphs
# we are using directed graphs with symmetric edges to denote undirected graphs.
@pytest.fixture(autouse=True)
def pg_rmat15_cleaned_symmetric():
    katana.local.initialize()
    pg = Graph(get_input("propertygraphs/rmat15_cleaned_symmetric"))
    return pg


@pytest.fixture(autouse=True)
def katanagraph_rmat15_cleaned_di(pg_rmat15_cleaned_symmetric):
    katana_graph = mg.wrappers.Graph.KatanaGraph(pg_rmat15_cleaned_symmetric)
    return katana_graph


@pytest.fixture(autouse=True)
def katanagraph_rmat15_cleaned_ud(pg_rmat15_cleaned_symmetric):
    katana_graph = mg.wrappers.Graph.KatanaGraph(
        pg_rmat15_cleaned_symmetric, is_weighted=True, edge_weight_prop_name="value", is_directed=False
    )
    return katana_graph


@pytest.fixture(autouse=True)
def networkx_weighted_undirected_8_12():
    df = pd.read_csv("tests/data/edge1.csv")
    em = mg.wrappers.EdgeMap.PandasEdgeMap(df, "Source", "Destination", "Weight", is_directed=False)
    graph1 = mg.algos.util.graph.build(em)
    return graph1


@pytest.fixture(autouse=True)
def networkx_weighted_directed_8_12():
    df = pd.read_csv("tests/data/edge1.csv")
    em = mg.wrappers.EdgeMap.PandasEdgeMap(df, "Source", "Destination", "Weight", is_directed=True)
    graph1 = mg.algos.util.graph.build(em)
    return graph1




# directed graph
@pytest.fixture(autouse=True)
def kg_from_nx_di_8_12(networkx_weighted_directed_8_12):
    pg_test_case = mg.translate(networkx_weighted_directed_8_12, mg.wrappers.Graph.KatanaGraph)
    return pg_test_case


# undirected graph
@pytest.fixture(autouse=True)
def kg_from_nx_ud_8_12(networkx_weighted_undirected_8_12):
    pg_test_case = mg.translate(networkx_weighted_undirected_8_12, mg.wrappers.Graph.KatanaGraph)
    return pg_test_case


@pytest.fixture(autouse=True)
def nx_from_kg_rmat15_cleaned_di(katanagraph_rmat15_cleaned_di):
    return mg.translate(katanagraph_rmat15_cleaned_di, mg.wrappers.Graph.NetworkXGraph)

@pytest.fixture(autouse=True)
def nx_from_kg_rmat15_cleaned_ud(katanagraph_rmat15_cleaned_ud):
    return mg.translate(katanagraph_rmat15_cleaned_ud, mg.wrappers.Graph.NetworkXGraph)


@pytest.fixture(autouse=True)
def kg_from_nx_di_8_12(networkx_weighted_directed_8_12):
    pg_test_case = mg.translate(networkx_weighted_directed_8_12, mg.wrappers.Graph.KatanaGraph)
    return pg_test_case


def pytest_addoption(parser):
    parser.addoption(
        "--runslow", action="store_true", default=False, help="run slow tests"
    )

def pytest_runtest_setup(item):
    if 'runslow' in item.keywords and not item.config.getoption("--runslow"):
        pytest.skip("need --runslow option to run this test")
