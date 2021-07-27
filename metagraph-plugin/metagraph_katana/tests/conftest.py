import metagraph as mg
import pandas as pd
import pytest

import katana.local
from katana.example_utils import get_input
from katana.property_graph import PropertyGraph


# Currently PropertyGraph does not support undirected graphs
# we are using directed graphs with symmetric edges to denote undirected graphs.
@pytest.fixture(autouse=True)
def pg_rmat15_cleaned_symmetric():
    katana.local.initialize()
    pg = PropertyGraph(get_input("propertygraphs/rmat15_cleaned_symmetric"))
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
    df = pd.read_csv("metagraph_katana/data/edge1.csv")
    em = mg.wrappers.EdgeMap.PandasEdgeMap(df, "Source", "Destination", "Weight", is_directed=False)
    graph1 = mg.algos.util.graph.build(em)
    return graph1


@pytest.fixture(autouse=True)
def networkx_weighted_directed_8_12():
    df = pd.read_csv("metagraph_katana/data/edge1.csv")
    em = mg.wrappers.EdgeMap.PandasEdgeMap(df, "Source", "Destination", "Weight", is_directed=True)
    graph1 = mg.algos.util.graph.build(em)
    return graph1


@pytest.fixture
def order():
    return []


@pytest.fixture
def top(order, innermost):
    order.append("top")
