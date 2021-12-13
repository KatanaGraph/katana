import metagraph as mg
import numpy as np
import pandas as pd
import pyarrow
import pytest
from scipy.sparse import csr_matrix

import katana.local
from katana.example_data import get_rdg_dataset
from katana.local import Graph
from katana.local.import_data import from_csr


# Currently Graph does not support undirected graphs
# we are using directed graphs with symmetric edges to denote undirected graphs.
@pytest.fixture(autouse=True)
def pg_rmat15_cleaned_symmetric():
    katana.local.initialize()
    pg = Graph(get_rdg_dataset("rmat15_cleaned_symmetric"))
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


def gen_pg_cleaned_8_12_from_csr(is_directed):
    """
    A helper function for the test, generating Katana's Graph from an edge list
    """
    katana.local.initialize()
    elist_raw = [
        (0, 1, 4),
        (0, 3, 2),
        (0, 4, 7),
        (1, 3, 3),
        (1, 4, 5),
        (2, 4, 5),
        (2, 5, 2),
        (2, 6, 8),
        (3, 4, 1),
        (4, 7, 4),
        (5, 6, 4),
        (5, 7, 6),
    ]
    src_list = [each[0] for each in elist_raw]
    dest_list = [each[1] for each in elist_raw]
    nlist_raw = list(set(src_list) | set(dest_list))
    # sort the eddge list and node list
    if is_directed:
        elist = sorted(elist_raw, key=lambda each: (each[0], each[1]))
    else:
        inv_elist = [(each[1], each[0], each[2]) for each in elist_raw]
        elist = sorted(elist_raw + inv_elist, key=lambda each: (each[0], each[1]))
    nlist = sorted(nlist_raw, key=lambda each: each)
    # build the CSR format from the edge list (weight, (src, dst))
    row = np.array([each_edge[0] for each_edge in elist])
    col = np.array([each_edge[1] for each_edge in elist])
    data = np.array([each_edge[2] for each_edge in elist])
    csr = csr_matrix((data, (row, col)), shape=(len(nlist), len(nlist)))
    # call the katana api to build a Graph (unweighted) from the CSR format
    # noting that the first 0 in csr.indptr is excluded
    pg = from_csr(csr.indptr[1:], csr.indices)
    t = pyarrow.table(dict(value=data))
    pg.add_edge_property(t)
    return pg


@pytest.fixture(autouse=True)
def katanagraph_cleaned_8_12_di():
    pg_cleaned_8_12_from_csr_di = gen_pg_cleaned_8_12_from_csr(is_directed=True)
    katana_graph = mg.wrappers.Graph.KatanaGraph(pg_cleaned_8_12_from_csr_di)
    return katana_graph


@pytest.fixture(autouse=True)
def katanagraph_cleaned_8_12_ud():
    pg_cleaned_8_12_from_csr_ud = gen_pg_cleaned_8_12_from_csr(is_directed=False)
    katana_graph = mg.wrappers.Graph.KatanaGraph(
        pg_cleaned_8_12_from_csr_ud, is_weighted=True, edge_weight_prop_name="value", is_directed=False
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
def nx_from_kg_di_8_12(katanagraph_cleaned_8_12_di):
    return mg.translate(katanagraph_cleaned_8_12_di, mg.wrappers.Graph.NetworkXGraph)


@pytest.fixture(autouse=True)
def nx_from_kg_ud_8_12(katanagraph_cleaned_8_12_ud):
    return mg.translate(katanagraph_cleaned_8_12_ud, mg.wrappers.Graph.NetworkXGraph)


def pytest_addoption(parser):
    parser.addoption("--runslow", action="store_true", default=False, help="run slow tests")


def pytest_runtest_setup(item):
    if "runslow" in item.keywords and not item.config.getoption("--runslow"):
        pytest.skip("need --runslow option to run this test")
