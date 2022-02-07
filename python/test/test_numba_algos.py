from test.lonestar.bfs import bfs_sync_pg, verify_bfs
from test.lonestar.jaccard import jaccard
from test.lonestar.sssp import sssp, verify_sssp

from katana.local import Graph
from katana.local.analytics import BfsStatistics, SsspStatistics, sssp_assert_valid


def test_bfs(graph: Graph):
    start_node = 0
    property_name = "NewProp"

    bfs_sync_pg(graph, start_node, property_name)

    verify_bfs(graph, start_node, property_name)

    stats = BfsStatistics(graph, property_name)
    assert stats.n_reached_nodes == 3


def test_sssp(graph):
    property_name = "NewProp"
    weight_name = "workFrom"
    start_node = 0

    sssp(graph, start_node, weight_name, 6, property_name)

    verify_sssp(graph, start_node, property_name)

    sssp_assert_valid(graph, start_node, weight_name, property_name)

    stats = SsspStatistics(graph, property_name)

    assert stats.max_distance == 0.0


def test_jaccard(graph):
    start_node = 0
    property_name = "NewProp"

    jaccard(graph, start_node, property_name)

    # TODO: This should assert that the results are correct.


# TODO: Add more tests.
