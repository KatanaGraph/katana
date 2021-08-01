from katana.analytics import BfsStatistics, SsspStatistics, bfs_assert_valid, sssp_assert_valid
from katana.lonestar.analytics.bfs import bfs_sync_pg, verify_bfs
from katana.lonestar.analytics.jaccard import jaccard
from katana.lonestar.analytics.sssp import sssp, verify_sssp
from katana.property_graph import PropertyGraph


def test_bfs(property_graph: PropertyGraph):
    start_node = 0
    property_name = "NewProp"

    bfs_sync_pg(property_graph, start_node, property_name)

    num_node_properties = len(property_graph.node_schema())
    new_property_id = num_node_properties - 1
    verify_bfs(property_graph, start_node, new_property_id)

    stats = BfsStatistics(property_graph, property_name)
    assert stats.n_reached_nodes == 3


def test_sssp(property_graph):
    property_name = "NewProp"
    weight_name = "workFrom"
    start_node = 0

    sssp(property_graph, start_node, weight_name, 6, property_name)

    num_node_properties = len(property_graph.node_schema())
    new_property_id = num_node_properties - 1
    verify_sssp(property_graph, start_node, new_property_id)

    sssp_assert_valid(property_graph, start_node, weight_name, property_name)

    stats = SsspStatistics(property_graph, property_name)

    print(stats)
    assert stats.max_distance == 0.0


def test_jaccard(property_graph):
    graph = property_graph
    start_node = 0
    property_name = "NewProp"

    jaccard(graph, start_node, property_name)

    # TODO: This should assert that the results are correct.


# TODO: Add more tests.
