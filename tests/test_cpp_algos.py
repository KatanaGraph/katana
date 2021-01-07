from galois.analytics import bfs, bfs_validate, BfsStatistics, sssp, sssp_validate, SsspStatistics
from galois.property_graph import PropertyGraph
from pyarrow import Schema

from galois.lonestar.analytics.bfs import verify_bfs
from galois.lonestar.analytics.sssp import verify_sssp


def test_bfs(property_graph: PropertyGraph):
    property_name = "NewProp"
    start_node = 0

    bfs(property_graph, start_node, property_name)

    node_schema: Schema = property_graph.node_schema()
    num_node_properties = len(node_schema)
    new_property_id = num_node_properties - 1
    assert node_schema.names[new_property_id] == property_name

    assert property_graph.get_node_property(property_name)[start_node].as_py() == 0

    assert bfs_validate(property_graph, property_name)

    stats = BfsStatistics(property_graph, property_name)

    assert stats.source_node == start_node
    assert stats.max_distance == 7

    # Verify with numba implementation of verifier as well
    verify_bfs(property_graph, start_node, new_property_id)


def test_sssp(property_graph: PropertyGraph):
    property_name = "NewProp"
    weight_name = "workFrom"
    start_node = 0

    sssp(property_graph, start_node, weight_name, property_name)

    node_schema: Schema = property_graph.node_schema()
    num_node_properties = len(node_schema)
    new_property_id = num_node_properties - 1
    assert node_schema.names[new_property_id] == property_name

    assert property_graph.get_node_property(property_name)[start_node].as_py() == 0

    assert sssp_validate(property_graph, start_node, weight_name, property_name)

    stats = SsspStatistics(property_graph, property_name)

    assert stats.max_distance == 4294967295.0

    # Verify with numba implementation of verifier
    verify_sssp(property_graph, start_node, new_property_id)


# TODO: Add more tests.
