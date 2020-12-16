from galois.analytics import bfs, sssp
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

    # Verify with numba implementation of verifier
    verify_bfs(property_graph, start_node, new_property_id)

    # TODO: This should assert that the results are correct.


def test_sssp(property_graph: PropertyGraph):
    property_name = "NewProp"
    start_node = 0

    sssp(property_graph, start_node, "workFrom", property_name)

    node_schema: Schema = property_graph.node_schema()
    num_node_properties = len(node_schema)
    new_property_id = num_node_properties - 1
    assert node_schema.names[new_property_id] == property_name

    assert property_graph.get_node_property(property_name)[start_node].as_py() == 0

    # Verify with numba implementation of verifier
    verify_sssp(property_graph, start_node, new_property_id)

    # TODO: This should assert that the results are correct.


# TODO: Add more tests.
