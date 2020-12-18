import pytest
from pytest import raises, approx

from galois.analytics import bfs, bfs_assert_valid, BfsStatistics, sssp, sssp_assert_valid, SsspStatistics, jaccard, JaccardPlan
from galois.property_graph import PropertyGraph
from pyarrow import Schema, table

import numpy as np

from galois.property_graph import PropertyGraph
from galois.lonestar.analytics.bfs import verify_bfs
from galois.lonestar.analytics.sssp import verify_sssp


def test_assert_valid(property_graph: PropertyGraph):
    with raises(AssertionError):
        bfs_assert_valid(property_graph, "workFrom")
    property_name = "NewProp"
    start_node = 0

    bfs(property_graph, start_node, property_name)

    v = property_graph.get_node_property(property_name).to_numpy().copy()
    v[0] = 100
    property_graph.add_node_property(table({"Prop2": v}))

    with raises(AssertionError):
        bfs_assert_valid(property_graph, "Prop2")


def test_bfs(property_graph: PropertyGraph):
    property_name = "NewProp"
    start_node = 0

    bfs(property_graph, start_node, property_name)

    node_schema: Schema = property_graph.node_schema()
    num_node_properties = len(node_schema)
    new_property_id = num_node_properties - 1
    assert node_schema.names[new_property_id] == property_name

    assert property_graph.get_node_property(property_name)[start_node].as_py() == 0

    bfs_assert_valid(property_graph, property_name)

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

    sssp_assert_valid(property_graph, start_node, weight_name, property_name)

    stats = SsspStatistics(property_graph, property_name)

    assert stats.max_distance == 4294967295.0

    # Verify with numba implementation of verifier
    verify_sssp(property_graph, start_node, new_property_id)


def test_jaccard(property_graph: PropertyGraph):
    property_name = "NewProp"
    compare_node = 0

    jaccard(property_graph, compare_node, property_name)

    node_schema: Schema = property_graph.node_schema()
    num_node_properties = len(node_schema)
    new_property_id = num_node_properties - 1
    assert node_schema.names[new_property_id] == property_name

    similarities: np.ndarray = property_graph.get_node_property(property_name).to_numpy()
    assert similarities[compare_node] == 1
    assert similarities[1917] == approx(0.28571428)
    assert similarities[2812] == approx(0.01428571)


@pytest.mark.skip("Not supported yet")
def test_jaccard_sorted(property_graph: PropertyGraph):
    sort_all_edges_by_dest(property_graph)

    property_name = "NewProp"
    compare_node = 0

    jaccard(property_graph, compare_node, property_name, JaccardPlan.sorted())

    similarities: np.ndarray = property_graph.get_node_property(property_name).to_numpy()
    assert similarities[compare_node] == 1
    assert similarities[1917] == approx(0.28571428)
    assert similarities[2812] == approx(0.01428571)


# TODO: Add more tests.
