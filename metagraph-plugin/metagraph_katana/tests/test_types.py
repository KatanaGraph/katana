import metagraph as mg
import pytest


def test_num_nodes(katanagraph_rmat15_cleaned_di):
    cnt = 0
    for nid in katanagraph_rmat15_cleaned_di.value:
        cnt += 1
    assert katanagraph_rmat15_cleaned_di.value.num_nodes() == 32768
    assert katanagraph_rmat15_cleaned_di.value.num_nodes() == cnt


def test_num_edges(katanagraph_rmat15_cleaned_di):
    cnt = 0
    for nid in katanagraph_rmat15_cleaned_di.value:
        cnt += len(katanagraph_rmat15_cleaned_di.value.edges(nid))
    assert katanagraph_rmat15_cleaned_di.value.num_edges() == 363194
    assert katanagraph_rmat15_cleaned_di.value.num_edges() == cnt


def test_node_schema(katanagraph_rmat15_cleaned_di):
    assert "names" in dir(katanagraph_rmat15_cleaned_di.value.node_schema())
    assert "types" in dir(katanagraph_rmat15_cleaned_di.value.node_schema())
    assert len(katanagraph_rmat15_cleaned_di.value.node_schema()) == 0


def test_edge_schema(katanagraph_rmat15_cleaned_di):
    assert "names" in dir(katanagraph_rmat15_cleaned_di.value.edge_schema())
    assert "types" in dir(katanagraph_rmat15_cleaned_di.value.edge_schema())
    assert len(katanagraph_rmat15_cleaned_di.value.edge_schema()) == 1


def test_edge_property(katanagraph_rmat15_cleaned_di):
    assert katanagraph_rmat15_cleaned_di.value.edge_schema()[0].name == "value"
    assert katanagraph_rmat15_cleaned_di.value.get_edge_property(0) == katanagraph_rmat15_cleaned_di.value.get_edge_property("value")
    assert katanagraph_rmat15_cleaned_di.value.get_edge_property("value").to_pandas()[0] == 339302416426


def test_topology(katanagraph_rmat15_cleaned_di):
    assert katanagraph_rmat15_cleaned_di.value.edges(0) == range(0, 20767)
    assert [katanagraph_rmat15_cleaned_di.value.get_edge_dest(i) for i in katanagraph_rmat15_cleaned_di.value.edges(0)][0:5] == [
        1,
        2,
        3,
        4,
        5,
    ]
    assert katanagraph_rmat15_cleaned_di.value.edges(8) == range(36475, 41133)
    assert [katanagraph_rmat15_cleaned_di.value.get_edge_dest(i) for i in katanagraph_rmat15_cleaned_di.value.edges(8)][0:5] == [
        0,
        9,
        10,
        11,
        12,
    ]


def test_num_nodes(networkx_weighted_undirected_8_12, networkx_weighted_directed_8_12):
    assert len(list(networkx_weighted_undirected_8_12.value.nodes(data=True))) == 8
    assert len(list(networkx_weighted_directed_8_12.value.nodes(data=True))) == 8


def test_num_nodes(networkx_weighted_undirected_8_12, networkx_weighted_directed_8_12):
    assert len(list(networkx_weighted_undirected_8_12.value.edges(data=True))) == 12
    assert len(list(networkx_weighted_directed_8_12.value.edges(data=True))) == 12


def test_topology(networkx_weighted_undirected_8_12, networkx_weighted_directed_8_12):
    assert list(networkx_weighted_undirected_8_12.value.nodes(data=True)) == list(
        networkx_weighted_directed_8_12.value.nodes(data=True)
    )
    assert list(networkx_weighted_undirected_8_12.value.nodes(data=True)) == [
        (0, {}),
        (1, {}),
        (3, {}),
        (4, {}),
        (2, {}),
        (5, {}),
        (6, {}),
        (7, {}),
    ]
    assert list(networkx_weighted_undirected_8_12.value.edges(data=True)) == [
        (0, 1, {"weight": 4}),
        (0, 3, {"weight": 2}),
        (0, 4, {"weight": 7}),
        (1, 3, {"weight": 3}),
        (1, 4, {"weight": 5}),
        (3, 4, {"weight": 1}),
        (4, 2, {"weight": 5}),
        (4, 7, {"weight": 4}),
        (2, 5, {"weight": 2}),
        (2, 6, {"weight": 8}),
        (5, 6, {"weight": 4}),
        (5, 7, {"weight": 6}),
    ]
    assert list(networkx_weighted_directed_8_12.value.edges(data=True)) == [
        (0, 1, {"weight": 4}),
        (0, 3, {"weight": 2}),
        (0, 4, {"weight": 7}),
        (1, 3, {"weight": 3}),
        (1, 4, {"weight": 5}),
        (3, 4, {"weight": 1}),
        (4, 7, {"weight": 4}),
        (2, 4, {"weight": 5}),
        (2, 5, {"weight": 2}),
        (2, 6, {"weight": 8}),
        (5, 6, {"weight": 4}),
        (5, 7, {"weight": 6}),
    ]
