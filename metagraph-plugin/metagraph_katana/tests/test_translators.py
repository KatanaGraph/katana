import metagraph as mg
import pytest


@pytest.fixture(autouse=True)
def kg_from_nx_di_8_12(networkx_weighted_directed_8_12):
    pg_test_case = mg.translate(networkx_weighted_directed_8_12, mg.wrappers.Graph.KatanaGraph)
    return pg_test_case


def test_num_nodes(kg_from_nx_di_8_12):
    nodes_total = 0
    for nid in kg_from_nx_di_8_12.value:
        nodes_total += 1
    assert kg_from_nx_di_8_12.value.num_nodes() == nodes_total
    assert kg_from_nx_di_8_12.value.num_nodes() == 8


def test_num_edges(kg_from_nx_di_8_12):
    edges_total = 0
    for nid in kg_from_nx_di_8_12.value:
        edges_total += len(kg_from_nx_di_8_12.value.edges(nid))
    assert kg_from_nx_di_8_12.value.num_edges() == edges_total
    assert kg_from_nx_di_8_12.value.num_edges() == 12


def test_topology(kg_from_nx_di_8_12):
    assert kg_from_nx_di_8_12.value.edges(0) == range(0, 3)
    assert kg_from_nx_di_8_12.value.edges(1) == range(3, 5)
    assert kg_from_nx_di_8_12.value.edges(2) == range(5, 8)
    assert kg_from_nx_di_8_12.value.edges(3) == range(8, 9)
    assert kg_from_nx_di_8_12.value.edges(4) == range(9, 10)
    assert kg_from_nx_di_8_12.value.edges(5) == range(10, 12)
    assert [kg_from_nx_di_8_12.value.get_edge_dest(i) for i in kg_from_nx_di_8_12.value.edges(0)] == [1, 3, 4]
    assert [kg_from_nx_di_8_12.value.get_edge_dest(i) for i in kg_from_nx_di_8_12.value.edges(2)] == [4, 5, 6]
    assert [kg_from_nx_di_8_12.value.get_edge_dest(i) for i in kg_from_nx_di_8_12.value.edges(4)] == [7]
    assert [kg_from_nx_di_8_12.value.get_edge_dest(i) for i in kg_from_nx_di_8_12.value.edges(5)] == [6, 7]


def test_schema(kg_from_nx_di_8_12):
    assert len(kg_from_nx_di_8_12.value.node_schema()) == 0
    assert len(kg_from_nx_di_8_12.value.edge_schema()) == 1


def test_edge_property_directed(kg_from_nx_di_8_12):
    assert kg_from_nx_di_8_12.value.edge_schema()[0].name == "value_from_translator"
    assert kg_from_nx_di_8_12.value.get_edge_property(0) == kg_from_nx_di_8_12.value.get_edge_property(
        "value_from_translator"
    )
    assert kg_from_nx_di_8_12.value.get_edge_property("value_from_translator").tolist() == [
        4,
        2,
        7,
        3,
        5,
        5,
        2,
        8,
        1,
        4,
        4,
        6,
    ]


@pytest.fixture(autouse=True)
def nx_from_kg_rmat15_cleaned_di(katanagraph_rmat15_cleaned_di):
    return mg.translate(katanagraph_rmat15_cleaned_di, mg.wrappers.Graph.NetworkXGraph)


def test_num_nodes(nx_from_kg_rmat15_cleaned_di, katanagraph_rmat15_cleaned_di):
    nlist = [each_node[0] for each_node in list(nx_from_kg_rmat15_cleaned_di.value.nodes(data=True))]
    num_no_edge_nodes = 0
    for nid in katanagraph_rmat15_cleaned_di.value:
        if nid not in nlist:
            assert katanagraph_rmat15_cleaned_di.value.edges(nid) == range(0, 0)
            num_no_edge_nodes += 1
    assert num_no_edge_nodes + len(nlist) == katanagraph_rmat15_cleaned_di.value.num_nodes()


def test_num_edges(nx_from_kg_rmat15_cleaned_di):
    assert len(list(nx_from_kg_rmat15_cleaned_di.value.edges(data=True))) == 363194


def test_num_edges(nx_from_kg_rmat15_cleaned_di, katanagraph_rmat15_cleaned_di):
    edge_dict_count = {
        (each_e[0], each_e[1]): 0 for each_e in list(nx_from_kg_rmat15_cleaned_di.value.edges(data=True))
    }
    num_same_src_dest_edges = 0
    for src in katanagraph_rmat15_cleaned_di.value:
        for dest in [katanagraph_rmat15_cleaned_di.value.get_edge_dest(e) for e in katanagraph_rmat15_cleaned_di.value.edges(src)]:
            if (src, dest) in edge_dict_count:
                edge_dict_count[(src, dest)] += 1
    assert sum([edge_dict_count[i] for i in edge_dict_count]) == katanagraph_rmat15_cleaned_di.value.num_edges()
