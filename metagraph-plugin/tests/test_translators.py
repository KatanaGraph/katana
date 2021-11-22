import metagraph as mg


def test_num_nodes(kg_from_nx_di_8_12):
    nodes_total = 0
    for nid in kg_from_nx_di_8_12.value:
        nodes_total += 1
    assert kg_from_nx_di_8_12.value.num_nodes() == nodes_total
    assert kg_from_nx_di_8_12.value.num_nodes() == 8


def test_num_edges(kg_from_nx_di_8_12):
    edges_total = 0
    for nid in kg_from_nx_di_8_12.value:
        edges_total += len(kg_from_nx_di_8_12.value.edge_ids(nid))
    assert kg_from_nx_di_8_12.value.num_edges() == edges_total
    assert kg_from_nx_di_8_12.value.num_edges() == 12


def test_topology(kg_from_nx_di_8_12):
    assert kg_from_nx_di_8_12.value.edge_ids(0) == range(0, 3)
    assert kg_from_nx_di_8_12.value.edge_ids(1) == range(3, 5)
    assert kg_from_nx_di_8_12.value.edge_ids(2) == range(5, 8)
    assert kg_from_nx_di_8_12.value.edge_ids(3) == range(8, 9)
    assert kg_from_nx_di_8_12.value.edge_ids(4) == range(9, 10)
    assert kg_from_nx_di_8_12.value.edge_ids(5) == range(10, 12)
    assert [kg_from_nx_di_8_12.value.get_edge_dest(i) for i in kg_from_nx_di_8_12.value.edge_ids(0)] == [1, 3, 4]
    assert [kg_from_nx_di_8_12.value.get_edge_dest(i) for i in kg_from_nx_di_8_12.value.edge_ids(2)] == [4, 5, 6]
    assert [kg_from_nx_di_8_12.value.get_edge_dest(i) for i in kg_from_nx_di_8_12.value.edge_ids(4)] == [7]
    assert [kg_from_nx_di_8_12.value.get_edge_dest(i) for i in kg_from_nx_di_8_12.value.edge_ids(5)] == [6, 7]


def test_schema(kg_from_nx_di_8_12):
    assert len(kg_from_nx_di_8_12.value.loaded_node_schema()) == 0
    assert len(kg_from_nx_di_8_12.value.loaded_edge_schema()) == 1


def test_edge_property_directed(kg_from_nx_di_8_12):
    assert kg_from_nx_di_8_12.value.loaded_edge_schema()[0].name == "value_from_translator"
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


def test_compare_node_count(nx_from_kg_di_8_12, katanagraph_cleaned_8_12_di):
    nlist = [each_node[0] for each_node in list(nx_from_kg_di_8_12.value.nodes(data=True))]
    num_no_edge_nodes = 0
    for nid in katanagraph_cleaned_8_12_di.value:
        if nid not in nlist:
            assert katanagraph_cleaned_8_12_di.value.edge_ids(nid) == range(0, 0)
            num_no_edge_nodes += 1
    assert num_no_edge_nodes + len(nlist) == katanagraph_cleaned_8_12_di.value.num_nodes()
    assert num_no_edge_nodes == 0


def test_compare_edge_count(nx_from_kg_di_8_12, katanagraph_cleaned_8_12_di):
    edge_dict_count = {(each_e[0], each_e[1]): 0 for each_e in list(nx_from_kg_di_8_12.value.edges(data=True))}
    for src in katanagraph_cleaned_8_12_di.value:
        for dest in [
            katanagraph_cleaned_8_12_di.value.get_edge_dest(e) for e in katanagraph_cleaned_8_12_di.value.edge_ids(src)
        ]:
            if (src, dest) in edge_dict_count:
                edge_dict_count[(src, dest)] += 1
    assert sum([edge_dict_count[i] for i in edge_dict_count]) == katanagraph_cleaned_8_12_di.value.num_edges()
    assert len(list(nx_from_kg_di_8_12.value.edges(data=True))) == katanagraph_cleaned_8_12_di.value.num_edges()
