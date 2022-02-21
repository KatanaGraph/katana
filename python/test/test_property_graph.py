from tempfile import NamedTemporaryFile, TemporaryDirectory

import numpy as np
import pandas
import pyarrow
import pytest

from katana import do_all, do_all_operator
from katana.local import Graph


def test_load(graph):
    assert graph.num_nodes() == 29946
    assert graph.num_edges() == 43072
    assert len(graph.loaded_node_schema()) == 17
    assert len(graph.loaded_edge_schema()) == 3


def test_write(graph):
    with TemporaryDirectory() as tmpdir:
        graph.write(tmpdir)
        del graph
        graph = Graph(tmpdir)
    assert graph.num_nodes() == 29946
    assert graph.num_edges() == 43072
    assert len(graph.loaded_node_schema()) == 17
    assert len(graph.loaded_edge_schema()) == 3


# TODO(amp): Reinstant this test once it matches the actual RDG semantics.
@pytest.mark.skip("Does not work. Underlying semantics may be wrong or different.")
def test_commit(graph):
    with TemporaryDirectory() as tmpdir:
        graph.path = tmpdir
        graph.write()
        del graph
        graph = Graph(tmpdir)
    assert graph.num_nodes() == 29092
    assert graph.num_edges() == 39283
    assert len(graph.loaded_node_schema()) == 17
    assert len(graph.loaded_edge_schema()) == 3


def test_get_edge_dst(graph):
    assert graph.get_edge_dst(0) == 8014
    assert graph.get_edge_dst(1) == 8014


def test_edge_data_frame(graph):
    edges = graph.out_edges()
    assert len(edges.columns) == 6
    assert len(edges) == 43072
    assert edges.at[0, "dest"] == 8014
    assert edges["dest"][1] == 8014
    assert edges["source"][0] == 0
    assert edges["source"][1] == 1
    assert edges["source"][19993] == 20000


def test_edge_data_frame_1_node(graph):
    edges = graph.out_edges(0)
    assert len(edges.columns) == 6
    assert len(edges) == 1
    assert edges.at[0, "dest"] == 8014
    assert edges["dest"][0] == 8014
    assert edges["source"][0] == 0


def test_edge_data_frame_selected_nodes(graph):
    edges = graph.out_edges([20000, 5000], properties={"classYear"})
    assert len(edges.columns) == 1 + 3
    assert len(edges) == len(graph.out_edge_ids(20000)) + len(graph.out_edge_ids(5000))
    assert edges.at[0, "id"] == 19993
    assert edges.at[1, "id"] == 5000
    assert edges.at[0, "dest"] == 9475
    assert edges.at[1, "dest"] == 9167
    assert edges["dest"][0] == 9475
    assert edges["dest"][1] == 9167
    assert edges["source"][0] == 20000
    assert edges["source"][1] == 5000


def test_edge_data_frame_simple_slice(graph):
    edges = graph.out_edges(slice(110, 112))
    assert len(edges) == 2
    assert edges.at[0, "dest"] == 8019
    assert edges.at[1, "dest"] == 8019
    assert edges["dest"][0] == 8019
    assert edges["dest"][1] == 8019
    assert edges["source"][0] == 110
    assert edges["source"][1] == 111


def test_edge_data_frame_complex_slice(graph):
    edges = graph.out_edges(slice(1010, 1020, 2))
    assert len(edges) == 5
    assert edges.at[0, "dest"] == 7993
    assert edges.at[1, "dest"] == 7993
    assert edges["dest"][0] == 7993
    assert edges["dest"][1] == 7993
    assert edges["source"][0] == 1010
    assert edges["source"][1] == 1012


def test_edge_data_frame_to_pandas(graph):
    edges = graph.out_edges().to_pandas()
    assert len(edges.columns) == 6
    assert len(edges) == 43072
    assert edges.at[0, "dest"] == 8014
    assert edges["dest"][1] == 8014
    assert edges["source"][0] == 0


def test_reachable_from_10(graph):
    reachable = []
    for eid in graph.out_edge_ids(10):
        reachable.append(graph.get_edge_dst(eid))
    assert reachable == [8015]


def test_nodes_count_edges(graph):
    total = 0
    for nid in range(graph.num_nodes()):
        total += len(graph.out_edge_ids(nid))
    assert graph.num_edges() == total


def test_get_node_property_exception(graph):
    with pytest.raises(LookupError):
        graph.get_node_property("_mispelled")


def test_get_node_property(graph):
    prop2 = graph.get_node_property("length")
    assert prop2[10].as_py() is None


def test_remove_node_property(graph):
    graph.remove_node_property("length")
    assert len(graph.loaded_node_schema()) == 16
    assert graph.loaded_node_schema()[4].name == "email"


def test_add_node_property_exception(graph):
    t = pyarrow.table(dict(new_prop=[1, 2]))
    with pytest.raises(ValueError):
        # Should raise because new property isn't long enough for the node set
        graph.add_node_property(t)


def test_add_node_property(graph):
    t = pyarrow.table(dict(new_prop=range(graph.num_nodes())))
    graph.add_node_property(t)
    assert graph.get_node_property("new_prop").combine_chunks() == pyarrow.array(range(graph.num_nodes()))


def test_add_node_property_kwarg(graph):
    graph.add_node_property(new_prop=range(graph.num_nodes()))
    assert graph.get_node_property("new_prop").combine_chunks() == pyarrow.array(range(graph.num_nodes()))


def test_add_node_property_dataframe(graph):
    graph.add_node_property(pandas.DataFrame(dict(new_prop=range(graph.num_nodes()))))
    assert graph.get_node_property("new_prop").combine_chunks() == pyarrow.array(range(graph.num_nodes()))


def test_upsert_node_property(graph):
    prop = graph.loaded_node_schema().names[0]
    t = pyarrow.table({prop: range(graph.num_nodes())})
    graph.upsert_node_property(t)
    assert len(graph.loaded_node_schema()) == 17
    assert graph.get_node_property(prop).combine_chunks() == pyarrow.array(range(graph.num_nodes()))


def test_get_edge_property(graph):
    prop1 = graph.get_edge_property("creationDate")
    assert not prop1[10].as_py()


def test_remove_edge_property(graph):
    graph.remove_edge_property("classYear")
    assert len(graph.loaded_edge_schema()) == 2
    assert graph.loaded_edge_schema()[0].name == "creationDate"


def test_add_edge_property_exception(graph):
    t = pyarrow.table(dict(new_prop=[1, 2]))
    with pytest.raises(ValueError):
        # Should raise because new property isn't long enough for the node set
        graph.add_edge_property(t)


def test_add_edge_property(graph):
    t = pyarrow.table(dict(new_prop=range(graph.num_edges())))
    graph.add_edge_property(t)
    assert len(graph.loaded_edge_schema()) == 4
    assert graph.get_edge_property("new_prop").combine_chunks() == pyarrow.array(range(graph.num_edges()))


def test_upsert_edge_property(graph):
    prop = graph.loaded_edge_schema().names[0]
    t = pyarrow.table({prop: range(graph.num_edges())})
    graph.upsert_edge_property(t)
    assert len(graph.loaded_edge_schema()) == 3
    assert graph.get_edge_property(prop).combine_chunks() == pyarrow.array(range(graph.num_edges()))


def test_upsert_edge_property_dict(graph):
    prop = graph.loaded_edge_schema().names[0]
    graph.upsert_edge_property({prop: range(graph.num_edges())})
    assert len(graph.loaded_edge_schema()) == 3
    assert graph.get_edge_property(prop).combine_chunks() == pyarrow.array(range(graph.num_edges()))


def test_load_invalid_path():
    with pytest.raises(ValueError):
        Graph("non-existent")


def test_load_directory():
    with pytest.raises(ValueError):
        Graph("/tmp")


def test_load_garbage_file():
    with NamedTemporaryFile(delete=True) as fi:
        fi.write(b"Test")
        fi.flush()
        with pytest.raises(ValueError):
            Graph(fi.name)


def test_simple_algorithm(graph):
    @do_all_operator()
    def func_operator(g, prop, out, nid):
        t = 0
        for eid in g.out_edge_ids(nid):
            nid2 = g.out_edge_dst(eid)
            if prop.is_valid(nid2):
                t += prop[nid2]
        out[nid] = t

    g = graph
    prop = g.get_node_property("length")
    out = np.empty((g.num_nodes(),), dtype=int)

    do_all(range(g.num_nodes()), func_operator(g, prop, out), "operator")

    g.add_node_property(pyarrow.table(dict(referenced_total_length=out)))

    oprop = g.get_node_property("referenced_total_length")

    assert oprop[0].as_py() == 0
    assert oprop[4].as_py() == 0
    assert oprop[-1].as_py() == 0


def test_types(graph):
    node_type_set = set()
    edge_type_set = set()
    for nid in range(graph.num_nodes()):
        node_type_set.add(graph.get_node_type(nid))
    for eid in range(graph.num_edges()):
        edge_type_set.add(graph.get_edge_type(eid))

    assert len(node_type_set) == 11
    assert len(edge_type_set) == 15

    node_entity_type = graph.node_types.type_from_id(17)
    edge_entity_type = graph.edge_types.type_from_id(8)
    assert graph.does_node_have_type(0, node_entity_type)
    assert graph.does_edge_have_type(0, edge_entity_type)

    node_atomic_types = graph.node_types.atomic_types
    node_name_to_id_map = {name: node_atomic_types[name].id for name in node_atomic_types}
    assert node_name_to_id_map == {
        "Tag": 12,
        "Organisation": 8,
        "City": 1,
        "Comment": 2,
        "University": 14,
        "Forum": 6,
        "Company": 3,
        "Continent": 4,
        "Country": 5,
        "Place": 10,
        "TagClass": 13,
        "Person": 9,
        "Message": 7,
        "Post": 11,
    }
    assert graph.node_types.is_subtype_of(0, 1) is True
    non_atomic_type = graph.node_types.get_or_add_non_atomic_entity_type(
        [node_atomic_types["Message"], node_atomic_types["Post"]]
    )
    assert (
        graph.node_types.get_non_atomic_entity_type([node_atomic_types["Message"], node_atomic_types["Post"]])
        == non_atomic_type
    )
    assert graph.node_types.get_atomic_subtypes(non_atomic_type) == {
        node_atomic_types["Message"],
        node_atomic_types["Post"],
    }
    assert graph.node_types.get_supertypes(node_atomic_types["Message"]).issuperset(
        {node_atomic_types["Message"], non_atomic_type}
    )

    edge_atomic_types = graph.edge_types.atomic_types
    edge_name_to_id_map = {name: edge_atomic_types[name].id for name in edge_atomic_types}
    assert edge_name_to_id_map == {
        "KNOWS": 11,
        "IS_SUBCLASS_OF": 10,
        "REPLY_OF": 13,
        "LIKES": 12,
        "CONTAINER_OF": 1,
        "WORK_AT": 15,
        "HAS_TYPE": 7,
        "HAS_CREATOR": 2,
        "STUDY_AT": 14,
        "IS_LOCATED_IN": 8,
        "HAS_INTEREST": 3,
        "HAS_TAG": 6,
        "HAS_MEMBER": 4,
        "HAS_MODERATOR": 5,
        "IS_PART_OF": 9,
    }
    assert graph.edge_types.is_subtype_of(0, 1) is True


def test_projected(graph):
    projected_graph = graph.project([])
    assert projected_graph.num_nodes() == 0
    assert projected_graph.num_edges() == 0
    projected_graph = graph.project([graph.node_types.atomic_types["Person"]])
    assert projected_graph.num_nodes() == 45
    assert projected_graph.num_edges() == 58
    projected_graph = graph.project([graph.node_types.atomic_types["Person"]], edge_types=[])
    assert projected_graph.num_nodes() == 45
    assert projected_graph.num_edges() == 0
    projected_graph = graph.project(
        [graph.node_types.atomic_types["Message"]], [graph.edge_types.atomic_types["REPLY_OF"]]
    )
    assert projected_graph.num_nodes() == 3928
    assert projected_graph.num_edges() == 371
    projected_graph = graph.project(edge_types=[graph.edge_types.atomic_types["REPLY_OF"]])
    assert projected_graph.num_nodes() == 29946
    assert projected_graph.num_edges() == 371
