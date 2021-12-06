from tempfile import NamedTemporaryFile, TemporaryDirectory

import numpy as np
import pandas
import pyarrow
import pytest

from katana import TsubaError, do_all, do_all_operator
from katana.local import Graph
from katana.local.import_data import from_csr


def test_load(graph):
    assert graph.num_nodes() == 29946
    assert graph.num_edges() == 43072
    assert len(graph.loaded_node_schema()) == 17
    assert len(graph.loaded_edge_schema()) == 3


def test_write(graph):
    with TemporaryDirectory() as tmpdir:
        graph.write(tmpdir)
        old_graph = graph
        del graph
        graph = Graph(tmpdir)
    assert graph.num_nodes() == 29946
    assert graph.num_edges() == 43072
    assert len(graph.loaded_node_schema()) == 17
    assert len(graph.loaded_edge_schema()) == 3

    assert graph == old_graph


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


def test_get_edge_dest(graph):
    assert graph.get_edge_dest(0) == 8014
    assert graph.get_edge_dest(1) == 8014


def test_edge_data_frame(graph):
    edges = graph.edges()
    assert len(edges.columns) == 6
    assert len(edges) == 43072
    assert edges.at[0, "dest"] == 8014
    assert edges["dest"][1] == 8014
    assert edges["source"][0] == 0
    assert edges["source"][1] == 1
    assert edges["source"][19993] == 20000


def test_edge_data_frame_1_node(graph):
    edges = graph.edges(0)
    assert len(edges.columns) == 6
    assert len(edges) == 1
    assert edges.at[0, "dest"] == 8014
    assert edges["dest"][0] == 8014
    assert edges["source"][0] == 0


def test_edge_data_frame_selected_nodes(graph):
    edges = graph.edges([20000, 5000], properties={"classYear"})
    assert len(edges.columns) == 1 + 3
    assert len(edges) == len(graph.edge_ids(20000)) + len(graph.edge_ids(5000))
    assert edges.at[0, "id"] == 19993
    assert edges.at[1, "id"] == 5000
    assert edges.at[0, "dest"] == 9475
    assert edges.at[1, "dest"] == 9167
    assert edges["dest"][0] == 9475
    assert edges["dest"][1] == 9167
    assert edges["source"][0] == 20000
    assert edges["source"][1] == 5000


def test_edge_data_frame_simple_slice(graph):
    edges = graph.edges(slice(110, 112))
    assert len(edges) == 2
    assert edges.at[0, "dest"] == 8019
    assert edges.at[1, "dest"] == 8019
    assert edges["dest"][0] == 8019
    assert edges["dest"][1] == 8019
    assert edges["source"][0] == 110
    assert edges["source"][1] == 111


def test_edge_data_frame_complex_slice(graph):
    edges = graph.edges(slice(1010, 1020, 2))
    assert len(edges) == 5
    assert edges.at[0, "dest"] == 7993
    assert edges.at[1, "dest"] == 7993
    assert edges["dest"][0] == 7993
    assert edges["dest"][1] == 7993
    assert edges["source"][0] == 1010
    assert edges["source"][1] == 1012


def test_edge_data_frame_to_pandas(graph):
    edges = graph.edges().to_pandas()
    assert len(edges.columns) == 6
    assert len(edges) == 43072
    assert edges.at[0, "dest"] == 8014
    assert edges["dest"][1] == 8014
    assert edges["source"][0] == 0


def test_reachable_from_10(graph):
    reachable = []
    for eid in graph.edge_ids(10):
        reachable.append(graph.get_edge_dest(eid))
    assert reachable == [8015]


def test_nodes_count_edges(graph):
    total = 0
    for nid in graph:
        total += len(graph.edge_ids(nid))
    assert graph.num_edges() == total


def test_get_node_property_exception(graph):
    with pytest.raises(KeyError):
        graph.get_node_property("_mispelled")


def test_get_node_property_index_exception(graph):
    with pytest.raises(IndexError):
        graph.get_node_property(100)


def test_get_node_property(graph):
    prop1 = graph.get_node_property(3)
    assert prop1[10].as_py() is None
    # TODO re-enable this
    # prop2 = graph.get_node_property("length")
    # assert prop1 == prop2


def test_get_node_property_chunked(graph):
    prop1 = graph.get_node_property(4)
    assert isinstance(prop1, pyarrow.Array)
    prop2 = graph.get_node_property_chunked(4)
    assert isinstance(prop2, pyarrow.ChunkedArray)
    assert prop1 == prop2.chunk(0)


def test_remove_node_property(graph):
    graph.remove_node_property(10)
    assert len(graph.loaded_node_schema()) == 16
    graph.remove_node_property("length")
    assert len(graph.loaded_node_schema()) == 15
    assert graph.loaded_node_schema()[4].name == "email"


def test_add_node_property_exception(graph):
    t = pyarrow.table(dict(new_prop=[1, 2]))
    with pytest.raises(RuntimeError):
        # Should raise because new property isn't long enough for the node set
        graph.add_node_property(t)


def test_add_node_property(graph):
    t = pyarrow.table(dict(new_prop=range(graph.num_nodes())))
    graph.add_node_property(t)
    assert graph.get_node_property("new_prop") == pyarrow.array(range(graph.num_nodes()))


def test_add_node_property_kwarg(graph):
    graph.add_node_property(new_prop=range(graph.num_nodes()))
    assert graph.get_node_property("new_prop") == pyarrow.array(range(graph.num_nodes()))


def test_add_node_property_dataframe(graph):
    graph.add_node_property(pandas.DataFrame(dict(new_prop=range(graph.num_nodes()))))
    assert graph.get_node_property("new_prop") == pyarrow.array(range(graph.num_nodes()))


def test_upsert_node_property(graph):
    prop = graph.loaded_node_schema().names[0]
    t = pyarrow.table({prop: range(graph.num_nodes())})
    graph.upsert_node_property(t)
    assert len(graph.loaded_node_schema()) == 17
    assert graph.get_node_property_chunked(prop) == pyarrow.chunked_array([range(graph.num_nodes())])
    assert graph.get_node_property(prop) == pyarrow.array(range(graph.num_nodes()))


def test_get_edge_property(graph):
    prop1 = graph.get_edge_property(2)
    assert not prop1[10].as_py()
    # TODO re-enable this test
    # prop2 = graph.get_edge_property("creationDate")
    # assert prop1 == prop2


def test_get_edge_property_chunked(graph):
    prop1 = graph.get_edge_property(2)
    assert isinstance(prop1, pyarrow.Array)
    prop2 = graph.get_edge_property_chunked(2)
    assert isinstance(prop2, pyarrow.ChunkedArray)
    assert prop1 == prop2.chunk(0)


def test_remove_edge_property(graph):
    graph.remove_edge_property(2)
    assert len(graph.loaded_edge_schema()) == 2
    graph.remove_edge_property("classYear")
    assert len(graph.loaded_edge_schema()) == 1
    assert graph.loaded_edge_schema()[0].name == "creationDate"


def test_add_edge_property_exception(graph):
    t = pyarrow.table(dict(new_prop=[1, 2]))
    with pytest.raises(RuntimeError):
        # Should raise because new property isn't long enough for the node set
        graph.add_edge_property(t)


def test_add_edge_property(graph):
    t = pyarrow.table(dict(new_prop=range(graph.num_edges())))
    graph.add_edge_property(t)
    assert len(graph.loaded_edge_schema()) == 4
    assert graph.get_edge_property("new_prop") == pyarrow.array(range(graph.num_edges()))


def test_upsert_edge_property(graph):
    prop = graph.loaded_edge_schema().names[0]
    t = pyarrow.table({prop: range(graph.num_edges())})
    graph.upsert_edge_property(t)
    assert len(graph.loaded_edge_schema()) == 3
    assert graph.get_edge_property(prop) == pyarrow.array(range(graph.num_edges()))


def test_upsert_edge_property_dict(graph):
    prop = graph.loaded_edge_schema().names[0]
    graph.upsert_edge_property({prop: range(graph.num_edges())})
    assert len(graph.loaded_edge_schema()) == 3
    assert graph.get_edge_property(prop) == pyarrow.array(range(graph.num_edges()))


def test_from_csr():
    pg = from_csr(np.array([1, 1], dtype=np.uint32), np.array([1], dtype=np.uint64))
    assert pg.num_nodes() == 2
    assert pg.num_edges() == 1
    assert list(pg.edge_ids(0)) == [0]
    assert pg.get_edge_dest(0) == 1


def test_from_csr_int16():
    pg = from_csr(np.array([1, 1], dtype=np.int16), np.array([1], dtype=np.int16))
    assert pg.num_nodes() == 2
    assert pg.num_edges() == 1
    assert list(pg.edge_ids(0)) == [0]
    assert pg.get_edge_dest(0) == 1


def test_from_csr_k3():
    pg = from_csr(np.array([2, 4, 6]), np.array([1, 2, 0, 2, 0, 1]))
    assert pg.num_nodes() == 3
    assert pg.num_edges() == 6
    assert list(pg.edge_ids(2)) == [4, 5]
    assert pg.get_edge_dest(4) == 0
    assert pg.get_edge_dest(5) == 1


def test_load_invalid_path():
    with pytest.raises(TsubaError):
        Graph("non-existent")


def test_load_directory():
    with pytest.raises(TsubaError):
        Graph("/tmp")


def test_load_garbage_file():
    with NamedTemporaryFile(delete=True) as fi:
        fi.write(b"Test")
        fi.flush()
        with pytest.raises(TsubaError):
            Graph(fi.name)


def test_simple_algorithm(graph):
    @do_all_operator()
    def func_operator(g, prop, out, nid):
        t = 0
        for eid in g.edge_ids(nid):
            nid2 = g.get_edge_dest(eid)
            if prop.is_valid(nid2):
                t += prop[nid2]
        out[nid] = t

    g = graph
    prop = g.get_node_property("length")
    out = np.empty((g.num_nodes(),), dtype=int)

    do_all(g, func_operator(g, prop, out), "operator")

    g.add_node_property(pyarrow.table(dict(referenced_total_length=out)))

    oprop = g.get_node_property("referenced_total_length")

    assert oprop[0].as_py() == 0
    assert oprop[4].as_py() == 0
    assert oprop[-1].as_py() == 0


def test_types(graph):
    node_type_set = set()
    edge_type_set = set()
    for nid in range(0, graph.num_nodes()):
        node_type_set.add(graph.get_type_of_node(nid))
        for eid in graph.edge_ids(nid):
            edge_type_set.add(graph.get_type_of_edge(eid))

    assert len(node_type_set) == 11
    assert len(edge_type_set) == 15

    assert graph.does_node_have_type(0, 17)
    assert graph.does_edge_have_type(0, 8)

    node_type_manager = graph.get_node_type_manager()
    assert node_type_manager.get_num_atomic_types() == 14
    assert node_type_manager.get_num_entity_types() == 22
    assert node_type_manager.has_atomic_type("Person") is True
    assert node_type_manager.list_atomic_types() == [
        b"Tag",
        b"Organisation",
        b"City",
        b"Comment",
        b"University",
        b"Forum",
        b"Company",
        b"Continent",
        b"Country",
        b"Place",
        b"TagClass",
        b"Person",
        b"Message",
        b"Post",
    ]
    assert node_type_manager.has_entity_type(0) is True
    assert node_type_manager.get_entity_type_id("Person") == 9
    assert node_type_manager.get_atomic_type_name(9) == b"Person"
    assert node_type_manager.get_atomic_entity_type_ids() == [12, 8, 1, 2, 14, 6, 3, 4, 5, 10, 13, 9, 7, 11]
    assert node_type_manager.is_subtype_of(0, 1) is True
    assert node_type_manager.get_entity_type_id_to_atomic_type_name_map() == {
        13: b"TagClass",
        14: b"University",
        1: b"City",
        2: b"Comment",
        3: b"Company",
        4: b"Continent",
        5: b"Country",
        6: b"Forum",
        7: b"Message",
        8: b"Organisation",
        9: b"Person",
        10: b"Place",
        11: b"Post",
        12: b"Tag",
    }
    assert node_type_manager.set_of_entity_type_ids_size() == 256

    edge_type_manager = graph.get_edge_type_manager()
    assert edge_type_manager.get_num_atomic_types() == 15
    assert edge_type_manager.get_num_entity_types() == 16
    assert edge_type_manager.has_atomic_type("KNOWS") is True
    assert edge_type_manager.list_atomic_types() == [
        b"KNOWS",
        b"IS_SUBCLASS_OF",
        b"REPLY_OF",
        b"LIKES",
        b"CONTAINER_OF",
        b"WORK_AT",
        b"HAS_TYPE",
        b"HAS_CREATOR",
        b"STUDY_AT",
        b"IS_LOCATED_IN",
        b"HAS_INTEREST",
        b"HAS_TAG",
        b"HAS_MEMBER",
        b"HAS_MODERATOR",
        b"IS_PART_OF",
    ]
    assert edge_type_manager.has_entity_type(0) is True
    assert edge_type_manager.get_entity_type_id("REPLY_OF") == 13
    assert edge_type_manager.get_atomic_type_name(9) == b"IS_PART_OF"
    assert edge_type_manager.get_atomic_entity_type_ids() == [11, 10, 13, 12, 1, 15, 7, 2, 14, 8, 3, 6, 4, 5, 9]
    assert edge_type_manager.is_subtype_of(0, 1) is True
    assert edge_type_manager.get_entity_type_id_to_atomic_type_name_map() == {
        13: b"REPLY_OF",
        12: b"LIKES",
        15: b"WORK_AT",
        2: b"HAS_CREATOR",
        14: b"STUDY_AT",
        1: b"CONTAINER_OF",
        3: b"HAS_INTEREST",
        4: b"HAS_MEMBER",
        5: b"HAS_MODERATOR",
        6: b"HAS_TAG",
        7: b"HAS_TYPE",
        8: b"IS_LOCATED_IN",
        9: b"IS_PART_OF",
        10: b"IS_SUBCLASS_OF",
        11: b"KNOWS",
    }
    assert edge_type_manager.set_of_entity_type_ids_size() == 256
