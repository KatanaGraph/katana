import os
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory

import numpy as np
import pyarrow
import pytest

from katana.loops import do_all_operator, do_all
from katana.property_graph import PropertyGraph
from katana import TsubaError


def test_load(property_graph):
    assert property_graph.num_nodes() == 29092
    assert property_graph.num_edges() == 39283
    assert len(property_graph.node_schema()) == 31
    assert len(property_graph.edge_schema()) == 19


def test_write(property_graph):
    # TODO(amp): mark_all_properties_persistent shouldn't be required. Why is it?
    property_graph.mark_all_properties_persistent()
    with TemporaryDirectory() as tmpdir:
        property_graph.write(tmpdir)
        old_property_graph = property_graph
        del property_graph
        property_graph = PropertyGraph(tmpdir)
    assert property_graph.num_nodes() == 29092
    assert property_graph.num_edges() == 39283
    assert len(property_graph.node_schema()) == 31
    assert len(property_graph.edge_schema()) == 19

    assert property_graph == old_property_graph


# TODO(amp): Reinstant this test once it matches the actual RDG semantics.
@pytest.mark.skip("Does not work. Underlying semantics may be wrong or different.")
def test_commit(property_graph):
    # TODO(amp): mark_all_properties_persistent shouldn't be required. Why is it?
    property_graph.mark_all_properties_persistent()
    with TemporaryDirectory() as tmpdir:
        property_graph.path = tmpdir
        property_graph.write()
        del property_graph
        property_graph = PropertyGraph(tmpdir)
    assert property_graph.num_nodes() == 29092
    assert property_graph.num_edges() == 39283
    assert len(property_graph.node_schema()) == 31
    assert len(property_graph.edge_schema()) == 19


def test_get_edge_dst(property_graph):
    assert property_graph.get_edge_dst(0) == 1967
    assert property_graph.get_edge_dst(1) == 1419


def test_reachable_from_10(property_graph):
    reachable = []
    for eid in property_graph.edges(10):
        reachable.append(property_graph.get_edge_dst(eid))
    assert reachable == [2011, 1422, 1409, 4798, 9483]


def test_nodes_count_edges(property_graph):
    total = 0
    for nid in property_graph:
        total += len(property_graph.edges(nid))
    assert property_graph.num_edges() == total


def test_get_node_property_exception(property_graph):
    with pytest.raises(KeyError):
        property_graph.get_node_property("_mispelled")


def test_get_node_property_index_exception(property_graph):
    with pytest.raises(IndexError):
        property_graph.get_node_property(100)


def test_get_node_property(property_graph):
    prop1 = property_graph.get_node_property(3)
    assert prop1[10].as_py() == 82
    prop2 = property_graph.get_node_property("length")
    assert prop1 == prop2


def test_get_node_property_chunked(property_graph):
    prop1 = property_graph.get_node_property(4)
    assert isinstance(prop1, pyarrow.Array)
    prop2 = property_graph.get_node_property_chunked(4)
    assert isinstance(prop2, pyarrow.ChunkedArray)
    assert prop1 == prop2.chunk(0)


def test_remove_node_property(property_graph):
    property_graph.remove_node_property(10)
    assert len(property_graph.node_schema()) == 30
    property_graph.remove_node_property("length")
    assert len(property_graph.node_schema()) == 29
    assert property_graph.node_schema()[4].name == "title"


def test_add_node_property_exception(property_graph):
    t = pyarrow.table(dict(new_prop=[1, 2]))
    with pytest.raises(RuntimeError):
        # Should raise because new property isn't long enough for the node set
        property_graph.add_node_property(t)


def test_add_node_property(property_graph):
    t = pyarrow.table(dict(new_prop=range(property_graph.num_nodes())))
    property_graph.add_node_property(t)
    assert len(property_graph.node_schema()) == 32
    assert property_graph.get_node_property_chunked("new_prop") == pyarrow.chunked_array(
        [range(property_graph.num_nodes())]
    )
    assert property_graph.get_node_property("new_prop") == pyarrow.array(range(property_graph.num_nodes()))


def test_upsert_node_property(property_graph):
    prop = property_graph.node_schema().names[0]
    t = pyarrow.table({prop: range(property_graph.num_nodes())})
    property_graph.upsert_node_property(t)
    assert len(property_graph.node_schema()) == 31
    assert property_graph.get_node_property_chunked(prop) == pyarrow.chunked_array([range(property_graph.num_nodes())])
    assert property_graph.get_node_property(prop) == pyarrow.array(range(property_graph.num_nodes()))


def test_get_edge_property(property_graph):
    prop1 = property_graph.get_edge_property(15)
    assert prop1[10].as_py() == False
    prop2 = property_graph.get_edge_property("IS_SUBCLASS_OF")
    assert prop1 == prop2


def test_get_edge_property_chunked(property_graph):
    prop1 = property_graph.get_edge_property(5)
    assert isinstance(prop1, pyarrow.Array)
    prop2 = property_graph.get_edge_property_chunked(5)
    assert isinstance(prop2, pyarrow.ChunkedArray)
    assert prop1 == prop2.chunk(0)


def test_remove_edge_property(property_graph):
    property_graph.remove_edge_property(7)
    assert len(property_graph.edge_schema()) == 18
    property_graph.remove_edge_property("classYear")
    assert len(property_graph.edge_schema()) == 17
    assert property_graph.edge_schema()[3].name == "REPLY_OF"


def test_add_edge_property_exception(property_graph):
    t = pyarrow.table(dict(new_prop=[1, 2]))
    with pytest.raises(RuntimeError):
        # Should raise because new property isn't long enough for the node set
        property_graph.add_edge_property(t)


def test_add_edge_property(property_graph):
    t = pyarrow.table(dict(new_prop=range(property_graph.num_edges())))
    property_graph.add_edge_property(t)
    assert len(property_graph.edge_schema()) == 20
    assert property_graph.get_edge_property_chunked("new_prop") == pyarrow.chunked_array(
        [range(property_graph.num_edges())]
    )
    assert property_graph.get_edge_property("new_prop") == pyarrow.array(range(property_graph.num_edges()))


def test_upsert_edge_property(property_graph):
    prop = property_graph.edge_schema().names[0]
    t = pyarrow.table({prop: range(property_graph.num_edges())})
    property_graph.upsert_edge_property(t)
    assert len(property_graph.edge_schema()) == 19
    assert property_graph.get_edge_property_chunked(prop) == pyarrow.chunked_array([range(property_graph.num_edges())])
    assert property_graph.get_edge_property(prop) == pyarrow.array(range(property_graph.num_edges()))


def test_load_graphml():
    input_file = Path(__file__).parent.parent.parent / "tools" / "graph-convert" / "test-inputs" / "movies.graphml"
    pg = PropertyGraph.from_graphml(input_file)
    assert pg.get_node_property(0)[1].as_py() == "Keanu Reeves"


def test_load_graphml_write():
    input_file = Path(__file__).parent.parent.parent / "tools" / "graph-convert" / "test-inputs" / "movies.graphml"
    pg = PropertyGraph.from_graphml(input_file)
    pg.mark_all_properties_persistent()
    with TemporaryDirectory() as tmpdir:
        pg.write(tmpdir)
        del pg
        property_graph = PropertyGraph(tmpdir)
        assert property_graph.path == f"file://{tmpdir}"
    assert property_graph.get_node_property(0)[1].as_py() == "Keanu Reeves"


def test_load_invalid_path():
    with pytest.raises(TsubaError):
        PropertyGraph("non-existent")


def test_load_directory():
    with pytest.raises(TsubaError):
        PropertyGraph("/tmp")


def test_load_garbage_file():
    fi = NamedTemporaryFile(delete=False)
    try:
        with fi:
            fi.write(b"Test")
        with pytest.raises(TsubaError):
            PropertyGraph(fi.name)
    finally:
        os.unlink(fi.name)


def test_simple_algorithm(property_graph):
    @do_all_operator()
    def func_operator(g, prop, out, nid):
        t = 0
        for eid in g.edges(nid):
            nid2 = g.get_edge_dst(eid)
            if prop.is_valid(nid2):
                t += prop[nid2]
        out[nid] = t

    g = property_graph
    prop = g.get_node_property("length")
    out = np.empty((g.num_nodes(),), dtype=int)

    do_all(g, func_operator(g, prop, out), "operator")

    g.add_node_property(pyarrow.table(dict(referenced_total_length=out)))

    oprop = g.get_node_property("referenced_total_length")

    assert oprop[0].as_py() == 91
    assert oprop[4].as_py() == 239
    assert oprop[-1].as_py() == 0
