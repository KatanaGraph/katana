import os
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory

import numpy as np
import pandas
import pyarrow
import pytest
from pandas import isnull

from katana import TsubaError, do_all, do_all_operator
from katana.local import Graph


def test_load(graph):
    assert graph.num_nodes() == 29946
    assert graph.num_edges() == 43072
    assert len(graph.loaded_node_schema()) == 31
    assert len(graph.loaded_edge_schema()) == 18


def test_write(graph):
    with TemporaryDirectory() as tmpdir:
        graph.write(tmpdir)
        old_graph = graph
        del graph
        graph = Graph(tmpdir)
    assert graph.num_nodes() == 29946
    assert graph.num_edges() == 43072
    assert len(graph.loaded_node_schema()) == 31
    assert len(graph.loaded_edge_schema()) == 18

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
    assert len(graph.loaded_node_schema()) == 31
    assert len(graph.loaded_edge_schema()) == 19


def test_get_edge_dest(graph):
    assert graph.get_edge_dest(0) == 8014
    assert graph.get_edge_dest(1) == 8014


def test_reachable_from_10(graph):
    reachable = []
    for eid in graph.edges(10):
        reachable.append(graph.get_edge_dest(eid))
    assert reachable == [8015]


def test_nodes_count_edges(graph):
    total = 0
    for nid in graph:
        total += len(graph.edges(nid))
    assert graph.num_edges() == total


def test_get_node_property_exception(graph):
    with pytest.raises(KeyError):
        graph.get_node_property("_mispelled")


def test_get_node_property_index_exception(graph):
    with pytest.raises(IndexError):
        graph.get_node_property(100)


def test_get_node_property(graph):
    prop1 = graph.get_node_property(3)
    assert isnull(prop1[10])
    # TODO re-enable this
    # prop2 = graph.get_node_property("length")
    # assert prop1 == prop2


def test_get_node_property_chunked(graph):
    prop1 = graph.get_node_property(4)
    assert isinstance(prop1, pandas.Series)
    prop2 = graph.get_node_property_chunked(4)
    assert isinstance(prop2, pyarrow.ChunkedArray)


def test_remove_node_property(graph):
    graph.remove_node_property(10)
    assert len(graph.loaded_node_schema()) == 30
    graph.remove_node_property("length")
    assert len(graph.loaded_node_schema()) == 29
    assert graph.loaded_node_schema()[4].name == "email"


def test_add_node_property_exception(graph):
    t = pyarrow.table(dict(new_prop=[1, 2]))
    with pytest.raises(RuntimeError):
        # Should raise because new property isn't long enough for the node set
        graph.add_node_property(t)


def test_add_node_property(graph):
    t = pyarrow.table(dict(new_prop=range(graph.num_nodes())))
    graph.add_node_property(t)
    assert (graph.get_node_property("new_prop") == pandas.Series(range(graph.num_nodes()))).all()


def test_add_node_property_kwarg(graph):
    graph.add_node_property(new_prop=range(graph.num_nodes()))
    assert (graph.get_node_property("new_prop") == pandas.Series(range(graph.num_nodes()))).all()


def test_add_node_property_dataframe(graph):
    graph.add_node_property(pandas.DataFrame(dict(new_prop=range(graph.num_nodes()))))
    assert (graph.get_node_property("new_prop") == pandas.Series(range(graph.num_nodes()))).all()


def test_upsert_node_property(graph):
    prop = graph.loaded_node_schema().names[0]
    t = pyarrow.table({prop: range(graph.num_nodes())})
    graph.upsert_node_property(t)
    assert len(graph.loaded_node_schema()) == 31
    assert graph.get_node_property_chunked(prop) == pyarrow.chunked_array([range(graph.num_nodes())])
    assert (graph.get_node_property(prop) == pandas.Series(range(graph.num_nodes()))).all()


def test_get_edge_property(graph):
    prop1 = graph.get_edge_property(15)
    assert isnull(prop1[10])
    # TODO re-enable this test
    # prop2 = graph.get_edge_property("IS_SUBCLASS_OF")
    # assert prop1 == prop2


def test_get_edge_property_chunked(graph):
    prop1 = graph.get_edge_property(5)
    assert isinstance(prop1, pandas.Series)
    prop2 = graph.get_edge_property_chunked(5)
    assert isinstance(prop2, pyarrow.ChunkedArray)


def test_remove_edge_property(graph):
    graph.remove_edge_property(7)
    assert len(graph.loaded_edge_schema()) == 17
    graph.remove_edge_property("classYear")
    assert len(graph.loaded_edge_schema()) == 16
    assert graph.loaded_edge_schema()[3].name == "HAS_CREATOR"


def test_add_edge_property_exception(graph):
    t = pyarrow.table(dict(new_prop=[1, 2]))
    with pytest.raises(RuntimeError):
        # Should raise because new property isn't long enough for the node set
        graph.add_edge_property(t)


def test_add_edge_property(graph):
    t = pyarrow.table(dict(new_prop=range(graph.num_edges())))
    graph.add_edge_property(t)
    assert len(graph.loaded_edge_schema()) == 19
    assert (graph.get_edge_property("new_prop") == pandas.Series(range(graph.num_edges()))).all()


def test_upsert_edge_property(graph):
    prop = graph.loaded_edge_schema().names[0]
    t = pyarrow.table({prop: range(graph.num_edges())})
    graph.upsert_edge_property(t)
    assert len(graph.loaded_edge_schema()) == 18
    assert (graph.get_edge_property(prop) == pandas.Series(range(graph.num_edges()))).all()


def test_upsert_edge_property_dict(graph):
    prop = graph.loaded_edge_schema().names[0]
    graph.upsert_edge_property({prop: range(graph.num_edges())})
    assert len(graph.loaded_edge_schema()) == 18
    assert (graph.get_edge_property(prop) == pandas.Series(range(graph.num_edges()))).all()


def test_from_csr():
    pg = Graph.from_csr(np.array([1, 1], dtype=np.uint32), np.array([1], dtype=np.uint64))
    assert pg.num_nodes() == 2
    assert pg.num_edges() == 1
    assert list(pg.edges(0)) == [0]
    assert pg.get_edge_dest(0) == 1


def test_from_csr_int16():
    pg = Graph.from_csr(np.array([1, 1], dtype=np.int16), np.array([1], dtype=np.int16))
    assert pg.num_nodes() == 2
    assert pg.num_edges() == 1
    assert list(pg.edges(0)) == [0]
    assert pg.get_edge_dest(0) == 1


def test_from_csr_k3():
    pg = Graph.from_csr(np.array([2, 4, 6]), np.array([1, 2, 0, 2, 0, 1]))
    assert pg.num_nodes() == 3
    assert pg.num_edges() == 6
    assert list(pg.edges(2)) == [4, 5]
    assert pg.get_edge_dest(4) == 0
    assert pg.get_edge_dest(5) == 1


@pytest.mark.required_env("KATANA_SOURCE_DIR")
def test_load_graphml():
    input_file = Path(os.environ["KATANA_SOURCE_DIR"]) / "tools" / "graph-convert" / "test-inputs" / "movies.graphml"
    pg = Graph.from_graphml(input_file)
    assert pg.get_node_property(0)[1] == "Keanu Reeves"


@pytest.mark.required_env("KATANA_SOURCE_DIR")
def test_load_graphml_write():
    input_file = Path(os.environ["KATANA_SOURCE_DIR"]) / "tools" / "graph-convert" / "test-inputs" / "movies.graphml"
    pg = Graph.from_graphml(input_file)
    with TemporaryDirectory() as tmpdir:
        pg.write(tmpdir)
        del pg
        graph = Graph(tmpdir)
        assert graph.path == f"file://{tmpdir}"
    assert graph.get_node_property(0)[1] == "Keanu Reeves"


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


def test_simple_algorithm(graph: Graph):
    @do_all_operator()
    def func_operator(g, prop, out, nid):
        t = 0
        for eid in g.edges(nid):
            nid2 = g.get_edge_dest(eid)
            if prop.is_valid(nid2):
                t += prop[nid2]
        out[nid] = t

    g = graph
    # TODO(amp): Must is a pyarrow array here because we support it in numba. Pandas support in numba will be required.
    prop = g.get_node_property_chunked("length").chunk(0)
    out = np.empty((g.num_nodes(),), dtype=int)

    do_all(g, func_operator(g, prop, out), "operator")

    g.add_node_property(pyarrow.table(dict(referenced_total_length=out)))

    oprop = g.get_node_property("referenced_total_length").to_numpy()

    assert oprop[0] == 0
    assert oprop[4] == 0
    assert oprop[-1] == 0
