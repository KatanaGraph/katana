import os
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pandas
import pytest

from katana.local import Graph
from katana.local.import_data import (
    from_adjacency_matrix,
    from_edge_list_arrays,
    from_edge_list_dataframe,
    from_edge_list_matrix,
    from_graphml,
    from_sorted_edge_list_arrays,
)


def test_adjacency_matrix():
    g = from_adjacency_matrix(np.array([[0, 1, 0], [0, 0, 2], [3, 0, 0]]))
    assert [g.edge_ids(n) for n in g] == [range(0, 1), range(1, 2), range(2, 3)]
    assert [g.get_edge_dest(i) for i in range(g.num_edges())] == [1, 2, 0]
    assert list(g.get_edge_property("weight").to_numpy()) == [1, 2, 3]


def test_trivial_arrays_unsorted():
    g = from_edge_list_arrays(np.array([0, 10, 1]), np.array([1, 0, 2]))
    assert [g.edge_ids(n) for n in g] == [
        range(0, 1),
        range(1, 2),
        range(2, 2),
        range(2, 2),
        range(2, 2),
        range(2, 2),
        range(2, 2),
        range(2, 2),
        range(2, 2),
        range(2, 2),
        range(2, 3),
    ]
    assert [g.get_edge_dest(i) for i in range(g.num_edges())] == [1, 2, 0]


def test_trivial_arrays_sorted():
    g = from_sorted_edge_list_arrays(np.array([0, 1, 1, 10]), np.array([1, 2, 1, 0]))
    assert [g.edge_ids(n) for n in g] == [
        range(0, 1),
        range(1, 3),
        range(3, 3),
        range(3, 3),
        range(3, 3),
        range(3, 3),
        range(3, 3),
        range(3, 3),
        range(3, 3),
        range(3, 3),
        range(3, 4),
    ]
    assert [g.get_edge_dest(i) for i in range(g.num_edges())] == [1, 2, 1, 0]


def test_properties_arrays_unsorted():
    g = from_edge_list_arrays(np.array([0, 1, 10, 1]), np.array([1, 2, 0, 2]), prop=np.array([1, 2, 3, 2]))
    assert list(g.get_edge_property("prop").to_numpy()) == [1, 2, 2, 3]


def test_properties_arrays_sorted():
    g = from_sorted_edge_list_arrays(np.array([0, 1, 1, 10]), np.array([1, 2, 1, 0]), prop=np.array([1, 2, 3, 4]))
    assert list(g.get_edge_property("prop").to_numpy()) == [1, 2, 3, 4]


def test_trivial_matrix():
    g = from_edge_list_matrix(np.array([[0, 1], [1, 2], [10, 0]]))
    assert [g.edge_ids(n) for n in g] == [
        range(0, 1),
        range(1, 2),
        range(2, 2),
        range(2, 2),
        range(2, 2),
        range(2, 2),
        range(2, 2),
        range(2, 2),
        range(2, 2),
        range(2, 2),
        range(2, 3),
    ]
    assert [g.get_edge_dest(i) for i in range(g.num_edges())] == [1, 2, 0]


def test_arrays_bad_arguments():
    with pytest.raises(TypeError):
        from_edge_list_arrays(np.array([[0, 0], [1, 0]]), np.array([1, 2, 0]))
    with pytest.raises(TypeError):
        from_edge_list_arrays(np.array([1, 2, 0]), np.array([[0, 0], [1, 0]]))
    with pytest.raises(ValueError):
        from_edge_list_arrays(np.array([1, 2, 0]), np.array([0, 0, 1, 0]))
    with pytest.raises(ValueError):
        from_edge_list_arrays(np.array([]), np.array([]))


def test_matrix_bad_arguments():
    with pytest.raises(TypeError):
        from_edge_list_matrix(np.array([1, 2, 0]))
    with pytest.raises(TypeError):
        from_edge_list_matrix(np.array([[0, 0, 1], [1, 0, 3]]))


def test_dataframe():
    g = from_edge_list_dataframe(pandas.DataFrame(dict(source=[0, 1, 10], destination=[1, 2, 0], prop=[1, 2, 3])))
    assert [g.edge_ids(n) for n in g] == [
        range(0, 1),
        range(1, 2),
        range(2, 2),
        range(2, 2),
        range(2, 2),
        range(2, 2),
        range(2, 2),
        range(2, 2),
        range(2, 2),
        range(2, 2),
        range(2, 3),
    ]
    assert [g.get_edge_dest(i) for i in range(g.num_edges())] == [1, 2, 0]
    assert list(g.get_edge_property("prop").to_numpy()) == [1, 2, 3]


@pytest.mark.required_env("KATANA_SOURCE_DIR")
def test_load_graphml():
    input_file = Path(os.environ["KATANA_SOURCE_DIR"]) / "tools" / "graph-convert" / "test-inputs" / "movies.graphml"
    pg = from_graphml(input_file)
    assert pg.get_node_property(0)[1].as_py() == "Keanu Reeves"


@pytest.mark.required_env("KATANA_SOURCE_DIR")
def test_load_graphml_write():
    input_file = Path(os.environ["KATANA_SOURCE_DIR"]) / "tools" / "graph-convert" / "test-inputs" / "movies.graphml"
    pg = from_graphml(input_file)
    with TemporaryDirectory() as tmpdir:
        pg.write(tmpdir)
        del pg
        graph = Graph(tmpdir)
        assert graph.path == f"file://{tmpdir}"
    assert graph.get_node_property(0)[1].as_py() == "Keanu Reeves"
