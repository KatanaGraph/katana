import numpy as np
import pandas
import pytest

from katana.local.convert_graph import (
    from_adjacency_matrix,
    from_edge_list_arrays,
    from_edge_list_dataframe,
    from_edge_list_matrix,
)


def test_adjacency_matrix():
    g = from_adjacency_matrix(np.array([[0, 1, 0], [0, 0, 2], [3, 0, 0]]))
    assert [g.edges(n) for n in g] == [range(0, 1), range(1, 2), range(2, 3)]
    assert [g.get_edge_dest(i) for i in range(g.num_edges())] == [1, 2, 0]
    assert list(g.get_edge_property("weight").to_numpy()) == [1, 2, 3]


def test_trivial_arrays():
    g = from_edge_list_arrays(np.array([0, 1, 10]), np.array([1, 2, 0]))
    assert [g.edges(n) for n in g] == [
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


def test_properties_arrays():
    g = from_edge_list_arrays(np.array([0, 1, 10]), np.array([1, 2, 0]), prop=np.array([1, 2, 3]))
    assert [g.edges(n) for n in g] == [
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


def test_trivial_matrix():
    g = from_edge_list_matrix(np.array([[0, 1], [1, 2], [10, 0]]))
    assert [g.edges(n) for n in g] == [
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
    assert [g.edges(n) for n in g] == [
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
