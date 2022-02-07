"""
The :py:mod:`~katana.local.import_data` module provides a several graph conversion routines which take external data
and convert it into a Katana :py:class:`~katana.local.Graph` object.
"""
import pathlib
from typing import Collection, Dict, Optional, Union

import numba
import numpy as np

from katana.local_native import Graph, TxnContext, from_csr, from_graphml_native
from katana.native_interfacing.buffer_access import to_numpy

__all__ = [
    "from_graphml",
    "from_csr",
    "from_adjacency_matrix",
    "from_edge_list_matrix",
    "from_edge_list_arrays",
    "from_edge_list_dataframe",
]


def from_adjacency_matrix(adjacency: np.ndarray, property_name: str = "weight") -> Graph:
    """
    Convert an adjacency matrix with shape (n_nodes, n_nodes) into a :py:class:`~katana.local.Graph` with the
    non-zero values as an edge weight property.

    This preserves node IDs, but not edge IDs.
    """
    sources, destinations = adjacency.nonzero()
    weights = adjacency[sources, destinations]

    return from_edge_list_arrays(sources, destinations, {property_name: weights})


def from_edge_list_matrix(edges: np.ndarray) -> Graph:
    """
    Convert an edge list with shape (n_edges, 2) into a :py:class:`~katana.local.Graph`.

    This preserves node IDs, but not edge IDs.
    """
    if len(edges.shape) != 2 or edges.shape[1] != 2:
        raise TypeError("edges must have shape (n_edges, 2).")

    return from_edge_list_arrays(edges[:, 0], edges[:, 1])


@numba.njit()
def _fill_indices(sort_order: np.ndarray, sources: np.ndarray, indices: np.ndarray):
    last_source = 0
    # Code is duplicated because numba cannot perform type based cloning within a function and cannot pass ranges
    # into a function.
    if sort_order is None:
        for edge_index, source in enumerate(sources):
            if source != last_source:
                for j in range(last_source, source):
                    indices[j] = edge_index
                last_source = source
    else:
        for edge_index, source_index in enumerate(sort_order):
            source = sources[source_index]
            if source != last_source:
                for j in range(last_source, source):
                    indices[j] = edge_index
                last_source = source
    for j in range(last_source, len(indices)):
        indices[j] = len(sources)


def from_edge_list_arrays(
    sources: np.ndarray, destinations: np.ndarray, property_dict: Dict[str, np.ndarray] = None, **properties: np.ndarray
) -> Graph:
    """
    Convert an edge list represented as two parallel arrays into a :py:class:`~katana.local.Graph`.

    This preserves node IDs, but **not** edge IDs.
    """
    return _from_edge_list_arrays_impl(sources, destinations, property_dict, edges_sorted=False, properties=properties)


def from_sorted_edge_list_arrays(
    sources: np.ndarray, destinations: np.ndarray, property_dict: Dict[str, np.ndarray] = None, **properties: np.ndarray
) -> Graph:
    """
    Convert an **sorted** edge list represented as two parallel arrays into a :py:class:`~katana.local.Graph`. The
    ``sources`` array must be sorted or the resulting graph will be incorrect.

    This preserves node IDs and edge IDs.
    """
    return _from_edge_list_arrays_impl(sources, destinations, property_dict, edges_sorted=True, properties=properties)


def _from_edge_list_arrays_impl(
    sources: np.ndarray, destinations: np.ndarray, property_dict: Dict[str, np.ndarray], edges_sorted: bool, properties,
) -> Graph:
    """
    Convert an edge list represented as two parallel arrays into a :py:class:`~katana.local.Graph`.

    This preserves node IDs, but not edge IDs.
    """
    sources = to_numpy(sources)
    destinations = to_numpy(destinations)

    if len(sources.shape) != 1:
        raise TypeError("sources must be a 1-d array")
    if len(destinations.shape) != 1:
        raise TypeError("destinations must be a 1-d array")

    n_edges = len(sources)

    if not n_edges:
        raise ValueError("Must have at least one edge")

    if len(destinations) != n_edges:
        raise ValueError("Sources and destinations must have the same length")

    if property_dict is not None:
        properties.update(property_dict)

    for name, prop in properties.items():
        if len(prop) != n_edges:
            raise ValueError(f"{name} does not have length equal to sources.")

    n_nodes = max(np.max(sources), np.max(destinations)) + 1

    if not edges_sorted:
        sort_order = sources.argsort()
        csr_destinations = destinations[sort_order]
    else:
        sort_order = None
        csr_destinations = destinations

    csr_indices = np.empty(n_nodes, dtype=np.uint64)
    _fill_indices(sort_order, sources, csr_indices)

    graph = from_csr(csr_indices, csr_destinations)
    if properties:
        graph.add_edge_property(properties if edges_sorted else {n: p[sort_order] for n, p in properties.items()})
    return graph


def from_edge_list_dataframe(
    df,
    source_column: Union[str, int] = "source",
    destination_column: Union[str, int] = "destination",
    property_columns: Optional[Collection[Union[str, int]]] = None,
):
    """
    Convert an edge list in the form of a dataframe-like object into a :py:class:`~katana.local.Graph` with edge
    properties.

    :param df: The edge list and edge properties.
    :type df: A data frame like type, such as a `pandas.DataFrame` or even a dict of numpy arrays.
    :param source_column: The name or ID of the column which holds edge sources.
    :param destination_column: The name or ID of the column which holds edge destinations.
    :param property_columns: The names or IDs of columns which should be used as edge properties. If this is ``None``,
        use all columns other than sources and destinations specified above.
    """
    if property_columns is None:
        property_columns = set(df) - {source_column, destination_column}

    edge_properties = {col_name: df[col_name] for col_name in property_columns}
    return from_edge_list_arrays(df[source_column], df[destination_column], **edge_properties)


def from_graphml(path: Union[str, pathlib.Path], chunk_size: int = 25000, *, txn_ctx=None):
    """
    Load a GraphML file into Katana form.

    :param path: Path to source GraphML file.
    :type path: Union[str, Path]
    :param chunk_size: Chunk size for in memory representations during conversion. Generally this value can be ignored,
        but it can be decreased to reduce memory usage when converting large inputs.
    :param txn_ctx: The tranaction context for passing read write sets.
    :type chunk_size: int
    :returns: the new :py:class:`~katana.local.Graph`
    """
    return from_graphml_native(str(path), chunk_size, txn_ctx or TxnContext())
