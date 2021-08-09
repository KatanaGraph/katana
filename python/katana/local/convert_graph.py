from typing import Collection, Dict, Optional, Union

import numba
import numpy as np

import katana.local
from katana.native_interfacing.buffer_access import to_numpy


def from_adjacency_matrix(adjacency: np.ndarray, property_name: str = "weight") -> katana.local.Graph:
    """
    Convert an adjacency matrix with shape (n_edges, n_edges) into a :py:class:`~katana.local.Graph` with the
    non-zero values as an edge weight property.

    This preserves node IDs, but not edge IDs.
    """
    sources, destinations = adjacency.nonzero()
    weights = adjacency[sources, destinations]

    return from_edge_list_arrays(sources, destinations, {property_name: weights})


def from_edge_list_matrix(edges: np.ndarray) -> katana.local.Graph:
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
    for source_index, edge_index in enumerate(sort_order):
        source = sources[source_index]
        if source != last_source:
            for j in range(last_source, source):
                indices[j] = edge_index
            last_source = source
    for j in range(last_source, len(indices)):
        indices[j] = len(sources)


def from_edge_list_arrays(
    sources: np.ndarray, destinations: np.ndarray, property_dict: Dict[str, np.ndarray] = None, **properties: np.ndarray
) -> katana.local.Graph:
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

    sort_order = sources.argsort()

    csr_destinations = destinations[sort_order]
    csr_indices = np.empty(n_nodes, dtype=np.uint64)
    _fill_indices(sort_order, sources, csr_indices)

    graph = katana.local.Graph.from_csr(csr_indices, csr_destinations)
    if properties:
        graph.add_edge_property(properties)
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
