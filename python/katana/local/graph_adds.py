from typing import Optional, Sequence, Union

import numpy

import katana.local._graph_numba
from katana.dataframe import DataFrame, LazyDataFrame
from katana.local_native import Graph, GraphBaseEdgeDestAccessor, GraphBaseEdgeSourceAccessor


def out_edges(
    self: Graph,
    nodes: Union[int, slice, Sequence[int], None] = None,
    *,
    properties: Optional[Sequence[Union[int, str]]] = None,
    may_copy=True
) -> DataFrame:
    """
    :param nodes: A node ID or set of node IDs to select which nodes' edges should be returned. The default, None,
        returns all edges for all nodes. Contiguous and compact slices over the nodes are *much* more
        efficient than other kinds of slice.
    :param properties: A list of properties to include in the dataframe. This is useful if this operation is
        expected to perform a copy.
    :param may_copy: If true, this operation may create a copy of the data (it is not required to). Otherwise, copying
        is an error and will raise an exception.
    :returns: a collection of edges which are the outgoing edges of the node `n` as a data frame.

    .. code-block:: Python

        edges = graph.out_edges()
        print("Number of edges", len(edges))
        print("One destination with at indexing", edges.at[0, "dest"])
        print("One source with columnar indexing", edges["source"][0]) # This is O(log(E)) time

    Getting an edges source is an O(log(N)) operation to avoid storing an O(E) table of edge sources.

    This operation may copy some or all of the underlying data to create the resulting dataframe. If `nodes`, is
    contiguous and compact (just a starting node and an ending node with stride 1) data will not be copied.
    """
    property_pyarrow_columns = {
        name: self.get_edge_property(name)
        for name in self.loaded_edge_schema().names
        if properties is None or name in properties
    }
    property_types = tuple(a.type.to_pandas_dtype() for a in property_pyarrow_columns.values())
    dtypes = (numpy.uint64, numpy.uint32, numpy.uint32, *property_types)

    if isinstance(nodes, int):
        nodes = slice(nodes, nodes + 1)

    if nodes is None:
        # Optimized all edges case
        columns = property_pyarrow_columns
        columns.update(_build_edge_views(self))
        return LazyDataFrame(columns, dtypes)
    if isinstance(nodes, slice) and (nodes.step == 1 or nodes.step is None):
        # Optimized compact slice case
        node_start, node_stop, node_step = nodes.indices(self.num_nodes())
        assert node_step == 1

        if node_stop >= self.num_nodes():
            raise IndexError(node_stop)

        start = self.out_edge_ids(node_start).start
        stop = self.out_edge_ids(node_stop - 1).stop

        columns = property_pyarrow_columns
        columns.update(_build_edge_views(self))
        return LazyDataFrame(columns, dtypes, offset=start, length=stop - start)

    if isinstance(nodes, slice):
        nodes = range(*nodes.indices(self.num_nodes()))

    if isinstance(nodes, Sequence):
        # General copying implementation
        if not may_copy:
            raise ValueError("Node indexes require a copy, which is not allowed.")
        output_len = sum(len(self.out_edge_ids(i)) for i in nodes)
        column_arrays = _build_edge_arrays(self, nodes, output_len)
        columns = {k: v.to_pandas()[column_arrays["id"]] for k, v in property_pyarrow_columns.items()}
        columns.update(column_arrays)
        return LazyDataFrame(columns, dtypes, offset=0, length=output_len)

    raise ValueError()


def _build_edge_views(self):
    return dict(
        id=range(0, self.num_edges()), source=GraphBaseEdgeSourceAccessor(self), dest=GraphBaseEdgeDestAccessor(self),
    )


def _build_edge_arrays(self, nodes, output_len):
    ids = numpy.empty(output_len, dtype=numpy.uint64)
    sources = numpy.empty(output_len, dtype=numpy.uint32)
    dests = numpy.empty(output_len, dtype=numpy.uint32)
    i = 0
    for n in nodes:
        for e in self.out_edge_ids(n):
            ids[i] = e
            sources[i] = n
            dests[i] = self.get_edge_dst(e)
            i += 1
    return dict(id=ids, source=sources, dest=dests)
