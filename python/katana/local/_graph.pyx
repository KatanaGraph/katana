from typing import Optional, Sequence, Union

import numpy
import numpy as np
import pyarrow

from pyarrow.lib cimport pyarrow_unwrap_table, pyarrow_wrap_chunked_array, pyarrow_wrap_schema, to_shared

from katana.cpp.libgalois.graphs cimport Graph as CGraph
from katana.cpp.libsupport.entity_type_manager cimport EntityTypeManager
from katana.cpp.libsupport.result cimport Result, handle_result_void, raise_error_code

from katana.native_interfacing._pyarrow_wrappers import unchunked

from . cimport datastructures

from . import datastructures

from cython.operator cimport dereference as deref
from libc.stdint cimport uint32_t
from libcpp.memory cimport shared_ptr, unique_ptr
from libcpp.string cimport string
from libcpp.utility cimport move
from libcpp.vector cimport vector

from katana.dataframe import DataFrame, LazyDataAccessor, LazyDataFrame

from ..native_interfacing.buffer_access cimport to_pyarrow
from .entity_type cimport EntityType

from abc import abstractmethod

__all__ = ["GraphBase", "Graph", "TxnContext"]


cdef _convert_string_list(l):
    return [bytes(s, "utf-8") for s in l or []]


cdef shared_ptr[_PropertyGraph] handle_result_PropertyGraph(Result[unique_ptr[_PropertyGraph]] res) nogil except *:
    if not res.has_value():
        with gil:
            raise_error_code(res.error())
    return to_shared(res.value())


class GraphBaseEdgeDestAccessor(LazyDataAccessor):
    def __init__(self, underlying: GraphBase, start, end):
        self._underlying = underlying
        self._start = start
        self._end = end

    def __getitem__(self, i):
        return self._underlying.get_edge_dest(i)

    def array(self, item: slice):
        start, stop, step = item.indices(self._end - self._start)
        npdata = numpy.empty(len(range(start, stop, step)), dtype=numpy.uint32)
        cdef uint32_t[:] data = npdata
        for i in range(start, stop, step):
            data[i - start] = self._underlying.get_edge_dest(i)
        return npdata


class GraphBaseEdgeSourceAccessor(LazyDataAccessor):
    def __init__(self, underlying: GraphBase, start, end):
        """
        :param underlying: The underlying graph.
        :param start: The search start, aka the first possible node
        :param end:
        """
        self._underlying = underlying
        self._start = start
        self._end = end

    def __getitem__(self, i):
        return self._underlying._get_edge_source(i, self._start, self._end)

    def array(self, item: slice):
        # TODO(amp):PERFORMANCE: This generates the entire sources array even when we only need part.
        array = numpy.empty(self._underlying.num_edges(), dtype=numpy.uint32)
        cdef uint32_t[:] data = array
        for n in range(self._underlying.num_nodes()):
            for i in self._underlying.edge_ids(n):
                data[<uint64_t>i] = n
        return array[item]


cdef class GraphBase:
    """
    A property graph loaded into memory.
    """
    cdef _PropertyGraph* underlying_property_graph(self) nogil except NULL:
        with gil:
            raise NotImplementedError()

    def write(self, path = None, str command_line = "katana.local.Graph"):
        """
        Write the property graph to the specified path or URL (or the original path it was loaded from if path is
        not provided). Provide lineage information in the form of a command line.

        :param path: The path to which to write or None to use `self.path`.
        :type path: str or Path
        :param command_line: Lineage information in the form of a command line.
        :type command_line: str
        """
        command_line_str = <string>bytes(command_line, "utf-8")
        if path is None:
            with nogil:
                handle_result_void(self.underlying_property_graph().Commit(command_line_str))
        path_str = <string>bytes(str(path), "utf-8")
        with nogil:
            handle_result_void(self.underlying_property_graph().Write(path_str, command_line_str))

    cdef const GraphTopology* topology(Graph self):
        return &self.underlying_property_graph().topology()

    cpdef uint64_t num_nodes(Graph self):
        return self.topology().num_nodes()

    def __eq__(self, Graph other):
        return self.underlying_property_graph().Equals(other.underlying_property_graph())

    def __len__(self):
        """
        >>> len(self)

        Return the number of nodes in the graph.

        Can be called from numba compiled code.
        """
        return self.num_nodes()

    cpdef uint64_t num_edges(Graph self):
        """
        Return the number of edges in the graph.

        Can be called from numba compiled code.
        """
        return self.topology().num_edges()

    def loaded_node_schema(self):
        """
        Return the `pyarrow` schema for the node properties loaded for this graph.
        """
        return pyarrow_wrap_schema(self.underlying_property_graph().loaded_node_schema())

    def loaded_edge_schema(self):
        """
        Return the `pyarrow` schema for the edge properties loaded for this graph.
        """
        return pyarrow_wrap_schema(self.underlying_property_graph().loaded_edge_schema())

    @staticmethod
    cdef uint64_t _property_name_to_id(object prop, Schema schema) except -1:
        cdef uint64_t pid
        if isinstance(prop, str):
            try:
                pid = schema.names.index(prop)
            except ValueError:
                raise KeyError(prop)
        elif isinstance(prop, int):
            if prop < 0 or prop >= len(schema):
                raise IndexError(prop)
            pid = prop
        else:
            raise TypeError("Properties must be identified by int index or str name")
        return pid

    def node_property_name_to_id(self, prop):
        """
        Return the index (ID) of a node property specified by name. If an index is provided, it is returned.
        """
        return Graph._property_name_to_id(prop, self.loaded_node_schema())

    def edge_property_name_to_id(self, prop):
        """
        Return the index (ID) of a edge property specified by name. If an index is provided, it is returned.
        """
        return Graph._property_name_to_id(prop, self.loaded_edge_schema())

    def __iter__(self):
        """
        >>> for nid in self: ...

        Iterate over the node IDs of this graph.

        Can be called from numba compiled code with the name `self.nodes()`.
        """
        return iter(range(self.num_nodes()))

    def edge_ids(self, uint64_t n):
        """
        Return a collection of edge IDs which are the outgoing edges of the node `n`.

        Can be called from numba compiled code.
        """
        if n > self.num_nodes():
            raise IndexError(n)

        edge_range = self.topology().edges(n)
        return range(deref(edge_range.begin()), deref(edge_range.end()))

    def get_edge_source(self, uint64_t e):
        """
        Get the source of an edge. This is O(log(N)) since it uses binary search to avoid having to store a source list.
        :param e: an edge ID.
        :return: The source node of that edge.
        """
        return self._get_edge_source(e)

    def _get_edge_source(self, uint64_t e, uint64_t start = 0, end = None):
        """
        The implementation for `get_edge_source`. This accepts the start and end of the search range as well.
        """
        bottom = start
        top = end or self.num_nodes()
        while bottom < top:
            pivot = bottom + (top - bottom) // 2
            edge_range = self.edge_ids(pivot)
            if edge_range.start <= e < edge_range.stop:
                return pivot
            elif e >= edge_range.stop:
                bottom = edge_range.stop
            elif e < edge_range.start:
                top = edge_range.start
            else:
                raise AssertionError("Unreachable")
        raise ValueError(e)

    def edges(self,
              nodes : Union[int, slice, Sequence[int], None] = None,
              *,
              properties: Optional[Sequence[Union[int, str]]] = None,
              may_copy = True
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

            edges = graph.edges()
            print("Number of edges", len(edges))
            print("One destination with at indexing", edges.at[0, "dest"])
            print("One source with columnar indexing", edges["source"][0]) # This is O(log(E)) time

        Getting an edges source is an O(log(N)) operation to avoid storing an O(E) table of edge sources.

        This operation may copy some or all of the underlying data to create the resulting dataframe. If `nodes`, is
        contiguous and compact (just a starting node and an ending node with stride 1) data will not be copied.
        """
        property_pyarrow_columns = {name: self.get_edge_property(i)
                                    for i, name in enumerate(self.loaded_edge_schema().names)
                                    if properties is None or name in properties or i in properties}
        property_types = tuple(a.type.to_pandas_dtype() for a in property_pyarrow_columns.values())
        dtypes = (numpy.uint64, numpy.uint32, numpy.uint32, *property_types)

        if isinstance(nodes, int):
            nodes = slice(nodes, nodes+1)

        if nodes is None:
            # Optimized all edges case
            return LazyDataFrame(
                dict(
                    id=range(0, self.num_edges()),
                    source=GraphBaseEdgeSourceAccessor(self, 0, self.num_nodes()),
                    dest=GraphBaseEdgeDestAccessor(self, 0, self.num_edges()),
                    **property_pyarrow_columns),
                dtypes)
        if isinstance(nodes, slice) and (nodes.step == 1 or nodes.step == None):
            # Optimized compact slice case
            node_start, node_stop, node_step = nodes.indices(self.num_nodes())
            assert node_step == 1

            if node_stop >= self.num_nodes():
                raise IndexError(node_stop)

            start = deref(self.topology().edges(node_start).begin())
            stop = deref(self.topology().edges(node_stop-1).end())

            return LazyDataFrame(
                dict(id=range(0, self.num_edges()),
                     source=GraphBaseEdgeSourceAccessor(self, node_start, node_stop-1),
                     dest=GraphBaseEdgeDestAccessor(self, 0, self.num_edges()),
                     **property_pyarrow_columns),
                dtypes,
                offset=start,
                length=stop-start)
        if isinstance(nodes, slice):
            nodes = range(*nodes.indices(self.num_nodes()))
        if isinstance(nodes, Sequence):
            # General copying implementation
            if not may_copy:
                raise ValueError("Node indexes require a copy, which is not allowed.")
            output_len = sum(len(self.edge_ids(i)) for i in nodes)
            edge_ids = np.empty(output_len, dtype=np.uint64)
            sources = np.empty(output_len, dtype=np.uint32)
            dests = np.empty(output_len, dtype=np.uint32)
            i = 0
            for n in nodes:
                for e in self.edge_ids(n):
                    edge_ids[i] = e
                    sources[i] = n
                    dests[i] = self.get_edge_dest(e)
                    i += 1
            property_pyarrow_columns = {k: v.to_pandas()[edge_ids] for k, v in property_pyarrow_columns.items()}
            return LazyDataFrame(
                dict(id=edge_ids,
                     source=sources,
                     dest=dests,
                     **property_pyarrow_columns),
                dtypes,
                offset=0,
                length=output_len)



    cpdef uint64_t get_edge_dest(Graph self, uint64_t e):
        """
        Return the destination node ID of the edge `e`.

        Can be called from numba compiled code.
        """
        if e > self.num_edges():
            raise IndexError(e)
        return self.topology().edge_dest(e)

    def get_node_property(self, prop):
        """
        Return a `pyarrow` array or chunked array storing the data for node property `prop`.
        This attempts to unwrap chunked arrays if possible.
        `prop` may be either a name or an index.
        """
        return unchunked(self.get_node_property_chunked(prop))

    def get_node_property_chunked(self, prop):
        """
        Return a `pyarrow` chunked array storing the data for node property `prop`.
        `prop` may be either a name or an index.
        `get_node_property` should be used unless a chunked array is explicitly needed as non-chunked arrays are much more efficient.
        """
        return pyarrow_wrap_chunked_array(
            self.underlying_property_graph().GetNodeProperty(Graph._property_name_to_id(prop, self.loaded_node_schema()))
        )

    def get_edge_property(self, prop):
        """
        Return a `pyarrow` array or chunked array storing the data for edge property `prop`.
        This attempts to unwrap chunked arrays if possible.
        `prop` may be either a name or an index.
        """
        return unchunked(self.get_edge_property_chunked(prop))

    def get_edge_property_chunked(self, prop):
        """
        Return a `pyarrow` chunked array storing the data for edge property `prop`.
        `prop` may be either a name or an index.
        `get_edge_property` should be used unless a chunked array is explicitly needed as non-chunked arrays are much more efficient.
        """
        return pyarrow_wrap_chunked_array(
            self.underlying_property_graph().GetEdgeProperty(Graph._property_name_to_id(prop, self.loaded_edge_schema()))
        )

    @staticmethod
    cdef shared_ptr[CTable] _convert_table(object table, dict kwargs) except *:
        if isinstance(table, pyarrow.Table):
            arrow_table = table
        elif table is not None:
            arrow_table = pyarrow.table(table)
        else:
            arrow_table = None

        for name, data in kwargs.items():
            if arrow_table:
                arrow_table = arrow_table.append_column(name, to_pyarrow(data))
            else:
                arrow_table = pyarrow.table({name: to_pyarrow(data)})

        return pyarrow_unwrap_table(arrow_table)

    def add_node_property(self, table=None, **kwargs):
        """
        Insert new node properties into this graph.

        :param table: A pyarrow Table or other dataframe-like object containing the properties. The table must have
            length ``self.num_nodes()``. (Optional)
        :param kwargs: Properties to add. The values must be arrays or sequences of length ``self.num_nodes()``. (Optional)
        """
        handle_result_void(self.underlying_property_graph().AddNodeProperties(GraphBase._convert_table(table, kwargs)))

    def upsert_node_property(self, table=None, TxnContext txn_ctx=None, **kwargs):
        """
        Update or insert node properties into this graph.

        :param txn_ctx: The tranaction context for passing read write sets.
        :param table: A pyarrow Table or other dataframe-like object containing the properties. The table must have
            length ``self.num_nodes()``. (Optional)
        :param kwargs: Properties to add. The values must be arrays or sequences of length ``self.num_nodes()``. (Optional)
        """
        txn_ctx = TxnContext() if txn_ctx is None else txn_ctx
        handle_result_void(self.underlying_property_graph().UpsertNodeProperties(GraphBase._convert_table(table, kwargs), &txn_ctx._txn_ctx))

    def add_edge_property(self, table=None, **kwargs):
        """
        Insert new edge properties into this graph.

        :param table: A pyarrow Table or other dataframe-like object containing the properties. The table must have
            length ``self.num_edges()``. (Optional)
        :param kwargs: Properties to add. The values must be arrays or sequences of length ``self.num_edges()``. (Optional)
        """
        handle_result_void(self.underlying_property_graph().AddEdgeProperties(GraphBase._convert_table(table, kwargs)))

    def upsert_edge_property(self, table=None, TxnContext txn_ctx=None, **kwargs):
        """
        Update or insert edge properties into this graph.

        :param txn_ctx: The tranaction context for passing read write sets.
        :param table: A pyarrow Table or other dataframe-like object containing the properties. The table must have
            length ``self.num_edges()``. (Optional)
        :param kwargs: Properties to add. The values must be arrays or sequences of length ``self.num_edges()``. (Optional)
        """
        txn_ctx = TxnContext() if txn_ctx is None else txn_ctx
        handle_result_void(self.underlying_property_graph().UpsertEdgeProperties(GraphBase._convert_table(table, kwargs), &txn_ctx._txn_ctx))

    def remove_node_property(self, prop):
        """
        Remove a node property from the graph by name or index.
        """
        handle_result_void(self.underlying_property_graph().RemoveNodeProperty(Graph._property_name_to_id(prop, self.loaded_node_schema())))

    def remove_edge_property(self, prop):
        """
        Remove an edge property from the graph by name or index.
        """
        handle_result_void(self.underlying_property_graph().RemoveEdgeProperty(Graph._property_name_to_id(prop, self.loaded_edge_schema())))

    @property
    def path(self):
        """
        The path that the graph was read from and will be written to by default. This can be set.

        :rtype: str
        """
        return str(self.underlying_property_graph().rdg_dir(), encoding="UTF-8")

    @property
    def node_types(self):
        """
        The types of atomic node types in the graph.

        :rtype: list[EntityType]
        """
        cdef const EntityTypeManager* manager = &self.underlying_property_graph().GetNodeTypeManager()
        type_ids = manager.GetAtomicEntityTypeIDs()
        types = [EntityType.make(manager, type_id) for type_id in type_ids]
        return types

    @property
    def edge_types(self):
        """
        The types of atomic edge types in the graph.

        :rtype: list
        """
        cdef const EntityTypeManager* manager = &self.underlying_property_graph().GetEdgeTypeManager()
        types = manager.GetAtomicEntityTypeIDs()
        return [EntityType.make(manager, typeid) for typeid in types]

    @abstractmethod
    def global_out_degree(self, uint64_t node):
        raise NotImplementedError()

    @abstractmethod
    def global_in_degree(self, uint64_t node):
        raise NotImplementedError()

cdef class Graph(GraphBase):
    """
    A property graph loaded into memory.
    """

    cdef _PropertyGraph * underlying_property_graph(self) nogil except NULL:
        return self._underlying_property_graph.get()

    def __init__(self, path, node_properties=None, edge_properties=None, partition_id_to_load=None):
        """
        __init__(self, path, node_properties=None, edge_properties=None, partition_id_to_load=None)

        Load a property graph.

        :param path: the path or URL from which to load the graph. This support local paths or s3 URLs.
        :type path: Union[str, Path]
        :param partition_id_to_load: The partition number in the graph to load. If this is None (default), then the
            partition corresponding to this host's network id will be loaded.
        :param node_properties: A list of node property names to load into memory. If this is None (default), then all
            properties are loaded.
        :param edge_properties: A list of edge property names to load into memory. If this is None (default), then all
            properties are loaded.
        """
        cdef CGraph.RDGLoadOptions opts
        cdef vector[string] node_props
        cdef vector[string] edge_props
        # we need to generate a reference to a uint32_t in order to construct an
        # optional[uint32_t] in opts
        cdef uint32_t part_to_load
        if partition_id_to_load is not None:
            part_to_load = partition_id_to_load
            opts.partition_id_to_load = part_to_load
        if node_properties is not None:
            node_props = _convert_string_list(node_properties)
            opts.node_properties = node_props
        if edge_properties is not None:
            edge_props = _convert_string_list(edge_properties)
            opts.edge_properties = edge_props
        path_str = <string>bytes(str(path), "utf-8")
        with nogil:
            self._underlying_property_graph = handle_result_PropertyGraph(_PropertyGraph.Make(path_str, opts))

    @staticmethod
    cdef Graph make(shared_ptr[_PropertyGraph] u):
        f = <Graph>Graph.__new__(Graph)
        f._underlying_property_graph = u
        return f

    @property
    def __katana_address__(self):
        """
        Internal.
        """
        return <uint64_t>self.underlying_property_graph()

    def global_out_degree(self, uint64_t node):
        return len(self.edge_ids(node))

    def global_in_degree(self, uint64_t node):
        # TODO(loc) needs shared-memory bi-directional view
        raise NotImplementedError()
