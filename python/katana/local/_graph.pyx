from typing import Optional, Sequence, Union

import numpy
import numpy as np
import pyarrow

from pyarrow.lib cimport pyarrow_unwrap_table, pyarrow_wrap_chunked_array, pyarrow_wrap_schema, to_shared

from katana.cpp.libgalois.graphs cimport Graph as CGraph
from katana.cpp.libsupport.result cimport Result, handle_result_void, raise_error_code

from katana.native_interfacing._pyarrow_wrappers import unchunked

from . cimport datastructures

from . import datastructures

from cython.operator cimport dereference as deref
from libc.stdint cimport uint16_t, uint32_t, uintptr_t
from libcpp cimport bool
from libcpp.memory cimport shared_ptr, unique_ptr
from libcpp.string cimport string
from libcpp.utility cimport move
from libcpp.vector cimport vector

from katana.dataframe import DataFrame, LazyDataAccessor, LazyDataFrame
from katana.local import TxnContext
from katana.local_native import EntityTypeManager

from ..native_interfacing.buffer_access cimport to_pyarrow

from abc import abstractmethod

ctypedef uint16_t EntityTypeID

__all__ = ["GraphBase"]

cdef _PropertyGraph* underlying_property_graph(graph) nogil:
    with gil:
        if isinstance(graph, GraphBase):
            return (<GraphBase>graph).underlying_property_graph()
        else:
            return <_PropertyGraph*><uintptr_t>graph.__katana_address__

cdef CTxnContext* underlying_txn_context(txn_context) nogil:
    with gil:
        if txn_context:
            return <CTxnContext*><uintptr_t>txn_context.__katana_address__
        return NULL



cdef _convert_string_list(l):
    return [bytes(s, "utf-8") for s in l or []]


cdef shared_ptr[_PropertyGraph] handle_result_PropertyGraph(Result[unique_ptr[_PropertyGraph]] res) nogil except *:
    if not res.has_value():
        with gil:
            raise_error_code(res.error())
    return to_shared(res.value())


cdef class GraphBase:
    """
    A property graph loaded into memory.
    """
    cdef _PropertyGraph* underlying_property_graph(self) nogil except NULL:
        with gil:
            raise NotImplementedError()

    def write(self, path = None, str command_line = "katana.local.Graph", txn_ctx=None):
        """
        Write the property graph to the specified path or URL (or the original path it was loaded from if path is
        not provided). Provide lineage information in the form of a command line.

        :param path: The path to which to write or None to use `self.path`.
        :type path: str or Path
        :param command_line: Lineage information in the form of a command line.
        :type command_line: str
        :param txn_ctx: The tranaction context for passing read write sets.
        :type txn_ctx: TxnContext
        """
        command_line_str = <string>bytes(command_line, "utf-8")
        txn_ctx = txn_ctx or TxnContext()
        if path is None:
            with nogil:
                handle_result_void(self.underlying_property_graph().Commit(command_line_str, underlying_txn_context(txn_ctx)))
        path_str = <string>bytes(str(path), "utf-8")
        with nogil:
            handle_result_void(self.underlying_property_graph().Write(path_str, command_line_str, underlying_txn_context(txn_ctx)))

    cdef const GraphTopology* topology(self):
        return &self.underlying_property_graph().topology()

    cpdef uint64_t num_nodes(self):
        return self.topology().NumNodes()

    def __len__(self):
        """
        >>> len(self)

        Return the number of nodes in the graph.

        Can be called from numba compiled code.
        """
        return self.num_nodes()

    cpdef uint64_t num_edges(self):
        """
        Return the number of edges in the graph.

        Can be called from numba compiled code.
        """
        return self.topology().NumEdges()

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
        return GraphBase._property_name_to_id(prop, self.loaded_node_schema())

    def edge_property_name_to_id(self, prop):
        """
        Return the index (ID) of a edge property specified by name. If an index is provided, it is returned.
        """
        return GraphBase._property_name_to_id(prop, self.loaded_edge_schema())

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

        edge_range = self.topology().OutEdges(n)
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


    cpdef uint64_t get_edge_dst(self, uint64_t e):
        """
        Return the destination node ID of the edge `e`.

        Can be called from numba compiled code.
        """
        if e > self.num_edges():
            raise IndexError(e)
        return self.topology().OutEdgeDst(e)

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
            self.underlying_property_graph().GetNodeProperty(GraphBase._property_name_to_id(prop, self.loaded_node_schema()))
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
            self.underlying_property_graph().GetEdgeProperty(GraphBase._property_name_to_id(prop, self.loaded_edge_schema()))
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

    def add_node_property(self, table=None, *, txn_ctx=None, **kwargs):
        """
        Insert new node properties into this graph.

        :param table: A pyarrow Table or other dataframe-like object containing the properties. The table must have
            length ``self.num_nodes()``. (Optional)
        :param txn_ctx: The tranaction context for passing read write sets.
        :param kwargs: Properties to add. The values must be arrays or sequences of length ``self.num_nodes()``. (Optional)
        """
        txn_ctx = txn_ctx or TxnContext()
        handle_result_void(self.underlying_property_graph().AddNodeProperties(GraphBase._convert_table(table, kwargs), underlying_txn_context(txn_ctx)))

    def upsert_node_property(self, table=None, *, txn_ctx=None, **kwargs):
        """
        Update or insert node properties into this graph.

        :param table: A pyarrow Table or other dataframe-like object containing the properties. The table must have
            length ``self.num_nodes()``. (Optional)
        :param txn_ctx: The tranaction context for passing read write sets.
        :param kwargs: Properties to add. The values must be arrays or sequences of length ``self.num_nodes()``. (Optional)
        """
        txn_ctx = txn_ctx or TxnContext()
        handle_result_void(self.underlying_property_graph().UpsertNodeProperties(GraphBase._convert_table(table, kwargs), underlying_txn_context(txn_ctx)))

    def add_edge_property(self, table=None, *, txn_ctx=None, **kwargs):
        """
        Insert new edge properties into this graph.

        :param table: A pyarrow Table or other dataframe-like object containing the properties. The table must have
            length ``self.num_edges()``. (Optional)
        :param txn_ctx: The tranaction context for passing read write sets.
        :param kwargs: Properties to add. The values must be arrays or sequences of length ``self.num_edges()``. (Optional)
        """
        txn_ctx = txn_ctx or TxnContext()
        handle_result_void(self.underlying_property_graph().AddEdgeProperties(GraphBase._convert_table(table, kwargs), underlying_txn_context(txn_ctx)))

    def upsert_edge_property(self, table=None, *, txn_ctx=None, **kwargs):
        """
        Update or insert edge properties into this graph.

        :param table: A pyarrow Table or other dataframe-like object containing the properties. The table must have
            length ``self.num_edges()``. (Optional)
        :param txn_ctx: The tranaction context for passing read write sets.
        :param kwargs: Properties to add. The values must be arrays or sequences of length ``self.num_edges()``. (Optional)
        """
        txn_ctx = txn_ctx or TxnContext()
        handle_result_void(self.underlying_property_graph().UpsertEdgeProperties(GraphBase._convert_table(table, kwargs), underlying_txn_context(txn_ctx)))

    def remove_node_property(self, prop, txn_ctx=None):
        """
        Remove a node property from the graph by name or index.
        """
        txn_ctx = txn_ctx or TxnContext()
        handle_result_void(self.underlying_property_graph().RemoveNodeProperty(GraphBase._property_name_to_id(prop, self.loaded_node_schema()), underlying_txn_context(txn_ctx)))

    def remove_edge_property(self, prop, txn_ctx=None):
        """
        Remove an edge property from the graph by name or index.
        """
        txn_ctx = txn_ctx or TxnContext()
        handle_result_void(self.underlying_property_graph().RemoveEdgeProperty(GraphBase._property_name_to_id(prop, self.loaded_edge_schema()), underlying_txn_context(txn_ctx)))

    @property
    def path(self):
        """
        The path that the graph was read from and will be written to by default. This can be set.

        :rtype: str
        """
        return str(self.underlying_property_graph().rdg_dir_raw_string(), encoding="UTF-8")

    @property
    def node_types(self):
        """
        :return: the node type manager
        """
        return EntityTypeManager._make_from_address(<uint64_t>&self.underlying_property_graph().GetNodeTypeManager(), self)

    def get_type_of_node(self, uint64_t n):
        """
        Return the type ID of the most specific type of a node `n`

        :param n: node id
        :return: the type id of the node
        """
        return self.node_types.type_from_id(self.underlying_property_graph().GetTypeOfNode(n))

    def does_node_have_type(self, uint64_t n, entity_type):
        """
        Check whether a given node has a certain type

        :param n: node id
        :param type_id: type id of type int or EntityType
        :return: True iff node n has the given type
        """
        if isinstance(entity_type, int):
            type_id = entity_type
        elif hasattr(entity_type, "id"):
            type_id = entity_type.id
        else:
            raise ValueError(f"{entity_type}'s type is not supported")
        return self.underlying_property_graph().DoesNodeHaveType(n, type_id)

    @property
    def edge_types(self):
        """
        :return: the edge type manager
        """
        return EntityTypeManager._make_from_address(<uint64_t>&self.underlying_property_graph().GetEdgeTypeManager(), self)

    def get_type_of_edge(self, uint64_t e):
        """
        Return the type ID of the most specific type of an edge `e`

        :param e: edge id
        :return: the type id of the edge
        """
        return self.edge_types.type_from_id(self.underlying_property_graph().GetTypeOfEdgeFromPropertyIndex(e))

    def does_edge_have_type(self, uint64_t e, entity_type):
        """
        Check whether a given edge has a certain type

        :param e: edge id
        :param type_id: type id of type int or EntityType
        :return: True iff edge e has the given type
        """
        if isinstance(entity_type, int):
            type_id = entity_type
        elif hasattr(entity_type, "id"):
            type_id = entity_type.id
        else:
            raise ValueError(f"{entity_type}'s type is not supported")
        return self.underlying_property_graph().DoesEdgeHaveTypeFromPropertyIndex(e, type_id)

    @abstractmethod
    def global_out_degree(self, uint64_t node):
        raise NotImplementedError()

    @abstractmethod
    def global_in_degree(self, uint64_t node):
        raise NotImplementedError()
