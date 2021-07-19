import numpy

from pyarrow.lib cimport pyarrow_unwrap_table, pyarrow_wrap_chunked_array, pyarrow_wrap_schema, to_shared

from .cpp.libgalois.graphs cimport Graph as CGraph
from .cpp.libsupport.result cimport Result, handle_result_void, raise_error_code

from .numba_support._pyarrow_wrappers import unchunked

from . cimport datastructures

from . import datastructures

from cython.operator cimport dereference as deref
from libc.stdint cimport uint32_t
from libcpp.memory cimport make_shared, shared_ptr, unique_ptr
from libcpp.string cimport string
from libcpp.utility cimport move
from libcpp.vector cimport vector


cdef _convert_string_list(l):
    return [bytes(s, "utf-8") for s in l or []]


cdef shared_ptr[_PropertyGraph] handle_result_PropertyGraph(Result[unique_ptr[_PropertyGraph]] res) nogil except *:
    if not res.has_value():
        with gil:
            raise_error_code(res.error())
    return to_shared(res.value())


cdef CGraph.GraphComponents handle_result_GraphComponents(Result[CGraph.GraphComponents] res) nogil except *:
    if not res.has_value():
        with gil:
            raise_error_code(res.error())
    return move(res.value())

# TODO(amp): Wrap Copy

cdef class PropertyGraphBase:
    """
    A property graph loaded into memory.
    """
    cdef _PropertyGraph* underlying_property_graph(self) nogil except NULL:
        with gil:
            raise NotImplementedError()

    def write(self, path = None, str command_line = "katana.property_graph.PropertyGraph"):
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

    cdef const GraphTopology* topology(PropertyGraph self):
        return &self.underlying_property_graph().topology()

    cpdef uint64_t num_nodes(PropertyGraph self):
        return self.topology().num_nodes()

    def __eq__(self, PropertyGraph other):
        return self.underlying_property_graph().Equals(other.underlying_property_graph())

    def __len__(self):
        """
        >>> len(self)

        Return the number of nodes in the graph.

        Can be called from numba compiled code.
        """
        return self.num_nodes()

    cpdef uint64_t num_edges(PropertyGraph self):
        """
        Return the number of edges in the graph.

        Can be called from numba compiled code.
        """
        return self.topology().num_edges()

    def node_schema(self):
        """
        Return the `pyarrow` schema for the node properties stored with this graph.
        """
        return pyarrow_wrap_schema(self.underlying_property_graph().node_schema())

    def edge_schema(self):
        """
        Return the `pyarrow` schema for the edge properties stored with this graph.
        """
        return pyarrow_wrap_schema(self.underlying_property_graph().edge_schema())

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
        return PropertyGraph._property_name_to_id(prop, self.node_schema())

    def edge_property_name_to_id(self, prop):
        """
        Return the index (ID) of a edge property specified by name. If an index is provided, it is returned.
        """
        return PropertyGraph._property_name_to_id(prop, self.edge_schema())

    def __iter__(self):
        """
        >>> for nid in self: ...

        Iterate over the node IDs of this graph.

        Can be called from numba compiled code with the name `self.nodes()`.
        """
        return iter(range(self.num_nodes()))

    def edges(self, uint64_t n):
        """
        Return a collection of edge IDs which are the outgoing edges of the node `n`.

        Can be called from numba compiled code.
        """
        if n > self.num_nodes():
            raise IndexError(n)

        edge_range = self.topology().edges(n)
        return range(deref(edge_range.begin()), deref(edge_range.end()))

    cpdef uint64_t get_edge_dest(PropertyGraph self, uint64_t e):
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
            self.underlying_property_graph().GetNodeProperty(PropertyGraph._property_name_to_id(prop, self.node_schema()))
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
            self.underlying_property_graph().GetEdgeProperty(PropertyGraph._property_name_to_id(prop, self.edge_schema()))
        )

    def add_node_property(self, table):
        """
        Insert new node properties into this graph.

        :param table: A pyarrow Table containing the properties (the names are taken from the table). The table must have length `len(self)`.
        """
        handle_result_void(self.underlying_property_graph().AddNodeProperties(pyarrow_unwrap_table(table)))

    def upsert_node_property(self, table):
        """
        Update or insert node properties into this graph.

        :param table: A pyarrow Table containing the properties (the names are taken from the table). The table must have length `len(self)`.
        """
        handle_result_void(self.underlying_property_graph().UpsertNodeProperties(pyarrow_unwrap_table(table)))

    def add_edge_property(self, table):
        """
        Insert new edge properties into this graph.

        :param table: A pyarrow Table containing the properties (the names are taken from the table). The table must have length `self.num_edges()`.
        """
        handle_result_void(self.underlying_property_graph().AddEdgeProperties(pyarrow_unwrap_table(table)))

    def upsert_edge_property(self, table):
        """
        Update or insert edge properties into this graph.

        :param table: A pyarrow Table containing the properties (the names are taken from the table). The table must have length `self.num_edges()`.
        """
        handle_result_void(self.underlying_property_graph().UpsertEdgeProperties(pyarrow_unwrap_table(table)))

    def remove_node_property(self, prop):
        """
        Remove a node property from the graph by name or index.
        """
        handle_result_void(self.underlying_property_graph().RemoveNodeProperty(PropertyGraph._property_name_to_id(prop, self.node_schema())))

    def remove_edge_property(self, prop):
        """
        Remove an edge property from the graph by name or index.
        """
        handle_result_void(self.underlying_property_graph().RemoveEdgeProperty(PropertyGraph._property_name_to_id(prop, self.edge_schema())))

    @property
    def path(self):
        """
        The path that the graph was read from and will be written to by default. This can be set.

        :rtype: str
        """
        return str(self.underlying_property_graph().rdg_dir(), encoding="UTF-8")

    @path.setter
    def path(self, path):
        handle_result_void(self.underlying_property_graph().InformPath(bytes(str(path), encoding="UTF-8")))

    @property
    def address(self):
        """
        Internal.
        """
        return <uint64_t>self.underlying_property_graph()


cdef class PropertyGraph(PropertyGraphBase):
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
        :type path: str
        :param partition_id_to_load: The partition number in the graph to load. If this is None (default), then the
            partition corresponding to this host's network id will be loaded.
        :param node_properties: A list of node property names to load into memory. If this is None (default), then all
            properties are loaded.
        :param edge_properties: A list of edge property names to load into memory. If this is None (default), then all
            properties are loaded.
        """
        cdef RDGLoadOptions opts
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
    cdef PropertyGraph make(shared_ptr[_PropertyGraph] u):
        f = <PropertyGraph>PropertyGraph.__new__(PropertyGraph)
        f._underlying_property_graph = u
        return f

    @staticmethod
    def from_graphml(path, uint64_t chunk_size=25000):
        """
        Load a GraphML file into katana form

        :param path: Path to source GraphML file.
        :type path: str or Path
        :param chunk_size: Chunk size for in memory representations during conversion. Generally this term can be
            ignored, but it can be decreased to reduce memory usage when converting large inputs.
        :type chunk_size: int
        :returns: the new :py:class:`~katana.property_graph.PropertyGraph`
        """
        path_str = <string>bytes(str(path), "utf-8")
        with nogil:
            pg = handle_result_PropertyGraph(
                CGraph.ConvertToPropertyGraph(
                move(handle_result_GraphComponents(CGraph.ConvertGraphML(path_str, chunk_size, False)))
                ))
        return PropertyGraph.make(pg)


    @staticmethod
    def from_csr(edge_indices, edge_destinations):
        """
        Create a new `PropertyGraph` from a raw Compressed Sparse Row representation.

        :param edge_indices: The indicies of the first edge for each node in the destinations vector.
        :type edge_indices: `numpy.ndarray` or another type supporting the buffer protocol. Element type must be an
            integer.
        :param edge_destinations: The destinations of edges in the new graph.
        :type edge_destinations: `numpy.ndarray` or another type supporting the buffer protocol. Element type must be an
            integer.
        :returns: the new :py:class:`~katana.property_graph.PropertyGraph`
        """
        cdef datastructures.NUMAArray_uint64_t edge_indices_numa = datastructures.NUMAArray_uint64_t(
            len(edge_indices), datastructures.AllocationPolicy.INTERLEAVED
        )
        cdef datastructures.NUMAArray_uint32_t edge_destinations_numa = datastructures.NUMAArray_uint32_t(
            len(edge_destinations), datastructures.AllocationPolicy.INTERLEAVED
        )

        edge_indices_numa.as_numpy()[:] = edge_indices
        edge_destinations_numa.as_numpy()[:] = edge_destinations

        cdef shared_ptr[_PropertyGraph] pg = make_shared[CGraph._PropertyGraph](
            CGraph.GraphTopology(move(edge_indices_numa.underlying), move(edge_destinations_numa.underlying))
        )
        return PropertyGraph.make(pg)
