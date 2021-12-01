from libc.stdint cimport uint32_t, uint64_t
from libcpp.memory cimport shared_ptr, unique_ptr
from libcpp.string cimport string
from libcpp.vector cimport vector
from pyarrow.lib cimport CArray, CChunkedArray, CSchema, CTable, CUInt32Array, CUInt64Array

from katana.cpp.boost cimport counting_iterator
from katana.cpp.libgalois.datastructures cimport NUMAArray
from katana.cpp.libstd.optional cimport optional
from katana.cpp.libsupport.entity_type_manager cimport EntityTypeManager
from katana.cpp.libsupport.result cimport Result

from ..Galois cimport MethodFlag, NoDerefIterator, StandardRange


# This is very similar to the block below, but needs to be in the tsuba namespace
cdef extern from "tsuba/RDG.h" namespace "tsuba" nogil:
    cdef struct RDGLoadOptions:
        optional[uint32_t] partition_id_to_load
        optional[vector[string]] node_properties
        optional[vector[string]] edge_properties

cdef extern from "tsuba/TxnContext.h" namespace "tsuba" nogil:
    cdef cppclass TxnContext:
        TxnContext()


# Omit the exception specifications here to
# allow returning lvalues.
# Since the exception specifications are omitted here,
# these classes/functions ABSOLUTELY MUST be used only
# within functions with C++ exception handling specifications.
# This is intentional and is required to ensure that C++ exceptions
# thrown in the code written using these forward declarations
# are forwarded properly into the Galois library rather than
# being converted into Python exceptions.
cdef extern from "katana/Graph.h" namespace "katana" nogil:
    cppclass MorphGraph[node_data, edge_data, is_directed]:

        morph_graph()
        cppclass GraphNode:
            pass

        cppclass edge_iterator:
            bint operator==(edge_iterator)
            bint operator!=(edge_iterator)
            edge_iterator operator++()
            edge_iterator operator--()

        cppclass iterator:
            bint operator==(iterator)
            bint operator!=(iterator)
            iterator operator++()
            iterator operator--()
            GraphNode operator*()

        edge_iterator edge_begin(GraphNode)
        edge_iterator edge_end(GraphNode)

        iterator begin()
        iterator end()

        GraphNode getEdgeDst(edge_iterator)
        node_data& getData(GraphNode)

        edge_data& getEdgeData(edge_iterator)
        edge_data& getEdgeData(edge_iterator, MethodFlag)

        GraphNode createNode(...)
        void addNode(GraphNode)
        edge_iterator addEdge(GraphNode, GraphNode)

    cppclass LC_CSR_Graph[node_data, edge_data, is_directed]:

        LC_CSR_Graph()
        cppclass GraphNode:
            bint operator==(unsigned long)
            bint operator==(GraphNode)

        cppclass edge_iterator:
            bint operator==(edge_iterator)
            bint operator!=(edge_iterator)
            edge_iterator operator++()
            edge_iterator operator--()

        cppclass iterator:
            bint operator==(iterator)
            bint operator!=(iterator)
            iterator operator++()
            iterator operator--()
            GraphNode operator*()

        StandardRange[NoDerefIterator[edge_iterator]] edges(GraphNode)
        StandardRange[NoDerefIterator[edge_iterator]] edges(unsigned long)
        StandardRange[NoDerefIterator[edge_iterator]] edges(GraphNode, MethodFlag)
        StandardRange[NoDerefIterator[edge_iterator]] edges(unsigned long, MethodFlag)

        edge_iterator edge_begin(GraphNode)
        edge_iterator edge_end(GraphNode)
        edge_iterator edge_begin(unsigned long)
        edge_iterator edge_end(unsigned long)

        edge_iterator edge_begin(GraphNode, MethodFlag)
        edge_iterator edge_end(GraphNode, MethodFlag)
        edge_iterator edge_begin(unsigned long, MethodFlag)
        edge_iterator edge_end(unsigned long, MethodFlag)

        iterator begin()
        iterator end()

        edge_iterator findEdge(GraphNode, GraphNode)

        GraphNode getEdgeDst(edge_iterator)
        node_data& getData(GraphNode)
        node_data& getData(GraphNode, MethodFlag)
        node_data& getData(unsigned long)
        node_data& getData(unsigned long, MethodFlag)
        void readGraphFromGRFile(string filename)
        unsigned long size()
        edge_data& getEdgeData(edge_iterator)
        edge_data& getEdgeData(edge_iterator, MethodFlag)

    ctypedef uint32_t Node "katana::GraphTopology::Node"
    ctypedef uint64_t Edge "katana::GraphTopology::Edge"

    cppclass GraphTopology:
        GraphTopology(
                const Edge * adj_indices, size_t numNodes, const Node * dests,
                size_t numEdges)
        GraphTopology(
                NUMAArray[uint64_t] &&adj_indices, NUMAArray[uint32_t] &&dests)

        StandardRange[counting_iterator[Edge]] edges(Node node) const
        Node edge_dest(Edge edge_id) const
        uint64_t num_nodes() const
        uint64_t num_edges() const

    cppclass _PropertyGraph "katana::PropertyGraph":
        PropertyGraph()
        # PropertyGraph(GraphTopology&&)

        @staticmethod
        Result[unique_ptr[_PropertyGraph]] Make(string filename, RDGLoadOptions opts)

        # TODO(amp/amber): Having multiple methods with the same name 'Make'
        # confuses cython, so applying the trick of renaming for python API
        @staticmethod
        Result[unique_ptr[_PropertyGraph]] MakeFromTopo "Make" (GraphTopology&& topo)

        bint Equals(const _PropertyGraph*)

        Result[void] Write(string path, string command_line)
        Result[void] Commit(string command_line)

        GraphTopology& topology()

        shared_ptr[CSchema] loaded_node_schema()
        shared_ptr[CSchema] loaded_edge_schema()

        shared_ptr[CTable] node_properties()
        shared_ptr[CTable] edge_properties()
        EntityTypeManager& GetNodeTypeManager() const
        EntityTypeManager& GetEdgeTypeManager() const

        const string& rdg_dir()

        shared_ptr[CChunkedArray] GetNodeProperty(int i)
        shared_ptr[CChunkedArray] GetNodeProperty(const string&)

        shared_ptr[CChunkedArray] GetEdgeProperty(int i)
        shared_ptr[CChunkedArray] GetEdgeProperty(const string&)

        Result[void] AddNodeProperties(shared_ptr[CTable])
        Result[void] AddEdgeProperties(shared_ptr[CTable])
        Result[void] UpsertNodeProperties(shared_ptr[CTable], TxnContext*)
        Result[void] UpsertEdgeProperties(shared_ptr[CTable], TxnContext*)

        Result[void] RemoveNodeProperty(int)
        Result[void] RemoveNodeProperty(const string&)

        Result[void] RemoveEdgeProperty(int)
        Result[void] RemoveEdgeProperty(const string&)

        void MarkAllPropertiesPersistent()
        Result[void] MarkNodePropertiesPersistent(const vector[string]& persist_node_props)
        Result[void] MarkEdgePropertiesPersistent(const vector[string]& persist_edge_props)



cdef extern from "katana/BuildGraph.h" namespace "katana" nogil:
    cppclass GraphComponents:
        # These exist in C++, but are not needed in Cython yet, so commented to avoid accidentally using untested code.

        # GraphComponent nodes
        # GraphComponent edges
        # shared_ptr[GraphTopology] topology
        #
        # GraphComponents(GraphComponent nodes_, GraphComponent edges_, shared_ptr[GraphTopology] topology_)
        # GraphComponents()

        void Dump()

cdef extern from "katana/GraphML.h" namespace "katana" nogil:
    Result[unique_ptr[_PropertyGraph]] ConvertToPropertyGraph(GraphComponents&& graph_comps);

    Result[GraphComponents] ConvertGraphML(
        string input_filename, size_t chunk_size, bint verbose)
