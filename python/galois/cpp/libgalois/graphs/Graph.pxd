from libcpp.string cimport string
from ..Galois cimport MethodFlag, NoDerefIterator, StandardRange
from libcpp.memory cimport unique_ptr, shared_ptr
from libcpp.vector cimport vector
from libc.stdint cimport uint64_t
from galois.cpp.libstd.boost cimport std_result
from pyarrow.lib cimport CSchema, CChunkedArray, CArray, CTable, CUInt32Array, CUInt64Array

# Omit the exception specifications here to
# allow returning lvalues.
# Since the exception specifications are omitted here,
# these classes/functions ABSOLUTELY MUST be used only
# within functions with C++ exception handling specifications.
# This is intentional and is required to ensure that C++ exceptions
# thrown in the code written using these forward declarations
# are forwarded properly into the Galois library rather than
# being converted into Python exceptions.
cdef extern from "galois/graphs/Graph.h" namespace "galois::graphs" nogil:
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

    cppclass GraphTopology:
        shared_ptr[CUInt64Array] out_indices
        shared_ptr[CUInt32Array] out_dests
        uint64_t num_nodes()
        uint64_t num_edges()

    cppclass PropertyFileGraph:
        PropertyFileGraph()
        @staticmethod
        std_result[unique_ptr[PropertyFileGraph]] Make(string filename)
        @staticmethod
        std_result[unique_ptr[PropertyFileGraph]] MakeWithProperties "Make" (string filename, vector[string] node_properties, vector[string] edge_properties)

        std_result[void] Write(string path, string command_line)
        std_result[void] Commit(string command_line)

        GraphTopology& topology()

        shared_ptr[CSchema] node_schema()
        shared_ptr[CSchema] edge_schema()

        vector[shared_ptr[CChunkedArray]] NodeProperties()
        vector[shared_ptr[CChunkedArray]] EdgeProperties()

        shared_ptr[CChunkedArray] NodeProperty(int i)
        shared_ptr[CChunkedArray] EdgeProperty(int i)

        std_result[void] AddNodeProperties(shared_ptr[CTable])
        std_result[void] AddEdgeProperties(shared_ptr[CTable])

        std_result[void] RemoveNodeProperty(int)
        std_result[void] RemoveEdgeProperty(int)
