# distutils: extra_compile_args=["-std=c++17"]

from libcpp.string cimport string
from ..Galois cimport MethodFlag, NoDerefIterator, StandardRange
from libcpp.memory cimport shared_ptr
from libc.stdint cimport *
from ....cpp.libstd.boost cimport *
from cython.operator cimport dereference as df
from pyarrow.lib cimport *

# Fake types to work around Cython's lack of support
# for non-type template parameters.
cdef extern from *:
    cppclass dummy_true "true"
    cppclass dummy_false "false"

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
        std_result[shared_ptr[PropertyFileGraph]] Make(string filename)
        @staticmethod
        std_result[shared_ptr[PropertyFileGraph]] MakeWithProperties "Make" (string filename, vector[string] node_properties, vector[string] edge_properties)

        std_result[void] Write(string path)
        std_result[void] Write()

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
