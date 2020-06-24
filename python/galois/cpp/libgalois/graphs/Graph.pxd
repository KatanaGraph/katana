# distutils: extra_compile_args=["-std=c++17"]

from libcpp.string cimport string
from ..Galois cimport MethodFlag, iterable, NoDerefIterator

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

        iterable[NoDerefIterator[edge_iterator]] edges(GraphNode)
        iterable[NoDerefIterator[edge_iterator]] edges(unsigned long)
        iterable[NoDerefIterator[edge_iterator]] edges(GraphNode, MethodFlag)
        iterable[NoDerefIterator[edge_iterator]] edges(unsigned long, MethodFlag)

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
