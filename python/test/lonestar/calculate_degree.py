import numpy as np
import pyarrow

from katana import do_all, do_all_operator
from katana.local import Graph
from katana.local.atomic import atomic_add
from katana.local.datastructures import AllocationPolicy, NUMAArray


@do_all_operator()
def initialize_in_degree(nin, nid):
    nin[nid] = 0


@do_all_operator()
def count_in_and_out_degree(graph: Graph, nout, nin, nid):
    out_degree = 0
    for edge in graph.edge_ids(nid):
        out_degree += 1
        dst = graph.get_edge_dest(edge)
        atomic_add(nin, dst, 1)
    nout[nid] = out_degree


@do_all_operator()
def count_weighted_in_and_out_degree(graph: Graph, nout, nin, weight_array, nid):
    out_degree = 0
    for edge in graph.edge_ids(nid):
        weight = weight_array[edge]
        out_degree += weight
        dst = graph.get_edge_dest(edge)
        atomic_add(nin, dst, weight)
    nout[nid] = out_degree


def calculate_degree(graph: Graph, in_degree_property, out_degree_property, weight_property=None):
    """
    Calculate the (potentially weighted) in and out degrees of a graph.
    The function will modify the given graph by adding two new node properties,
    one for the in degree and one for the out degree. Nothing is returned.
    Parameters:
        graph: a Graph
        in_degree_property: the property name for the in degree
        out_degree_property: the property name for the out degree
        weight_property: an edge property to use in calculating the weighted degree
    """
    num_nodes = graph.num_nodes()
    nout = NUMAArray[np.uint64](num_nodes, AllocationPolicy.INTERLEAVED)
    nin = NUMAArray[np.uint64](num_nodes, AllocationPolicy.INTERLEAVED)

    do_all(range(num_nodes), initialize_in_degree(nin.as_numpy()), steal=False)

    # are we calculating weighted degree?
    if not weight_property:
        count_operator = count_in_and_out_degree(graph, nout.as_numpy(), nin.as_numpy())
    else:
        count_operator = count_weighted_in_and_out_degree(
            graph, nout.as_numpy(), nin.as_numpy(), graph.get_edge_property(weight_property)
        )
    do_all(range(num_nodes), count_operator, steal=True)

    graph.add_node_property(pyarrow.table({in_degree_property: nin, out_degree_property: nout}))
