import numpy as np
import numba.types
import pyarrow

from galois.loops import do_all, do_all_operator
from galois.property_graph import PropertyGraph
from galois.shmem import setActiveThreads
from .calculate_degree import calculate_degree


def print_results(result_array):
    """driver program to print results of array"""
    for node_id, avg_neighbor in enumerate(result_array):
        print(node_id, " : ", avg_neighbor)


@do_all_operator()
def sum_neighbor_degree(graph: PropertyGraph, result_array, deg_array, weight, weight_property, nid):
    """function that sums up the nonweighted or weighted degrees of the current node's neighbors
    map that will hold the results key = node id and value = avg_neighbor_deg"""
    sum_neighbor_degree = 0

    for edge in graph.edges(nid):
        # get destination node
        dst = graph.get_edge_dst(edge)
        deg_of_dst_node = deg_array[dst]
        if weight == None:
            sum_neighbor_degree += deg_of_dst_node

        else:
            weight_array = graph.get_edge_property(weight_property)
            weight_of_edge = weight_array[edge]
            sum_neighbor_degree += deg_of_dst_node * weight_of_edge

    avg_neighbor_degree = sum_neighbor_degree / deg_array[nid]
    result_array[nid] = avg_neighbor_degree


def helper(graph: PropertyGraph, deg_array, weight, weight_property):
    """helper function that fills array that will hold the results index = node id and value = avg_neighbor_deg"""

    num_nodes = graph.num_nodes()

    result_array = np.empty((num_nodes,), dtype=int)

    # for each node in graph G
    do_all(
        range(num_nodes),
        # taking out nid parameter
        sum_neighbor_degree(graph, result_array, deg_array, weight, weight_property),
        # I think I dont necessarily need this here
        steal=True,
    )

    return result_array


def average_neighbor_degree(graph: PropertyGraph, source, target, weight, weight_property=None):

    calculate_degree(
        graph,
        in_degree_property="in_degree_property",
        out_degree_property="out_degree_property",
        weight_property=weight,
    )

    if source == "in":
        deg_array = graph.get_node_property("in_degree_property")
    else:
        deg_array = graph.get_node_property("out_degree_property")

    result_array = helper(graph, deg_array, weight, weight_property)

    print_results(result_array)
