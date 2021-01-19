import numpy as np
import numba.types
import pyarrow

from galois.loops import do_all, do_all_operator
from galois.property_graph import PropertyGraph
from galois.shmem import setActiveThreads
from .average_neighbor_degree import average_neighbor_degree


from enum import Enum


class Direction(Enum):
    IN = "in"
    OUT = "out"


def print_results(result_dict):
    """driver program to print results of dictionary"""
    for key, value in result_dict.items():
        print(key, ":", value)


def average_neighbor_sum(arrNid):
    """ function to get the sum of the average neighbor degree for nodes with degree k"""
    sum = 0

    for nid in arrNid:
        sum += average_neighbor_degree(nid)

    return sum


def fill_graph_inout_degree(graph: PropertyGraph, degree_array):

    """function that creates a dictionary of all the in/out degrees seen in graph g
    where key = degree and value = array of node_id's with that degree"""

    result_dict = dict()

    for node_id, deg in enumerate(degree_array):
        if deg in result_dict.keys():

            newvalue = result_dict.get(deg)

            newvalue.append(node_id)

            result_dict[deg] = newvalue

        else:

            nid_array = result_dict.setdefault(deg, [])

            newvalue.append(node_id)

    return result_dict


def average_degree_connectivity(graph: PropertyGraph, source, target, nodes, weight):
    """function that calculates the average degree connectivity of a graph g
    returns dictionary where key = degree and value = degree's average degree connectivity"""

    calculate_degree(
        graph,
        in_degree_property="in_degree_property",
        out_degree_property="out_degree_property",
        weight_property=weight,
    )

    if source == Direction.IN:
        degree_array = graph.get_node_property("in_degree_property")

    elif source == Direction.OUT:
        degree_array = graph.get_node_property("out_degree_property")

    degree_dict = fill_graph_inout_deg(graph, degree_array)

    # create map that will hold the results, where key = deg and value = avg_degconn
    result_dict = dict()

    for degree in degree_dict.keys():

        value = degree_dict.get(degree)

        avg_degree_connectivity = avg_n_sum(value) / len(value)

        result_dict[degree] = avg_degree_connectivity

    print_results(result_dict)
