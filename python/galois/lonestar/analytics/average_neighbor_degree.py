import numpy as np
import numba.types
import pyarrow

from galois.loops import do_all, do_all_operator
from galois.property_graph import PropertyGraph
from galois.shmem import setActiveThreads
from .deg_count import calculate_degree

#QUESTIONS: what is the legal way to get value from certain index of an array in pyarrow?
#           what is the legal way to pass parameters? 


##method that sums up the nonweighted or weighted degrees of the current node's neighbors
def sum_neighbor_degree(graph, nid, result_dict, deg_array, weight):
    # create map that will hold the results key = node id and value = avg_neighbor_deg
    sum_neighbor_degree = 0
    #for edge connected to curr node: 
    for edge in graph.edges(nid):
        #get destination node 
        dst = graph.get_edge_dst(edge)

        if weight == "None":
            sum_neighbor_degree += deg_array[nid]

        else: 
            sum_neighbor_degree += (deg_array[nid] * getEdgeData(edge))

    avg_neighbor_degree: sum_neighbor_degree() / deg_array[nid]
    result_dict[nid] = avg_neighbor_degree

#helper method that fills the result dictionary where key = nid and value = its average neighbor degree 
@do_all_operator()
def helper(graph, nid, deg_array, weight):
    # create map that will hold the results key = node id and value = avg_neighbor_deg
    result_dict: dict()
    num_nodes = graph.num_nodes()
    #for each node in graph G
    do_all(
        range(num_nodes),

        sum_neighbor_deg(graph, nid, result_dict, deg_array, weight),

        steal=True,
    )

    return resultDict



def average_neighbor_degree(graph: PropertyGraph, source, target, weight):
   
    calculate_degree(graph, in_degree_property, out_degree_property, weight_property = weight)

    if source == "in" and target == "in":
        deg_array = graph.get_node_property(inPop)
    else: 
        deg_array = graph.get_node_property(outPop)

    print(list(helper(graph, deg_array, weight)))