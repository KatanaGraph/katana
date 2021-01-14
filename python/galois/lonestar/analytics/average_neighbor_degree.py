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
@do_all_operator()
def sum_neighbor_degree(graph: PropertyGraph, nid, result_array, deg_array, weight):
    # fill in map that will hold the results key = node id and value = avg_neighbor_deg
    sum_neighbor_degree = 0
    #for edge connected to curr node: 
    for edge in graph.edges(nid):
        #get destination node 

        ##graph.get_edge_dst gives you the distance, not the node on the other end of the edge!!!
        ## HUGE ERROR
        dst = graph.get_edge_dst(edge)

        if weight == "None":
            sum_neighbor_degree += deg_array[nid]

        else: 
            sum_neighbor_degree += (deg_array[nid] * graph.getEdgeData(edge))

        #should I use steal = true here?

    avg_neighbor_degree= sum_neighbor_degree() / deg_array[nid]
    result_array[nid] = avg_neighbor_degree

#helper method that fills the result dictionary where key = nid and value = its average neighbor degree 
def helper(graph: PropertyGraph, deg_array, weight):
    # create map that will hold the results key = node id and value = avg_neighbor_deg
    #result_dict= dict()

    num_nodes = graph.num_nodes()

    result_array = [0] * num_nodes

    #for each node in graph G
    do_all(
        range(num_nodes),

        #taking out nid parameter
        sum_neighbor_degree(graph, result_array, deg_array, weight),

        #I think I dont necessarily need this here
        steal=True,
    )

    return result_array



def average_neighbor_degree(graph: PropertyGraph, source, target, weight):
   
    calculate_degree(graph, in_degree_property="in_degree_property", out_degree_property="out_degree_property", weight_property = weight)

    if source == "in" and target == "in":
        deg_array = graph.get_node_property("in_degree_property")
    else: 
        deg_array = graph.get_node_property("out_degree_property")

    result_array = helper(graph, deg_array, weight)

    for node_id, avg_neighbor in enumerate(result_array):
        print(node_id, ' : ', avg_neighbor)
   
