import numpy as np
import numba.types
import pyarrow

from galois.loops import do_all, do_all_operator
from galois.property_graph import PropertyGraph
from galois.shmem import setActiveThreads
from .deg_count import calculate_degree


#ALGORITHM SUMMARY: the algorithm returns the average_neighbor_degree of each 
#node in the graph
#We attain the average_neighbor_degree in two different ways, depending on 
#if the edges are weighted or not
#if the edges are NOT weighted, then we attain the average_neighbor_degree by
#summing the degrees of node k's neighbor and dividing that by the neighbors
#of node k
#if the edges ARE weighted, then we attain the average_neighbor_degree by 
#summing the degrees of node k's neighbors times the weight of the edge that
#respectively connects the two nodes, all of that divided by the weighted degree
#of node k 


#QUESTIONS: what is the legal way to get value from certain index of an array in pyarrow?


##method that sums up the nonweighted or weighted degrees of the current node's neighbors
@do_all_operator()
def sum_neighbor_degree(graph: PropertyGraph, result_array, deg_array, weight, weight_property, nid):
    # fill in map that will hold the results key = node id and value = avg_neighbor_deg
    sum_neighbor_degree = 0
    #for edge connected to curr node: 
    for edge in graph.edges(nid):
        #get destination node 
        dst = graph.get_edge_dst(edge)
        deg_of_dst_node = deg_array[dst]
        if weight == None:
            sum_neighbor_degree += deg_of_dst_node

        else: 
            weight_array = graph.get_edge_property(weight_property)
            weight_of_edge = weight_array[edge]
            sum_neighbor_degree += (deg_of_dst_node * weight_of_edge)

        #should I use steal = true here?

    avg_neighbor_degree= sum_neighbor_degree / deg_array[nid]
    result_array[nid] = avg_neighbor_degree

#helper method that fills the result dictionary where key = nid and value = its average neighbor degree 
def helper(graph: PropertyGraph, deg_array, weight, weight_property):
    # create an array that will hold the results index = node id and value = avg_neighbor_deg
    #result_dict= dict()

    num_nodes = graph.num_nodes()

    result_array = np.empty((num_nodes,), dtype=int)

    #for each node in graph G
    do_all(
        range(num_nodes),
        #taking out nid parameter
        sum_neighbor_degree(graph, result_array, deg_array, weight, weight_property),

        #I think I dont necessarily need this here
        steal=True,
    )

    return result_array



def average_neighbor_degree(graph: PropertyGraph, source, target, weight, weight_property=None):
   
    calculate_degree(graph, in_degree_property="in_degree_property", out_degree_property="out_degree_property", weight_property = weight)

    if source == "in" and target == "in":
        deg_array = graph.get_node_property("in_degree_property")
    else: 
        deg_array = graph.get_node_property("out_degree_property")

    result_array = helper(graph, deg_array, weight, weight_property)

    for node_id, avg_neighbor in enumerate(result_array):
        print(node_id, ' : ', avg_neighbor)
   
