import numpy as np
import numba.types
import pyarrow

from galois.loops import do_all, do_all_operator
from galois.property_graph import PropertyGraph
from galois.shmem import setActiveThreads

#QUESTIONS: ask about correct way to pass parameters :) example: line 76
#TODO: fix indentation

#method to get the sum of the average neighbor degree for nodes with degree k 
def avg_n_sum(arrNid):
    sum=0

    for nid in arrNid:
        sum += average_neighbor_degree(nid)

    return sum

#method that creates a dictionary of all the out degrees seen in graph g
#where key = degree and value = array of node_id's with that degree 
def fill_graph_out_deg(graph):
    #get out degree ARRAY
    out_array = graph.get_node_property(outPop)
    out_dict = dict()

    for node_id, deg in enumerate(out_array):
        if deg in out_dict.keys():
            #update value array
            #get value array
            newvalue = out_dict.get(deg) 
            #add nid to value -----> am i allowed to use append? not with large arrays, but can use with 
            #python arrays
            newvalue.append(node_id)
            #add new value
            outDict[deg] = newvalue
        elif: 
            #add key and value 
            #create an array with no finite size
            nid_array = []
            #add nid to value array
            nid_array.append(node_id)
            out_dict[deg] = nid_array

    return out_dict

#method that creates a dictionary of all the in degrees seen in graph g
#where key = degree and value = array of node_id's with that degree 
def fill_graph_in_deg(graph):
    #get in degree array
    in_array = graph.get_node_property(inPop)
    in_dict = dict()

    for node_id, deg in enumerate(in_array): 
        if deg in in_dict:
            #update value array
            #get value 
            newvalue = in_dict.get(deg) 
            #add nid to value 
            newvalue.append(node_id)
            #add new value
            in_dict[deg] = newvalue

        elif: 
            #add key and value 
            #create an array with no finite size 
            nid_array = []
            #add nid to value array
            nid_array.append(node_id)
            in_dict[deg] = nid_array 

    return in_dict

#method that calculates the average degree connectivity of a graph g
#returns dictionary where key = degree and value = degree's average degree connectivity
@do_all_operator()
def get_avg_degconn(graph: PropertyGraph, source, target, nodes, weight):

    calculate_degree(graph: PropertyGraph, weight_propery=None)

    #ask about correct way to pass in parameters :)
    in_dict = fill_graph_in_deg(graph)
    out_dict = fill_graph_out_deg(graph)

    #create map that will hold the results 
    #where key = deg and value = avg_degconn
    result_dict = dict()

    if source = "in" and target = "in":

        for degree in in_dict.keys(): 

            #get value (array of NID's)
            value = inDict.get(degree)
            avg_degree_connectivity = avg_n_sum(value) / len(value)
            
            #add key value pair
            result_dict[degree] = avg_degree_connectivity

    elif source = "out" and target = "out":

        for degree in out_dict.keys(): 

            #get value (array of NID's)
            value = outDict.get(degree)
            avg_degree_connectivity = avg_n_sum(value) / len(value)

            #add key value pair
            result_dict[degree] = avg_degree_connectivity

    print() list(Dict)




