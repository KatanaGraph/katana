import numpy as np
import numba.types
import pyarrow

from galois.loops import do_all, do_all_operator
from galois.property_graph import PropertyGraph
from galois.shmem import setActiveThreads
from .average_neighbor_degree import average_neighbor_degree

#SUMMARY OF THE ALGORITHM: 
##the average degree connectivity of a degree in graph g is the sum 
#of the average neighbor degree for nodes with degree k divided by 
#the number of nodes with that degree

#Thus the first step is to create two dictionaries that will hold all
#of the in/out degrees seen in graph g these are name in_dict and out_dict
#where key = degree and value = array of node_id's with that degree 

#Second step is to have a method that calculate the sume of the average
#neighbor degree for nodes with degree k

#Thus when we call get_avg_degconn, we can use our in_dict and out_dict 
#to go through every degree in the dictionary and calculate the 
#average degree connectivity for degree k by calling 
# avg_n_sum / value of key (degree) which the number of nodes with that degree
#We store this in a result dictionary where
#key = degree and value = degree's average degree connectivity


#QUESTIONS: ask about correct way to pass parameters :) example: line 76
#TODO: fix indentation

#method to get the sum of the average neighbor degree for nodes with degree k 
def avg_n_sum(arrNid):
    sum = 0

    for nid in arrNid:
        sum += average_neighbor_degree(nid)

    return sum

#method that creates a dictionary of all the out degrees seen in graph g
#where key = degree and value = array of node_id's with that degree 
def fill_graph_out_deg(graph: PropertyGraph):
    #get out degree ARRAY
    out_array = graph.get_node_property("out_degree_property")
    out_dict = dict()

    for node_id, deg in enumerate(out_array):
        if deg in out_dict.keys():
            #update value array
            #but first get value
            newvalue = out_dict.get(deg) 
            #add nid to new value
            newvalue.append(node_id)
            #set new value as this degrees value
            outDict[deg] = newvalue
        else: 
            #add key and value 
            #create an array with no finite size
            nid_array = []
            #add nid to value array
            nid_array.append(node_id)
            #set nid array as this degrees value
            out_dict[deg] = nid_array

    return out_dict

#method that creates a dictionary of all the in degrees seen in graph g
#where key = degree and value = array of node_id's with that degree 
def fill_graph_in_deg(graph: PropertyGraph):
    #get in degree array
    in_array = graph.get_node_property("in_degree_property")
    in_dict = dict()

    for node_id, deg in enumerate(in_array): 
        if deg in in_dict.keys():
            #update value array
            #but first get value 
            newvalue = in_dict.get(deg) 
            #add nid to new value 
            newvalue.append(node_id)
            #set new value as this degrees value
            in_dict[deg] = newvalue

        else: 
            #add key and value 
            #create an array with no finite size 
            nid_array = []
            #add nid to value array
            nid_array.append(node_id)
            #set nid array as this degrees value
            in_dict[deg] = nid_array 

    return in_dict

#method that calculates the average degree connectivity of a graph g
#returns dictionary where key = degree and value = degree's average degree connectivity
def get_avg_degconn(graph: PropertyGraph, source, target, nodes, weight):

    calculate_degree(graph, in_degree_property="in_degree_property", out_degree_property="out_degree_property", weight_property= weight)

    #ask about correct way to pass in parameters :)
    in_dict = fill_graph_in_deg(graph)
    out_dict = fill_graph_out_deg(graph)

    #create map that will hold the results 
    #where key = deg and value = avg_degconn
    result_dict = dict()

    if source == "in" and target == "in":

        for degree in in_dict.keys(): 

            #get value (array of NID's) that have that degree
            value = in_dict.get(degree)

            #the average degree connectivity of a degree in graph g is the sum 
            #of the average neighbor degree for nodes with degree k divided by 
            #the number of nodes with that degree
            avg_degree_connectivity = avg_n_sum(value) / len(value)
            
            #add key value pair
            result_dict[degree] = avg_degree_connectivity

    elif source == "out" and target == "out":

        for degree in out_dict.keys(): 

            #get value (array of NID's)
            value = outDict.get(degree)
            avg_degree_connectivity = avg_n_sum(value) / len(value)

            #add key value pair
            result_dict[degree] = avg_degree_connectivity

    print(result_dict)




