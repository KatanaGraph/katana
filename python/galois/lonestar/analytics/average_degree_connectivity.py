import numpy as np
import numba.types
import pyarrow

from galois.loops import do_all, do_all_operator
from galois.property_graph import PropertyGraph
from galois.shmem import setActiveThreads


#method is correct
def avg_n_sum(arrNid)
    sum:0

    for nid in arrNid:
        sum += average_neighbor_degree(nid)

    returns sum


def fill_graph_out_deg(graph: PropertyGraph) 
    #get out degree table 

    outDict = dict()

    for node_id, deg in enumerate(outdeg table of graph):
        if deg in outDict:
            #update value array
            #get value 
            newvalue = outDict.get(get) 
            #add nid to value -----> am i allowed to use append?
            newvalue.append(node_id)
            #add new value
            outDict[deg] = newvalue
        else: 
            #add key and value 
            #create an array with no finite size -----> can I do that?
            nid_array = LargeArray[float](num_nodes, AllocationPolicy.INTERLEAVED)
            #add nid to value array
            nid_array.append(node_id)
            outDict[deg] = value array

    returns outDict

def fill_graph_in_deg(graph: PropertyGraph)
#get in degree table
inDict = dict()

for node_id, deg in enumerate(indeg table of graph): 
        if deg in inDict:
            #update value array
            #get value 
            newvalue = inDict.get(deg) 
            #add nid to value -----> am i allowed to use append?
            newvalue.append(node_id)
            #add new value
            inDict[deg] = newvalue

        else: 
            #add key and value 
            #create an array with no finite size -----> can I do that?
            nid_array = LargeArray[float](num_nodes, AllocationPolicy.INTERLEAVED)
            #add nid to value array
            nid_array.append(node_id)
            inDict[deg] = value array 

returns inDict

@do_all_operator
def get_avg_degconn(Graph: PropertyGraph, source, target, nodes, weight)

    calculate_degree(graph: PropertyGraph, weight_propery=None)
    inDict: fill_graph_in_deg(graph: PropertyGraph)
    outDict: fill_graph_out_deg(graph: PropertyGraph)

    #create map that will hold the results key = deg and value = avg_degconn
    Dict = dict()

    if(source = "in" AND target == "in"):

        for degree in inDict: 

            #get value (array of NID's)
            value: inDict.get(degree)
            avg_degree_connectivity = avg_n_sum(value) / len(value)

            Dict[degree] = avg_degree_connectivity

    else(source = "out" AND target == "out"):

        for degree in inDict: 

            #get value (array of NID's)
            value: outDict.get(degree)
            avg_degree_connectivity = avg_n_sum(value) / len(value)

            Dict[degree] = avg_degree_connectivity

    print list(Dict)




