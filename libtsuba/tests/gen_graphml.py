#!/usr/bin/env python3

import typing, random, sys, getopt, pprint

head = '''<?xml version="1.0" encoding="UTF-8"?>
<graphml xmlns="http://graphml.graphdrawing.org/xmlns"  
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://graphml.graphdrawing.org/xmlns
     http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd">
'''

edge_properties = '''<key id="str" for="edge" attr.name="str" attr.type="string"/>
  <graph id="G" edgedefault="undirected">
'''

tail = '''</graph>
</graphml>
'''
numNodeProperties = 10
numNodes = 10_000
numEdges = 100_000

def print_node_properties(numProperties: int) :
    for i in range(numProperties) :
        print(f'<key id="p{i:02}" for="node" attr.name="p{i:02}" attr.type="long"/>')

def print_nodes(numNodes: int) :
    """
    Print num nodes, each with age property in range [Start, Stop] such that all sum to 0
    """
    Start = -1_000_000
    Stop  =  1_000_000
    randInt = [[random.randrange(Start, Stop+1) for iter in range(numNodes-1)]
               for i in range(numNodeProperties)]
    for p in range(numNodeProperties) :
        randInt[p].append( 0 - sum(randInt[p]) )

    for i in range(numNodes) :
        print(f'<node id="n{i:04}">')
        for p in range(numNodeProperties) :
            print(f'<data key="p{p:02}">{randInt[p][i]}</data>')
        print('</node>')

def print_edges(numNodes: int, numEdges: int) :
    """
    Print edges between numNodes nodes, each with a str property of length [0,100] of a's (positive) or b's (negative) that sum to 0
    """
    Start = -100
    Stop  =  100

    randInt = [random.randrange(Start, Stop+1) for iter in range(numEdges-1)]
    randInt.append( 0 - sum(randInt) )

    for i in range(len(randInt)) :
        nodeCycle = int(i / numNodes) + 1
        print(f'<edge id="e{i:04}" source="n{i%numNodes:04}" target="n{(i+nodeCycle)%numNodes:04}">')
        if(randInt[i] == 0) :
            print('<data key="str"></data> </edge>')
        elif(randInt[i] > 0) :
          print(f'<data key="str">{"a" * randInt[i]}</data> </edge>')
        else :
          print(f'<data key="str">{"b" * -randInt[i]}</data> </edge>')

          ######################################################################
## Parse options and usage
def usage():
    print("./gen_graph.py [-h] [-n numNodes] [-e numEdges] [-p numNodeProperties]")
    print("-n (node) is the number of nodes in the graph (default 1,000)")
    print("-p (node_prop) is the number of properties for each node (default 10)")    
    print("-e (edge) is the number of edges (default 10,000, less than nunNodes @@ 2")
    print("-h is for help")

def parse_args(args):
    global numNodes, numEdges, numNodeProperties

    try:
        opts, pos_args = getopt.getopt(sys.argv[1:], "he:n:p:", ["help", "edge=", "node=", "node_prop="])
    except getopt.GetoptError:
        # print help information and exit:
        usage()
        sys.exit(2)
    opt_update = False
    for o, a in opts:
        if o in ("-e", "--edge"):
            numEdges = int(a)
        elif o in ("-n", "--node"):
            numNodes = int(a)
        elif o in ("-p", "--node_prop"):
            numNodeProperties = int(a)
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        else :
            print("Option error (%s)" % o)
            usage()
            sys.exit()
    return pos_args
            
if __name__ == '__main__':
    pos_args = parse_args(sys.argv[1:])
    assert numEdges <= (numNodes * numNodes), "At most numNode*numNode edges"
    print(head)
    print_node_properties(numNodeProperties)
    print(edge_properties)
    print_nodes(numNodes)
    print_edges(numNodes, numEdges)
    print(tail)
