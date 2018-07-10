/*
 * This file belongs to the Galois project, a C++ library for exploiting parallelism.
 * The code is being released under the terms of the 3-Clause BSD License (a
 * copy is located in LICENSE.txt at the top-level directory).
 *
 * Copyright (C) 2018, The University of Texas at Austin. All rights reserved.
 * UNIVERSITY EXPRESSLY DISCLAIMS ANY AND ALL WARRANTIES CONCERNING THIS
 * SOFTWARE AND DOCUMENTATION, INCLUDING ANY WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR ANY PARTICULAR PURPOSE, NON-INFRINGEMENT AND WARRANTIES OF
 * PERFORMANCE, AND ANY WARRANTY THAT MIGHT OTHERWISE ARISE FROM COURSE OF
 * DEALING OR USAGE OF TRADE.  NO WARRANTY IS EITHER EXPRESS OR IMPLIED WITH
 * RESPECT TO THE USE OF THE SOFTWARE OR DOCUMENTATION. Under no circumstances
 * shall University be liable for incidental, special, indirect, direct or
 * consequential damages or loss of profits, interruption of business, or
 * related expenses which may arise from use of Software or Documentation,
 * including but not limited to those resulting from defects in Software and/or
 * Documentation, or loss or inaccuracy of data of any kind.
 */

/**
 * @file PythonGraph.h
 *
 * Contains declarations for the Galois runtime, functions to modify an
 * AttributedGraph, and graph simulation functions.
 */

#ifndef GALOIS_PYTHON_GRAPH_H
#define GALOIS_PYTHON_GRAPH_H
#include "../graphsimulation/GraphSimulation.h"

extern "C" {

////////////////////////////////////////////////////////////////////////////////
// API for Galois runtime
////////////////////////////////////////////////////////////////////////////////
/**
 * Galois runtime initialization. Must be called before running anything from
 * Galois libraries.
 */
void initGaloisRuntime();
/**
 * Set number of threads Galois will run with.
 *
 * @param numThreads New number of threads to run with
 */
void setNumThreads(int numThreads);
/**
 * Get currently set number of threads to run with.
 */
int getNumThreads();

/**
 * Creates a new AttributedGraph.
 * @returns pointer to newly created graph
 */
AttributedGraph* createGraph();
/**
 * Frees memory involved with an AttributedGraph
 * @param g AttributedGraph to free
 */
void deleteGraph(AttributedGraph* g);
/**
 * Serialize the AttributedGraph onto disk for later use.
 * @param g Graph to save
 * @param filename Name to save serialized graph on disk to
 */
void saveGraph(AttributedGraph* g, char* filename);
/**
 * Load an AttributedGraph from disk for use.
 * @param g Graph object to load into
 * @param filename Name of serialized graph on disk to load
 */
void loadGraph(AttributedGraph* g, char* filename);
/**
 * Prints out the data in an AttributedGraph for debugging purposes.
 * @param g Graph to print
 */
void printGraph(AttributedGraph* g);

/**
 * Allocate memory for the AttributedGraph.
 * @param g Graph to allocate memory for
 * @param numNodes Number of nodes to allocate memory for
 * @param numEdges Number of edges to allocate memory for
 * @param numNodeLabels Number of node labels to allocate memory for
 * @param numEdgesLabels Number of edge labels to allocate memory for
 */
void allocateGraph(AttributedGraph* g, size_t numNodes, size_t numEdges,
                   size_t numNodeLabels, size_t numEdgeLabels);
/**
 * Set the end edge for a particular node in the CSR representation.
 * @param g Graph to change
 * @param nodeIndex node to set the last edge of
 * @param edgeIndex edge ID of last edge belonging to the specified node
 */
void fixEndEdge(AttributedGraph* g, uint32_t nodeIndex, uint64_t edgeIndex);
/**
 * Set a node in the AttributedGraph. Graph memory should have been allocated
 * already.
 * @param g Graph to set node in
 * @param nodeIndex Node ID to set/change
 * @param uuid unique ID to node
 * @param label Label to give node
 * @param name Name to give node (e.g. name of a process)
 */
void setNode(AttributedGraph* g, uint32_t nodeIndex, uint32_t uuid,
             uint32_t label, char* name);
/**
 * Assign a node label string to a particular integer (for mapping purposes).
 *
 * @param g Graph to save mapping to
 * @param label Integer to map to a particular string
 * @param name String to be associated with the integer label
 */
void setNodeLabel(AttributedGraph* g, uint32_t label, char* name);
/**
 * Assign a edge label string to a particular integer (for mapping purposes).
 * @param g Graph to save mapping to
 * @param label Integer to map to a particular string
 * @param name String to be associated with the integer label
 */
void setEdgeLabel(AttributedGraph* g, uint32_t label, char* name);
/**
 * Label a node with a value for a particular attribute.
 * @param g Graph to label
 * @param nodeIndex node ID to give the attribute
 * @param key Attribute name
 * @param value Value of the attribute to give the node
 */
void setNodeAttribute(AttributedGraph* g, uint32_t nodeIndex, char* key,
                      char* value);
/**
 * Construct an edge in the AttributedGraph. Graph memory should have already
 * been allocated.
 * @param g Graph to construct edge in
 * @param edgeIndex edge ID to construct
 * @param dstNodeIndex destination of edge to construct (source is implicit
 * from edgeID)
 * @param label Label to give edge
 * @param timestamp Timestamp to give edge
 */
void constructEdge(AttributedGraph* g, uint64_t edgeIndex,
                   uint32_t dstNodeIndex, uint32_t label, uint64_t timestamp);
/**
 * Label an edge with a value for a particular attribute.
 * @param g Graph to label
 * @param edgeIndex edge ID to give the attribute
 * @param key Attribute name
 * @param value Value of the attribute to give the edge
 */
void setEdgeAttribute(AttributedGraph* g, uint32_t edgeIndex, char* key,
                      char* value);
/**
 * Gets the number of nodes in the graph.
 * @param g Graph to get nodes of
 * @returns Number of nodes in the graph
 */
size_t getNumNodes(AttributedGraph* g);
/**
 * Gets the number of edges in the graph.
 * @param g Graph to get edges of
 * @returns Number of edges in the graph
 */
size_t getNumEdges(AttributedGraph* g);

////////////////////////////////////////////////////////////////////////////////
// Graph simulation related calls
////////////////////////////////////////////////////////////////////////////////

// todo doxygen all of the things below

size_t runAttributedGraphSimulation(AttributedGraph* queryGraph,
                                    AttributedGraph* dataGraph,
                                    EventLimit limit, EventWindow window);

size_t findFilesWithMultipleWrites(AttributedGraph* dataGraph,
                                   EventWindow window);
size_t findProcessesWithReadFileWriteNetwork(AttributedGraph* dataGraph,
                                             EventWindow window);
size_t findProcessesWritingNetworkIndirectly(AttributedGraph* dataGraph,
                                             EventLimit limit,
                                             EventWindow window);
size_t findProcessesOriginatingFromNetwork(AttributedGraph* dataGraph,
                                           EventLimit limit,
                                           EventWindow window);
size_t findProcessesOriginatingFromNetworkIndirectly(AttributedGraph* dataGraph,
                                                     EventLimit limit,
                                                     EventWindow window);
size_t findProcessesExecutingModifiedFile(AttributedGraph* dataGraph,
                                          EventLimit limit, EventWindow window);

size_t processesReadFromFile(AttributedGraph* dataGraph, uint32_t file_uuid,
                             EventWindow window);
size_t processesWroteToFile(AttributedGraph* dataGraph, uint32_t file_uuid,
                            EventWindow window);
size_t processesReadFromNetwork(AttributedGraph* dataGraph,
                                uint32_t network_uuid, EventWindow window);
size_t processesWroteToNetwork(AttributedGraph* dataGraph,
                               uint32_t network_uuid, EventWindow window);
size_t processesReadFromRegistry(AttributedGraph* dataGraph,
                                 uint32_t registry_uuid, EventWindow window);
size_t processesWroteToRegistry(AttributedGraph* dataGraph,
                                uint32_t registry_uuid, EventWindow window);
size_t processesReadFromMemory(AttributedGraph* dataGraph, uint32_t memory_uuid,
                               EventWindow window);
size_t processesWroteToMemory(AttributedGraph* dataGraph, uint32_t memory_uuid,
                              EventWindow window);

size_t filesReadByProcess(AttributedGraph* dataGraph, uint32_t process_uuid,
                          EventWindow window);
size_t filesWrittenByProcess(AttributedGraph* dataGraph, uint32_t process_uuid,
                             EventWindow window);
size_t networksReadByProcess(AttributedGraph* dataGraph, uint32_t process_uuid,
                             EventWindow window);
size_t networksWrittenByProcess(AttributedGraph* dataGraph,
                                uint32_t process_uuid, EventWindow window);
size_t registriesReadByProcess(AttributedGraph* dataGraph,
                               uint32_t process_uuid, EventWindow window);
size_t registriesWrittenByProcess(AttributedGraph* dataGraph,
                                  uint32_t process_uuid, EventWindow window);
size_t memoriesReadByProcess(AttributedGraph* dataGraph, uint32_t process_uuid,
                             EventWindow window);
size_t memoriesWrittenByProcess(AttributedGraph* dataGraph,
                                uint32_t process_uuid, EventWindow window);

void reportGraphSimulation(AttributedGraph& queryGraph,
                           AttributedGraph& dataGraph, char* outputFile);

void returnMatchedNodes(AttributedGraph& graph, MatchedNode* matchedNodes);
void reportMatchedNodes(AttributedGraph& graph, char* outputFile);
void returnMatchedNeighbors(AttributedGraph& graph, uint32_t uuid,
                            MatchedNode* matchedNeighbors);
void reportMatchedNeighbors(AttributedGraph& graph, uint32_t uuid,
                            char* outputFile);
void returnMatchedEdges(AttributedGraph& graph, MatchedEdge* matchedEdges);
void reportMatchedEdges(AttributedGraph& graph, char* outputFile);
void returnMatchedNeighborEdges(AttributedGraph& graph, uint32_t uuid,
                                MatchedEdge* matchedEdges);
void reportMatchedNeighborEdges(AttributedGraph& graph, uint32_t uuid,
                                char* outputFile);
} // extern "C"

#endif // GALOIS_PYTHON_GRAPH_H
