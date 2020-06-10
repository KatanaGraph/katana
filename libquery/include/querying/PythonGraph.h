/*
 * This file belongs to the Galois project, a C++ library for exploiting
 * parallelism. The code is being released under the terms of the 3-Clause BSD
 * License (a copy is located in LICENSE.txt at the top-level directory).
 *
 * Copyright (C) 2020, The University of Texas at Austin. All rights reserved.
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
 *
 * @todo if we're not using cython anymore to interface, may be worth
 * cleaning up code to use namespaces and the like
 */

#ifndef GALOIS_PYTHON_GRAPH_H
#define GALOIS_PYTHON_GRAPH_H
#include "querying/GraphSimulation.h"

/**
 * Gets position of rightmost set bit from 0 to 31.
 * @param n number to get rightmost set bit of
 * @returns position of rightmost set bit, zero-indexed
 */
unsigned rightmostSetBitPos(uint32_t n);

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
 * Outputs an edge list from the attributed graph.
 * Ignores vertex labels and only chooses a single label from the
 * edge to output. Also ignores vertex/edge attributes.
 */
void saveEdgeList(AttributedGraph* g, char* filename);

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

//! Same as allocateGraph except it doesn't allocate memory for unused
//! uuid/names metadata + initializes attribute types
void allocateGraphLDBC(AttributedGraph* g, size_t numNodes, size_t numEdges,
                       size_t numNodeLabels, size_t numEdgeLabels);

/**
 * Set the end edge for a particular node in the CSR representation.
 * @param g Graph to change
 * @param nodeIndex node to set the last edge of
 * @param edgeIndex edge ID of last edge belonging to the specified node
 */
void fixEndEdge(AttributedGraph* g, uint32_t nodeIndex, uint64_t edgeIndex);

/**
 * Set a new node in the AttributedGraph with ONE label specified with a bit
 * position. Graph memory should have been allocated already.
 *
 * @param g Graph to set node in
 * @param nodeIndex Node ID to set/change
 * @param uuid unique ID to node
 * @param labelBitPosition bit position to set label of
 * @param name Name to give node (e.g. name of a process)
 */
void setNewNode(AttributedGraph* g, uint32_t nodeIndex, char* uuid,
                uint32_t labelBitPosition, char* name);

/**
 * Set a node in the AttributedGraph. Graph memory should have been allocated
 * already.
 * @param g Graph to set node in
 * @param nodeIndex Node ID to set/change
 * @param uuid unique ID to node
 * @param label Label to give node
 * @param name Name to give node (e.g. name of a process)
 */
void setNode(AttributedGraph* g, uint32_t nodeIndex, char* uuid, uint32_t label,
             char* name);

/**
 * Assign a node label to a node.
 *
 * @param g Graph to save mapping to
 * @param nodeIndex Node to save mapping to
 * @param label Label to give node
 */
void setNodeLabel(AttributedGraph* g, uint32_t nodeIndex, uint32_t label);

/**
 * Assign a node label string to a particular bit position (for mapping
 * purposes).
 *
 * @param g Graph to save mapping to
 * @param labelBitPosition Bit position to map to a particular string
 * @param name String to be associated with the integer label
 */
void setNodeLabelMetadata(AttributedGraph* g, uint32_t labelBitPosition,
                          const char* name);
/**
 * Assign a edge label string to a particular bit position (for mapping
 * purposes).
 * @param g Graph to save mapping to
 * @param labelBitPosition Bit position to map to a particular string
 * @param name String to be associated with the integer label
 */
void setEdgeLabelMetadata(AttributedGraph* g, uint32_t labelBitPosition,
                          const char* name);

/**
 * Label a node with a value for a particular attribute.
 * @param g Graph to label
 * @param nodeIndex node ID to give the attribute
 * @param key Attribute name
 * @param value Value of the attribute to give the node
 */
void setNodeAttribute(AttributedGraph* g, uint32_t nodeIndex, const char* key,
                      const char* value);

/**
 * Construct an edge in the AttributedGraph for the first time, i.e. it only
 * has a SINGLE edge label specified by bit position to set.
 * Graph memory should have already been allocated.
 *
 * @param g Graph to construct edge in
 * @param edgeIndex edge ID to construct
 * @param dstNodeIndex destination of edge to construct (source is implicit
 * from edgeID)
 * @param labelBitPosition Bit position to set on label of the edge
 * @param timestamp Timestamp to give edge
 */
void constructNewEdge(AttributedGraph* g, uint64_t edgeIndex,
                      uint32_t dstNodeIndex, uint32_t labelBitPosition,
                      uint64_t timestamp);

/**
 * Construct an edge in the AttributedGraph using an existing label (i.e.
 * label is set directly as passed in).
 * Graph memory should have already been allocated.
 *
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

///////
// New Functions Added for Incremental Graph Construction
///////

/**
 * Node label add that returns the label for a string if it already
 * exists/a correct label for the string.
 * @param g Graph to save mapping to
 * @param name String to be associated with the integer label
 * @returns Bit position that string is ultimately mapped to
 */
uint32_t addNodeLabelMetadata(AttributedGraph* g, char* name);

/**
 * Edge label add that returns the label for a string if it already
 * exists/a correct label for the string.
 * @param g Graph to save mapping to
 * @param name String to be associated with the integer label
 * @returns Bit position that string is ultimately mapped to
 */
uint32_t addEdgeLabelMetadata(AttributedGraph* g, char* name);

/**
 * Resizes existing node attribute vectors to a new size.
 * @param g Graph to change
 * @param nodeCount new size of node attribute vectors. Should be larger than
 * current size
 */
void resizeNodeAttributeMap(AttributedGraph* g, uint32_t nodeCount);

/**
 * Add a new node attribute map with a particular size. Does nothing if key
 * already exists (assumption is resizeNodeAttributeMap will be called before
 * this function).
 * @param g Graph to change
 * @param key Attribute name
 * @param nodeCount size of map
 */
void addNodeAttributeMap(AttributedGraph* g, const char* key,
                         uint32_t nodeCount);
/**
 * Designates some node attribute as having some type
 *
 * @param g Graph to change
 * @param key Node attribute name
 * @param t Type to designate
 */
void addNodeAttributeType(AttributedGraph* g, const char* key,
                          AttributedType t);

/**
 * Add a new edge attribute map with a particular size. Does nothing if key
 * already exists .
 *
 * @param g Graph to change
 * @param key Attribute name
 * @param edgeCount size of map
 */
void addEdgeAttributeMap(AttributedGraph* g, const char* key,
                         uint32_t edgeCount);

/**
 * Designates some edge attribute as having some type
 *
 * @param g Graph to change
 * @param key Edge attribute name
 * @param t Type to designate
 */
void addEdgeAttributeType(AttributedGraph* g, const char* key,
                          AttributedType t);

/**
 * Resizes the node maps in an attributed graph.
 * @param g Graph to change
 * @param nodeCount Size to change to. Should be at least as big as the
 * original size of the map.
 */
void resizeNodeMetadata(AttributedGraph* g, uint32_t nodeCount);

/**
 * Checks if a node with a particular uuid exists in the graph
 * @param g Graph to check
 * @param uuid UUID to check existince of
 * @returns 1 if it exists in the graph, 0 otherwise
 */
uint32_t nodeExists(AttributedGraph* g, char* uuid);

/**
 * Set a node in the AttributedGraph ONLY for the CSR for a SINGLE label
 * by specifying bit position to set; do not update any
 * metadata maps of the graph. Graph memory should have been allocated
 * already.
 * @param g Graph to set node in
 * @param nodeIndex Node index to set/change
 * @param uuid unique ID of node
 * @param labelBitPosition Bit position to set on label of the node
 */
void setNewNodeCSR(AttributedGraph* g, uint32_t nodeIndex, char* uuid,
                   uint32_t labelBitPosition);

/**
 * Set a node in the AttributedGraph ONLY for the CSR; do not update any
 * metadata maps of the graph. Graph memory should have been allocated
 * already.
 * @param g Graph to set node in
 * @param nodeIndex Node index to set/change
 * @param uuid unique ID of node
 * @param label Label to give node
 */
void setNodeCSR(AttributedGraph* g, uint32_t nodeIndex, char* uuid,
                uint32_t label);

/**
 * Set a node in the AttributedGraph ONLY for the metadata (node indices and
 * node name); do not touch the LC CSR representation of the graph.
 * @param g Graph to set node in
 * @param nodeIndex Node index to set/change
 * @param uuid unique ID of node
 * @param name Name to give node (e.g. name of a process)
 */
void setNodeMetadata(AttributedGraph* g, uint32_t nodeIndex, char* uuid,
                     char* name);

/**
 * Get the index of a node from its UUID (assumes uuid is valid)
 * @param g Graph to check
 * @param uuid unique ID of node
 * @returns Node index of uuid
 */
uint32_t getIndexFromUUID(AttributedGraph* g, char* uuid);

/**
 * Get the UUID of a node from its node index. Assumes that the nodeIndex is
 * valid (i.e. if not, program will crash or return an invalid value).
 * @param g Graph to check
 * @param nodeIndex node index to get UUID of
 */
const char* getUUIDFromIndex(AttributedGraph* g, uint32_t nodeIndex);

/**
 * Get the node label of a particular node. Calling end will have to interpret
 * the set bits.
 * @param g Graph to check
 * @param nodeIndex Node index to check
 * @returns Node label on node at provided index. Note that the calling end
 * will have to interpret the set bits.
 */
uint32_t getNodeLabel(AttributedGraph* g, uint32_t nodeIndex);

/**
 * Copy all the edges of a certain node from a source CSR graph to the dest CSR
 * graph in the AttributedGraphs. Also copies edge attribute data to the dest
 * AttGraph.
 * @param destGraph graph to copy to
 * @param srcGraph graph to copy from
 * @param nodeIndex index of the node to copy from srcGraph
 * @param edgeIndex index to start copying to in the destGraph
 */
uint64_t copyEdgesOfNode(AttributedGraph* destGraph, AttributedGraph* srcGraph,
                         uint32_t nodeIndex, uint64_t edgeIndex);

/**
 * Swaps the inner CSRs of 2 AttributedGraphs.
 * @param g1 graph 1
 * @param g2 graph 2
 */
void swapCSR(AttributedGraph* g1, AttributedGraph* g2);

/**
 * Swap the edge attributes map of 2 AttributedGraphs
 * @param g1 graph 1
 * @param g2 graph 2
 */
void swapEdgeAttributes(AttributedGraph* g1, AttributedGraph* g2);

/**
 * Adds a new label to the node at the specified index.
 *
 * @param g Graph to alter
 * @param nodeIndex Node index to set/change
 * @param labelBitPosition Bit position to add on label of the node
 */
void addNewLabel(AttributedGraph* g, uint32_t nodeIndex,
                 uint32_t labelBitPosition);

/**
 * Merge provided label with existing label at specified node index.
 *
 * @param g Graph to alter
 * @param nodeIndex Node index to set/change
 * @param labelToMerge Label to "or" with existing label
 */
void mergeLabels(AttributedGraph* g, uint32_t nodeIndex, uint32_t labelToMerge);

////////////////////////////////////////
// Removal Calls/Helpers
////////////////////////////////////////

/**
 * Changes the matched field on all nodes and all edges to 0.
 */
void unmatchAll(AttributedGraph* g);

/**
 * Mark an edge as dead (assumes not dead has a marking of 0).
 *
 * @param g Graph to alter
 * @param srcUUID ID of source
 * @param dstUUID ID of destination
 * @param labelBitPosition Bit position of label on edge
 * @param timestamp Timestamp to match on edge
 */
uint64_t killEdge(AttributedGraph* g, char* srcUUID, char* dstUUID,
                  uint32_t labelBitPosition, uint64_t timestamp);

/**
 * Given a graph with nodes/edges that need to be removed marked, compress the
 * graph by removing such nodes/edges and revising graph metadata accordingly.
 *
 * @param g Graph to compress
 * @param nodesRemoved Number of nodes that need to be removed
 * @param edgesRemoved Number of edges that need to be removed
 */
AttributedGraph* compressGraph(AttributedGraph* g, uint32_t nodesRemoved,
                               uint64_t edgesRemoved);

/**
 * Mark node as if all edges are dead (assumes not dead has a marking of 0).
 *
 * @param g Graph to alter
 */
uint32_t nodeRemovalPass(AttributedGraph* g);

////////////////////////////////////////////////////////////////////////////////
// Graph simulation related calls
////////////////////////////////////////////////////////////////////////////////

// TODO doxygen all of the things below

size_t matchCypherQuery(AttributedGraph* dataGraph, EventLimit limit,
                        EventWindow window, const char* cypherQueryStr,
                        bool useGraphSimulation);

size_t matchQuery(AttributedGraph* dataGraph, EventLimit limit,
                  EventWindow window, MatchedEdge* queryEdges,
                  size_t numQueryEdges, const char** filters,
                  bool useGraphSimulation);

/**
 * Wrapper call to graph simulation call on LC_CSR Graph.
 *
 * @todo doxygen limit and window
 * @param queryGraph pattern graph to match
 * @param dataGraph graph to match pattern to
 * @returns Number of matched edges from graph simulation.
 */
size_t runAttributedGraphSimulation(AttributedGraph* queryGraph,
                                    AttributedGraph* dataGraph,
                                    EventLimit limit, EventWindow window);

void reportGraphSimulation(AttributedGraph& queryGraph,
                           AttributedGraph& dataGraph, char* outputFile);

void returnMatchedNodes(AttributedGraph& graph, MatchedNode* matchedNodes);
void reportMatchedNodes(AttributedGraph& graph, char* outputFile);
void returnMatchedNeighbors(AttributedGraph& graph, char* uuid,
                            MatchedNode* matchedNeighbors);
void reportMatchedNeighbors(AttributedGraph& graph, char* uuid,
                            char* outputFile);
void returnMatchedEdges(AttributedGraph& graph, MatchedEdge* matchedEdges);
void reportMatchedEdges(AttributedGraph& graph, char* outputFile);
void returnMatchedNeighborEdges(AttributedGraph& graph, char* uuid,
                                MatchedEdge* matchedEdges);
void reportMatchedNeighborEdges(AttributedGraph& graph, char* uuid,
                                char* outputFile);

/**
 * Prints various graph statistics.
 * @param graph Graph to get stats on
 */
void reportGraphStats(AttributedGraph& graph);

} // extern "C"

#endif // GALOIS_PYTHON_GRAPH_H
