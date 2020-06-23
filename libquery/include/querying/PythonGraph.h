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
void saveGraph(AttributedGraph* g, const char* filename);

/**
 * Load an AttributedGraph from disk for use.
 * @param g Graph object to load into
 * @param filename Name of serialized graph on disk to load
 */
void loadGraph(AttributedGraph* g, const char* filename);

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
 * Label a node with a value for a particular existing attribute.
 * @param g Graph to label
 * @param nodeIndex node ID to give the attribute
 * @param key Attribute name
 * @param value Value of the attribute to give the node
 */
void setExistingNodeAttribute(AttributedGraph* g, uint32_t nodeIndex,
                              const char* key, const char* value);

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
 * Label an edge with a value for a particular existing attribute.
 * @param g Graph to label
 * @param edgeIndex edge ID to give the attribute
 * @param key Attribute name
 * @param value Value of the attribute to give the edge
 */
void setExistingEdgeAttribute(AttributedGraph* g, uint32_t edgeIndex,
                              const char* key, const char* value);


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
 * Prints various graph statistics.
 * @param graph Graph to get stats on
 */
void reportGraphStats(AttributedGraph& graph);

} // extern "C"

#endif // GALOIS_PYTHON_GRAPH_H
