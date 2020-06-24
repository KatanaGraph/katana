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

#ifndef GALOIS_CYPHER_GRAPH
#define GALOIS_CYPHER_GRAPH

#include "galois/graphs/QueryGraph.h"
#include "querying/GraphSimulation.h"

// TODO find a better place to put this
/**
 * Gets position of rightmost set bit from 0 to 31.
 * @param n number to get rightmost set bit of
 * @returns position of rightmost set bit, zero-indexed
 */
unsigned rightmostSetBitPos(uint32_t n);

enum AttributedType {
  AT_INT32,
  AT_STRING,
  AT_LONGSTRING,
  AT_DATE,
  AT_DATETIME,
  AT_TEXT,
  AT_STRINGARRAY,
  AT_LONGSTRINGARRAY
};

namespace galois::graphs {

/**
 * Wrapped graph that contains metadata maps explaining what the compressed
 * data stored in the graph proper mean. For example, instead of storing
 * node types directly on the Graph, an int (which maps to a node type)
 * is stored on the node data.
 */
class AttributedGraph {
  // TODO privatize some of these vars
public:
  //! Graph structure class
  QueryGraph graph;

  // Note the below structures are not provided by property file graph as it is
  // not aware bit positions on labels being used as indicators of what
  // a node/edge is
  //! Given a node label i, get its name
  std::vector<std::string> nodeLabelNames;
  //! Given a node name, get its numerical index i
  std::unordered_map<std::string, uint32_t> nodeLabelIDs;
  //! Given an edge label i, get its name
  std::vector<std::string> edgeLabelNames;
  //! Given an edge name, get its numerical index i
  std::unordered_map<std::string, uint32_t> edgeLabelIDs;

  // TODO below structures may not actually need to exist
  //! maps node UUID/ID to index/GraphNode
  std::unordered_map<std::string, uint32_t> nodeIndices;
  //! maps node index to UUID
  std::vector<std::string> index2UUID;
  //! actual names of nodes
  std::vector<std::string> nodeNames; // cannot use LargeArray because serialize
                                      // does not do deep-copy

  // custom attributes: maps from an attribute name to a vector that contains
  // the attribute for each node/edge
  //! attribute name (example: file) to vector of names for that attribute
  std::unordered_map<std::string, std::vector<std::string>> nodeAttributes;
  //! type for a node attribute
  std::unordered_map<std::string, AttributedType> nodeAttributeTypes;
  //! edge attribute name to vector of names for that attribute
  std::unordered_map<std::string, std::vector<std::string>> edgeAttributes;
  //! type for an edge attribute
  std::unordered_map<std::string, AttributedType> edgeAttributeTypes;

  /**
   * Prints out the data in an AttributedGraph for debugging purposes.
   */
  void printGraph();

  /**
   * Prints various graph statistics.
   * @param graph Graph to get stats on
   */
  void reportGraphStats();

  /**
   * Allocate memory for the AttributedGraph.
   *
   * @param numNodes Number of nodes to allocate memory for
   * @param numEdges Number of edges to allocate memory for
   * @param numNodeLabels Number of node labels to allocate memory for
   * @param numEdgesLabels Number of edge labels to allocate memory for
   */
  void allocateGraph(size_t numNodes, size_t numEdges, size_t numNodeLabels,
                     size_t numEdgeLabels);

  //! Same as allocateGraph except it doesn't allocate memory for unused
  //! uuid/names metadata + initializes attribute types
  void allocateGraphLDBC(size_t numNodes, size_t numEdges, size_t numNodeLabels,
                         size_t numEdgeLabels);

  /**
   * Set the end edge for a particular node in the CSR representation.
   *
   * @param nodeIndex node to set the last edge of
   * @param edgeIndex edge ID of last edge belonging to the specified node
   */
  void fixEndEdge(uint32_t nodeIndex, uint64_t edgeIndex);

  /**
   * Set a new node in the AttributedGraph with ONE label specified with a bit
   * position. Graph memory should have been allocated already.
   *
   * @param nodeIndex Node ID to set/change
   * @param uuid unique ID to node
   * @param labelBitPosition bit position to set label of
   * @param name Name to give node (e.g. name of a process)
   */
  void setNewNode(uint32_t nodeIndex, char* uuid, uint32_t labelBitPosition,
                  char* name);

  /**
   * Assign a node label to a node.
   *
   * @param nodeIndex Node to save mapping to
   * @param label Label to give node
   */
  void setNodeLabel(uint32_t nodeIndex, uint32_t label);

  /**
   * Assign a node label string to a particular bit position (for mapping
   * purposes).
   *
   * @param labelBitPosition Bit position to map to a particular string
   * @param name String to be associated with the integer label
   */
  void setNodeLabelMetadata(uint32_t labelBitPosition, const char* name);
  /**
   * Assign a edge label string to a particular bit position (for mapping
   * purposes).
   *
   * @param labelBitPosition Bit position to map to a particular string
   * @param name String to be associated with the integer label
   */
  void setEdgeLabelMetadata(uint32_t labelBitPosition, const char* name);

  /**
   * Label a node with a value for a particular existing attribute.
   *
   * @param nodeIndex node ID to give the attribute
   * @param key Attribute name
   * @param value Value of the attribute to give the node
   */
  void setExistingNodeAttribute(uint32_t nodeIndex, const char* key,
                                const char* value);

  /**
   * Construct an edge in the AttributedGraph for the first time, i.e. it only
   * has a SINGLE edge label specified by bit position to set.
   * Graph memory should have already been allocated.
   *
   * @param edgeIndex edge ID to construct
   * @param dstNodeIndex destination of edge to construct (source is implicit
   * from edgeID)
   * @param labelBitPosition Bit position to set on label of the edge
   * @param timestamp Timestamp to give edge
   */
  void constructNewEdge(uint64_t edgeIndex, uint32_t dstNodeIndex,
                        uint32_t labelBitPosition, uint64_t timestamp);

  /**
   * Construct an edge in the AttributedGraph using an existing label (i.e.
   * label is set directly as passed in).
   * Graph memory should have already been allocated.
   *
   * @param edgeIndex edge ID to construct
   * @param dstNodeIndex destination of edge to construct (source is implicit
   * from edgeID)
   * @param label Label to give edge
   * @param timestamp Timestamp to give edge
   */
  void constructEdge(uint64_t edgeIndex, uint32_t dstNodeIndex, uint32_t label,
                     uint64_t timestamp);

  /**
   * Label an edge with a value for a particular existing attribute.
   *
   * @param edgeIndex edge ID to give the attribute
   * @param key Attribute name
   * @param value Value of the attribute to give the edge
   */
  void setExistingEdgeAttribute(uint32_t edgeIndex, const char* key,
                                const char* value);

  /**
   * Add a new node attribute map with a particular size. Does nothing if key
   * already exists (assumption is resizeNodeAttributeMap will be called before
   * this function).
   *
   * @param key Attribute name
   * @param nodeCount size of map
   */
  void addNodeAttributeMap(const char* key, uint32_t nodeCount);
  /**
   * Designates some node attribute as having some type
   *
   * @param key Node attribute name
   * @param t Type to designate
   */
  void addNodeAttributeType(const char* key, AttributedType t);

  /**
   * Add a new edge attribute map with a particular size. Does nothing if key
   * already exists .
   *
   * @param key Attribute name
   * @param edgeCount size of map
   */
  void addEdgeAttributeMap(const char* key, uint32_t edgeCount);

  /**
   * Designates some edge attribute as having some type
   *
   * @param key Edge attribute name
   * @param t Type to designate
   */
  void addEdgeAttributeType(const char* key, AttributedType t);

#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
  /**
   * Return an integer with the bit representing the specified node label set.
   * Assumes the label actually exists, else undefined behavior.
   *
   * @param nodeLabel Node label to get mask for
   * @returns Boolean saying if a node label is valid +
   * A number with the bit representing the specified label set for use
   * in bitmasks + a number with bit representing labels NOT to match
   */
  std::pair<bool, std::pair<uint32_t, uint32_t>>
  getNodeLabelMask(const std::string& nodeLabel);
#endif

  /**
   * Return an integer with the bit representing the specified edge label set.
   * Assumes the label actually exists, else undefined behavior.
   *
   * @param nodeLabel Edge label to get mask for
   * @returns Boolean saying if a node label is valid +
   * A number with the bit representing the specified label set for use
   * in bitmasks + a number with bit representing labels NOT to match
   */
  std::pair<bool, std::pair<uint32_t, uint32_t>>
  getEdgeLabelMask(const std::string& edgeLabel);

  /**
   * Serialize the AttributedGraph onto disk for later use.
   * @param filename Name to save serialized graph on disk to
   */
  void saveGraph(const char* filename);

  /**
   * Load an AttributedGraph from disk for use.
   * @param filename Name of serialized graph on disk to load
   */
  void loadGraph(const char* filename);

  // TODO doxygen all of the things below
  // TODO maybe move these somewhere else rather than have them as funcs
  // here
  size_t matchCypherQuery(EventLimit limit, EventWindow window,
                          const char* cypherQueryStr, bool useGraphSimulation);

  size_t matchQuery(EventLimit limit, EventWindow window,
                    std::vector<MatchedNode>& queryNodes,
                    MatchedEdge* queryEdges, size_t numQueryEdges,
                    const char** filters, bool useGraphSimulation);
};
} // namespace galois::graphs
#endif
