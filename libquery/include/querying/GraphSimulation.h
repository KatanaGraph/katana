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
 * @file GraphSimulation.h
 *
 * Contains definitions of graph related structures for graph simluation in
 * Galois, most notably the AttributedGraph structure.
 */
#ifndef GALOIS_GRAPH_SIMULATION_H
#define GALOIS_GRAPH_SIMULATION_H
#include "galois/Galois.h"
#include "galois/graphs/QueryGraph.h"

#include <string>

/**
 * Represents a matched node.
 */
struct MatchedNode {
  //! ID of matched node
  const char* id;
  // const char* label;
  //! Name for matched node
  const char* name;
};

/**
 * Represents a matched edge.
 */
struct MatchedEdge {
  //! timestamp on edge
  uint64_t timestamp;
  //! label on edge
  const char* label;
  //! actor of edge
  MatchedNode caused_by;
  //! target of edge's action
  MatchedNode acted_on;
  // TODO very hacky way to do singletons, revamp later
  //! if non-zero, this edge only represents a single node
  char singleton;

  //! default constructor, notably to init singleton to 0
  MatchedEdge() : timestamp(0), label(NULL), singleton(0) {}
};

//! Time-limit of consecutive events (inclusive)
struct EventLimit {
  bool valid;
  uint64_t time; // inclusive
  EventLimit() : valid(false) {}
};

//! Time-span of all events (inclusive)
struct EventWindow {
  bool valid;
  uint64_t startTime; // inclusive
  uint64_t endTime;   // inclusive
  EventWindow() : valid(false) {}
};

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

/**
 * Wrapped graph that contains metadata maps explaining what the compressed
 * data stored in the graph proper mean. For example, instead of storing
 * node types directly on the Graph, an int (which maps to a node type)
 * is stored on the node data.
 */
struct AttributedGraph {
  //! Graph structure class
  QueryGraph graph;
  std::vector<std::string> nodeLabelNames;                //!< maps ID to Name
  std::unordered_map<std::string, uint32_t> nodeLabelIDs; //!< maps Name to ID
  std::vector<std::string> edgeLabelNames;                //!< maps ID to Name
  std::unordered_map<std::string, uint32_t> edgeLabelIDs; //!< maps Name to ID
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
};

bool matchNodeLabel(const QueryNode& query, const QueryNode& data);

bool matchNodeDegree(const QueryGraph& queryGraph,
                     const QueryGNode& queryNodeID, const QueryGraph& dataGraph,
                     const QueryGNode& dataNodeID);

bool matchEdgeLabel(const QueryEdgeData& query, const QueryEdgeData& data);

#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
/**
 * Return an integer with the bit representing the specified node label set.
 * Assumes the label actually exists, else undefined behavior.
 *
 * @param g Graph to get label from
 * @param nodeLabel Node label to get mask for
 * @returns Boolean saying if a node label is valid +
 * A number with the bit representing the specified label set for use
 * in bitmasks + a number with bit representing labels NOT to match
 */
std::pair<bool, std::pair<uint32_t, uint32_t>>
getNodeLabelMask(AttributedGraph& g, const std::string& nodeLabel);
#endif

/**
 * Return an integer with the bit representing the specified edge label set.
 * Assumes the label actually exists, else undefined behavior.
 *
 * @param g Graph to get label from
 * @param nodeLabel Edge label to get mask for
 * @returns Boolean saying if a node label is valid +
 * A number with the bit representing the specified label set for use
 * in bitmasks + a number with bit representing labels NOT to match
 */
std::pair<bool, std::pair<uint32_t, uint32_t>>
getEdgeLabelMask(AttributedGraph& g, const std::string& edgeLabel);

/**
 * Reset matched status on all nodes to 0
 *
 * @param graph Graph to reset matched status on
 */
void resetMatchedStatus(QueryGraph& graph);

/**
 * TODO doxygen
 */
void matchNodesUsingGraphSimulation(QueryGraph& qG, QueryGraph& dG,
                                    bool reinitialize, EventLimit limit,
                                    EventWindow window,
                                    bool queryNodeHasMoreThan2Edges,
                                    std::vector<std::string>& nodeContains,
                                    std::vector<std::string>& nodeNames);

#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
//! TODO doxygen
void matchEdgesAfterGraphSimulation(QueryGraph& qG, QueryGraph& dG);
#endif

/**
 * @todo doxygen
 */
void runGraphSimulation(QueryGraph& queryGraph, QueryGraph& dataGraph,
                        EventLimit limit, EventWindow window,
                        bool queryNodeHasMoreThan2Edges,
                        std::vector<std::string>& nodeContains,
                        std::vector<std::string>& nodeNames);

/**
 * @todo doxygen
 */
void findShortestPaths(QueryGraph& dataGraph, uint32_t srcQueryNode,
                       uint32_t dstQueryNode, QueryEdgeData queryEdgeData,
                       uint32_t matchedQueryNode, uint32_t matchedQueryEdge);

/**
 * Get the number of matched nodes in the graph.
 * @param graph Graph to count matched nodes in
 * @returns Number of matched nodes in the graph
 */
size_t countMatchedNodes(QueryGraph& graph);


#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
/**
 * Get the number of matched edges in the graph.
 * @param graph Graph to count matched edges in
 * @returns Number of matched edges in the graph
 */
size_t countMatchedEdges(QueryGraph& graph);
#endif

#endif // GALOIS_GRAPH_SIMULATION_H
