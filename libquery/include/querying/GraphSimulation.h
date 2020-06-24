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
 * Contains definitions of functions/structs for graph simluation.

 */
#ifndef GALOIS_GRAPH_SIMULATION_H
#define GALOIS_GRAPH_SIMULATION_H
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

  //! default constructor, notably to init singleton to 0
  MatchedEdge() : timestamp(0), label(NULL) {}
};

bool matchNodeLabel(const QueryNode& query, const QueryNode& data);

bool matchNodeDegree(const QueryGraph& queryGraph,
                     const QueryGNode& queryNodeID, const QueryGraph& dataGraph,
                     const QueryGNode& dataNodeID);

bool matchEdgeLabel(const QueryEdgeData& query, const QueryEdgeData& data);

/**
 * Reset matched status on all nodes to 0
 *
 * @param graph Graph to reset matched status on
 */
void resetMatchedStatus(QueryGraph& graph);

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
