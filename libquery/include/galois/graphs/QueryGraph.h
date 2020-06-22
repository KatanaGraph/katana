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
#pragma once
#include "galois/graphs/LCGraph.h"
#include "galois/graphs/LC_CSR_Labeled_Graph.h"

/**
 * Node data type.
 */
struct QueryNode {
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
  //! Label on node. Maximum of 32 node labels.
  uint32_t label;
#endif
  //! Matched status of node represented in bits. Max of 64 matched in query
  //! graph.
  //! @todo make matched a dynamic bitset
  uint64_t matched;
};

/**
 * Edge data type
 */
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
struct QueryEdgeData {
  //! Label on the edge (like the type of action). Max of 32 edge labels.
  uint32_t label;
  //! Timestamp of action the edge represents. Range is limited.
  uint64_t timestamp;
  //! Matched status on the edge represented in bits. Max of 64 matched in
  //! query graph.
  uint64_t matched;
  /**
   * Constructor for edge data. Defaults to unmatched.
   * @param l Type of action this edge represents
   * @param t Timestamp of action
   */
  EdgeData() : label(0), timestamp(0), matched(0) {}
  EdgeData(uint32_t l, uint64_t t) : label(l), timestamp(t), matched(0) {}
  EdgeData(uint32_t l, uint64_t t, uint64_t m)
      : label(l), timestamp(t), matched(m) {}
};
#else
// EdgeData() : label(0) {}
// EdgeData(uint32_t l) : label(l) {}
typedef uint32_t QueryEdgeData;
#endif

//! QueryGraph typedef. This is the main point of this file: providing a
//! common def for other files to include a bidirectional query node with
//! certain properties
using QueryGraph =
    galois::graphs::LC_CSR_Labeled_Graph<QueryNode, QueryEdgeData, false, true,
                                         true>;
//! Graph node typedef
using QueryGNode = QueryGraph::GraphNode;
