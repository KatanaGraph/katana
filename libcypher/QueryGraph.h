#pragma once
#include "galois/graphs/LCGraph.h"
#include "galois/graphs/LC_CSR_Labeled_Graph.h"

/**
 * Node data type.
 */
struct Node {
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
struct EdgeData {
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
typedef uint32_t EdgeData;
#endif

//! Graph typedef
using Graph =
    galois::graphs::LC_CSR_Labeled_Graph<Node, EdgeData, false, true, true>;
// using Graph = galois::graphs::B_LC_CSR_Graph<Node, EdgeData>::
//                 with_no_lockable<true>::type::
//                 with_numa_alloc<true>::type;
//! Graph node typedef
using GNode = Graph::GraphNode;
