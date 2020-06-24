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
 * @file Matching.cpp
 *
 * Contains implementations of functions defined in Matching.h as well
 * as various helpers for those functions.
 */

#include "querying/Matching.h"
#include "galois/substrate/PerThreadStorage.h"
#include <regex>

// query.label: bitwise-OR of tags that should MATCH and tags that should
// NOT-MATCH query.matched: tags that should MATCH
bool matchNodeLabel(const QueryNode& GALOIS_UNUSED(query),
                    const QueryNode& GALOIS_UNUSED(data)) {
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
  return ((query.label & data.label) == query.matched);
#else
  return true;
#endif
}

bool matchNodeDegree(const QueryGraph& queryGraph,
                     const QueryGNode& queryNodeID, const QueryGraph& dataGraph,
                     const QueryGNode& dataNodeID) {
  // if the degree is smaller than that of its corresponding query vertex
#ifdef USE_QUERY_GRAPH_WITH_MULTIPLEXING_EDGE_LABELS
  if (dataGraph.degree(dataNodeID) < queryGraph.degree(queryNodeID))
    return false;
  if (dataGraph.in_degree(dataNodeID) < queryGraph.in_degree(queryNodeID))
    return false;
#else
  for (auto qeData : queryGraph.data_range()) {
    if (dataGraph.degree(dataNodeID, *qeData) <
        queryGraph.degree(queryNodeID, *qeData))
      return false;
    if (dataGraph.in_degree(dataNodeID, *qeData) <
        queryGraph.in_degree(queryNodeID, *qeData))
      return false;
  }
#endif
  return true;
}

bool matchEdgeLabel(const QueryEdgeData& query, const QueryEdgeData& data) {
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
  return ((query.label & data.label) == query.matched);
#else
#ifdef USE_QUERY_GRAPH_WITH_MULTIPLEXING_EDGE_LABELS
  return ((query & data) == query);
#else
  return (query == data);
#endif
#endif
}

void resetMatchedStatus(QueryGraph& graph) {
  galois::do_all(
      galois::iterate(graph.begin(), graph.end()),
      [&](auto n) {
        auto& data   = graph.getData(n);
        data.matched = 0; // matches to none
      },
      galois::loopname("ResetMatched"));
}

void findShortestPaths(QueryGraph& graph, uint32_t srcQueryNode,
                       uint32_t dstQueryNode, QueryEdgeData qeData,
                       uint32_t matchedQueryNode,
                       uint32_t GALOIS_UNUSED(matchedQueryEdge)) {
  galois::LargeArray<std::atomic<uint32_t>> parent;
  parent.allocateInterleaved(graph.size());
  const uint32_t infinity = std::numeric_limits<uint32_t>::max();

  using WorkQueue = galois::InsertBag<QueryGraph::GraphNode>;
  WorkQueue w[2];
  WorkQueue* cur  = &w[0];
  WorkQueue* next = &w[1];

  // add source nodes to the work-list
  galois::do_all(
      galois::iterate(graph.begin(), graph.end()),
      [&](auto n) {
        parent[n] = infinity;

        auto& data    = graph.getData(n);
        uint64_t mask = (1 << srcQueryNode);
        if (data.matched & mask) {
          next->push_back(n);
        }
      },
      galois::loopname("ResetParent"));

  auto sizeNext = std::distance(next->begin(), next->end());

  // loop until no more data nodes are left to traverse
  while (sizeNext > 0) {
    std::swap(cur, next);
    next->clear();

    // traverse edges
    galois::do_all(
        galois::iterate(*cur),
        [&](auto n) {
          for (auto edge : graph.edges(n)) {
            auto deData = graph.getEdgeData(edge);
            if (matchEdgeLabel(qeData, deData)) {
              auto dst                = graph.getEdgeDst(edge);
              uint32_t old_parent_dst = parent[dst];
              if (old_parent_dst == infinity) {
                auto& dstData = graph.getData(dst);
                uint64_t mask = (1 << srcQueryNode);
                if (!(dstData.matched & mask)) {
                  if (parent[dst].compare_exchange_strong(
                          old_parent_dst, n, std::memory_order_relaxed)) {
                    mask = (1 << dstQueryNode);
                    if (!(dstData.matched & mask)) {
                      next->push_back(dst);
                    }
                  }
                }
              }
            }
          }
        },
        galois::loopname("TraverseEdges"));

    sizeNext = std::distance(next->begin(), next->end());
  }

  // add destination nodes to the work-list or un-match destination nodes
  galois::do_all(
      galois::iterate(graph.begin(), graph.end()),
      [&](auto n) {
        auto& data    = graph.getData(n);
        uint64_t mask = (1 << dstQueryNode);
        if (data.matched & mask) {
          if (parent[n] == infinity) {
            data.matched &= ~mask; // no longer a match
          } else {
            next->push_back(n);
          }
        }
      },
      galois::loopname("MatchDestination"));

  // back traverse edges
  galois::do_all(
      galois::iterate(*next),
      [&](auto n) {
        uint32_t pred = n;
        while ((parent[pred] != infinity) && (parent[pred] != pred)) {
          uint32_t succ = parent[pred];
          if (parent[pred].compare_exchange_weak(succ, infinity,
                                                 std::memory_order_relaxed)) {
            if (pred != n) {
              auto& data = graph.getData(pred);
              data.matched |= 1 << matchedQueryNode;
            }
            for (auto edge : graph.edges(pred)) {
              auto dst = graph.getEdgeDst(edge);
              if (dst == succ) {
                auto& deData = graph.getEdgeData(edge);
                if (matchEdgeLabel(qeData, deData)) {
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
                  deData.matched |= 1 << matchedQueryEdge;
#endif
                  break;
                }
              }
            }
            pred = succ;
          }
        }
        auto& srcData = graph.getData(pred);
        uint64_t mask = (1 << srcQueryNode);
        if (srcData.matched & mask) {
          parent[pred] = pred;
        }
      },
      galois::loopname("BackTraverseEdges"));

  // un-match source nodes
  galois::do_all(
      galois::iterate(graph.begin(), graph.end()),
      [&](auto n) {
        auto& data    = graph.getData(n);
        uint64_t mask = (1 << srcQueryNode);
        if (data.matched & mask) {
          if (parent[n] == infinity) {
            data.matched &= ~mask; // no longer a match
          }
        }
      },
      galois::loopname("MatchSource"));
}

size_t countMatchedNodes(QueryGraph& graph) {
  galois::GAccumulator<size_t> numMatched;
  galois::do_all(
      galois::iterate(graph.begin(), graph.end()),
      [&](auto n) {
        auto& data = graph.getData(n);
        if (data.matched) {
          numMatched += 1;
        }
      },
      galois::loopname("CountMatchedNodes"));
  return numMatched.reduce();
}

#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
size_t countMatchedEdges(QueryGraph& graph) {
  galois::GAccumulator<size_t> numMatched;
  galois::do_all(
      galois::iterate(graph.begin(), graph.end()),
      [&](auto n) {
        auto& data = graph.getData(n);
        if (data.matched) {
          for (auto e : graph.edges(n)) {
            auto eData = graph.getEdgeData(e);
            if (eData.matched) {
              numMatched += 1;
            }
          }
        }
      },
      galois::loopname("CountMatchedEdges"));
  return numMatched.reduce();
}
#endif
