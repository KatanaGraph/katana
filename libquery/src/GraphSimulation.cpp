/*
 * This file belongs to the Galois project, a C++ library for exploiting
 * parallelism. The code is being released under the terms of the 3-Clause BSD
 * License (a copy is located in LICENSE.txt at the top-level directory).
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
 * @file GraphSimulation.cpp
 *
 * Contains implementations of functions defined in GraphSimulation.h as well
 * as various helpers for those functions.
 */

#include "querying/GraphSimulation.h"
#include "galois/substrate/PerThreadStorage.h"
#include <regex>

// query.label: bitwise-OR of tags that should MATCH and tags that should
// NOT-MATCH query.matched: tags that should MATCH
bool matchNodeLabel(const QueryNode& query, const QueryNode& data) {
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

/**
 * Match query nodes with nodes in the data graph.
 *
 * @param qG Query graph
 * @param dG Data graph
 * @param w matched data nodes added to a worklist for later processing
 * @param queryMatched bitset of querying status
 */
template <typename QG, typename DG, typename W>
void matchLabel(QG& qG, DG& dG, W& w, galois::GAccumulator<uint32_t>& workItems,
                std::vector<bool>& queryMatched,
                std::vector<std::string>& nodeContains,
                std::vector<std::string>& nodeNames) {
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
  queryMatched.resize(qG.size(), false);
#else
  queryMatched.resize(qG.size(), true);
#endif
  assert(qG.size() <= 64); // because matched is 64-bit
  galois::do_all(
      galois::iterate(dG.begin(), dG.end()),
      [&](auto dn) {
        auto& dData   = dG.getData(dn);
        dData.matched = 0; // matches to none
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
        for (auto qn : qG) {
          auto& qData = qG.getData(qn);
          if (matchNodeLabel(qData, dData)) {
            if (nodeContains.size() == 0 || nodeContains[qn] == "") {
              queryMatched[qn] = true;
              if (!dData.matched) {
                w.push_back(dn);
                workItems += 1;
              }
              dData.matched |= 1 << qn; // multiple matches
            } else {
              std::string dataName = nodeNames[dn];
              std::regex e(nodeContains[qn], std::regex_constants::ECMAScript);
              if (std::regex_match(dataName, e)) {
                // if (dataName.find(nodeContains[qn]) != std::string::npos) {
                // TODO reduce code duplication
                queryMatched[qn] = true;
                if (!dData.matched) {
                  w.push_back(dn);
                  workItems += 1;
                }
                dData.matched |= 1 << qn; // multiple matches
              }
            }
          }
        }
#else
        for (auto qn : qG) {
          if (matchNodeDegree(qG, qn, dG, dn)) {
            if (!dData.matched) {
              w.push_back(dn);
              workItems += 1;
            }
            dData.matched |= 1 << qn; // multiple matches
          }
        }
#endif
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
        for (auto de : dG.edges(dn)) {
          auto& deData   = dG.getEdgeData(de);
          deData.matched = 0; // matches to none
        }
#endif
      },
      galois::steal(), galois::loopname("MatchLabel"));
}

/**
 * Checks to see if any query nodes were unmatched
 *
 * @param qG query graph
 * @param queryMatched matched status of query nodes
 * @returns true if there is an unmatched query node
 */
template <typename QG>
bool existEmptyLabelMatchQGNode(QG& qG, std::vector<bool>& queryMatched) {
  for (auto qn : qG) {
    if (!queryMatched[qn]) {
      galois::gDebug("No label matched for query node ", qn, "\n");
      return true;
    }
  }
  return false;
}

typedef galois::gstl::Vector<uint64_t> VecTy;
typedef galois::gstl::Vector<bool> VecBoolTy;
typedef galois::gstl::Vector<VecTy> VecVecTy;

#ifdef SLOW_NO_MATCH_FAST_MATCH
void matchQuerySlowerNoMatchFasterMatch() {
  if (queryNodeHasMoreThan2Edges) {
    uint64_t qPrevEdgeTimestamp = 0;
    uint64_t dPrevEdgeTimestamp = 0;
    for (auto qe : qG.edges(qn)) {
      auto qeData = qG.getEdgeData(qe);
      auto qDst   = qG.getEdgeDst(qe);

      bool matched                = false;
      uint64_t dNextEdgeTimestamp = std::numeric_limits<uint64_t>::max();
      for (auto de : dG.edges(dn)) {
        auto& deData = dG.getEdgeData(de);
        if (useWindow) {
          if ((deData.timestamp > window.endTime) ||
              (deData.timestamp < window.startTime)) {
            continue; // skip this edge since it is not in the
                      // time-span of interest
          }
        }
        if (matchEdgeLabel(qeData, deData)) {
          auto& dDstData = dG.getData(dG.getEdgeDst(de));
          if (dDstData.matched & (1 << qDst)) {
            if ((qPrevEdgeTimestamp <= qeData.timestamp) ==
                (dPrevEdgeTimestamp <= deData.timestamp)) {
              if (dNextEdgeTimestamp > deData.timestamp) {
                dNextEdgeTimestamp =
                    deData.timestamp; // minimum of matched edges
              }
              matched = true;
            }
          }
        }
      }

      // remove qn from dn when we have an unmatched edge
      if (!matched) {
        dData.matched &= ~mask;
        break;
      }

      qPrevEdgeTimestamp = qeData.timestamp;
      dPrevEdgeTimestamp = dNextEdgeTimestamp;
    }
  } else {
    // assume query graph has at the most 2 edges for any node
    auto qe1  = qG.edge_begin(qn);
    auto qend = qG.edge_end(qn);
    if (qe1 != qend) {
      auto& qeData = qG.getEdgeData(qe1);
      auto qDst    = qG.getEdgeDst(qe1);

      bool matched = false;
      for (auto& de : dG.edges(dn)) {
        auto& deData = dG.getEdgeData(de);
        if (useWindow) {
          if ((deData.timestamp > window.endTime) ||
              (deData.timestamp < window.startTime)) {
            continue; // skip this edge since it is not in the
                      // time-span of interest
          }
        }
        if (matchEdgeLabel(qeData, deData)) {
          auto dDst      = dG.getEdgeDst(de);
          auto& dDstData = dG.getData(dDst);
          if (dDstData.matched & (1 << qDst)) {

            auto qe2 = qe1 + 1;
            if (qe2 == qend) { // only 1 edge
              matched = true;
              break;
            } else {
              assert((qe2 + 1) == qend);
              // match the second edge
              auto& qeData2 = qG.getEdgeData(qe2);
              auto qDst2    = qG.getEdgeDst(qe2);

              for (auto& de2 : dG.edges(dn)) {
                auto& deData2 = dG.getEdgeData(de2);
                if (matchEdgeLabel(qeData2, deData2)) {
                  auto dDst2      = dG.getEdgeDst(de2);
                  auto& dDstData2 = dG.getData(dDst2);
                  if (dDstData2.matched & (1 << qDst2)) {
                    assert(qeData.timestamp != qeData2.timestamp);
                    if (useWindow) {
                      if ((deData2.timestamp > window.endTime) ||
                          (deData2.timestamp < window.startTime)) {
                        continue; // skip this edge since it is not in
                                  // the time-span of interest
                      }
                    }
                    if ((qeData.timestamp <= qeData2.timestamp) ==
                        (deData.timestamp <= deData2.timestamp)) {
                      if (useLimit) {
                        if (std::abs(deData.timestamp - deData2.timestamp) >
                            limit.time) {
                          continue; // skip this sequence of events
                                    // because too much time has
                                    // lapsed between them
                        }
                      }
#ifdef UNIQUE_QUERY_NODES
                      if ((qDst != qDst2) == (dDst != dDst2)) {
#endif
                        matched = true;
                        break;
#ifdef UNIQUE_QUERY_NODES
                      }
#endif
                    }
                  }
                }
              }

              if (matched)
                break;
            }
          }
        }
      }

      // remove qn from dn when we have an unmatched edge
      if (!matched) {
        dData.matched &= ~mask;
        break;
      }
    }
  }
}
#endif

#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
template <bool inEdges, bool useLimit>
void matchQueryTimestampOrder(QueryGraph& qG, QueryGraph& dG, EventLimit limit,
                              uint32_t qn, VecVecTy& matchedEdges,
                              bool& matched) {
  VecTy queryEdgeOrder;
  VecTy queryTimestamps;
  auto qEdges = inEdges ? qG.in_edges(qn) : qG.edges(qn);
  for (auto qe : qEdges) {
    auto qeData = inEdges ? qG.getInEdgeData(qe) : qG.getEdgeData(qe);
    queryTimestamps.push_back(qeData.timestamp);
  }
  uint64_t prev = 0;
  while (prev != std::numeric_limits<uint32_t>::max()) {
    uint64_t next    = std::numeric_limits<uint32_t>::max();
    size_t minEdgeID = 0;
    for (size_t i = 0; i < queryTimestamps.size(); ++i) {
      uint64_t cur = queryTimestamps[i];
      if (cur != std::numeric_limits<uint32_t>::max()) {
        if (cur >= prev) {
          if (cur < next) {
            next      = cur;
            minEdgeID = i;
          }
        }
      }
    }
    if (next != std::numeric_limits<uint32_t>::max()) {
      queryEdgeOrder.push_back(minEdgeID);
      queryTimestamps[minEdgeID] = std::numeric_limits<uint32_t>::max();
    }
    prev = next;
  }
  prev = 0;
  for (size_t k = 0; k < queryEdgeOrder.size(); ++k) {
    size_t i      = queryEdgeOrder[k];
    uint64_t next = std::numeric_limits<uint64_t>::max();
    for (size_t j = 0; j < matchedEdges[i].size(); ++j) {
      uint64_t cur = matchedEdges[i][j];
      if (cur >= prev) {
        if (cur < next) {
          next = cur;
        }
      }
    }
    if ((next == std::numeric_limits<uint64_t>::max()) || (next < prev)) {
      matched = false;
      break;
    }
    if (useLimit) {
      if ((next - prev) > limit.time) { // TODO: imprecise - fix this
        matched = false; // skip this sequence of events because too
                         // much time has lapsed between them
      }
    }
    prev = next;
    matchedEdges[i].clear();
  }
}
#endif

template <bool inEdges, bool useLimit, bool useWindow>
bool matchQueryEdges(QueryGraph& qG, QueryGraph& dG,
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
                     EventLimit limit, EventWindow window,
#endif
                     uint32_t qn, uint32_t dn,
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
                     VecVecTy& matchedEdges) {
#else
                     VecBoolTy& matchedEdges) {
#endif
  bool matched = true;
  uint32_t num_qEdges;
  if (inEdges) {
    num_qEdges = qG.in_degree(qn);
  } else {
    num_qEdges = qG.degree(qn);
  }
  if (num_qEdges > 0) {
    // match children links
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
    matchedEdges.clear();
    matchedEdges.resize(num_qEdges);
    auto dEdges = inEdges ? dG.in_edges(dn) : dG.edges(dn);
    for (auto de : dEdges) {
      auto& deData = inEdges ? dG.getInEdgeData(de) : dG.getEdgeData(de);
      if (useWindow) {
        if ((deData.timestamp > window.endTime) ||
            (deData.timestamp < window.startTime)) {
          continue; // skip this edge since it is not in the time-span
                    // of interest
        }
      }
      uint32_t edgeID = 0;
      // Assumption: each query edge of this query node has a different label
      auto qEdges = inEdges ? qG.in_edges(qn) : qG.edges(qn);
      for (auto qe : qEdges) {
        auto qeData = inEdges ? qG.getInEdgeData(qe) : qG.getEdgeData(qe);
        if (matchEdgeLabel(qeData, deData)) {
          auto qDst      = inEdges ? qG.getInEdgeDst(qe) : qG.getEdgeDst(qe);
          auto dDst      = inEdges ? dG.getInEdgeDst(de) : dG.getEdgeDst(de);
          auto& dDstData = dG.getData(dDst);
          if (dDstData.matched & (1 << qDst)) {
            matchedEdges[edgeID].push_back(deData.timestamp);
          }
        }
        ++edgeID;
      }
    }
    // Assumption: each query edge of this query node has a different label
    for (uint32_t i = 0; i < matchedEdges.size(); ++i) {
      if (matchedEdges[i].size() == 0) {
        matched = false;
        break;
      }
    }
    if (matched) { // check if it matches query timestamp order
      matchQueryTimestampOrder<inEdges, useLimit>(qG, dG, limit, qn,
                                                  matchedEdges, matched);
    }
#else
#ifdef USE_QUERY_GRAPH_WITH_MULTIPLEXING_EDGE_LABELS
    matchedEdges.clear();
    matchedEdges.resize(num_qEdges);
    uint32_t numMatchedEdges = 0;
    auto dEdges = inEdges ? dG.in_edges(dn) : dG.edges(dn);
    auto qEdgeBegin = inEdges ? qG.in_edge_begin(qn) : qG.edge_begin(qn);
    for (auto de : dEdges) {
      auto& deData = inEdges ? dG.getInEdgeData(de) : dG.getEdgeData(de);
      for (uint32_t edgeID = 0; edgeID < num_qEdges; ++edgeID) {
        if (matchedEdges[edgeID] == true)
          continue;
        auto qe = qEdgeBegin + edgeID;
        auto qeData = inEdges ? qG.getInEdgeData(qe) : qG.getEdgeData(qe);
        if (matchEdgeLabel(qeData, deData)) {
          auto qDst = inEdges ? qG.getInEdgeDst(qe) : qG.getEdgeDst(qe);
          auto dDst = inEdges ? dG.getInEdgeDst(de) : dG.getEdgeDst(de);
          auto& dDstData = dG.getData(dDst);
          if (dDstData.matched & (1 << qDst)) {
            matchedEdges[edgeID] = true;
            ++numMatchedEdges;
            break; // FIXTHIS: this could lead to imprecise results
          }
        }
      }
      if (numMatchedEdges == num_qEdges)
        break;
    }
    if (numMatchedEdges < num_qEdges)
      return false;
#else
    for (auto qeData : qG.data_range()) {
      uint32_t qDegree =
          inEdges ? qG.in_degree(qn, *qeData) : qG.degree(qn, *qeData);
      if (qDegree > 0) {
        matchedEdges.clear();
        matchedEdges.resize(qDegree);
        uint32_t numMatchedEdges = 0;
        auto dEdges =
            inEdges ? dG.in_edges(dn, *qeData) : dG.edges(dn, *qeData);
        auto qEdgeBegin = inEdges ? qG.in_edge_begin(qn, *qeData)
                                  : qG.edge_begin(qn, *qeData);
        for (auto de : dEdges) {
          auto dDst      = inEdges ? dG.getInEdgeDst(de) : dG.getEdgeDst(de);
          auto& dDstData = dG.getData(dDst);
          for (uint32_t edgeID = 0; edgeID < qDegree; ++edgeID) {
            if (matchedEdges[edgeID] == true)
              continue;
            auto qe   = qEdgeBegin + edgeID;
            auto qDst = inEdges ? qG.getInEdgeDst(qe) : qG.getEdgeDst(qe);
            if (dDstData.matched & (1 << qDst)) {
              matchedEdges[edgeID] = true;
              ++numMatchedEdges;
              break; // FIXTHIS: this could lead to imprecise results
            }
          }
          if (numMatchedEdges == qDegree)
            break;
        }
        if (numMatchedEdges < qDegree)
          return false;
      }
    }
#endif
#endif
  }
  return matched;
}

/**
 * @todo doxygen
 */
template <bool useLimit, bool useWindow, bool queryNodeHasMoreThan2Edges>
void matchNodesOnce(QueryGraph& qG, QueryGraph& dG,
                    galois::InsertBag<QueryGraph::GraphNode>* cur,
                    galois::InsertBag<QueryGraph::GraphNode>* next,
                    galois::GAccumulator<uint32_t>& workItems
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
                    ,
                    EventLimit limit, EventWindow window
#endif
) {
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
  galois::substrate::PerThreadStorage<VecVecTy> matchedEdgesPerThread;
#else
  galois::substrate::PerThreadStorage<VecBoolTy> matchedEdgesPerThread;
#endif
  galois::do_all(
      galois::iterate(*cur),
      [&](auto dn) {
        auto& dData        = dG.getData(dn);
        auto& matchedEdges = *matchedEdgesPerThread.getLocal();

        for (auto qn : qG) { // multiple matches
          uint64_t mask = (1 << qn);
          if (dData.matched & mask) {
            bool matched =
                matchQueryEdges<true, useLimit, useWindow>(qG, dG,
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
                                                           limit, window,
#endif
                                                           qn, dn,
                                                           matchedEdges) &&
                matchQueryEdges<false, useLimit, useWindow>(qG, dG,
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
                                                            limit, window,
#endif
                                                            qn, dn,
                                                            matchedEdges);
            // remove qn from dn
            if (!matched) {
              dData.matched &= ~mask;
            }
            // TODO: add support for dst-id inequality
          }
        }

        // keep dn for next round
        if (dData.matched) {
          next->push_back(dn);
          workItems += 1;
        }
      },
      galois::steal(), galois::loopname("MatchNeighbors"));
}

void matchNodesUsingGraphSimulation(QueryGraph& qG, QueryGraph& dG,
                                    bool reinitialize, EventLimit limit,
                                    EventWindow window,
                                    bool queryNodeHasMoreThan2Edges,
                                    std::vector<std::string>& nodeContains,
                                    std::vector<std::string>& nodeNames) {
  using WorkQueue = galois::InsertBag<QueryGraph::GraphNode>;
  WorkQueue w[2];
  WorkQueue* cur  = &w[0];
  WorkQueue* next = &w[1];
  galois::GAccumulator<uint32_t> workItems;

  if (reinitialize) {
    std::vector<bool> queryMatched;
    workItems.reset();
    matchLabel(qG, dG, *next, workItems, queryMatched, nodeContains, nodeNames);
    // see if a query node remained unmatched; if so, reset match status on data
    // nodes and return
    if (existEmptyLabelMatchQGNode(qG, queryMatched)) {
      galois::do_all(
          galois::iterate(dG.begin(), dG.end()),
          [&qG, &dG, &w](auto dn) {
            auto& dData   = dG.getData(dn);
            dData.matched = 0; // matches to none
          },
          galois::loopname("ResetMatched"));
      return;
    }
  } else {
    // already have matched labels on data graphs
    galois::do_all(
        galois::iterate(dG.begin(), dG.end()),
        [&](auto dn) {
          auto& dData = dG.getData(dn);

          if (dData.matched) {
            next->push_back(dn);
          }
        },
        galois::loopname("ReinsertMatchedNodes"));
  }

  uint32_t sizeCur  = 0;
  uint32_t sizeNext = workItems.reduce();

  uint32_t numRounds = 0;

  galois::runtime::reportStat_Tmax(
      "GraphSimulation", "RemovedNodes_Round" + std::to_string(numRounds),
      (unsigned long)100 * (dG.size() - sizeNext) / dG.size());

  // loop until no more data nodes are removed
  while (sizeCur != sizeNext) {
    std::swap(cur, next);
    next->clear();
    workItems.reset();

#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
    if (limit.valid) {
      if (window.valid) {
        if (queryNodeHasMoreThan2Edges) {
          matchNodesOnce<true, true, true>(qG, dG, cur, next, workItems, limit,
                                           window);
        } else {
          matchNodesOnce<true, true, false>(qG, dG, cur, next, workItems, limit,
                                            window);
        }
      } else {
        if (queryNodeHasMoreThan2Edges) {
          matchNodesOnce<true, false, true>(qG, dG, cur, next, workItems, limit,
                                            window);
        } else {
          matchNodesOnce<true, false, false>(qG, dG, cur, next, workItems,
                                             limit, window);
        }
      }
    } else {
      if (window.valid) {
        if (queryNodeHasMoreThan2Edges) {
          matchNodesOnce<false, true, true>(qG, dG, cur, next, workItems, limit,
                                            window);
        } else {
          matchNodesOnce<false, true, false>(qG, dG, cur, next, workItems,
                                             limit, window);
        }
      } else {
        if (queryNodeHasMoreThan2Edges) {
          matchNodesOnce<false, false, true>(qG, dG, cur, next, workItems,
                                             limit, window);
        } else {
          matchNodesOnce<false, false, false>(qG, dG, cur, next, workItems,
                                              limit, window);
        }
      }
    }
#else
    if (queryNodeHasMoreThan2Edges) {
      matchNodesOnce<false, false, true>(qG, dG, cur, next, workItems);
    } else {
      matchNodesOnce<false, false, false>(qG, dG, cur, next, workItems);
    }
#endif

    sizeCur  = sizeNext;
    sizeNext = workItems.reduce();
    ++numRounds;

    galois::runtime::reportStat_Tmax(
        "GraphSimulation", "RemovedNodes_Round" + std::to_string(numRounds),
        (unsigned long)100 * (dG.size() - sizeNext) / dG.size());
  }

  galois::runtime::reportStat_Tmax("GraphSimulation", "NumRounds",
                                   (unsigned long)numRounds);

  galois::runtime::reportStat_Tmax("GraphSimulation", "RemovedNodes",
                                   (unsigned long)100 * (dG.size() - sizeCur) /
                                       dG.size());
}

#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
std::pair<bool, std::pair<uint32_t, uint32_t>>
getNodeLabelMask(AttributedGraph& g, const std::string& nodeLabel) {
  if (nodeLabel.find(";") == std::string::npos) {
    // no semicolon = only 1 node label
    if (nodeLabel != "any") {
      bool notMode      = false;
      std::string label = nodeLabel;
      // trim out the ~ sign
      if (nodeLabel.find("~") == 0) {
        notMode = true;
        label   = nodeLabel.substr(1);
      }

      // see if label exists
      if (g.nodeLabelIDs.find(label) != g.nodeLabelIDs.end()) {
        if (!notMode) {
          return std::make_pair(true,
                                std::make_pair(1u << g.nodeLabelIDs[label], 0));
        } else {
          return std::make_pair(true,
                                std::make_pair(0, 1u << g.nodeLabelIDs[label]));
        }
      } else {
        // bad label; fine if in not mode, bad otherwise
        if (!notMode) {
          return std::make_pair(false, std::make_pair(0, 0));
        } else {
          return std::make_pair(true, std::make_pair(0, 0));
        }
      }
    } else {
      // any string = match everything; return string of all 0s
      return std::make_pair(true, std::make_pair(0, 0));
    }
  } else {
    // semicolon = multiple node labels; split and create mask
    uint32_t labelMask    = 0;
    uint32_t notLabelMask = 0;

    std::istringstream tokenStream(nodeLabel);
    std::string token;

    // loop through semi-colon separated labels
    while (std::getline(tokenStream, token, ';')) {
      bool notMode = false;
      // trim out the ~ sign
      if (token.find("~") == 0) {
        notMode = true;
        token   = token.substr(1);
      }

      if (g.nodeLabelIDs.find(token) != g.nodeLabelIDs.end()) {
        // mark bit based on ~ token
        if (!notMode) {
          labelMask |= 1u << g.nodeLabelIDs[token];
        } else {
          notLabelMask |= 1u << g.nodeLabelIDs[token];
        }
      } else {
        // label not found; get out if not in not mode, else keep going
        if (!notMode) {
          return std::make_pair(false, std::make_pair(0, 0));
        }
      }
    }

    return std::make_pair(true, std::make_pair(labelMask, notLabelMask));
  }
}
#endif

std::pair<bool, std::pair<uint32_t, uint32_t>>
getEdgeLabelMask(AttributedGraph& g, const std::string& edgeLabel) {
  if (edgeLabel.find(";") == std::string::npos) {
    if (edgeLabel != "ANY") {
      bool notMode      = false;
      std::string label = edgeLabel;
      // trim out the ~ sign
      if (edgeLabel.find("~") == 0) {
        notMode = true;
        label   = edgeLabel.substr(1);
      }

      if (g.edgeLabelIDs.find(label) != g.edgeLabelIDs.end()) {
        if (!notMode) {
          return std::make_pair(true,
                                std::make_pair(1u << g.edgeLabelIDs[label], 0));
        } else {
          return std::make_pair(true,
                                std::make_pair(0, 1u << g.edgeLabelIDs[label]));
        }
      } else {
        // bad label; fine if in not mode, bad otherwise
        if (!notMode) {
          return std::make_pair(false, std::make_pair(0, 0));
        } else {
          return std::make_pair(true, std::make_pair(0, 0));
        }
      }
    } else {
      // ANY; return all 0s = match anything
      return std::make_pair(true, std::make_pair(0, 0));
    }
  } else {
    // found ; means multiedge specification: used for restricting * path
    // searches

    // semicolon = multiple node labels; split and create mask
    uint32_t labelMask    = 0;
    uint32_t notLabelMask = 0;

    std::istringstream tokenStream(edgeLabel);
    std::string token;

    // loop through semi-colon separated labels
    while (std::getline(tokenStream, token, ';')) {
      galois::gPrint(token, "\n");
      bool notMode = false;
      // trim out the ~ sign
      if (token.find("~") == 0) {
        notMode = true;
        token   = token.substr(1);
      }

      if (g.edgeLabelIDs.find(token) != g.edgeLabelIDs.end()) {
        // mark bit based on ~ token
        if (!notMode) {
          labelMask |= 1u << g.edgeLabelIDs[token];
        } else {
          notLabelMask |= 1u << g.edgeLabelIDs[token];
        }
      } else {
        // label not found; get out if not in not mode, else keep going
        if (!notMode) {
          return std::make_pair(false, std::make_pair(0, 0));
        }
      }
    }

    return std::make_pair(true, std::make_pair(labelMask, notLabelMask));
  }
}

bool nodeLabelExists(AttributedGraph& g, const std::string& nodeLabel) {
  return (g.nodeLabelIDs.find(nodeLabel) != g.nodeLabelIDs.end());
}

bool edgeLabelExists(AttributedGraph& g, const std::string& edgeLabel) {
  return (g.edgeLabelIDs.find(edgeLabel) != g.edgeLabelIDs.end());
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

#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
void matchEdgesAfterGraphSimulation(QueryGraph& qG, QueryGraph& dG) {
  galois::do_all(
      galois::iterate(dG.begin(), dG.end()),
      [&](auto dn) {
        auto& dData = dG.getData(dn);

        if (dData.matched) {
          for (auto qn : qG) { // multiple matches
            uint64_t mask = (1 << qn);
            if (dData.matched & mask) {
              for (auto qe : qG.edges(qn)) {
                auto qeData = qG.getEdgeData(qe);
                auto qDst   = qG.getEdgeDst(qe);

                for (auto de : dG.edges(dn)) {
                  auto& deData = dG.getEdgeData(de);
                  auto dDst    = dG.getEdgeDst(de);
                  // if (dn < dDst) { // match only one of the symmetric edges
                  if (matchEdgeLabel(qeData, deData)) {
                    auto& dDstData = dG.getData(dDst);
                    if (dDstData.matched & (1 << qDst)) {
                      deData.matched |= 1 << *qe;
                    }
                  }
                  //}
                }
              }
            }
          }
        }
      },
      galois::loopname("MatchNeighborEdges"));
}
#endif

void runGraphSimulationOld(QueryGraph& qG, QueryGraph& dG, EventLimit limit,
                           EventWindow window,
                           bool queryNodeHasMoreThan2Edges) {
  std::vector<std::string> dummy1;
  std::vector<std::string> dummy2;
  matchNodesUsingGraphSimulation(qG, dG, true, limit, window,
                                 queryNodeHasMoreThan2Edges, dummy1, dummy2);
  // matchEdgesAfterGraphSimulation(qG, dG);
}

void runGraphSimulation(QueryGraph& qG, QueryGraph& dG, EventLimit limit,
                        EventWindow window, bool queryNodeHasMoreThan2Edges,
                        std::vector<std::string>& nodeContains,
                        std::vector<std::string>& nodeNames) {
  matchNodesUsingGraphSimulation(qG, dG, true, limit, window,
                                 queryNodeHasMoreThan2Edges, nodeContains,
                                 nodeNames);
  // matchEdgesAfterGraphSimulation(qG, dG);
}

void findShortestPaths(QueryGraph& graph, uint32_t srcQueryNode,
                       uint32_t dstQueryNode, QueryEdgeData qeData,
                       uint32_t matchedQueryNode, uint32_t matchedQueryEdge) {
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

void findAllPaths(QueryGraph& graph, uint32_t srcQueryNode,
                  uint32_t dstQueryNode, QueryEdgeData qeData,
                  uint32_t matchedQueryNode, uint32_t matchedQueryEdge) {
  galois::LargeArray<std::atomic<uint32_t>> visited; // require only 2 bits
  visited.allocateInterleaved(graph.size());

  using WorkQueue = galois::InsertBag<QueryGraph::GraphNode>;
  WorkQueue w[2];
  WorkQueue* cur  = &w[0];
  WorkQueue* next = &w[1];

  // add source and destination nodes to the work-list
  galois::do_all(
      galois::iterate(graph.begin(), graph.end()),
      [&](auto n) {
        visited[n] = 0;

        auto& data    = graph.getData(n);
        uint64_t mask = (1 << srcQueryNode);
        if (data.matched & mask) {
          visited[n] |= 1; // 1st bit
          next->push_back(n);
        }
        mask = (1 << dstQueryNode);
        if (data.matched & mask) {
          visited[n] |= 2; // 2nd bit
          next->push_back(n);
        }
      },
      galois::loopname("ResetVisited"));

  auto sizeNext = std::distance(next->begin(), next->end());

  // loop until no more data nodes are left to traverse
  while (sizeNext > 0) {
    std::swap(cur, next);
    next->clear();

    // traverse edges
    galois::do_all(
        galois::iterate(*cur),
        [&](auto n) {
          uint64_t srcMask = (1 << srcQueryNode);
          uint64_t dstMask = (1 << dstQueryNode);
          for (auto edge : graph.edges(n)) {
            auto deData = graph.getEdgeData(edge);
            if (matchEdgeLabel(qeData, deData)) {
              auto dst                 = graph.getEdgeDst(edge);
              uint32_t old_visited_dst = visited[dst];
              while ((old_visited_dst & visited[n]) != visited[n]) {
                uint32_t new_visited_dst = old_visited_dst | visited[n];
                if (visited[dst].compare_exchange_weak(
                        old_visited_dst, new_visited_dst,
                        std::memory_order_relaxed)) {
                  auto& data = graph.getData(dst);
                  // do not add source or destination to the work-list again
                  if (!(data.matched & srcMask) && !(data.matched & dstMask)) {
                    next->push_back(dst);
                  }
                }
              }
            }
          }
        },
        galois::loopname("TraverseEdges"));

    sizeNext = std::distance(next->begin(), next->end());
  }

  // match visited nodes and edges : can be merged with the above do_all?
  galois::do_all(
      galois::iterate(graph.begin(), graph.end()),
      [&](auto n) {
        if (visited[n] == 3) {
          auto& data       = graph.getData(n);
          uint64_t srcMask = (1 << srcQueryNode);
          uint64_t dstMask = (1 << dstQueryNode);
          if (!(data.matched & srcMask) && !(data.matched & dstMask)) {
            data.matched |= 1 << matchedQueryNode;
          }
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
          for (auto edge : graph.edges(n)) {
            auto dst = graph.getEdgeDst(edge);
            if (visited[dst] == 3) {
              auto& edgeData = graph.getEdgeData(edge);
              edgeData.matched |= 1 << matchedQueryEdge;
            }
          }
#endif
        }
      },
      galois::loopname("MatchNodesInPath"));

  // un-match source and destination nodes : can be merged with the above
  // do_all?
  galois::do_all(
      galois::iterate(graph.begin(), graph.end()),
      [&](auto n) {
        auto& data    = graph.getData(n);
        uint64_t mask = (1 << srcQueryNode);
        if (data.matched & mask) {
          if (visited[n] != 3) {
            data.matched &= ~mask; // no longer a match
          }
        }
        mask = (1 << dstQueryNode);
        if (data.matched & mask) {
          if (visited[n] != 3) {
            data.matched &= ~mask; // no longer a match
          }
        }
      },
      galois::loopname("MatchSourceDestination"));
}

template <bool useWindow>
void matchNodeWithRepeatedActionsSelf(QueryGraph& graph, uint32_t nodeLabel,
                                      uint32_t action, EventWindow window) {
  galois::do_all(
      galois::iterate(graph.begin(), graph.end()),
      [&](auto n) {
        auto& data = graph.getData(n);
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
        if ((data.label & nodeLabel) == nodeLabel)
#endif
        {
          unsigned numActions        = 0;
          QueryGraph::GraphNode prev = 0;
          for (auto e : graph.edges(n)) {
            auto& eData = graph.getEdgeData(e);
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
            if (useWindow) {
              if ((eData.timestamp > window.endTime) ||
                  (eData.timestamp < window.startTime)) {
                continue; // skip this edge since it is not in the
                          // time-span of interest
              }
            }
#endif
            if ((eData & action) == action) {
              ++numActions;
              if (numActions == 1) {
                prev = graph.getEdgeDst(e);
              } else {
                if (prev != graph.getEdgeDst(e)) {
                  data.matched = 1;
                  break;
                }
              }
            }
          }
        }
      },
      galois::loopname("MatchNodes"));

  // match destination of matched nodes
  galois::do_all(
      galois::iterate(graph.begin(), graph.end()),
      [&](auto n) {
        auto& data = graph.getData(n);
        if (data.matched & 1) {
          for (auto e : graph.edges(n)) {
            auto& eData = graph.getEdgeData(e);
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
            if (useWindow) {
              if ((eData.timestamp > window.endTime) ||
                  (eData.timestamp < window.startTime)) {
                continue; // skip this edge since it is not in the
                          // time-span of interest
              }
            }
#endif
            if ((eData & action) == action) {
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
              eData.matched = 1;
#endif
              auto dst      = graph.getEdgeDst(e);
              auto& dstData = graph.getData(dst);
              dstData.matched |= 2; // atomicity not required
            }
          }
        }
      },
      galois::loopname("MatchNodesDsts"));
}

void matchNodeWithRepeatedActions(QueryGraph& graph, uint32_t nodeLabel,
                                  uint32_t action, EventWindow window) {
  // initialize matched
  resetMatchedStatus(graph);

  // match nodes
  if (window.valid) {
    matchNodeWithRepeatedActionsSelf<true>(graph, nodeLabel, action, window);
  } else {
    matchNodeWithRepeatedActionsSelf<false>(graph, nodeLabel, action, window);
  }
}

template <bool useWindow>
void matchNodeWithTwoActionsSelf(QueryGraph& graph, uint32_t nodeLabel,
                                 uint32_t action1, uint32_t dstNodeLabel1,
                                 uint32_t action2, uint32_t dstNodeLabel2,
                                 EventWindow window) {
  galois::do_all(
      galois::iterate(graph.begin(), graph.end()),
      [&](auto n) {
        auto& data = graph.getData(n);
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
        if ((data.label & nodeLabel) == nodeLabel)
#endif
        {
          bool foundAction1 = false;
          bool foundAction2 = false;
          for (auto e : graph.edges(n)) {
            auto& eData = graph.getEdgeData(e);
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
            if (useWindow) {
              if ((eData.timestamp > window.endTime) ||
                  (eData.timestamp < window.startTime)) {
                continue; // skip this edge since it is not in the
                          // time-span of interest
              }
            }
#endif
            bool mayAction1 = ((eData & action1) == action1);
            bool mayAction2 = ((eData & action2) == action2);
            if (mayAction1 || mayAction2) {
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
              auto dst      = graph.getEdgeDst(e);
              auto& dstData = graph.getData(dst);
#endif
              if (mayAction1
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
                  && ((dstData.label & dstNodeLabel1) == dstNodeLabel1)
#endif
              ) {
                foundAction1 = true;
              } else if (mayAction2
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
                         && ((dstData.label & dstNodeLabel2) == dstNodeLabel2)
#endif
              ) {
                foundAction2 = true;
              }
            }
          }
          if (foundAction1 && foundAction2) {
            data.matched = 1;
          }
        }
      },
      galois::loopname("MatchNodes"));

  // match destination of matched nodes
  galois::do_all(
      galois::iterate(graph.begin(), graph.end()),
      [&](auto n) {
        auto& data = graph.getData(n);
        if (data.matched & 1) {
          for (auto e : graph.edges(n)) {
            auto& eData = graph.getEdgeData(e);
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
            if (useWindow) {
              if ((eData.timestamp > window.endTime) ||
                  (eData.timestamp < window.startTime)) {
                continue; // skip this edge since it is not in the
                          // time-span of interest
              }
            }
#endif
            bool mayAction1 = ((eData & action1) == action1);
            bool mayAction2 = ((eData & action2) == action2);
            if (mayAction1 || mayAction2) {
              auto dst      = graph.getEdgeDst(e);
              auto& dstData = graph.getData(dst);
              if (mayAction1
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
                  && ((dstData.label & dstNodeLabel1) == dstNodeLabel1)
#endif
              ) {
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
                eData.matched = 1;
#endif
                dstData.matched |= 2; // atomicity not required
              } else if (mayAction2
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
                         && ((dstData.label & dstNodeLabel2) == dstNodeLabel2)
#endif
              ) {
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
                eData.matched = 1;
#endif
                dstData.matched |= 4; // atomicity not required
              }
            }
          }
        }
      },
      galois::loopname("MatchNodesDsts"));
}

void matchNodeWithTwoActions(QueryGraph& graph, uint32_t nodeLabel,
                             uint32_t action1, uint32_t dstNodeLabel1,
                             uint32_t action2, uint32_t dstNodeLabel2,
                             EventWindow window) {
  // initialize matched
  resetMatchedStatus(graph);

  // match nodes
  if (window.valid) {
    matchNodeWithTwoActionsSelf<true>(graph, nodeLabel, action1, dstNodeLabel1,
                                      action2, dstNodeLabel2, window);
  } else {
    matchNodeWithTwoActionsSelf<false>(graph, nodeLabel, action1, dstNodeLabel1,
                                       action2, dstNodeLabel2, window);
  }
}

/**
 * @todo doxygen
 */
template <bool useWindow>
void matchNeighborsDsts(QueryGraph& graph, QueryGraph::GraphNode node, uint32_t,
                        uint32_t action, uint32_t neighborLabel,
                        EventWindow window) {
  galois::do_all(
      galois::iterate(graph.edges(node).begin(), graph.edges(node).end()),
      [&](auto e) {
        auto& eData = graph.getEdgeData(e);
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
        if (!useWindow || ((eData.timestamp <= window.endTime) &&
                           (eData.timestamp >= window.startTime))) {
#endif
          if ((eData & action) == action) {
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
            eData.matched = 1;
#endif
            auto dst      = graph.getEdgeDst(e);
            auto& dstData = graph.getData(dst);
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
            if ((dstData.label & neighborLabel) == neighborLabel)
#endif
            {
              dstData.matched |= 1; // atomicity not required
            }
          }
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
        } else {
          // skip this edge since it is not in the time-span of interest
        }
#endif
      },
      galois::loopname("MatchNodesDsts"));
}

void matchNeighbors(QueryGraph& graph, QueryGraph::GraphNode node,
                    uint32_t nodeLabel, uint32_t action, uint32_t neighborLabel,
                    EventWindow window) {
  // initialize matched
  resetMatchedStatus(graph);

  // match destinations of node
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
  assert((graph.getData(node).label & nodeLabel) == nodeLabel);
#endif
  if (window.valid) {
    matchNeighborsDsts<true>(graph, node, nodeLabel, action, neighborLabel,
                             window);
  } else {
    matchNeighborsDsts<false>(graph, node, nodeLabel, action, neighborLabel,
                              window);
  }
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

// note right now it's doing the exact same thing as countMatchedNodes above
size_t countMatchedNeighbors(QueryGraph& graph, QueryGraph::GraphNode node) {
  galois::GAccumulator<size_t> numMatched;
  // do not count the same node twice (multiple edges to the same node)
  galois::do_all(
      galois::iterate(graph.begin(), graph.end()),
      [&](auto n) {
        auto& data = graph.getData(n);
        if (data.matched) {
          numMatched += 1;
        }
      },
      galois::loopname("CountMatchedNeighbors"));
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

size_t countMatchedNeighborEdges(QueryGraph& graph,
                                 QueryGraph::GraphNode node) {
  galois::GAccumulator<size_t> numMatched;
  galois::do_all(
      galois::iterate(graph.edges(node).begin(), graph.edges(node).end()),
      [&](auto e) {
        auto eData = graph.getEdgeData(e);
        if (eData.matched) {
          numMatched += 1; // count the same neighbor for each edge to it
        }
      },
      galois::loopname("CountMatchedEdges"));
  return numMatched.reduce();
}
#endif
