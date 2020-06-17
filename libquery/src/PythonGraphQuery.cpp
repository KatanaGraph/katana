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

#include "querying/PythonGraph.h"
#include "querying/CypherCompiler.h"
#include "querying/SubgraphQuery.h"
#include "galois/Timer.h"
#include <fstream>

void printIR(std::vector<MatchedEdge>& ir, std::vector<const char*> filters) {
  std::ofstream out(".temp_ir.q");
  for (size_t i = 0; i < ir.size(); ++i) {
    out << ir[i].caused_by.name << ",";
    out << ir[i].caused_by.id << ",";
    out << filters[2 * i] << ",";
    out << ir[i].label << ",";
    out << ir[i].timestamp << ",";
    out << ir[i].acted_on.name << ",";
    out << ir[i].acted_on.id << ",";
    out << filters[2 * i + 1] << "\n";
  }
  out.close();
}

size_t matchCypherQuery(AttributedGraph* dataGraph, EventLimit limit,
                        EventWindow window, const char* cypherQueryStr,
                        bool useGraphSimulation) {
  galois::StatTimer compileTime("CypherCompileTime");

  // parse query, get AST
  compileTime.start();
  CypherCompiler cc;
  cc.compile(cypherQueryStr);
  compileTime.stop();

#ifndef NDEBUG
  printIR(cc.getIR(), cc.getFilters());
#endif

  // do actual matching
  // the things passed from the compiler are the following:
  // - edges of a query graph
  // - filters on nodes (contains)
  size_t numMatches =
      matchQuery(dataGraph, limit, window, cc.getIR().data(), cc.getIR().size(),
                 cc.getFilters().data(), useGraphSimulation);
  cc.getIR().clear();
  cc.getFilters().clear();

  return numMatches;
}

size_t matchQuery(AttributedGraph* dataGraph, EventLimit limit,
                  EventWindow window, MatchedEdge* queryEdges,
                  size_t numQueryEdges, const char** filters,
                  bool useGraphSimulation) {
  // build node types and prefix sum of edges
  // tracks number of nodes to be constructed in the query graph; unknown
  // until all query edges are looped over
  size_t numQueryNodes = 0;
  std::vector<const char*> nodeTypes;
  std::vector<std::string> nodeContains;
  std::vector<size_t> prefixSum;
  std::vector<std::pair<size_t, size_t>> starEdgeList;
  std::vector<QueryEdgeData> starEdgeData;
  galois::StatTimer compileTime("IRCompileTime");

  compileTime.start();
  // loop through all edges parsed from compiler and do bookkeeping
  for (size_t j = 0; j < numQueryEdges; ++j) {
    // ids of nodes of this edge
    // assumes that the id is an int
    size_t srcID = std::stoi(queryEdges[j].caused_by.id);
    size_t dstID = std::stoi(queryEdges[j].acted_on.id);
    // grab strings to filter nodes against
    std::string s1 = std::string(filters[2 * j]);
    std::string s2 = std::string(filters[2 * j + 1]);

    // allocate more memory for nodes if node ids go past what we currently
    // have
    if (srcID >= numQueryNodes) {
      numQueryNodes = srcID + 1;
    }
    if (dstID >= numQueryNodes) {
      numQueryNodes = dstID + 1;
    }
    nodeTypes.resize(numQueryNodes, NULL);
    nodeContains.resize(numQueryNodes, "");
    prefixSum.resize(numQueryNodes, 0);

    // node types check: save node types for each id
    if (nodeTypes[srcID] == NULL) {
      nodeTypes[srcID] = queryEdges[j].caused_by.name;
    } else {
      // assert(std::string(nodeTypes[srcID]) ==
      //          std::string(queryEdges[j].caused_by.name));
    }
    if (nodeTypes[dstID] == NULL) {
      nodeTypes[dstID] = queryEdges[j].acted_on.name;
    } else {
      // assert(std::string(nodeTypes[dstID]) ==
      //          std::string(queryEdges[j].acted_on.name));
    }

    // node contains check; save string filters for each node
    if (nodeContains[srcID] == "") {
      nodeContains[srcID] = s1;
    } else {
      assert(nodeContains[srcID] == s1);
    }
    if (nodeContains[dstID] == "") {
      nodeContains[dstID] = s2;
    } else {
      assert(nodeContains[dstID] == s2);
    }

    // check if query edge is a * edge; if not, do degree management
    if (std::string(queryEdges[j].label).find("*") == std::string::npos) {
      // not found; increment edge count on this node by 1
      prefixSum[srcID]++;
    } else {
      starEdgeList.push_back(std::make_pair(srcID, dstID));
    }
  }

  for (std::string i : nodeContains) {
    // debug print for limitations on nodes
    galois::gDebug("Contains ", i);
  }

  // ignore edges that have the star label when constructing query graph
  auto actualNumQueryEdges = numQueryEdges - starEdgeList.size();

  // get number of edges per node
  for (size_t i = 1; i < numQueryNodes; ++i) {
    prefixSum[i] += prefixSum[i - 1];
  }
  assert(prefixSum[numQueryNodes - 1] == actualNumQueryEdges);
  // shift prefix sum to the right; the result is an array that gives the
  // starting point for where to write new edges for a particular vertex
  for (size_t i = numQueryNodes - 1; i >= 1; --i) {
    prefixSum[i] = prefixSum[i - 1];
  }
  prefixSum[0] = 0;

  // do some trivial checking to make sure we even need to bother matching

#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
  // check for trivial absence of query by node label checking; make sure
  // labels exist; if they don't, it's an easy no match
  for (size_t i = 0; i < numQueryNodes; ++i) {
    assert(nodeTypes[i] != NULL);
    if (!getNodeLabelMask(*dataGraph, nodeTypes[i]).first) {
      // query node label does not exist in the data graph
      resetMatchedStatus(dataGraph->graph);
      return 0;
    }
  }
#endif

  // TODO refactor code below
  // edge label checking to  make sure labels exist in the graph; if not,
  // easy no match
  for (size_t j = 0; j < numQueryEdges; ++j) {
    std::string curEdge = std::string(queryEdges[j].label);
    if (curEdge.find("*") == std::string::npos) {
      if (!getEdgeLabelMask(*dataGraph, queryEdges[j].label).first) {
        // query edge label does not exist in the data graph
        resetMatchedStatus(dataGraph->graph);
        return 0;
      }
    } else {
      // * label: check if there are restrictions on it (i.e. only traverse
      // certain edges)
      if (curEdge.find("=") != std::string::npos) {
        // *=... means restrictions exist; get them
        std::string restrictions = curEdge.substr(2);
        std::pair<bool, std::pair<uint32_t, uint32_t>> edgeResult =
            getEdgeLabelMask(*dataGraph, restrictions);

        galois::gDebug("* Restrictions ", restrictions, "\n");

        if (!edgeResult.first) {
          resetMatchedStatus(dataGraph->graph);
          return 0;
        }

        // pass existence check: save mask
        uint32_t label = edgeResult.second.first
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
                         | edgeResult.second.second;
        uint64_t matched = edgeResult.second.first;
        starEdgeData.emplace_back(label, 0, matched);
#else
            ;
        starEdgeData.emplace_back(label);
#endif
      } else {
        // no restrictions, 0, 0 means match anything
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
        starEdgeData.emplace_back(0, 0, 0);
#else
        starEdgeData.emplace_back(0);
#endif
      }
    }
  }

  // make sure pairs are even
  GALOIS_ASSERT(starEdgeList.size() == starEdgeData.size());

  // build query graph
  QueryGraph queryGraph;
  queryGraph.allocateFrom(numQueryNodes, actualNumQueryEdges);
  queryGraph.constructNodes();
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
  for (size_t i = 0; i < numQueryNodes; ++i) {
    // first is the "YES" query, second is the "NO" query
    std::pair<uint32_t, uint32_t> masks =
        getNodeLabelMask(*dataGraph, nodeTypes[i]).second;
    queryGraph.getData(i).label   = masks.first | masks.second;
    queryGraph.getData(i).matched = masks.first;
  }
#endif
  for (size_t j = 0; j < numQueryEdges; ++j) {
    if (std::string(queryEdges[j].label).find("*") == std::string::npos) {
      size_t srcID = std::stoi(queryEdges[j].caused_by.id);
      size_t dstID = std::stoi(queryEdges[j].acted_on.id);

      std::pair<uint32_t, uint32_t> edgeMasks =
          getEdgeLabelMask(*dataGraph, queryEdges[j].label).second;
      uint32_t label = edgeMasks.first;
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
      label |= edgeMasks.second;
      uint64_t matched = edgeMasks.first;
#endif

      queryGraph.constructEdge(prefixSum[srcID]++, dstID,
                               QueryEdgeData(label
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
                                             ,
                                             queryEdges[j].timestamp, matched
#endif
                                             ));
    }
  }

  for (size_t i = 0; i < numQueryNodes; ++i) {
    queryGraph.fixEndEdge(i, prefixSum[i]);
  }

  queryGraph.constructAndSortIndex();
  compileTime.stop();

  // at this point query graph is constructed; can do matching using it

  galois::StatTimer simulationTime("GraphSimulationTime");
  // do special handling if * edges were used in the query edges
  if (starEdgeList.size() > 0) {
    assert(useGraphSimulation);

    simulationTime.start();
    // first, match query graph without star
    matchNodesUsingGraphSimulation(queryGraph, dataGraph->graph, true, limit,
                                   window, false, nodeContains,
                                   dataGraph->nodeNames);
    // handle stars
    uint32_t currentStar = 0;
    for (std::pair<size_t, size_t>& sdPair : starEdgeList) {
      findShortestPaths(dataGraph->graph, sdPair.first, sdPair.second,
                        starEdgeData[currentStar], numQueryNodes + currentStar,
                        actualNumQueryEdges + currentStar);
      currentStar++;
    }
    // rematch taking star into account (handling star should have limited scope
    // of possible matches)
    matchNodesUsingGraphSimulation(queryGraph, dataGraph->graph, false, limit,
                                   window, false, nodeContains,
                                   dataGraph->nodeNames);
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
    matchEdgesAfterGraphSimulation(queryGraph, dataGraph->graph);
    simulationTime.stop();
    return countMatchedEdges(dataGraph->graph);
#else
    simulationTime.stop();
    return countMatchedNodes(dataGraph->graph);
#endif
  } else if (useGraphSimulation) {
    // run graph simulation before running the subgraph querying
    simulationTime.start();
    runGraphSimulation(queryGraph, dataGraph->graph, limit, window, false,
                       nodeContains, dataGraph->nodeNames);
    simulationTime.stop();
    return subgraphQuery<true>(queryGraph, dataGraph->graph);
  } else {
    return subgraphQuery<false>(queryGraph, dataGraph->graph);
  }
}
