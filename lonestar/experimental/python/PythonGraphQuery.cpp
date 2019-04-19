/*
 * This file belongs to the Galois project, a C++ library for exploiting parallelism.
 * The code is being released under the terms of the 3-Clause BSD License (a
 * copy is located in LICENSE.txt at the top-level directory).
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

#include "PythonGraph.h"

size_t matchQuery(AttributedGraph* dataGraph,
                  EventLimit limit,
                  EventWindow window,
                  MatchedEdge* queryEdges,
                  size_t numQueryEdges,
                  const char** filters) {
  // build node types and prefix sum of edges
  size_t numQueryNodes = 0;
  std::vector<const char*> nodeTypes;
  std::vector<std::string> nodeContains;
  std::vector<size_t> prefixSum;
  std::vector<std::pair<size_t, size_t>> starEdgeList;
  std::vector<EdgeData> starEdgeData;

  for (size_t j = 0; j < numQueryEdges; ++j) {
    // ids of nodes of this edge
    size_t srcID = std::stoi(queryEdges[j].caused_by.id);
    size_t dstID = std::stoi(queryEdges[j].acted_on.id);
    // grab strings to filter nodes against
    std::string s1 = std::string(filters[2 * j]);
    std::string s2 = std::string(filters[2 * j + 1]);

    if (srcID >= numQueryNodes) {
      numQueryNodes = srcID + 1;
    }
    if (dstID >= numQueryNodes) {
      numQueryNodes = dstID + 1;
    }
    nodeTypes.resize(numQueryNodes, NULL);
    nodeContains.resize(numQueryNodes, "");
    prefixSum.resize(numQueryNodes, 0);

    // node types check
    if (nodeTypes[srcID] == NULL) {
      nodeTypes[srcID] = queryEdges[j].caused_by.name;
    } else {
      assert(std::string(nodeTypes[srcID]) ==
               std::string(queryEdges[j].caused_by.name));
    }
    if (nodeTypes[dstID] == NULL) {
      nodeTypes[dstID] = queryEdges[j].acted_on.name;
    } else {
      assert(std::string(nodeTypes[dstID]) ==
               std::string(queryEdges[j].acted_on.name));
    }
    // node contains check
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

    // check if query edge is a * edge
    if (std::string(queryEdges[j].label).find("*") == std::string::npos) {
      prefixSum[srcID]++;
      prefixSum[dstID]++;
    } else {
      starEdgeList.push_back(std::make_pair(srcID, dstID));
    }
  }

  for (std::string i : nodeContains) {
    galois::gDebug("Contains ", i, "\n");
  }

  // ignore edges that have the star label
  auto actualNumQueryEdges = numQueryEdges - starEdgeList.size();

  for (size_t i = 1; i < numQueryNodes; ++i) {
    prefixSum[i] += prefixSum[i-1];
  }
  assert(prefixSum[numQueryNodes - 1] == (actualNumQueryEdges * 2));
  for (size_t i = numQueryNodes - 1; i >= 1; --i) {
    prefixSum[i] = prefixSum[i-1];
  }
  prefixSum[0] = 0;

  // check for trivial absence of query
  // node label checking; make sure labels exist
  for (size_t i = 0; i < numQueryNodes; ++i) {
    assert(nodeTypes[i] != NULL);
    if (!getNodeLabelMask(*dataGraph, nodeTypes[i]).first) {
      // query node label does not exist in the data graph
      resetMatchedStatus(dataGraph->graph);
      return 0;
    }
  }

  // TODO refactor code below
  // edge label checking; make sure labels exist
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

        galois::gPrint("* Restrictions ", restrictions, "\n");

        if (!edgeResult.first) {
          resetMatchedStatus(dataGraph->graph);
          return 0;
        }

        // pass existence check: save mask
        uint32_t label = edgeResult.second.first | edgeResult.second.second;
        uint64_t matched = edgeResult.second.first;
        starEdgeData.emplace_back(label, 0, matched);
      } else {
        // no restrictions, 0, 0 means match anything
        starEdgeData.emplace_back(0, 0, 0);
      }
    }
  }

  // make sure pairs are even
  GALOIS_ASSERT(starEdgeList.size() == starEdgeData.size());

  // build query graph
  Graph queryGraph;
  queryGraph.allocateFrom(numQueryNodes, actualNumQueryEdges * 2);
  queryGraph.constructNodes();
  for (size_t i = 0; i < numQueryNodes; ++i) {
    // first is the "YES" query, second is the "NO" query
    std::pair<uint32_t, uint32_t> masks =
      getNodeLabelMask(*dataGraph, nodeTypes[i]).second;
    queryGraph.getData(i).label = masks.first | masks.second;
    queryGraph.getData(i).matched = masks.first;
  }
  for (size_t j = 0; j < numQueryEdges; ++j) {
    if (std::string(queryEdges[j].label).find("*") == std::string::npos) {
      size_t srcID = std::stoi(queryEdges[j].caused_by.id);
      size_t dstID = std::stoi(queryEdges[j].acted_on.id);

      std::pair<uint32_t, uint32_t> edgeMasks =
        getEdgeLabelMask(*dataGraph, queryEdges[j].label).second;
      uint32_t label = edgeMasks.first | edgeMasks.second;
      uint64_t matched = edgeMasks.first;

      // symmetric edge; construct in both directions
      queryGraph.constructEdge(prefixSum[srcID]++, dstID,
                               EdgeData(label, queryEdges[j].timestamp, matched));

      queryGraph.constructEdge(prefixSum[dstID]++, srcID,
                               EdgeData(label, queryEdges[j].timestamp, matched));
    }
  }

  for (size_t i = 0; i < numQueryNodes; ++i) {
    queryGraph.fixEndEdge(i, prefixSum[i]);
  }

  // do special handling if * edges were used in the query edges
  if (starEdgeList.size() > 0) {
    matchNodesUsingGraphSimulation(queryGraph, dataGraph->graph, true, limit,
                                   window, false, nodeContains,
                                   dataGraph->nodeNames);
    uint32_t currentStar = 0;
    for (std::pair<size_t, size_t>& sdPair : starEdgeList) {
      findShortestPaths(dataGraph->graph, sdPair.first, sdPair.second, 
                        starEdgeData[currentStar],
                        numQueryNodes + currentStar,
                        actualNumQueryEdges + currentStar);
      currentStar++;
    }
    matchNodesUsingGraphSimulation(queryGraph, dataGraph->graph, false, limit,
                                   window, false, nodeContains,
                                   dataGraph->nodeNames);
    matchEdgesAfterGraphSimulation(queryGraph, dataGraph->graph);
  } else {
    // run graph simulation
    runGraphSimulation(queryGraph, dataGraph->graph, limit, window, false, nodeContains,
                       dataGraph->nodeNames);
  }

  return countMatchedEdges(dataGraph->graph);
}
