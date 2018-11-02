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
                size_t numQueryEdges) {
  //getNodeLabelMask(*dataGraph, "process;network");
  // build node types and prefix sum of edges
  size_t numQueryNodes = 0;
  std::vector<const char*> nodeTypes;
  std::vector<size_t> prefixSum;
  for (size_t j = 0; j < numQueryEdges; ++j) {
    size_t srcID = std::stoi(queryEdges[j].caused_by.id);
    size_t dstID = std::stoi(queryEdges[j].acted_on.id);

    if (srcID >= numQueryNodes) {
      numQueryNodes = srcID + 1;
    }
    if (dstID >= numQueryNodes) {
      numQueryNodes = dstID + 1;
    }
    nodeTypes.resize(numQueryNodes, NULL);
    prefixSum.resize(numQueryNodes, 0);

    if (nodeTypes[srcID] == NULL) {
      nodeTypes[srcID] = queryEdges[j].caused_by.name;
    } else {
      assert(nodeTypes[srcID] == queryEdges[j].caused_by.name);
    }
    if (nodeTypes[dstID] == NULL) {
      nodeTypes[dstID] = queryEdges[j].acted_on.name;
    } else {
      assert(nodeTypes[dstID] == queryEdges[j].acted_on.name);
    }

    prefixSum[srcID]++;
    prefixSum[dstID]++;
  }
  for (size_t i = 1; i < numQueryNodes; ++i) {
    prefixSum[i] += prefixSum[i-1];
  }
  assert(prefixSum[numQueryNodes - 1] == (numQueryEdges * 2));
  for (size_t i = numQueryNodes - 1; i >= 1; --i) {
    prefixSum[i] = prefixSum[i-1];
  }
  prefixSum[0] = 0;

  // check for trivial absence of query
  for (size_t i = 0; i < numQueryNodes; ++i) {
    assert(nodeTypes[i] != NULL);
    if (dataGraph->nodeLabelIDs.find(nodeTypes[i]) ==
       dataGraph->nodeLabelIDs.end()) {
      // query node label does not exist in the data graph
      resetMatchedStatus(dataGraph->graph);
      return 0;
    }
  }
  for (size_t j = 0; j < numQueryEdges; ++j) {
    if (dataGraph->edgeLabelIDs.find(queryEdges[j].label) ==
       dataGraph->edgeLabelIDs.end()) {
      // query edge label does not exist in the data graph
      resetMatchedStatus(dataGraph->graph);
      return 0;
    }
  }

  // build query graph
  Graph queryGraph;
  queryGraph.allocateFrom(numQueryNodes, numQueryEdges * 2);
  queryGraph.constructNodes();
  for (size_t i = 0; i < numQueryNodes; ++i) {
    queryGraph.getData(i).label = getNodeLabelMask(*dataGraph, nodeTypes[i]);
  }
  for (size_t j = 0; j < numQueryEdges; ++j) {
    size_t srcID = std::stoi(queryEdges[j].caused_by.id);
    size_t dstID = std::stoi(queryEdges[j].acted_on.id);
    queryGraph.constructEdge(prefixSum[srcID]++, dstID,
        EdgeData(getEdgeLabelMask(*dataGraph, queryEdges[j].label), queryEdges[j].timestamp));
    queryGraph.constructEdge(prefixSum[dstID]++, srcID,
        EdgeData(getEdgeLabelMask(*dataGraph, queryEdges[j].label), queryEdges[j].timestamp));
  }
  for (size_t i = 0; i < numQueryNodes; ++i) {
    queryGraph.fixEndEdge(i, prefixSum[i]);
  }

  // run graph simulation
  runGraphSimulation(queryGraph, dataGraph->graph, limit, window, false);
  return countMatchedEdges(dataGraph->graph);
}

