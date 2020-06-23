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
 * @file PythonGraph.cpp
 *
 * Implementations for AttributedGraph construction/modification
 * declared in PythonGraph.h.
 */

#include "querying/PythonGraph.h"
#include "galois/DynamicBitset.h"

#include <iostream>
#include <fstream>
#include <boost/serialization/map.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/unordered_map.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>

//////////////////////////////////////////
// APIs for PythonGraph
//////////////////////////////////////////

AttributedGraph* createGraph() {
  AttributedGraph* g = new AttributedGraph();
  return g;
}

void deleteGraph(AttributedGraph* g) { delete g; }

void saveGraph(AttributedGraph* g, const char* filename) {
  // test prints
  // for (auto d : g->graph) {
  //  galois::gPrint(d, " ", g->index2UUID[d], "\n");

  //  for (auto e : g->graph.edges(d)) {
  //    auto bleh = g->graph.getEdgeDst(e);
  //    galois::gPrint("     ", "to ", bleh, " ", g->index2UUID[bleh], "\n");
  //  }
  //}

  std::ofstream file(filename, std::ios::out | std::ios::binary);
  boost::archive::binary_oarchive oarch(file);
  g->graph.serializeGraph(oarch);
  oarch << g->nodeLabelNames;

  oarch << g->nodeLabelIDs;
  oarch << g->edgeLabelNames;
  oarch << g->edgeLabelIDs;
  oarch << g->nodeIndices;
  oarch << g->index2UUID;
  oarch << g->nodeNames;
  // node/edge attributes
  oarch << g->nodeAttributes;
  oarch << g->nodeAttributeTypes;
  oarch << g->edgeAttributes;
  oarch << g->edgeAttributeTypes;

  // test prints
  // for (auto& pair : g->nodeLabelIDs) {
  //  printf("nodelabel pair first is %s second %u\n", pair.first.c_str(),
  //  pair.second);
  //}
  // for (auto& pair : g->nodeAttributes) {
  //  printf("pair first is %s\n", pair.first.c_str());
  //  for (auto s : pair.second) {
  //    printf("  - %s\n", s.c_str());
  //  }
  //}

  file.close();
}

void loadGraph(AttributedGraph* g, const char* filename) {
  std::ifstream file(filename, std::ios::in | std::ios::binary);
  boost::archive::binary_iarchive iarch(file);
  g->graph.deSerializeGraph(iarch);
  g->graph.constructAndSortIndex();
  iarch >> g->nodeLabelNames;

  // node label IDs
  iarch >> g->nodeLabelIDs;
  iarch >> g->edgeLabelNames;
  // edge label IDs
  iarch >> g->edgeLabelIDs;
  // node indices
  iarch >> g->nodeIndices;
  iarch >> g->index2UUID;
  iarch >> g->nodeNames;
  iarch >> g->nodeAttributes;
  iarch >> g->nodeAttributeTypes;
  iarch >> g->edgeAttributes;
  iarch >> g->edgeAttributeTypes;

  // test prints
  // for (auto& pair : g->nodeLabelIDs) {
  //  printf("nodelabel pair first is %s second %u\n", pair.first.c_str(),
  //  pair.second);
  //}
  // for (auto& pair : g->nodeAttributes) {
  //  printf("pair first is %s\n", pair.first.c_str());
  //  for (auto s : pair.second) {
  //    printf("  - %s\n", s.c_str());
  //  }
  //}

  file.close();
}

void printGraph(AttributedGraph* g) {
  QueryGraph& graph = g->graph;
  // auto& nodeLabelNames = g->nodeLabelNames;
  auto& edgeLabelNames = g->edgeLabelNames;
  auto& nodeNames      = g->nodeNames;
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
  uint32_t sourceLabelID = 1 << g->nodeLabelIDs["process"];
#endif
  uint64_t numEdges = 0;

  for (auto src : graph) {
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
    auto& srcData = graph.getData(src);
    // only print if source is a process
    if ((srcData.label & sourceLabelID) != sourceLabelID)
      continue;
    auto& srcLabel = g->nodeLabelNames[rightmostSetBitPos(srcData.label)];
#else
    auto srcLabel = 0;
#endif
    auto& srcName = nodeNames[src];
    for (auto e : graph.edges(src)) {
      auto dst = graph.getEdgeDst(e);
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
      auto& dstData = graph.getData(dst);

      if (((dstData.label & sourceLabelID) == sourceLabelID) &&
#else
      if (
#endif
          (dst < src))
        continue;

#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
      auto& dstLabel = g->nodeLabelNames[rightmostSetBitPos(dstData.label)];
#else
      auto dstLabel = 0;
#endif
      auto& dstName   = nodeNames[dst];
      auto& ed        = graph.getEdgeData(e);
      auto& edgeLabel = edgeLabelNames[rightmostSetBitPos(ed)];
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
      auto& edgeTimestamp = ed.timestamp;
      std::cout << edgeTimestamp << ", ";
#endif
      std::cout << srcName << ", " << edgeLabel << ", " << dstName << " ("
                << srcLabel << ", " << dstLabel << ")" << std::endl;
      ++numEdges;
    }
  }
  assert((numEdges * 2) == graph.sizeEdges());
}

void allocateGraph(AttributedGraph* g, size_t numNodes, size_t numEdges,
                   size_t numNodeLabels, size_t numEdgeLabels) {
  g->graph.allocateFrom(numNodes, numEdges);
  g->graph.constructNodes();
  assert(numNodeLabels <= 32);
  g->nodeLabelNames.resize(numNodeLabels);
  assert(numEdgeLabels <= 32);
  g->edgeLabelNames.resize(numEdgeLabels);
  g->index2UUID.resize(numNodes);
  g->nodeNames.resize(numNodes);
}

void allocateGraphLDBC(AttributedGraph* g, size_t numNodes, size_t numEdges,
                       size_t numNodeLabels, size_t numEdgeLabels) {
  g->graph.allocateFrom(numNodes, numEdges);
  g->graph.constructNodes();
  assert(numNodeLabels <= 32);
  g->nodeLabelNames.resize(numNodeLabels);
  assert(numEdgeLabels <= 32);
  g->edgeLabelNames.resize(numEdgeLabels);
}

void fixEndEdge(AttributedGraph* g, uint32_t nodeIndex, uint64_t edgeIndex) {
  g->graph.fixEndEdge(nodeIndex, edgeIndex);
}

void setNewNode(AttributedGraph* g, uint32_t nodeIndex, char* uuid,
                uint32_t GALOIS_UNUSED(labelBitPosition), char* name) {
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
  auto& nd = g->graph.getData(nodeIndex);
  nd.label = 1 << labelBitPosition;
#endif
  g->nodeIndices[uuid]     = nodeIndex;
  g->index2UUID[nodeIndex] = uuid;
  g->nodeNames[nodeIndex]  = name;
}

void setNodeLabel(AttributedGraph* GALOIS_UNUSED(g),
                  uint32_t GALOIS_UNUSED(nodeIndex),
                  uint32_t GALOIS_UNUSED(label)) {
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
  auto& nd = g->graph.getData(nodeIndex);
  nd.label = label;
#endif
}

void setNodeLabelMetadata(AttributedGraph* g, uint32_t labelBitPosition,
                          const char* name) {
  g->nodeLabelNames[labelBitPosition] = name;
  g->nodeLabelIDs[name]               = labelBitPosition;
}

void setEdgeLabelMetadata(AttributedGraph* g, uint32_t labelBitPosition,
                          const char* name) {
  g->edgeLabelNames[labelBitPosition] = name;
  g->edgeLabelIDs[name]               = labelBitPosition;
}

void setExistingNodeAttribute(AttributedGraph* g, uint32_t nodeIndex,
                              const char* key, const char* value) {
  auto& attributes = g->nodeAttributes;
  if (attributes.find(key) == attributes.end()) {
    GALOIS_DIE("node attribute ", key, "doesn't already exist");
  }
  attributes[key][nodeIndex] = value;
}

void constructNewEdge(AttributedGraph* g, uint64_t edgeIndex,
                      uint32_t dstNodeIndex, uint32_t labelBitPosition,
                      uint64_t GALOIS_UNUSED(timestamp)) {
  g->graph.constructEdge(edgeIndex, dstNodeIndex,
                         QueryEdgeData(1 << labelBitPosition
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
                                       ,
                                       timestamp));
#else
                                       ));
#endif
}

void constructEdge(AttributedGraph* g, uint64_t edgeIndex,
                   uint32_t dstNodeIndex, uint32_t label,
                   uint64_t GALOIS_UNUSED(timestamp)) {
  g->graph.constructEdge(edgeIndex, dstNodeIndex,
                         QueryEdgeData(label
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
                                       ,
                                       timestamp));
#else
                                       ));
#endif
}

void setExistingEdgeAttribute(AttributedGraph* g, uint32_t edgeIndex,
                              const char* key, const char* value) {
  auto& attributes = g->edgeAttributes;
  if (attributes.find(key) == attributes.end()) {
    GALOIS_DIE("edge attribute ", key, "doesn't already exist");
  }
  attributes[key][edgeIndex] = value;
}

void addNodeAttributeMap(AttributedGraph* g, const char* key,
                         uint32_t nodeCount) {
  auto& attributes = g->nodeAttributes;
  if (attributes.find(key) == attributes.end()) {
    attributes[key] = std::vector<std::string>();
    attributes[key].resize(nodeCount);
  }
}

void addNodeAttributeType(AttributedGraph* g, const char* key,
                          AttributedType t) {
  auto& attributes = g->nodeAttributeTypes;
  attributes[key]  = t;
}

void addEdgeAttributeMap(AttributedGraph* g, const char* key,
                         uint32_t edgeCount) {
  auto& attributes = g->edgeAttributes;
  if (attributes.find(key) == attributes.end()) {
    attributes[key] = std::vector<std::string>();
    attributes[key].resize(edgeCount);
  }
}

void addEdgeAttributeType(AttributedGraph* g, const char* key,
                          AttributedType t) {
  auto& attributes = g->edgeAttributeTypes;
  attributes[key]  = t;
}
