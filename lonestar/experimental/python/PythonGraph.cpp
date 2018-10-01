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

/**
 * @file PythonGraph.cpp
 *
 * Implementations for AttributedGraph construction/modification
 * declared in PythonGraph.h.
 */

#include "PythonGraph.h"

#include <iostream>
#include <fstream>
#include <boost/serialization/map.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/string.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>

void initGaloisRuntime() {
  static galois::SharedMemSys* G;
  if (G != NULL)
    delete G;
  G = new galois::SharedMemSys();
}

void setNumThreads(int numThreads) {
  galois::setActiveThreads(numThreads < 1 ? 1 : numThreads);
}

int getNumThreads() { return galois::getActiveThreads(); }

//////////////////////////////////////////
// APIs for PythonGraph
//////////////////////////////////////////

AttributedGraph* createGraph() {
  AttributedGraph* g = new AttributedGraph();
  return g;
}

void deleteGraph(AttributedGraph* g) { delete g; }

void saveGraph(AttributedGraph* g, char* filename) {
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
  size_t size = g->nodeAttributes.size();
  oarch << size;
  for (auto& pair : g->nodeAttributes) {
    oarch << pair.first;
    oarch << pair.second;
  }
  size = g->edgeAttributes.size();
  oarch << size;
  for (auto& pair : g->edgeAttributes) {
    oarch << pair.first;
    oarch << pair.second;
  }
  file.close();
}

void loadGraph(AttributedGraph* g, char* filename) {
  std::ifstream file(filename, std::ios::in | std::ios::binary);
  boost::archive::binary_iarchive iarch(file);
  g->graph.deSerializeGraph(iarch);
  iarch >> g->nodeLabelNames;
  iarch >> g->nodeLabelIDs;
  iarch >> g->edgeLabelNames;
  iarch >> g->edgeLabelIDs;
  iarch >> g->nodeIndices;
  iarch >> g->index2UUID;
  iarch >> g->nodeNames;
  size_t size;
  iarch >> size;
  for (size_t i = 0; i < size; ++i) {
    std::string key;
    iarch >> key;
    g->nodeAttributes[key] = std::vector<std::string>();
    iarch >> g->nodeAttributes[key];
  }
  iarch >> size;
  for (size_t i = 0; i < size; ++i) {
    std::string key;
    iarch >> key;
    g->edgeAttributes[key] = std::vector<std::string>();
    iarch >> g->edgeAttributes[key];
  }
  file.close();
}

void printGraph(AttributedGraph* g) {
  Graph& graph = g->graph;
  // auto& nodeLabelNames = g->nodeLabelNames;
  auto& edgeLabelNames = g->edgeLabelNames;
  auto& nodeNames      = g->nodeNames;
  uint32_t sourceLabelID   = 1 << g->nodeLabelIDs["process"];
  uint64_t numEdges    = 0;

  for (auto src : graph) {
    auto& srcData = graph.getData(src);
    // only print if source is a process
    if ((srcData.label & sourceLabelID) != sourceLabelID) continue;
    auto& srcLabel = g->nodeLabelNames[rightmostSetBitPos(srcData.label)];
    auto& srcName  = nodeNames[src];
    for (auto e : graph.edges(src)) {
      auto dst      = graph.getEdgeDst(e);
      auto& dstData = graph.getData(dst);

      if (((dstData.label & sourceLabelID) == sourceLabelID) && (dst < src))
        continue;

      auto& dstLabel   = g->nodeLabelNames[rightmostSetBitPos(dstData.label)];
      auto& dstName        = nodeNames[dst];
      auto& ed             = graph.getEdgeData(e);
      auto& edgeLabel  = edgeLabelNames[rightmostSetBitPos(ed.label)];
      auto& edgeTimestamp  = ed.timestamp;
      std::cout << edgeTimestamp << ", " << srcName << ", " << edgeLabel << ", "
                << dstName << " (" << srcLabel << ", " << dstLabel << ")" <<
                std::endl;
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

void fixEndEdge(AttributedGraph* g, uint32_t nodeIndex, uint64_t edgeIndex) {
  g->graph.fixEndEdge(nodeIndex, edgeIndex);
}

void setNewNode(AttributedGraph* g, uint32_t nodeIndex, char* uuid,
                uint32_t labelBitPosition, char* name) {
  auto& nd                  = g->graph.getData(nodeIndex);
  nd.label                  = 1 << labelBitPosition;
  g->nodeIndices[uuid]      = nodeIndex;
  g->index2UUID[nodeIndex]  = uuid;
  g->nodeNames[nodeIndex]   = name;
}

void setNode(AttributedGraph* g, uint32_t nodeIndex, char* uuid,
             uint32_t label, char* name) {
  auto& nd                  = g->graph.getData(nodeIndex);
  nd.label                  = label;
  g->nodeIndices[uuid]      = nodeIndex;
  g->index2UUID[nodeIndex]  = uuid;
  g->nodeNames[nodeIndex]   = name;
}

void setNodeLabelMetadata(AttributedGraph* g, uint32_t labelBitPosition,
                          char* name) {
  g->nodeLabelNames[labelBitPosition] = name;
  g->nodeLabelIDs[name]               = labelBitPosition;
}

void setEdgeLabelMetadata(AttributedGraph* g, uint32_t labelBitPosition,
                          char* name) {
  g->edgeLabelNames[labelBitPosition] = name;
  g->edgeLabelIDs[name]               = labelBitPosition;
}

void setNodeAttribute(AttributedGraph* g, uint32_t nodeIndex, char* key,
                      char* value) {
  auto& attributes = g->nodeAttributes;
  if (attributes.find(key) == attributes.end()) {
    attributes[key] = std::vector<std::string>();
    attributes[key].resize(g->graph.size());
  }
  attributes[key][nodeIndex] = value;
}

void constructNewEdge(AttributedGraph* g, uint64_t edgeIndex,
                      uint32_t dstNodeIndex, uint32_t labelBitPosition,
                      uint64_t timestamp) {
  g->graph.constructEdge(edgeIndex, dstNodeIndex,
                         EdgeData(1 << labelBitPosition, timestamp));
}

void constructEdge(AttributedGraph* g, uint64_t edgeIndex,
                   uint32_t dstNodeIndex, uint32_t label, uint64_t timestamp) {
  g->graph.constructEdge(edgeIndex, dstNodeIndex, EdgeData(label, timestamp));
}

void setEdgeAttribute(AttributedGraph* g, uint32_t edgeIndex, char* key,
                      char* value) {
  auto& attributes = g->edgeAttributes;
  if (attributes.find(key) == attributes.end()) {
    attributes[key] = std::vector<std::string>();
    attributes[key].resize(g->graph.sizeEdges());
  }
  attributes[key][edgeIndex] = value;
}

size_t getNumNodes(AttributedGraph* g) { return g->graph.size(); }

size_t getNumEdges(AttributedGraph* g) { return g->graph.sizeEdges(); }

///////
// New Functions Added for Incremental Graph Construction
///////

uint32_t addNodeLabelMetadata(AttributedGraph* g, char* name) {
    auto foundValue = g->nodeLabelIDs.find(name);
    // already exists; return it
    if (foundValue != g->nodeLabelIDs.end()) {
      return foundValue->second;
    // doesn't exist: append to existing vector + return new label bit position
    } else {
      uint32_t newLabel = g->nodeLabelNames.size();
      g->nodeLabelNames.emplace_back(name);
      g->nodeLabelIDs[name] = newLabel;
      return newLabel;
    }
}

uint32_t addEdgeLabelMetadata(AttributedGraph* g, char* name) {
    auto foundValue = g->edgeLabelIDs.find(name);
    // already exists; return it
    if (foundValue != g->edgeLabelIDs.end()) {
      return foundValue->second;
    // doesn't exist: append to existing vector + return new label bit position
    } else {
      uint32_t newLabel = g->edgeLabelNames.size();
      g->edgeLabelNames.emplace_back(name);
      g->edgeLabelIDs[name] = newLabel;
      return newLabel;
    }
}

void resizeNodeAttributeMap(AttributedGraph* g, uint32_t nodeCount) {
  auto& attributes = g->nodeAttributes;

  for (auto& keyVal : attributes) {
    assert(keyVal.second.size() <= nodeCount);
    (keyVal.second).resize(nodeCount);
  }
}

void addNodeAttributeMap(AttributedGraph* g, char* key, uint32_t nodeCount) {
  auto& attributes = g->nodeAttributes;
  if (attributes.find(key) == attributes.end()) {
    attributes[key] = std::vector<std::string>();
    attributes[key].resize(nodeCount);
  }
}

void resizeNodeMetadata(AttributedGraph* g, uint32_t nodeCount) {
  assert(g->nodeNames.size() <= nodeCount);
  g->nodeNames.resize(nodeCount);
  assert(g->index2UUID.size() <= nodeCount);
  g->index2UUID.resize(nodeCount);
}

uint32_t nodeExists(AttributedGraph* g, char* uuid) {
  if (g->nodeIndices.find(uuid) != g->nodeIndices.end()) {
    return 1u;
  } else {
    return 0u;
  }
}

void setNewNodeCSR(AttributedGraph* g, uint32_t nodeIndex, char* uuid,
                   uint32_t labelBitPosition) {
  auto& nd                = g->graph.getData(nodeIndex);
  nd.label                = 1 << labelBitPosition;
}


void setNodeCSR(AttributedGraph* g, uint32_t nodeIndex, char* uuid,
                uint32_t label) {
  auto& nd                = g->graph.getData(nodeIndex);
  nd.label                = label;
}

void setNodeMetadata(AttributedGraph* g, uint32_t nodeIndex, char* uuid,
                     char* nodeName) {
  g->nodeIndices[uuid]    = nodeIndex;
  g->index2UUID[nodeIndex] = uuid;
  g->nodeNames[nodeIndex] = nodeName;
}

uint32_t getIndexFromUUID(AttributedGraph* g, char* uuid) {
  return g->nodeIndices[uuid];
}

const char* getUUIDFromIndex(AttributedGraph* g, uint32_t nodeIndex) {
  return g->index2UUID[nodeIndex].c_str();
}

uint32_t getNodeLabel(AttributedGraph* g, uint32_t nodeIndex) {
  auto& nd = g->graph.getData(nodeIndex);
  return nd.label;
}

uint64_t copyEdgesOfNode(AttributedGraph* destGraph, AttributedGraph* srcGraph,
                         uint32_t nodeIndex, uint64_t edgeIndex) {
  Graph& dst = destGraph->graph;
  Graph& src = srcGraph->graph;

  // copy edges + data
  uint64_t curEdgeIndex = edgeIndex;
  for (auto e : src.edges(nodeIndex)) {
    uint32_t edgeDst = src.getEdgeDst(e);
    auto& data = src.getEdgeData(e);

    // uses non-new variant of construct edge i.e. direct copy of label
    dst.constructEdge(curEdgeIndex, edgeDst,
                      EdgeData(data.label, data.timestamp));
    curEdgeIndex++;
  }

  // copy edge attributes
  uint64_t firstEdge = *(src.edge_begin(nodeIndex));
  uint64_t lastEdge = *(src.edge_end(nodeIndex));
  auto& attributes = destGraph->edgeAttributes;

  for (auto& keyValue : srcGraph->edgeAttributes) {
    curEdgeIndex = edgeIndex;
    std::string key = keyValue.first;
    for (uint64_t i = firstEdge; i < lastEdge; i++) {
      if (attributes.find(key) == attributes.end()) {
        attributes[key] = std::vector<std::string>();
        attributes[key].resize(destGraph->graph.sizeEdges());
      }
      attributes[key][curEdgeIndex] = keyValue.second[i];

      curEdgeIndex++;
    }
  }
  return (lastEdge - firstEdge);
}

void swapCSR(AttributedGraph* g1, AttributedGraph* g2) {
  swap(g1->graph, g2->graph);
}

void swapEdgeAttributes(AttributedGraph* g1, AttributedGraph* g2) {
  std::swap(g1->edgeAttributes, g2->edgeAttributes);
}

void addNewLabel(AttributedGraph* g, uint32_t nodeIndex,
                 uint32_t labelBitPosition) {
  auto& nd                = g->graph.getData(nodeIndex);
  nd.label                = nd.label | (1 << labelBitPosition);
}

void mergeLabels(AttributedGraph*g, uint32_t nodeIndex,
                 uint32_t labelToMerge) {
  auto& nd                = g->graph.getData(nodeIndex);
  nd.label                = nd.label | labelToMerge;
}
