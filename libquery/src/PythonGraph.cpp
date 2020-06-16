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

void saveEdgeList(AttributedGraph* g, char* filename) {
  QueryGraph& graph = g->graph;
  std::ofstream file(filename);
  std::ofstream nodeFile("nodelabels.nodes");
  uint32_t maxNodeLabel = 0;
  uint32_t maxEdgeLabel = 0;

  for (uint32_t src : graph) {
    uint32_t srcLabel =
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
        rightmostSetBitPos(graph.getData(src).label);
#else
        0;
#endif
    if (srcLabel > maxNodeLabel) {
      maxNodeLabel = srcLabel;
    }

    nodeFile << src << "," << srcLabel << "\n";

    for (auto e : graph.edges(src)) {
      uint32_t dst       = graph.getEdgeDst(e);
      auto& ed           = graph.getEdgeData(e);
      uint32_t edgeLabel = rightmostSetBitPos(ed);
      // track max edge label
      if (edgeLabel > maxEdgeLabel) {
        maxEdgeLabel = edgeLabel;
      }
      // output edge to file with a single lab3el
      file << src << " " << dst << " " << edgeLabel << "\n";
    }
  }
  // max edge label: number of labels is 1 + that
  printf("# of node labels is %u\n", maxNodeLabel + 1);
  printf("# of edge labels is %u\n", maxEdgeLabel + 1);
  file.close();
  nodeFile.close();
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

void setNode(AttributedGraph* g, uint32_t nodeIndex, char* uuid,
             uint32_t GALOIS_UNUSED(label), char* name) {
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
  auto& nd = g->graph.getData(nodeIndex);
  nd.label = label;
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

void setNodeAttribute(AttributedGraph* g, uint32_t nodeIndex, const char* key,
                      const char* value) {
  auto& attributes = g->nodeAttributes;
  if (attributes.find(key) == attributes.end()) {
    attributes[key] = std::vector<std::string>();
    attributes[key].resize(g->graph.size());
  }
  attributes[key][nodeIndex] = value;
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

void setEdgeAttribute(AttributedGraph* g, uint32_t edgeIndex, const char* key,
                      const char* value) {
  auto& attributes = g->edgeAttributes;
  if (attributes.find(key) == attributes.end()) {
    attributes[key] = std::vector<std::string>();
    attributes[key].resize(g->graph.sizeEdges());
  }
  attributes[key][edgeIndex] = value;
}

void setExistingEdgeAttribute(AttributedGraph* g, uint32_t edgeIndex,
                              const char* key, const char* value) {
  auto& attributes = g->edgeAttributes;
  if (attributes.find(key) == attributes.end()) {
    GALOIS_DIE("edge attribute ", key, "doesn't already exist");
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

void setNewNodeCSR(AttributedGraph* GALOIS_UNUSED(g),
                   uint32_t GALOIS_UNUSED(nodeIndex), char* GALOIS_UNUSED(uuid),
                   uint32_t GALOIS_UNUSED(labelBitPosition)) {
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
  auto& nd = g->graph.getData(nodeIndex);
  nd.label = 1 << labelBitPosition;
#endif
}

void setNodeCSR(AttributedGraph* GALOIS_UNUSED(g),
                uint32_t GALOIS_UNUSED(nodeIndex), char* GALOIS_UNUSED(uuid),
                uint32_t GALOIS_UNUSED(label)) {
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
  auto& nd = g->graph.getData(nodeIndex);
  nd.label = label;
#endif
}

void setNodeMetadata(AttributedGraph* g, uint32_t nodeIndex, char* uuid,
                     char* nodeName) {
  g->nodeIndices[uuid]     = nodeIndex;
  g->index2UUID[nodeIndex] = uuid;
  g->nodeNames[nodeIndex]  = nodeName;
}

uint32_t getIndexFromUUID(AttributedGraph* g, char* uuid) {
  return g->nodeIndices[uuid];
}

const char* getUUIDFromIndex(AttributedGraph* g, uint32_t nodeIndex) {
  return g->index2UUID[nodeIndex].c_str();
}

uint32_t getNodeLabel(AttributedGraph* GALOIS_UNUSED(g),
                      uint32_t GALOIS_UNUSED(nodeIndex)) {
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
  auto& nd = g->graph.getData(nodeIndex);
  return nd.label;
#else
  return 0;
#endif
}

uint64_t copyEdgesOfNode(AttributedGraph* destGraph, AttributedGraph* srcGraph,
                         uint32_t nodeIndex, uint64_t edgeIndex) {
  QueryGraph& dst = destGraph->graph;
  QueryGraph& src = srcGraph->graph;

  // copy edges + data
  uint64_t curEdgeIndex = edgeIndex;
  for (auto e : src.edges(nodeIndex)) {
    uint32_t edgeDst = src.getEdgeDst(e);
    auto& data       = src.getEdgeData(e);

    // uses non-new variant of construct edge i.e. direct copy of label
    dst.constructEdge(curEdgeIndex, edgeDst,
                      QueryEdgeData(data
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
                                    ,
                                    data.timestamp));
#else
                                    ));
#endif
    curEdgeIndex++;
  }

  // copy edge attributes
  uint64_t firstEdge = *(src.edge_begin(nodeIndex));
  uint64_t lastEdge  = *(src.edge_end(nodeIndex));
  auto& attributes   = destGraph->edgeAttributes;

  for (auto& keyValue : srcGraph->edgeAttributes) {
    curEdgeIndex    = edgeIndex;
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

void addNewLabel(AttributedGraph* GALOIS_UNUSED(g),
                 uint32_t GALOIS_UNUSED(nodeIndex),
                 uint32_t GALOIS_UNUSED(labelBitPosition)) {
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
  auto& nd = g->graph.getData(nodeIndex);
  nd.label = nd.label | (1 << labelBitPosition);
#endif
}

void mergeLabels(AttributedGraph* GALOIS_UNUSED(g),
                 uint32_t GALOIS_UNUSED(nodeIndex),
                 uint32_t GALOIS_UNUSED(labelToMerge)) {
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
  auto& nd = g->graph.getData(nodeIndex);
  nd.label = nd.label | labelToMerge;
#endif
}

////////////////////////////////////////////////////////////////////////////////
// Functions for Removing Data
////////////////////////////////////////////////////////////////////////////////

void unmatchAll(AttributedGraph* g) {
  QueryGraph& actualGraph = g->graph;

  galois::do_all(
      galois::iterate(actualGraph.begin(), actualGraph.end()),
      [&](auto node) {
        QueryNode& nd = actualGraph.getData(node);
        nd.matched    = 0;

#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
        auto curEdge = actualGraph.edge_begin(node);
        auto end     = actualGraph.edge_end(node);
        for (; curEdge < end; curEdge++) {
          QueryEdgeData& curEdgeData = actualGraph.getEdgeData(curEdge);
          curEdgeData.matched        = 0;
        }
#endif
      },
      galois::steal(), galois::no_stats());
}

uint64_t killEdge(AttributedGraph* g, char* srcUUID, char* dstUUID,
                  uint32_t labelBitPosition,
                  uint64_t GALOIS_UNUSED(timestamp)) {
  QueryGraph& actualGraph = g->graph;

  if (g->nodeIndices.find(srcUUID) == g->nodeIndices.end() ||
      g->nodeIndices.find(dstUUID) == g->nodeIndices.end()) {
    return 0;
  }

  // get src index and dst index
  uint32_t srcIndex = g->nodeIndices[srcUUID];
  uint32_t dstIndex = g->nodeIndices[dstUUID];

  uint32_t removed = 0;

  // get edges of source, find edge with dst (if it exists)
  auto curEdge = actualGraph.edge_begin(srcIndex);
  auto end     = actualGraph.edge_end(srcIndex);

  for (; curEdge < end; curEdge++) {
    uint32_t curDest = actualGraph.getEdgeDst(curEdge);

    if (curDest == dstIndex) {
      // get this edge's metadata to see if it matches what we know
      QueryEdgeData& curEdgeData = actualGraph.getEdgeData(curEdge);

#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
      // step 1: check for it not already being marked dead
      if (curEdgeData.matched == 0) {
        // step 2: check for matching timestamp
        if (curEdgeData.timestamp == timestamp) {
#endif
          // step 3: check label to make sure it has what we want
          if ((curEdgeData & (1u << labelBitPosition)) != 0) {
            // match found; mark dead and break (assumption is that we won't
            // see another exact match again...)
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
            curEdgeData.matched = 1;
#endif
            removed = 1;
            break;
          } else {
            // here for debugging purposes TODO switch to gDebug later?
            // galois::gPrint("Label match failure ", labelBitPosition, " ",
            //               1u << labelBitPosition, " ", curEdgeData.label,
            //               "\n");
          }
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
        }
      }
#endif
    }
  }

  return removed;
}

#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
uint32_t nodeRemovalPass(AttributedGraph* g) {
  QueryGraph& actualGraph = g->graph;
  galois::GAccumulator<uint32_t> deadNodes;
  deadNodes.reset();

  galois::do_all(
      galois::iterate(actualGraph.begin(), actualGraph.end()),
      [&](auto node) {
        QueryNode& nd = actualGraph.getData(node);
        nd.matched    = 0;

        auto curEdge = actualGraph.edge_begin(node);
        auto end     = actualGraph.edge_end(node);
        // galois::gPrint("num edges of ", node, " is ", end - curEdge, "\n");
        bool dead = true;
        // what about in edges? the idea is that all edges are symmetric, so
        // if my outgoing edge is dead, so is the corresponding incoming edge
        for (; curEdge < end; curEdge++) {
          // uint32_t curDest = actualGraph.getEdgeDst(curEdge);
          QueryEdgeData& curEdgeData = actualGraph.getEdgeData(curEdge);
          // galois::gPrint(node, " ", curDest, " label ", curEdgeData.label,
          //               " stamp ", curEdgeData.timestamp, " dead ",
          //               curEdgeData.matched, "\n");
          if (curEdgeData.matched != 1) {
            dead = false;
            break;
          }
        }
        if (dead) {
          // galois::gPrint("node ", node, " id ", g->index2UUID[node], " is
          // dead\n");
          nd.matched = 1;
          deadNodes += 1;
        } else {
          // galois::gPrint("node ", node, " id ", g->index2UUID[node], " is
          // alive\n");
        }
      },
      galois::steal(), galois::no_stats());
  return deadNodes.reduce();
}

AttributedGraph* compressGraph(AttributedGraph* g, uint32_t nodesRemoved,
                               uint64_t edgesRemoved) {
  AttributedGraph* newGraph = createGraph();
  // swap over things we can reuse without issue
  std::swap(newGraph->nodeLabelNames, g->nodeLabelNames);
  std::swap(newGraph->nodeLabelIDs, g->nodeLabelIDs);
  std::swap(newGraph->edgeLabelNames, g->edgeLabelNames);
  std::swap(newGraph->edgeLabelIDs, g->edgeLabelIDs);

  auto& actualGraph  = g->graph;
  size_t oldNumNodes = actualGraph.size();
  size_t oldNumEdges = actualGraph.sizeEdges();
  size_t newNumNodes = oldNumNodes - nodesRemoved;
  size_t newNumEdges = oldNumEdges - edgesRemoved;

  // allocate space for new CSR and construct it
  QueryGraph& newCSR = newGraph->graph;
  newCSR.allocateFrom(newNumNodes, newNumEdges);
  newCSR.constructNodes();

  // prepare new LC CSR graph
  uint32_t activeThreadCount = galois::getActiveThreads();

  // figure out which nodes need to be killed
  galois::DynamicBitSet nodesToRemove;
  nodesToRemove.resize(oldNumNodes);

  // allocate vectors for storing thread start points for later processing
  std::vector<uint32_t> nodesToHandlePerThread;
  nodesToHandlePerThread.resize(activeThreadCount, 0);
  std::vector<uint64_t> edgesToHandlePerThread;
  edgesToHandlePerThread.resize(activeThreadCount, 0);

  // determine node/edge start points for each thread by counting dead nodes
  // and edges
  galois::on_each([&](unsigned tid, unsigned nthreads) {
    size_t beginNode;
    size_t endNode;
    std::tie(beginNode, endNode) =
        galois::block_range((size_t)0u, oldNumNodes, tid, nthreads);

    for (size_t n = beginNode; n < endNode; n++) {
      auto& nodeData = actualGraph.getData(n);

      if (nodeData.matched) {
        nodesToRemove.set(n);
      } else {
        // loop over edges, determine how many this thread needs to work with
        nodesToHandlePerThread[tid] += 1;

        for (auto e : actualGraph.edges(n)) {
          auto& data = actualGraph.getEdgeData(e);

          // not matched means not deleted edge
          if (!data.matched) {
            edgesToHandlePerThread[tid] += 1;
          }
        }
      }
    }
  });

  // thread level node/edge prefix sum summation
  uint32_t tNSum = 0;
  uint32_t tESum = 0;
  for (size_t i = 0; i < activeThreadCount; i++) {
    tNSum += nodesToHandlePerThread[i];
    tESum += edgesToHandlePerThread[i];
  }

  GALOIS_ASSERT(tNSum == newNumNodes, "new num nodes doesn't match found");
  GALOIS_ASSERT(tESum == newNumEdges, "new num edges doesn't match found");

  // prefix sum nodes/edges
  for (size_t i = 1; i < activeThreadCount; i++) {
    nodesToHandlePerThread[i] += nodesToHandlePerThread[i - 1];
    edgesToHandlePerThread[i] += edgesToHandlePerThread[i - 1];
  }

  std::vector<uint32_t> indicesToRemove = nodesToRemove.getOffsets();
  uint32_t numNodesToRemove             = indicesToRemove.size();

  GALOIS_ASSERT(nodesRemoved == numNodesToRemove,
                "nodes to remove doesn't match argument nodes to remove ",
                numNodesToRemove, " ", nodesRemoved);

  // swap over the map from old graph, then remove uuids/indices that don't
  // exist anymore
  galois::gPrint("Removing removed nodes from UUID to index map\n");
  std::swap(newGraph->nodeIndices, g->nodeIndices);
  for (uint32_t i : indicesToRemove) {
    // get UUID, remove from map
    size_t removed = newGraph->nodeIndices.erase(g->index2UUID[i]);
    GALOIS_ASSERT(removed);
  }

  GALOIS_ASSERT(newGraph->nodeIndices.size() == newNumNodes, "indices size is ",
                newGraph->nodeIndices.size(), " new num nodes is ",
                newNumNodes);
  // at this point, need to remap old UUIDs to new index in graph; do in later
  // loop

  // allocate memory for new node structures in compressed graph
  newGraph->index2UUID.resize(newNumNodes);
  newGraph->nodeNames.resize(newNumNodes);
  // setup attributes structures; set up keys and vectors
  // nodes
  for (auto keyIter = g->nodeAttributes.begin();
       keyIter != g->nodeAttributes.end(); keyIter++) {
    std::string key = keyIter->first;
    newGraph->nodeAttributes[key].resize(newNumNodes);
  }
  // edges
  for (auto keyIter = g->edgeAttributes.begin();
       keyIter != g->edgeAttributes.end(); keyIter++) {
    std::string key = keyIter->first;
    newGraph->edgeAttributes[key].resize(newNumEdges);
  }

  galois::on_each([&](unsigned tid, unsigned nthreads) {
    size_t beginNode;
    size_t endNode;
    std::tie(beginNode, endNode) =
        galois::block_range((size_t)0u, oldNumNodes, tid, nthreads);
    // get node and edge array starting points
    uint32_t curNode;
    uint32_t curEdge;
    if (tid != 0) {
      curNode = nodesToHandlePerThread[tid - 1];
      curEdge = edgesToHandlePerThread[tid - 1];
    } else {
      curNode = 0;
      curEdge = 0;
    }

    for (size_t n = beginNode; n < endNode; n++) {
      auto& nodeData = actualGraph.getData(n);

      // if not dead, we need to do some processing
      if (!nodeData.matched) {
        // first, update uuid/index maps
        std::string myUUID            = g->index2UUID[n];
        newGraph->nodeIndices[myUUID] = curNode;
        newGraph->index2UUID[curNode] = myUUID;

        // next, node attribute map
        for (auto keyIter = g->nodeAttributes.begin();
             keyIter != g->nodeAttributes.end(); keyIter++) {
          std::string key                        = keyIter->first;
          std::vector<std::string>& attrs        = keyIter->second;
          newGraph->nodeAttributes[key][curNode] = attrs[n];
        }

        // node names
        newGraph->nodeNames[curNode] = g->nodeNames[n];

        for (auto e : actualGraph.edges(n)) {
          auto& data = actualGraph.getEdgeData(e);
          // if not dead, work to do
          if (!data.matched) {
            // copy edge attributes
            for (auto keyIter = g->edgeAttributes.begin();
                 keyIter != g->edgeAttributes.end(); keyIter++) {
              std::string key                        = keyIter->first;
              std::vector<std::string>& attrs        = keyIter->second;
              newGraph->edgeAttributes[key][curEdge] = attrs[*e];
            }
            // construct edge
            newCSR.constructEdge(curEdge, actualGraph.getEdgeDst(e));
            // copy edge data
            newCSR.getEdgeData(curEdge) = data;
            curEdge++;
          }
        }

        // set node end in CSR
        newCSR.fixEndEdge(curNode, curEdge);
        // copy node data
        newCSR.getData(curNode) = nodeData;

        curNode++; // increment node count
      }
    }

    // sanity checks
    GALOIS_ASSERT(curNode == nodesToHandlePerThread[tid], tid, " ", curNode,
                  " ", nodesToHandlePerThread[tid]);
    GALOIS_ASSERT(curEdge == edgesToHandlePerThread[tid], tid, " ", curEdge,
                  " ", edgesToHandlePerThread[tid]);
  });

  // other sanity checks; comment out in production code
  for (auto d : newGraph->graph) {
    std::string myUUID = newGraph->index2UUID[d];
    GALOIS_ASSERT(newGraph->nodeIndices[myUUID] == d,
                  newGraph->nodeIndices[myUUID], " ", d);
  }

  // delete older graph
  deleteGraph(g);
  printf("Graph compression complete\n");

  unmatchAll(newGraph);
  return newGraph;
}
#endif
