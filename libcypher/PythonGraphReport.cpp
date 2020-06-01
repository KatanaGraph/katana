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

#include "PythonGraph.h"

#include <iostream>
#include <fstream>

unsigned rightmostSetBitPos(uint32_t n) {
  assert(n != 0);
  if (n & 1)
    return 0;

  // unset rightmost bit and xor with itself
  n = n ^ (n & (n - 1));

  unsigned pos = 0;
  while (n) {
    n >>= 1;
    pos++;
  }
  return pos - 1;
}

void reportGraphSimulation(AttributedGraph& qG, AttributedGraph& dG,
                           char* outputFile) {
  std::streambuf* buf;
  std::ofstream ofs;

  if ((outputFile != NULL) && (strcmp(outputFile, "") != 0)) {
    ofs.open(outputFile);
    buf = ofs.rdbuf();
  } else {
    buf = std::cout.rdbuf();
  }

  std::ostream os(buf);

  Graph& qgraph        = qG.graph;
  auto& qnodeNames     = qG.nodeNames;
  Graph& graph         = dG.graph;
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
  auto& nodeLabelNames = dG.nodeLabelNames;
#endif
  auto& edgeLabelNames = dG.edgeLabelNames;
  auto& nodeNames      = dG.nodeNames;
  for (auto n : graph) {
    auto& src      = graph.getData(n);
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
    auto& srcLabel = nodeLabelNames[rightmostSetBitPos(src.label)];
#else
    auto srcLabel = 0;
#endif
    auto& srcName  = nodeNames[n];
    for (auto e : graph.edges(n)) {
      auto& dst           = graph.getData(graph.getEdgeDst(e));
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
      auto& dstLabel      = nodeLabelNames[rightmostSetBitPos(dst.label)];
#else
      auto dstLabel = 0;
#endif
      auto& dstName       = nodeNames[graph.getEdgeDst(e)];
      auto& ed            = graph.getEdgeData(e);
      auto& edgeLabel     = edgeLabelNames[rightmostSetBitPos(ed)];
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
      auto& edgeTimestamp = ed.timestamp;
#endif
      for (auto qn : qgraph) {
        uint64_t mask = (1 << qn);
        if (src.matched & mask) {
          for (auto qe : qgraph.edges(qn)) {
            auto& qeData = qgraph.getEdgeData(qe);
            if ((qeData & ed) == qeData) {
              auto qDst = qgraph.getEdgeDst(qe);
              mask      = (1 << qDst);
              if (dst.matched & mask) {
                auto& qSrcName = qnodeNames[qn];
                auto& qDstName = qnodeNames[qDst];
                os << srcLabel << " " << srcName << " (" << qSrcName << ") "
                   << edgeLabel << " " << dstLabel << " " << dstName << " ("
                   << qDstName << ") "
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
                   << " at " << edgeTimestamp 
#endif
                   << std::endl;
                break;
              }
            }
          }
        }
      }
    }
  }

  if ((outputFile != NULL) && (strcmp(outputFile, "") != 0)) {
    ofs.close();
  }
}

void returnMatchedNodes(AttributedGraph& dataGraph, MatchedNode* matchedNodes) {
  Graph& graph = dataGraph.graph;
  // auto& nodeLabelNames = dataGraph.nodeLabelNames;
  auto& nodeNames = dataGraph.nodeNames;

  size_t i = 0;
  for (auto n : graph) {
    auto& data = graph.getData(n);
    if (data.matched) {
      matchedNodes[i].id = dataGraph.index2UUID[n].c_str();
      // matchedNodes[i].label = nodeLabelNames[data.label].c_str();
      matchedNodes[i].name = nodeNames[n].c_str();
      ++i;
    }
  }
}

void reportMatchedNodes(AttributedGraph& dataGraph, char* outputFile) {
  Graph& graph         = dataGraph.graph;
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
  auto& nodeLabelNames = dataGraph.nodeLabelNames;
#endif
  auto& nodeNames      = dataGraph.nodeNames;

  std::streambuf* buf;
  std::ofstream ofs;

  if ((outputFile != NULL) && (strcmp(outputFile, "") != 0)) {
    ofs.open(outputFile);
    buf = ofs.rdbuf();
  } else {
    buf = std::cout.rdbuf();
  }

  std::ostream os(buf);

  for (auto n : graph) {
    auto& data = graph.getData(n);
    if (data.matched) {
      // print node names
      os << 
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
      nodeLabelNames[rightmostSetBitPos(data.label)] << " " <<
#endif
      nodeNames[n] << std::endl;
      // print uuid instead
      //os << nodeLabelNames[data.label] << " " << dataGraph.index2UUID[n] << std::endl;
    }
  }

  if ((outputFile != NULL) && (strcmp(outputFile, "") != 0)) {
    ofs.close();
  }
}

void returnMatchedNeighbors(AttributedGraph& dataGraph, char*,
                            MatchedNode* matchedNeighbors) {
  Graph& graph = dataGraph.graph;
  // auto& nodeLabelNames = dataGraph.nodeLabelNames;
  auto& nodeNames = dataGraph.nodeNames;

  size_t i = 0;
  // do not include the same node twice (multiple edges to the same node)
  for (auto n : graph) {
    auto& data = graph.getData(n);
    if (data.matched) {
      matchedNeighbors[i].id = dataGraph.index2UUID[n].c_str();
      // matchedNeighbors[i].label = nodeLabelNames[data.label].c_str();
      matchedNeighbors[i].name = nodeNames[n].c_str();
      ++i;
    }
  }
}

void reportMatchedNeighbors(AttributedGraph& dataGraph, char*,
                            char* outputFile) {
  Graph& graph         = dataGraph.graph;
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
  auto& nodeLabelNames = dataGraph.nodeLabelNames;
#endif
  auto& nodeNames      = dataGraph.nodeNames;

  std::streambuf* buf;
  std::ofstream ofs;

  if ((outputFile != NULL) && (strcmp(outputFile, "") != 0)) {
    ofs.open(outputFile);
    buf = ofs.rdbuf();
  } else {
    buf = std::cout.rdbuf();
  }

  std::ostream os(buf);

  // do not include the same node twice (multiple edges to the same node)
  for (auto n : graph) {
    auto& data = graph.getData(n);
    if (data.matched) {
      os << 
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
        nodeLabelNames[rightmostSetBitPos(data.label)] << " " <<
#endif
        nodeNames[n] << std::endl;
    }
  }

  if ((outputFile != NULL) && (strcmp(outputFile, "") != 0)) {
    ofs.close();
  }
}

void returnMatchedEdges(AttributedGraph& g, MatchedEdge* matchedEdges) {
  Graph& graph = g.graph;
  // auto& nodeLabelNames = g.nodeLabelNames;
  auto& edgeLabelNames = g.edgeLabelNames;
  auto& nodeNames      = g.nodeNames;
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
  auto sourceLabelID   = getNodeLabelMask(g, "process").second.first;
#endif

  size_t i = 0;
  for (auto src : graph) {
    auto& srcData = graph.getData(src);
    if (!srcData.matched)
      continue;
    // if ((srcData.label != sourceLabelID) || !srcData.matched) continue;
    // auto& srcLabel = nodeLabelNames[srcData.label];
    for (auto e : graph.edges(src)) {
      auto eData = graph.getEdgeData(e);
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
      if (eData.matched) 
#endif
      {
        auto dst      = graph.getEdgeDst(e);
        // if ((dstData.label == sourceLabelID) && (dst < src)) continue;
        // auto& dstLabel = nodeLabelNames[dstData.label];
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
        matchedEdges[i].timestamp = eData.timestamp;
#endif
        matchedEdges[i].label     = edgeLabelNames[rightmostSetBitPos(eData)].c_str();
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
        auto& dstData = graph.getData(dst);
        if (((dstData.label & sourceLabelID) != sourceLabelID) ||
            (((srcData.label & sourceLabelID) == sourceLabelID) && 
            (src < dst))) 
#else
        if (src < dst)
#endif
        {
          matchedEdges[i].caused_by.id   = g.index2UUID[src].c_str();
          matchedEdges[i].caused_by.name = nodeNames[src].c_str();
          matchedEdges[i].acted_on.id    = g.index2UUID[dst].c_str();
          matchedEdges[i].acted_on.name  = nodeNames[dst].c_str();
        } else {
          matchedEdges[i].caused_by.id   = g.index2UUID[dst].c_str();
          matchedEdges[i].caused_by.name = nodeNames[dst].c_str();
          matchedEdges[i].acted_on.id    = g.index2UUID[src].c_str();
          matchedEdges[i].acted_on.name  = nodeNames[src].c_str();
        }
        ++i;
      }
    }
  }
}

void reportMatchedEdges(AttributedGraph& g, char* outputFile) {
  Graph& graph = g.graph;
  // auto& nodeLabelNames = g.nodeLabelNames;
  auto& edgeLabelNames = g.edgeLabelNames;
  auto& nodeNames      = g.nodeNames;
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
  auto sourceLabelID   = getNodeLabelMask(g, "process").second.first;
#endif

  std::streambuf* buf;
  std::ofstream ofs;

  if ((outputFile != NULL) && (strcmp(outputFile, "") != 0)) {
    ofs.open(outputFile);
    buf = ofs.rdbuf();
  } else {
    buf = std::cout.rdbuf();
  }

  std::ostream os(buf);

  for (auto src : graph) {
    auto& srcData = graph.getData(src);
    if (!srcData.matched)
      continue;
    // if ((srcData.label != sourceLabelID) || !srcData.matched) continue;
    // auto& srcLabel = nodeLabelNames[srcData.label];
    auto& srcName = nodeNames[src];
    for (auto e : graph.edges(src)) {
      auto eData = graph.getEdgeData(e);
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
      if (eData.matched) 
#endif
      {
        auto dst      = graph.getEdgeDst(e);
        // if ((dstData.label == sourceLabelID) && (dst < src)) continue;
        // auto& dstLabel = nodeLabelNames[dstData.label];
        auto& dstName              = nodeNames[dst];
        std::string& edgeLabel     = edgeLabelNames[rightmostSetBitPos(eData)];
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
        auto& edgeTimestamp = eData.timestamp;
#endif
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
        auto& dstData = graph.getData(dst);
        if (((dstData.label & sourceLabelID) != sourceLabelID) ||
            (((srcData.label & sourceLabelID) == sourceLabelID) && (src < dst)))
#else
        if (src < dst)
#endif
        {
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
          os << edgeTimestamp << ", "; 
#endif
          os << srcName << ", " << edgeLabel << ", "
            << dstName << std::endl;
        } else {
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
          os << edgeTimestamp << ", ";
#endif
          os << dstName << ", " << edgeLabel << ", "
             << srcName << std::endl;
        }
      }
    }
  }

  if ((outputFile != NULL) && (strcmp(outputFile, "") != 0)) {
    ofs.close();
  }
}

void returnMatchedNeighborEdges(AttributedGraph& g, char* uuid,
                                MatchedEdge* matchedEdges) {
  Graph& graph = g.graph;
  // auto& nodeLabelNames = g.nodeLabelNames;
  auto& edgeLabelNames = g.edgeLabelNames;
  auto& nodeNames      = g.nodeNames;
  auto src             = g.nodeIndices[uuid];

  size_t i      = 0;
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
  auto& srcData = graph.getData(src);
  auto sourceLabelID   = getNodeLabelMask(g, "process").second.first;
#endif
  // auto& srcLabel = nodeLabelNames[srcData.label];
  for (auto e : graph.edges(src)) {
    auto dst      = graph.getEdgeDst(e);
    auto& dstData = graph.getData(dst);
    if (dstData.matched) {
      // auto& dstLabel = nodeLabelNames[dstData.label];
      auto& eData               = graph.getEdgeData(e);
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
      matchedEdges[i].timestamp = eData.timestamp;
#endif
      matchedEdges[i].label     =
          edgeLabelNames[rightmostSetBitPos(eData)].c_str();
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
      if (((dstData.label & sourceLabelID) != sourceLabelID) ||
          (((srcData.label & sourceLabelID) == sourceLabelID) && (src < dst)))
#else
      if (src < dst)
#endif
      {
        matchedEdges[i].caused_by.id   = g.index2UUID[src].c_str();
        matchedEdges[i].caused_by.name = nodeNames[src].c_str();
        matchedEdges[i].acted_on.id    = g.index2UUID[dst].c_str();
        matchedEdges[i].acted_on.name  = nodeNames[dst].c_str();
      } else {
        matchedEdges[i].caused_by.id   = g.index2UUID[dst].c_str();
        matchedEdges[i].caused_by.name = nodeNames[dst].c_str();
        matchedEdges[i].acted_on.id    = g.index2UUID[src].c_str();
        matchedEdges[i].acted_on.name  = nodeNames[src].c_str();
      }
      ++i;
    }
  }
}

void reportMatchedNeighborEdges(AttributedGraph& g, char* uuid,
                                char* outputFile) {
  Graph& graph = g.graph;
  // auto& nodeLabelNames = g.nodeLabelNames;
  auto& edgeLabelNames = g.edgeLabelNames;
  auto& nodeNames      = g.nodeNames;
  auto src             = g.nodeIndices[uuid];

  std::streambuf* buf;
  std::ofstream ofs;

  if ((outputFile != NULL) && (strcmp(outputFile, "") != 0)) {
    ofs.open(outputFile);
    buf = ofs.rdbuf();
  } else {
    buf = std::cout.rdbuf();
  }

  std::ostream os(buf);

#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
  auto& srcData = graph.getData(src);
  auto sourceLabelID   = getNodeLabelMask(g, "process").second.first;
#endif
  // auto& srcLabel = nodeLabelNames[srcData.label];
  auto& srcName = nodeNames[src];
  for (auto e : graph.edges(src)) {
    auto dst      = graph.getEdgeDst(e);
    auto& dstData = graph.getData(dst);
    if (dstData.matched) {
      // auto& dstLabel = nodeLabelNames[dstData.label];
      auto& dstName       = nodeNames[dst];
      auto& ed            = graph.getEdgeData(e);
      auto& edgeLabel     = edgeLabelNames[rightmostSetBitPos(ed)];
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
      auto& edgeTimestamp = ed.timestamp;
#endif
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
      if (((dstData.label & sourceLabelID) != sourceLabelID) ||
          (((srcData.label & sourceLabelID) == sourceLabelID) && (src < dst)))
#else
      if (src < dst)
#endif
      {
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
        os << edgeTimestamp << ", ";
#endif
        os << srcName << ", " << edgeLabel << ", "
           << dstName << std::endl;
      } else {
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
        os << edgeTimestamp << ", ";
#endif
        os << dstName << ", " << edgeLabel << ", "
           << srcName << std::endl;
      }
    }
  }

  if ((outputFile != NULL) && (strcmp(outputFile, "") != 0)) {
    ofs.close();
  }
}

void reportGraphStats(AttributedGraph& g) {
  galois::gPrint("GRAPH STATS\n");
  galois::gPrint("----------------------------------------------------------------------\n");
  galois::gPrint("Number of Nodes: ", g.graph.size(), "\n");
  galois::gPrint("Number of Edges: ", g.graph.sizeEdges(), "\n\n");

  // print all node label names
  galois::gPrint("Node Labels:\n");
  galois::gPrint("------------------------------\n");
  for (std::string nLabel : g.nodeLabelNames) {
    galois::gPrint(nLabel, ", ");
  }

  galois::gPrint("\n\n");

  // print all edge label names
  galois::gPrint("Edge Labels:\n");
  galois::gPrint("------------------------------\n");
  for (std::string eLabel : g.edgeLabelNames) {
    galois::gPrint(eLabel, ", ");
  }
  galois::gPrint("\n\n");

  // print all node attribute names
  galois::gPrint("Node Attributes:\n");
  galois::gPrint("------------------------------\n");
  for (auto& nLabel : g.nodeAttributes) {
    galois::gPrint(nLabel.first, ", ");
  }
  galois::gPrint("\n\n");

  // print all edge attribute names
  galois::gPrint("Edge Attributes:\n");
  galois::gPrint("------------------------------\n");
  for (auto& eLabel : g.edgeAttributes) {
    galois::gPrint(eLabel.first, ", ");
  }
  galois::gPrint("\n");

  galois::gPrint("----------------------------------------------------------------------\n");
}
