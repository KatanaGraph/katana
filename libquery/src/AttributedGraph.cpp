#include "galois/graphs/AttributedGraph.h"
#include "galois/Timer.h"
#include "galois/Logging.h"
#include "querying/CypherCompiler.h"
#include "querying/SubgraphQuery.h"

#include <iostream>
#include <fstream>
#include <boost/serialization/map.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/unordered_map.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>

#ifndef NDEBUG
namespace {
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
} // namespace
#endif

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

namespace galois::graphs {

void AttributedGraph::printGraph() {
  QueryGraph& graph = this->graph;
  // auto& nodeLabelNames = this->nodeLabelNames;
  auto& edgeLabelNames = this->edgeLabelNames;
  auto& nodeNames      = this->nodeNames;
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
  uint32_t sourceLabelID = 1 << this->nodeLabelIDs["process"];
#endif
  uint64_t numEdges = 0;

  for (auto src : graph) {
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
    auto& srcData = graph.getData(src);
    // only print if source is a process
    if ((srcData.label & sourceLabelID) != sourceLabelID)
      continue;
    auto& srcLabel = this->nodeLabelNames[rightmostSetBitPos(srcData.label)];
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
      auto& dstLabel = this->nodeLabelNames[rightmostSetBitPos(dstData.label)];
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

void AttributedGraph::allocateGraph(size_t numNodes, size_t numEdges,
                                    size_t numNodeLabels,
                                    size_t numEdgeLabels) {
  this->graph.allocateFrom(numNodes, numEdges);
  this->graph.constructNodes();
  assert(numNodeLabels <= 32);
  this->nodeLabelNames.resize(numNodeLabels);
  assert(numEdgeLabels <= 32);
  this->edgeLabelNames.resize(numEdgeLabels);
  this->index2UUID.resize(numNodes);
  this->nodeNames.resize(numNodes);
}

void AttributedGraph::allocateGraphLDBC(size_t numNodes, size_t numEdges,
                                        size_t numNodeLabels,
                                        size_t numEdgeLabels) {
  this->graph.allocateFrom(numNodes, numEdges);
  this->graph.constructNodes();
  assert(numNodeLabels <= 32);
  this->nodeLabelNames.resize(numNodeLabels);
  assert(numEdgeLabels <= 32);
  this->edgeLabelNames.resize(numEdgeLabels);
}

void AttributedGraph::fixEndEdge(uint32_t nodeIndex, uint64_t edgeIndex) {
  this->graph.fixEndEdge(nodeIndex, edgeIndex);
}

void AttributedGraph::setNewNode(uint32_t nodeIndex, char* uuid,
                                 uint32_t GALOIS_UNUSED(labelBitPosition),
                                 char* name) {
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
  auto& nd = this->graph.getData(nodeIndex);
  nd.label = 1 << labelBitPosition;
#endif
  this->nodeIndices[uuid]     = nodeIndex;
  this->index2UUID[nodeIndex] = uuid;
  this->nodeNames[nodeIndex]  = name;
}

void AttributedGraph::setNodeLabel(uint32_t GALOIS_UNUSED(nodeIndex),
                                   uint32_t GALOIS_UNUSED(label)) {
#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
  auto& nd = this->graph.getData(nodeIndex);
  nd.label = label;
#endif
}

void AttributedGraph::setNodeLabelMetadata(uint32_t labelBitPosition,
                                           const char* name) {
  this->nodeLabelNames[labelBitPosition] = name;
  this->nodeLabelIDs[name]               = labelBitPosition;
}

void AttributedGraph::setEdgeLabelMetadata(uint32_t labelBitPosition,
                                           const char* name) {
  this->edgeLabelNames[labelBitPosition] = name;
  this->edgeLabelIDs[name]               = labelBitPosition;
}

void AttributedGraph::setExistingNodeAttribute(uint32_t nodeIndex,
                                               const char* key,
                                               const char* value) {
  auto& attributes = this->nodeAttributes;
  if (attributes.find(key) == attributes.end()) {
    GALOIS_DIE("node attribute ", key, "doesn't already exist");
  }
  attributes[key][nodeIndex] = value;
}

void AttributedGraph::constructNewEdge(uint64_t edgeIndex,
                                       uint32_t dstNodeIndex,
                                       uint32_t labelBitPosition,
                                       uint64_t GALOIS_UNUSED(timestamp)) {
  this->graph.constructEdge(edgeIndex, dstNodeIndex,
                            QueryEdgeData(1 << labelBitPosition
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
                                          ,
                                          timestamp));
#else
                                          ));
#endif
}

void AttributedGraph::constructEdge(uint64_t edgeIndex, uint32_t dstNodeIndex,
                                    uint32_t label,
                                    uint64_t GALOIS_UNUSED(timestamp)) {
  this->graph.constructEdge(edgeIndex, dstNodeIndex,
                            QueryEdgeData(label
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
                                          ,
                                          timestamp));
#else
                                          ));
#endif
}

void AttributedGraph::setExistingEdgeAttribute(uint32_t edgeIndex,
                                               const char* key,
                                               const char* value) {
  auto& attributes = this->edgeAttributes;
  if (attributes.find(key) == attributes.end()) {
    GALOIS_DIE("edge attribute ", key, "doesn't already exist");
  }
  attributes[key][edgeIndex] = value;
}

void AttributedGraph::addNodeAttributeMap(const char* key, uint32_t nodeCount) {
  auto& attributes = this->nodeAttributes;
  if (attributes.find(key) == attributes.end()) {
    attributes[key] = std::vector<std::string>();
    attributes[key].resize(nodeCount);
  }
}

void AttributedGraph::addNodeAttributeType(const char* key, AttributedType t) {
  auto& attributes = this->nodeAttributeTypes;
  attributes[key]  = t;
}

void AttributedGraph::addEdgeAttributeMap(const char* key, uint32_t edgeCount) {
  auto& attributes = this->edgeAttributes;
  if (attributes.find(key) == attributes.end()) {
    attributes[key] = std::vector<std::string>();
    attributes[key].resize(edgeCount);
  }
}

void AttributedGraph::addEdgeAttributeType(const char* key, AttributedType t) {
  auto& attributes = this->edgeAttributeTypes;
  attributes[key]  = t;
}

#ifdef USE_QUERY_GRAPH_WITH_NODE_LABEL
std::pair<bool, std::pair<uint32_t, uint32_t>>
AttributedGraph::getNodeLabelMask(const std::string& nodeLabel) {
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
      if (this->nodeLabelIDs.find(label) != this->nodeLabelIDs.end()) {
        if (!notMode) {
          return std::make_pair(
              true, std::make_pair(1u << this->nodeLabelIDs[label], 0));
        } else {
          return std::make_pair(
              true, std::make_pair(0, 1u << this->nodeLabelIDs[label]));
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

      if (this->nodeLabelIDs.find(token) != this->nodeLabelIDs.end()) {
        // mark bit based on ~ token
        if (!notMode) {
          labelMask |= 1u << this->nodeLabelIDs[token];
        } else {
          notLabelMask |= 1u << this->nodeLabelIDs[token];
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
AttributedGraph::getEdgeLabelMask(const std::string& edgeLabel) {
  if (edgeLabel.find(";") == std::string::npos) {
    if (edgeLabel != "ANY") {
      bool notMode      = false;
      std::string label = edgeLabel;
      // trim out the ~ sign
      if (edgeLabel.find("~") == 0) {
        notMode = true;
        label   = edgeLabel.substr(1);
      }

      if (this->edgeLabelIDs.find(label) != this->edgeLabelIDs.end()) {
        if (!notMode) {
          return std::make_pair(
              true, std::make_pair(1u << this->edgeLabelIDs[label], 0));
        } else {
          return std::make_pair(
              true, std::make_pair(0, 1u << this->edgeLabelIDs[label]));
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

      if (this->edgeLabelIDs.find(token) != this->edgeLabelIDs.end()) {
        // mark bit based on ~ token
        if (!notMode) {
          labelMask |= 1u << this->edgeLabelIDs[token];
        } else {
          notLabelMask |= 1u << this->edgeLabelIDs[token];
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

void AttributedGraph::saveGraph(const char* filename) {
  // test prints
  // for (auto d : this->graph) {
  //  galois::gPrint(d, " ", this->index2UUID[d], "\n");

  //  for (auto e : this->graph.edges(d)) {
  //    auto bleh = this->graph.getEdgeDst(e);
  //    galois::gPrint("     ", "to ", bleh, " ", this->index2UUID[bleh], "\n");
  //  }
  //}

  std::ofstream file(filename, std::ios::out | std::ios::binary);
  boost::archive::binary_oarchive oarch(file);
  this->graph.serializeGraph(oarch);
  oarch << this->nodeLabelNames;

  oarch << this->nodeLabelIDs;
  oarch << this->edgeLabelNames;
  oarch << this->edgeLabelIDs;
  oarch << this->nodeIndices;
  oarch << this->index2UUID;
  oarch << this->nodeNames;
  // node/edge attributes
  oarch << this->nodeAttributes;
  oarch << this->nodeAttributeTypes;
  oarch << this->edgeAttributes;
  oarch << this->edgeAttributeTypes;

  // test prints
  // for (auto& pair : this->nodeLabelIDs) {
  //  printf("nodelabel pair first is %s second %u\n", pair.first.c_str(),
  //  pair.second);
  //}
  // for (auto& pair : this->nodeAttributes) {
  //  printf("pair first is %s\n", pair.first.c_str());
  //  for (auto s : pair.second) {
  //    printf("  - %s\n", s.c_str());
  //  }
  //}

  file.close();
}

void AttributedGraph::loadGraph(const char* filename) {
  std::ifstream file(filename, std::ios::in | std::ios::binary);
  boost::archive::binary_iarchive iarch(file);
  this->graph.deSerializeGraph(iarch);
  this->graph.constructAndSortIndex();
  iarch >> this->nodeLabelNames;

  // node label IDs
  iarch >> this->nodeLabelIDs;
  iarch >> this->edgeLabelNames;
  // edge label IDs
  iarch >> this->edgeLabelIDs;
  // node indices
  iarch >> this->nodeIndices;
  iarch >> this->index2UUID;
  iarch >> this->nodeNames;
  iarch >> this->nodeAttributes;
  iarch >> this->nodeAttributeTypes;
  iarch >> this->edgeAttributes;
  iarch >> this->edgeAttributeTypes;

  // test prints
  // for (auto& pair : this->nodeLabelIDs) {
  //  printf("nodelabel pair first is %s second %u\n", pair.first.c_str(),
  //  pair.second);
  //}
  // for (auto& pair : this->nodeAttributes) {
  //  printf("pair first is %s\n", pair.first.c_str());
  //  for (auto s : pair.second) {
  //    printf("  - %s\n", s.c_str());
  //  }
  //}

  file.close();
}

void AttributedGraph::reportGraphStats() {
  galois::gPrint("GRAPH STATS\n");
  galois::gPrint("-------------------------------------------------------------"
                 "---------\n");
  galois::gPrint("Number of Nodes: ", this->graph.size(), "\n");
  galois::gPrint("Number of Edges: ", this->graph.sizeEdges(), "\n\n");

  // print all node label names
  galois::gPrint("Node Labels:\n");
  galois::gPrint("------------------------------\n");
  for (std::string nLabel : this->nodeLabelNames) {
    galois::gPrint(nLabel, ", ");
  }

  galois::gPrint("\n\n");

  // print all edge label names
  galois::gPrint("Edge Labels:\n");
  galois::gPrint("------------------------------\n");
  for (std::string eLabel : this->edgeLabelNames) {
    galois::gPrint(eLabel, ", ");
  }
  galois::gPrint("\n\n");

  // print all node attribute names
  galois::gPrint("Node Attributes:\n");
  galois::gPrint("------------------------------\n");
  for (auto& nLabel : this->nodeAttributes) {
    galois::gPrint(nLabel.first, ", ");
  }
  galois::gPrint("\n\n");

  // print all edge attribute names
  galois::gPrint("Edge Attributes:\n");
  galois::gPrint("------------------------------\n");
  for (auto& eLabel : this->edgeAttributes) {
    galois::gPrint(eLabel.first, ", ");
  }
  galois::gPrint("\n");

  // print node/edge attribute names from arrow arrays
  galois::gPrint("Node Attributes (Arrow):\n");
  galois::gPrint("------------------------------\n");
  for (auto& nLabel : this->node_arrow_attributes) {
    galois::gPrint(nLabel.first, ", ");
  }
  galois::gPrint("\n\n");

  // print all edge attribute names
  galois::gPrint("Edge Attributes (Arrow):\n");
  galois::gPrint("------------------------------\n");
  for (auto& eLabel : this->edge_arrow_attributes) {
    galois::gPrint(eLabel.first, ", ");
  }
  galois::gPrint("\n");

  galois::gPrint("-------------------------------------------------------------"
                 "---------\n");
}

void AttributedGraph::insertNodeArrowAttribute(
    std::string attribute_name,
    const std::shared_ptr<arrow::ChunkedArray>& arr) {
  auto result = this->node_arrow_attributes.try_emplace(attribute_name, arr);
  // report if a duplicate attribute was added
  if (!result.second) {
    GALOIS_LOG_ERROR("Inserting a duplicate node attribute {}", attribute_name);
  }
}

void AttributedGraph::insertEdgeArrowAttribute(
    std::string attribute_name,
    const std::shared_ptr<arrow::ChunkedArray>& arr) {
  auto result = this->edge_arrow_attributes.try_emplace(attribute_name, arr);
  // report if a duplicate attribute was added
  if (!result.second) {
    GALOIS_LOG_ERROR("Inserting a duplicate edge attribute {}", attribute_name);
  }
}

void AttributedGraph::addToNodeLabel(uint32_t node_id, unsigned label_bit) {
  auto& nd = this->graph.getData(node_id);
  nd.label = nd.label | (1 << label_bit);
}

void AttributedGraph::addToEdgeLabel(uint32_t edge_id, unsigned label_bit) {
  auto& ed = this->graph.getEdgeData(edge_id);
  ed       = ed | (1 << label_bit);
}

size_t AttributedGraph::matchCypherQuery(const char* cypherQueryStr) {
  galois::StatTimer compileTime("CypherCompileTime");

  // parse query, get AST
  compileTime.start();
  CypherCompiler cc;
  cc.compile(cypherQueryStr);
  compileTime.stop();

#ifndef NDEBUG
  printIR(cc.getQueryEdges(), cc.getFilters());
#endif

  // do actual matching
  // the things passed from the compiler are the following:
  // - edges of a query graph
  // - filters on nodes (contains)
  size_t numMatches =
      this->matchQuery(cc.getQueryNodes(), cc.getQueryEdges(), cc.getFilters());
  cc.getQueryEdges().clear();
  cc.getFilters().clear();

  return numMatches;
}

size_t AttributedGraph::matchQuery(std::vector<MatchedNode>& queryNodes,
                                   std::vector<MatchedEdge>& queryEdges,
                                   std::vector<const char*>& filters) {
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

  // loop through the nodes only if no edges exist
  if (queryEdges.size() == 0) {
    numQueryNodes = queryNodes.size();
    nodeTypes.resize(numQueryNodes, NULL);
    nodeContains.resize(numQueryNodes, "");
    prefixSum.resize(numQueryNodes, 0);

    assert(numQueryNodes == 1);
    size_t id        = std::stoi(queryNodes[0].id);
    nodeTypes[id]    = queryNodes[0].name;
    nodeContains[id] = std::string(filters[0]);
  }

  // loop through all edges parsed from compiler and do bookkeeping
  for (size_t j = 0; j < queryEdges.size(); ++j) {
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
  auto actualNumQueryEdges = queryEdges.size() - starEdgeList.size();

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
    if (!this->getNodeLabelMask(nodeTypes[i]).first) {
      // query node label does not exist in the data graph
      resetMatchedStatus(this->graph);
      return 0;
    }
  }
#endif

  // TODO refactor code below
  // edge label checking to  make sure labels exist in the graph; if not,
  // easy no match
  for (size_t j = 0; j < queryEdges.size(); ++j) {
    std::string curEdge = std::string(queryEdges[j].label);
    if (curEdge.find("*") == std::string::npos) {
      if (!this->getEdgeLabelMask(queryEdges[j].label).first) {
        // query edge label does not exist in the data graph
        resetMatchedStatus(this->graph);
        return 0;
      }
    } else {
      // * label: check if there are restrictions on it (i.e. only traverse
      // certain edges)
      if (curEdge.find("=") != std::string::npos) {
        // *=... means restrictions exist; get them
        std::string restrictions = curEdge.substr(2);
        std::pair<bool, std::pair<uint32_t, uint32_t>> edgeResult =
            this->getEdgeLabelMask(restrictions);

        galois::gDebug("* Restrictions ", restrictions, "\n");

        if (!edgeResult.first) {
          resetMatchedStatus(this->graph);
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
        this->getNodeLabelMask(nodeTypes[i]).second;
    queryGraph.getData(i).label   = masks.first | masks.second;
    queryGraph.getData(i).matched = masks.first;
  }
#endif
  for (size_t j = 0; j < queryEdges.size(); ++j) {
    if (std::string(queryEdges[j].label).find("*") == std::string::npos) {
      size_t srcID = std::stoi(queryEdges[j].caused_by.id);
      size_t dstID = std::stoi(queryEdges[j].acted_on.id);

      std::pair<uint32_t, uint32_t> edgeMasks =
          this->getEdgeLabelMask(queryEdges[j].label).second;
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

  // do special handling if * edges were used in the query edges
  if (starEdgeList.size() > 0) {
    // first, match query graph without star
    subgraphQuery(queryGraph, this->graph);

    // handle stars
    uint32_t currentStar = 0;
    for (std::pair<size_t, size_t>& sdPair : starEdgeList) {
      findShortestPaths(this->graph, sdPair.first, sdPair.second,
                        starEdgeData[currentStar], numQueryNodes + currentStar,
                        actualNumQueryEdges + currentStar);
      currentStar++;
    }

    // rematch taking star into account (handling star should have limited scope
    // of possible matches)
    subgraphQuery(queryGraph, this->graph);

#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
    return countMatchedEdges(this->graph);
#else
    return countMatchedNodes(this->graph);
#endif
  } else {
    return subgraphQuery(queryGraph, this->graph);
  }
}

} // namespace galois::graphs
