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
#ifndef _GALOIS_DB_GRAPH_
#define _GALOIS_DB_GRAPH_

#include "galois/graphs/AttributedGraph.h"
#include "galois/graphs/OfflineGraph.h"
#include "galois/graphs/BufferedGraph.h"

// @todo remove need for this file completely

namespace galois {
namespace graphs {

/**
 * Acts as a C++ wrapper around attributed graph + adds functionality for
 * using .gr files instead of going through RIPE graph construction code.
 */
class DBGraph {
  //! Underlying attribute graph
  AttributedGraph* attGraph = nullptr;

  //! number of different node labels
  size_t numNodeLabels = 0;
  //! number of different edge labels
  size_t numEdgeLabels = 0;

  /**
   * Setup the different node and edge labels in the attributed graph; assumes
   * it is already allocated.
   */
  void setupNodeEdgeLabelsMeta() {
    // create node/edge labels and save them
    char dummy[10]; // assumption that labels won't get to 8+ digits
    for (size_t i = 0; i < numNodeLabels; i++) {
      std::string thisLabel = "";
      thisLabel             = thisLabel + std::to_string(i);
      strcpy(dummy, thisLabel.c_str());
      this->attGraph->setNodeLabelMetadata(i, dummy);
    }
    for (size_t i = 0; i < numEdgeLabels; i++) {
      std::string thisLabel = "";
      thisLabel             = thisLabel + std::to_string(i);
      strcpy(dummy, thisLabel.c_str());
      this->attGraph->setEdgeLabelMetadata(i, dummy);
    }
  }

  /**
   * Setup node data.
   *
   * For now it just sets up the metadata; labels and attributes are a TODO
   */
  void setupNodes(uint32_t numNodes) {
    char dummy[30];
    // set node metadata: uuid is node id as a string and name is also just
    // node id
    // Unfortunately must be done serially as it messes with maps which are
    // not thread safe
    for (size_t i = 0; i < numNodes; i++) {
      std::string id = "ID" + std::to_string(i);
      strcpy(dummy, id.c_str());
      // TODO node labels are round-robin; make this more controllable?
      this->attGraph->setNewNode(i, dummy, i % numNodeLabels, dummy);
    }

    // TODO node may have more than one label; can add randomly?

    // TODO node attributes
  }

  /**
   * Returns number of edges per vertex where the number of edges for vertex
   * i is in array[i + 1] (array[0] is 0)
   *
   * @param graphTopology Topology of original graph in a buffered graph
   * @returns Array of edges counts where array[i + 1] is number of edges
   * for vertex i
   */
  std::vector<uint64_t>
  getEdgeCounts(galois::graphs::BufferedGraph<uint32_t>& graphTopology) {
    // allocate vector where counts will be stored
    std::vector<uint64_t> edgeCounts;
    // + 1 so that it can be used as a counter for how many edges have been
    // added for a particular vertex
    edgeCounts.resize(graphTopology.size() + 1, 0);

    // loop over all edges, add to that source vertex's edge counts for each
    // endpoint (ignore self loops)
    galois::do_all(
        galois::iterate(0u, graphTopology.size()),
        [&](uint32_t vertexID) {
          for (auto i = graphTopology.edgeBegin(vertexID);
               i < graphTopology.edgeEnd(vertexID); i++) {
            uint64_t dst = graphTopology.edgeDestination(*i);
            if (vertexID != dst) {
              // src increment
              __sync_add_and_fetch(&(edgeCounts[vertexID + 1]), 1);
            }
          }
        },
        galois::steal(), galois::loopname("GetEdgeCounts"));

    return edgeCounts;
  }

public:
  /**
   * Setup meta parameters
   */
  DBGraph() {
    // TODO unique ptr
    this->attGraph = new AttributedGraph;
    numNodeLabels  = 1;
    numEdgeLabels  = 1;
  }

  /**
   * Destroy attributed graph object
   */
  ~DBGraph() {
    if (this->attGraph) {
      delete this->attGraph;
    }
  }

  /**
   * Given graph topology, construct the attributed graph by
   * ignoring self loops. Note that multiedges are allowed.
   */
  // void constructDataGraph(const std::string filename, bool useWeights = true)
  // {
  void constructDataGraph(const std::string filename) {
    // first, load graph topology
    // NOTE: assumes weighted
    galois::graphs::BufferedGraph<uint32_t> graphTopology;
    graphTopology.loadGraph(filename);

    galois::GAccumulator<uint64_t> keptEdgeCountAccumulator;
    galois::GReduceMax<uint64_t> maxLabels;
    keptEdgeCountAccumulator.reset();
    maxLabels.reset();
    // next, count the number of edges we want to keep (i.e. ignore the self
    // loops)
    galois::do_all(
        galois::iterate(0u, graphTopology.size()),
        [&](uint32_t vertexID) {
          for (auto i = graphTopology.edgeBegin(vertexID);
               i < graphTopology.edgeEnd(vertexID); i++) {
            uint64_t dst = graphTopology.edgeDestination(*i);
            if (vertexID != dst) {
              keptEdgeCountAccumulator += 1;
            }
            maxLabels.update(graphTopology.edgeData(*i));
          }
        },
        galois::steal(), // steal due to edge imbalance among nodes
        galois::loopname("CountKeptEdges"));

    numEdgeLabels = maxLabels.reduce() + 1;
    galois::gInfo("Edge label count is ", numEdgeLabels);

    uint64_t keptEdgeCount = keptEdgeCountAccumulator.reduce();

    galois::gDebug("Kept edge count is ", keptEdgeCount,
                   " compared to original ", graphTopology.sizeEdges());

    uint64_t finalEdgeCount = keptEdgeCount;

    ////////////////////////////////////////////////////////////////////////////
    // META SETUP
    ////////////////////////////////////////////////////////////////////////////

    // allocate the memory for the new graph
    this->attGraph->allocateGraph(graphTopology.size(), finalEdgeCount,
                                  numNodeLabels, numEdgeLabels);

    setupNodeEdgeLabelsMeta();

    ////////////////////////////////////////////////////////////////////////////
    // NODE TOPOLOGY
    ////////////////////////////////////////////////////////////////////////////

    setupNodes(graphTopology.size());

    ////////////////////////////////////////////////////////////////////////////
    // EDGE TOPOLOGY
    ////////////////////////////////////////////////////////////////////////////

    // need to count how many edges for each vertex in the graph
    std::vector<uint64_t> edgeCountsPerVertex = getEdgeCounts(graphTopology);

    // prefix sum the edge counts; this will tell us where we can write
    // new edges of a particular vertex
    for (size_t i = 1; i < edgeCountsPerVertex.size(); i++) {
      edgeCountsPerVertex[i] += edgeCountsPerVertex[i - 1];
    }

    // fix edge end points
    galois::do_all(
        galois::iterate(0u, graphTopology.size()),
        [&](uint32_t vertexID) {
          this->attGraph->fixEndEdge(vertexID,
                                     edgeCountsPerVertex[vertexID + 1]);
        },
        galois::loopname("EdgeEndpointFixing"));

    // loop over edges of a graph, add edges (again, ignore self loops)
    galois::do_all(
        galois::iterate(0u, graphTopology.size()),
        [&](uint32_t vertexID) {
          for (auto i = graphTopology.edgeBegin(vertexID);
               i < graphTopology.edgeEnd(vertexID); i++) {
            uint64_t edgeID = *i;
            // label to use for this edge pointing both ways
            // commented out part here is random edge label assignment
            // unsigned labelBit = edgeID % numEdgeLabels;
            unsigned labelBit = graphTopology.edgeData(edgeID);

            // TODO for now timestamp is original edge id
            uint64_t timestamp = edgeID;
            uint64_t dst       = graphTopology.edgeDestination(*i);

            // check if not a self loop
            if (vertexID != dst) {
              // get forward edge id
              uint64_t forwardEdge =
                  __sync_fetch_and_add(&(edgeCountsPerVertex[vertexID]), 1);
              // set forward
              this->attGraph->constructNewEdge(forwardEdge, dst, labelBit,
                                               timestamp);
            }
          }
        },
        galois::steal(), // steal due to edge imbalance among nodes
        galois::loopname("ConstructEdges"));

    // TODO edge attributes and other labels?

    // at this point graph is constructed: build and sort index
    attGraph->graph.constructAndSortIndex();

    GALOIS_ASSERT(edgeCountsPerVertex[graphTopology.size() - 1] ==
                  finalEdgeCount);
    galois::gInfo("Data graph construction from GR complete");

    ////////////////////////////////////////////////////////////////////////////
    // Finish
    ////////////////////////////////////////////////////////////////////////////
  }

  /**
   * Load an attributed graph save prior from disk into memory.
   *
   * @param graphOnDisk graph to load from disk into memory
   */
  void loadSerializedAttributedGraph(const std::string graphOnDisk) {
    this->attGraph->loadGraph(graphOnDisk.c_str());
    this->attGraph->reportGraphStats();
    // ignore setting numNodeLabels/numEdgeLabels; only used by the other
    // construction interface which is unnecessary if you use a serial
    // attributed graph directly
    // QueryGraph& asdf = this->attGraph->graph;
    // galois::do_all(galois::iterate((size_t)0, asdf.size()),
    //  [&] (auto N) {
    //    //unsigned test = rightmostSetBitPos(asdf.getData(N).label);
    //    uint32_t l = asdf.getData(N).label;
    //    if (l > 0) {
    //      galois::gPrint("label on ", N, " ",  l, "\n");
    //    }
    //    //for (auto e : asdf.edges(N)) {
    //    //  auto& data = asdf.getEdgeData(e);
    //    //}
    //  }
    //);
  }

  /**
   * Given a Cypher query string, run it on the underlying data graph using
   * the Pangolin engine.
   */
  size_t
  runCypherQuery(const std::string cypherQueryStr,
                 std::string GALOIS_UNUSED(outputFile) = "matched.edges") {
    // run the query, get number of matched edges
    size_t mEdgeCount = this->attGraph->matchCypherQuery(cypherQueryStr.c_str());
    return mEdgeCount;
  }
};

} // namespace graphs
} // namespace galois

#endif
