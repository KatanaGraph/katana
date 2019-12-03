/*
 * This file belongs to the Galois project, a C++ library for exploiting parallelism.
 * The code is being released under the terms of the 3-Clause BSD License (a
 * copy is located in LICENSE.txt at the top-level directory).
 *
 * Copyright (C) 2019, The University of Texas at Austin. All rights reserved.
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

#include "PythonGraph.h"
#include "galois/graphs/OfflineGraph.h"

namespace galois {
namespace graphs {

/**
 * Acts as a C++ wrapper around attributed graph + adds functionality for
 * using .gr files instead of going through RIPE graph construction code.
 */
class DBGraph {
  //! Underlying attribute graph
  AttributedGraph* attGraph = nullptr;

  size_t numNodeLabels;
  size_t numEdgeLabels;

 public:
  /**
   * Setup meta parameters
   */
  DBGraph() {
    attGraph = new AttributedGraph;
    numNodeLabels = 3;
    numEdgeLabels = 3;
  }

  /**
   * Destroy attributed graph object
   */
  ~DBGraph() {
    if (attGraph) {
      delete attGraph;
    }
  }

  //! Reads graph topology into attributed graph, then sets up its metadata.
  void readGr(const std::string filename) {
    // assumes AttGraph is already allocated

    ////////////////////////////////////////////////////////////////////////////
    // Graph topology loading
    ////////////////////////////////////////////////////////////////////////////
    // use offline graph for metadata things
    galois::graphs::OfflineGraph og(filename);
    size_t numNodes = og.size();
    size_t numEdges = og.sizeEdges();

    // allocate the graph + node/edge labels
    allocateGraph(attGraph, numNodes, numEdges, numNodeLabels, numEdgeLabels);

    // open file, pass to LCCSR to directly load topology
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd == -1) GALOIS_SYS_DIE("failed opening ", "'", filename, "', LC_CSR");
		Graph& lcGraph = attGraph->graph;
    lcGraph.readGraphTopology(fd, numNodes, numEdges);
    // file done, close it
    close(fd);

    // TODO problem: directly loading graph does not work as querying code
    // currently assume undirected graph; fix this later

    ////////////////////////////////////////////////////////////////////////////
    // Metadata setup
    ////////////////////////////////////////////////////////////////////////////

    // Topology now exists: need to create the metadata mappings and such
    // TODO
    
    // create node/edge labels and save them
    char dummy[10]; // assumption that labels won't get to 8+ digits
    for (size_t i = 0; i < numNodeLabels; i++) {
      std::string thisLabel = "n";
      thisLabel = thisLabel + std::to_string(i);
      strcpy(dummy, thisLabel.c_str());
      setNodeLabelMetadata(attGraph, i, dummy);
    }
    for (size_t i = 0; i < numEdgeLabels; i++) {
      std::string thisLabel = "e";
      thisLabel = thisLabel + std::to_string(i);
      strcpy(dummy, thisLabel.c_str());
      setEdgeLabelMetadata(attGraph, i, dummy);
    }

    // set node metadata: uuid is node id as a string and name is also just
    // node id
    // Unfortunately must be done serially as it messes with maps which are
    // not thread safe
    for (size_t i = 0; i < numNodes; i++) {
      std::string id = std::to_string(i);
      strcpy(dummy, id.c_str());
      // node labels are round-robin
      setNewNode(attGraph, i, dummy, i % numNodeLabels, dummy);
    }

    // TODO node may have more than one label; can add randomly?

    // TODO node attributes

    // edges; TODO may require symmetric since that's current assumption
    // of AttributedGraph
    for (size_t i = 0; i < numEdges; i++) {
      // fill out edge data as edge destinations already come from gr file
      // TODO timestamps currently grow with edge index i
      lcGraph.setEdgeData(i, EdgeData(i % numEdgeLabels, i));
    }

    // TODO edge attributes
  }

  size_t runCypherQuery(const std::string cypherQueryStr) {
    return matchCypherQuery(attGraph, EventLimit(), EventWindow(),
                            cypherQueryStr.c_str());

  }
};

} // graph namepsace
} // galois namepsace

#endif
