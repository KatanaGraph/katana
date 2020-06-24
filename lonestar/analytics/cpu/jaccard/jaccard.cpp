/*
 * This file belongs to the Galois project, a C++ library for exploiting
 * parallelism. The code is being released under the terms of the 3-Clause BSD
 * License (a copy is located in LICENSE.txt at the top-level directory).
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

#include "galois/Galois.h"
#include "galois/gstl.h"
#include "galois/Reduction.h"
#include "galois/Timer.h"
#include "galois/graphs/LCGraph.h"
#include "galois/graphs/TypeTraits.h"
#include "Lonestar/BoilerPlate.h"
#include "Lonestar/BFS_SSSP.h"

#include "llvm/Support/CommandLine.h"

#include <iostream>
#include <deque>
#include <type_traits>
#include <unordered_set>

namespace cll = llvm::cl;

static const char* name = "Jaccard Similarity";

static const char* desc = "Compute the similarity of nodes (to some base node) "
                          "based on the similarity of their neighbor sets.";

static const char* url = "jaccard";

static cll::opt<std::string>
    inputFile(cll::Positional, cll::desc("<input file>"), cll::Required);
static cll::opt<unsigned int>
    baseNode("baseNode",
             cll::desc("Node to compute similarity to (default value 0)"),
             cll::init(0));
static cll::opt<unsigned int>
    reportNode("reportNode",
               cll::desc("Node to report the similarity of (default value 1)"),
               cll::init(1));

using Graph =
    galois::graphs::LC_CSR_Graph<double, void>::with_no_lockable<true>::type;
//::with_numa_alloc<true>::type;

using GNode = Graph::GraphNode;

void algo(Graph& graph, GNode base) {
  std::unordered_set<GNode> base_neighbors;

  // Collect all the neighbors of the base node into a hash set.
  for (auto e : graph.edges(base)) {
    auto n = graph.getEdgeDst(e);
    base_neighbors.emplace(n);
  }

  // Compute the similarity for each node
  galois::do_all(
      galois::iterate(graph),
      [&](const GNode& n2) {
        double& n2_data  = graph.getData(n2);
        uint32_t n2_size = 0, intersection_size = 0;
        // TODO: Using a sorted edge list would allow a much faster intersection
        // operation. Use that here.
        // Count the number of neighbors of n2 and the number that are shared
        // with base
        for (auto e : graph.edges(n2, galois::MethodFlag::UNPROTECTED)) {
          GNode neighbor = graph.getEdgeDst(e);
          if (base_neighbors.count(neighbor) > 0)
            intersection_size++;
          n2_size++;
        }
        uint32_t union_size =
            base_neighbors.size() + n2_size - intersection_size;
        double similarity =
            union_size > 0 ? (double)intersection_size / union_size : 1;
        // Store the similarity back into the graph.
        n2_data = similarity;
      },
      galois::steal(), galois::loopname("jaccard"));
}

int main(int argc, char** argv) {
  galois::SharedMemSys G;
  LonestarStart(argc, argv, name, desc, url, &inputFile);

  galois::StatTimer totalTime("TimerTotal");
  totalTime.start();

  Graph graph;
  GNode base;
  GNode report;

  std::cout << "Reading from file: " << inputFile << "\n";
  galois::graphs::readGraph(graph, inputFile);
  std::cout << "Read " << graph.size() << " nodes, " << graph.sizeEdges()
            << " edges\n";

  if (baseNode >= graph.size() || reportNode >= graph.size()) {
    std::cerr << "failed to set report: " << reportNode
              << " or failed to set base: " << baseNode << "\n";
    abort();
  }

  auto it = graph.begin();
  std::advance(it, baseNode.getValue());
  base = *it;
  it   = graph.begin();
  std::advance(it, reportNode.getValue());
  report = *it;

  // size_t approxNodeData = 4 * (graph.size() + graph.sizeEdges());
  // galois::preAlloc(8 * numThreads +
  //                  approxNodeData / galois::runtime::pagePoolSize());

  galois::reportPageAlloc("MeminfoPre");

  galois::StatTimer execTime("Timer_0");
  execTime.start();

  algo(graph, base);

  execTime.stop();

  galois::reportPageAlloc("MeminfoPost");

  std::cout << "Node " << reportNode << " has similarity "
            << graph.getData(report) << "\n";

  // Sanity checking code
  galois::GReduceMax<double> maxSimilarity;
  galois::GReduceMin<double> minSimilarity;
  maxSimilarity.reset();
  minSimilarity.reset();

  galois::do_all(
      galois::iterate(graph),
      [&](const GNode& i) {
        double similarity = graph.getData(i);
        if ((unsigned int)i != (unsigned int)base) {
          maxSimilarity.update(similarity);
          minSimilarity.update(similarity);
        }
      },
      galois::loopname("Sanity check"), galois::no_stats());

  double rMaxSimilarity = maxSimilarity.reduce();
  double rMinSimilarity = minSimilarity.reduce();
  galois::gInfo("Maximum similarity (excluding base) is ", rMaxSimilarity);
  galois::gInfo("Minimum similarity is ", rMinSimilarity);
  galois::gInfo("Base similarity is ", graph.getData(base));

  // TODO: Verify?

  if (!skipVerify) {
    if (graph.getData(base) == 1.0) {
      std::cout << "Verification successful.\n";
    } else {
      GALOIS_DIE("verification failed (this algorithm does not support graphs "
                 "with duplicate edges)");
    }
  }

  totalTime.stop();

  return 0;
}
