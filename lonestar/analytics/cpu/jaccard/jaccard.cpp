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

#include <deque>
#include <iostream>
#include <type_traits>
#include <unordered_set>

#include "Lonestar/BoilerPlate.h"

namespace cll = llvm::cl;

static const char* name = "Jaccard Similarity";

static const char* desc =
    "Compute the similarity of nodes (to some base node) "
    "based on the similarity of their neighbor sets.";

static const char* url = "jaccard";

static cll::opt<std::string> inputFile(
    cll::Positional, cll::desc("<input file>"), cll::Required);
static cll::opt<unsigned int> base_node(
    "baseNode", cll::desc("Node to compute similarity to (default value 0)"),
    cll::init(0));
static cll::opt<unsigned int> report_node(
    "reportNode",
    cll::desc("Node to report the similarity of (default value 1)"),
    cll::init(1));

struct NodeValue : public galois::PODProperty<double> {};

using NodeData = std::tuple<NodeValue>;
using EdgeData = std::tuple<>;

typedef galois::graphs::PropertyGraph<NodeData, EdgeData> Graph;
typedef typename Graph::Node GNode;

void
algo(Graph* graph, const GNode& base) {
  std::unordered_set<GNode> base_neighbors;

  // Collect all the neighbors of the base node into a hash set.
  for (const auto& e : graph->edges(base)) {
    const GNode& dest = *graph->GetEdgeDest(e);
    base_neighbors.emplace(dest);
  }

  // Compute the similarity for each node
  galois::do_all(
      galois::iterate(*graph),
      [&](const GNode& n2) {
        double& n2_data = graph->GetData<NodeValue>(n2);
        uint32_t n2_size = 0, intersection_size = 0;
        // TODO: Using a sorted edge list would allow a much faster intersection
        // operation. Use that here.
        // Count the number of neighbors of n2 and the number that are shared
        // with base
        for (const auto& e : graph->edges(n2)) {
          const GNode& neighbor = *graph->GetEdgeDest(e);
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

int
main(int argc, char** argv) {
  std::unique_ptr<galois::SharedMemSys> G =
      LonestarStart(argc, argv, name, desc, url, &inputFile);

  galois::StatTimer totalTime("TimerTotal");
  totalTime.start();

  std::cout << "Reading from file: " << inputFile << "\n";
  std::unique_ptr<galois::graphs::PropertyFileGraph> pfg =
      MakeFileGraph(inputFile, edge_property_name);

  auto result = ConstructNodeProperties<NodeData>(pfg.get());
  if (!result) {
    GALOIS_LOG_FATAL("failed to construct node properties: {}", result.error());
  }

  auto pg_result =
      galois::graphs::PropertyGraph<NodeData, EdgeData>::Make(pfg.get());
  if (!pg_result) {
    GALOIS_LOG_FATAL("could not make property graph: {}", pg_result.error());
  }
  Graph graph = pg_result.value();

  std::cout << "Read " << graph.num_nodes() << " nodes, " << graph.num_edges()
            << " edges\n";

  if (base_node >= graph.size() || report_node >= graph.size()) {
    std::cerr << "failed to set report: " << report_node
              << " or failed to set base: " << base_node << "\n";
    abort();
  }

  auto it = graph.begin();
  std::advance(it, base_node.getValue());
  GNode base = *it;
  it = graph.begin();
  std::advance(it, report_node.getValue());
  GNode report = *it;

  galois::reportPageAlloc("MeminfoPre");

  galois::StatTimer execTime("Timer_0");
  execTime.start();

  algo(&graph, base);

  execTime.stop();

  galois::reportPageAlloc("MeminfoPost");

  std::cout << "Node " << report_node << " has similarity "
            << graph.GetData<NodeValue>(report) << "\n";

  // Sanity checking code
  galois::GReduceMax<double> max_similarity;
  galois::GReduceMin<double> min_similarity;
  max_similarity.reset();
  min_similarity.reset();

  galois::do_all(
      galois::iterate(graph),
      [&](const GNode& i) {
        double similarity = graph.GetData<NodeValue>(i);
        if ((unsigned int)i != (unsigned int)base) {
          max_similarity.update(similarity);
          min_similarity.update(similarity);
        }
      },
      galois::loopname("Sanity check"), galois::no_stats());

  galois::gInfo(
      "Maximum similarity (excluding base) is ", max_similarity.reduce());
  galois::gInfo("Minimum similarity is ", min_similarity.reduce());
  galois::gInfo("Base similarity is ", graph.GetData<NodeValue>(base));

  // TODO: Verify?

  if (!skipVerify) {
    if (graph.GetData<NodeValue>(base) == 1.0) {
      std::cout << "Verification successful.\n";
    } else {
      GALOIS_LOG_FATAL(
          "verification failed (this algorithm does not support graphs "
          "with duplicate edges)");
    }
  }

  totalTime.stop();

  return 0;
}
