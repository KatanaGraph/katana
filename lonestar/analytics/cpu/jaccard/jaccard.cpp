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
#include <unordered_set>

#include <galois/analytics/jaccard/jaccard.h>

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

using NodeValue = galois::PODProperty<double>;

using NodeData = std::tuple<NodeValue>;
using EdgeData = std::tuple<>;

typedef galois::graphs::PropertyGraph<NodeData, EdgeData> Graph;
typedef typename Graph::Node GNode;

int
main(int argc, char** argv) {
  std::unique_ptr<galois::SharedMemSys> G =
      LonestarStart(argc, argv, name, desc, url, &inputFile);

  galois::StatTimer totalTime("TimerTotal");
  totalTime.start();

  std::cout << "Reading from file: " << inputFile << "\n";
  std::unique_ptr<galois::graphs::PropertyFileGraph> pfg =
      MakeFileGraph(inputFile, edge_property_name);
  std::string output_property_name = "jaccard_output_property";

  std::cout << "Read " << pfg->topology().num_nodes() << " nodes, "
            << pfg->topology().num_edges() << " edges\n";

  if (base_node >= pfg->topology().num_nodes() ||
      report_node >= pfg->topology().num_nodes()) {
    std::cerr << "failed to set report: " << report_node
              << " or failed to set base: " << base_node << "\n";
    abort();
  }

  galois::reportPageAlloc("MeminfoPre");

  galois::StatTimer execTime("Timer_0");
  execTime.start();

  if (auto r = galois::analytics::Jaccard(
          pfg.get(), base_node, output_property_name,
          galois::analytics::JaccardPlan::Automatic());
      !r) {
    GALOIS_LOG_FATAL(
        "Jaccard failed: {} {}", r.error().category().name(),
        r.error().message());
  }

  execTime.stop();

  galois::reportPageAlloc("MeminfoPost");

  auto pg_result = galois::graphs::PropertyGraph<NodeData, EdgeData>::Make(
      pfg.get(), {output_property_name}, {});
  if (!pg_result) {
    GALOIS_LOG_FATAL("could not make property graph: {}", pg_result.error());
  }
  Graph graph = pg_result.value();

  std::cout << "Node " << report_node << " has similarity "
            << graph.GetData<NodeValue>(report_node) << "\n";

  // Sanity checking code
  galois::GReduceMax<double> max_similarity;
  galois::GReduceMin<double> min_similarity;
  max_similarity.reset();
  min_similarity.reset();

  galois::do_all(
      galois::iterate(graph),
      [&](const GNode& i) {
        double similarity = graph.GetData<NodeValue>(i);
        if ((unsigned int)i != (unsigned int)base_node) {
          max_similarity.update(similarity);
          min_similarity.update(similarity);
        }
      },
      galois::loopname("Sanity check"), galois::no_stats());

  galois::gInfo(
      "Maximum similarity (excluding base) is ", max_similarity.reduce());
  galois::gInfo("Minimum similarity is ", min_similarity.reduce());
  galois::gInfo("Base similarity is ", graph.GetData<NodeValue>(base_node));

  // TODO: Verify?

  if (!skipVerify) {
    if (graph.GetData<NodeValue>(base_node) == 1.0) {
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
