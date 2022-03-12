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

#include "Lonestar/BoilerPlate.h"
#include "katana/TypedPropertyGraph.h"
#include "katana/analytics/jaccard/jaccard.h"

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

using NodeValue = katana::PODProperty<double>;

using NodeData = std::tuple<NodeValue>;
using EdgeData = std::tuple<>;

using Graph = katana::TypedPropertyGraphView<
    katana::PropertyGraphViews::Default, NodeData, EdgeData>;
using GNode = typename Graph::Node;

int
main(int argc, char** argv) {
  std::unique_ptr<katana::SharedMemSys> G =
      LonestarStart(argc, argv, name, desc, url, &inputFile);

  katana::StatTimer totalTime("TimerTotal");
  totalTime.start();

  std::cout << "Reading from file: " << inputFile << "\n";
  auto res = katana::URI::Make(inputFile);
  if (!res) {
    KATANA_LOG_FATAL("input file {} error: {}", inputFile, res.error());
  }
  auto inputURI = res.value();
  std::unique_ptr<katana::PropertyGraph> pg =
      MakeFileGraph(inputURI, edge_property_name);
  std::string output_property_name = "jaccard_output_property";

  std::cout << "Read " << pg->topology().NumNodes() << " nodes, "
            << pg->topology().NumEdges() << " edges\n";

  std::unique_ptr<katana::PropertyGraph> pg_projected_view =
      ProjectPropertyGraphForArguments(pg);

  std::cout << "Projected graph has: "
            << pg_projected_view->topology().NumNodes() << " nodes, "
            << pg_projected_view->topology().NumEdges() << " edges\n";

  if (base_node >= pg_projected_view->topology().NumNodes() ||
      report_node >= pg_projected_view->topology().NumNodes()) {
    std::cerr << "failed to set report: " << report_node
              << " or failed to set base: " << base_node << "\n";
    abort();
  }

  katana::TxnContext txn_ctx;
  if (auto r = katana::analytics::Jaccard(
          pg_projected_view.get(), base_node, output_property_name, &txn_ctx,
          katana::analytics::JaccardPlan());
      !r) {
    KATANA_LOG_FATAL("Jaccard failed: {}", r.error());
  }

  /// TODO (Yasin): not sure whythe following  KATANA_CHECKED gave me error here.
  /// Graph graph = KATANA_CHECKED(Graph::Make(pg_projected_view.get(), {output_property_name}, {}));
  auto pg_result = katana::TypedPropertyGraphView<
      katana::PropertyGraphViews::Default, NodeData,
      EdgeData>::Make(pg_projected_view.get(), {output_property_name}, {});
  if (!pg_result) {
    KATANA_LOG_FATAL("could not make property graph: {}", pg_result.error());
  }
  Graph graph = pg_result.value();

  std::cout << "Node " << report_node << " has similarity "
            << graph.GetData<NodeValue>(report_node) << "\n";

  auto stats_result = katana::analytics::JaccardStatistics::Compute(
      pg_projected_view.get(), base_node, output_property_name);
  if (!stats_result) {
    KATANA_LOG_FATAL(
        "could not make compute statistics: {}", stats_result.error());
  }

  stats_result.value().Print();

  if (!skipVerify) {
    if (katana::analytics::JaccardAssertValid(
            pg_projected_view.get(), base_node, output_property_name)) {
      std::cout << "Verification successful.\n";
    } else {
      KATANA_LOG_FATAL(
          "verification failed (this algorithm does not support graphs "
          "with duplicate edges)");
    }
  }

  totalTime.stop();

  return 0;
}
