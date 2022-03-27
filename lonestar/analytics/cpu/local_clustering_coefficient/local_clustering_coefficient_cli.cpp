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

#include <iostream>

#include "Lonestar/BoilerPlate.h"
#include "katana/analytics/local_clustering_coefficient/local_clustering_coefficient.h"

using namespace katana::analytics;

const char* name = "Local Clustering Coefficient";
const char* desc = "Computes the local clustering coefficient for each node";

namespace cll = llvm::cl;

static cll::opt<std::string> inputFile(
    cll::Positional, cll::desc("<input file>"), cll::Required);
static cll::opt<LocalClusteringCoefficientPlan::Algorithm> algo(
    "algo", cll::desc("Choose an algorithm:"),
    cll::values(clEnumValN(
        LocalClusteringCoefficientPlan::kOrderedCountAtomics,
        "orderedCountAtomics", "Ordered Simple Count using Atomics")),
    cll::values(clEnumValN(
        LocalClusteringCoefficientPlan::kOrderedCountPerThread,
        "orderedCountPerThread",
        "Ordered Simple Count using PerThreadStorage (default)")),
    cll::init(LocalClusteringCoefficientPlan::kOrderedCountPerThread));

static cll::opt<bool> relabel(
    "relabel",
    cll::desc("Relabel nodes of the graph (default value of false => "
              "choose automatically)"),
    cll::init(false));
int
main(int argc, char** argv) {
  std::unique_ptr<katana::SharedMemSys> G =
      LonestarStart(argc, argv, name, desc, nullptr, &inputFile);

  katana::StatTimer totalTime("TimerTotal");
  totalTime.start();

  if (!symmetricGraph) {
    KATANA_DIE(
        "This application requires a symmetric graph input;"
        " please use the -symmetricGraph flag "
        " to indicate the input is a symmetric graph.");
  }

  std::cout << "Reading from file: " << inputFile << "\n";
  auto res = katana::URI::Make(inputFile);
  if (!res) {
    KATANA_LOG_FATAL("input file {} error: {}", inputFile, res.error());
  }
  auto uri = res.value();

  std::unique_ptr<katana::PropertyGraph> pg =
      MakeFileGraph(uri, edge_property_name);

  std::cout << "Read " << pg->topology().NumNodes() << " nodes, "
            << pg->topology().NumEdges() << " edges\n";

  std::unique_ptr<katana::PropertyGraph> pg_projected_view =
      ProjectPropertyGraphForArguments(pg);

  std::cout << "Projected graph has: "
            << pg_projected_view->topology().NumNodes() << " nodes, "
            << pg_projected_view->topology().NumEdges() << " edges\n";

  LocalClusteringCoefficientPlan plan;

  LocalClusteringCoefficientPlan::Relabeling relabeling_flag =
      relabel ? LocalClusteringCoefficientPlan::kRelabel
              : LocalClusteringCoefficientPlan::kAutoRelabel;

  switch (algo) {
  case LocalClusteringCoefficientPlan::kOrderedCountAtomics:
    plan = LocalClusteringCoefficientPlan::OrderedCountAtomics(relabeling_flag);
    break;
  case LocalClusteringCoefficientPlan::kOrderedCountPerThread:
    plan =
        LocalClusteringCoefficientPlan::OrderedCountPerThread(relabeling_flag);
    break;
  default:
    std::cerr << "Unknown algo: " << algo << "\n";
  }

  katana::TxnContext txn_ctx;
  auto lcc_result = LocalClusteringCoefficient(
      pg_projected_view.get(), "localClusteringCoefficient", &txn_ctx, plan);
  if (!lcc_result) {
    KATANA_LOG_FATAL("Failed to run algorithm: {}", lcc_result.error());
  }

  totalTime.stop();

  return 0;
}
