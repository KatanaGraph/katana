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

#include <katana/analytics/louvain_clustering/louvain_clustering.h>

#include "Lonestar/BoilerPlate.h"

using namespace katana::analytics;

namespace cll = llvm::cl;

static const char* name = "Louvain Clustering";

static const char* desc =
    "Computes the clusters in the graph using Louvain Clustering algorithm";

static const char* url = "louvain_clustering";

static cll::opt<std::string> inputFile(
    cll::Positional, cll::desc("<input file>"), cll::Required);

namespace cll = llvm::cl;
static cll::opt<bool> enable_vf(
    "enable_vf", cll::desc("Flag to enable vertex following optimization."),
    cll::init(false));

static cll::opt<double> modularity_threshold_per_round(
    "modularity_threshold_per_round",
    cll::desc("Threshold for modularity gain"), cll::init(0.01));

static cll::opt<double> modularity_threshold_total(
    "modularity_threshold_total",
    cll::desc("Total modularity_threshold_total for modularity gain"),
    cll::init(0.01));

static cll::opt<uint32_t> max_iterations(
    "max_iterations", cll::desc("Maximum number of iterations to execute"),
    cll::init(10));

static cll::opt<uint32_t> min_graph_size(
    "min_graph_size", cll::desc("Minimum coarsened graph size"),
    cll::init(100));

static cll::opt<LouvainClusteringPlan::Algorithm> algo(
    "algo", cll::desc("Choose an algorithm (default value DoAll):"),
    cll::values(clEnumValN(
        LouvainClusteringPlan::kDoAll, "DoAll",
        "Use Katana do_all loop for conflict mitigation")),
    cll::init(LouvainClusteringPlan::kDoAll));

std::string
AlgorithmName(LouvainClusteringPlan::Algorithm algorithm) {
  switch (algorithm) {
  case LouvainClusteringPlan::kDoAll:
    return "DoAll";
  default:
    return "Unknown";
  }
}

int
main(int argc, char** argv) {
  std::unique_ptr<katana::SharedMemSys> G =
      LonestarStart(argc, argv, name, desc, url, &inputFile);

  katana::StatTimer totalTime("TimerTotal");
  totalTime.start();

  if (!symmetricGraph) {
    KATANA_LOG_FATAL(
        "This application requires a symmetric graph input;"
        " please use the -symmetricGraph flag "
        " to indicate the input is a symmetric graph.");
  }

  std::cout << "Reading from file: " << inputFile << "\n";
  std::unique_ptr<katana::PropertyGraph> pg =
      MakeFileGraph(inputFile, edge_property_name);

  std::cout << "Read " << pg->topology().num_nodes() << " nodes, "
            << pg->topology().num_edges() << " edges\n";

  std::cout << "Running " << AlgorithmName(algo) << " algorithm\n";

  LouvainClusteringPlan plan = LouvainClusteringPlan();
  switch (algo) {
  case LouvainClusteringPlan::kDoAll:
    plan = LouvainClusteringPlan::DoAll(
        enable_vf, modularity_threshold_per_round, modularity_threshold_total,
        max_iterations, min_graph_size);
    break;
  default:
    KATANA_LOG_FATAL("invalid algorithm");
  }

  auto pg_result =
      LouvainClustering(pg.get(), edge_property_name, "clusterId", plan);
  if (!pg_result) {
    KATANA_LOG_FATAL("Failed to run LouvainClustering: {}", pg_result.error());
  }

  auto stats_result = LouvainClusteringStatistics::Compute(
      pg.get(), edge_property_name, "clusterId");
  if (!stats_result) {
    KATANA_LOG_FATAL(
        "Failed to compute LouvainClustering statistics: {}",
        stats_result.error());
  }
  auto stats = stats_result.value();
  stats.Print();

  if (!skipVerify) {
    if (LouvainClusteringAssertValid(
            pg.get(), edge_property_name, "clusterId")) {
      std::cout << "Verification successful.\n";
    } else {
      KATANA_LOG_FATAL("verification failed");
    }
  }

  if (output) {
    auto r = pg->GetNodePropertyTyped<uint64_t>("clusterId");
    if (!r) {
      KATANA_LOG_FATAL("Failed to get node property {}", r.error());
    }
    auto results = r.value();
    KATANA_LOG_DEBUG_ASSERT(
        uint64_t(results->length()) == pg->topology().num_nodes());

    writeOutput(outputLocation, results->raw_values(), results->length());
  }

  totalTime.stop();

  return 0;
}
