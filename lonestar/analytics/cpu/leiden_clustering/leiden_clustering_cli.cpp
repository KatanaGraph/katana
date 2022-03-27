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

#include <katana/analytics/leiden_clustering/leiden_clustering.h>

#include "Lonestar/BoilerPlate.h"

using namespace katana::analytics;

namespace cll = llvm::cl;

static const char* name = "Leiden Clustering";

static const char* desc =
    "Computes the clusters in the graph using Leiden Clustering algorithm";

static const char* url = "leiden_clustering";

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

static cll::opt<LeidenClusteringPlan::Algorithm> algo(
    "algo", cll::desc("Choose an algorithm (default value DoAll):"),
    cll::values(
        clEnumValN(
            LeidenClusteringPlan::kDoAll, "DoAll",
            "Use Katana do_all loop for conflict mitigation"),
        clEnumValN(
            LeidenClusteringPlan::kDeterministic, "Deterministic",
            "Use Deterministic implementation")),
    cll::init(LeidenClusteringPlan::kDoAll));

static cll::opt<double> resolution(
    "resolution", cll::desc("Resolution for CPM quality function."),
    cll::init(1.0));

static cll::opt<double> randomness(
    "randomness",
    cll::desc("Randomness factor for refining clusters in Leiden."),
    cll::init(0.01));

std::string
AlgorithmName(LeidenClusteringPlan::Algorithm algorithm) {
  switch (algorithm) {
  case LeidenClusteringPlan::kDoAll:
    return "DoAll";
  case LeidenClusteringPlan::kDeterministic:
    return "Deterministic";
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

  if (symmetricGraph) {
    KATANA_LOG_WARN(
        "This application requires a symmetric graph input;"
        " Using the -symmetricGraph flag "
        " indicates that the input is a symmetric graph and can be used as it "
        "is.");
  }

  std::cout << "Reading from file: " << inputFile << "\n";
  auto res = katana::URI::Make(inputFile);
  if (!res) {
    KATANA_LOG_FATAL("input file {} error: {}", inputFile, res.error());
  }
  auto inputURI = res.value();
  std::unique_ptr<katana::PropertyGraph> pg =
      MakeFileGraph(inputURI, edge_property_name);

  std::cout << "Read " << pg->topology().NumNodes() << " nodes, "
            << pg->topology().NumEdges() << " edges\n";

  std::cout << "Running " << AlgorithmName(algo) << " algorithm\n";

  std::unique_ptr<katana::PropertyGraph> pg_projected_view =
      ProjectPropertyGraphForArguments(pg);

  std::cout << "Projected graph has: "
            << pg_projected_view->topology().NumNodes() << " nodes, "
            << pg_projected_view->topology().NumEdges() << " edges\n";

  LeidenClusteringPlan plan = LeidenClusteringPlan();
  switch (algo) {
  case LeidenClusteringPlan::kDoAll:
    plan = LeidenClusteringPlan::DoAll(
        enable_vf, modularity_threshold_per_round, modularity_threshold_total,
        max_iterations, min_graph_size, resolution, randomness);
    break;
  case LeidenClusteringPlan::kDeterministic:
    plan = LeidenClusteringPlan::Deterministic(
        enable_vf, modularity_threshold_per_round, modularity_threshold_total,
        max_iterations, min_graph_size, resolution, randomness);
    break;
  default:
    KATANA_LOG_FATAL("invalid algorithm");
  }

  katana::TxnContext txn_ctx;
  auto pg_result = LeidenClustering(
      pg_projected_view.get(), edge_property_name, "clusterId", &txn_ctx,
      symmetricGraph, plan);
  if (!pg_result) {
    KATANA_LOG_FATAL("Failed to run LeidenClustering: {}", pg_result.error());
  }

  auto stats_result = LeidenClusteringStatistics::Compute(
      pg_projected_view.get(), edge_property_name, "clusterId", &txn_ctx);
  if (!stats_result) {
    KATANA_LOG_FATAL(
        "Failed to compute LeidenClustering statistics: {}",
        stats_result.error());
  }
  auto stats = stats_result.value();
  stats.Print();

  if (!skipVerify) {
    if (LeidenClusteringAssertValid(
            pg_projected_view.get(), edge_property_name, "clusterId")) {
      std::cout << "Verification successful.\n";
    } else {
      KATANA_LOG_FATAL("verification failed");
    }
  }

  if (output) {
    auto r = pg_projected_view->GetNodePropertyTyped<uint64_t>("clusterId");
    if (!r) {
      KATANA_LOG_FATAL("Failed to get node property {}", r.error());
    }
    auto results = r.value();
    KATANA_LOG_DEBUG_ASSERT(
        uint64_t(results->length()) ==
        pg_projected_view->topology().NumNodes());

    writeOutput(outputLocation, results->raw_values(), results->length());
  }

  totalTime.stop();

  return 0;
}
