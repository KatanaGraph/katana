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
#include "katana/analytics/k_shortest_paths/ksssp.h"

using namespace katana::analytics;

namespace cll = llvm::cl;
namespace {

static const char* name = "Single Source k Shortest Paths";
static const char* desc =
    "Computes the k shortest paths from a source node to all nodes in a "
    "directed "
    "graph using a modified chaotic iteration algorithm";
static const char* url = "k_shortest_paths";

static cll::opt<std::string> inputFile(
    cll::Positional, cll::desc("<input file>"), cll::Required);
static cll::opt<uint32_t> startNode(
    "startNode", cll::desc("Node to start search from (default value 0)"),
    cll::init(0));
static cll::opt<uint32_t> reportNode(
    "reportNode", cll::desc("Node to report distance to (default value 1)"),
    cll::init(0));
static cll::opt<uint32_t> stepShift(
    "delta", cll::desc("Shift value for the deltastep (default value 13)"),
    cll::init(13));
static cll::opt<uint32_t> numPaths(
    "numPaths",
    cll::desc("Number of paths to compute from source to report node (default "
              "value 1)"),
    cll::init(1));

static cll::opt<KssspPlan::Algorithm> algo(
    "algo", cll::desc("Choose an algorithm (default value kDeltaStep):"),
    cll::values(
        clEnumValN(KssspPlan::kDeltaTile, "DeltaTile", "Delta stepping tiled"),
        clEnumValN(KssspPlan::kDeltaStep, "DeltaStep", "Delta stepping"),
        clEnumValN(
            KssspPlan::kDeltaStepBarrier, "DeltaStepBarrier",
            "Delta stepping with barrier")),
    cll::init(KssspPlan::kDeltaTile));

static cll::opt<KssspPlan::Reachability> reachability(
    "reachability", cll::desc("Choose an algorithm for reachability:"),
    cll::values(
        clEnumValN(KssspPlan::asyncLevel, "async", "Asynchronous reachability"),
        clEnumValN(
            KssspPlan::syncLevel, "syncLevel", "Synchronous reachability")),
    cll::init(KssspPlan::syncLevel));

static cll::opt<bool> thread_spin(
    "threadSpin",
    cll::desc("If enabled, threads busy-wait for work rather than use "
              "condition variable (default false)"),
    cll::init(false));

std::string
AlgorithmName(KssspPlan::Algorithm algorithm) {
  switch (algorithm) {
  case KssspPlan::kDeltaTile:
    return "DeltaTile";
  case KssspPlan::kDeltaStep:
    return "DeltaStep";
  case KssspPlan::kDeltaStepBarrier:
    return "DeltaStepBarrier";
  default:
    return "Unknown";
  }
}

template <typename Weight>
static void
OutputResults(
    katana::PropertyGraph* pg, std::string node_distance_prop,
    std::string output_filename = "output") {
  auto r = pg->GetNodePropertyTyped<Weight>(node_distance_prop);
  if (!r) {
    KATANA_LOG_FATAL("Error getting results: {}", r.error());
  }
  auto results = r.value();
  KATANA_LOG_DEBUG_ASSERT(
      uint64_t(results->length()) == pg->topology().NumNodes());

  writeOutput(
      outputLocation, results->raw_values(), results->length(),
      output_filename);
}
}  // namespace

int
main(int argc, char** argv) {
  std::unique_ptr<katana::SharedMemSys> G =
      LonestarStart(argc, argv, name, desc, url, &inputFile);

  katana::StatTimer totalTime("TimerTotal");
  totalTime.start();

  katana::gInfo("Reading from file: ", inputFile, "\n");
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
  if (algo == KssspPlan::kDeltaStep || algo == KssspPlan::kDeltaTile) {
    katana::gInfo("Using delta-step of ", (1 << stepShift), "\n");
    KATANA_LOG_WARN(
        "Performance varies considerably due to delta parameter.\n");
    KATANA_LOG_WARN("Do not expect the default to be good for your graph.\n");
  }

  std::cout << "Running " << AlgorithmName(algo) << " algorithm\n";

  if (reachability != KssspPlan::asyncLevel &&
      reachability != KssspPlan::syncLevel) {
    KATANA_LOG_FATAL("Invalid reachability algorithm selected");
  }

  KssspPlan plan;
  switch (algo) {
  case KssspPlan::kDeltaTile:
    plan = KssspPlan::DeltaTile(reachability, stepShift);
    break;
  case KssspPlan::kDeltaStep:
    plan = KssspPlan::DeltaStep(reachability, stepShift);
    break;
  case KssspPlan::kDeltaStepBarrier:
    plan = KssspPlan::DeltaStepBarrier(reachability, stepShift);
    break;
  default:
    KATANA_LOG_FATAL("Invalid algorithm selected");
  }

  if (startNode >= pg_projected_view->topology().size() ||
      reportNode >= pg_projected_view->topology().size()) {
    KATANA_LOG_ERROR(
        "failed to set report: ", reportNode,
        " or failed to set source: ", startNode, "\n");
    assert(0);
    abort();
  }

  katana::TxnContext txn_ctx;

  auto pg_result = Ksssp(
      pg_projected_view.get(), edge_property_name, startNode, reportNode,
      numPaths, symmetricGraph, &txn_ctx, plan);

  if (!pg_result) {
    KATANA_LOG_FATAL("failed to run ksssp: {}", pg_result.error());
  }

  totalTime.stop();

  return 0;
}
