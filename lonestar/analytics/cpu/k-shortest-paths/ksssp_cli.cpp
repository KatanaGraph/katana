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

static cll::opt<SsspPlan::Algorithm> algo(
    "algo", cll::desc("Choose an algorithm (default value auto):"),
    cll::values(
        clEnumValN(SsspPlan::kDeltaTile, "DeltaTile", "Delta stepping tiled"),
        clEnumValN(SsspPlan::kDeltaStep, "DeltaStep", "Delta stepping"),
        clEnumValN(
            SsspPlan::kDeltaStepBarrier, "DeltaStepBarrier",
            "Delta stepping with barrier")),
    cll::init(SsspPlan::kDeltaTile));

static cll::opt<AlgoReachability> algoReachability(
    "algoReachability", cll::desc("Choose an algorithm for reachability:"),
    cll::values(clEnumVal(async, "async"), clEnumVal(syncLevel, "syncLevel")),
    cll::init(syncLevel));

static cll::opt<bool> thread_spin(
    "threadSpin",
    cll::desc("If enabled, threads busy-wait for work rather than use "
              "condition variable (default false)"),
    cll::init(false));

std::string
AlgorithmName(SsspPlan::Algorithm algorithm) {
  switch (algorithm) {
  case SsspPlan::kDeltaTile:
    return "DeltaTile";
  case SsspPlan::kDeltaStep:
    return "DeltaStep";
  case SsspPlan::kDeltaStepBarrier:
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
  std::unique_ptr<katana::PropertyGraph> pg =
      MakeFileGraph(inputFile, edge_property_name);

  std::cout << "Read " << pg->topology().NumNodes() << " nodes, "
            << pg->topology().NumEdges() << " edges\n";

  if (algo == SsspPlan::kDeltaStep || algo == SsspPlan::kDeltaTile) {
    katana::gInfo("Using delta-step of ", (1 << stepShift), "\n");
    KATANA_LOG_WARN(
        "Performance varies considerably due to delta parameter.\n");
    KATANA_LOG_WARN("Do not expect the default to be good for your graph.\n");
  }

  std::cout << "Running " << AlgorithmName(algo) << " algorithm\n";

  SsspPlan plan;
  switch (algo) {
  case SsspPlan::kDeltaTile:
    plan = SsspPlan::DeltaTile(stepShift);
    break;
  case SsspPlan::kDeltaStep:
    plan = SsspPlan::DeltaStep(stepShift);
    break;
  case SsspPlan::kDeltaStepBarrier:
    plan = SsspPlan::DeltaStepBarrier(stepShift);
    break;
  default:
    KATANA_LOG_FATAL("Invalid algorithm selected");
  }

  if (startNode >= pg->topology().size() ||
      reportNode >= pg->topology().size()) {
    KATANA_LOG_ERROR(
        "failed to set report: ", reportNode,
        " or failed to set source: ", startNode, "\n");
    assert(0);
    abort();
  }

  katana::TxnContext txn_ctx;

  auto pg_result = Ksssp(
      pg.get(), edge_property_name, startNode, reportNode, &txn_ctx,
      algoReachability, numPaths, stepShift, symmetricGraph, plan);

  if (!pg_result) {
    KATANA_LOG_FATAL("failed to run ksssp: {}", pg_result.error());
  }

  totalTime.stop();

  return 0;
}
