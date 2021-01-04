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
#include "galois/analytics/sssp/sssp.h"

using namespace galois::analytics;

namespace cll = llvm::cl;

static const char* name = "Single Source Shortest Path";
static const char* desc =
    "Computes the shortest path from a source node to all nodes in a directed "
    "graph using a modified chaotic iteration algorithm";
static const char* url = "single_source_shortest_path";

static cll::opt<std::string> inputFile(
    cll::Positional, cll::desc("<input file>"), cll::Required);
static cll::opt<unsigned int> startNode(
    "startNode", cll::desc("Node to start search from (default value 0)"),
    cll::init(0));
static cll::opt<unsigned int> reportNode(
    "reportNode", cll::desc("Node to report distance to(default value 1)"),
    cll::init(1));
static cll::opt<unsigned int> stepShift(
    "delta", cll::desc("Shift value for the deltastep (default value 13)"),
    cll::init(13));

static cll::opt<SsspPlan::Algorithm> algo(
    "algo", cll::desc("Choose an algorithm (default value auto):"),
    cll::values(
        clEnumVal(SsspPlan::kDeltaTile, "DeltaTile"),
        clEnumVal(SsspPlan::kDeltaStep, "DeltaStep"),
        clEnumVal(SsspPlan::kDeltaStepBarrier, "DeltaStepBarrier"),
        clEnumVal(SsspPlan::kSerialDeltaTile, "SerialDeltaTile"),
        clEnumVal(SsspPlan::kSerialDelta, "SerialDelta"),
        clEnumVal(SsspPlan::kDijkstraTile, "DijkstraTile"),
        clEnumVal(SsspPlan::kDijkstra, "Dijkstra"),
        clEnumVal(SsspPlan::kTopo, "Topo"),
        clEnumVal(SsspPlan::kTopoTile, "TopoTile"),
        clEnumVal(
            SsspPlan::kAutomatic,
            "Automatic: choose among the algorithms automatically")),
    cll::init(SsspPlan::kAutomatic));

//TODO (gill) Remove snippets from documentation
//! [withnumaalloc]
//! [withnumaalloc]

std::string
AlgorithmName(SsspPlan::Algorithm algorithm) {
  switch (algorithm) {
  case SsspPlan::kDeltaTile:
    return "DeltaTile";
  case SsspPlan::kDeltaStep:
    return "DeltaStep";
  case SsspPlan::kDeltaStepBarrier:
    return "DeltaStepBarrier";
  case SsspPlan::kSerialDeltaTile:
    return "SerialDeltaTile";
  case SsspPlan::kSerialDelta:
    return "SerialDelta";
  case SsspPlan::kDijkstraTile:
    return "DijkstraTile";
  case SsspPlan::kDijkstra:
    return "Dijkstra";
  case SsspPlan::kTopo:
    return "Topo";
  case SsspPlan::kTopoTile:
    return "TopoTile";
  case SsspPlan::kAutomatic:
    return "Automatic";
  default:
    return "Unknown";
  }
}

template <typename Weight>
static void
OutputResults(galois::graphs::PropertyFileGraph* pfg) {
  auto r = pfg->NodePropertyTyped<Weight>("distance");
  if (!r) {
    GALOIS_LOG_FATAL("Error getting results: {}", r.error().message());
  }
  auto results = r.value();
  assert(uint64_t(results->length()) == pfg->topology().num_nodes());

  writeOutput(outputLocation, results->raw_values(), results->length());
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

  std::cout << "Read " << pfg->topology().num_nodes() << " nodes, "
            << pfg->topology().num_edges() << " edges\n";

  if (startNode >= pfg->topology().num_nodes() ||
      reportNode >= pfg->topology().num_nodes()) {
    GALOIS_LOG_FATAL(
        "failed to set report: {} or failed to set source: {}", reportNode,
        startNode);
  }

  galois::reportPageAlloc("MeminfoPre");

  if (algo == SsspPlan::kDeltaStep || algo == SsspPlan::kDeltaTile ||
      algo == SsspPlan::kSerialDelta || algo == SsspPlan::kSerialDeltaTile) {
    std::cout
        << "INFO: Using delta-step of " << (1 << stepShift) << "\n"
        << "WARNING: Performance varies considerably due to delta parameter.\n"
        << "WARNING: Do not expect the default to be good for your graph.\n";
  }

  std::cout << "Running " << AlgorithmName(algo) << " algorithm\n";

  SsspPlan plan = SsspPlan::Automatic();
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
  case SsspPlan::kSerialDeltaTile:
    plan = SsspPlan::SerialDeltaTile(stepShift);
    break;
  case SsspPlan::kSerialDelta:
    plan = SsspPlan::SerialDelta(stepShift);
    break;
  case SsspPlan::kDijkstraTile:
    plan = SsspPlan::DijkstraTile();
    break;
  case SsspPlan::kDijkstra:
    plan = SsspPlan::Dijkstra();
    break;
  case SsspPlan::kTopo:
    plan = SsspPlan::Topo();
    break;
  case SsspPlan::kTopoTile:
    plan = SsspPlan::TopoTile();
    break;
  case SsspPlan::kAutomatic:
    plan = SsspPlan::Automatic();
    break;
  default:
    GALOIS_LOG_FATAL("Invalid algorithm selected");
  }

  auto pg_result =
      Sssp(pfg.get(), startNode, edge_property_name, "distance", plan);
  if (!pg_result) {
    GALOIS_LOG_FATAL("Failed to run SSSP: {}", pg_result.error());
  }

  auto stats_result = SsspStatistics::Compute(pfg.get(), "distance");
  if (!stats_result) {
    GALOIS_LOG_FATAL(
        "Computing statistics: {}", stats_result.error().message());
  }
  auto stats = stats_result.value();
  stats.Print();

  if (!skipVerify) {
    if (stats.n_reached_nodes < pfg->topology().num_nodes()) {
      GALOIS_LOG_WARN(
          "{} unvisited nodes; this is an error if the graph is strongly "
          "connected",
          pfg->topology().num_nodes() - stats.n_reached_nodes);
    }
    if (auto r =
            SsspValidate(pfg.get(), startNode, edge_property_name, "distance");
        r && r.value()) {
      std::cout << "Verification successful.\n";
    } else {
      GALOIS_LOG_FATAL(
          "verification failed: ", r.has_error() ? r.error().message() : "");
    }
  }

  if (output) {
    switch (pfg->NodeProperty("distance")->type()->id()) {
    case arrow::UInt32Type::type_id:
      OutputResults<uint32_t>(pfg.get());
      break;
    case arrow::Int32Type::type_id:
      OutputResults<int32_t>(pfg.get());
      break;
    case arrow::UInt64Type::type_id:
      OutputResults<uint64_t>(pfg.get());
      break;
    case arrow::Int64Type::type_id:
      OutputResults<int64_t>(pfg.get());
      break;
    case arrow::FloatType::type_id:
      OutputResults<float>(pfg.get());
      break;
    case arrow::DoubleType::type_id:
      OutputResults<double>(pfg.get());
      break;
    default:
      GALOIS_LOG_FATAL(
          "Unsupported type: {}", pfg->NodeProperty("distance")->type());
      break;
    }
  }

  totalTime.stop();

  return 0;
}
