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
#include "katana/analytics/sssp/sssp.h"

using namespace katana::analytics;

namespace cll = llvm::cl;
namespace {

const char* name = "Single Source Shortest Path";
const char* desc =
    "Computes the shortest path from a source node to all nodes in a directed "
    "graph using a modified chaotic iteration algorithm";
const char* url = "single_source_shortest_path";

cll::opt<std::string> inputFile(
    cll::Positional, cll::desc("<input file>"), cll::Required);
static cll::opt<std::string> startNodesFile(
    "startNodesFile",
    cll::desc("File containing whitespace separated list of source "
              "nodes for computing single-source-shortest path search; "
              "if set, -startNodes is ignored"));
static cll::opt<std::string> startNodesString(
    "startNodes",
    cll::desc("String containing whitespace separated list of source nodes for "
              "computing single-source-shortest path search (default value "
              "'0'); ignore if "
              "-startNodesFile is used"),
    cll::init("0"));
static cll::opt<bool> persistAllDistances(
    "persistAllDistances",
    cll::desc(
        "Flag to indicate whether to persist the distances from all "
        "sources in startNodeFile or startNodesString; By default only the "
        "distances for the last source are persisted (default value false)"),
    cll::init(false));
static cll::opt<unsigned int> reportNode(
    "reportNode", cll::desc("Node to report distance to(default value 1)"),
    cll::init(1));
static cll::opt<unsigned int> stepShift(
    "delta", cll::desc("Shift value for the deltastep (default value 13)"),
    cll::init(13));

static cll::opt<SsspPlan::Algorithm> algo(
    "algo", cll::desc("Choose an algorithm (default value auto):"),
    cll::values(
        clEnumValN(SsspPlan::kDeltaTile, "DeltaTile", "Delta stepping tiled"),
        clEnumValN(SsspPlan::kDeltaStep, "DeltaStep", "Delta stepping"),
        clEnumValN(
            SsspPlan::kDeltaStepBarrier, "DeltaStepBarrier",
            "Delta stepping with barrier"),
        clEnumValN(
            SsspPlan::kSerialDeltaTile, "SerialDeltaTile",
            "Serial delta stepping tiled"),
        clEnumValN(
            SsspPlan::kDeltaStepFusion, "DeltaStepFusion",
            "Delta stepping with barrier and fused buckets"),
        clEnumValN(
            SsspPlan::kSerialDelta, "SerialDelta", "Serial delta stepping"),
        clEnumValN(
            SsspPlan::kDijkstraTile, "DijkstraTile",
            "Dijkstra's algorithm tiled"),
        clEnumValN(SsspPlan::kDijkstra, "Dijkstra", "Dijkstra's algorithm"),
        clEnumValN(SsspPlan::kTopological, "Topo", "Topological"),
        clEnumValN(SsspPlan::kTopologicalTile, "TopoTile", "Topological tiled"),
        clEnumValN(
            SsspPlan::kAutomatic, "Automatic",
            "Automatic: choose among the algorithms automatically")),
    cll::init(SsspPlan::kAutomatic));

static cll::opt<bool> thread_spin(
    "threadSpin",
    cll::desc("If enabled, threads busy-wait for work rather than use "
              "condition variable (default false)"),
    cll::init(false));

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
  case SsspPlan::kDeltaStepFusion:
    return "DeltaStepFusion";
  case SsspPlan::kSerialDeltaTile:
    return "SerialDeltaTile";
  case SsspPlan::kSerialDelta:
    return "SerialDelta";
  case SsspPlan::kDijkstraTile:
    return "DijkstraTile";
  case SsspPlan::kDijkstra:
    return "Dijkstra";
  case SsspPlan::kTopological:
    return "Topological";
  case SsspPlan::kTopologicalTile:
    return "TopologicalTile";
  case SsspPlan::kAutomatic:
    return "Automatic";
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

  if (thread_spin) {
    katana::GetThreadPool().burnPower(katana::getActiveThreads());
  }

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

  std::cout << "Read " << pg->topology().NumNodes() << " nodes, "
            << pg->topology().NumEdges() << " edges\n";

  std::unique_ptr<katana::PropertyGraph> pg_projected_view =
      ProjectPropertyGraphForArguments(pg);

  std::cout << "Projected graph has: "
            << pg_projected_view->topology().NumNodes() << " nodes, "
            << pg_projected_view->topology().NumEdges() << " edges\n";

  if (reportNode >= pg_projected_view->topology().NumNodes()) {
    KATANA_LOG_FATAL("failed to set report: {}", reportNode);
  }

  std::vector<uint32_t> startNodes;
  if (!startNodesFile.getValue().empty()) {
    std::ifstream file(startNodesFile);
    if (!file.good()) {
      KATANA_LOG_FATAL("failed to open file: {}", startNodesFile);
    }
    startNodes.insert(
        startNodes.end(), std::istream_iterator<uint64_t>{file},
        std::istream_iterator<uint64_t>{});
  } else {
    std::istringstream str(startNodesString);
    startNodes.insert(
        startNodes.end(), std::istream_iterator<uint64_t>{str},
        std::istream_iterator<uint64_t>{});
  }
  uint32_t num_sources = startNodes.size();
  std::cout << "Running SSSP for " << num_sources << " sources\n";

  if (algo == SsspPlan::kDeltaStep || algo == SsspPlan::kDeltaTile ||
      algo == SsspPlan::kSerialDelta || algo == SsspPlan::kSerialDeltaTile) {
    std::cout
        << "INFO: Using delta-step of " << (1 << stepShift) << "\n"
        << "WARNING: Performance varies considerably due to delta parameter.\n"
        << "WARNING: Do not expect the default to be good for your graph.\n";
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
  case SsspPlan::kDeltaStepFusion:
    plan = SsspPlan::DeltaStepFusion(stepShift);
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
  case SsspPlan::kTopological:
    plan = SsspPlan::Topological();
    break;
  case SsspPlan::kTopologicalTile:
    plan = SsspPlan::TopologicalTile();
    break;
  case SsspPlan::kAutomatic:
    plan = SsspPlan();
    break;
  default:
    KATANA_LOG_FATAL("Invalid algorithm selected");
  }

  for (auto startNode : startNodes) {
    if (startNode >= pg_projected_view->topology().NumNodes()) {
      KATANA_LOG_FATAL("failed to set source: {}", startNode);
    }

    std::string node_distance_prop = "distance-" + std::to_string(startNode);
    katana::TxnContext txn_ctx;
    auto pg_result = Sssp(
        pg_projected_view.get(), startNode, edge_property_name,
        node_distance_prop, &txn_ctx, plan);
    if (!pg_result) {
      KATANA_LOG_FATAL("Failed to run SSSP: {}", pg_result.error());
    }
    std::cout << "---------------> sssp done\n";

    auto stats_result =
        SsspStatistics::Compute(pg_projected_view.get(), node_distance_prop);
    if (!stats_result) {
      KATANA_LOG_FATAL("Computing statistics: {}", stats_result.error());
    }
    auto stats = stats_result.value();
    stats.Print();
    std::cout << "---------------> sssp statistics done\n";

    if (!skipVerify) {
      if (stats.n_reached_nodes < pg_projected_view->topology().NumNodes()) {
        KATANA_LOG_WARN(
            "{} unvisited nodes; this is an error if the graph is strongly "
            "connected",
            pg_projected_view->topology().NumNodes() - stats.n_reached_nodes);
      }
      if (auto r = SsspAssertValid(
              pg_projected_view.get(), startNode, edge_property_name,
              node_distance_prop, &txn_ctx);
          r) {
        std::cout << "Verification successful.\n";
      } else {
        KATANA_LOG_FATAL("verification failed: ", r.error());
      }
    }

    if (output) {
      std::string output_filename = "output-" + std::to_string(startNode);
      switch (pg_projected_view->GetNodeProperty(node_distance_prop)
                  .value()
                  ->type()
                  ->id()) {
      case arrow::UInt32Type::type_id:
        OutputResults<uint32_t>(
            pg_projected_view.get(), node_distance_prop, output_filename);
        break;
      case arrow::Int32Type::type_id:
        OutputResults<int32_t>(
            pg_projected_view.get(), node_distance_prop, output_filename);
        break;
      case arrow::UInt64Type::type_id:
        OutputResults<uint64_t>(
            pg_projected_view.get(), node_distance_prop, output_filename);
        break;
      case arrow::Int64Type::type_id:
        OutputResults<int64_t>(
            pg_projected_view.get(), node_distance_prop, output_filename);
        break;
      case arrow::FloatType::type_id:
        OutputResults<float>(
            pg_projected_view.get(), node_distance_prop, output_filename);
        break;
      case arrow::DoubleType::type_id:
        OutputResults<double>(
            pg_projected_view.get(), node_distance_prop, output_filename);
        break;
      default:
        KATANA_LOG_FATAL(
            "Unsupported type: {}",
            pg_projected_view->GetNodeProperty(node_distance_prop)
                .value()
                ->type());
        break;
      }
    }
    --num_sources;
    if (num_sources != 0 && !persistAllDistances) {
      if (auto r = pg_projected_view->RemoveNodeProperty(
              node_distance_prop, &txn_ctx);
          !r) {
        KATANA_LOG_FATAL(
            "Failed to remove the node distance property stats {}", r.error());
      }
    }
  }

  totalTime.stop();

  return 0;
}
