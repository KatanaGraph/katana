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

/******************************************************************************/
/* Make results */
/******************************************************************************/

template <typename Weight>
static std::vector<Weight>
MakeResults(const typename SsspImplementation<Weight>::Graph& graph) {
  std::vector<Weight> values;

  values.reserve(graph.num_nodes());
  for (auto node : graph) {
    auto& dist_current = graph.template GetData<SsspNodeDistance<Weight>>(node);
    values.push_back(dist_current);
  }

  return values;
}

template <typename Weight>
static void
ProcessOutput(
    galois::graphs::PropertyFileGraph* pfg,
    const std::string& edge_weight_property_name,
    const std::string& output_property_name) {
  using Impl = galois::analytics::SsspImplementation<Weight>;

  auto pg_result = galois::graphs::
      PropertyGraph<typename Impl::NodeData, typename Impl::EdgeData>::Make(
          pfg, {output_property_name}, {edge_weight_property_name});
  auto graph = pg_result.value();

  galois::reportPageAlloc("MeminfoPost");

  auto it = graph.begin();
  std::advance(it, startNode.getValue());
  auto source = *it;
  it = graph.begin();
  std::advance(it, reportNode.getValue());
  auto report = *it;

  std::cout << "Node " << reportNode << " has distance "
            << graph.template GetData<SsspNodeDistance<Weight>>(report) << "\n";

  // Sanity checking code
  galois::GReduceMax<uint64_t> max_dist;
  galois::GAccumulator<uint64_t> sum_dist;
  galois::GAccumulator<uint32_t> num_visited;
  max_dist.reset();
  sum_dist.reset();
  num_visited.reset();

  galois::do_all(
      galois::iterate(graph),
      [&](uint64_t i) {
        uint32_t my_distance =
            graph.template GetData<SsspNodeDistance<Weight>>(i);

        if (my_distance != Impl::kDistanceInfinity) {
          max_dist.update(my_distance);
          sum_dist += my_distance;
          num_visited += 1;
        }
      },
      galois::loopname("Sanity check"), galois::no_stats());

  // report sanity stats
  galois::gInfo("# visited nodes is ", num_visited.reduce());
  galois::gInfo("Max distance is ", max_dist.reduce());
  galois::gInfo("Sum of visited distances is ", sum_dist.reduce());

  if (!skipVerify) {
    if (Impl::template Verify<SsspNodeDistance<Weight>, SsspEdgeWeight<Weight>>(
            &graph, source)) {
      std::cout << "Verification successful.\n";
    } else {
      GALOIS_DIE("verification failed");
    }
  }

  if (output) {
    std::vector<Weight> results = MakeResults<Weight>(graph);
    assert(results.size() == graph.size());

    writeOutput(outputLocation, results.data(), results.size());
  }
}

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
    std::cerr << "failed to set report: " << reportNode
              << " or failed to set source: " << startNode << "\n";
    abort();
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
    std::cerr << "Invalid algorithm\n";
    abort();
  }

  auto pg_result =
      Sssp(pfg.get(), startNode, edge_property_name, "distance", plan);
  if (!pg_result) {
    GALOIS_LOG_FATAL("Failed to run SSSP: {}", pg_result.error());
  }

  switch (pfg->EdgeProperty(edge_property_name)->type()->id()) {
  case arrow::UInt32Type::type_id:
    ProcessOutput<uint32_t>(pfg.get(), edge_property_name, "distance");
    break;
  case arrow::Int32Type::type_id:
    ProcessOutput<int32_t>(pfg.get(), edge_property_name, "distance");
    break;
  case arrow::UInt64Type::type_id:
    ProcessOutput<uint64_t>(pfg.get(), edge_property_name, "distance");
    break;
  case arrow::Int64Type::type_id:
    ProcessOutput<int64_t>(pfg.get(), edge_property_name, "distance");
    break;
  case arrow::FloatType::type_id:
    ProcessOutput<float>(pfg.get(), edge_property_name, "distance");
    break;
  case arrow::DoubleType::type_id:
    ProcessOutput<double>(pfg.get(), edge_property_name, "distance");
    break;
  default:
    abort();
  }

  totalTime.stop();

  return 0;
}
