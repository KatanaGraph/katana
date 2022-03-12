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
#include "katana/Mem.h"
#include "katana/analytics/connected_components/connected_components.h"

using namespace katana::analytics;

namespace cll = llvm::cl;

const char* name = "Connected Components";
const char* desc = "Computes the connected components of a graph";
static const char* url = "connected_components";

static cll::opt<std::string> inputFile(
    cll::Positional, cll::desc("<input file>"), cll::Required);
static cll::opt<unsigned int> reportNode(
    "reportNode", cll::desc("Node to report distance to(default value 1)"),
    cll::init(1));

static cll::opt<ConnectedComponentsPlan::Algorithm> algo(
    "algo", cll::desc("Choose an algorithm (default value Afforest):"),
    cll::values(
        clEnumValN(
            ConnectedComponentsPlan::kSerial, "Serial", "Serial algorithm"),
        clEnumValN(
            ConnectedComponentsPlan::kLabelProp, "LabelProp",
            "Label propagation algorithms"),
        clEnumValN(
            ConnectedComponentsPlan::kSynchronous, "Synchronous",
            "Synchronous algorithm"),
        clEnumValN(
            ConnectedComponentsPlan::kAsynchronous, "Asynchronous",
            "Asynchronous algorithm"),
        clEnumValN(
            ConnectedComponentsPlan::kEdgeAsynchronous, "EdgeAsynchronous",
            "Edge asynchronous algorithm"),
        clEnumValN(
            ConnectedComponentsPlan::kEdgeTiledAsynchronous,
            "EdgeTiledAsynchronous", "Edge tiled asynchronous algorithm"),
        clEnumValN(
            ConnectedComponentsPlan::kBlockedAsynchronous,
            "BlockedASynchronous", "Blocked asynchronous algorithm"),
        clEnumValN(
            ConnectedComponentsPlan::kAfforest, "Afforest",
            "Afforest sampling algorithm"),
        clEnumValN(
            ConnectedComponentsPlan::kEdgeAfforest, "EdgeAfforest",
            "Afforest (edge-wise) sampling algorithm"),
        clEnumValN(
            ConnectedComponentsPlan::kEdgeTiledAfforest, "EdgeTiledAfforest",
            "Afforest (tiled edge-wise) sampling algorithm")),
    cll::init(ConnectedComponentsPlan::kAfforest));

static cll::opt<uint32_t> edgeTileSize(
    "edgeTileSize",
    cll::desc("(For Edgetiled algos) Size of edge tiles "
              "(default 512)"),
    cll::init(512));
//! parameter for the Vertex Neighbor Sampling step of Afforest algorithm
static cll::opt<uint32_t> neighborSampleSize(
    "neighborSampleSize",
    cll::desc("(For Afforest and its variants) number of edges "
              "per vertice to process initially for exposing "
              "partial connectivity (default 2)"),
    cll::init(2));
//! parameter for the Large Component Skipping step of Afforest algorithm
static cll::opt<uint32_t> componentSampleFrequency(
    "componentSampleFrequency",
    cll::desc("(For Afforest and its variants) number of times "
              "randomly sampling over vertices to approximately "
              "capture the largest intermediate component "
              "(default 1024)"),
    cll::init(1024));

std::string
AlgorithmName(ConnectedComponentsPlan::Algorithm algorithm) {
  switch (algorithm) {
  case ConnectedComponentsPlan::kSerial:
    return "Serial";
  case ConnectedComponentsPlan::kLabelProp:
    return "LabelProp";
  case ConnectedComponentsPlan::kSynchronous:
    return "Synchronous";
  case ConnectedComponentsPlan::kAsynchronous:
    return "Asynchronous";
  case ConnectedComponentsPlan::kEdgeAsynchronous:
    return "EdgeAsynchronous";
  case ConnectedComponentsPlan::kEdgeTiledAsynchronous:
    return "EdgeTiledAsynchronous";
  case ConnectedComponentsPlan::kBlockedAsynchronous:
    return "BlockedAsynchronous";
  case ConnectedComponentsPlan::kAfforest:
    return "Afforest";
  case ConnectedComponentsPlan::kEdgeAfforest:
    return "EdgeAfforest";
  case ConnectedComponentsPlan::kEdgeTiledAfforest:
    return "EdgeTiledAfforest";
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

  std::unique_ptr<katana::PropertyGraph> pg_projected_view =
      ProjectPropertyGraphForArguments(pg);

  std::cout << "Projected graph has: "
            << pg_projected_view->topology().NumNodes() << " nodes, "
            << pg_projected_view->topology().NumEdges() << " edges\n";

  if (reportNode >= pg_projected_view->topology().NumNodes()) {
    std::cerr << "failed to set report: " << reportNode << "\n";
    abort();
  }

  std::cout << "Running " << AlgorithmName(algo) << " algorithm\n";

  ConnectedComponentsPlan plan = ConnectedComponentsPlan();
  switch (algo) {
  case ConnectedComponentsPlan::kSerial:
    plan = ConnectedComponentsPlan::Serial();
    break;
  case ConnectedComponentsPlan::kLabelProp:
    plan = ConnectedComponentsPlan::LabelProp();
    break;
  case ConnectedComponentsPlan::kSynchronous:
    plan = ConnectedComponentsPlan::Synchronous();
    break;
  case ConnectedComponentsPlan::kAsynchronous:
    plan = ConnectedComponentsPlan::Asynchronous();
    break;
  case ConnectedComponentsPlan::kEdgeAsynchronous:
    plan = ConnectedComponentsPlan::EdgeAsynchronous();
    break;
  case ConnectedComponentsPlan::kEdgeTiledAsynchronous:
    katana::gInfo("INFO: Using edge tile size: ", edgeTileSize);
    katana::gInfo("WARNING: Performance may vary due to parameter");
    plan = ConnectedComponentsPlan::EdgeTiledAsynchronous(edgeTileSize);
    break;
  case ConnectedComponentsPlan::kBlockedAsynchronous:
    plan = ConnectedComponentsPlan::BlockedAsynchronous();
    break;
  case ConnectedComponentsPlan::kAfforest:
    katana::gInfo(
        "INFO: Using neighbor sample size: ", neighborSampleSize,
        " component sample frequency: ", componentSampleFrequency);
    katana::gInfo("WARNING: Performance may vary due to the parameters");
    plan = ConnectedComponentsPlan::Afforest(
        neighborSampleSize, componentSampleFrequency);
    break;
  case ConnectedComponentsPlan::kEdgeAfforest:
    katana::gInfo(
        "INFO: Using neighbor sample size: ", neighborSampleSize,
        " component sample frequency: ", componentSampleFrequency);
    katana::gInfo("WARNING: Performance may vary due to the parameters");
    plan = ConnectedComponentsPlan::EdgeAfforest(
        neighborSampleSize, componentSampleFrequency);
    break;
  case ConnectedComponentsPlan::kEdgeTiledAfforest:
    katana::gInfo(
        "INFO: Using edge tile size: ", edgeTileSize,
        " neighbor sample size: ", neighborSampleSize,
        " component sample frequency: ", componentSampleFrequency);
    katana::gInfo("WARNING: Performance may vary due to the parameters");
    plan = ConnectedComponentsPlan::EdgeTiledAfforest(
        neighborSampleSize, componentSampleFrequency);
    break;
  default:
    std::cerr << "Invalid algorithm\n";
    abort();
  }

  katana::TxnContext txn_ctx;
  auto pg_result = ConnectedComponents(
      pg_projected_view.get(), "component", &txn_ctx, symmetricGraph, plan);
  if (!pg_result) {
    KATANA_LOG_FATAL(
        "Failed to run ConnectedComponents: {}", pg_result.error());
  }

  auto stats_result = ConnectedComponentsStatistics::Compute(
      pg_projected_view.get(), "component");
  if (!stats_result) {
    KATANA_LOG_FATAL(
        "Failed to compute ConnectedComponents statistics: {}",
        stats_result.error());
  }
  auto stats = stats_result.value();
  stats.Print();

  if (!skipVerify) {
    if (ConnectedComponentsAssertValid(pg_projected_view.get(), "component")) {
      std::cout << "Verification successful.\n";
    } else {
      KATANA_LOG_FATAL("verification failed");
    }
  }

  if (output) {
    auto r = pg_projected_view->GetNodePropertyTyped<uint64_t>("component");
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
