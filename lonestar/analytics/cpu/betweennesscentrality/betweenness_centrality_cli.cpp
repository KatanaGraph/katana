/*
 * This file belongs to the Galois project, a C++ library for exploiting
 * parallelism. The code is being released under the terms of the 3-Clause BSD
 * License (a copy is located in LICENSE.txt at the top-level directory).
 *
 * Copyright (C) 2020, The University of Texas at Austin. All rights reserved.
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

#include <llvm/Support/CommandLine.h>

#include "Lonestar/BoilerPlate.h"
#include "Lonestar/Utils.h"
#include "katana/SharedMemSys.h"
#include "katana/ThreadPool.h"
#include "katana/analytics/betweenness_centrality/betweenness_centrality.h"

using namespace katana::analytics;

namespace cll = llvm::cl;

static cll::opt<std::string> inputFile(
    cll::Positional, cll::desc("<input file>"), cll::Required);

static cll::opt<std::string> startNodesFile(
    "startNodesFile",
    cll::desc("File containing whitespace separated list of source "
              "nodes for computing betweenness-centrality; "
              "if set, -startNodes is ignored"));
static cll::opt<std::string> startNodesString(
    "startNodes",
    cll::desc("String containing whitespace separated list of source nodes for "
              "computing betweenness centrality; ignore if "
              "-startNodesFile is used"),
    cll::init(""));
static cll::opt<unsigned int> numberOfSources(
    "numberOfSources",
    cll::desc("Number of sources to compute betweenness-centrality on (default "
              "1); pick first numberOfSources from -startNodesFile or "
              "-startNodes if used or pick sources 0 to numberOfSources - 1"),
    cll::init(1));
static cll::opt<bool> allSources(
    "allSources",
    cll::desc(
        "Flag to compute betweenness centrality on all the sources (default "
        "false); if set -startNodesFile and -startNodes are ignored"),
    cll::init(false));
static cll::opt<BetweennessCentralityPlan::Algorithm> algo(
    "algo", cll::desc("Choose an algorithm (default value AutoAlgo):"),
    cll::values(
        clEnumValN(
            BetweennessCentralityPlan::kLevel, "Level",
            "Level parallel algorithm"),
        // clEnumValN(BetweennessCentralityPlan::kAsynchronous, "Async", "Asynchronous"),
        clEnumValN(
            BetweennessCentralityPlan::kOuter, "Outer",
            "Outer parallel algorithm")
        // clEnumValN(BetweennessCentralityPlan::kAutoAlgo, "Auto", "Auto: choose among the algorithms automatically")
        ),
    cll::init(BetweennessCentralityPlan::kLevel));

static cll::opt<bool> thread_spin(
    "threadSpin",
    cll::desc("If enabled, threads busy-wait for work rather than use "
              "condition variable (default false)"),
    cll::init(false));

////////////////////////////////////////////////////////////////////////////////

static const char* name = "Betweenness Centrality";
static const char* desc =
    "Computes betweenness centrality in an unweighted graph";

////////////////////////////////////////////////////////////////////////////////

int
main(int argc, char** argv) {
  std::unique_ptr<katana::SharedMemSys> G =
      LonestarStart(argc, argv, name, desc, nullptr, &inputFile);

  if (thread_spin) {
    katana::GetThreadPool().burnPower(katana::getActiveThreads());
  }

  katana::StatTimer autoAlgoTimer("AutoAlgo_0");
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

  BetweennessCentralityPlan plan =
      BetweennessCentralityPlan::FromAlgorithm(algo);

  BetweennessCentralitySources sources = kBetweennessCentralityAllNodes;
  uint32_t num_sources = pg_projected_view->NumNodes();

  if (!allSources) {
    if (!startNodesFile.getValue().empty()) {
      std::ifstream file(startNodesFile);
      if (!file.good()) {
        KATANA_LOG_FATAL("failed to open file: {}", startNodesFile);
      }
      std::vector<uint32_t> startNodes;
      startNodes.insert(
          startNodes.end(), std::istream_iterator<uint32_t>{file},
          std::istream_iterator<uint32_t>{});
      sources = startNodes;
      num_sources = startNodes.size();
    } else if (!startNodesString.empty()) {
      std::istringstream str(startNodesString);
      std::vector<uint32_t> startNodes;
      startNodes.insert(
          startNodes.end(), std::istream_iterator<uint32_t>{str},
          std::istream_iterator<uint32_t>{});
      sources = startNodes;
      num_sources = startNodes.size();
    }

    if (sources == kBetweennessCentralityAllNodes) {
      sources = numberOfSources;
      num_sources = numberOfSources;
    } else {
      KATANA_LOG_ASSERT(std::holds_alternative<std::vector<uint32_t>>(sources));
      auto& sources_vec = std::get<std::vector<uint32_t>>(sources);
      if (sources_vec.size() > numberOfSources) {
        sources_vec.resize(numberOfSources);
      }
      num_sources = sources_vec.size();
    }
  } else {
    sources = num_sources;
  }

  std::cout << "Running betweenness-centrality on " << num_sources
            << " sources\n";
  katana::TxnContext txn_ctx;
  if (auto r = BetweennessCentrality(
          pg_projected_view.get(), "betweenness_centrality", &txn_ctx, sources,
          plan);
      !r) {
    KATANA_LOG_FATAL("Couldn't run algorithm: {}", r.error());
  }

  auto stats_result = BetweennessCentralityStatistics::Compute(
      pg_projected_view.get(), "betweenness_centrality");
  if (!stats_result) {
    KATANA_LOG_FATAL("Failed to compute statistics: {}", stats_result.error());
  }
  auto stats = stats_result.value();
  stats.Print();

  if (output) {
    auto results_result = pg_projected_view->GetNodePropertyTyped<float>(
        "betweenness_centrality");
    if (!results_result) {
      KATANA_LOG_FATAL("Failed to get results: {}", results_result.error());
    }
    auto results = results_result.value();

    KATANA_LOG_ASSERT(
        (uint64_t)results->length() ==
        pg_projected_view->topology().NumNodes());

    writeOutput(outputLocation, results->raw_values(), results->length());
  }

  // TODO(gill): Enable once we have BetweennessCentralityAssertValid
  //  if (!skipVerify)
  //    ...

  totalTime.stop();
  return 0;
}
