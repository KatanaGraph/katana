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

#include <katana/analytics/bfs/bfs.h>

#include "Lonestar/BoilerPlate.h"

using namespace katana::analytics;

namespace cll = llvm::cl;

static const char* name = "Breadth-first Search";

static const char* desc =
    "Computes the shortest path from a source node to all nodes in a directed "
    "graph using a modified Bellman-Ford algorithm";

static const char* url = "breadth_first_search";

static cll::opt<std::string> inputFile(
    cll::Positional, cll::desc("<input file>"), cll::Required);

static cll::opt<std::string> startNodesFile(
    "startNodesFile",
    cll::desc("File containing whitespace separated list of source "
              "nodes for computing breadth-first search; "
              "if set, -startNodes is ignored"));
static cll::opt<std::string> startNodesString(
    "startNodes",
    cll::desc("String containing whitespace separated list of source nodes for "
              "computing breadth-first search (default value '0'); ignore if "
              "-startNodesFile is used"),
    cll::init("0"));

static cll::opt<unsigned int> reportNode(
    "reportNode", cll::desc("Node to report distance to (default value 1)"),
    cll::init(1));

static cll::opt<bool> persistAllDistances(
    "persistAllDistances",
    cll::desc(
        "Flag to indicate whether to persist the distances from all "
        "sources in startNodeFile or startNodesString; By default only the "
        "distances for the last source are persisted (default value false)"),
    cll::init(false));

static cll::opt<unsigned int> alpha(
    "alpha", cll::desc("Alpha for direction optimization (default value: 15)"),
    cll::init(15));

static cll::opt<unsigned int> beta(
    "beta", cll::desc("Beta for direction optimization (default value: 18)"),
    cll::init(18));

static cll::opt<bool> thread_spin(
    "threadSpin",
    cll::desc("If enabled, threads busy-wait for rather than use "
              "condition variable (default false)"),
    cll::init(false));

std::string
AlgorithmName(BfsPlan::Algorithm algorithm) {
  switch (algorithm) {
  case BfsPlan::kAsynchronousTile:
    return "AsyncTile";
  case BfsPlan::kAsynchronous:
    return "Async";
  case BfsPlan::kSynchronousTile:
    return "SyncTile";
  case BfsPlan::kSynchronous:
    return "Sync";
  case BfsPlan::kSynchronousDirectOpt:
    return "SyncDO";
  default:
    return "Unknown";
  }
}

int
main(int argc, char** argv) {
  std::unique_ptr<katana::SharedMemSys> G =
      LonestarStart(argc, argv, name, desc, url, &inputFile);

  if (thread_spin) {
    katana::GetThreadPool().burnPower(katana::getActiveThreads());
  }

  katana::StatTimer totalTime("TimerTotal");
  totalTime.start();

  BfsPlan::Algorithm algo = BfsPlan::kSynchronousDirectOpt;

  std::cout << "Reading from file: " << inputFile << "\n";
  std::unique_ptr<katana::PropertyGraph> pg =
      MakeFileGraph(inputFile, edge_property_name);

  std::cout << "Read " << pg->topology().num_nodes() << " nodes, "
            << pg->topology().num_edges() << " edges\n";

  std::cout << "Running " << AlgorithmName(algo) << "\n";

  if (reportNode >= pg->topology().num_nodes()) {
    KATANA_LOG_FATAL("failed to set report: {}", reportNode);
  }

  katana::reportPageAlloc("MeminfoPre");

  std::vector<uint32_t> startNodes;
  if (!startNodesFile.getValue().empty()) {
    std::ifstream file(startNodesFile);
    if (!file.good()) {
      KATANA_LOG_FATAL("failed to open file: {}", startNodesFile);
    }
    startNodes.insert(
        startNodes.end(), std::istream_iterator<uint32_t>{file},
        std::istream_iterator<uint32_t>{});
  } else {
    std::istringstream str(startNodesString);
    startNodes.insert(
        startNodes.end(), std::istream_iterator<uint32_t>{str},
        std::istream_iterator<uint32_t>{});
  }
  uint32_t num_sources = startNodes.size();
  std::cout << "Running BFS for " << num_sources << " sources\n";

  BfsPlan plan = BfsPlan::SynchronousDirectOpt(alpha, beta);

  for (auto start_node : startNodes) {
    if (start_node >= pg->topology().num_nodes()) {
      KATANA_LOG_FATAL("failed to set source: {}", start_node);
    }

    std::string node_distance_prop = "level-" + std::to_string(start_node);
    if (auto r = Bfs(pg.get(), start_node, node_distance_prop, plan); !r) {
      KATANA_LOG_FATAL("Failed to run bfs {}", r.error());
    }

    katana::reportPageAlloc("MeminfoPost");

    auto r = pg->GetNodePropertyTyped<uint32_t>(node_distance_prop);
    if (!r) {
      KATANA_LOG_FATAL("Failed to get node property {}", r.error());
    }
    auto results = r.value();

    std::cout << "Node " << reportNode << " has distance "
              << results->Value(reportNode) << "\n";

    auto stats_result = BfsStatistics::Compute(pg.get(), node_distance_prop);
    if (!stats_result) {
      KATANA_LOG_FATAL("Failed to compute stats {}", stats_result.error());
    }
    auto stats = stats_result.value();
    stats.Print();

    if (!skipVerify) {
      if (stats.n_reached_nodes < pg->num_nodes()) {
        KATANA_LOG_WARN(
            "{} unvisited nodes; this is an error if the graph is strongly "
            "connected",
            pg->num_nodes() - stats.n_reached_nodes);
      }
      if (BfsAssertValid(pg.get(), start_node, node_distance_prop)) {
        std::cout << "Verification successful.\n";
      } else {
        KATANA_LOG_FATAL("verification failed");
      }
    }

    if (output) {
      KATANA_LOG_DEBUG_ASSERT(uint64_t(results->length()) == pg->size());

      std::string output_filename = "output-" + std::to_string(start_node);
      writeOutput(
          outputLocation, results->raw_values(), results->length(),
          output_filename);
    }
    --num_sources;
    if (num_sources != 0 && !persistAllDistances) {
      if (auto r = pg->RemoveNodeProperty(node_distance_prop); !r) {
        KATANA_LOG_FATAL(
            "Failed to remove the node distance property stats {}", r.error());
      }
    }
  }

  totalTime.stop();

  return 0;
}
