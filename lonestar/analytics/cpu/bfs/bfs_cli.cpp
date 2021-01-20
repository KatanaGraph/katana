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
#include "katana/analytics/bfs/bfs_internal.h"

using namespace katana::analytics;

namespace cll = llvm::cl;

static const char* name = "Breadth-first Search";

static const char* desc =
    "Computes the shortest path from a source node to all nodes in a directed "
    "graph using a modified Bellman-Ford algorithm";

static const char* url = "breadth_first_search";

static cll::opt<std::string> inputFile(
    cll::Positional, cll::desc("<input file>"), cll::Required);
static cll::opt<unsigned int> startNode(
    "startNode", cll::desc("Node to start search from (default value 0)"),
    cll::init(0));
static cll::opt<unsigned int> reportNode(
    "reportNode", cll::desc("Node to report distance to (default value 1)"),
    cll::init(1));

static cll::opt<BfsPlan::Algorithm> algo(
    "algo", cll::desc("Choose an algorithm (default value SyncTile):"),
    cll::values(
        clEnumValN(
            BfsPlan::kAsynchronousTile, "AsyncTile", "Asynchronous tiled"),
        clEnumValN(BfsPlan::kAsynchronous, "Async", "Asynchronous"),
        clEnumValN(BfsPlan::kSynchronousTile, "SyncTile", "Synchronous tiled"),
        clEnumValN(BfsPlan::kSynchronous, "Sync", "Synchronous")),
    cll::init(BfsPlan::kSynchronousTile));

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

  std::cout << "Reading from file: " << inputFile << "\n";
  std::unique_ptr<katana::PropertyFileGraph> pfg =
      MakeFileGraph(inputFile, edge_property_name);

  std::cout << "Read " << pfg->topology().num_nodes() << " nodes, "
            << pfg->topology().num_edges() << " edges\n";

  std::cout << "Running " << AlgorithmName(algo) << "\n";

  if (startNode >= pfg->topology().num_nodes() ||
      reportNode >= pfg->topology().num_nodes()) {
    std::cerr << "failed to set report: " << reportNode
              << " or failed to set source: " << startNode << "\n";
    abort();
  }

  katana::reportPageAlloc("MeminfoPre");

  if (auto r = Bfs(pfg.get(), startNode, "level", BfsPlan::FromAlgorithm(algo));
      !r) {
    KATANA_LOG_FATAL("Failed to run bfs {}", r.error());
  }

  auto pg_result = BfsImplementation::Graph::Make(pfg.get(), {"level"}, {});
  if (!pg_result) {
    KATANA_LOG_FATAL("Failed to create graph {}", pg_result.error());
  }

  BfsImplementation::Graph graph = pg_result.value();

  auto it = graph.begin();
  std::advance(it, reportNode.getValue());
  BfsImplementation::Graph::Node report = *it;

  katana::reportPageAlloc("MeminfoPost");

  std::cout << "Node " << reportNode << " has distance "
            << graph.GetData<BfsNodeDistance>(report) << "\n";

  auto stats_result = BfsStatistics::Compute(pfg.get(), "level");
  if (!stats_result) {
    KATANA_LOG_FATAL("Failed to compute stats {}", stats_result.error());
  }
  auto stats = stats_result.value();
  stats.Print();

  if (!skipVerify) {
    if (stats.n_reached_nodes < graph.num_nodes()) {
      std::cerr << (graph.num_nodes() - stats.n_reached_nodes)
                << " unvisited nodes; this is an error if the graph is "
                   "strongly connected\n";
    }
    if (BfsAssertValid(pfg.get(), "level")) {
      std::cout << "Verification successful.\n";
    } else {
      KATANA_DIE("verification failed");
    }
  }

  if (output) {
    auto r = pfg->NodePropertyTyped<uint32_t>("level");
    if (!r) {
      KATANA_LOG_FATAL("Failed to get node property {}", r.error());
    }
    auto results = r.value();
    KATANA_LOG_DEBUG_ASSERT(uint64_t(results->length()) == graph.size());

    writeOutput(outputLocation, results->raw_values(), results->length());
  }

  totalTime.stop();

  return 0;
}
