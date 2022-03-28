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

#include "Lonestar/BoilerPlate.h"
#include "katana/analytics/pagerank/pagerank.h"

constexpr static const float kAlpha = 0.85;

static const char* name = "Page Rank";
static const char* url = nullptr;

const char* desc = "Computes page ranks a la Page and Brin.";

using namespace katana::analytics;
namespace cll = llvm::cl;

static cll::opt<std::string> inputFile(
    cll::Positional, cll::desc("<input file>"), cll::Required);
static cll::opt<float> tolerance(
    "tolerance", cll::desc("tolerance"), cll::init(1.0e-3));
static cll::opt<unsigned int> maxIterations(
    "maxIterations",
    cll::desc("Maximum iterations, applies round-based versions only"),
    cll::init(1000));

static cll::opt<PagerankPlan::Algorithm> algo(
    "algo", cll::desc("Choose an algorithm:"),
    cll::values(
        clEnumValN(
            PagerankPlan::kPullTopological, "PullTopological",
            "PullTopological"),
        clEnumValN(PagerankPlan::kPullResidual, "PullResidual", "PullResidual"),
        clEnumValN(PagerankPlan::kPushSynchronous, "PushSync", "PushSync"),
        clEnumValN(PagerankPlan::kPushAsynchronous, "PushAsync", "PushAsync")),
    cll::init(PagerankPlan::kPushAsynchronous));

int
main(int argc, char** argv) {
  std::unique_ptr<katana::SharedMemSys> G =
      LonestarStart(argc, argv, name, desc, url, &inputFile);

  katana::StatTimer totalTime("TimerTotal");
  totalTime.start();
  std::cout << "WARNING: pull style algorithms work on the transpose of the "
               "actual graph\n"
            << "WARNING: this program assumes that " << inputFile
            << " contains transposed representation\n\n"
            << "Reading graph: " << inputFile << "\n";

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

  PagerankPlan plan{kCPU, algo, tolerance, maxIterations, kAlpha};

  katana::TxnContext txn_ctx;
  if (auto r = Pagerank(pg_projected_view.get(), "rank", &txn_ctx, plan); !r) {
    KATANA_LOG_FATAL("Failed to run Pagerank {}", r.error());
  }

  auto stats_result =
      PagerankStatistics::Compute(pg_projected_view.get(), "rank");
  if (!stats_result) {
    KATANA_LOG_FATAL("Failed to compute stats {}", stats_result.error());
  }
  auto stats = stats_result.value();
  stats.Print();

  if (!skipVerify) {
    if (PagerankAssertValid(pg_projected_view.get(), "rank")) {
      std::cout << "Verification successful.\n";
    } else {
      KATANA_LOG_FATAL("verification failed");
    }
  }

  if (output) {
    auto r = pg_projected_view->GetNodePropertyTyped<float>("rank");
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
