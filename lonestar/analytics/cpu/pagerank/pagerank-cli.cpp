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

//! Flag that forces user to be aware that they should be passing in a
//! transposed graph.
static cll::opt<bool> transposedGraph(
    "transposedGraph", cll::desc("Specify that the input graph is transposed"),
    cll::init(false));

int
main(int argc, char** argv) {
  std::unique_ptr<katana::SharedMemSys> G =
      LonestarStart(argc, argv, name, desc, url, &inputFile);

  if ((algo == PagerankPlan::kPullResidual ||
       algo == PagerankPlan::kPullTopological) &&
      !transposedGraph) {
    KATANA_DIE(
        "This application requires a transposed graph input;"
        " please use the -transposedGraph flag "
        " to indicate the input is a transposed graph.");
  }

  katana::StatTimer totalTime("TimerTotal");
  totalTime.start();
  std::cout << "WARNING: pull style algorithms work on the transpose of the "
               "actual graph\n"
            << "WARNING: this program assumes that " << inputFile
            << " contains transposed representation\n\n"
            << "Reading graph: " << inputFile << "\n";

  std::cout << "Reading from file: " << inputFile << "\n";
  std::unique_ptr<katana::PropertyFileGraph> pfg =
      MakeFileGraph(inputFile, edge_property_name);

  std::cout << "Read " << pfg->topology().num_nodes() << " nodes, "
            << pfg->topology().num_edges() << " edges\n";

  PagerankPlan plan{kCPU, algo, tolerance, maxIterations, kAlpha};

  if (auto r = Pagerank(pfg.get(), "rank", plan); !r) {
    KATANA_LOG_FATAL("Failed to run Pagerank {}", r.error());
  }

  auto stats_result = PagerankStatistics::Compute(pfg.get(), "rank");
  if (!stats_result) {
    KATANA_LOG_FATAL("Failed to compute stats {}", stats_result.error());
  }
  auto stats = stats_result.value();
  stats.Print();

  if (!skipVerify) {
    if (PagerankAssertValid(pfg.get(), "rank")) {
      std::cout << "Verification successful.\n";
    } else {
      KATANA_LOG_FATAL("verification failed");
    }
  }

  if (output) {
    auto r = pfg->NodePropertyTyped<float>("rank");
    if (!r) {
      KATANA_LOG_FATAL("Failed to get node property {}", r.error());
    }
    auto results = r.value();
    assert(uint64_t(results->length()) == pfg->topology().num_nodes());

    writeOutput(outputLocation, results->raw_values(), results->length());
  }

  totalTime.stop();

  return 0;
}
