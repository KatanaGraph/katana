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

#include <cmath>
#include <iostream>

#include <llvm/Support/CommandLine.h>

#include "Lonestar/BoilerPlate.h"
#include "katana/Bag.h"
#include "katana/Galois.h"
#include "katana/Reduction.h"
#include "katana/Timer.h"
#include "katana/analytics/independent_set/independent_set.h"

namespace {

using namespace katana::analytics;

const char* name = "Maximal Independent Set";
const char* desc =
    "Computes a maximal independent set (not maximum) of nodes in a graph";
const char* url = "independent_set";

namespace cll = llvm::cl;
cll::opt<std::string> inputFile(
    cll::Positional, cll::desc("<input file>"), cll::Required);

cll::opt<IndependentSetPlan::Algorithm> algo(
    "algo", cll::desc("Choose an algorithm:"),
    cll::values(
        clEnumValN(IndependentSetPlan::kSerial, "Serial", "Serial"),
        clEnumValN(
            IndependentSetPlan::kPull, "Pull",
            "Pull-based (node 0 is initially in the independent set)"),
        //        clEnumValN(
        //            IndependentSetPlan::kNondeterministic, "Nondeterministic",
        //            "Non-deterministic, use bulk synchronous worklist"),
        //        clEnumValN(
        //            IndependentSetPlan::kDeterministicBase, "DeterministicBase",
        //            "use deterministic worklist"),
        clEnumValN(
            IndependentSetPlan::kPriority, "Priority",
            "prio algo based on Martin's GPU ECL-MIS algorithm (default)"),
        clEnumValN(
            IndependentSetPlan::kEdgeTiledPriority, "EdgeTiledPriority",
            "edge-tiled prio algo based on Martin's GPU ECL-MIS algorithm")),
    cll::init(IndependentSetPlan::kPriority));

}  // namespace

int
main(int argc, char** argv) {
  std::unique_ptr<katana::SharedMemSys> G =
      LonestarStart(argc, argv, name, desc, url, &inputFile);

  katana::StatTimer totalTime("TimerTotal");
  totalTime.start();

  if (!symmetricGraph) {
    KATANA_DIE(
        "independent set requires a symmetric graph input;"
        " please use the -symmetricGraph flag "
        " to indicate the input is a symmetric graph");
  }

  std::cout << "Reading from file: " << inputFile << "\n";
  auto res = katana::URI::Make(inputFile);
  if (!res) {
    KATANA_LOG_FATAL("input file {} error: {}", inputFile, res.error());
  }
  auto inputURI = res.value();
  std::unique_ptr<katana::PropertyGraph> pg =
      MakeFileGraph(inputURI, edge_property_name);

  std::cout << "Read " << pg->NumNodes() << " nodes, " << pg->NumEdges()
            << " edges\n";

  IndependentSetPlan plan = IndependentSetPlan::FromAlgorithm(algo);

  katana::TxnContext txn_ctx;
  if (auto r = IndependentSet(pg.get(), "indicator", &txn_ctx, plan); !r) {
    KATANA_LOG_FATAL("Failed to run algorithm: {}", r.error());
  }

  auto stats_result = IndependentSetStatistics::Compute(pg.get(), "indicator");
  if (!stats_result) {
    KATANA_LOG_FATAL("Failed to compute statistics: {}", stats_result.error());
  }
  auto stats = stats_result.value();
  stats.Print();

  if (!skipVerify) {
    if (IndependentSetAssertValid(pg.get(), "indicator")) {
      std::cout << "Verification successful.\n";
    } else {
      KATANA_LOG_FATAL("verification failed");
    }
  }

  if (output) {
    auto r = pg->GetNodePropertyTyped<uint8_t>("indicator");
    if (!r) {
      KATANA_LOG_FATAL("Failed to get node property {}", r.error());
    }
    auto results = r.value();
    KATANA_LOG_DEBUG_ASSERT(uint64_t(results->length()) == pg->size());

    writeOutput(outputLocation, results->raw_values(), results->length());
  }

  totalTime.stop();

  return 0;
}
