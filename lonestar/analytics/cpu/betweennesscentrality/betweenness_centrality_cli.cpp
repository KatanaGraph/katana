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
#include "katana/analytics/betweenness_centrality/betweenness_centrality.h"

using namespace katana::analytics;

namespace cll = llvm::cl;

static cll::opt<std::string> inputFile(
    cll::Positional, cll::desc("<input file>"), cll::Required);

static cll::opt<std::string> sourcesToUse(
    "sourcesToUse",
    cll::desc("Whitespace separated list of sources in a file to use in BC"),
    cll::init(""));

static cll::opt<unsigned int> numOfSources(
    "numOfSources",
    cll::desc("Number of sources to compute BC on (default all)"),
    cll::init(-1u));

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

////////////////////////////////////////////////////////////////////////////////

static const char* name = "Betweenness Centrality";
static const char* desc =
    "Computes betweenness centrality in an unweighted graph";

////////////////////////////////////////////////////////////////////////////////

int
main(int argc, char** argv) {
  std::unique_ptr<katana::SharedMemSys> G =
      LonestarStart(argc, argv, name, desc, nullptr, &inputFile);

  katana::StatTimer autoAlgoTimer("AutoAlgo_0");
  katana::StatTimer totalTime("TimerTotal");
  totalTime.start();

  std::cout << "Reading from file: " << inputFile << "\n";
  std::unique_ptr<katana::PropertyFileGraph> pfg =
      MakeFileGraph(inputFile, edge_property_name);

  BetweennessCentralityPlan plan =
      BetweennessCentralityPlan::FromAlgorithm(algo);

  BetweennessCentralitySources sources = kBetweennessCentralityAllNodes;

  if (sourcesToUse != "") {
    std::ifstream source_file(sourcesToUse);
    std::vector<uint32_t> t(
        std::istream_iterator<uint32_t>{source_file},
        std::istream_iterator<uint32_t>{});
    sources = t;
    source_file.close();
  }
  if (numOfSources != -1u) {
    if (sources == kBetweennessCentralityAllNodes) {
      sources = numOfSources;
    } else {
      KATANA_ASSERT(std::holds_alternative<std::vector<uint32_t>>(sources));
      auto& sources_vec = std::get<std::vector<uint32_t>>(sources);
      if (sources_vec.size() > numOfSources) {
        sources_vec.resize(numOfSources);
      }
    }
  }

  if (auto r = BetweennessCentrality(
          pfg.get(), "betweenness_centrality", sources, plan);
      !r) {
    KATANA_LOG_FATAL("Couldn't run algorithm: {}", r.error());
  }

  auto stats_result = BetweennessCentralityStatistics::Compute(
      pfg.get(), "betweenness_centrality");
  if (!stats_result) {
    KATANA_LOG_FATAL("Failed to compute statistics: {}", stats_result.error());
  }
  auto stats = stats_result.value();

  stats.Print();

  if (output) {
    auto results_result =
        pfg->NodePropertyTyped<float>("betweenness_centrality");
    if (!results_result) {
      KATANA_LOG_FATAL("Failed to get results: {}", results_result.error());
    }
    auto results = results_result.value();

    KATANA_ASSERT((uint64_t)results->length() == pfg->topology().num_nodes());

    writeOutput(outputLocation, results->raw_values(), results->length());
  }

  // TODO(gill): Enable once we have BetweennessCentralityAssertValid
  //  if (!skipVerify)
  //    ...

  totalTime.stop();
  return 0;
}
