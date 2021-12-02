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
#include "katana/analytics/cdlp/cdlp.h"

using namespace katana::analytics;

namespace cll = llvm::cl;

/// Limited number of iterations to limit the oscillation of the label
/// in Synchronous algorithm. We dont need to limit it in Asynchronous algorithm.
/// Set to 10 same as Graphalytics benchmark.
/// TODO (Yasin): duplicated from cdlp.cpp; needs to defnied in one place. maybe in cdlp.hpp.
const unsigned int kMaxIterations = 10;

const std::string property_name = "community";
const char* name = "CDLP";
const char* desc = "Detects the communities of a graph using label propagation";
static const char* url = "cdlp";

static cll::opt<std::string> inputFile(
    cll::Positional, cll::desc("<input file>"), cll::Required);

static cll::opt<size_t> maxIterations(
    "maxIterations", cll::desc("Maximum number of running iterations"),
    cll::init(kMaxIterations));

static cll::opt<CdlpPlan::Algorithm> algo(
    "algo", cll::desc("Choose an algorithm (default value Synchronous):"),
    cll::values(
        clEnumValN(
            CdlpPlan::kSynchronous, "Synchronous",
            "Synchronous algorithm")/*,

		/// TODO (Yasin): Asynchronous Algorithm will be implemented later after Synchronous
		/// is done for both shared and distributed versions.
		
        clEnumValN(
            CdlpPlan::kAsynchronous, "Asynchronous",
            "Asynchronous algorithm")*/),
    cll::init(CdlpPlan::kSynchronous));

std::string
AlgorithmName(CdlpPlan::Algorithm algorithm) {
  switch (algorithm) {
  case CdlpPlan::kSynchronous:
    return "Synchronous";
  /// TODO (Yasin): Asynchronous Algorithm will be implemented later after Synchronous
  /// is done for both shared and distributed versions.
  /*
  case CdlpPlan::kAsynchronous:
    return "Asynchronous";
  */
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
  std::unique_ptr<katana::PropertyGraph> pg =
      MakeFileGraph(inputFile, edge_property_name);

  std::cout << "Read " << pg->topology().num_nodes() << " nodes, "
            << pg->topology().num_edges() << " edges\n";

  std::cout << "Running " << AlgorithmName(algo) << " algorithm\n";

  CdlpPlan plan = CdlpPlan();
  switch (algo) {
  case CdlpPlan::kSynchronous:
    plan = CdlpPlan::Synchronous();
    break;
  /// TODO (Yasin): Asynchronous Algorithm will be implemented later after Synchronous
  /// is done for both shared and distributed versions.
  /*
  case CdlpPlan::kAsynchronous:
    plan = CdlpPlan::Asynchronous();
    break;
  */
  default:
    std::cerr << "Invalid algorithm\n";
    abort();
  }

  auto pg_result = Cdlp(pg.get(), property_name, maxIterations, plan);
  if (!pg_result) {
    KATANA_LOG_FATAL("Failed to run Cdlp: {}", pg_result.error());
  }

  auto stats_result = CdlpStatistics::Compute(pg.get(), property_name);
  if (!stats_result) {
    KATANA_LOG_FATAL(
        "Failed to compute Cdlp statistics: {}", stats_result.error());
  }
  auto stats = stats_result.value();
  stats.Print();

  if (output) {
    auto r = pg->GetNodePropertyTyped<uint64_t>(property_name);
    if (!r) {
      KATANA_LOG_FATAL("Failed to get node property {}", r.error());
    }
    auto results = r.value();
    KATANA_LOG_DEBUG_ASSERT(
        uint64_t(results->length()) == pg->topology().num_nodes());

    writeOutput(outputLocation, results->raw_values(), results->length());
  }

  totalTime.stop();

  return 0;
}
