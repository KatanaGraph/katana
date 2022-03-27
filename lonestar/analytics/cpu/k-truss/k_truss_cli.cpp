/*
 * This file belongs to the Galois project, a C++ library for exploiting
 * parallelism. The code is being released under the terms of the 3-Clause
 * BSD License (a copy is located in LICENSE.txt at the top-level directory).
 *
 * Copyright (C) 2019, The University of Texas at Austin. All rights reserved.
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

#include <katana/analytics/k_truss/k_truss.h>

#include "Lonestar/BoilerPlate.h"

using namespace katana::analytics;
namespace cll = llvm::cl;

static const char* name = "Maximal k-trusses";
static const char* desc =
    "Computes the maximal k-trusses for a given undirected graph";
static const char* url = "k_truss";

static cll::opt<std::string> inputFile(
    cll::Positional, cll::desc("<input file>"), cll::Required);
static cll::opt<unsigned int> kTrussNumber(
    "kTrussNumber", cll::desc("report kTrussNumber (default value 3)"),
    cll::init(3));

static cll::opt<std::string> outName(
    "o", cll::desc("output file for the edgelist of resulting truss"));

static cll::opt<KTrussPlan::Algorithm> algo(
    "algo", cll::desc("Choose an algorithm:"),
    cll::values(
        clEnumValN(
            KTrussPlan::kBspJacobi, "BspJacobi",
            "Bulk-synchronous parallel with separated edge removal"),
        clEnumValN(
            KTrussPlan::kBsp, "Bsp", "Bulk-synchronous parallel (default)"),
        clEnumValN(
            KTrussPlan::kBspCoreThenTruss, "BspCoreThenTruss",
            "Compute k-1 core and then k-truss")),
    cll::init(KTrussPlan::kBsp));

std::string
AlgorithmName(KTrussPlan::Algorithm algorithm) {
  switch (algorithm) {
  case KTrussPlan::kBsp:
    return "Bsp";
  case KTrussPlan::kBspJacobi:
    return "BspJacobi";
  case KTrussPlan::kBspCoreThenTruss:
    return "BspCoreThenTruss";
  default:
    return "Unknown";
  }
}

int
main(int argc, char** argv) {
  std::unique_ptr<katana::SharedMemSys> G =
      LonestarStart(argc, argv, name, desc, url, &inputFile);

  katana::StatTimer total_timer("TimerTotal");
  total_timer.start();

  if (!symmetricGraph) {
    KATANA_LOG_FATAL(
        "This application requires a symmetric graph input;"
        " please use the -symmetricGraph flag "
        " to indicate the input is a symmetric graph.");
  }

  if (kTrussNumber < 2) {
    KATANA_LOG_FATAL("kTrussNumber must be >= 2");
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

  std::cout << "Running " << AlgorithmName(algo) << "\n";

  std::unique_ptr<katana::PropertyGraph> pg_projected_view =
      ProjectPropertyGraphForArguments(pg);

  std::cout << "Projected graph has: "
            << pg_projected_view->topology().NumNodes() << " nodes, "
            << pg_projected_view->topology().NumEdges() << " edges\n";

  KTrussPlan plan = KTrussPlan();
  switch (algo) {
  case KTrussPlan::kBsp:
    plan = KTrussPlan::Bsp();
    break;
  case KTrussPlan::kBspJacobi:
    plan = KTrussPlan::BspJacobi();
    break;
  case KTrussPlan::kBspCoreThenTruss:
    plan = KTrussPlan::BspCoreThenTruss();
    break;
  default:
    KATANA_LOG_FATAL("Invalid algorithm");
  }

  katana::TxnContext txn_ctx;
  if (auto r = KTruss(
          &txn_ctx, pg_projected_view.get(), kTrussNumber, "edge-alive", plan);
      !r) {
    KATANA_LOG_FATAL("Failed to compute k-truss: {}", r.error());
  }

  auto stats_result = KTrussStatistics::Compute(
      pg_projected_view.get(), kTrussNumber, "edge-alive");
  if (!stats_result) {
    KATANA_LOG_FATAL(
        "Failed to compute KTruss statistics: {}", stats_result.error());
  }
  auto stats = stats_result.value();
  stats.Print();

  if (!skipVerify) {
    if (KTrussAssertValid(
            pg_projected_view.get(), kTrussNumber, "edge-alive")) {
      std::cout << "Verification successful.\n";
    } else {
      KATANA_LOG_FATAL("verification failed");
    }
  }

  if (output) {
    auto r = pg_projected_view->GetEdgePropertyTyped<uint32_t>("edge-alive");
    if (!r) {
      KATANA_LOG_FATAL("Failed to get edge property {}", r.error());
    }
    auto results = r.value();
    KATANA_LOG_DEBUG_ASSERT(
        uint64_t(results->length()) ==
        pg_projected_view->topology().NumNodes());

    writeOutput(outputLocation, results->raw_values(), results->length());
  }

  total_timer.stop();

  return 0;
}
