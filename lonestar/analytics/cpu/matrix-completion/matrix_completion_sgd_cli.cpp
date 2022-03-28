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

#include "Lonestar/BoilerPlate.h"
#include "katana/AtomicHelpers.h"
#include "katana/AtomicWrapper.h"
#include "katana/Bag.h"
#include "katana/Galois.h"
#include "katana/Reduction.h"
#include "katana/Timer.h"
#include "katana/TypedPropertyGraph.h"
#include "katana/analytics/matrix_completion/matrix_completion.h"

using namespace katana::analytics;

/**
 * Common commandline parameters to for matrix completion algorithms
 */
namespace cll = llvm::cl;
static cll::opt<std::string> inputFile(
    cll::Positional, cll::desc("<input file>"), cll::Required);

// (Purdue, Neflix): 0.012, (Purdue, Yahoo Music): 0.00075, (Purdue, HugeWiki):
// 0.001 Intel: 0.001 Bottou: 0.1
static cll::opt<double> learningRate(
    "learningRate",
    cll::desc("learning rate parameter [alpha] "
              "for Bold, Bottou, Intel and "
              "Purdue step size function"),
    cll::init(MatrixCompletionPlan::kDefaultLearningRate));

// (Purdue, Netflix): 0.015, (Purdue, Yahoo Music): 0.01,
// (Purdue, HugeWiki): 0.0, Intel: 0.9
static cll::opt<double> decayRate(
    "decayRate",
    cll::desc("decay rate parameter [beta] for "
              "Intel and Purdue step size "
              "function"),
    cll::init(MatrixCompletionPlan::kDefaultDecayRate));

// (Purdue, Netflix): 0.05, (Purdue, Yahoo Music): 1.0, (Purdue, HugeWiki): 0.01
// Intel: 0.001
static cll::opt<double> lambda(
    "lambda", cll::desc("regularization parameter [lambda]"),
    cll::init(MatrixCompletionPlan::kDefaultLambda));

static cll::opt<double> tolerance(
    "tolerance", cll::desc("convergence tolerance"),
    cll::init(MatrixCompletionPlan::kDefaultTolerance));

static cll::opt<bool> useSameLatentVector(
    "useSameLatentVector",
    cll::desc("initialize all nodes to "
              "use same latent vector"),
    cll::init(MatrixCompletionPlan::kDefaultUseSameLatentVector));

// Regarding algorithm termination
static cll::opt<uint32_t> maxUpdates(
    "maxUpdates",
    cll::desc("Max number of times to update "
              "latent vectors (default 100)"),
    cll::init(MatrixCompletionPlan::kDefaultMaxUpdates));

static cll::opt<uint32_t> updatesPerEdge(
    "updatesPerEdge", cll::desc("number of updates per edge"),
    cll::init(MatrixCompletionPlan::kDefaultUpdatesPerEdge));

static cll::opt<uint32_t> fixedRounds(
    "fixedRounds", cll::desc("run for a fixed number of rounds"),
    cll::init(MatrixCompletionPlan::kDefaultFixedRounds));

static cll::opt<bool> useExactError(
    "useExactError",
    cll::desc("use exact error for testing "
              "convergence"),
    cll::init(MatrixCompletionPlan::kDefaultUseExactError));

static cll::opt<bool> useDetInit(
    "useDetInit",
    cll::desc("initialize all nodes to "
              "use deterministic values for latent vector"),
    cll::init(MatrixCompletionPlan::kDefaultUseDetInit));

static cll::opt<MatrixCompletionPlan::Algorithm> algo(
    "algo", cll::desc("Choose an algorithm:"),
    cll::values(clEnumValN(
        MatrixCompletionPlan::kSGDByItems, "sgdByItems",
        "Simple SGD on Items")),
    cll::init(MatrixCompletionPlan::kSGDByItems));
/*
 * Commandline options for different learning functions
 */
static cll::opt<MatrixCompletionPlan::Step> learningRateFunction(
    "learningRateFunction", cll::desc("Choose learning rate function:"),
    cll::values(
        clEnumValN(MatrixCompletionPlan::kIntel, "intel", "Intel"),
        clEnumValN(MatrixCompletionPlan::kPurdue, "purdue", "Purdue"),
        clEnumValN(MatrixCompletionPlan::kBottou, "bottou", "Bottou"),
        clEnumValN(MatrixCompletionPlan::kBold, "bold", "Bold (default)"),
        clEnumValN(MatrixCompletionPlan::kInverse, "inverse", "Inverse")),
    cll::init(MatrixCompletionPlan::kDefaultLearningRateFunction));

const char* name = "Matrix Completion";
const char* desc = "Matrix Completion by SGD";
const char* url = "matrix_completion";

#define LATENT_VECTOR_SIZE 20

int
main(int argc, char** argv) {
  std::unique_ptr<katana::SharedMemSys> G =
      LonestarStart(argc, argv, name, desc, url, &inputFile);

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

  std::cout << "Read " << pg->NumNodes() << " nodes, " << pg->NumEdges()
            << " edges\n";

  MatrixCompletionPlan plan = MatrixCompletionPlan();

  switch (algo) {
  case MatrixCompletionPlan::kSGDByItems:
    plan = MatrixCompletionPlan::SGDByItems(
        learningRate, decayRate, lambda, tolerance, useSameLatentVector,
        maxUpdates, updatesPerEdge, fixedRounds, useExactError, useDetInit,
        learningRateFunction);
    break;
  default:
    KATANA_LOG_FATAL("invalid algorithm");
  }

  katana::TxnContext txn_ctx;
  if (auto r = MatrixCompletion(pg.get(), &txn_ctx, plan); !r) {
    KATANA_LOG_FATAL("Failed to run algorithm: {}", r.error());
  }

  totalTime.stop();

  return 0;
}
