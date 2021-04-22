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
#include "katana/analytics/skip_gram/skip_gram.h"

using namespace katana::analytics;

namespace cll = llvm::cl;

const char* name = "Embeddings";
const char* desc = "Generate embeddings";

static cll::opt<std::string> inputFile(
    cll::Positional, cll::desc("<input file>"), cll::Required);

static cll::opt<std::string> outputFile(
    cll::Positional, cll::desc("<output file>"), cll::Required);

static cll::opt<uint32_t> embeddingSize(
    "embeddingSize",
    cll::desc("Size of the embedding vector (default value 100)"),
    cll::init(100));

static cll::opt<double> alpha(
    "alpha", cll::desc("alpha (default value 0.025)"), cll::init(0.025f));

static cll::opt<uint32_t> window(
    "window", cll::desc("window size (default value 5)"), cll::init(5));

static cll::opt<double> downSampleRate(
    "downSampleRate", cll::desc("down-sampling rate (default value 0.001)"),
    cll::init(0.001f));

static cll::opt<bool> hierarchicalSoftmax(
    "hierarchicalSoftmax",
    cll::desc("Enable/disable hierarchical softmax (default value false)"),
    cll::init(false));

static cll::opt<uint32_t> numNegSamples(
    "numNegSamples", cll::desc("Number of negative samples (default value 5)"),
    cll::init(5));

static cll::opt<uint32_t> numIterations(
    "numIterations",
    cll::desc("Number of Training Iterations (default value 5)"), cll::init(5));

static cll::opt<uint32_t> minimumFrequency(
    "minimumFrequency", cll::desc("Minimum Frequency (default 5)"),
    cll::init(5));

int
main(int argc, char** argv) {
  std::unique_ptr<katana::SharedMemSys> G =
      LonestarStart(argc, argv, name, desc, nullptr, &inputFile);

  katana::StatTimer totalTime("TimerTotal");
  totalTime.start();

  SkipGramPlan plan = SkipGramPlan();

  std::cout << "Reading from file: " << inputFile << "\n";

  auto embeddings_result = SkipGram(
      inputFile, plan, embeddingSize, alpha, window, downSampleRate,
      hierarchicalSoftmax, numNegSamples, numIterations, minimumFrequency);

  if (!embeddings_result) {
    KATANA_LOG_FATAL("failed to run algorithm: {}", embeddings_result.error());
  }
  auto embeddings = embeddings_result.value();

  std::ofstream of(outputFile.c_str());

  for (auto pair : embeddings) {
    of << pair.first;

    std::vector<double>& embedding = pair.second;
    for (auto val : embedding) {
      of << " " << val;
    }
    of << "\n";
  }

  of.close();

  totalTime.stop();

  return 0;
}
