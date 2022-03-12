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
#include "katana/analytics/random_walks/random_walks.h"

using namespace katana::analytics;

namespace cll = llvm::cl;

const char* name = "RandomWalks";
const char* desc = "Find paths by random walks on the graph";
static const char* url = "random_walks";

static cll::opt<std::string> inputFile(
    cll::Positional, cll::desc("<input file>"), cll::Required);

static cll::opt<std::string> outputFile(
    "outputFile", cll::desc("File name to output walks (Default: walks.txt)"),
    cll::init("walks.txt"));

static cll::opt<RandomWalksPlan::Algorithm> algo(
    "algo", cll::desc("Choose an algorithm (default value Node2Vec):"),
    cll::values(
        clEnumValN(
            RandomWalksPlan::kNode2Vec, "Node2Vec", "Node2Vec algorithm"),
        clEnumValN(
            RandomWalksPlan::kEdge2Vec, "Edge2Vec", "Edge2Vec algorithm")),
    cll::init(RandomWalksPlan::kNode2Vec));

static cll::opt<uint32_t> maxIterations(
    "maxIterations", cll::desc("Number of iterations for Edge2vec algorithm"),
    cll::init(10));

static cll::opt<uint32_t> walkLength(
    "walkLength", cll::desc("Length of random walks (Default: 10)"),
    cll::init(10));

static cll::opt<double> backwardProbability(
    "backwardProbability", cll::desc("Probability of moving back to parent"),
    cll::init(1.0));

static cll::opt<double> forwardProbability(
    "forwardProbability", cll::desc("Probability of moving forward (2-hops)"),
    cll::init(1.0));

static cll::opt<double> numberOfWalks(
    "numberOfWalks", cll::desc("Number of walks per node"), cll::init(1));

static cll::opt<uint32_t> numberOfEdgeTypes(
    "numberOfEdgeTypes", cll::desc("Number of edge types (only for Edge2Vec)"),
    cll::init(1));

std::string
AlgorithmName(RandomWalksPlan::Algorithm algorithm) {
  switch (algorithm) {
  case RandomWalksPlan::kNode2Vec:
    return "Node2Vec";
  case RandomWalksPlan::kEdge2Vec:
    return "Edge2Vec";
  default:
    return "Unknown";
  }
}

void
PrintWalks(
    const std::vector<std::vector<uint32_t>>& walks,
    const std::string& output_file) {
  std::ofstream f(output_file);

  for (auto walk : walks) {
    for (auto node : walk) {
      f << node << " ";
    }
    f << std::endl;
  }
}

int
main(int argc, char** argv) {
  std::unique_ptr<katana::SharedMemSys> G =
      LonestarStart(argc, argv, name, desc, url, &inputFile);

  katana::StatTimer totalTime("TimerTotal");
  totalTime.start();

  if (!symmetricGraph) {
    KATANA_LOG_FATAL(
        "This application requires a symmetric graph input;"
        " please use the -symmetricGraph flag "
        " to indicate the input is a symmetric graph.");
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

  std::cout << "Running " << AlgorithmName(algo) << " algorithm\n";

  RandomWalksPlan plan = RandomWalksPlan();
  switch (algo) {
  case RandomWalksPlan::kNode2Vec:
    plan = RandomWalksPlan::Node2Vec(
        walkLength, numberOfWalks, backwardProbability, forwardProbability);
    break;
  case RandomWalksPlan::kEdge2Vec:
    plan = RandomWalksPlan::Edge2Vec(
        walkLength, numberOfWalks, backwardProbability, forwardProbability,
        maxIterations, numberOfEdgeTypes);
    break;
  default:
    KATANA_LOG_FATAL("Invalid algorithm");
  }

  auto walks_result = RandomWalks(pg.get(), plan);
  if (!walks_result) {
    KATANA_LOG_FATAL("Failed to run RandomWalks: {}", walks_result.error());
  }

  if (output) {
    std::string output_file = outputLocation + "/" + outputFile;
    katana::gInfo("Writing random walks to a file: ", output_file);
    PrintWalks(walks_result.value(), output_file);
  }

  return 0;
}
