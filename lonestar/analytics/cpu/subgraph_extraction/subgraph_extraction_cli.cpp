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
#include "katana/analytics/subgraph_extraction/subgraph_extraction.h"

using namespace katana::analytics;

const char* name = "Subgraph Extraction";
const char* desc = "Constructs the subgraph topology from a given node set";

namespace cll = llvm::cl;

static cll::opt<std::string> inputFile(
    cll::Positional, cll::desc("<input file>"), cll::Required);
static cll::opt<std::string> nodesFile(
    "nodesFile",
    cll::desc("File containing whitespace separated list of node ids "
              "for extracting subgraph; "
              "if set, -nodes is ignored"));
static cll::opt<std::string> nodesString(
    "nodes",
    cll::desc("String containing whitespace separated list of nodes ids for "
              "extracting subgraph (default value "
              "''); ignore if "
              "-nodesFile is used"),
    cll::init(""));
static cll::opt<SubGraphExtractionPlan::Algorithm> algo(
    "algo", cll::desc("Choose an algorithm:"),
    cll::values(clEnumValN(
        SubGraphExtractionPlan::kNodeSet, "nodeSet",
        "Extract subgraph topology from node set")),
    cll::init(SubGraphExtractionPlan::kNodeSet));

int
main(int argc, char** argv) {
  std::unique_ptr<katana::SharedMemSys> G =
      LonestarStart(argc, argv, name, desc, nullptr, &inputFile);

  katana::StatTimer totalTime("TimerTotal");
  totalTime.start();

  std::cout << "Reading from file: " << inputFile << "\n";
  std::unique_ptr<katana::PropertyGraph> pg =
      MakeFileGraph(inputFile, edge_property_name);

  std::cout << "Read " << pg->topology().NumNodes() << " nodes, "
            << pg->topology().NumEdges() << " edges\n";

  std::vector<std::string> vec_node_types;
  if (node_types != "") {
    katana::analytics::SplitStringByComma(node_types, &vec_node_types);
  }

  std::vector<std::string> vec_edge_types;
  if (edge_types != "") {
    katana::analytics::SplitStringByComma(edge_types, &vec_edge_types);
  }

  auto pg_projected_view = katana::PropertyGraph::MakeProjectedGraph(
      *pg.get(), vec_node_types, vec_edge_types);

  std::cout << "Projected graph has: "
            << pg_projected_view->topology().NumNodes() << " nodes, "
            << pg_projected_view->topology().NumEdges() << " edges\n";
  SubGraphExtractionPlan plan;

  std::vector<uint32_t> node_vec;
  if (!nodesFile.getValue().empty()) {
    std::ifstream file(nodesFile);
    if (!file.good()) {
      KATANA_LOG_FATAL("failed to open file: {}", nodesFile);
    }
    node_vec.insert(
        node_vec.end(), std::istream_iterator<uint64_t>{file},
        std::istream_iterator<uint64_t>{});
  } else {
    std::cout << "nodes list arg = " << nodesString << std::endl;
    std::istringstream str(nodesString);
    node_vec.insert(
        node_vec.end(), std::istream_iterator<uint64_t>{str},
        std::istream_iterator<uint64_t>{});
  }
  uint64_t num_nodes = node_vec.size();
  std::cout << "Extracting subgraph with " << num_nodes << " num nodes\n";
  std::cout << "INFO: This is extracting the topology containing nodes from "
               "the user defined node set.\n";

  auto subgraph_result =
      SubGraphExtraction(pg_projected_view.get(), node_vec, plan);
  if (!subgraph_result) {
    KATANA_LOG_FATAL("Failed to run algorithm: {}", subgraph_result.error());
  }

  auto subgraph = std::move(subgraph_result.value());
  std::cout << "Number of nodes in subgraph: "
            << subgraph->topology().NumNodes() << "\n";
  std::cout << "Number of edges in subgraph: "
            << subgraph->topology().NumEdges() << "\n";

  totalTime.stop();

  return 0;
}
