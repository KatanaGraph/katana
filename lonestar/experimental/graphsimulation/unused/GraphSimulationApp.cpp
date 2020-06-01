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

#include "GraphSimulation.h"
#include "galois/Reduction.h"
#include "galois/Timer.h"
#include "galois/graphs/TypeTraits.h"
#include "llvm/Support/CommandLine.h"
#include "Lonestar/BoilerPlate.h"

namespace cll = llvm::cl;

static const char* name = "Graph Simulation";
static const char* desc =
    "Compute graph simulation for a pair of given query and data graphs";
static const char* url = "graph_simulation";

enum Simulation { graph, dual, strong };

static cll::opt<Simulation> simType(
    "simType", cll::desc("Type of simulation:"),
    cll::values(
        clEnumValN(Simulation::graph, "graphSim",
                   "keep node labeling + outgoing transitions (default)"),
        clEnumValN(Simulation::dual, "dualSim",
                   "graphSim + keep incoming transitions"),
        clEnumValN(Simulation::strong, "strongSim",
                   "dualSim + nodes matched within a ball of r = "
                   "diameter(query graph)"),
        clEnumValEnd),
    cll::init(Simulation::graph));

static cll::opt<std::string> queryGraph("q", cll::desc("<query graph>"),
                                        cll::Required);

static cll::opt<std::string> dataGraph("d", cll::desc("<data graph>"),
                                       cll::Required);

static cll::opt<std::string> outputFile("o", cll::desc("[match output]"));

template <typename G>
void initializeQueryGraph(G& g, uint32_t labelCount) {
  galois::do_all(
      galois::iterate(g),
      [&g, labelCount](typename Graph::GraphNode n) {
        auto& data   = g.getData(n);
        data.matched = 0; // matches to none
        data.label   = 0; // TODO random label
        uint32_t edgeid = 0;
        for (auto e : g.edges(n)) {
          auto& eData     = g.getEdgeData(e);
          eData.label     = edgeid % labelCount; // TODO: change to random
          eData.timestamp = 0; // TODO random timestamp
        }
      });
}

template <typename G>
void initializeDataGraph(G& g, uint32_t labelCount) {
  galois::do_all(
      galois::iterate(g.begin(), g.end()),
      [&g, labelCount](typename Graph::GraphNode n) {
        auto& data  = g.getData(n);
        data.label  = 0; // TODO: change to random
        uint32_t edgeid = 0;
        for (auto e : g.edges(n)) {
          auto& eData     = g.getEdgeData(e);
          eData.label     = edgeid % labelCount; // TODO: change to random
          eData.timestamp = 0; // TODO random
        }
      });
}

int main(int argc, char** argv) {
  galois::StatTimer T("TotalTime");
  T.start();

  galois::SharedMemSys G;
  LonestarStart(argc, argv, name, desc, url);

  Graph qG;
  galois::graphs::readGraph(qG, queryGraph);
  std::cout << "Read query graph of " << qG.size() << " nodes" << std::endl;
  initializeQueryGraph(qG, qG.size());

  Graph dG;
  galois::graphs::readGraph(dG, dataGraph);
  std::cout << "Read data graph of " << dG.size() << " nodes" << std::endl;
  initializeDataGraph(dG, qG.size());

  galois::StatTimer SimT("GraphSimulation");
  SimT.start();

  EventLimit dummy1;
  EventWindow dummy2;

  switch (simType) {
  case Simulation::graph:
    runGraphSimulationOld(qG, dG, dummy1, dummy2, false);
    break;
  case Simulation::dual:
    //    runDualSimulation();
    break;
  case Simulation::strong:
    //    runStrongSimulation();
    break;
  default:
    std::cerr << "Unknown algorithm!" << std::endl;
    abort();
  }

  SimT.stop();
  // This isn't included in this at all (it's in PythonGraph.h); additionally,
  // it takes attributed graphs, not LC_CSR Graphs
  //reportGraphSimulation(qG, dG, outputFile);

  T.stop();

  return 0;
}
