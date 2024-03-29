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

// This examples shows
// 1. how to pass a range for data-driven algorithms
// 2. how to add new work items using context
// 3. how to specify schedulers
// 4. how to write an indexer for OBIM
#include <iostream>
#include <string>

#include "katana/LCGraph.h"
#include "katana/SharedMemSys.h"
#include "katana/Timer.h"

using Graph = katana::LC_Linear_Graph<unsigned int, unsigned int>;
using GNode = Graph::GraphNode;
using UpdateRequest = std::pair<unsigned, GNode>;

static const unsigned int DIST_INFINITY =
    std::numeric_limits<unsigned int>::max();

constexpr unsigned int stepShift = 14;

int
main(int argc, char** argv) {
  katana::SharedMemSys G;
  katana::setActiveThreads(256);  // Galois will cap at hw max

  if (argc != 3) {
    std::cout << "Usage: " << argv[0]
              << " filename <dchunk16|obim|ParaMeter|det>\n";
    return 1;
  } else {
    std::cout << "Note: This is just a very simple example and provides no "
                 "useful information for performance\n";
  }

  Graph graph;
  katana::readGraph(graph,
                    argv[1]);  // argv[1] is the file name for graph

  // initialization
  katana::do_all(
      katana::iterate(graph),
      [&graph](GNode N) {
        graph.getData(N) = DIST_INFINITY;
      }  // operator as lambda expression
  );

  katana::StatTimer T;
  T.start();

  //! [SSSP push operator]
  // SSSP operator
  // auto& ctx expands to katana::UserContext<GNode>& ctx
  auto SSSP = [&](GNode active_node, auto& ctx) {
    // Get the value on the node
    auto srcData = graph.getData(active_node);

    // loop over neighbors to compute new value
    for (auto ii : graph.edges(active_node)) {  // cautious point
      auto dst = graph.getEdgeDst(ii);
      auto weight = graph.getEdgeData(ii);
      auto& dstData = graph.getData(dst);
      if (dstData > weight + srcData) {
        dstData = weight + srcData;
        ctx.push(dst);  // add new work items
      }
    }
  };
  //! [SSSP push operator]

  //! [Scheduler examples]
  // Priority Function in SSSPPushSimple
  // Map user-defined priority to a bucket number in OBIM
  auto reqIndexer = [&](const GNode& N) {
    return (graph.getData(N, katana::MethodFlag::UNPROTECTED) >> stepShift);
  };

  using namespace katana;
  using PSchunk = PerSocketChunkLIFO<16>;  // chunk size 16
  using OBIM = OrderedByIntegerMetric<decltype(reqIndexer), PSchunk>;
  //! [Scheduler examples]

  //! [Data-driven loops]
  std::string schedule = argv[2];  // argv[2] is the scheduler to be used

  // clear source
  graph.getData(*graph.begin()) = 0;

  if ("dchunk16" == schedule) {
    //! [chunk worklist]
    katana::for_each(
        katana::iterate(
            {*graph.begin()}),  // initial range using initializer list
        SSSP                    // operator
        ,
        katana::wl<PSchunk>()  // options. PSchunk expands to
                               // katana::PerSocketChunkLIFO<16>,
                               // where 16 is chunk size
        ,
        katana::loopname("sssp_dchunk16"));
    //! [chunk worklist]
  } else if ("obim" == schedule) {
    //! [OBIM]
    katana::for_each(
        katana::iterate(
            {*graph.begin()}),  // initial range using initializer list
        SSSP                    // operator
        ,
        katana::wl<OBIM>(reqIndexer)  // options. Pass an indexer instance for
                                      // OBIM construction.
        ,
        katana::loopname("sssp_obim"));
    //! [OBIM]
  }
  //! [Data-driven loops]

  else if ("ParaMeter" == schedule) {
    //! [ParaMeter loop iterator]
    katana::for_each(
        katana::iterate(
            {*graph.begin()}),  // initial range using initializer list
        SSSP                    // operator
        ,
        katana::wl<katana::ParaMeter<>>()  // options
        ,
        katana::loopname("sssp_ParaMeter"));
    //! [ParaMeter loop iterator]
  } else if ("det") {
    //! [Deterministic loop iterator]
    katana::for_each(
        katana::iterate(
            {*graph.begin()}),  // initial range using initializer list
        SSSP                    // operator
        ,
        katana::wl<katana::Deterministic<>>()  // options
        ,
        katana::loopname("sssp_deterministic"));
    //! [Deterministic loop iterator]
  } else {
    std::cerr << "Unknown schedule " << schedule << std::endl;
    return 1;
  }

  T.stop();
  return 0;
}
