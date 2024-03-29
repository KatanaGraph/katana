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
#include "katana/LCGraph.h"
#include "katana/Reduction.h"
#include "katana/SharedMemSys.h"
#include "katana/Statistics.h"
#include "katana/Timer.h"

static const char* name = "Count levels";
static const char* desc = "Computes the number of degree levels";

#define DEBUG false

namespace cll = llvm::cl;

static cll::opt<std::string> inputFile(
    cll::Positional, cll::desc("<input graph>"), cll::Required);
static cll::opt<unsigned int> startNode(
    "startNode", cll::desc("Node to start search from"), cll::init(0));

enum COLOR { WHITE, GRAY, BLACK };

struct LNode {
  uint32_t dist;
  COLOR color;
};

using Graph = katana::LC_CSR_Graph<LNode, void>::with_numa_alloc<
    true>::type ::with_no_lockable<true>::type;
using GNode = Graph::GraphNode;

static const unsigned int DIST_INFINITY =
    std::numeric_limits<unsigned int>::max();

const katana::gstl::Vector<size_t>&
countLevels(Graph& graph) {
  using Vec = katana::gstl::Vector<size_t>;

  //! [Define GReducible]
  auto merge = [](Vec& lhs, Vec&& rhs) -> Vec& {
    Vec v(std::move(rhs));
    if (lhs.size() < v.size()) {
      lhs.resize(v.size());
    }
    auto ll = lhs.begin();
    for (auto ii = v.begin(), ei = v.end(); ii != ei; ++ii, ++ll) {
      *ll += *ii;
    }
    return lhs;
  };

  auto identity = []() -> Vec { return Vec(); };

  auto r = katana::make_reducible(merge, identity);

  katana::do_all(katana::iterate(graph), [&](GNode n) {
    LNode srcdata = graph.getData(n);
    if (srcdata.dist == DIST_INFINITY) {
      return;
    }

    auto& vec = r.getLocal();
    if (vec.size() <= srcdata.dist) {
      vec.resize(srcdata.dist + 1);
    }
    vec[srcdata.dist] += 1;
  });

  return r.reduce();
  //! [Define GReducible]
}

void
bfsSerial(Graph& graph, GNode source) {
  constexpr katana::MethodFlag flag = katana::MethodFlag::UNPROTECTED;

  LNode& sdata = graph.getData(source, flag);
  sdata.dist = 0u;
  sdata.color = GRAY;

  std::queue<GNode> queue;
  queue.push(source);

  while (!queue.empty()) {
    GNode curr = queue.front();
    sdata = graph.getData(curr, flag);
    queue.pop();

    // iterate over edges from node n
    for (auto e : graph.edges(curr)) {
      GNode dst = graph.getEdgeDst(e);
      LNode& ddata = graph.getData(dst);

      if (ddata.color == WHITE) {
        ddata.color = GRAY;
        ddata.dist = sdata.dist + 1;
        queue.push(dst);
      }
    }
    sdata.color = BLACK;
  }  // end while
}

int
main(int argc, char** argv) {
  std::unique_ptr<katana::SharedMemSys> G =
      LonestarStart(argc, argv, name, desc, nullptr, &inputFile);

  katana::StatTimer OT("OverheadTime");
  OT.start();

  Graph graph;
  katana::readGraph(graph, inputFile);
  std::cout << "Read " << graph.size() << " nodes, " << graph.sizeEdges()
            << " edges\n";

  katana::Prealloc(
      5, 2 * graph.size() * sizeof(typename Graph::node_data_type));
  katana::ReportPageAllocGuard page_alloc;

  katana::do_all(
      katana::iterate(graph),
      [&](const GNode& src) {
        LNode& sdata = graph.getData(src);
        sdata.color = WHITE;
        sdata.dist = DIST_INFINITY;
      },
      katana::no_stats());

  if (startNode >= graph.size()) {
    std::cerr << "Source node index " << startNode
              << " is greater than the graph size" << graph.size()
              << ", failed to set source: " << startNode << "\n";
    abort();
  }
  GNode source;
  auto it = graph.begin();
  std::advance(it, startNode.getValue());
  source = *it;

  katana::StatTimer T;
  T.start();
  bfsSerial(graph, source);
  const auto& counts = countLevels(graph);
  T.stop();

  page_alloc.Report();

#if DEBUG
  for (auto n : graph) {
    LNode& data = graph.getData(n);
    std::cout << "Node: " << n << " BFS dist:" << data.dist << std::endl;
  }
#endif

  std::cout << "Number of BFS levels: " << counts.size() << "\n";

  OT.stop();

  return EXIT_SUCCESS;
}
