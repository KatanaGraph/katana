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

#include <algorithm>
#include <iostream>
#include <vector>

#include <boost/iterator/transform_iterator.hpp>

#include "Lonestar/BoilerPlate.h"
#include "katana/Profile.h"
#include "katana/SharedMemSys.h"
#include "katana/TypedPropertyGraph.h"

const char* name = "Triangles";
const char* desc = "Counts the triangles in a graph";

constexpr static const unsigned CHUNK_SIZE = 64U;
enum Algo { nodeiterator, edgeiterator };

namespace cll = llvm::cl;

static cll::opt<std::string> inputFile(
    cll::Positional, cll::desc("<input file>"), cll::Required);
static cll::opt<Algo> algo(
    "algo", cll::desc("Choose an algorithm:"),
    cll::values(
        clEnumValN(Algo::nodeiterator, "nodeiterator", "Node Iterator"),
        clEnumValN(Algo::edgeiterator, "edgeiterator", "Edge Iterator")),
    cll::init(Algo::nodeiterator));

using NodeData = std::tuple<>;
using EdgeData = std::tuple<>;

typedef katana::TypedPropertyGraph<NodeData, EdgeData> Graph;
typedef typename Graph::Node GNode;

/**
 * Like std::lower_bound but doesn't dereference iterators. Returns the first
 * element for which comp is not true.
 */
template <typename Iterator, typename Compare>
Iterator
LowerBound(Iterator first, Iterator last, Compare comp) {
  using difference_type =
      typename std::iterator_traits<Iterator>::difference_type;

  Iterator it;
  difference_type count;
  difference_type half;

  count = std::distance(first, last);
  while (count > 0) {
    it = first;
    half = count / 2;
    std::advance(it, half);
    if (comp(it)) {
      first = ++it;
      count -= half + 1;
    } else {
      count = half;
    }
  }
  return first;
}

/**
 * std::set_intersection over edge_iterators.
 */
template <typename G>
size_t
CountEqual(
    const G& g, typename G::edge_iterator aa, typename G::edge_iterator ea,
    typename G::edge_iterator bb, typename G::edge_iterator eb) {
  size_t retval = 0;
  while (aa != ea && bb != eb) {
    typename G::Node a = g.OutEdgeDst(*aa);
    typename G::Node b = g.OutEdgeDst(*bb);
    if (a < b) {
      ++aa;
    } else if (b < a) {
      ++bb;
    } else {
      retval += 1;
      ++aa;
      ++bb;
    }
  }
  return retval;
}

template <typename G>
struct LessThan {
  const G& g;
  typename G::Node n;
  LessThan(const G& g, typename G::Node n) : g(g), n(n) {}
  bool operator()(typename G::edge_iterator it) {
    return g.OutEdgeDst(*it) < n;
  }
};

template <typename G>
struct GreaterThanOrEqual {
  const G& g;
  typename G::Node n;
  GreaterThanOrEqual(const G& g, typename G::Node n) : g(g), n(n) {}
  bool operator()(typename G::edge_iterator it) {
    return !(n < g.OutEdgeDst(*it));
  }
};

/**
 * Node Iterator algorithm for counting triangles.
 * <code>
 * for (v in G)
 *   for (all pairs of neighbors (a, b) of v)
 *     if ((a,b) in G and a < v < b)
 *       triangle += 1
 * </code>
 *
 * Thomas Schank. Algorithmic Aspects of Triangle-Based Network Analysis. PhD
 * Thesis. Universitat Karlsruhe. 2007.
 */
void
NodeIteratingAlgo(const Graph& graph) {
  katana::GAccumulator<size_t> numTriangles;

  //! [profile w/ vtune]
  katana::profileVtune(
      [&]() {
        katana::do_all(
            katana::iterate(graph),
            [&](const GNode& n) {
              // Partition neighbors
              // [first, ea) [n] [bb, last)
              Graph::edge_iterator first = graph.OutEdges(n).begin();
              Graph::edge_iterator last = graph.OutEdges(n).end();
              Graph::edge_iterator ea =
                  LowerBound(first, last, LessThan<Graph>(graph, n));
              Graph::edge_iterator bb =
                  LowerBound(first, last, GreaterThanOrEqual<Graph>(graph, n));

              for (; bb != last; ++bb) {
                GNode B = graph.OutEdgeDst(*bb);
                for (auto aa = first; aa != ea; ++aa) {
                  GNode A = graph.OutEdgeDst(*aa);
                  Graph::edge_iterator vv = graph.OutEdges(A).begin();
                  Graph::edge_iterator ev = graph.OutEdges(A).end();
                  Graph::edge_iterator it =
                      LowerBound(vv, ev, LessThan<Graph>(graph, B));
                  if (it != ev && graph.OutEdgeDst(*it) == B) {
                    numTriangles += 1;
                  }
                }
              }
            },
            katana::chunk_size<CHUNK_SIZE>(), katana::steal(),
            katana::loopname("NodeIteratingAlgo"));
      },
      "nodeIteratorAlgo");
  //! [profile w/ vtune]

  std::cout << "Num Triangles: " << numTriangles.reduce() << "\n";
}

/**
 * Edge Iterator algorithm for counting triangles.
 * <code>
 * for ((a, b) in E)
 *   if (a < b)
 *     for (v in intersect(neighbors(a), neighbors(b)))
 *       if (a < v < b)
 *         triangle += 1
 * </code>
 *
 * Thomas Schank. Algorithmic Aspects of Triangle-Based Network Analysis. PhD
 * Thesis. Universitat Karlsruhe. 2007.
 */
void
EdgeIteratingAlgo(const Graph& graph) {
  struct WorkItem {
    GNode src;
    GNode dst;
    WorkItem(const GNode& a1, const GNode& a2) : src(a1), dst(a2) {}
  };

  katana::InsertBag<WorkItem> items;
  katana::GAccumulator<size_t> numTriangles;

  katana::do_all(
      katana::iterate(graph),
      [&](GNode n) {
        for (auto edge : graph.OutEdges(n)) {
          auto dest = graph.OutEdgeDst(edge);
          if (n < dest)
            items.push(WorkItem(n, dest));
        }
      },
      katana::loopname("Initialize"));

  //  katana::profileVtune(
  //! [profile w/ papi]
  katana::profilePapi(
      [&]() {
        katana::do_all(
            katana::iterate(items),
            [&](const WorkItem& w) {
              // Compute intersection of range (w.src, w.dst) in neighbors of
              // w.src and w.dst
              Graph::edge_iterator abegin = graph.OutEdges(w.src).begin();
              Graph::edge_iterator aend = graph.OutEdges(w.src).end();
              Graph::edge_iterator bbegin = graph.OutEdges(w.dst).begin();
              Graph::edge_iterator bend = graph.OutEdges(w.dst).end();

              Graph::edge_iterator aa = LowerBound(
                  abegin, aend, GreaterThanOrEqual<Graph>(graph, w.src));
              Graph::edge_iterator ea =
                  LowerBound(abegin, aend, LessThan<Graph>(graph, w.dst));
              Graph::edge_iterator bb = LowerBound(
                  bbegin, bend, GreaterThanOrEqual<Graph>(graph, w.src));
              Graph::edge_iterator eb =
                  LowerBound(bbegin, bend, LessThan<Graph>(graph, w.dst));

              numTriangles += CountEqual(graph, aa, ea, bb, eb);
            },
            katana::loopname("EdgeIteratingAlgo"),
            katana::chunk_size<CHUNK_SIZE>(), katana::steal());
      },
      "edgeIteratorAlgo");
  //! [profile w/ papi]

  std::cout << "NumTriangles: " << numTriangles.reduce() << "\n";
}

int
main(int argc, char** argv) {
  std::unique_ptr<katana::SharedMemSys> G =
      LonestarStart(argc, argv, name, desc, nullptr, &inputFile);

  katana::StatTimer totalTime("TimerTotal");
  totalTime.start();

  if (!symmetricGraph) {
    KATANA_DIE(
        "This application requires a symmetric graph input;"
        " please use the -symmetricGraph flag "
        " to indicate the input is a symmetric graph.");
  }

  katana::StatTimer timer_graph_read("GraphReadingTime");
  katana::StatTimer timer_auto_algo("AutoAlgo_0");

  timer_graph_read.start();

  std::cout << "Reading from file: " << inputFile << "\n";
  auto res = katana::URI::Make(inputFile);
  if (!res) {
    KATANA_LOG_FATAL("input file {} error: {}", inputFile, res.error());
  }
  auto inputURI = res.value();
  std::unique_ptr<katana::PropertyGraph> pg =
      MakeFileGraph(inputURI, edge_property_name);

  auto pg_result =
      katana::TypedPropertyGraph<NodeData, EdgeData>::Make(pg.get());
  if (!pg_result) {
    KATANA_LOG_FATAL("could not make property graph: {}", pg_result.error());
  }
  Graph graph = pg_result.value();

  timer_auto_algo.start();
  bool relabel =
      katana::analytics::IsApproximateDegreeDistributionPowerLaw(*pg);
  timer_auto_algo.stop();

  if (relabel) {
    katana::gInfo("Relabeling and sorting graph...");
    katana::StatTimer timer_relabel("GraphRelabelTimer");
    timer_relabel.start();
    if (auto r = katana::SortNodesByDegree(pg.get()); !r) {
      KATANA_LOG_FATAL(
          "Relabeling and sorting by node degree failed: {}", r.error());
    }
    timer_relabel.stop();
  }

  if (auto r = katana::SortAllEdgesByDest(pg.get()); !r) {
    KATANA_LOG_FATAL("Sorting edge destination failed: {}", r.error());
  }

  std::cout << "Read " << graph.NumNodes() << " nodes, " << graph.NumEdges()
            << " edges\n";

  timer_graph_read.stop();

  katana::Prealloc(1, 16 * (graph.NumNodes() + graph.NumEdges()));
  katana::ReportPageAllocGuard page_alloc;

  katana::gInfo("Starting triangle counting...");

  katana::StatTimer execTime("Timer_0");
  execTime.start();

  switch (algo) {
  case nodeiterator:
    NodeIteratingAlgo(graph);
    break;

  case edgeiterator:
    EdgeIteratingAlgo(graph);
    break;

  default:
    std::cerr << "Unknown algo: " << algo << "\n";
  }
  execTime.stop();

  page_alloc.Report();

  totalTime.stop();

  return 0;
}
