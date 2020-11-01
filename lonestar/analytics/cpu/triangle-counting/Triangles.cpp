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
#include <fstream>
#include <iostream>
#include <utility>
#include <vector>

#include <boost/iterator/transform_iterator.hpp>

#include "Lonestar/BoilerPlate.h"
#include "galois/runtime/Profile.h"

const char* name = "Triangles";
const char* desc = "Counts the triangles in a graph";

constexpr static const unsigned CHUNK_SIZE = 64U;
enum Algo { nodeiterator, edgeiterator, orderedCount };

namespace cll = llvm::cl;

static cll::opt<std::string> inputFile(
    cll::Positional, cll::desc("<input file>"), cll::Required);
static cll::opt<Algo> algo(
    "algo", cll::desc("Choose an algorithm:"),
    cll::values(
        clEnumValN(Algo::nodeiterator, "nodeiterator", "Node Iterator"),
        clEnumValN(Algo::edgeiterator, "edgeiterator", "Edge Iterator"),
        clEnumValN(
            Algo::orderedCount, "orderedCount",
            "Ordered Simple Count (default)")),
    cll::init(Algo::orderedCount));

static cll::opt<bool> relabel(
    "relabel",
    cll::desc("Relabel nodes of the graph (default value of false => "
              "choose automatically)"),
    cll::init(false));

using NodeData = std::tuple<>;
using EdgeData = std::tuple<>;

typedef galois::graphs::PropertyGraph<NodeData, EdgeData> Graph;
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
    typename G::Node a = *g.GetEdgeDest(aa);
    typename G::Node b = *g.GetEdgeDest(bb);
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
    return *g.GetEdgeDest(it) < n;
  }
};

template <typename G>
struct GreaterThanOrEqual {
  const G& g;
  typename G::Node n;
  GreaterThanOrEqual(const G& g, typename G::Node n) : g(g), n(n) {}
  bool operator()(typename G::edge_iterator it) {
    return !(n < *g.GetEdgeDest(it));
  }
};

template <typename G>
struct DegreeLess {
  typedef typename G::Node N;
  const G& g;
  DegreeLess(const G& g) : g(g) {}

  bool operator()(const N& n1, const N& n2) const {
    return std::distance(g.edge_begin(n1), g.edge_end(n1)) <
           std::distance(g.edge_begin(n2), g.edge_end(n2));
  }
};
template <typename G>
struct DegreeGreater {
  typedef typename G::Node N;
  const G& g;
  DegreeGreater(const G& g) : g(g) {}

  bool operator()(const N& n1, const N& n2) const {
    return std::distance(g.edge_begin(n1), g.edge_end(n1)) >
           std::distance(g.edge_begin(n2), g.edge_end(n2));
  }
};
template <typename G>
struct GetDegree {
  typedef typename G::Node N;
  const G& g;
  GetDegree(const G& g) : g(g) {}

  ptrdiff_t operator()(const N& n) const {
    return std::distance(g.edge_begin(n), g.edge_end(n));
  }
};

template <typename Node, typename EdgeTy>
struct IdLess {
  bool operator()(
      const galois::graphs::EdgeSortValue<Node, EdgeTy>& e1,
      const galois::graphs::EdgeSortValue<Node, EdgeTy>& e2) const {
    return e1.dst < e2.dst;
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
  galois::GAccumulator<size_t> numTriangles;

  //! [profile w/ vtune]
  galois::runtime::profileVtune(
      [&]() {
        galois::do_all(
            galois::iterate(graph),
            [&](const GNode& n) {
              // Partition neighbors
              // [first, ea) [n] [bb, last)
              Graph::edge_iterator first = graph.edge_begin(n);
              Graph::edge_iterator last = graph.edge_end(n);
              Graph::edge_iterator ea =
                  LowerBound(first, last, LessThan<Graph>(graph, n));
              Graph::edge_iterator bb =
                  LowerBound(first, last, GreaterThanOrEqual<Graph>(graph, n));

              for (; bb != last; ++bb) {
                GNode B = *graph.GetEdgeDest(bb);
                for (auto aa = first; aa != ea; ++aa) {
                  GNode A = *graph.GetEdgeDest(aa);
                  Graph::edge_iterator vv = graph.edge_begin(A);
                  Graph::edge_iterator ev = graph.edge_end(A);
                  Graph::edge_iterator it =
                      LowerBound(vv, ev, LessThan<Graph>(graph, B));
                  if (it != ev && *graph.GetEdgeDest(it) == B) {
                    numTriangles += 1;
                  }
                }
              }
            },
            galois::chunk_size<CHUNK_SIZE>(), galois::steal(),
            galois::loopname("NodeIteratingAlgo"));
      },
      "nodeIteratorAlgo");
  //! [profile w/ vtune]

  std::cout << "Num Triangles: " << numTriangles.reduce() << "\n";
}

/**
 * Lambda function to count triangles
 */
void
OrderedCountFunc(
    const Graph& graph, GNode n, galois::GAccumulator<size_t>& numTriangles) {
  size_t numTriangles_local = 0;
  for (auto it_v : graph.edges(n)) {
    auto v = *graph.GetEdgeDest(it_v);
    if (v > n)
      break;
    Graph::edge_iterator it_n = graph.edge_begin(n);

    for (auto it_vv : graph.edges(v)) {
      auto vv = *graph.GetEdgeDest(it_vv);
      if (vv > v)
        break;
      while (*graph.GetEdgeDest(it_n) < vv)
        it_n++;
      if (vv == *graph.GetEdgeDest(it_n)) {
        numTriangles_local += 1;
      }
    }
  }
  numTriangles += numTriangles_local;
}

/*
 * Simple counting loop, instead of binary searching.
 */
void
OrderedCountAlgo(const Graph& graph) {
  galois::GAccumulator<size_t> numTriangles;
  galois::do_all(
      galois::iterate(graph),
      [&](const GNode& n) { OrderedCountFunc(graph, n, numTriangles); },
      galois::chunk_size<CHUNK_SIZE>(), galois::steal(),
      galois::loopname("OrderedCountAlgo"));

  galois::gPrint("Num Triangles: ", numTriangles.reduce(), "\n");
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

  galois::InsertBag<WorkItem> items;
  galois::GAccumulator<size_t> numTriangles;

  galois::do_all(
      galois::iterate(graph),
      [&](GNode n) {
        for (auto edge : graph.edges(n)) {
          auto dest = graph.GetEdgeDest(edge);
          if (n < *dest)
            items.push(WorkItem(n, *dest));
        }
      },
      galois::loopname("Initialize"));

  //  galois::runtime::profileVtune(
  //! [profile w/ papi]
  galois::runtime::profilePapi(
      [&]() {
        galois::do_all(
            galois::iterate(items),
            [&](const WorkItem& w) {
              // Compute intersection of range (w.src, w.dst) in neighbors of
              // w.src and w.dst
              Graph::edge_iterator abegin = graph.edge_begin(w.src);
              Graph::edge_iterator aend = graph.edge_end(w.src);
              Graph::edge_iterator bbegin = graph.edge_begin(w.dst);
              Graph::edge_iterator bend = graph.edge_end(w.dst);

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
            galois::loopname("EdgeIteratingAlgo"),
            galois::chunk_size<CHUNK_SIZE>(), galois::steal());
      },
      "edgeIteratorAlgo");
  //! [profile w/ papi]

  std::cout << "NumTriangles: " << numTriangles.reduce() << "\n";
}

int
main(int argc, char** argv) {
  std::unique_ptr<galois::SharedMemSys> G =
      LonestarStart(argc, argv, name, desc, nullptr, &inputFile);

  galois::StatTimer totalTime("TimerTotal");
  totalTime.start();

  if (!symmetricGraph) {
    GALOIS_DIE(
        "This application requires a symmetric graph input;"
        " please use the -symmetricGraph flag "
        " to indicate the input is a symmetric graph.");
  }

  galois::StatTimer timer_graph_read("GraphReadingTime");
  galois::StatTimer timer_auto_algo("AutoAlgo_0");

  timer_graph_read.start();

  std::cout << "Reading from file: " << inputFile << "\n";
  std::unique_ptr<galois::graphs::PropertyFileGraph> pfg =
      MakeFileGraph(inputFile, edge_property_name);

  auto pg_result =
      galois::graphs::PropertyGraph<NodeData, EdgeData>::Make(pfg.get());
  if (!pg_result) {
    GALOIS_LOG_FATAL("could not make property graph: {}", pg_result.error());
  }
  Graph graph = pg_result.value();

  if (!relabel) {
    timer_auto_algo.start();
    relabel = isApproximateDegreeDistributionPowerLaw(graph);
    timer_auto_algo.stop();
  }

  if (relabel) {
    galois::gInfo("Relabeling and sorting graph...");
    galois::StatTimer timer_relabel("GraphRelabelTimer");
    timer_relabel.start();
    if (auto r = galois::graphs::SortNodesByDegree(pfg.get()); !r) {
      GALOIS_LOG_FATAL(
          "Relabeling and sorting by node degree failed: {}", r.error());
    }
    timer_relabel.stop();
  }

  if (auto r = galois::graphs::SortAllEdgesByDest(pfg.get()); !r) {
    GALOIS_LOG_FATAL("Sorting edge destination failed: {}", r.error());
  }

  std::cout << "Read " << graph.num_nodes() << " nodes, " << graph.num_edges()
            << " edges\n";

  timer_graph_read.stop();

  galois::Prealloc(1, 16 * (graph.num_nodes() + graph.num_edges()));
  galois::reportPageAlloc("MeminfoPre");

  galois::gInfo("Starting triangle counting...");

  galois::StatTimer execTime("Timer_0");
  execTime.start();
  // case by case preAlloc to avoid allocating unnecessarily
  switch (algo) {
  case nodeiterator:
    NodeIteratingAlgo(graph);
    break;

  case edgeiterator:
    EdgeIteratingAlgo(graph);
    break;

  case orderedCount:
    OrderedCountAlgo(graph);
    break;

  default:
    std::cerr << "Unknown algo: " << algo << "\n";
  }
  execTime.stop();

  galois::reportPageAlloc("MeminfoPost");

  totalTime.stop();

  return 0;
}
