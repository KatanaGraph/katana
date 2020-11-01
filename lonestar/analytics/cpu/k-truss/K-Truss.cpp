/*
 * This file belongs to the Galois project, a C++ library for exploiting
 * parallelism. The code is being released under the terms of the 3-Clause
 * BSD License (a copy is located in LICENSE.txt at the top-level directory).
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
#include <deque>
#include <fstream>
#include <iostream>
#include <memory>

#include "Lonestar/BoilerPlate.h"

enum Algo {
  bspJacobi,
  bsp,
  bspCoreThenTruss,
};

namespace cll = llvm::cl;

static const char* name = "Maximal k-trusses";
static const char* desc =
    "Computes the maximal k-trusses for a given undirected graph";
static const char* url = "k_truss";

static cll::opt<std::string> inputFile(
    cll::Positional, cll::desc("<input file>"), cll::Required);
static cll::opt<unsigned int> trussNum(
    "trussNum", cll::desc("report trussNum-trusses"), cll::Required);

static cll::opt<std::string> outName(
    "o", cll::desc("output file for the edgelist of resulting truss"));

static cll::opt<Algo> algo(
    "algo", cll::desc("Choose an algorithm:"),
    cll::values(
        clEnumValN(
            Algo::bspJacobi, "bspJacobi",
            "Bulk-synchronous parallel with separated edge removal"),
        clEnumValN(Algo::bsp, "bsp", "Bulk-synchronous parallel (default)"),
        clEnumValN(
            Algo::bspCoreThenTruss, "bspCoreThenTruss",
            "Compute k-1 core and then k-truss")),
    cll::init(Algo::bsp));

using NodeData = std::tuple<>;

struct EdgeFlag : public galois::PODProperty<uint32_t> {};
using EdgeData = std::tuple<EdgeFlag>;

typedef galois::graphs::PropertyGraph<NodeData, EdgeData> Graph;
typedef typename Graph::Node GNode;

using Edge = std::pair<GNode, GNode>;
using EdgeVec = galois::InsertBag<Edge>;
using NodeVec = galois::InsertBag<GNode>;

template <typename T>
using PerIterAlloc = typename galois::PerIterAllocTy::rebind<T>::other;

static const uint32_t valid = 0x0;
static const uint32_t removed = 0x1;

/**
 * Initialize edge data to valid.
 */
template <typename Graph>
void
initialize(Graph* g) {
  //! Initializa all edges to valid.
  galois::do_all(
      galois::iterate(*g),
      [&g](typename Graph::Node N) {
        for (auto e : g->edges(N)) {
          g->template GetEdgeData<EdgeFlag>(e) = valid;
        }
      },
      galois::steal());
}

/**
 * Dump ktruss for each node to a file.
 */
template <typename Graph>
void
reportKTruss(const Graph& g) {
  if (outName.empty()) {
    return;
  }

  std::ofstream of(outName);
  if (!of.is_open()) {
    std::cerr << "Cannot open " << outName << " for output.\n";
    return;
  }

  for (auto n = g.begin(); n != g.end(); ++n) {
    for (auto e : g.edges(n)) {
      auto dest = g.GetEdgeDest(e);
      if (*n < *dest &&
          (g.template GetEdgeData<EdgeFlag>(e) & 0x1) != removed) {
        of << *n << " " << *dest << " " << g.template GetEdgeData<EdgeFlag>(e)
           << "\n";
      }
    }
  }

  of.close();
}

/**
 * Check if the number of valid edges is more than or equal to j.
 * If it is, then the node n still could be processed.
 * Otherwise, the node n will be ignored in the following steps.
 *
 * @param g
 * @param n the target node n to be tested
 * @param j the target number of triangels
 *
 * @return true if the target node n has the number of degrees
 *         more than or equal to j
 */
bool
isValidDegreeNoLessThanJ(const Graph& g, GNode n, unsigned int j) {
  size_t numValid = 0;
  for (auto e : g.edges(n)) {
    if (!(g.GetEdgeData<EdgeFlag>(e) & removed)) {
      numValid += 1;
      if (numValid >= j) {
        return true;
      }
    }
  }
  return numValid >= j;
}

/**
 * Measure the number of intersected edges between the src and the dest nodes.
 *
 * @param g
 * @param src the source node
 * @param dest the destination node
 * @param j the number of the target triangles
 *
 * @return true if the src and the dest are included in more than j triangles
 */
bool
isSupportNoLessThanJ(const Graph& g, GNode src, GNode dest, unsigned int j) {
  size_t numValidEqual = 0;
  auto srcI = g.edge_begin(src), srcE = g.edge_end(src),
       dstI = g.edge_begin(dest), dstE = g.edge_end(dest);

  while (true) {
    //! Find the first valid edge.
    while (srcI != srcE && (g.GetEdgeData<EdgeFlag>(srcI) & removed)) {
      ++srcI;
    }
    while (dstI != dstE && (g.GetEdgeData<EdgeFlag>(dstI) & removed)) {
      ++dstI;
    }

    if (srcI == srcE || dstI == dstE) {
      return numValidEqual >= j;
    }

    //! Check for intersection.
    auto sN = *g.GetEdgeDest(srcI), dN = *g.GetEdgeDest(dstI);
    if (sN < dN) {
      ++srcI;
    } else if (dN < sN) {
      ++dstI;
    } else {
      numValidEqual += 1;
      if (numValidEqual >= j) {
        return true;
      }
      ++srcI;
      ++dstI;
    }
  }

  return numValidEqual >= j;
}

/**
 * BSPTrussJacobiAlgo:
 * 1. Scan for unsupported edges.
 * 2. If no unsupported edges are found, done.
 * 3. Remove unsupported edges in a separated loop.
 *    TODO why would it be processed in a separted loop?
 * 4. Go back to 1.
 */
struct BSPTrussJacobiAlgo {
  std::string name() { return "bsp"; }

  struct PickUnsupportedEdges {
    Graph* g;
    unsigned int j;
    EdgeVec& r;  ///< unsupported
    EdgeVec& s;  ///< next

    PickUnsupportedEdges(Graph* g, unsigned int j, EdgeVec& r, EdgeVec& s)
        : g(g), j(j), r(r), s(s) {}

    void operator()(Edge e) {
      EdgeVec& w = isSupportNoLessThanJ(*g, e.first, e.second, j) ? s : r;
      w.push_back(e);
    }
  };

  void operator()(Graph* g, unsigned int k) {
    if (k - 2 == 0) {
      return;
    }

    EdgeVec unsupported, work[2];
    EdgeVec *cur = &work[0], *next = &work[1];

    //! Symmetry breaking:
    //! Consider only edges (i, j) where i < j.
    galois::do_all(
        galois::iterate(*g),
        [&](GNode n) {
          for (auto e : g->edges(n)) {
            auto dest = g->GetEdgeDest(e);
            if (*dest > n) {
              cur->push_back(std::make_pair(n, *dest));
            }
          }
        },
        galois::steal());

    while (true) {
      galois::do_all(
          galois::iterate(*cur),
          PickUnsupportedEdges{g, k - 2, unsupported, *next}, galois::steal());

      if (std::distance(unsupported.begin(), unsupported.end()) == 0) {
        break;
      }

      //! Mark unsupported edges as removed.
      galois::do_all(
          galois::iterate(unsupported),
          [&](Edge e) {
            g->GetEdgeData<EdgeFlag>(galois::graphs::FindEdgeSortedByDest(
                *g, e.first, e.second)) = removed;
            g->GetEdgeData<EdgeFlag>(galois::graphs::FindEdgeSortedByDest(
                *g, e.second, e.first)) = removed;
          },
          galois::steal());

      unsupported.clear();
      cur->clear();
      std::swap(cur, next);
    }
  }  ///< End operator()
};   ///< End struct BSPTrussJacobiAlgo

/**
 * BSPTrussAlgo:
 * 1. Keep supported edges and remove unsupported edges.
 * 2. If all edges are kept, done.
 * 3. Go back to 3.
 */
struct BSPTrussAlgo {
  std::string name() { return "bsp"; }

  struct KeepSupportedEdges {
    Graph* g;
    unsigned int j;
    EdgeVec& s;

    KeepSupportedEdges(Graph* g, unsigned int j, EdgeVec& s)
        : g(g), j(j), s(s) {}

    void operator()(Edge e) {
      if (isSupportNoLessThanJ(*g, e.first, e.second, j)) {
        s.push_back(e);
      } else {
        g->GetEdgeData<EdgeFlag>(galois::graphs::FindEdgeSortedByDest(
            *g, e.first, e.second)) = removed;
        g->GetEdgeData<EdgeFlag>(galois::graphs::FindEdgeSortedByDest(
            *g, e.second, e.first)) = removed;
      }
    }
  };

  void operator()(Graph* g, unsigned int k) {
    if (k - 2 == 0) {
      return;
    }

    EdgeVec work[2];
    EdgeVec *cur = &work[0], *next = &work[1];
    size_t curSize, nextSize;

    //! Symmetry breaking:
    //! Consider only edges (i, j) where i < j.
    galois::do_all(
        galois::iterate(*g),
        [&g, cur](GNode n) {
          for (auto e : g->edges(n)) {
            auto dest = g->GetEdgeDest(e);
            if (*dest > n) {
              cur->push_back(std::make_pair(n, *dest));
            }
          }
        },
        galois::steal());
    curSize = std::distance(cur->begin(), cur->end());

    //! Remove unsupported edges until no more edges can be removed.
    while (true) {
      galois::do_all(
          galois::iterate(*cur), KeepSupportedEdges{g, k - 2, *next},
          galois::steal());
      nextSize = std::distance(next->begin(), next->end());

      if (curSize == nextSize) {
        //! Every edge in *cur is kept, done
        break;
      }

      cur->clear();
      curSize = nextSize;
      std::swap(cur, next);
    }
  }  ///< End operator()
};   ///< End struct BSPTrussAlgo

/**
 * BSPCoreAlgo:
 * 1. Keep nodes w/ degree >= k and remove all edges for nodes whose degree < k.
 * 2. If all nodes are kept, done.
 * 3. Go back to 1.
 */
struct BSPCoreAlgo {
  std::string name() { return "bspCore"; }

  struct KeepValidNodes {
    Graph* g;
    unsigned int j;
    NodeVec& s;

    KeepValidNodes(Graph* g, unsigned int j, NodeVec& s) : g(g), j(j), s(s) {}

    void operator()(GNode n) {
      if (isValidDegreeNoLessThanJ(*g, n, j)) {
        s.push_back(n);
      } else {
        for (auto e : g->edges(n)) {
          auto dest = g->GetEdgeDest(e);
          g->GetEdgeData<EdgeFlag>(
              galois::graphs::FindEdgeSortedByDest(*g, n, *dest)) = removed;
          g->GetEdgeData<EdgeFlag>(
              galois::graphs::FindEdgeSortedByDest(*g, *dest, n)) = removed;
        }
      }
    }
  };

  void operator()(Graph* g, unsigned int k) {
    NodeVec work[2];
    NodeVec *cur = &work[0], *next = &work[1];
    size_t curSize = g->num_nodes(), nextSize;

    galois::do_all(
        galois::iterate(*g), KeepValidNodes{g, k, *next}, galois::steal());
    nextSize = std::distance(next->begin(), next->end());

    while (curSize != nextSize) {
      cur->clear();
      curSize = nextSize;
      std::swap(cur, next);

      galois::do_all(
          galois::iterate(*cur), KeepValidNodes{g, k, *next}, galois::steal());
      nextSize = std::distance(next->begin(), next->end());
    }
  }
};  ///< End BSPCoreAlgo.

/**
 * BSPCoreThenTrussAlgo:
 * 1. Reduce the graph to k-1 core
 * 2. Compute k-truss from k-1 core
 */
struct BSPCoreThenTrussAlgo {
  std::string name() { return "bspCoreThenTruss"; }

  void operator()(Graph* g, unsigned int k) {
    if (k - 2 == 0) {
      return;
    }

    galois::StatTimer TCore("Reduce_to_(k-1)-core");
    TCore.start();

    BSPCoreAlgo bspCore;
    bspCore(g, k - 1);

    TCore.stop();

    galois::StatTimer TTruss("Reduce_to_k-truss");
    TTruss.start();

    BSPTrussAlgo bspTrussIm;
    bspTrussIm(g, k);

    TTruss.stop();
  }  ///< End operator().
};   ///< End struct BSPCoreThenTrussAlgo.

template <typename Algo>
void
run() {
  Algo algo;

  std::cout << "Reading from file: " << inputFile << "\n";
  std::unique_ptr<galois::graphs::PropertyFileGraph> pfg =
      MakeFileGraph(inputFile, edge_property_name);
  auto result = ConstructEdgeProperties<EdgeData>(pfg.get());
  if (!result) {
    GALOIS_LOG_FATAL("failed to construct node properties: {}", result.error());
  }
  auto res = galois::graphs::SortAllEdgesByDest(pfg.get());
  if (!res) {
    GALOIS_LOG_FATAL("Sorting property file graph failed: {}", res.error());
  }

  auto pg_result =
      galois::graphs::PropertyGraph<NodeData, EdgeData>::Make(pfg.get());
  if (!pg_result) {
    GALOIS_LOG_FATAL("could not make property graph: {}", pg_result.error());
  }
  Graph graph = pg_result.value();

  std::cout << "Read " << graph.num_nodes() << " nodes, " << graph.num_edges()
            << " edges\n";

  std::cout << "Running " << algo.name() << " algorithm for maximal "
            << trussNum << "-truss\n";

  size_t approxEdgeData = 4 * (graph.num_nodes() + graph.num_edges());
  galois::Prealloc(1, 4 * approxEdgeData);
  galois::reportPageAlloc("MeminfoPre");

  initialize(&graph);

  galois::StatTimer execTime("Timer_0");
  execTime.start();
  algo(&graph, trussNum);
  execTime.stop();

  galois::reportPageAlloc("MeminfoPost");
  reportKTruss(graph);

  uint64_t numEdges = 0;

  for (auto n : graph) {
    for (auto e : graph.edges(n)) {
      auto dest = graph.GetEdgeDest(e);
      if (n < *dest && (graph.GetEdgeData<EdgeFlag>(e) & 0x1) != removed) {
        numEdges++;
      }
    }
  }

  galois::gInfo("Number of edges left in truss is ", numEdges);
}

int
main(int argc, char** argv) {
  std::unique_ptr<galois::SharedMemSys> G =
      LonestarStart(argc, argv, name, desc, url, &inputFile);

  galois::StatTimer totalTime("TimerTotal");
  totalTime.start();

  if (!symmetricGraph) {
    GALOIS_DIE(
        "This application requires a symmetric graph input;"
        " please use the -symmetricGraph flag "
        " to indicate the input is a symmetric graph.");
  }

  if (2 > trussNum) {
    std::cerr << "trussNum >= 2\n";
    return -1;
  }

  switch (algo) {
  case bspJacobi:
    run<BSPTrussJacobiAlgo>();
    break;
  case bsp:
    run<BSPTrussAlgo>();
    break;
  case bspCoreThenTruss:
    run<BSPCoreThenTrussAlgo>();
    break;
  default:
    std::cerr << "Unknown algorithm\n";
    abort();
  }

  totalTime.stop();

  return 0;
}
