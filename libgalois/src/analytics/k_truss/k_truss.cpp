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

#include "katana/analytics/k_truss/k_truss.h"

#include "katana/ArrowRandomAccessBuilder.h"

using namespace katana::analytics;

using NodeData = std::tuple<>;

struct EdgeFlag : public katana::PODProperty<uint32_t> {};
using EdgeData = std::tuple<EdgeFlag>;

typedef katana::PropertyGraph<NodeData, EdgeData> Graph;
typedef typename Graph::Node GNode;

using Edge = std::pair<GNode, GNode>;
using EdgeVec = katana::InsertBag<Edge>;
using NodeVec = katana::InsertBag<GNode>;

static const uint32_t valid = 0x0;
static const uint32_t removed = 0x1;

/// Initialize edge data to valid.
template <typename Graph>
void
KTrussInitialization(Graph* g) {
  //! Initializa all edges to valid.
  katana::do_all(
      katana::iterate(*g),
      [&g](typename Graph::Node N) {
        for (auto e : g->edges(N)) {
          g->template GetEdgeData<EdgeFlag>(e) = valid;
        }
      },
      katana::steal());
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
IsValidDegreeNoLessThanJ(const Graph& g, GNode n, unsigned int j) {
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
IsSupportNoLessThanJ(const Graph& g, GNode src, GNode dest, unsigned int j) {
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

struct PickUnsupportedEdges {
  Graph* g;
  unsigned int j;
  EdgeVec& r;  ///< unsupported
  EdgeVec& s;  ///< next

  void operator()(Edge e) {
    EdgeVec& w = IsSupportNoLessThanJ(*g, e.first, e.second, j) ? s : r;
    w.push_back(e);
  }
};

/// BSPTrussJacobiAlgo:
/// 1. Scan for unsupported edges.
/// 2. If no unsupported edges are found, done.
/// 3. Remove unsupported edges in a separated loop.
/// 4. Go back to 1.
katana::Result<void>
BSPTrussJacobiAlgo(Graph* g, uint32_t k) {
  if (k - 2 == 0) {
    return katana::ErrorCode::InvalidArgument;
  }

  EdgeVec unsupported, work[2];
  EdgeVec *cur = &work[0], *next = &work[1];

  //! Symmetry breaking:
  //! Consider only edges (i, j) where i < j.
  katana::do_all(
      katana::iterate(*g),
      [&](GNode n) {
        for (auto e : g->edges(n)) {
          auto dest = g->GetEdgeDest(e);
          if (*dest > n) {
            cur->push_back(std::make_pair(n, *dest));
          }
        }
      },
      katana::steal());

  while (true) {
    katana::do_all(
        katana::iterate(*cur),
        PickUnsupportedEdges{g, k - 2, unsupported, *next}, katana::steal());

    if (std::distance(unsupported.begin(), unsupported.end()) == 0) {
      break;
    }

    //! Mark unsupported edges as removed.
    katana::do_all(
        katana::iterate(unsupported),
        [&](Edge e) {
          g->template GetEdgeData<EdgeFlag>(
              katana::FindEdgeSortedByDest(*g, e.first, e.second)) = removed;
          g->template GetEdgeData<EdgeFlag>(
              katana::FindEdgeSortedByDest(*g, e.second, e.first)) = removed;
        },
        katana::steal());

    unsupported.clear();
    cur->clear();
    std::swap(cur, next);
  }
  return katana::ResultSuccess();
}

struct KeepSupportedEdges {
  Graph* g;
  unsigned int j;
  EdgeVec& s;

  void operator()(Edge e) {
    if (IsSupportNoLessThanJ(*g, e.first, e.second, j)) {
      s.push_back(e);
    } else {
      g->template GetEdgeData<EdgeFlag>(
          katana::FindEdgeSortedByDest(*g, e.first, e.second)) = removed;
      g->template GetEdgeData<EdgeFlag>(
          katana::FindEdgeSortedByDest(*g, e.second, e.first)) = removed;
    }
  }
};

/// BSPTrussAlgo:
/// 1. Keep supported edges and remove unsupported edges.
/// 2. If all edges are kept, done.
/// 3. Go back to 3.
katana::Result<void>
BSPTrussAlgo(Graph* g, unsigned int k) {
  if (k - 2 == 0) {
    return katana::ErrorCode::InvalidArgument;
    ;
  }

  EdgeVec work[2];
  EdgeVec *cur = &work[0], *next = &work[1];
  size_t curSize, nextSize;

  //! Symmetry breaking:
  //! Consider only edges (i, j) where i < j.
  katana::do_all(
      katana::iterate(*g),
      [&g, cur](GNode n) {
        for (auto e : g->edges(n)) {
          auto dest = g->GetEdgeDest(e);
          if (*dest > n) {
            cur->push_back(std::make_pair(n, *dest));
          }
        }
      },
      katana::steal());
  curSize = std::distance(cur->begin(), cur->end());

  //! Remove unsupported edges until no more edges can be removed.
  while (true) {
    katana::do_all(
        katana::iterate(*cur), KeepSupportedEdges{g, k - 2, *next},
        katana::steal());
    nextSize = std::distance(next->begin(), next->end());

    if (curSize == nextSize) {
      //! Every edge in *cur is kept, done
      break;
    }

    cur->clear();
    curSize = nextSize;
    std::swap(cur, next);
  }
  return katana::ResultSuccess();
}

struct KeepValidNodes {
  Graph* g;
  unsigned int j;
  NodeVec& s;

  void operator()(GNode n) {
    if (IsValidDegreeNoLessThanJ(*g, n, j)) {
      s.push_back(n);
    } else {
      for (auto e : g->edges(n)) {
        auto dest = g->GetEdgeDest(e);
        g->template GetEdgeData<EdgeFlag>(
            katana::FindEdgeSortedByDest(*g, n, *dest)) = removed;
        g->template GetEdgeData<EdgeFlag>(
            katana::FindEdgeSortedByDest(*g, *dest, n)) = removed;
      }
    }
  }
};

/// BSPCoreAlgo:
/// 1. Keep nodes w/ degree >= k and remove all edges for nodes whose degree < k.
/// 2. If all nodes are kept, done.
/// 3. Go back to 1.
katana::Result<void>
BSPCoreAlgo(Graph* g, uint32_t k) {
  NodeVec work[2];
  NodeVec *cur = &work[0], *next = &work[1];
  size_t curSize = g->num_nodes(), nextSize;

  katana::do_all(
      katana::iterate(*g), KeepValidNodes{g, k, *next}, katana::steal());
  nextSize = std::distance(next->begin(), next->end());

  while (curSize != nextSize) {
    cur->clear();
    curSize = nextSize;
    std::swap(cur, next);

    katana::do_all(
        katana::iterate(*cur), KeepValidNodes{g, k, *next}, katana::steal());
    nextSize = std::distance(next->begin(), next->end());
  }
  return katana::ResultSuccess();
}

/// BSPCoreThenTrussAlgo:
/// 1. Reduce the graph to k-1 core
/// 2. Compute k-truss from k-1 core
katana::Result<void>
BSPCoreThenTrussAlgo(Graph* g, uint32_t k) {
  if (k - 2 == 0) {
    return katana::ErrorCode::InvalidArgument;
  }

  katana::StatTimer TCore("Reduce_to_(k-1)-core");
  TCore.start();

  if (auto r = BSPCoreAlgo(g, k - 1); !r) {
    return r.error();
  }

  TCore.stop();

  katana::StatTimer TTruss("Reduce_to_k-truss");
  TTruss.start();

  if (auto r = BSPTrussAlgo(g, k); !r) {
    return r.error();
  }

  TTruss.stop();
  return katana::ResultSuccess();
}

katana::Result<void>
katana::analytics::KTruss(
    katana::PropertyFileGraph* pfg, uint32_t k_truss_number,
    const std::string& output_property_name, KTrussPlan plan) {
  if (auto result =
          ConstructEdgeProperties<EdgeData>(pfg, {output_property_name});
      !result) {
    return result.error();
  }

  auto result = katana::SortAllEdgesByDest(pfg);
  if (!result) {
    return result.error();
  }

  auto pg_result = Graph::Make(pfg, {}, {output_property_name});
  if (!pg_result) {
    return pg_result.error();
  }
  auto graph = pg_result.value();

  KTrussInitialization(&graph);

  katana::StatTimer exec_time("KTruss");
  exec_time.start();

  switch (plan.algorithm()) {
  case KTrussPlan::kBsp:
    return BSPTrussAlgo(&graph, k_truss_number);
    break;
  case KTrussPlan::kBspJacobi:
    return BSPTrussJacobiAlgo(&graph, k_truss_number);
    break;
  case KTrussPlan::kBspCoreThenTruss:
    return BSPCoreThenTrussAlgo(&graph, k_truss_number);
    break;
  default:
    return katana::ErrorCode::InvalidArgument;
  }
}

// TODO (gill) Add a validity routine.
katana::Result<void>
katana::analytics::KTrussAssertValid(
    [[maybe_unused]] katana::PropertyFileGraph* pfg,
    [[maybe_unused]] uint32_t k_truss_number,
    [[maybe_unused]] const std::string& property_name) {
  return katana::ResultSuccess();
}

katana::Result<KTrussStatistics>
katana::analytics::KTrussStatistics::Compute(
    katana::PropertyFileGraph* pfg, uint32_t k_truss_number,
    const std::string& property_name) {
  auto pg_result = Graph::Make(pfg, {}, {property_name});
  if (!pg_result) {
    return pg_result.error();
  }

  auto graph = pg_result.value();

  katana::GAccumulator<uint32_t> alive_edges;
  alive_edges.reset();

  katana::do_all(
      katana::iterate(graph),
      [&](const GNode& node) {
        for (auto e : graph.edges(node)) {
          auto dest = graph.GetEdgeDest(e);
          if (node < *dest &&
              (graph.GetEdgeData<EdgeFlag>(e) & 0x1) != removed) {
            alive_edges += 1;
          }
        }
      },
      katana::loopname("KTruss sanity check"), katana::no_stats());

  return KTrussStatistics{k_truss_number, alive_edges.reduce()};
}

void
katana::analytics::KTrussStatistics::Print(std::ostream& os) {
  os << "K core number = " << k_truss_number << std::endl;
  os << "Number of nodes in the core = " << number_of_edges_left << std::endl;
}
