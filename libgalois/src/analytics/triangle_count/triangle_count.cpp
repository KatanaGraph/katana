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

#include "katana/analytics/triangle_count/triangle_count.h"

#include "katana/analytics/Utils.h"

using namespace katana::analytics;

using PropertyFileGraph = katana::PropertyFileGraph;
using Node = katana::PropertyFileGraph::Node;

constexpr static const unsigned kChunkSize = 64U;

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
    return n >= *g.GetEdgeDest(it);
  }
};

template <typename G>
struct GetDegree {
  typedef typename G::Node N;
  const G& g;
  GetDegree(const G& g) : g(g) {}

  ptrdiff_t operator()(const N& n) const { return g.edges(n).size(); }
};

template <typename Node, typename EdgeTy>
struct IdLess {
  bool operator()(
      const katana::EdgeSortValue<Node, EdgeTy>& e1,
      const katana::EdgeSortValue<Node, EdgeTy>& e2) const {
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
size_t
NodeIteratingAlgo(katana::PropertyFileGraph* graph) {
  katana::GAccumulator<size_t> numTriangles;

  katana::do_all(
      katana::iterate(*graph),
      [&](const PropertyFileGraph::Node& n) {
        // Partition neighbors
        // [first, ea) [n] [bb, last)
        PropertyFileGraph::edge_iterator first = graph->edges(n).begin();
        PropertyFileGraph::edge_iterator last = graph->edges(n).end();
        PropertyFileGraph::edge_iterator ea =
            LowerBound(first, last, LessThan<PropertyFileGraph>(*graph, n));
        PropertyFileGraph::edge_iterator bb = LowerBound(
            first, last, GreaterThanOrEqual<PropertyFileGraph>(*graph, n));

        for (; bb != last; ++bb) {
          Node B = *graph->GetEdgeDest(bb);
          for (auto aa = first; aa != ea; ++aa) {
            Node A = *graph->GetEdgeDest(aa);
            PropertyFileGraph::edge_iterator vv = graph->edges(A).begin();
            PropertyFileGraph::edge_iterator ev = graph->edges(A).end();
            PropertyFileGraph::edge_iterator it =
                LowerBound(vv, ev, LessThan<PropertyFileGraph>(*graph, B));
            if (it != ev && *graph->GetEdgeDest(it) == B) {
              numTriangles += 1;
            }
          }
        }
      },
      katana::chunk_size<kChunkSize>(), katana::steal(),
      katana::loopname("TriangleCount_NodeIteratingAlgo"));

  return numTriangles.reduce();
}

/**
 * Lambda function to count triangles
 */
void
OrderedCountFunc(
    PropertyFileGraph* graph, Node n,
    katana::GAccumulator<size_t>& numTriangles) {
  size_t numTriangles_local = 0;
  for (auto it_v : graph->edges(n)) {
    auto v = *graph->GetEdgeDest(it_v);
    if (v > n) {
      break;
    }
    PropertyFileGraph::edge_iterator it_n = graph->edges(n).begin();

    for (auto it_vv : graph->edges(v)) {
      auto vv = *graph->GetEdgeDest(it_vv);
      if (vv > v) {
        break;
      }
      while (*graph->GetEdgeDest(it_n) < vv) {
        it_n++;
      }
      if (vv == *graph->GetEdgeDest(it_n)) {
        numTriangles_local += 1;
      }
    }
  }
  numTriangles += numTriangles_local;
}

/*
 * Simple counting loop, instead of binary searching.
 */
size_t
OrderedCountAlgo(PropertyFileGraph* graph) {
  katana::GAccumulator<size_t> numTriangles;
  katana::do_all(
      katana::iterate(*graph),
      [&](const Node& n) { OrderedCountFunc(graph, n, numTriangles); },
      katana::chunk_size<kChunkSize>(), katana::steal(),
      katana::loopname("TriangleCount_OrderedCountAlgo"));

  return numTriangles.reduce();
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
size_t
EdgeIteratingAlgo(PropertyFileGraph* graph) {
  struct WorkItem {
    Node src;
    Node dst;
    WorkItem(const Node& a1, const Node& a2) : src(a1), dst(a2) {}
  };

  katana::InsertBag<WorkItem> items;
  katana::GAccumulator<size_t> numTriangles;

  katana::do_all(
      katana::iterate(*graph),
      [&](Node n) {
        for (auto edge : graph->edges(n)) {
          auto dest = graph->GetEdgeDest(edge);
          if (n < *dest) {
            items.push(WorkItem(n, *dest));
          }
        }
      },
      katana::loopname("TriangleCount_Initialize"));

  katana::do_all(
      katana::iterate(items),
      [&](const WorkItem& w) {
        // Compute intersection of range (w.src, w.dst) in neighbors of
        // w.src and w.dst
        PropertyFileGraph::edge_iterator abegin = graph->edges(w.src).begin();
        PropertyFileGraph::edge_iterator aend = graph->edges(w.src).end();
        PropertyFileGraph::edge_iterator bbegin = graph->edges(w.dst).begin();
        PropertyFileGraph::edge_iterator bend = graph->edges(w.dst).end();

        PropertyFileGraph::edge_iterator aa = LowerBound(
            abegin, aend, GreaterThanOrEqual<PropertyFileGraph>(*graph, w.src));
        PropertyFileGraph::edge_iterator ea = LowerBound(
            abegin, aend, LessThan<PropertyFileGraph>(*graph, w.dst));
        PropertyFileGraph::edge_iterator bb = LowerBound(
            bbegin, bend, GreaterThanOrEqual<PropertyFileGraph>(*graph, w.src));
        PropertyFileGraph::edge_iterator eb = LowerBound(
            bbegin, bend, LessThan<PropertyFileGraph>(*graph, w.dst));

        numTriangles += CountEqual(*graph, aa, ea, bb, eb);
      },
      katana::loopname("TriangleCount_EdgeIteratingAlgo"),
      katana::chunk_size<kChunkSize>(), katana::steal());

  return numTriangles.reduce();
}

katana::Result<uint64_t>
katana::analytics::TriangleCount(
    katana::PropertyFileGraph* pfg, TriangleCountPlan plan) {
  katana::StatTimer timer_graph_read("GraphReadingTime", "TriangleCount");
  katana::StatTimer timer_auto_algo("AutoRelabel", "TriangleCount");

  bool relabel;
  timer_graph_read.start();
  switch (plan.relabeling()) {
  case TriangleCountPlan::kNoRelabel:
    relabel = false;
    break;
  case TriangleCountPlan::kRelabel:
    relabel = true;
    break;
  case TriangleCountPlan::kAutoRelabel:
    timer_auto_algo.start();
    relabel = IsApproximateDegreeDistributionPowerLaw(*pfg);
    timer_auto_algo.stop();
    break;
  default:
    return katana::ErrorCode::AssertionFailed;
  }

  std::unique_ptr<katana::PropertyFileGraph> mutable_pfg;
  if (relabel || !plan.edges_sorted()) {
    // Copy the graph so we don't mutate the users graph.
    auto mutable_pfg_result = pfg->Copy({}, {});
    if (!mutable_pfg_result) {
      return mutable_pfg_result.error();
    }
    mutable_pfg = std::move(mutable_pfg_result.value());
    pfg = mutable_pfg.get();
  }

  if (relabel) {
    katana::StatTimer timer_relabel("GraphRelabelTimer", "TriangleCount");
    timer_relabel.start();
    if (auto r = katana::SortNodesByDegree(pfg); !r) {
      return r.error();
    }
    timer_relabel.stop();
  }

  // If we relabel we must also sort. Relabeling will break the sorting.
  if (relabel || !plan.edges_sorted()) {
    if (auto r = katana::SortAllEdgesByDest(pfg); !r) {
      return r.error();
    }
  }

  timer_graph_read.stop();

  katana::Prealloc(1, 16 * (pfg->num_nodes() + pfg->num_edges()));
  katana::reportPageAlloc("TriangleCount_MeminfoPre");

  size_t total_count;
  katana::StatTimer execTime("TriangleCount", "TriangleCount");
  execTime.start();
  switch (plan.algorithm()) {
  case TriangleCountPlan::kNodeIteration:
    total_count = NodeIteratingAlgo(pfg);
    break;
  case TriangleCountPlan::kEdgeIteration:
    total_count = EdgeIteratingAlgo(pfg);
    break;
  case TriangleCountPlan::kOrderedCount:
    total_count = OrderedCountAlgo(pfg);
    break;
  default:
    return katana::ErrorCode::InvalidArgument;
  }
  execTime.stop();

  katana::reportPageAlloc("TriangleCount_MeminfoPost");

  return total_count;
}
