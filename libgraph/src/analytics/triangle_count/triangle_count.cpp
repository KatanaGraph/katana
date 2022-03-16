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

using SortedGraphView =
    katana::PropertyGraphViews::NodesSortedByDegreeEdgesSortedByDestID;
using Node = SortedGraphView::Node;
using edge_iterator = SortedGraphView::edge_iterator;

constexpr static const unsigned kChunkSize = 16U;

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
template <typename G, typename EI = typename G::edge_iterator>
size_t
CountEqual(
    const G& g, EI aa, EI ea, EI bb,  EI eb) {
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
    return n >= g.OutEdgeDst(*it);
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
NodeIteratingAlgo(const SortedGraphView* graph) {
  katana::GAccumulator<size_t> numTriangles;

  katana::do_all(
      katana::iterate(*graph),
      [&](const Node& n) {
        // Partition neighbors
        // [first, ea) [n] [bb, last)
        edge_iterator first = graph->OutEdges(n).begin();
        edge_iterator last = graph->OutEdges(n).end();
        edge_iterator ea =
            LowerBound(first, last, LessThan<SortedGraphView>(*graph, n));
        edge_iterator bb = LowerBound(
            first, last, GreaterThanOrEqual<SortedGraphView>(*graph, n));

        for (; bb != last; ++bb) {
          Node B = graph->OutEdgeDst(*bb);
          for (auto aa = first; aa != ea; ++aa) {
            Node A = graph->OutEdgeDst(*aa);
            edge_iterator vv = graph->OutEdges(A).begin();
            edge_iterator ev = graph->OutEdges(A).end();
            edge_iterator it =
                LowerBound(vv, ev, LessThan<SortedGraphView>(*graph, B));
            if (it != ev && graph->OutEdgeDst(*it) == B) {
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
    const SortedGraphView* graph, Node n,
    katana::GAccumulator<size_t>& numTriangles) {
  size_t numTriangles_local = 0;
  for (auto edges_n : graph->OutEdges(n)) {
    Node v = graph->OutEdgeDst(edges_n);
    if (v >= n) {
      break;
    }
    edge_iterator it_n = graph->OutEdges(n).begin();

    for (auto edges_v : graph->OutEdges(v)) {
      auto dst_v = graph->OutEdgeDst(edges_v);
      if (dst_v >= v) {
        break;
      }
      while (graph->OutEdgeDst(*it_n) < dst_v) {
        it_n++;
      }
      if (dst_v == graph->OutEdgeDst(*it_n)) {
        edge_iterator multi_it_n = it_n;
        while (multi_it_n != graph->OutEdges(n).end() &&
               graph->OutEdgeDst(*multi_it_n) == dst_v) {
          numTriangles_local += 1;
          multi_it_n++;
        }
      }
    }
  }
  numTriangles += numTriangles_local;
}

/*
 * Simple counting loop, instead of binary searching.
 */
size_t
OrderedCountAlgo(const SortedGraphView* graph) {
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
EdgeIteratingAlgo(const SortedGraphView* graph) {
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
        for (auto edge : graph->OutEdges(n)) {
          auto dest = graph->OutEdgeDst(edge);
          if (n < dest) {
            items.push(WorkItem(n, dest));
          }
        }
      },
      katana::loopname("TriangleCount_Initialize"));

  katana::do_all(
      katana::iterate(items),
      [&](const WorkItem& w) {
        // Compute intersection of range (w.src, w.dst) in neighbors of
        // w.src and w.dst
        edge_iterator abegin = graph->OutEdges(w.src).begin();
        edge_iterator aend = graph->OutEdges(w.src).end();
        edge_iterator bbegin = graph->OutEdges(w.dst).begin();
        edge_iterator bend = graph->OutEdges(w.dst).end();

        edge_iterator aa = LowerBound(
            abegin, aend, GreaterThanOrEqual<SortedGraphView>(*graph, w.src));
        edge_iterator ea =
            LowerBound(abegin, aend, LessThan<SortedGraphView>(*graph, w.dst));
        edge_iterator bb = LowerBound(
            bbegin, bend, GreaterThanOrEqual<SortedGraphView>(*graph, w.src));
        edge_iterator eb =
            LowerBound(bbegin, bend, LessThan<SortedGraphView>(*graph, w.dst));

        numTriangles += CountEqual(*graph, aa, ea, bb, eb);
      },
      katana::loopname("TriangleCount_EdgeIteratingAlgo"),
      katana::chunk_size<kChunkSize>(), katana::steal());

  return numTriangles.reduce();
}

katana::Result<uint64_t>
katana::analytics::TriangleCount(
    katana::PropertyGraph* pg, TriangleCountPlan plan) {
  katana::StatTimer timer_graph_read("GraphReadingTime", "TriangleCount");
  katana::StatTimer timer_auto_algo("AutoRelabel", "TriangleCount");

  timer_graph_read.start();

  katana::StatTimer timer_relabel("GraphRelabelTimer", "TriangleCount");
  timer_relabel.start();
  SortedGraphView sorted_view = pg->BuildView<SortedGraphView>();
  timer_relabel.stop();

  // TODO(amber): Today we sort unconditionally. Figure out a way to re-enable the
  // logic below
#if 0
  bool relabel = false;;

  switch (plan.relabeling()) {
  case TriangleCountPlan::kNoRelabel:
    relabel = false;
    break;
  case TriangleCountPlan::kRelabel:
    relabel = true;
    break;
  case TriangleCountPlan::kAutoRelabel:
    timer_auto_algo.start();
    relabel = IsApproximateDegreeDistributionPowerLaw(*pg);
    timer_auto_algo.stop();
    break;
  default:
    return katana::ErrorCode::AssertionFailed;
  }

  std::unique_ptr<katana::PropertyGraph> mutable_pfg;
  if (relabel || !plan.edges_sorted()) {
    // Copy the graph so we don't mutate the users graph.
    auto mutable_pfg_result = pg->Copy({}, {});
    if (!mutable_pfg_result) {
      return mutable_pfg_result.error();
    }
    mutable_pfg = std::move(mutable_pfg_result.value());
    pg = mutable_pfg.get();
  }

  if (relabel) {
    katana::StatTimer timer_relabel("GraphRelabelTimer", "TriangleCount");
    timer_relabel.start();
    if (auto r = katana::SortNodesByDegree(pg); !r) {
      return r.error();
    }
    timer_relabel.stop();
  }

  // If we relabel we must also sort. Relabeling will break the sorting.
  if (relabel || !plan.edges_sorted()) {
    if (auto r = katana::SortAllEdgesByDest(pg); !r) {
      return r.error();
    }
  }

  timer_graph_read.stop();
#endif

  katana::EnsurePreallocated(1, 16 * (pg->NumNodes() + pg->NumEdges()));
  katana::ReportPageAllocGuard page_alloc;

  KATANA_LOG_VERBOSE("Done relabeling. Starting TriangleCount");

  size_t total_count;
  katana::StatTimer execTime("TriangleCount", "TriangleCount");
  execTime.start();
  switch (plan.algorithm()) {
  case TriangleCountPlan::kNodeIteration:
    total_count = NodeIteratingAlgo(&sorted_view);
    break;
  case TriangleCountPlan::kEdgeIteration:
    total_count = EdgeIteratingAlgo(&sorted_view);
    break;
  case TriangleCountPlan::kOrderedCount:
    total_count = OrderedCountAlgo(&sorted_view);
    break;
  default:
    return katana::ErrorCode::InvalidArgument;
  }
  execTime.stop();

  return total_count;
}
