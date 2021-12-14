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

#include "katana/analytics/local_clustering_coefficient/local_clustering_coefficient.h"

#include "katana/AtomicHelpers.h"

using namespace katana::analytics;

namespace {
constexpr static const unsigned kChunkSize = 64U;

struct NodeClusteringCoefficient : public katana::PODProperty<double> {};

using NodeData = typename std::tuple<NodeClusteringCoefficient>;
using EdgeData = typename std::tuple<>;

using SortedPropertyGraphView = katana::PropertyGraphViews::EdgesSortedByDestID;
using SortedGraphView =
    katana::TypedPropertyGraphView<SortedPropertyGraphView, NodeData, EdgeData>;
using Node = SortedGraphView::Node;

struct LocalClusteringCoefficientAtomics {
  /**
   * Counts the number of triangles for each node
   * in the graph using atomics.
   *
   * Uses simple 3-level nested algorithm to find
   * triangles. It assumes that edgelist of each node
   * is sorted.
   */
  template <typename CountVec>
  void OrderedCountFunc(
      const SortedGraphView& graph, Node n, CountVec* count_vec) {
    // TODO(amber): replace with NodeIteratingAlgo for triangle counting
    for (auto edges_n : graph.edges(n)) {
      auto v = graph.edge_dest(edges_n);
      if (v > n) {
        break;
      }
      auto e_it_n = graph.edges(n).begin();

      for (auto edges_v : graph.edges(v)) {
        auto dst_v = graph.edge_dest(edges_v);
        if (dst_v > v) {
          break;
        }
        while (graph.edge_dest(*e_it_n) < dst_v) {
          e_it_n++;
        }
        if (dst_v == graph.edge_dest(*e_it_n)) {
          __sync_fetch_and_add(&(*count_vec)[n], uint32_t{1});
          __sync_fetch_and_add(&(*count_vec)[v], uint32_t{1});
          __sync_fetch_and_add(&(*count_vec)[dst_v], uint32_t{1});
        }
      }
    }
  }

  void ComputeLocalClusteringCoefficient(SortedGraphView* graph) {
    katana::NUMAArray<uint32_t> per_node_triangles;
    per_node_triangles.allocateInterleaved(graph->num_nodes());

    katana::ParallelSTL::fill(
        per_node_triangles.begin(), per_node_triangles.end(), uint32_t{0});

    // Count triangles
    katana::do_all(
        katana::iterate(*graph),
        [&](const Node& n) {
          OrderedCountFunc(*graph, n, &per_node_triangles);
        },
        katana::chunk_size<kChunkSize>(), katana::steal(),
        katana::loopname("TriangleCount_OrderedCountAlgo"));

    katana::do_all(
        katana::iterate(*graph),
        [&](Node n) {
          auto degree = graph->degree(n);

          graph->template GetData<NodeClusteringCoefficient>(n) =
              static_cast<double>(2 * per_node_triangles[n]) /
              (degree * (degree - 1));
        },
        katana::no_stats());

    return;
  }

  katana::Result<void> operator()(SortedGraphView* graph) {
    katana::StatTimer execTime(
        "LocalClusteringCoefficient", "LocalClusteringCoefficient");
    execTime.start();

    // Compute the clustering coefficient of each
    // node based on the triangles.
    ComputeLocalClusteringCoefficient(graph);

    execTime.stop();
    return katana::ResultSuccess();
  }
};

struct LocalClusteringCoefficientPerThread {
  using TriangleCountVec = katana::NUMAArray<uint32_t>;
  using IterPair =
      std::pair<TriangleCountVec::iterator, TriangleCountVec::iterator>;
  TriangleCountVec node_triangle_count_;

  /**
 * Counts the number of triangles for each node
 * in the graph using a per-thread implementation.
 *
 * Uses simple 3-level nested algorithm to find
 * triangles. It assumes that edgelist of each node
 * is sorted.
 */
  void OrderedCountFunc(
      const SortedGraphView& graph, Node n, IterPair per_thread_count_range) {
    // TODO(amber): replace with NodeIteratingAlgo for triangle counting
    for (auto edges_n : graph.edges(n)) {
      auto v = graph.edge_dest(edges_n);
      if (v > n) {
        break;
      }
      auto e_it_n = graph.edges(n).begin();

      for (auto edges_v : graph.edges(v)) {
        auto dst_v = graph.edge_dest(edges_v);
        if (dst_v > v) {
          break;
        }
        while (graph.edge_dest(*e_it_n) < dst_v) {
          e_it_n++;
        }
        if (dst_v == graph.edge_dest(*e_it_n)) {
          *(per_thread_count_range.first + n) += 1;
          *(per_thread_count_range.first + v) += 1;
          *(per_thread_count_range.first + dst_v) += 1;
        }
      }
    }
  }

  /*
 * Simple counting loop, instead of binary searching.
 * It assumes that edgelist of each node is sorted.
 * This uses a PerThreadStorage implementation.
 */
  void OrderedCountAlgo(const SortedGraphView& graph) {
    const uint64_t num_nodes = graph.size();
    const uint32_t num_threads = katana::getActiveThreads();

    // allocate num_nodes * num_threads long array and divide it among threads
    TriangleCountVec all_thread_count_vec;
    all_thread_count_vec.allocateBlocked(num_nodes * num_threads);
    katana::ParallelSTL::fill(
        all_thread_count_vec.begin(), all_thread_count_vec.end(), uint32_t{0});

    katana::PerThreadStorage<IterPair> per_thread_node_triangle_count;

    katana::on_each([&](const unsigned tid, const unsigned numT) {
      *per_thread_node_triangle_count.getLocal() = katana::block_range(
          all_thread_count_vec.begin(), all_thread_count_vec.end(), tid, numT);
    });

    katana::do_all(
        katana::iterate(graph),
        [&](const Node& n) {
          OrderedCountFunc(
              graph, n, *per_thread_node_triangle_count.getLocal());
        },
        katana::chunk_size<kChunkSize>(), katana::steal(),
        katana::loopname("TriangleCount_OrderedCountAlgo"));

    katana::do_all(
        katana::iterate(graph),
        [&](const Node& n) {
          node_triangle_count_[n] = 0;
          for (uint32_t i = 0; i < num_threads; i++) {
            auto my_count_range = *per_thread_node_triangle_count.getRemote(i);

            node_triangle_count_[n] += *(my_count_range.first + n);
          }
        },
        katana::chunk_size<kChunkSize>(), katana::steal(),
        katana::loopname("TriangleCount_Reduce"));
  }

  void ComputeLocalClusteringCoefficient(SortedGraphView* graph) {
    katana::do_all(katana::iterate(*graph), [&](Node n) {
      auto degree = graph->degree(n);
      if (degree > 1) {
        graph->template GetData<NodeClusteringCoefficient>(n) =
            static_cast<double>(2 * node_triangle_count_[n]) /
            (degree * (degree - 1));
      } else {
        graph->template GetData<NodeClusteringCoefficient>(n) = 0.0;
      }
    });

    return;
  }

  katana::Result<void> operator()(SortedGraphView* graph) {
    katana::StatTimer execTime(
        "LocalClusteringCoefficient", "LocalClusteringCoefficient");
    execTime.start();

    node_triangle_count_.allocateBlocked(graph->num_nodes());

    // Calculate the number of triangles
    // on each node
    OrderedCountAlgo(*graph);

    // Compute the clustering coefficient of each
    // node based on the triangles.
    ComputeLocalClusteringCoefficient(graph);

    execTime.stop();
    return katana::ResultSuccess();
  }
};
}  // namespace

template <typename Algorithm>
katana::Result<void>
LocalClusteringCoefficientWithWrap(
    katana::PropertyGraph* pg, const std::string& output_property_name,
    tsuba::TxnContext* txn_ctx) {
  if (auto result = katana::analytics::ConstructNodeProperties<NodeData>(
          pg, txn_ctx, {output_property_name});
      !result) {
    return result.error();
  }
  auto sorted_view =
      KATANA_CHECKED(SortedGraphView::Make(pg, {output_property_name}, {}));

  Algorithm algo;
  return algo(&sorted_view);
}

katana::Result<void>
katana::analytics::LocalClusteringCoefficient(
    katana::PropertyGraph* pg, const std::string& output_property_name,
    tsuba::TxnContext* txn_ctx, LocalClusteringCoefficientPlan plan) {
  katana::StatTimer timer_graph_read(
      "GraphReadingTime", "LocalClusteringCoefficient");
  katana::StatTimer timer_auto_algo(
      "AutoRelabel", "LocalClusteringCoefficient");

  [[maybe_unused]] bool relabel;
  timer_graph_read.start();
  switch (plan.relabeling()) {
  case LocalClusteringCoefficientPlan::kNoRelabel:
    relabel = false;
    break;
  case LocalClusteringCoefficientPlan::kRelabel:
    relabel = true;
    break;
  case LocalClusteringCoefficientPlan::kAutoRelabel:
    timer_auto_algo.start();
    relabel = IsApproximateDegreeDistributionPowerLaw(*pg);
    timer_auto_algo.stop();
    break;
  default:
    return katana::ErrorCode::AssertionFailed;
  }

  // TODO(amber): For now, we create a sorted view (in
  // LocalClusteringCoefficientWithWrap) unconditionally. With current triangle
  // counting algorithm, relabelling is not expected to help, but it will once we
  // switch to NodeIteratingAlgo, at which point, change the
  // SortedPropertyGraphView to
  // PropertyGraphViews::NodesSortedByDegreeEdgesSortedByDestID
#if 0
  if (relabel) {
    katana::StatTimer timer_relabel(
        "GraphRelabelTimer", "LocalClusteringCoefficient");
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
#endif

  timer_graph_read.stop();

  katana::EnsurePreallocated(1, 16 * (pg->num_nodes() + pg->num_edges()));

  switch (plan.algorithm()) {
  case LocalClusteringCoefficientPlan::kOrderedCountAtomics: {
    return LocalClusteringCoefficientWithWrap<
        LocalClusteringCoefficientAtomics>(pg, output_property_name, txn_ctx);
  }
  case LocalClusteringCoefficientPlan::kOrderedCountPerThread: {
    return LocalClusteringCoefficientWithWrap<
        LocalClusteringCoefficientPerThread>(pg, output_property_name, txn_ctx);
  }
  default:
    return katana::ErrorCode::InvalidArgument;
  }
}
