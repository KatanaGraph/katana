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

struct LocalClusteringCoefficientAtomics {
  struct NodeTriangleCount {
    using ArrowType = arrow::CTypeTraits<uint64_t>::ArrowType;
    using ViewType = katana::PODPropertyView<std::atomic<uint64_t>>;
  };

  struct NodeClusteringCoefficient : public katana::PODProperty<double> {};

  using NodeData =
      typename std::tuple<NodeTriangleCount, NodeClusteringCoefficient>;
  using EdgeData = typename std::tuple<>;

  using Graph = katana::TypedPropertyGraph<NodeData, EdgeData>;

  using Node = Graph::Node;

  /**
 * Counts the number of triangles for each node
 * in the graph using atomics.
 *
 * Uses simple 3-level nested algorithm to find
 * triangles. It assumes that edgelist of each node
 * is sorted.
 */
  void OrderedCountFunc(Graph* graph, Node n) {
    for (auto it_v : graph->edges(n)) {
      auto v = *graph->GetEdgeDest(it_v);
      if (v > n) {
        break;
      }
      Graph::edge_iterator it_n = graph->edges(n).begin();

      for (auto it_vv : graph->edges(v)) {
        auto vv = *graph->GetEdgeDest(it_vv);
        if (vv > v) {
          break;
        }
        while (*graph->GetEdgeDest(it_n) < vv) {
          it_n++;
        }
        if (vv == *graph->GetEdgeDest(it_n)) {
          katana::atomicAdd<uint64_t>(
              graph->GetData<NodeTriangleCount>(n), (uint64_t)1);
          katana::atomicAdd<uint64_t>(
              graph->GetData<NodeTriangleCount>(v), (uint64_t)1);
          katana::atomicAdd<uint64_t>(
              graph->GetData<NodeTriangleCount>(vv), (uint64_t)1);
        }
      }
    }
  }

  /*
 * Simple counting loop, instead of binary searching.
 * It assumes that edgelist of each node is sorted.
 * This uses an atomic implementation.
 */
  void OrderedCountAlgo(Graph* graph) {
    katana::do_all(
        katana::iterate(*graph),
        [&](const Node& n) { OrderedCountFunc(graph, n); },
        katana::chunk_size<kChunkSize>(), katana::steal(), katana::no_stats(),
        katana::loopname("TriangleCount_OrderedCountAlgo"));
  }

  void ComputeLocalClusteringCoefficient(Graph* graph) {
    katana::do_all(katana::iterate(*graph), [&](Node n) {
      auto degree =
          std::distance(graph->edges(n).begin(), graph->edges(n).end());
      graph->template GetData<NodeClusteringCoefficient>(n) =
          ((double)(2 * graph->template GetData<NodeTriangleCount>(n))) /
          (degree * (degree - 1));
    });

    return;
  }

  katana::Result<void> operator()(
      katana::PropertyGraph* pg, const std::string& output_property_name) {
    katana::analytics::TemporaryPropertyGuard temporary_property{pg};

    if (auto result = katana::analytics::ConstructNodeProperties<NodeData>(
            pg, {output_property_name, temporary_property.name()});
        !result) {
      return result.error();
    }

    auto graph_result =
        Graph::Make(pg, {output_property_name, temporary_property.name()}, {});
    if (!graph_result) {
      return graph_result.error();
    }

    Graph graph = graph_result.value();

    katana::StatTimer execTime(
        "LocalClusteringCoefficient", "LocalClusteringCoefficient");
    execTime.start();

    // Calculate the number of triangles
    // on each node
    OrderedCountAlgo(&graph);

    // Compute the clustering coefficient of each
    // node based on the triangles.
    ComputeLocalClusteringCoefficient(&graph);

    execTime.stop();
    return katana::ResultSuccess();
  }
};

struct LocalClusteringCoefficientPerThread {
  struct NodeClusteringCoefficient : public katana::PODProperty<double> {};

  using NodeData = typename std::tuple<NodeClusteringCoefficient>;
  using EdgeData = typename std::tuple<>;

  typedef katana::TypedPropertyGraph<NodeData, EdgeData> Graph;

  typedef typename Graph::Node Node;

  katana::LargeArray<uint64_t> node_triangle_count_;

  /**
 * Counts the number of triangles for each node
 * in the graph using a per-thread implementation.
 *
 * Uses simple 3-level nested algorithm to find
 * triangles. It assumes that edgelist of each node
 * is sorted.
 */
  void OrderedCountFunc(
      Graph* graph, Node n, std::vector<uint64_t>* node_triangle_count) {
    for (auto it_v : graph->edges(n)) {
      auto v = *graph->GetEdgeDest(it_v);
      if (v > n) {
        break;
      }
      Graph::edge_iterator it_n = graph->edges(n).begin();

      for (auto it_vv : graph->edges(v)) {
        auto vv = *graph->GetEdgeDest(it_vv);
        if (vv > v) {
          break;
        }
        while (*graph->GetEdgeDest(it_n) < vv) {
          it_n++;
        }
        if (vv == *graph->GetEdgeDest(it_n)) {
          (*node_triangle_count)[n] += 1;
          (*node_triangle_count)[v] += 1;
          (*node_triangle_count)[vv] += 1;
        }
      }
    }
  }

  /*
 * Simple counting loop, instead of binary searching.
 * It assumes that edgelist of each node is sorted.
 * This uses a PerThreadStorage implementation.
 */
  void OrderedCountAlgo(Graph* graph) {
    katana::PerThreadStorage<std::vector<uint64_t>>
        per_thread_node_triangle_count;
    uint64_t num_nodes = graph->size();
    uint32_t num_threads = katana::getActiveThreads();

    katana::do_all(
        katana::iterate((uint32_t)0, num_threads), [&](uint32_t tid) {
          per_thread_node_triangle_count.getRemote(tid)->resize(num_nodes, 0);
        });

    katana::do_all(
        katana::iterate(*graph),
        [&](const Node& n) {
          OrderedCountFunc(
              graph, n, &(*per_thread_node_triangle_count.getLocal()));
        },
        katana::chunk_size<kChunkSize>(), katana::steal(),
        katana::loopname("TriangleCount_OrderedCountAlgo"));

    katana::do_all(
        katana::iterate(*graph),
        [&](const Node& n) {
          node_triangle_count_[n] = 0;
          for (uint32_t i = 0; i < num_threads; i++) {
            node_triangle_count_[n] +=
                (*(per_thread_node_triangle_count.getRemote(i)))[n];
          }
        },
        katana::chunk_size<kChunkSize>(), katana::steal(), katana::no_stats(),
        katana::loopname("TriangleCount_Reduce"));
  }

  void ComputeLocalClusteringCoefficient(Graph* graph) {
    katana::do_all(katana::iterate(*graph), [&](Node n) {
      auto degree =
          std::distance(graph->edges(n).begin(), graph->edges(n).end());
      if (degree > 1) {
        graph->template GetData<NodeClusteringCoefficient>(n) =
            ((double)(2 * node_triangle_count_[n])) / (degree * (degree - 1));
      } else {
        graph->template GetData<NodeClusteringCoefficient>(n) = 0.0;
      }
    });

    return;
  }

  katana::Result<void> operator()(
      katana::PropertyGraph* pg, const std::string& output_property_name) {
    if (auto result = katana::analytics::ConstructNodeProperties<NodeData>(
            pg, {output_property_name});
        !result) {
      return result.error();
    }

    auto graph_result = Graph::Make(pg, {output_property_name}, {});
    if (!graph_result) {
      return graph_result.error();
    }

    Graph graph = graph_result.value();

    katana::StatTimer execTime(
        "LocalClusteringCoefficient", "LocalClusteringCoefficient");
    execTime.start();

    node_triangle_count_.allocateBlocked(graph.size());

    // Calculate the number of triangles
    // on each node
    OrderedCountAlgo(&graph);

    // Compute the clustering coefficient of each
    // node based on the triangles.
    ComputeLocalClusteringCoefficient(&graph);

    node_triangle_count_.destroy();
    node_triangle_count_.deallocate();

    execTime.stop();
    return katana::ResultSuccess();
  }
};
}  // namespace

template <typename Algorithm>
katana::Result<void>
LocalClusteringCoefficientWithWrap(
    katana::PropertyGraph* pg, const std::string& output_property_name) {
  Algorithm algo;

  return algo(pg, output_property_name);
}

katana::Result<void>
katana::analytics::LocalClusteringCoefficient(
    katana::PropertyGraph* pg, const std::string& output_property_name,
    LocalClusteringCoefficientPlan plan) {
  katana::StatTimer timer_graph_read(
      "GraphReadingTime", "LocalClusteringCoefficient");
  katana::StatTimer timer_auto_algo(
      "AutoRelabel", "LocalClusteringCoefficient");

  bool relabel;
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

  timer_graph_read.stop();

  katana::Prealloc(1, 16 * (pg->num_nodes() + pg->num_edges()));

  switch (plan.algorithm()) {
  case LocalClusteringCoefficientPlan::kOrderedCountAtomics: {
    LocalClusteringCoefficientAtomics algo;
    return algo(pg, output_property_name);
  }
  case LocalClusteringCoefficientPlan::kOrderedCountPerThread: {
    LocalClusteringCoefficientPerThread algo_per_thread;
    return algo_per_thread(pg, output_property_name);
  }
  default:
    return katana::ErrorCode::InvalidArgument;
  }
}
