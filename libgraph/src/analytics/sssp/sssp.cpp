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

#include "katana/analytics/sssp/sssp.h"

#include "katana/Reduction.h"
#include "katana/Statistics.h"
#include "katana/TypedPropertyGraph.h"
#include "katana/analytics/BfsSsspImplementationBase.h"
#include "katana/gstl.h"

using namespace katana::analytics;

namespace {

template <typename Weight>
struct SsspNodeDistance : public katana::AtomicPODProperty<Weight> {};

template <typename Weight>
using SsspEdgeWeight = katana::PODProperty<Weight>;

template <typename Weight>
struct SsspImplementation : public katana::analytics::BfsSsspImplementationBase<
                                katana::TypedPropertyGraph<
                                    std::tuple<SsspNodeDistance<Weight>>,
                                    std::tuple<SsspEdgeWeight<Weight>>>,
                                Weight, true> {
  using NodeDistance = SsspNodeDistance<Weight>;
  using EdgeWeight = SsspEdgeWeight<Weight>;

  using NodeData = typename std::tuple<NodeDistance>;
  using EdgeData = typename std::tuple<EdgeWeight>;
  using Graph = katana::TypedPropertyGraph<NodeData, EdgeData>;

  using Base =
      katana::analytics::BfsSsspImplementationBase<Graph, Weight, true>;

  using Dist = typename Base::Dist;
  using UpdateRequest = typename Base::UpdateRequest;
  using UpdateRequestIndexer = typename Base::UpdateRequestIndexer;
  using SrcEdgeTile = typename Base::SrcEdgeTile;
  using SrcEdgeTileMaker = typename Base::SrcEdgeTileMaker;
  using SrcEdgeTilePushWrap = typename Base::SrcEdgeTilePushWrap;
  using ReqPushWrap = typename Base::ReqPushWrap;
  using OutEdgeRangeFn = typename Base::OutEdgeRangeFn;
  using TileRangeFn = typename Base::TileRangeFn;

  static constexpr bool kTrackWork = Base::kTrackWork;
  static constexpr unsigned kChunkSize = 64;
  static constexpr Dist kDistanceInfinity = Base::kDistanceInfinity;

  using PSchunk = katana::PerSocketChunkFIFO<kChunkSize>;
  using OBIM = katana::OrderedByIntegerMetric<UpdateRequestIndexer, PSchunk>;
  using OBIMBarrier = typename katana::OrderedByIntegerMetric<
      UpdateRequestIndexer, PSchunk>::template with_barrier<true>::type;

  template <typename T, typename OBIMTy = OBIM, typename P, typename R>
  static void DeltaStepAlgo(
      katana::NUMAArray<std::atomic<Weight>>* node_data,
      katana::NUMAArray<Weight>* edge_data, Graph* graph,
      const typename Graph::Node& source, const P& pushWrap, const R& edgeRange,
      unsigned stepShift) {
    //! [reducible for self-defined stats]
    katana::GAccumulator<size_t> BadWork;
    //! [reducible for self-defined stats]
    katana::GAccumulator<size_t> WLEmptyWork;

    katana::InsertBag<T> init_bag;
    pushWrap(init_bag, source, 0, "parallel");

    katana::for_each(
        katana::iterate(init_bag),
        [&](const T& item, auto& ctx) {
          const auto& sdata = (*node_data)[item.src];

          if (sdata < item.dist) {
            if (kTrackWork) {
              WLEmptyWork += 1;
            }
            return;
          }

          for (auto ii : edgeRange(item)) {
            auto dest = graph->GetEdgeDest(ii);
            auto& ddist = (*node_data)[*dest];
            Dist ew = (*edge_data)[ii];
            Dist new_dist = sdata + ew;
            Dist old_dist = katana::atomicMin(ddist, new_dist);
            if (new_dist < old_dist) {
              if (kTrackWork) {
                //! [per-thread contribution of self-defined stats]
                if (old_dist != kDistanceInfinity) {
                  BadWork += 1;
                }
                //! [per-thread contribution of self-defined stats]
              }
              pushWrap(ctx, *dest, new_dist);
            }
          }
        },
        katana::wl<OBIMTy>(UpdateRequestIndexer{stepShift}),
        katana::disable_conflict_detection(), katana::loopname("SSSP"));

    if (kTrackWork) {
      //! [report self-defined stats]
      katana::ReportStatSingle("SSSP", "BadWork", BadWork.reduce());
      //! [report self-defined stats]
      katana::ReportStatSingle("SSSP", "WLEmptyWork", WLEmptyWork.reduce());
    }
  }

  static void DeltaStepFusionAlgo(
      katana::NUMAArray<std::atomic<Weight>>* node_data,
      katana::NUMAArray<Weight>* edge_data, Graph* graph,
      const typename Graph::Node& source, unsigned stepShift) {
    constexpr size_t kMaxFusion = 1000;

    using Node = typename Graph::Node;
    using Bucket = katana::gstl::Vector<Node>;
    using Buckets = katana::gstl::Vector<Bucket>;

    katana::PerThreadStorage<Buckets> buckets;

    auto relax = [&](Node n, Dist sdist, Buckets& b) {
      for (auto ii : graph->edges(n)) {
        auto dest = graph->GetEdgeDest(ii);
        auto& ddist = (*node_data)[*dest];
        Dist ew = (*edge_data)[ii];
        const Dist new_dist = sdist + ew;

        Dist old_dist = katana::atomicMin(ddist, new_dist);
        if (new_dist < old_dist) {
          size_t idx = new_dist / (1 << stepShift);
          if (idx >= b.size()) {
            b.resize(idx + 1);
          }
          b[idx].push_back(*dest);
        }
      }
    };

    katana::GAccumulator<size_t> fused_rounds;

    katana::InsertBag<Node> wl;
    wl.push_back(source);

    size_t cur_bucket = 0;

    for (size_t rounds = 1; true; ++rounds) {
      Dist cur_dist = cur_bucket * (1 << stepShift);
      katana::do_all(
          katana::iterate(wl),
          [&](const Node& n) {
            Dist sdist = (*node_data)[n];
            if (sdist >= cur_dist) {
              relax(n, sdist, *buckets.getLocal());
            }
          },
          katana::wl<PSchunk>, katana::steal());

      katana::GReduceMin<size_t> least_bucket;

      katana::on_each([&](unsigned, unsigned) {
        Buckets& b = *buckets.getLocal();

        while (cur_bucket < b.size() && !b[cur_bucket].empty() &&
               b[cur_bucket].size() < kMaxFusion) {
          fused_rounds.update(1);
          Bucket cur;
          std::swap(b[cur_bucket], cur);
          for (Node n : cur) {
            Dist sdist = (*node_data)[n];
            relax(n, sdist, b);
          }
        }

        for (size_t idx = cur_bucket; idx < b.size(); ++idx) {
          if (b[idx].empty()) {
            continue;
          }
          least_bucket.update(idx);
          break;
        }
      });

      wl.clear();

      cur_bucket = least_bucket.reduce();
      if (cur_bucket == std::numeric_limits<size_t>::max()) {
        katana::ReportStatSingle("SSSP", "rounds", rounds);
        katana::ReportStatSingle("SSSP", "fused rounds", fused_rounds.reduce());
        break;
      }

      katana::on_each([&](unsigned, unsigned) {
        Buckets& b = *buckets.getLocal();
        if (cur_bucket >= b.size()) {
          return;
        }
        if (b[cur_bucket].empty()) {
          return;
        }
        for (Node n : b[cur_bucket]) {
          wl.push(n);
        }
        b[cur_bucket].clear();
        b[cur_bucket].shrink_to_fit();
      });
    }
  }

  template <typename T, typename P, typename R>
  static void SerDeltaAlgo(
      Graph* graph, const typename Graph::Node& source, const P& pushWrap,
      const R& edgeRange, unsigned stepShift) {
    SerialBucketWL<T, UpdateRequestIndexer> wl(UpdateRequestIndexer{stepShift});

    graph->template GetData<NodeDistance>(source) = 0;

    pushWrap(wl, source, 0);

    size_t iter = 0UL;
    while (!wl.empty()) {
      auto& curr = wl.minBucket();

      while (!curr.empty()) {
        ++iter;
        auto item = curr.front();
        curr.pop_front();

        if (graph->template GetData<NodeDistance>(item.src) < item.dist) {
          // empty work
          continue;
        }

        for (auto e : edgeRange(item)) {
          auto dest = graph->GetEdgeDest(e);
          auto& ddata = graph->template GetData<NodeDistance>(dest);

          const auto new_dist =
              item.dist + graph->template GetEdgeData<EdgeWeight>(e);

          if (new_dist < ddata) {
            ddata = new_dist;
            pushWrap(wl, *dest, new_dist);
          }
        }
      }

      wl.goToNextBucket();
    }

    if (!wl.allEmpty()) {
      std::abort();
    }
    katana::ReportStatSingle("SSSP-Serial-Delta", "Iterations", iter);
  }

  template <typename T, typename P, typename R>
  static void DijkstraAlgo(
      Graph* graph, const typename Graph::Node& source, const P& pushWrap,
      const R& edgeRange) {
    using WL = katana::MinHeap<T>;

    graph->template GetData<NodeDistance>(source) = 0;

    WL wl;
    pushWrap(wl, source, 0);

    size_t iter = 0;

    while (!wl.empty()) {
      ++iter;

      T item = wl.pop();

      if (graph->template GetData<NodeDistance>(item.src) < item.dist) {
        // empty work
        continue;
      }

      for (auto e : edgeRange(item)) {
        auto dest = graph->GetEdgeDest(e);
        auto& ddata = graph->template GetData<NodeDistance>(dest);

        const auto new_dist =
            item.dist + graph->template GetEdgeData<EdgeWeight>(e);

        if (new_dist < ddata) {
          ddata = new_dist;
          pushWrap(wl, *dest, new_dist);
        }
      }
    }

    katana::ReportStatSingle("SSSP-Dijkstra", "Iterations", iter);
  }

  static void TopoAlgo(Graph* graph, const typename Graph::Node& source) {
    katana::NUMAArray<Dist> old_dist;
    old_dist.allocateInterleaved(graph->size());

    katana::do_all(
        katana::iterate(size_t{0}, graph->size()),
        [&](size_t i) { old_dist.constructAt(i, kDistanceInfinity); },
        katana::no_stats(), katana::loopname("initDistArray"));

    graph->template GetData<NodeDistance>(source) = 0;

    katana::GReduceLogicalOr changed;
    size_t rounds = 0;

    do {
      ++rounds;
      changed.reset();

      katana::do_all(
          katana::iterate(*graph),
          [&](const typename Graph::Node& n) {
            const auto& sdata = graph->template GetData<NodeDistance>(n);

            if (old_dist[n] > sdata) {
              old_dist[n] = sdata;
              changed.update(true);

              for (auto e : graph->edges(n)) {
                const Weight new_dist =
                    sdata + graph->template GetEdgeData<EdgeWeight>(e);
                auto dest = graph->GetEdgeDest(e);
                auto& ddata = graph->template GetData<NodeDistance>(dest);
                katana::atomicMin(ddata, new_dist);
              }
            }
          },
          katana::steal(), katana::loopname("Update"));

    } while (changed.reduce());

    katana::ReportStatSingle("SSSP-Topo", "rounds", rounds);
  }

  void TopoTileAlgo(Graph* graph, const typename Graph::Node& source) {
    katana::InsertBag<SrcEdgeTile> tiles;

    graph->template GetData<NodeDistance>(source) = 0;

    katana::do_all(
        katana::iterate(*graph),
        [&](const typename Graph::Node& n) {
          Base::PushEdgeTiles(
              tiles, graph, n, SrcEdgeTileMaker{n, kDistanceInfinity});
        },
        katana::steal(), katana::loopname("MakeTiles"));

    katana::GReduceLogicalOr changed;
    size_t rounds = 0;

    do {
      ++rounds;
      changed.reset();

      katana::do_all(
          katana::iterate(tiles),
          [&](SrcEdgeTile& t) {
            const auto& sdata = graph->template GetData<NodeDistance>(t.src);

            if (t.dist > sdata) {
              t.dist = sdata;
              changed.update(true);

              for (auto e = t.beg; e != t.end; ++e) {
                const Weight new_dist =
                    sdata + graph->template GetEdgeData<EdgeWeight>(e);
                auto dest = graph->GetEdgeDest(e);
                auto& ddata = graph->template GetData<NodeDistance>(dest);
                katana::atomicMin(ddata, new_dist);
              }
            }
          },
          katana::steal(), katana::loopname("Update"));

    } while (changed.reduce());

    katana::ReportStatSingle("SSSP-Topo", "rounds", rounds);
  }

public:
  katana::Result<void> SSSP(Graph& graph, size_t start_node, SsspPlan plan) {
    if (start_node >= graph.size()) {
      return katana::ErrorCode::InvalidArgument;
    }

    auto it = graph.begin();
    std::advance(it, start_node);
    typename Graph::Node source = *it;

    size_t approxNodeData = graph.size() * 64;
    katana::EnsurePreallocated(1, approxNodeData);
    katana::ReportPageAllocGuard page_alloc;

    katana::NUMAArray<std::atomic<Weight>> node_data;
    katana::NUMAArray<Weight> edge_data;
    bool use_block = false;
    if (use_block) {
      node_data.allocateBlocked(graph.size());
      edge_data.allocateBlocked(graph.num_edges());
    } else {
      node_data.allocateInterleaved(graph.size());
      edge_data.allocateInterleaved(graph.num_edges());
    }

    katana::do_all(katana::iterate(graph), [&](const typename Graph::Node& n) {
      graph.template GetData<NodeDistance>(n) = kDistanceInfinity;
      node_data[n] = kDistanceInfinity;
      for (auto e : graph.edges(n)) {
        edge_data[e] = graph.template GetEdgeData<EdgeWeight>(e);
      }
    });

    graph.template GetData<NodeDistance>(source) = 0;
    node_data[source] = 0;

    katana::StatTimer execTime("SSSP");
    execTime.start();

    if (plan.algorithm() == SsspPlan::kAutomatic) {
      plan = SsspPlan(&graph.GetPropertyGraph());
    }

    switch (plan.algorithm()) {
    case SsspPlan::kDeltaTile:
      DeltaStepAlgo<SrcEdgeTile>(
          &node_data, &edge_data, &graph, source,
          SrcEdgeTilePushWrap{&graph, *this}, TileRangeFn(), plan.delta());
      break;
    case SsspPlan::kDeltaStep:
      DeltaStepAlgo<UpdateRequest>(
          &node_data, &edge_data, &graph, source, ReqPushWrap(),
          OutEdgeRangeFn{&graph}, plan.delta());
      break;
    case SsspPlan::kDeltaStepBarrier:
      DeltaStepAlgo<UpdateRequest, OBIMBarrier>(
          &node_data, &edge_data, &graph, source, ReqPushWrap(),
          OutEdgeRangeFn{&graph}, plan.delta());
      break;
    case SsspPlan::kDeltaStepFusion:
      DeltaStepFusionAlgo(&node_data, &edge_data, &graph, source, plan.delta());
      break;
    case SsspPlan::kSerialDeltaTile:
      SerDeltaAlgo<SrcEdgeTile>(
          &graph, source, SrcEdgeTilePushWrap{&graph, *this}, TileRangeFn(),
          plan.delta());
      break;
    case SsspPlan::kSerialDelta:
      SerDeltaAlgo<UpdateRequest>(
          &graph, source, ReqPushWrap(), OutEdgeRangeFn{&graph}, plan.delta());
      break;
    case SsspPlan::kDijkstraTile:
      DijkstraAlgo<SrcEdgeTile>(
          &graph, source, SrcEdgeTilePushWrap{&graph, *this}, TileRangeFn());
      break;
    case SsspPlan::kDijkstra:
      DijkstraAlgo<UpdateRequest>(
          &graph, source, ReqPushWrap(), OutEdgeRangeFn{&graph});
      break;
    case SsspPlan::kTopological:
      TopoAlgo(&graph, source);
      break;
    case SsspPlan::kTopologicalTile:
      TopoTileAlgo(&graph, source);
      break;
    default:
      return katana::ErrorCode::InvalidArgument;
    }

    execTime.stop();

    katana::do_all(katana::iterate(graph), [&](const typename Graph::Node& n) {
      graph.template GetData<NodeDistance>(n) = node_data[n].load();
    });

    return katana::ResultSuccess();
  }
};

template <typename Weight>
katana::Result<void>
Sssp(
    katana::TypedPropertyGraph<
        std::tuple<SsspNodeDistance<Weight>>,
        std::tuple<SsspEdgeWeight<Weight>>>& pg,
    size_t start_node, SsspPlan plan) {
  static_assert(std::is_integral_v<Weight> || std::is_floating_point_v<Weight>);
  SsspImplementation<Weight> impl{{plan.edge_tile_size()}};
  return impl.SSSP(pg, start_node, plan);
}

template <typename Weight>
static katana::Result<void>
SSSPWithWrap(
    tsuba::TxnContext* txn_ctx, katana::PropertyGraph* pg, size_t start_node,
    const std::string& edge_weight_property_name,
    const std::string& output_property_name, SsspPlan plan) {
  if (auto r = ConstructNodeProperties<std::tuple<SsspNodeDistance<Weight>>>(
          txn_ctx, pg, {output_property_name});
      !r) {
    return r.error();
  }
  auto graph = katana::TypedPropertyGraph<
      std::tuple<SsspNodeDistance<Weight>>,
      std::tuple<SsspEdgeWeight<Weight>>>::
      Make(pg, {output_property_name}, {edge_weight_property_name});
  if (!graph && graph.error() == katana::ErrorCode::TypeError) {
    KATANA_LOG_DEBUG(
        "Incorrect edge property type: {}",
        KATANA_CHECKED(pg->GetEdgeProperty(edge_weight_property_name))
            ->type()
            ->ToString());
  }
  if (!graph) {
    return graph.error();
  }

  return Sssp(graph.value(), start_node, plan);
}

}  // namespace

katana::Result<void>
katana::analytics::Sssp(
    tsuba::TxnContext* txn_ctx, PropertyGraph* pg, size_t start_node,
    const std::string& edge_weight_property_name,
    const std::string& output_property_name, SsspPlan plan) {
  switch (KATANA_CHECKED(pg->GetEdgeProperty(edge_weight_property_name))
              ->type()
              ->id()) {
  case arrow::UInt32Type::type_id:
    return SSSPWithWrap<uint32_t>(
        txn_ctx, pg, start_node, edge_weight_property_name,
        output_property_name, plan);
  case arrow::Int32Type::type_id:
    return SSSPWithWrap<int32_t>(
        txn_ctx, pg, start_node, edge_weight_property_name,
        output_property_name, plan);
  case arrow::UInt64Type::type_id:
    return SSSPWithWrap<uint64_t>(
        txn_ctx, pg, start_node, edge_weight_property_name,
        output_property_name, plan);
  case arrow::Int64Type::type_id:
    return SSSPWithWrap<int64_t>(
        txn_ctx, pg, start_node, edge_weight_property_name,
        output_property_name, plan);
  case arrow::FloatType::type_id:
    return SSSPWithWrap<float>(
        txn_ctx, pg, start_node, edge_weight_property_name,
        output_property_name, plan);
  case arrow::DoubleType::type_id:
    return SSSPWithWrap<double>(
        txn_ctx, pg, start_node, edge_weight_property_name,
        output_property_name, plan);
  default:
    return KATANA_ERROR(
        katana::ErrorCode::TypeError, "Unsupported type: {}",
        KATANA_CHECKED(pg->GetEdgeProperty(edge_weight_property_name))
            ->type()
            ->ToString());
  }
}

namespace {

template <typename Weight>
static katana::Result<void>
SsspValidateImpl(
    katana::PropertyGraph* pg, size_t start_node,
    const std::string& edge_weight_property_name,
    const std::string& output_property_name) {
  using Impl = SsspImplementation<Weight>;
  auto pg_result = Impl::Graph::Make(
      pg, {output_property_name}, {edge_weight_property_name});
  if (!pg_result) {
    return pg_result.error();
  }

  typename Impl::Graph graph = pg_result.value();

  if (graph.template GetData<SsspNodeDistance<Weight>>(start_node) != 0) {
    return katana::ErrorCode::AssertionFailed;
  }

  std::atomic<bool> not_consistent(false);
  do_all(
      iterate(graph), typename Impl::template NotConsistent<
                          SsspNodeDistance<Weight>, typename Impl::EdgeWeight>(
                          &graph, not_consistent));

  if (not_consistent) {
    return katana::ErrorCode::AssertionFailed;
  }

  return katana::ResultSuccess();
}

}  // namespace

katana::Result<void>
katana::analytics::SsspAssertValid(
    katana::PropertyGraph* pg, size_t start_node,
    const std::string& edge_weight_property_name,
    const std::string& output_property_name) {
  switch (
      KATANA_CHECKED(pg->GetNodeProperty(output_property_name))->type()->id()) {
  case arrow::UInt32Type::type_id:
    return SsspValidateImpl<uint32_t>(
        pg, start_node, edge_weight_property_name, output_property_name);
  case arrow::Int32Type::type_id:
    return SsspValidateImpl<int32_t>(
        pg, start_node, edge_weight_property_name, output_property_name);
  case arrow::UInt64Type::type_id:
    return SsspValidateImpl<uint64_t>(
        pg, start_node, edge_weight_property_name, output_property_name);
  case arrow::Int64Type::type_id:
    return SsspValidateImpl<int64_t>(
        pg, start_node, edge_weight_property_name, output_property_name);
  case arrow::FloatType::type_id:
    return SsspValidateImpl<float>(
        pg, start_node, edge_weight_property_name, output_property_name);
  case arrow::DoubleType::type_id:
    return SsspValidateImpl<double>(
        pg, start_node, edge_weight_property_name, output_property_name);
  default:
    return KATANA_ERROR(
        katana::ErrorCode::TypeError, "Unsupported type: {}",
        KATANA_CHECKED(pg->GetEdgeProperty(edge_weight_property_name))
            ->type()
            ->ToString());
  }
}

namespace {

template <typename Weight>
static katana::Result<SsspStatistics>
ComputeStatistics(
    katana::PropertyGraph* pg, const std::string& output_property_name) {
  auto pg_result = katana::TypedPropertyGraph<
      typename SsspImplementation<Weight>::NodeData,
      std::tuple<>>::Make(pg, {output_property_name}, {});
  if (!pg_result) {
    return pg_result.error();
  }

  auto graph = pg_result.value();

  katana::GReduceMax<Weight> max_dist;
  katana::GAccumulator<Weight> sum_dist;
  katana::GAccumulator<uint64_t> num_visited;

  do_all(
      katana::iterate(graph),
      [&](uint64_t i) {
        Weight my_distance =
            graph.template GetData<SsspNodeDistance<Weight>>(i);

        if (my_distance < SsspImplementation<Weight>::kDistanceInfinity) {
          max_dist.update(my_distance);
          sum_dist += my_distance;
          num_visited += 1;
        }
      },
      katana::loopname("Compute Statistics"), katana::no_stats());

  uint64_t total_visited_nodes = num_visited.reduce();
  double average_dist = double(sum_dist.reduce()) / total_visited_nodes;
  return SsspStatistics{
      total_visited_nodes, double(max_dist.reduce()), average_dist};
}

}  // namespace

katana::Result<SsspStatistics>
SsspStatistics::Compute(
    PropertyGraph* pg, const std::string& output_property_name) {
  switch (
      KATANA_CHECKED(pg->GetNodeProperty(output_property_name))->type()->id()) {
  case arrow::UInt32Type::type_id:
    return ComputeStatistics<uint32_t>(pg, output_property_name);
  case arrow::Int32Type::type_id:
    return ComputeStatistics<int32_t>(pg, output_property_name);
  case arrow::UInt64Type::type_id:
    return ComputeStatistics<uint64_t>(pg, output_property_name);
  case arrow::Int64Type::type_id:
    return ComputeStatistics<int64_t>(pg, output_property_name);
  case arrow::FloatType::type_id:
    return ComputeStatistics<float>(pg, output_property_name);
  case arrow::DoubleType::type_id:
    return ComputeStatistics<double>(pg, output_property_name);
  default:
    return KATANA_ERROR(
        katana::ErrorCode::TypeError, "Unsupported type: {}",
        KATANA_CHECKED(pg->GetEdgeProperty(output_property_name))
            ->type()
            ->ToString());
  }
}

void
SsspStatistics::Print(std::ostream& os) const {
  os << "Number of reached nodes = " << n_reached_nodes << std::endl;
  os << "Maximum distance = " << max_distance << std::endl;
  os << "Average distance = " << average_visited_distance << std::endl;
}
