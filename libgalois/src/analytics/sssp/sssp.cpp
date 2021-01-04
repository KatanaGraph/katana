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

#include "galois/analytics/sssp/sssp.h"

// Implementation

namespace galois::analytics {

template <typename Weight>
struct SsspImplementation : public galois::analytics::BfsSsspImplementationBase<
                                graphs::PropertyGraph<
                                    std::tuple<SsspNodeDistance<Weight>>,
                                    std::tuple<SsspEdgeWeight<Weight>>>,
                                Weight, true> {
  using NodeDistance = SsspNodeDistance<Weight>;
  using EdgeWeight = SsspEdgeWeight<Weight>;

  using NodeData = typename std::tuple<NodeDistance>;
  using EdgeData = typename std::tuple<EdgeWeight>;
  using Graph = graphs::PropertyGraph<NodeData, EdgeData>;

  using Base =
      galois::analytics::BfsSsspImplementationBase<Graph, Weight, true>;

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

  using PSchunk = galois::worklists::PerSocketChunkFIFO<kChunkSize>;
  using OBIM =
      galois::worklists::OrderedByIntegerMetric<UpdateRequestIndexer, PSchunk>;
  using OBIMBarrier = typename galois::worklists::OrderedByIntegerMetric<
      UpdateRequestIndexer, PSchunk>::template with_barrier<true>::type;

  template <typename T, typename OBIMTy = OBIM, typename P, typename R>
  static void DeltaStepAlgo(
      Graph* graph, const typename Graph::Node& source, const P& pushWrap,
      const R& edgeRange, unsigned stepShift) {
    //! [reducible for self-defined stats]
    galois::GAccumulator<size_t> BadWork;
    //! [reducible for self-defined stats]
    galois::GAccumulator<size_t> WLEmptyWork;

    graph->template GetData<NodeDistance>(source) = 0;

    galois::InsertBag<T> init_bag;
    pushWrap(init_bag, source, 0, "parallel");

    galois::for_each(
        galois::iterate(init_bag),
        [&](const T& item, auto& ctx) {
          const auto& sdata = graph->template GetData<NodeDistance>(item.src);

          if (sdata < item.dist) {
            if (kTrackWork)
              WLEmptyWork += 1;
            return;
          }

          for (auto ii : edgeRange(item)) {
            auto dest = graph->GetEdgeDest(ii);
            auto& ddist = graph->template GetData<NodeDistance>(dest);
            Dist ew = graph->template GetEdgeData<EdgeWeight>(ii);
            const Dist new_dist = sdata + ew;
            Dist old_dist = galois::atomicMin(ddist, new_dist);
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
        galois::wl<OBIMTy>(UpdateRequestIndexer{stepShift}),
        galois::disable_conflict_detection(), galois::loopname("SSSP"));

    if (kTrackWork) {
      //! [report self-defined stats]
      galois::ReportStatSingle("SSSP", "BadWork", BadWork.reduce());
      //! [report self-defined stats]
      galois::ReportStatSingle("SSSP", "WLEmptyWork", WLEmptyWork.reduce());
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
    galois::ReportStatSingle("SSSP-Serial-Delta", "Iterations", iter);
  }

  template <typename T, typename P, typename R>
  static void DijkstraAlgo(
      Graph* graph, const typename Graph::Node& source, const P& pushWrap,
      const R& edgeRange) {
    using WL = galois::MinHeap<T>;

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

    galois::ReportStatSingle("SSSP-Dijkstra", "Iterations", iter);
  }

  static void TopoAlgo(Graph* graph, const typename Graph::Node& source) {
    galois::LargeArray<Dist> old_dist;
    old_dist.allocateInterleaved(graph->size());

    galois::do_all(
        galois::iterate(size_t{0}, graph->size()),
        [&](size_t i) { old_dist.constructAt(i, kDistanceInfinity); },
        galois::no_stats(), galois::loopname("initDistArray"));

    graph->template GetData<NodeDistance>(source) = 0;

    galois::GReduceLogicalOr changed;
    size_t rounds = 0;

    do {
      ++rounds;
      changed.reset();

      galois::do_all(
          galois::iterate(*graph),
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
                galois::atomicMin(ddata, new_dist);
              }
            }
          },
          galois::steal(), galois::loopname("Update"));

    } while (changed.reduce());

    galois::ReportStatSingle("SSSP-Topo", "rounds", rounds);
  }

  void TopoTileAlgo(Graph* graph, const typename Graph::Node& source) {
    galois::InsertBag<SrcEdgeTile> tiles;

    graph->template GetData<NodeDistance>(source) = 0;

    galois::do_all(
        galois::iterate(*graph),
        [&](const typename Graph::Node& n) {
          Base::PushEdgeTiles(
              tiles, graph, n, SrcEdgeTileMaker{n, kDistanceInfinity});
        },
        galois::steal(), galois::loopname("MakeTiles"));

    galois::GReduceLogicalOr changed;
    size_t rounds = 0;

    do {
      ++rounds;
      changed.reset();

      galois::do_all(
          galois::iterate(tiles),
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
                galois::atomicMin(ddata, new_dist);
              }
            }
          },
          galois::steal(), galois::loopname("Update"));

    } while (changed.reduce());

    galois::ReportStatSingle("SSSP-Topo", "rounds", rounds);
  }

public:
  galois::Result<void> SSSP(Graph& graph, size_t start_node, SsspPlan plan) {
    if (start_node >= graph.size()) {
      return galois::ErrorCode::InvalidArgument;
    }

    auto it = graph.begin();
    std::advance(it, start_node);
    typename Graph::Node source = *it;

    size_t approxNodeData = graph.size() * 64;
    galois::Prealloc(1, approxNodeData);

    galois::do_all(
        galois::iterate(graph), [&graph](const typename Graph::Node& n) {
          graph.template GetData<NodeDistance>(n) = kDistanceInfinity;
        });

    graph.template GetData<NodeDistance>(source) = 0;

    galois::StatTimer execTime("SSSP");
    execTime.start();

    if (plan.algorithm() == SsspPlan::kAutomatic) {
      plan = SsspPlan::Automatic(&graph.GetPropertyFileGraph());
    }

    switch (plan.algorithm()) {
    case SsspPlan::kDeltaTile:
      DeltaStepAlgo<SrcEdgeTile>(
          &graph, source, SrcEdgeTilePushWrap{&graph, *this}, TileRangeFn(),
          plan.delta());
      break;
    case SsspPlan::kDeltaStep:
      DeltaStepAlgo<UpdateRequest>(
          &graph, source, ReqPushWrap(), OutEdgeRangeFn{&graph}, plan.delta());
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
    case SsspPlan::kTopo:
      TopoAlgo(&graph, source);
      break;
    case SsspPlan::kTopoTile:
      TopoTileAlgo(&graph, source);
      break;
    case SsspPlan::kDeltaStepBarrier:
      DeltaStepAlgo<UpdateRequest, OBIMBarrier>(
          &graph, source, ReqPushWrap(), OutEdgeRangeFn{&graph}, plan.delta());
      break;
    default:
      return galois::ErrorCode::InvalidArgument;
    }

    execTime.stop();

    return galois::ResultSuccess();
  }
};

template <typename Weight>
Result<void>
Sssp(
    graphs::PropertyGraph<
        std::tuple<SsspNodeDistance<Weight>>,
        std::tuple<SsspEdgeWeight<Weight>>>& pg,
    size_t start_node, SsspPlan plan) {
  static_assert(std::is_integral_v<Weight> || std::is_floating_point_v<Weight>);
  galois::analytics::SsspImplementation<Weight> impl{{plan.edge_tile_size()}};
  return impl.SSSP(pg, start_node, plan);
}

}  // namespace galois::analytics

using namespace galois::analytics;

template <typename Weight>
static galois::Result<void>
SSSPWithWrap(
    galois::graphs::PropertyFileGraph* pfg, size_t start_node,
    std::string edge_weight_property_name, std::string output_property_name,
    SsspPlan plan) {
  if (auto r = ConstructNodeProperties<std::tuple<SsspNodeDistance<Weight>>>(
          pfg, {output_property_name});
      !r) {
    return r.error();
  }
  auto graph = galois::graphs::PropertyGraph<
      std::tuple<SsspNodeDistance<Weight>>,
      std::tuple<SsspEdgeWeight<Weight>>>::
      Make(pfg, {output_property_name}, {edge_weight_property_name});
  if (!graph && graph.error() == galois::ErrorCode::TypeError) {
    GALOIS_LOG_DEBUG(
        "Incorrect edge property type: {}",
        pfg->edge_table()
            ->GetColumnByName(edge_weight_property_name)
            ->type()
            ->ToString());
  }
  if (!graph) {
    return graph.error();
  }

  return galois::analytics::Sssp(graph.value(), start_node, plan);
}

galois::Result<void>
galois::analytics::Sssp(
    graphs::PropertyFileGraph* pfg, size_t start_node,
    const std::string& edge_weight_property_name,
    const std::string& output_property_name, SsspPlan plan) {
  switch (pfg->EdgeProperty(edge_weight_property_name)->type()->id()) {
  case arrow::UInt32Type::type_id:
    return SSSPWithWrap<uint32_t>(
        pfg, start_node, edge_weight_property_name, output_property_name, plan);
  case arrow::Int32Type::type_id:
    return SSSPWithWrap<int32_t>(
        pfg, start_node, edge_weight_property_name, output_property_name, plan);
  case arrow::UInt64Type::type_id:
    return SSSPWithWrap<uint64_t>(
        pfg, start_node, edge_weight_property_name, output_property_name, plan);
  case arrow::Int64Type::type_id:
    return SSSPWithWrap<int64_t>(
        pfg, start_node, edge_weight_property_name, output_property_name, plan);
  case arrow::FloatType::type_id:
    return SSSPWithWrap<float>(
        pfg, start_node, edge_weight_property_name, output_property_name, plan);
  case arrow::DoubleType::type_id:
    return SSSPWithWrap<double>(
        pfg, start_node, edge_weight_property_name, output_property_name, plan);
  default:
    return galois::ErrorCode::TypeError;
  }
}

template <typename Weight>
static galois::Result<bool>
SsspValidateImpl(
    galois::graphs::PropertyFileGraph* pfg, size_t start_node,
    const std::string& edge_weight_property_name,
    const std::string& output_property_name) {
  using Impl = SsspImplementation<Weight>;
  auto pg_result = Impl::Graph::Make(
      pfg, {output_property_name}, {edge_weight_property_name});
  if (!pg_result) {
    return pg_result.error();
  }

  typename Impl::Graph graph = pg_result.value();

  if (graph.template GetData<SsspNodeDistance<Weight>>(start_node) != 0) {
    return false;
  }

  std::atomic<bool> not_consistent(false);
  do_all(
      iterate(graph), typename Impl::template NotConsistent<
                          SsspNodeDistance<Weight>, typename Impl::EdgeWeight>(
                          &graph, not_consistent));

  if (not_consistent) {
    return false;
  }

  return true;
}

galois::Result<bool>
galois::analytics::SsspValidate(
    galois::graphs::PropertyFileGraph* pfg, size_t start_node,
    const std::string& edge_weight_property_name,
    const std::string& output_property_name) {
  switch (pfg->NodeProperty(output_property_name)->type()->id()) {
  case arrow::UInt32Type::type_id:
    return SsspValidateImpl<uint32_t>(
        pfg, start_node, edge_weight_property_name, output_property_name);
  case arrow::Int32Type::type_id:
    return SsspValidateImpl<int32_t>(
        pfg, start_node, edge_weight_property_name, output_property_name);
  case arrow::UInt64Type::type_id:
    return SsspValidateImpl<uint64_t>(
        pfg, start_node, edge_weight_property_name, output_property_name);
  case arrow::Int64Type::type_id:
    return SsspValidateImpl<int64_t>(
        pfg, start_node, edge_weight_property_name, output_property_name);
  case arrow::FloatType::type_id:
    return SsspValidateImpl<float>(
        pfg, start_node, edge_weight_property_name, output_property_name);
  case arrow::DoubleType::type_id:
    return SsspValidateImpl<double>(
        pfg, start_node, edge_weight_property_name, output_property_name);
  default:
    return galois::ErrorCode::TypeError;
  }
}

template <typename Weight>
static galois::Result<SsspStatistics>
ComputeStatistics(
    galois::graphs::PropertyFileGraph* pfg,
    const std::string& output_property_name) {
  auto pg_result = galois::graphs::PropertyGraph<
      typename SsspImplementation<Weight>::NodeData,
      std::tuple<>>::Make(pfg, {output_property_name}, {});
  if (!pg_result) {
    return pg_result.error();
  }

  auto graph = pg_result.value();

  galois::GReduceMax<Weight> max_dist;
  galois::GAccumulator<Weight> sum_dist;
  galois::GAccumulator<uint32_t> num_visited;
  max_dist.reset();
  sum_dist.reset();
  num_visited.reset();

  do_all(
      galois::iterate(graph),
      [&](uint64_t i) {
        uint32_t my_distance =
            graph.template GetData<SsspNodeDistance<Weight>>(i);

        if (my_distance != SsspImplementation<Weight>::kDistanceInfinity) {
          max_dist.update(my_distance);
          sum_dist += my_distance;
          num_visited += 1;
        }
      },
      galois::loopname("Sanity check"), galois::no_stats());

  return SsspStatistics{
      double(max_dist.reduce()), double(sum_dist.reduce()),
      num_visited.reduce()};
}

galois::Result<SsspStatistics>
SsspStatistics::Compute(
    graphs::PropertyFileGraph* pfg, const std::string& output_property_name) {
  switch (pfg->NodeProperty(output_property_name)->type()->id()) {
  case arrow::UInt32Type::type_id:
    return ComputeStatistics<uint32_t>(pfg, output_property_name);
  case arrow::Int32Type::type_id:
    return ComputeStatistics<int32_t>(pfg, output_property_name);
  case arrow::UInt64Type::type_id:
    return ComputeStatistics<uint64_t>(pfg, output_property_name);
  case arrow::Int64Type::type_id:
    return ComputeStatistics<int64_t>(pfg, output_property_name);
  case arrow::FloatType::type_id:
    return ComputeStatistics<float>(pfg, output_property_name);
  case arrow::DoubleType::type_id:
    return ComputeStatistics<double>(pfg, output_property_name);
  default:
    return galois::ErrorCode::TypeError;
  }
}

void
SsspStatistics::Print(std::ostream& os) {
  os << "Number of reached nodes = " << n_reached_nodes << std::endl;
  os << "Maximum distance = " << max_distance << std::endl;
  os << "Sum of distances = " << total_distance << std::endl;
  os << "Average distance = " << average_distance() << std::endl;
}
