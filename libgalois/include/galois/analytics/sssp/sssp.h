#ifndef GALOIS_LIBGALOIS_GALOIS_ANALYTICS_SSSP_SSSP_H_
#define GALOIS_LIBGALOIS_GALOIS_ANALYTICS_SSSP_SSSP_H_

#include <galois/analytics/Plan.h>

#include "galois/AtomicHelpers.h"
#include "galois/analytics/BfsSsspImplementationBase.h"
#include "galois/analytics/Utils.h"

// API

namespace galois::analytics {

/// A computational plan to for SSSP, specifying the algorithm and any
/// parameters associated with it.
class SsspPlan : Plan {
public:
  /// Algorithm selectors for Single-Source Shortest Path
  enum Algorithm {
    kDeltaTile,
    kDeltaStep,
    kDeltaStepBarrier,
    kSerialDeltaTile,  // TODO: Do we want to expose these at all?
    kSerialDelta,
    kDijkstraTile,
    kDijkstra,
    kTopo,
    kTopoTile,
    kAutomatic,
  };

  // Don't allow people to directly construct these, so as to have only one
  // consistent way to configure.
private:
  Algorithm algorithm_;
  unsigned delta_;
  ptrdiff_t edge_tile_size_;
  // TODO: should chunk_size be in the plan? Or fixed?
  //  It cannot be in the plan currently because it is a template parameter and
  //  cannot be easily changed since the value is statically passed on to
  //  FixedSizeRing.
  //unsigned chunk_size_ = 64;

  SsspPlan(
      Architecture architecture, Algorithm algorithm, unsigned delta,
      ptrdiff_t edge_tile_size)
      : Plan(architecture),
        algorithm_(algorithm),
        delta_(delta),
        edge_tile_size_(edge_tile_size) {}

public:
  Algorithm algorithm() const { return algorithm_; }
  unsigned delta() const { return delta_; }
  ptrdiff_t edge_tile_size() const { return edge_tile_size_; }

  static SsspPlan DeltaTile(
      unsigned delta = 13, ptrdiff_t edge_tile_size = 512) {
    return {kCPU, kDeltaTile, delta, edge_tile_size};
  }

  static SsspPlan DeltaStep(unsigned delta = 13) {
    return {kCPU, kDeltaStep, delta, 0};
  }

  static SsspPlan DeltaStepBarrier(ptrdiff_t edge_tile_size = 512) {
    return {kCPU, kDeltaStepBarrier, 0, edge_tile_size};
  }

  static SsspPlan SerialDeltaTile(
      unsigned delta = 13, ptrdiff_t edge_tile_size = 512) {
    return {kCPU, kSerialDeltaTile, delta, edge_tile_size};
  }

  static SsspPlan SerialDelta(unsigned delta = 13) {
    return {kCPU, kSerialDelta, delta, 0};
  }

  static SsspPlan DijkstraTile(ptrdiff_t edge_tile_size = 512) {
    return {kCPU, kDijkstraTile, 0, edge_tile_size};
  }

  static SsspPlan Dijkstra() { return {kCPU, kDijkstra, 0, 0}; }

  // TODO: Should this be "Topological"
  static SsspPlan Topo() { return {kCPU, kTopo, 0, 0}; }

  static SsspPlan TopoTile(ptrdiff_t edge_tile_size = 512) {
    return {kCPU, kTopoTile, 0, edge_tile_size};
  }

  static SsspPlan Automatic() { return {kCPU, kAutomatic, 0, 0}; }

  static SsspPlan Automatic(const galois::graphs::PropertyFileGraph* pfg) {
    // TODO: What to do about const cast? We know we don't modify pfg, but there
    //  is no way to construct a const PropertyGraph.
    auto graph =
        galois::graphs::PropertyGraph<std::tuple<>, std::tuple<>>::Make(
            const_cast<galois::graphs::PropertyFileGraph*>(pfg), {}, {});
    if (!graph)
      GALOIS_LOG_FATAL(
          "PropertyGraph should always be constructable here: {}",
          graph.error());
    galois::StatTimer autoAlgoTimer("SSSP_Automatic_Algorithm_Selection");
    autoAlgoTimer.start();
    bool isPowerLaw = isApproximateDegreeDistributionPowerLaw(graph.value());
    autoAlgoTimer.stop();
    if (isPowerLaw) {
      return DeltaStep();
    } else {
      return DeltaStepBarrier();
    }
  }
};

template <typename Weight>
struct SsspNodeDistance {
  using ArrowType = typename arrow::CTypeTraits<Weight>::ArrowType;
  using ViewType = galois::PODPropertyView<std::atomic<Weight>>;
};

template <typename Weight>
using SsspEdgeWeight = galois::PODProperty<Weight>;

/// Compute the Single-Source Shortest Path for pfg starting from start_node.
/// The edge weights are taken from the property named
/// edge_weight_property_name (which may be a 32- or 64-bit sign or unsigned
/// int), and the computed path lengths are stored in the property named
/// output_property_name (as uint32_t). The algorithm and delta stepping
/// parameter can be specified, but have reasonable defaults.
/// The property named output_property_name is created by this function and may
/// not exist before the call.
GALOIS_EXPORT Result<void> Sssp(
    graphs::PropertyFileGraph* pfg, size_t start_node,
    std::string edge_weight_property_name, std::string output_property_name,
    SsspPlan plan = SsspPlan::Automatic());

/// Compute the Single-Source Shortest Path for pg start from start_node.
/// The edge weights are the edge data and the computed path lengths are stored
/// in the edge data. The algorithm and delta stepping parameter can be
/// specified, but have reasonable defaults.
/// Weight must be an integral type.
template <typename Weight>
GALOIS_EXPORT Result<void> Sssp(
    graphs::PropertyGraph<
        std::tuple<SsspNodeDistance<Weight>>,
        std::tuple<SsspEdgeWeight<Weight>>>& pg,
    size_t start_node, SsspPlan plan = SsspPlan::Automatic());

}  // namespace galois::analytics

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

// extern template references for instantiations available in libgalois
/// \cond DO_NOT_DOCUMENT
extern template galois::Result<void> galois::analytics::Sssp<uint32_t>(
    galois::graphs::PropertyGraph<
        std::tuple<SsspNodeDistance<uint32_t>>,
        std::tuple<SsspEdgeWeight<uint32_t>>>& pg,
    size_t start_node, SsspPlan plan);
extern template galois::Result<void> galois::analytics::Sssp<int32_t>(
    galois::graphs::PropertyGraph<
        std::tuple<SsspNodeDistance<int32_t>>,
        std::tuple<SsspEdgeWeight<int32_t>>>& pg,
    size_t start_node, SsspPlan plan);
extern template galois::Result<void> galois::analytics::Sssp<uint64_t>(
    galois::graphs::PropertyGraph<
        std::tuple<SsspNodeDistance<uint64_t>>,
        std::tuple<SsspEdgeWeight<uint64_t>>>& pg,
    size_t start_node, SsspPlan plan);
extern template galois::Result<void> galois::analytics::Sssp<int64_t>(
    galois::graphs::PropertyGraph<
        std::tuple<SsspNodeDistance<int64_t>>,
        std::tuple<SsspEdgeWeight<int64_t>>>& pg,
    size_t start_node, SsspPlan plan);
extern template galois::Result<void> galois::analytics::Sssp<float>(
    galois::graphs::PropertyGraph<
        std::tuple<SsspNodeDistance<float>>, std::tuple<SsspEdgeWeight<float>>>&
        pg,
    size_t start_node, SsspPlan plan);
extern template galois::Result<void> galois::analytics::Sssp<double>(
    galois::graphs::PropertyGraph<
        std::tuple<SsspNodeDistance<double>>,
        std::tuple<SsspEdgeWeight<double>>>& pg,
    size_t start_node, SsspPlan plan);
/// \endcond DO_NOT_DOCUMENT

#endif
