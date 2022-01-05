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

#include "katana/analytics/k_shortest_paths/sssp.h"

#include "katana/AtomicHelpers.h"
#include "katana/Reduction.h"
#include "katana/Statistics.h"
#include "katana/TypedPropertyGraph.h"
#include "katana/analytics/BfsSsspImplementationBase.h"
#include "katana/analytics/kSsspImplementationBase.h"
#include "katana/gstl.h"

using namespace katana::analytics;

namespace {

struct Path {
  uint32_t parent;
  const Path* last{nullptr};
};

struct NodeCount : public katana::AtomicPODProperty<uint32_t> {};

struct NodeMax : public katana::AtomicPODProperty<uint32_t> {};

using EdgeWeight = katana::UInt32Property;

template <typename Weight>
struct SsspImplementation
    : public katana::analytics::KSsspImplementationBase<
          katana::TypedPropertyGraph<
              std::tuple<NodeCount, NodeMax>, std::tuple<EdgeWeight>>,
          Weight, const Path, true> {
  using NodeData = std::tuple<NodeCount, NodeMax>;
  using EdgeData = std::tuple<EdgeWeight>;

  typedef katana::TypedPropertyGraph<NodeData, EdgeData> Graph;
  typedef typename Graph::Node GNode;

  constexpr static const bool kTrackWork = false;
  constexpr static const unsigned kChunkSize = 64U;
  constexpr static const ptrdiff_t kEdgeTileSize = 512;

  using SSSP = katana::analytics::KSsspImplementationBase<
      Graph, Weight, const Path, true>;

  using Distance = uint32_t;
  using SSSPUpdateRequest = typename SSSP::UpdateRequest;
  using SSSPUpdateRequestIndexer = typename SSSP::UpdateRequestIndexer;
  using SSSPSrcEdgeTile = typename SSSP::SrcEdgeTile;
  using SSSPSrcEdgeTileMaker = typename SSSP::SrcEdgeTileMaker;
  using SSSPSrcEdgeTilePushWrap = typename SSSP::SrcEdgeTilePushWrap;
  using SSSPReqPushWrap = typename SSSP::ReqPushWrap;
  using SSSPOutEdgeRangeFn = typename SSSP::OutEdgeRangeFn;
  using SSSPTileRangeFn = typename SSSP::TileRangeFn;

  using PSchunk = katana::PerSocketChunkFIFO<kChunkSize>;
  using OBIM =
      katana::OrderedByIntegerMetric<SSSPUpdateRequestIndexer, PSchunk>;
  using OBIM_Barrier = typename katana::OrderedByIntegerMetric<
      SSSPUpdateRequestIndexer, PSchunk>::template with_barrier<true>::type;

  using BFS = BfsSsspImplementationBase<Graph, unsigned int, false>;
  using BFSUpdateRequest = typename BFS::UpdateRequest;
  using BFSSrcEdgeTile = typename BFS::SrcEdgeTile;
  using BFSSrcEdgeTileMaker = typename BFS::SrcEdgeTileMaker;
  using BFSSrcEdgeTilePushWrap = typename BFS::SrcEdgeTilePushWrap;
  using BFSReqPushWrap = typename BFS::ReqPushWrap;
  using BFSOutEdgeRangeFn = typename BFS::OutEdgeRangeFn;
  using BFSTileRangeFn = typename BFS::TileRangeFn;

  class PathAlloc {
  public:
    Path* NewPath() {
      Path* path = allocator_.allocate(1);
      allocator_.construct(path, Path());
      return path;
    }

    void DeletePath(Path* path) {
      allocator_.destroy(path);
      allocator_.deallocate(path, 1);
    }

  private:
    katana::FixedSizeAllocator<Path> allocator_;
  };

  template <typename Item, typename PushWrap, typename EdgeRange>
  bool CheckReachabilityAsync(
      Graph* graph, const GNode& source, const PushWrap& pushWrap,
      const EdgeRange& edgeRange, unsigned int reportNode) {
    using FIFO = katana::PerSocketChunkFIFO<kChunkSize>;
    using WL = FIFO;

    using Loop = katana::ForEach;

    Loop loop;

    graph->GetData<NodeCount>(source) = 1;
    katana::InsertBag<Item> initBag;

    pushWrap(initBag, source, 1, "parallel");

    loop(
        katana::iterate(initBag),
        [&](const Item& item, auto& ctx) {
          for (auto ii : edgeRange(item)) {
            GNode dst = graph->OutEdgeDst(ii);
            if (graph->GetData<NodeCount>(dst) == 0) {
              graph->GetData<NodeCount>(dst) = 1;
              pushWrap(ctx, dst, 1);
            }
          }
        },
        katana::wl<WL>(), katana::loopname("runBFS"),
        katana::disable_conflict_detection());

    if (graph->GetData<NodeCount>(reportNode) == 0) {
      return false;
    }

    katana::do_all(katana::iterate(*graph), [&graph](GNode n) {
      graph->GetData<NodeCount>(n) = 0;
    });

    return true;
  }

  bool CheckReachabilitySync(
      Graph* graph, const GNode& source, unsigned int reportNode) {
    katana::InsertBag<GNode> current_bag;
    katana::InsertBag<GNode> next_bag;

    current_bag.push(source);
    graph->GetData<NodeCount>(source) = 1;

    while (current_bag.begin() != current_bag.end()) {
      katana::do_all(
          katana::iterate(current_bag),
          [&](GNode n) {
            for (auto edge : graph->OutEdges(n)) {
              auto dest = graph->OutEdgeDst(edge);
              if (graph->GetData<NodeCount>(dest) == 0) {
                graph->GetData<NodeCount>(dest) = 1;
                next_bag.push(dest);
              }
            }
          },
          katana::steal());

      current_bag.clear();
      std::swap(current_bag, next_bag);
    }

    if (graph->GetData<NodeCount>(reportNode) == 0) {
      return false;
    }

    katana::do_all(katana::iterate(*graph), [&graph](GNode n) {
      graph->GetData<NodeCount>(n) = 0;
    });

    return true;
  }

  //delta stepping implementation for finding a shortest path from source to report node
  template <
      typename Item, typename OBIMTy, typename PushWrap, typename EdgeRange>
  void DeltaStepAlgo(
      katana::NUMAArray<Weight>* edge_data, Graph* graph, const GNode& source,
      const PushWrap& pushWrap, const EdgeRange& edgeRange,
      katana::InsertBag<std::pair<Weight, Path*>>* report_paths_bag,
      katana::InsertBag<Path*>* path_pointers, PathAlloc& path_alloc,
      unsigned int report_node, unsigned int num_paths,
      unsigned int step_shift) {
    //! [reducible for self-defined stats]
    katana::GAccumulator<size_t> bad_work;
    //! [reducible for self-defined stats]
    katana::GAccumulator<size_t> wl_empty_work;

    graph->template GetData<NodeCount>(source) = 1;

    katana::InsertBag<Item> init_bag;

    Path* path = path_alloc.NewPath();
    path->last = nullptr;
    path->parent = source;

    path_pointers->push(path);

    pushWrap(init_bag, source, 0, path, "parallel");

    katana::for_each(
        katana::iterate(init_bag),
        [&](const Item& item, auto& ctx) {
          for (auto ii : edgeRange(item)) {
            GNode dst = graph->OutEdgeDst(ii);
            auto& ddata_count = graph->template GetData<NodeCount>(dst);
            auto& ddata_max = graph->template GetData<NodeMax>(dst);

            Distance ew = (*edge_data)[ii];
            const Distance new_dist = item.distance + ew;

            if ((ddata_count >= num_paths) && (ddata_max <= new_dist))
              continue;

            Path* path = path_alloc.NewPath();
            path->parent = item.src;
            path->last = item.path;
            path_pointers->push(path);

            if (ddata_count < num_paths) {
              katana::atomicAdd<uint32_t>(ddata_count, (uint32_t)1);
              katana::atomicMax<uint32_t>(ddata_max, new_dist);
            }

            if (dst == report_node) {
              report_paths_bag->push(std::make_pair(new_dist, path));
            }

            //check if this new extended path needs to be added to the worklist
            bool should_add =
                (graph->template GetData<NodeCount>(report_node) < num_paths) ||
                ((graph->template GetData<NodeCount>(report_node) >=
                  num_paths) &&
                 (graph->template GetData<NodeMax>(report_node) > new_dist));

            if (should_add) {
              const Path* const_path = path;
              pushWrap(ctx, dst, new_dist, const_path);
            }
          }
        },
        katana::wl<OBIM>(SSSPUpdateRequestIndexer{step_shift}),
        katana::disable_conflict_detection(), katana::loopname("SSSP"));

    if (kTrackWork) {
      //! [report self-defined stats]
      katana::ReportStatSingle("SSSP", "BadWork", bad_work.reduce());
      //! [report self-defined stats]
      katana::ReportStatSingle("SSSP", "WLEmptyWork", wl_empty_work.reduce());
    }
  }

  void PrintPath(const Path* path) {
    if (path->last != nullptr) {
      PrintPath(path->last);
    }

    katana::gPrint(" ", path->parent);
  }

public:
  katana::Result<void> KSP(
      Graph& graph, unsigned int startNode, unsigned int reportNode,
      AlgoReachability algoReachability, unsigned int numPaths,
      unsigned int stepShift, SsspPlan plan) {
    auto it = graph.begin();
    std::advance(it, startNode);
    GNode source = *it;
    it = graph.begin();
    std::advance(it, reportNode);
    GNode report = *it;

    size_t approxNodeData = graph.size() * 64;
    katana::Prealloc(1, approxNodeData);
    katana::ReportPageAllocGuard page_alloc;

    katana::NUMAArray<Weight> edge_data;
    bool use_block = false;
    if (use_block) {
      edge_data.allocateBlocked(graph.NumEdges());
    } else {
      edge_data.allocateInterleaved(graph.NumEdges());
    }

    katana::do_all(katana::iterate(graph), [&](const GNode& n) {
      graph.template GetData<NodeMax>(n) = 0;
      graph.template GetData<NodeCount>(n) = 0;
      for (auto e : graph.OutEdges(n)) {
        edge_data[e] = graph.template GetEdgeData<EdgeWeight>(e);
      }
    });

    katana::StatTimer execTime("SSSP");
    execTime.start();

    katana::InsertBag<std::pair<Weight, Path*>> paths;
    katana::InsertBag<Path*> path_pointers;

    bool reachable = true;

    switch (algoReachability) {
    case async:
      reachable = CheckReachabilityAsync<BFSUpdateRequest>(
          &graph, source, BFSReqPushWrap(), BFSOutEdgeRangeFn{&graph},
          reportNode);
      break;
    case syncLevel:
      reachable = CheckReachabilitySync(&graph, source, reportNode);
      break;
    default:
      std::abort();
    }

    PathAlloc path_alloc;

    if (reachable) {
      switch (plan.algorithm()) {
      case SsspPlan::kDeltaTile:
        DeltaStepAlgo<SSSPSrcEdgeTile, OBIM>(
            &edge_data, &graph, source, SSSPSrcEdgeTilePushWrap{&graph},
            SSSPTileRangeFn(), &paths, &path_pointers, path_alloc, reportNode,
            numPaths, stepShift);
        break;
      case SsspPlan::kDeltaStep:
        DeltaStepAlgo<SSSPUpdateRequest, OBIM>(
            &edge_data, &graph, source, SSSPReqPushWrap(),
            SSSPOutEdgeRangeFn{&graph}, &paths, &path_pointers, path_alloc,
            reportNode, numPaths, stepShift);
        break;
      case SsspPlan::kDeltaStepBarrier:
        katana::gInfo("Using OBIM with barrier\n");
        DeltaStepAlgo<SSSPUpdateRequest, OBIM_Barrier>(
            &edge_data, &graph, source, SSSPReqPushWrap(),
            SSSPOutEdgeRangeFn{&graph}, &paths, &path_pointers, path_alloc,
            reportNode, numPaths, stepShift);
        break;

      default:
        return katana::ErrorCode::InvalidArgument;
      }
    }

    execTime.stop();
    page_alloc.Report();

    if (reachable) {
      std::multimap<uint32_t, Path*> paths_map;

      for (auto pair : paths) {
        paths_map.insert(std::make_pair(pair.first, pair.second));
      }

      katana::gPrint("Node ", report, " has these k paths:\n");

      uint32_t num =
          (paths_map.size() > numPaths) ? numPaths : paths_map.size();

      auto it_report = paths_map.begin();

      for (uint32_t iter = 0; iter < num; iter++) {
        const Path* path = it_report->second;
        PrintPath(path);
        katana::gPrint(" ", report, "\n");
        katana::gPrint("Weight: ", it_report->first, "\n");
        it_report++;
      }

      katana::do_all(katana::iterate(path_pointers), [&](Path* p) {
        path_alloc.DeletePath(p);
      });
    }

    return katana::ResultSuccess();
  }
};

template <typename Weight>
katana::Result<void>
Ksp(katana::TypedPropertyGraph<
        std::tuple<NodeCount, NodeMax>, std::tuple<EdgeWeight>>& pg,
    unsigned int startNode, unsigned int reportNode,
    AlgoReachability algoReachability, unsigned int numPaths,
    unsigned int stepShift, SsspPlan plan) {
  static_assert(std::is_integral_v<Weight> || std::is_floating_point_v<Weight>);
  SsspImplementation<Weight> impl{{plan.edge_tile_size()}};
  return impl.KSP(
      pg, startNode, reportNode, algoReachability, numPaths, stepShift, plan);
}

template <typename Weight>
static katana::Result<void>
SSSPWithWrap(
    katana::PropertyGraph* pg, unsigned int startNode, unsigned int reportNode,
    katana::TxnContext* txn_ctx, AlgoReachability algoReachability,
    unsigned int numPaths, unsigned int stepShift, SsspPlan plan) {
  auto result =
      ConstructNodeProperties<std::tuple<NodeCount, NodeMax>>(pg, txn_ctx);
  if (!result) {
    return result.error();
  }

  auto graph = katana::TypedPropertyGraph<
      std::tuple<NodeCount, NodeMax>, std::tuple<EdgeWeight>>::Make(pg);

  if (!graph) {
    return graph.error();
  }

  return Ksp<Weight>(
      graph.value(), startNode, reportNode, algoReachability, numPaths,
      stepShift, plan);
}

}  // namespace

katana::Result<void>
katana::analytics::Ksp(
    katana::PropertyGraph* pg, unsigned int startNode, unsigned int reportNode,
    const std::string& edge_weight_property_name, katana::TxnContext* txn_ctx,
    AlgoReachability algoReachability, unsigned int numPaths,
    unsigned int stepShift, SsspPlan plan) {
  switch (KATANA_CHECKED(pg->GetEdgeProperty(edge_weight_property_name))
              ->type()
              ->id()) {
  case arrow::UInt32Type::type_id:
    return SSSPWithWrap<uint32_t>(
        pg, startNode, reportNode, txn_ctx, algoReachability, numPaths,
        stepShift, plan);
  case arrow::Int32Type::type_id:
    return SSSPWithWrap<int32_t>(
        pg, startNode, reportNode, txn_ctx, algoReachability, numPaths,
        stepShift, plan);
  case arrow::UInt64Type::type_id:
    return SSSPWithWrap<uint64_t>(
        pg, startNode, reportNode, txn_ctx, algoReachability, numPaths,
        stepShift, plan);
  case arrow::Int64Type::type_id:
    return SSSPWithWrap<int64_t>(
        pg, startNode, reportNode, txn_ctx, algoReachability, numPaths,
        stepShift, plan);
  case arrow::FloatType::type_id:
    return SSSPWithWrap<float>(
        pg, startNode, reportNode, txn_ctx, algoReachability, numPaths,
        stepShift, plan);
  case arrow::DoubleType::type_id:
    return SSSPWithWrap<double>(
        pg, startNode, reportNode, txn_ctx, algoReachability, numPaths,
        stepShift, plan);
  default:
    return KATANA_ERROR(
        katana::ErrorCode::TypeError, "Unsupported type: {}",
        KATANA_CHECKED(pg->GetEdgeProperty(edge_weight_property_name))
            ->type()
            ->ToString());
  }
}
