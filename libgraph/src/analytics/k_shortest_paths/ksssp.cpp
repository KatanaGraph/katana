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

#include "katana/analytics/k_shortest_paths/ksssp.h"

#include "katana/AtomicHelpers.h"
#include "katana/Reduction.h"
#include "katana/Statistics.h"
#include "katana/TypedPropertyGraph.h"
#include "katana/analytics/BfsSsspImplementationBase.h"
#include "katana/analytics/kSsspImplementationBase.h"
#include "katana/gstl.h"

using namespace katana::analytics;

struct Path {
  uint32_t parent;
  const Path* last{nullptr};
};

struct NodeCount : public katana::AtomicPODProperty<uint32_t> {};

struct NodeMax : public katana::AtomicPODProperty<uint32_t> {};

using EdgeWeight = katana::UInt32Property;

using NodeData = std::tuple<NodeCount, NodeMax>;
using EdgeData = std::tuple<EdgeWeight>;
using Weight = uint32_t;
using Distance = uint32_t;

constexpr static const bool kTrackWork = false;
constexpr static const unsigned kChunkSize = 64U;

using PSchunk = katana::PerSocketChunkFIFO<kChunkSize>;

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

template <
    typename GraphTy, typename Item, typename PushWrap, typename EdgeRange>
bool
CheckReachabilityAsync(
    GraphTy* graph, const typename GraphTy::Node& source,
    const PushWrap& push_wrap, const EdgeRange& edge_range,
    uint32_t report_node) {
  using FIFO = katana::PerSocketChunkFIFO<kChunkSize>;
  using WL = FIFO;

  using Loop = katana::ForEach;

  using GNode = typename GraphTy::Node;

  Loop loop;

  graph->template GetData<NodeCount>(source) = 1;
  katana::InsertBag<Item> initBag;

  push_wrap(initBag, source, 1, "parallel");

  loop(
      katana::iterate(initBag),
      [&](const Item& item, auto& ctx) {
        for (auto ii : edge_range(item)) {
          GNode dst = graph->OutEdgeDst(ii);
          if (graph->template GetData<NodeCount>(dst) == 0) {
            graph->template GetData<NodeCount>(dst) = 1;
            push_wrap(ctx, dst, 1);
          }
        }
      },
      katana::wl<WL>(), katana::loopname("runBFS"),
      katana::disable_conflict_detection());

  if (graph->template GetData<NodeCount>(report_node) == 0) {
    return false;
  }

  katana::do_all(katana::iterate(*graph), [&graph](GNode n) {
    graph->template GetData<NodeCount>(n) = 0;
  });

  return true;
}

template <typename GraphTy>
bool
CheckReachabilitySync(
    GraphTy* graph, const typename GraphTy::Node& source,
    uint32_t report_node) {
  using GNode = typename GraphTy::Node;

  katana::InsertBag<GNode> current_bag;
  katana::InsertBag<GNode> next_bag;

  current_bag.push(source);
  graph->template GetData<NodeCount>(source) = 1;

  while (current_bag.begin() != current_bag.end()) {
    katana::do_all(
        katana::iterate(current_bag),
        [&](GNode n) {
          for (auto edge : graph->OutEdges(n)) {
            auto dest = graph->OutEdgeDst(edge);
            if (graph->template GetData<NodeCount>(dest) == 0) {
              graph->template GetData<NodeCount>(dest) = 1;
              next_bag.push(dest);
            }
          }
        },
        katana::steal());

    current_bag.clear();
    std::swap(current_bag, next_bag);
  }

  if (graph->template GetData<NodeCount>(report_node) == 0) {
    return false;
  }

  katana::do_all(katana::iterate(*graph), [&graph](GNode n) {
    graph->template GetData<NodeCount>(n) = 0;
  });

  return true;
}

//delta stepping implementation for finding a shortest path from source to report node
template <
    typename GraphTy, typename Item, typename OBIMTy, typename PushWrap,
    typename EdgeRange>
void
DeltaStepAlgo(
    GraphTy* graph,
    const typename GraphTy::Node& source, const PushWrap& push_wrap,
    const EdgeRange& edge_range,
    katana::InsertBag<std::pair<Weight, Path*>>* report_paths_bag,
    katana::InsertBag<Path*>* path_pointers, PathAlloc& path_alloc,
    uint32_t report_node, uint32_t num_paths, uint32_t step_shift) {
  using GNode = typename GraphTy::Node;

  using kSSSP = katana::analytics::KSsspImplementationBase<
      GraphTy, Weight, const Path, true>;
  using kSSSPUpdateRequestIndexer = typename kSSSP::UpdateRequestIndexer;

  using OBIM =
      katana::OrderedByIntegerMetric<kSSSPUpdateRequestIndexer, PSchunk>;

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

  push_wrap(init_bag, source, 0, path, "parallel");

  katana::for_each(
      katana::iterate(init_bag),
      [&](const Item& item, auto& ctx) {
        for (auto ii : edge_range(item)) {
          GNode dst = graph->OutEdgeDst(ii);
          auto& ddata_count = graph->template GetData<NodeCount>(dst);
          auto& ddata_max = graph->template GetData<NodeMax>(dst);

          Distance ew = graph->template GetEdgeData<EdgeWeight>(ii);
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
              ((graph->template GetData<NodeCount>(report_node) >= num_paths) &&
               (graph->template GetData<NodeMax>(report_node) > new_dist));

          if (should_add) {
            const Path* const_path = path;
            push_wrap(ctx, dst, new_dist, const_path);
          }
        }
      },
      katana::wl<OBIM>(kSSSPUpdateRequestIndexer{step_shift}),
      katana::disable_conflict_detection(), katana::loopname("kSSSP"));

  if (kTrackWork) {
    //! [report self-defined stats]
    katana::ReportStatSingle("kSSSP", "BadWork", bad_work.reduce());
    //! [report self-defined stats]
    katana::ReportStatSingle("kSSSP", "WLEmptyWork", wl_empty_work.reduce());
  }
}

void
PrintPath(const Path* path) {
  if (path->last != nullptr) {
    PrintPath(path->last);
  }

  katana::gPrint(" ", path->parent);
}

template <typename GraphTy>
katana::Result<void>
KssspImpl(
    GraphTy graph, uint32_t start_node, uint32_t report_node,
    AlgoReachability algo_reachability, uint32_t num_paths, uint32_t step_shift,
    kSsspPlan plan) {
  using GNode = typename GraphTy::Node;

  using kSSSP = katana::analytics::KSsspImplementationBase<
      GraphTy, Weight, const Path, true>;
  using kSSSPUpdateRequest = typename kSSSP::UpdateRequest;
  using kSSSPUpdateRequestIndexer = typename kSSSP::UpdateRequestIndexer;
  using kSSSPSrcEdgeTile = typename kSSSP::SrcEdgeTile;
  using kSSSPSrcEdgeTilePushWrap = typename kSSSP::SrcEdgeTilePushWrap;
  using kSSSPReqPushWrap = typename kSSSP::ReqPushWrap;
  using kSSSPOutEdgeRangeFn = typename kSSSP::OutEdgeRangeFn;
  using kSSSPTileRangeFn = typename kSSSP::TileRangeFn;

  using OBIM =
      katana::OrderedByIntegerMetric<kSSSPUpdateRequestIndexer, PSchunk>;
  using OBIM_Barrier = typename katana::OrderedByIntegerMetric<
      kSSSPUpdateRequestIndexer, PSchunk>::template with_barrier<true>::type;

  using BFS = BfsSsspImplementationBase<GraphTy, unsigned int, false>;
  using BFSUpdateRequest = typename BFS::UpdateRequest;
  using BFSReqPushWrap = typename BFS::ReqPushWrap;
  using BFSOutEdgeRangeFn = typename BFS::OutEdgeRangeFn;

  auto it = graph.begin();
  std::advance(it, start_node.getValue());
  GNode source = *it;
  it = graph.begin();
  std::advance(it, report_node.getValue());
  GNode report = *it;

  size_t approxNodeData = graph.size() * 64;
  katana::Prealloc(1, approxNodeData);
  katana::ReportPageAllocGuard page_alloc;

  katana::do_all(katana::iterate(graph), [&](const GNode& n) {
    graph.template GetData<NodeMax>(n) = 0;
    graph.template GetData<NodeCount>(n) = 0;
  });

  katana::StatTimer execTime("kSSSP");
  execTime.start();

  katana::InsertBag<std::pair<Weight, Path*>> paths;
  katana::InsertBag<Path*> path_pointers;

  bool reachable = true;

  switch (algo_reachability) {
  case async:
    reachable = CheckReachabilityAsync<GraphTy, BFSUpdateRequest>(
        &graph, source, BFSReqPushWrap(), BFSOutEdgeRangeFn{&graph},
        report_node);
    break;
  case syncLevel:
    reachable = CheckReachabilitySync(&graph, source, report_node);
    break;
  default:
    std::abort();
  }

  PathAlloc path_alloc;

  if (reachable) {
    switch (plan.algorithm()) {
    case kSsspPlan::kDeltaTile:
      DeltaStepAlgo<GraphTy, kSSSPSrcEdgeTile, OBIM>(
          &graph, source, kSSSPSrcEdgeTilePushWrap{&graph},
          kSSSPTileRangeFn(), &paths, &path_pointers, path_alloc, report_node,
          num_paths, step_shift);
      break;
    case kSsspPlan::kDeltaStep:
      DeltaStepAlgo<GraphTy, kSSSPUpdateRequest, OBIM>(
          &graph, source, kSSSPReqPushWrap(),
          kSSSPOutEdgeRangeFn{&graph}, &paths, &path_pointers, path_alloc,
          report_node, num_paths, step_shift);
      break;
    case kSsspPlan::kDeltaStepBarrier:
      katana::gInfo("Using OBIM with barrier\n");
      DeltaStepAlgo<GraphTy, kSSSPUpdateRequest, OBIM_Barrier>(
          &graph, source, kSSSPReqPushWrap(),
          kSSSPOutEdgeRangeFn{&graph}, &paths, &path_pointers, path_alloc,
          report_node, num_paths, step_shift);
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
        (paths_map.size() > num_paths) ? num_paths : paths_map.size();

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

katana::Result<void>
katana::analytics::Ksssp(
    katana::PropertyGraph* pg, uint32_t start_node, uint32_t report_node,
    katana::TxnContext* txn_ctx, AlgoReachability algo_reachability,
    uint32_t num_paths, uint32_t step_shift, const bool& is_symmetric,
    kSsspPlan plan) {
  KATANA_CHECKED(ConstructNodeProperties<NodeData>(pg, txn_ctx));

  static_assert(std::is_integral_v<Weight> || std::is_floating_point_v<Weight>);

  if (is_symmetric) {
    using Graph = katana::TypedPropertyGraphView<
        katana::PropertyGraphViews::Default, NodeData, EdgeData>;
    Graph graph = KATANA_CHECKED(Graph::Make(pg));

    return KssspImpl(
        graph, start_node, report_node, algo_reachability, num_paths,
        step_shift, plan);
  } else {
    using Graph = katana::TypedPropertyGraphView<
        katana::PropertyGraphViews::Undirected, NodeData, EdgeData>;

    Graph graph = KATANA_CHECKED(Graph::Make(pg));

    return KssspImpl(
        graph, start_node, report_node, algo_reachability, num_paths,
        step_shift, plan);
  }
}
