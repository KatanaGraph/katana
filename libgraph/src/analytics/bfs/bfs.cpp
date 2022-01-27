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

#include "katana/analytics/bfs/bfs.h"

#include <deque>
#include <type_traits>

#include "katana/DynamicBitset.h"
#include "katana/ErrorCode.h"
#include "katana/Result.h"
#include "katana/Statistics.h"
#include "katana/TypedPropertyGraph.h"
#include "katana/analytics/BfsSsspImplementationBase.h"

using namespace katana::analytics;

namespace {

/// The tag for the output property of BFS in TypedPropertyGraphs.
using BfsNodeDistance = katana::PODProperty<uint32_t>;
using BfsNodeParent = katana::PODProperty<uint32_t>;

struct BfsImplementation
    : BfsSsspImplementationBase<
          katana::TypedPropertyGraph<std::tuple<BfsNodeParent>, std::tuple<>>,
          unsigned int, false> {
  BfsImplementation(ptrdiff_t edge_tile_size)
      : BfsSsspImplementationBase<
            katana::TypedPropertyGraph<std::tuple<BfsNodeParent>, std::tuple<>>,
            unsigned int, false>{edge_tile_size} {}
};

using Graph = BfsImplementation::Graph;
using GNode = Graph::Node;
using Dist = BfsImplementation::Dist;
using BiDirGraphView = katana::TypedPropertyGraphView<
    katana::PropertyGraphViews::BiDirectional, std::tuple<BfsNodeParent>,
    std::tuple<>>;

constexpr unsigned kChunkSize = 256U;

constexpr bool kTrackWork = BfsImplementation::kTrackWork;

using UpdateRequest = BfsImplementation::UpdateRequest;
using ReqPushWrap = BfsImplementation::ReqPushWrap;
using OutEdgeRangeFn = BfsImplementation::OutEdgeRangeFn;

struct EdgeTile {
  Graph::edge_iterator beg;
  Graph::edge_iterator end;
};

struct EdgeTileMaker {
  EdgeTile operator()(
      const Graph::edge_iterator& beg, const Graph::edge_iterator& end) const {
    return EdgeTile{beg, end};
  }
};

struct NodePushWrap {
  template <typename C>
  void operator()(C& cont, const GNode& n, const char* const /*tag*/) const {
    (*this)(cont, n);
  }

  template <typename C>
  void operator()(C& cont, const GNode& n) const {
    cont.push(n);
  }
};

struct EdgeTilePushWrap {
  Graph* graph;
  BfsImplementation& impl;

  template <typename C>
  void operator()(C& cont, const GNode& n, const char* const /*tag*/) const {
    impl.PushEdgeTilesParallel(cont, graph, n, EdgeTileMaker{});
  }

  template <typename C>
  void operator()(C& cont, const GNode& n) const {
    impl.PushEdgeTiles(cont, graph, n, EdgeTileMaker{});
  }
};

struct OneTilePushWrap {
  Graph* graph;

  template <typename C>
  void operator()(C& cont, const GNode& n, const char* const /*tag*/) const {
    (*this)(cont, n);
  }

  template <typename C>
  void operator()(C& cont, const GNode& n) const {
    auto edges = graph->OutEdges(n);
    cont.push(EdgeTile{edges.begin(), edges.end()});
  }
};

template <typename WL>
void
WlToBitset(const WL& wl, katana::DynamicBitset* bitset) {
  katana::do_all(
      katana::iterate(wl), [&](const GNode& src) { bitset->set(src); },
      katana::chunk_size<kChunkSize>(), katana::loopname("WlToBitset"));
}

template <typename G, typename WL>
void
BitsetToWl(const G& graph, const katana::DynamicBitset& bitset, WL* wl) {
  wl->clear();
  katana::do_all(
      katana::iterate(graph),
      [&](const GNode& src) {
        if (bitset.test(src)) {
          wl->push(src);
        }
      },
      katana::chunk_size<kChunkSize>(), katana::loopname("BitsetToWl"));
}

template <typename T, typename P, typename R>
void
AsynchronousAlgo(
    const Graph& graph, const GNode source, katana::NUMAArray<Dist>* node_data,
    const P& pushWrap, const R& edgeRange) {
  namespace gwl = katana;
  using FIFO = gwl::PerSocketChunkFIFO<kChunkSize>;
  using BSWL = gwl::BulkSynchronous<gwl::PerSocketChunkLIFO<kChunkSize>>;
  using WL = FIFO;

  using Loop = katana::ForEach;

  KATANA_GCC7_IGNORE_UNUSED_BUT_SET
  constexpr bool use_CAS = !std::is_same<WL, BSWL>::value;
  KATANA_END_GCC7_IGNORE_UNUSED_BUT_SET

  Loop loop;

  katana::GAccumulator<size_t> bad_work;
  katana::GAccumulator<size_t> WL_empty_work;

  (*node_data)[source] = 0;

  katana::InsertBag<T> init_bag;

  pushWrap(init_bag, source, 1, "parallel");

  loop(
      katana::iterate(init_bag),
      [&](const T& item, auto& ctx) {
        const Dist& sdist = (*node_data)[item.src];

        if (kTrackWork) {
          if (item.dist != sdist) {
            WL_empty_work += 1;
            return;
          }
        }

        const Dist new_dist = item.dist;

        for (auto ii : edgeRange(item)) {
          auto dest = graph.OutEdgeDst(ii);
          Dist& ddata = (*node_data)[dest];

          while (true) {
            Dist old_dist = ddata;
            if (old_dist <= new_dist) {
              break;
            }

            if (!use_CAS ||
                __sync_bool_compare_and_swap(&ddata, old_dist, new_dist)) {
              if (!use_CAS) {
                ddata = new_dist;
              }

              if (kTrackWork) {
                if (old_dist != BfsImplementation::kDistanceInfinity) {
                  bad_work += 1;
                }
              }

              pushWrap(ctx, dest, new_dist + 1);
              break;
            }
          }
        }
      },
      katana::wl<WL>(), katana::loopname("runBFS"),
      katana::disable_conflict_detection());

  if (kTrackWork) {
    katana::ReportStatSingle("BFS", "bad_work", bad_work.reduce());
    katana::ReportStatSingle("BFS", "EmptyWork", WL_empty_work.reduce());
  }
}

template <typename P>
void
SynchronousDirectOpt(
    const BiDirGraphView& bidir_view, katana::NUMAArray<GNode>* node_data,
    const GNode source, const P& pushWrap, const uint32_t alpha,
    const uint32_t beta) {
  using Cont = katana::InsertBag<GNode>;
  using Loop = katana::DoAll;

  katana::GAccumulator<uint32_t> work_items;
  katana::StatTimer bitset_to_wl_timer("Bitset_To_WL_Timer");
  katana::StatTimer wl_to_bitset_timer("WL_To_Bitset_Timer");

  Loop loop;

  katana::DynamicBitset front_bitset;
  katana::DynamicBitset next_bitset;

  uint32_t num_nodes = bidir_view.NumNodes();
  uint64_t num_edges = bidir_view.NumEdges();

  front_bitset.resize(num_nodes);
  next_bitset.resize(num_nodes);

  auto frontier = std::make_unique<Cont>();
  auto next_frontier = std::make_unique<Cont>();

  (*node_data)[source] = source;

  pushWrap(*next_frontier, source, "parallel");

  work_items += 1;

  int64_t edges_to_check = num_edges;
  int64_t scout_count = bidir_view.OutDegree(source);
  uint64_t old_num_work_items{0};

  katana::GAccumulator<uint64_t> writes_pull;
  katana::GAccumulator<uint64_t> writes_push;

  while (!next_frontier->empty()) {
    std::swap(frontier, next_frontier);
    next_frontier->clear();
    if (scout_count > edges_to_check / alpha) {
      wl_to_bitset_timer.start();
      WlToBitset(*frontier, &front_bitset);
      wl_to_bitset_timer.stop();
      do {
        old_num_work_items = work_items.reduce();
        work_items.reset();

        loop(
            katana::iterate(bidir_view),
            [&](const GNode& dst) {
              GNode& ddata = (*node_data)[dst];
              if (ddata == BfsImplementation::kDistanceInfinity) {
                for (auto e : bidir_view.InEdges(dst)) {
                  auto src = bidir_view.InEdgeSrc(e);

                  if (front_bitset.test(src)) {
                    // assign parents on the bfs path.
                    ddata = src;
                    next_bitset.set(dst);
                    work_items += 1;
                    break;
                  }
                }
              }
            },
            katana::steal(), katana::chunk_size<kChunkSize>(),
            katana::loopname(std::string("SyncDO-pull").c_str()));
        std::swap(front_bitset, next_bitset);
        next_bitset.reset();
      } while (work_items.reduce() >= old_num_work_items ||
               (work_items.reduce() > num_nodes / beta));
      bitset_to_wl_timer.start();
      BitsetToWl(bidir_view, front_bitset, next_frontier.get());
      bitset_to_wl_timer.stop();
      scout_count = 1;
    } else {
      edges_to_check -= scout_count;
      work_items.reset();

      loop(
          katana::iterate(*frontier),
          [&](const GNode& src) {
            for (auto e : bidir_view.OutEdges(src)) {
              auto dst = bidir_view.OutEdgeDst(e);
              GNode& ddata = (*node_data)[dst];
              if (ddata == BfsImplementation::kDistanceInfinity) {
                GNode old_parent = ddata;
                if (__sync_bool_compare_and_swap(&ddata, old_parent, src)) {
                  next_frontier->push(dst);
                  work_items += bidir_view.OutDegree(dst);
                }
              }
            }
          },
          katana::steal(), katana::chunk_size<kChunkSize>(),
          katana::loopname(std::string("SyncDO-push").c_str()));
      scout_count = work_items.reduce();
    }
  }
}

template <typename NDType, typename ValueTy>
void
InitNodeDataVec(const ValueTy& value, katana::NUMAArray<NDType>* node_data) {
  katana::do_all(katana::iterate(0ul, node_data->size()), [&](auto n) {
    (*node_data)[n] = value;
  });
}

template <typename NDType>
void
UpdateGraphNodeData(Graph* graph, const katana::NUMAArray<NDType>& node_data) {
  katana::do_all(katana::iterate(*graph), [&](auto& node) {
    graph->GetData<BfsNodeParent>(node) = node_data[node];
  });
}

void
ComputeParentFromDistance(
    const BiDirGraphView& bidir_view, katana::NUMAArray<GNode>* node_parent,
    const katana::NUMAArray<Dist>& node_dist, const GNode source) {
  (*node_parent)[source] = source;
  katana::do_all(
      katana::iterate(bidir_view.Nodes()),
      [&](const GNode v) {
        GNode& v_parent = (*node_parent)[v];
        Dist v_dist = node_dist[v];

        if (v_dist == BfsImplementation::kDistanceInfinity) {
          return;
        } else if (v_dist == 1) {
          v_parent = source;
          return;
        }

        for (auto e : bidir_view.InEdges(v)) {
          GNode u = bidir_view.InEdgeSrc(e);
          if (node_dist[v] == node_dist[u] + 1) {
            v_parent = u;
            break;
          }
        }
      },
      katana::steal(),
      katana::loopname(std::string("ComputeParentFromDistance").c_str()));
}

katana::Result<void>
RunAlgo(
    BfsPlan algo, Graph* graph, const BiDirGraphView& bidir_view,
    const GNode& source) {
  BfsImplementation impl{algo.edge_tile_size()};
  katana::StatTimer exec_time("BFS");

  switch (algo.algorithm()) {
  case BfsPlan::kSynchronousDirectOpt: {
    // Set up node data
    katana::NUMAArray<GNode> node_data;
    node_data.allocateInterleaved(graph->NumNodes());
    InitNodeDataVec(BfsImplementation::kDistanceInfinity, &node_data);

    exec_time.start();
    SynchronousDirectOpt(
        bidir_view, &node_data, source, NodePushWrap(), algo.alpha(),
        algo.beta());
    exec_time.stop();

    UpdateGraphNodeData(graph, node_data);
    break;
  }
  case BfsPlan::kAsynchronous: {
    katana::NUMAArray<GNode> node_parent;
    katana::NUMAArray<Dist> node_dist;
    node_parent.allocateInterleaved(graph->NumNodes());
    node_dist.allocateInterleaved(graph->NumNodes());

    InitNodeDataVec(BfsImplementation::kDistanceInfinity, &node_parent);
    InitNodeDataVec(BfsImplementation::kDistanceInfinity, &node_dist);

    exec_time.start();
    AsynchronousAlgo<UpdateRequest>(
        *graph, source, &node_dist, ReqPushWrap(), OutEdgeRangeFn{graph});
    ComputeParentFromDistance(bidir_view, &node_parent, node_dist, source);
    exec_time.stop();

    UpdateGraphNodeData(graph, node_parent);
    break;
  }
  default:
    return KATANA_ERROR(
        katana::ErrorCode::InvalidArgument, "unknown algorithm {}",
        algo.algorithm());
  }

  return katana::ResultSuccess();
}

katana::Result<void>
BfsImpl(
    Graph* graph, const BiDirGraphView& bidir_view, size_t start_node,
    BfsPlan algo) {
  if (start_node >= graph->NumNodes()) {
    return katana::ErrorCode::InvalidArgument;
  }

  if (algo.algorithm() != BfsPlan::kSynchronousDirectOpt &&
      algo.algorithm() != BfsPlan::kAsynchronous) {
    return KATANA_ERROR(
        katana::ErrorCode::NotImplemented, "Unsupported algorithm: {}",
        algo.algorithm());
  }

  auto it = graph->begin();
  std::advance(it, start_node);
  GNode source = *it;

  size_t approxNodeData = 4 * (graph->NumNodes() + graph->NumEdges());
  katana::EnsurePreallocated(8, approxNodeData);
  katana::ReportPageAllocGuard page_alloc;

  if (auto res = RunAlgo(algo, graph, bidir_view, source); !res) {
    return res.error();
  }

  return katana::ResultSuccess();
}

}  // namespace

katana::Result<void>
katana::analytics::Bfs(
    const std::shared_ptr<PropertyGraph>& pg, GNode start_node,
    const std::string& output_property_name, katana::TxnContext* txn_ctx,
    BfsPlan algo) {
  if (auto result = ConstructNodeProperties<std::tuple<BfsNodeParent>>(
          pg.get(), txn_ctx, {output_property_name});
      !result) {
    return result.error();
  }

  auto graph = KATANA_CHECKED(Graph::Make(pg, {output_property_name}, {}));
  auto bidir_view =
      KATANA_CHECKED(BiDirGraphView::Make(pg, {output_property_name}, {}));

  /*
  auto pg_result = Graph::Make(pg, {output_property_name}, {});
  if (!pg_result) {
    return pg_result.error();
  }
  */

  return BfsImpl(&graph, bidir_view, start_node, algo);
}

template <typename LevelVec>
void
ComputeLevels(
    const Graph& graph, const GNode& source, LevelVec& levels) noexcept {
  using Cont = katana::InsertBag<GNode>;
  using Loop = katana::DoAll;

  Loop loop;

  auto curr = std::make_unique<Cont>();
  auto next = std::make_unique<Cont>();

  Dist next_level = 0U;
  levels[source] = 0u;

  next->push(source);

  KATANA_LOG_DEBUG_ASSERT(!next->empty());

  while (!next->empty()) {
    std::swap(curr, next);
    next->clear();
    ++next_level;

    loop(
        katana::iterate(*curr),
        [&](const GNode& src) {
          for (auto e : graph.OutEdges(src)) {
            auto dest = graph.OutEdgeDst(e);

            if (levels[dest] == BfsImplementation::kDistanceInfinity) {
              levels[dest] = next_level;
              next->push(dest);
            }
          }
        },
        katana::steal(), katana::chunk_size<kChunkSize>(),
        katana::loopname("ComputeLevels"));
  }
}

template <typename LevelVec>
katana::Result<void>
CheckParentByLevel(
    const BiDirGraphView& bidir_view, const GNode& source,
    const LevelVec& levels) {
  if (levels[source] != 0u ||
      bidir_view.GetData<BfsNodeParent>(source) != source) {
    return KATANA_ERROR(
        katana::ErrorCode::AssertionFailed, "incorrect state of source");
  }

  constexpr auto kUnvisited = BfsImplementation::kDistanceInfinity;

  bool found_level_too_low = false;

  bool found_node_with_wrong_level = false;

  bool found_node_with_wrong_parent = false;

  bool found_reachable_node_with_no_parent = false;

  katana::GAccumulator<size_t> num_unvisited;

  katana::do_all(
      katana::iterate(bidir_view),
      [&](const GNode& u) {
        auto u_parent = bidir_view.GetData<BfsNodeParent>(u);

        if (u != source && levels[u] == 0ul) {
          found_level_too_low = true;
        }

        if (u == source) {
          return;
        }

        if (u_parent != kUnvisited && levels[u] != kUnvisited) {
          bool parent_found = false;

          for (auto e : bidir_view.InEdges(u)) {
            auto v = bidir_view.InEdgeSrc(e);

            if (v == u_parent) {
              parent_found = true;

              if (levels[u] != levels[v] + 1) {
                found_node_with_wrong_level = true;
              }
            }
          }

          if (!parent_found) {
            found_node_with_wrong_parent = true;
          }

        } else if (u_parent == kUnvisited && levels[u] != kUnvisited) {
          found_reachable_node_with_no_parent = true;
        } else {
          KATANA_LOG_DEBUG_ASSERT(
              u_parent == kUnvisited && levels[u] == kUnvisited);
          num_unvisited += 1;
        }
      },
      katana::steal(), katana::no_stats());

  constexpr auto kErrorCode = katana::ErrorCode::AssertionFailed;
  if (found_level_too_low) {
    return KATANA_ERROR(
        kErrorCode, "Found a node with Level lower than expected");
  }
  if (found_node_with_wrong_level) {
    return KATANA_ERROR(
        kErrorCode, "Found a node or its parent with wrong level");
  }
  if (found_node_with_wrong_parent) {
    return KATANA_ERROR(
        kErrorCode,
        "Found a node whose parent is not one of its incoming neighbors");
  }
  if (found_reachable_node_with_no_parent) {
    return KATANA_ERROR(
        kErrorCode, "Found a reachable node with unassigned parent");
  }

  if (num_unvisited.reduce() > 0) {
    KATANA_LOG_WARN(
        "BFS: Found {} nodes unreachable, error if graph is strongly connected",
        num_unvisited.reduce());
  }

  return katana::ResultSuccess();
}

katana::Result<void>
katana::analytics::BfsAssertValid(
    const std::shared_ptr<PropertyGraph>& pg, const GNode source,
    const std::string& output_property_name) {
  auto graph = KATANA_CHECKED(Graph::Make(pg, {output_property_name}, {}));
  auto bidir_view =
      KATANA_CHECKED(BiDirGraphView::Make(pg, {output_property_name}, {}));

  katana::NUMAArray<Dist> levels;
  levels.allocateInterleaved(graph.NumNodes());

  katana::ParallelSTL::fill(
      levels.begin(), levels.end(), BfsImplementation::kDistanceInfinity);

  ComputeLevels(graph, source, levels);

  return CheckParentByLevel(bidir_view, source, levels);
}

katana::Result<BfsStatistics>
katana::analytics::BfsStatistics::Compute(
    const std::shared_ptr<katana::PropertyGraph>& pg,
    const std::string& property_name) {
  auto pg_result = BfsImplementation::Graph::Make(pg, {property_name}, {});
  if (!pg_result) {
    return pg_result.error();
  }

  BfsImplementation::Graph graph = pg_result.value();

  GNode source_node = std::numeric_limits<GNode>::max();
  GAccumulator<uint64_t> num_visited;

  auto max_possible_parent = graph.NumNodes();

  do_all(
      iterate(graph),
      [&](GNode i) {
        GNode my_parent = graph.GetData<BfsNodeParent>(i);

        if (my_parent == i) {
          source_node = i;
        }
        if (my_parent <= max_possible_parent) {
          num_visited += 1;
        }
      },
      loopname("BFS Sanity check"), no_stats());

  KATANA_LOG_DEBUG_ASSERT(source_node != std::numeric_limits<GNode>::max());
  uint64_t total_visited_nodes = num_visited.reduce();
  return BfsStatistics{total_visited_nodes};
}

void
katana::analytics::BfsStatistics::Print(std::ostream& os) const {
  os << "Number of reached nodes = " << n_reached_nodes << std::endl;
}
