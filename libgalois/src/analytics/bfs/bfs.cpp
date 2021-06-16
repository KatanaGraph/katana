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
    EdgeTile t{graph->edge_begin(n), graph->edge_end(n)};

    cont.push(t);
  }
};

template <typename WL>
void
WlToBitset(const WL& wl, katana::DynamicBitset* bitset) {
  katana::do_all(
      katana::iterate(wl), [&](const GNode& src) { bitset->set(src); },
      katana::chunk_size<kChunkSize>(), katana::loopname("WlToBitset"));
}

template <typename WL>
void
BitsetToWl(
    const katana::PropertyGraph& graph, const katana::DynamicBitset& bitset,
    WL* wl) {
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

template <bool CONCURRENT, typename T, typename P, typename R>
void
AsynchronousAlgo(
    const katana::PropertyGraph& graph, const GNode source,
    katana::LargeArray<Dist>* node_data, const P& pushWrap,
    const R& edgeRange) {
  namespace gwl = katana;
  using FIFO = gwl::PerSocketChunkFIFO<kChunkSize>;
  using BSWL = gwl::BulkSynchronous<gwl::PerSocketChunkLIFO<kChunkSize>>;
  using WL = FIFO;

  using Loop = typename std::conditional<
      CONCURRENT, katana::ForEach, katana::WhileQ<katana::SerFIFO<T>>>::type;

  KATANA_GCC7_IGNORE_UNUSED_BUT_SET
  constexpr bool use_CAS = CONCURRENT && !std::is_same<WL, BSWL>::value;
  KATANA_END_GCC7_IGNORE_UNUSED_BUT_SET

  Loop loop;

  katana::GAccumulator<size_t> bad_work;
  katana::GAccumulator<size_t> WL_empty_work;

  (*node_data)[source] = 0;

  katana::InsertBag<T> init_bag;

  if (CONCURRENT) {
    pushWrap(init_bag, source, 1, "parallel");
  } else {
    pushWrap(init_bag, source, 1);
  }

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
          auto dest = graph.GetEdgeDest(ii);
          Dist& ddata = (*node_data)[*dest];

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

              pushWrap(ctx, *dest, new_dist + 1);
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

template <bool CONCURRENT, typename T, typename P, typename R>
void
SynchronousAlgo(
    Graph* graph, const GNode source, const P& pushWrap, const R& edgeRange) {
  using Cont = typename std::conditional<
      CONCURRENT, katana::InsertBag<T>, katana::SerStack<T>>::type;
  using Loop = typename std::conditional<
      CONCURRENT, katana::DoAll, katana::StdForEach>::type;

  Loop loop;

  auto curr = std::make_unique<Cont>();
  auto next = std::make_unique<Cont>();

  Dist next_level = 0U;
  graph->GetData<BfsNodeDistance>(source) = 0U;

  if (CONCURRENT) {
    pushWrap(*next, source, "parallel");
  } else {
    pushWrap(*next, source);
  }

  KATANA_LOG_DEBUG_ASSERT(!next->empty());

  while (!next->empty()) {
    std::swap(curr, next);
    next->clear();
    ++next_level;

    loop(
        katana::iterate(*curr),
        [&](const T& item) {
          for (auto e : edgeRange(item)) {
            auto dest = graph->GetEdgeDest(e);
            auto& dest_data = graph->GetData<BfsNodeDistance>(dest);

            if (dest_data == BfsImplementation::kDistanceInfinity) {
              dest_data = next_level;
              pushWrap(*next, *dest);
            }
          }
        },
        katana::steal(), katana::chunk_size<kChunkSize>(),
        katana::loopname("Synchronous"));
  }
}

template <bool CONCURRENT, typename P>
void
SynchronousDirectOpt(
    const katana::PropertyGraph& graph,
    const katana::PropertyGraph& transpose_graph,
    katana::LargeArray<GNode>* node_data, const GNode source, const P& pushWrap,
    const uint32_t alpha, const uint32_t beta) {
  using Cont = typename std::conditional<
      CONCURRENT, katana::InsertBag<GNode>, katana::SerStack<GNode>>::type;
  using Loop = typename std::conditional<
      CONCURRENT, katana::DoAll, katana::StdForEach>::type;

  katana::GAccumulator<uint32_t> work_items;
  katana::StatTimer bitset_to_wl_timer("Bitset_To_WL_Timer");
  katana::StatTimer wl_to_bitset_timer("WL_To_Bitset_Timer");

  Loop loop;

  katana::DynamicBitset front_bitset;
  katana::DynamicBitset next_bitset;

  uint32_t num_nodes = graph.size();
  uint64_t num_edges = graph.num_edges();

  front_bitset.resize(num_nodes);
  next_bitset.resize(num_nodes);

  auto frontier = std::make_unique<Cont>();
  auto next_frontier = std::make_unique<Cont>();

  (*node_data)[source] = source;

  if (CONCURRENT) {
    pushWrap(*next_frontier, source, "parallel");
  } else {
    pushWrap(*next_frontier, source);
  }

  work_items += 1;

  int64_t edges_to_check = num_edges;
  int64_t scout_count = graph.edges(source).size();
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
            katana::iterate(transpose_graph),
            [&](const GNode& dst) {
              GNode& ddata = (*node_data)[dst];
              if (ddata == BfsImplementation::kDistanceInfinity) {
                for (auto e : transpose_graph.edges(dst)) {
                  auto src = transpose_graph.GetEdgeDest(e);

                  if (front_bitset.test(*src)) {
                    // assign parents on the bfs path.
                    ddata = *src;
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
      BitsetToWl(graph, front_bitset, next_frontier.get());
      bitset_to_wl_timer.stop();
      scout_count = 1;
    } else {
      edges_to_check -= scout_count;
      work_items.reset();

      loop(
          katana::iterate(*frontier),
          [&](const GNode& src) {
            for (auto e : graph.edges(src)) {
              auto dst = graph.GetEdgeDest(e);
              GNode& ddata = (*node_data)[*dst];
              if (ddata == BfsImplementation::kDistanceInfinity) {
                GNode old_parent = ddata;
                if (__sync_bool_compare_and_swap(&ddata, old_parent, src)) {
                  next_frontier->push(*dst);
                  auto [begin_edge, end_edge] =
                      graph.topology().edge_range(*dst);
                  work_items += end_edge - begin_edge;
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
InitializeNodeData(const ValueTy value, katana::LargeArray<NDType>* node_data) {
  katana::do_all(katana::iterate(0ul, node_data->size()), [&](auto n) {
    (*node_data)[n] = value;
  });
}

template <typename NDType, typename ValueTy, typename... Args>
void
InitializeNodeData(
    const ValueTy value, katana::LargeArray<NDType>* node_data, Args... args) {
  InitializeNodeData(value, node_data);
  InitializeNodeData(value, args...);
}

template <typename NDType>
void
InitializeGraphNodeData(
    Graph* graph, const katana::LargeArray<NDType>& node_data) {
  katana::do_all(katana::iterate(*graph), [&](auto& node) {
    graph->GetData<BfsNodeParent>(node) = node_data[node];
  });
}

void
ComputeParentFromDistance(
    katana::PropertyGraph& transpose_graph,
    katana::LargeArray<GNode>* node_parent,
    const katana::LargeArray<Dist>& node_dist, const GNode source) {
  (*node_parent)[source] = source;
  katana::do_all(
      katana::iterate(transpose_graph),
      [&](const GNode v) {
        GNode& v_parent = (*node_parent)[v];
        Dist v_dist = node_dist[v];

        if (v_dist == BfsImplementation::kDistanceInfinity) {
          return;
        } else if (v_dist == 1) {
          v_parent = source;
          return;
        }

        for (auto e : transpose_graph.edges(v)) {
          GNode u = *(transpose_graph.GetEdgeDest(e));
          if (node_dist[v] == node_dist[u] + 1) {
            v_parent = u;
            break;
          }
        }
      },
      katana::steal(),
      katana::loopname(std::string("ComputeParentFromDistance").c_str()));
}

template <bool CONCURRENT>
katana::Result<void>
RunAlgo(
    BfsPlan algo, Graph* graph, katana::PropertyGraph* pg,
    katana::PropertyGraph& transpose_graph, const GNode& source) {
  BfsImplementation impl{algo.edge_tile_size()};
  katana::StatTimer exec_time("BFS");

  switch (algo.algorithm()) {
  case BfsPlan::kSynchronousDirectOpt: {
    // Set up node data
    katana::LargeArray<GNode> node_data;
    node_data.allocateInterleaved(graph->num_nodes());
    InitializeNodeData(BfsImplementation::kDistanceInfinity, &node_data);

    exec_time.start();
    SynchronousDirectOpt<CONCURRENT>(
        *pg, transpose_graph, &node_data, source, NodePushWrap(), algo.alpha(),
        algo.beta());
    exec_time.stop();

    InitializeGraphNodeData(graph, node_data);
    break;
  }
  case BfsPlan::kAsynchronous: {
    katana::LargeArray<GNode> node_parent;
    katana::LargeArray<Dist> node_dist;
    node_parent.allocateInterleaved(graph->num_nodes());
    node_dist.allocateInterleaved(graph->num_nodes());

    InitializeNodeData(
        BfsImplementation::kDistanceInfinity, &node_parent, &node_dist);

    exec_time.start();
    AsynchronousAlgo<CONCURRENT, UpdateRequest>(
        *pg, source, &node_dist, ReqPushWrap(), OutEdgeRangeFn{graph});
    ComputeParentFromDistance(transpose_graph, &node_parent, node_dist, source);
    exec_time.stop();

    InitializeGraphNodeData(graph, node_parent);
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
    katana::TypedPropertyGraph<std::tuple<BfsNodeParent>, std::tuple<>>& graph,
    katana::PropertyGraph* pg, size_t start_node, BfsPlan algo) {
  if (start_node >= graph.size()) {
    return katana::ErrorCode::InvalidArgument;
  }

  if (algo.algorithm() != BfsPlan::kSynchronousDirectOpt &&
      algo.algorithm() != BfsPlan::kAsynchronous) {
    return KATANA_ERROR(
        katana::ErrorCode::NotImplemented, "Unsupported algorithm: {}",
        algo.algorithm());
  }

  auto it = graph.begin();
  std::advance(it, start_node);
  GNode source = *it;

  size_t approxNodeData = 4 * (graph.num_nodes() + graph.num_edges());
  katana::EnsurePreallocated(8, approxNodeData);
  katana::ReportPageAllocGuard page_alloc;

  // TODO(lhc): due to lack of in-edge iteration, manually creates a transposed graph
  const katana::GraphTopology& topology = pg->topology();
  auto transpose_graph = katana::CreateTransposeGraphTopology(topology);

  if (auto res = RunAlgo<true>(
          algo, &graph, pg, *(transpose_graph.value().get()), source);
      !res) {
    return res.error();
  }

  return katana::ResultSuccess();
}

}  // namespace

katana::Result<void>
katana::analytics::Bfs(
    PropertyGraph* pg, GNode start_node,
    const std::string& output_property_name, BfsPlan algo) {
  if (auto result = ConstructNodeProperties<std::tuple<BfsNodeParent>>(
          pg, {output_property_name});
      !result) {
    return result.error();
  }

  auto pg_result = Graph::Make(pg, {output_property_name}, {});
  if (!pg_result) {
    return pg_result.error();
  }

  return BfsImpl(pg_result.value(), pg, start_node, algo);
}

katana::Result<void>
katana::analytics::BfsAssertValid(
    PropertyGraph* pg, const GNode source, const std::string& property_name) {
  auto pg_result = BfsImplementation::Graph::Make(pg, {property_name}, {});
  if (!pg_result) {
    return pg_result.error();
  }

  BfsImplementation::Graph graph = pg_result.value();

  // TODO(lhc): due to lack of in-edge iteration, manually creates a transposed graph
  const katana::GraphTopology& topology = pg->topology();
  katana::LargeArray<GNode> node_data;
  node_data.allocateInterleaved(topology.num_nodes());
  auto transpose_graph_topo = katana::CreateTransposeGraphTopology(topology);
  const auto& transpose_graph = *(transpose_graph_topo.value().get());

  uint32_t num_nodes = graph.num_nodes();
  LargeArray<Dist> levels;
  gstl::Vector<uint32_t> visited_nodes;
  levels.allocateInterleaved(num_nodes);
  visited_nodes.reserve(num_nodes);

  do_all(iterate(0ul, levels.size()), [&](size_t i) {
    levels[i] = BfsImplementation::kDistanceInfinity;
  });

  levels[source] = 0;
  visited_nodes.push_back(source);

  // First, visit all reachable nodes and calculate level for each node sequentially
  for (auto it = visited_nodes.begin(); it != visited_nodes.end(); it++) {
    GNode u = *it;
    for (auto e : graph.edges(u)) {
      GNode v = *(graph.GetEdgeDest(e));
      if (levels[v] == BfsImplementation::kDistanceInfinity) {
        levels[v] = levels[u] + 1;
        visited_nodes.push_back(v);
      }
    }
  }

  for (GNode u : graph) {
    GNode u_parent = graph.GetData<BfsNodeParent>(u);
    if ((levels[u] != BfsImplementation::kDistanceInfinity) &&
        (u_parent != BfsImplementation::kDistanceInfinity)) {
      if (u == source) {
        if (!(u_parent == u && levels[u] == 0)) {
          return KATANA_ERROR(
              katana::ErrorCode::AssertionFailed, "incorrect source");
        }
        continue;
      }
      bool parent_found = false;

      for (auto e : transpose_graph.edges(u)) {
        GNode v = *(transpose_graph.GetEdgeDest(e));
        if (v == u_parent) {
          if (levels[v] != levels[u] - 1) {
            return KATANA_ERROR(
                katana::ErrorCode::AssertionFailed, "incorrect depth");
          }
          parent_found = true;
          break;
        }
      }

      if (!parent_found) {
        return KATANA_ERROR(
            katana::ErrorCode::AssertionFailed, "parent must exist");
      }
    } else if (u_parent != BfsImplementation::kDistanceInfinity) {
      return KATANA_ERROR(katana::ErrorCode::AssertionFailed, "unvisited node");
    }
  }

  return katana::ResultSuccess();
}

katana::Result<BfsStatistics>
katana::analytics::BfsStatistics::Compute(
    katana::PropertyGraph* pg, const std::string& property_name) {
  auto pg_result = BfsImplementation::Graph::Make(pg, {property_name}, {});
  if (!pg_result) {
    return pg_result.error();
  }

  BfsImplementation::Graph graph = pg_result.value();

  GNode source_node = std::numeric_limits<GNode>::max();
  GAccumulator<uint64_t> num_visited;

  auto max_possible_parent = graph.num_nodes();

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
