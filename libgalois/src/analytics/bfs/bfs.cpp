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
#include "katana/TypedPropertyGraph.h"
#include "katana/analytics/BfsSsspImplementationBase.h"

using namespace katana::analytics;

namespace {

/// The tag for the output property of BFS in TypedPropertyGraphs.
using BfsNodeDistance = katana::PODProperty<uint32_t>;

struct BfsImplementation
    : BfsSsspImplementationBase<
          katana::TypedPropertyGraph<std::tuple<BfsNodeDistance>, std::tuple<>>,
          unsigned int, false> {
  BfsImplementation(ptrdiff_t edge_tile_size)
      : BfsSsspImplementationBase<
            katana::TypedPropertyGraph<
                std::tuple<BfsNodeDistance>, std::tuple<>>,
            unsigned int, false>{edge_tile_size} {}
};

using Graph = BfsImplementation::Graph;

constexpr unsigned kChunkSize = 256U;

constexpr bool kTrackWork = BfsImplementation::kTrackWork;

using UpdateRequest = BfsImplementation::UpdateRequest;
using Dist = BfsImplementation::Dist;
using SrcEdgeTile = BfsImplementation::SrcEdgeTile;
using SrcEdgeTilePushWrap = BfsImplementation::SrcEdgeTilePushWrap;
using ReqPushWrap = BfsImplementation::ReqPushWrap;
using OutEdgeRangeFn = BfsImplementation::OutEdgeRangeFn;
using TileRangeFn = BfsImplementation::TileRangeFn;

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
  void operator()(
      C& cont, const Graph::Node& n, const char* const /*tag*/) const {
    (*this)(cont, n);
  }

  template <typename C>
  void operator()(C& cont, const Graph::Node& n) const {
    cont.push(n);
  }
};

struct EdgeTilePushWrap {
  Graph* graph;
  BfsImplementation& impl;

  template <typename C>
  void operator()(
      C& cont, const Graph::Node& n, const char* const /*tag*/) const {
    impl.PushEdgeTilesParallel(cont, graph, n, EdgeTileMaker{});
  }

  template <typename C>
  void operator()(C& cont, const Graph::Node& n) const {
    impl.PushEdgeTiles(cont, graph, n, EdgeTileMaker{});
  }
};

struct OneTilePushWrap {
  Graph* graph;

  template <typename C>
  void operator()(
      C& cont, const Graph::Node& n, const char* const /*tag*/) const {
    (*this)(cont, n);
  }

  template <typename C>
  void operator()(C& cont, const Graph::Node& n) const {
    EdgeTile t{graph->edge_begin(n), graph->edge_end(n)};

    cont.push(t);
  }
};

template <typename WL>
void
WlToBitset(const WL& wl, katana::DynamicBitset* bitset) {
  katana::do_all(
      katana::iterate(wl), [&](const Graph::Node& src) { bitset->set(src); },
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
      [&](const Graph::Node& src) {
        if (bitset.test(src)) {
          wl->push(src);
        }
      },
      katana::chunk_size<kChunkSize>(), katana::loopname("BitsetToWl"));
}

template <bool CONCURRENT, typename T, typename P, typename R>
void
AsynchronousAlgo(
    const katana::PropertyGraph& graph, const Graph::Node source,
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
    Graph* graph, Graph::Node source, const P& pushWrap, const R& edgeRange) {
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
    katana::LargeArray<Dist>* node_data, const Graph::Node source,
    const P& pushWrap, const uint32_t alpha, const uint32_t beta) {
  using Cont = typename std::conditional<
      CONCURRENT, katana::InsertBag<Graph::Node>,
      katana::SerStack<Graph::Node>>::type;
  using Loop = typename std::conditional<
      CONCURRENT, katana::DoAll, katana::StdForEach>::type;

  katana::GAccumulator<uint32_t> work_items;
  katana::StatTimer bitset_to_wl_timer("Bitset_To_WL_Timer");
  katana::StatTimer wl_to_bitset_timer("WL_To_Bitset_Timer");

  Loop loop;
  katana::DynamicBitset front_bitset, next_bitset;

  uint32_t num_nodes = graph.size();
  uint64_t num_edges = graph.num_edges();

  front_bitset.resize(num_nodes);
  next_bitset.resize(num_nodes);
  front_bitset.reset();
  next_bitset.reset();

  Cont* frontier = new Cont();
  Cont* next_frontier = new Cont();

  Dist next_level{0};
  (*node_data)[source] = 0;

  if (CONCURRENT) {
    pushWrap(*next_frontier, source, "parallel");
  } else {
    pushWrap(*next_frontier, source);
  }

  work_items += 1;

  int64_t edges_to_check = num_edges;
  int64_t scout_count = graph.edges(source).size();
  uint64_t old_num_work_items{0};

  katana::GAccumulator<uint64_t> writes_pull, writes_push;
  writes_pull.reset();
  writes_push.reset();

  while (!next_frontier->empty()) {
    std::swap(frontier, next_frontier);
    next_frontier->clear();
    if (scout_count > edges_to_check / alpha) {
      wl_to_bitset_timer.start();
      WlToBitset(*frontier, &front_bitset);
      wl_to_bitset_timer.stop();
      do {
        ++next_level;
        old_num_work_items = work_items.reduce();
        work_items.reset();

        loop(
            katana::iterate(transpose_graph),
            [&](const typename Graph::Node& dst) {
              Dist& ddata = (*node_data)[dst];
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
      BitsetToWl(graph, front_bitset, next_frontier);
      bitset_to_wl_timer.stop();
      scout_count = 1;
    } else {
      ++next_level;
      edges_to_check -= scout_count;
      work_items.reset();

      loop(
          katana::iterate(*frontier),
          [&](const typename Graph::Node& src) {
            for (auto e : graph.edges(src)) {
              auto dst = graph.GetEdgeDest(e);
              Dist& ddata = (*node_data)[*dst];
              if (ddata == BfsImplementation::kDistanceInfinity) {
                Dist old_dist = ddata;
                if (__sync_bool_compare_and_swap(&ddata, old_dist, src)) {
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

  delete frontier;
  delete next_frontier;
}

template <bool CONCURRENT>
void
RunAlgo(
    BfsPlan algo, Graph* graph, katana::PropertyGraph* pg,
    const katana::PropertyGraph& transpose_graph,
    katana::LargeArray<Dist>* node_data, const Graph::Node& source) {
  BfsImplementation impl{algo.edge_tile_size()};
  switch (algo.algorithm()) {
  case BfsPlan::kAsynchronousTile:
    AsynchronousAlgo<CONCURRENT, SrcEdgeTile>(
        *pg, source, node_data, SrcEdgeTilePushWrap{graph, impl},
        TileRangeFn());
    break;
  case BfsPlan::kAsynchronous:
    AsynchronousAlgo<CONCURRENT, UpdateRequest>(
        *pg, source, node_data, ReqPushWrap(),
        BfsImplementation::OutEdgeRangeFnUsingPG{pg});
    break;
  case BfsPlan::kSynchronousTile:
    SynchronousAlgo<CONCURRENT, EdgeTile>(
        graph, source, EdgeTilePushWrap{graph, impl}, TileRangeFn());
    break;
  case BfsPlan::kSynchronous:
    SynchronousAlgo<CONCURRENT, Graph::Node>(
        graph, source, NodePushWrap(), OutEdgeRangeFn{graph});
    break;
  case BfsPlan::kSynchronousDirectOpt:
    SynchronousDirectOpt<CONCURRENT>(
        *pg, transpose_graph, node_data, source, NodePushWrap(), algo.alpha(),
        algo.beta());
    break;
  default:
    std::cerr << "ERROR: unkown algo type\n";
  }
}

katana::Result<void>
BfsImpl(
    katana::TypedPropertyGraph<std::tuple<BfsNodeDistance>, std::tuple<>>&
        graph,
    katana::PropertyGraph* pg, size_t start_node, BfsPlan algo) {
  if (start_node >= graph.size()) {
    return katana::ErrorCode::InvalidArgument;
  }

  auto it = graph.begin();
  std::advance(it, start_node);
  Graph::Node source = *it;

  size_t approxNodeData = 4 * (graph.num_nodes() + graph.num_edges());
  katana::EnsurePreallocated(8, approxNodeData);

  // TODO(lhc): due to lack of in-edge iteration, manually creates a transposed graph
  const katana::GraphTopology& topology = pg->topology();
  katana::LargeArray<Dist> node_data;
  bool use_block = false;
  if (use_block) {
    node_data.allocateBlocked(topology.num_nodes());
  } else {
    node_data.allocateInterleaved(topology.num_nodes());
  }
  auto transpose_graph = katana::CreateTransposeGraphTopology(topology);

  katana::do_all(katana::iterate(graph.begin(), graph.end()), [&](auto n) {
    graph.GetData<BfsNodeDistance>(n) = BfsImplementation::kDistanceInfinity;
    node_data[n] = BfsImplementation::kDistanceInfinity;
  });

  katana::StatTimer execTime("BFS");
  execTime.start();
  RunAlgo<true>(
      algo, &graph, pg, *(transpose_graph.value().get()), &node_data, source);
  execTime.stop();

  if (algo.algorithm() == BfsPlan::kAsynchronous ||
      algo.algorithm() == BfsPlan::kSynchronousDirectOpt) {
    katana::do_all(
        katana::iterate(graph.begin(), graph.end()), [&](auto& node) {
          graph.GetData<BfsNodeDistance>(node) = node_data[node];
        });
  }

  return katana::ResultSuccess();
}

}  // namespace

katana::Result<void>
katana::analytics::Bfs(
    katana::PropertyGraph* pg, size_t start_node,
    const std::string& output_property_name, BfsPlan algo) {
  if (auto result = ConstructNodeProperties<std::tuple<BfsNodeDistance>>(
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
    PropertyGraph* pg, const std::string& property_name) {
  auto pg_result = BfsImplementation::Graph::Make(pg, {property_name}, {});
  if (!pg_result) {
    return pg_result.error();
  }

  BfsImplementation::Graph graph = pg_result.value();

  GAccumulator<uint64_t> n_zeros;
  do_all(iterate(graph), [&](uint32_t node) {
    if (graph.GetData<BfsNodeDistance>(node) == 0) {
      n_zeros += 1;
    }
  });

  if (n_zeros.reduce() != 1) {
    return katana::ErrorCode::AssertionFailed;
  }

  std::atomic<bool> not_consistent(false);
  do_all(
      iterate(graph),
      BfsImplementation::NotConsistent<BfsNodeDistance, BfsNodeDistance>(
          &graph, not_consistent));

  if (not_consistent) {
    return katana::ErrorCode::AssertionFailed;
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

  uint32_t source_node = std::numeric_limits<uint32_t>::max();
  GReduceMax<uint32_t> max_dist;
  GAccumulator<uint64_t> sum_dist;
  GAccumulator<uint64_t> num_visited;

  auto max_possible_distance = graph.num_nodes();

  do_all(
      iterate(graph),
      [&](uint64_t i) {
        uint32_t my_distance = graph.GetData<BfsNodeDistance>(i);

        if (my_distance == 0) {
          source_node = i;
        }
        if (my_distance <= max_possible_distance) {
          max_dist.update(my_distance);
          sum_dist += my_distance;
          num_visited += 1;
        }
      },
      loopname("BFS Sanity check"), no_stats());

  KATANA_LOG_DEBUG_ASSERT(source_node != std::numeric_limits<uint32_t>::max());
  uint64_t total_visited_nodes = num_visited.reduce();
  double average_dist = double(sum_dist.reduce()) / total_visited_nodes;
  return BfsStatistics{total_visited_nodes, max_dist.reduce(), average_dist};
}

void
katana::analytics::BfsStatistics::Print(std::ostream& os) const {
  os << "Number of reached nodes = " << n_reached_nodes << std::endl;
  os << "Maximum distance = " << max_distance << std::endl;
  os << "Average distance = " << average_visited_distance << std::endl;
}
