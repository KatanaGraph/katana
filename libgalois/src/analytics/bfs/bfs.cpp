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

#include <deque>
#include <type_traits>

#include "galois/analytics/bfs/bfs_internal.h"

using namespace galois::analytics;

using Graph = BfsImplementation::Graph;

constexpr static unsigned kChunkSize = 256U;

constexpr static bool kTrackWork = BfsImplementation::kTrackWork;

using UpdateRequest = BfsImplementation::UpdateRequest;
using Dist = BfsImplementation::Dist;
using SrcEdgeTile = BfsImplementation::SrcEdgeTile;
using SrcEdgeTileMaker = BfsImplementation::SrcEdgeTileMaker;
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
      Graph::edge_iterator beg, Graph::edge_iterator end) const {
    return EdgeTile{beg, end};
  }
};

struct NodePushWrap {
  template <typename C>
  void operator()(C& cont, const Graph::Node& n, const char* const) const {
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
  void operator()(C& cont, const Graph::Node& n, const char* const) const {
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
  void operator()(C& cont, const Graph::Node& n, const char* const) const {
    (*this)(cont, n);
  }

  template <typename C>
  void operator()(C& cont, const Graph::Node& n) const {
    EdgeTile t{graph->edge_begin(n), graph->edge_end(n)};

    cont.push(t);
  }
};

template <bool CONCURRENT, typename T, typename P, typename R>
void
AsyncAlgo(
    Graph* graph, Graph::Node source, const P& pushWrap, const R& edgeRange) {
  namespace gwl = galois::worklists;
  // typedef PerSocketChunkFIFO<kChunkSize> dFIFO;
  using FIFO = gwl::PerSocketChunkFIFO<kChunkSize>;
  using BSWL = gwl::BulkSynchronous<gwl::PerSocketChunkLIFO<kChunkSize>>;
  using WL = FIFO;

  using Loop = typename std::conditional<
      CONCURRENT, galois::ForEach, galois::WhileQ<galois::SerFIFO<T>>>::type;

  GALOIS_GCC7_IGNORE_UNUSED_BUT_SET
  constexpr bool useCAS = CONCURRENT && !std::is_same<WL, BSWL>::value;
  GALOIS_END_GCC7_IGNORE_UNUSED_BUT_SET

  Loop loop;

  galois::GAccumulator<size_t> BadWork;
  galois::GAccumulator<size_t> WLEmptyWork;

  graph->GetData<BfsNodeDistance>(source) = 0;
  galois::InsertBag<T> init_bag;

  if (CONCURRENT) {
    pushWrap(init_bag, source, 1, "parallel");
  } else {
    pushWrap(init_bag, source, 1);
  }

  loop(
      galois::iterate(init_bag),
      [&](const T& item, auto& ctx) {
        const auto& sdist = graph->GetData<BfsNodeDistance>(item.src);

        if (kTrackWork) {
          if (item.dist != sdist) {
            WLEmptyWork += 1;
            return;
          }
        }

        const auto new_dist = item.dist;

        for (auto ii : edgeRange(item)) {
          auto dest = graph->GetEdgeDest(ii);
          auto& ddata = graph->GetData<BfsNodeDistance>(dest);

          while (true) {
            Dist old_dist = ddata;

            if (old_dist <= new_dist) {
              break;
            }

            if (!useCAS ||
                __sync_bool_compare_and_swap(&ddata, old_dist, new_dist)) {
              if (!useCAS) {
                ddata = new_dist;
              }

              if (kTrackWork) {
                if (old_dist != BfsImplementation::kDistanceInfinity) {
                  BadWork += 1;
                }
              }

              pushWrap(ctx, *dest, new_dist + 1);
              break;
            }
          }
        }
      },
      galois::wl<WL>(), galois::loopname("runBFS"),
      galois::disable_conflict_detection());

  if (kTrackWork) {
    galois::ReportStatSingle("BFS", "BadWork", BadWork.reduce());
    galois::ReportStatSingle("BFS", "EmptyWork", WLEmptyWork.reduce());
  }
}

template <bool CONCURRENT, typename T, typename P, typename R>
void
SyncAlgo(
    Graph* graph, Graph::Node source, const P& pushWrap, const R& edgeRange) {
  using Cont = typename std::conditional<
      CONCURRENT, galois::InsertBag<T>, galois::SerStack<T>>::type;
  using Loop = typename std::conditional<
      CONCURRENT, galois::DoAll, galois::StdForEach>::type;

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

  assert(!next->empty());

  while (!next->empty()) {
    std::swap(curr, next);
    next->clear();
    ++next_level;

    loop(
        galois::iterate(*curr),
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
        galois::steal(), galois::chunk_size<kChunkSize>(),
        galois::loopname("Sync"));
  }
}

template <bool CONCURRENT>
void
RunAlgo(BfsPlan algo, Graph* graph, const Graph::Node& source) {
  BfsImplementation impl{algo.edge_tile_size()};
  switch (algo.algorithm()) {
  case BfsPlan::kAsyncTile:
    AsyncAlgo<CONCURRENT, SrcEdgeTile>(
        graph, source, SrcEdgeTilePushWrap{graph, impl}, TileRangeFn());
    break;
  case BfsPlan::kAsync:
    AsyncAlgo<CONCURRENT, UpdateRequest>(
        graph, source, ReqPushWrap(), OutEdgeRangeFn{graph});
    break;
  case BfsPlan::kSyncTile:
    SyncAlgo<CONCURRENT, EdgeTile>(
        graph, source, EdgeTilePushWrap{graph, impl}, TileRangeFn());
    break;
  case BfsPlan::kSync:
    SyncAlgo<CONCURRENT, Graph::Node>(
        graph, source, NodePushWrap(), OutEdgeRangeFn{graph});
    break;
  default:
    std::cerr << "ERROR: unkown algo type\n";
  }
}

static galois::Result<void>
BfsImpl(
    galois::graphs::PropertyGraph<std::tuple<BfsNodeDistance>, std::tuple<>>&
        graph,
    size_t start_node, BfsPlan algo) {
  if (start_node >= graph.size()) {
    return galois::ErrorCode::InvalidArgument;
  }

  auto it = graph.begin();
  std::advance(it, start_node);
  Graph::Node source = *it;

  size_t approxNodeData = 4 * (graph.num_nodes() + graph.num_edges());
  galois::Prealloc(8, approxNodeData);

  galois::do_all(galois::iterate(graph.begin(), graph.end()), [&graph](auto n) {
    graph.GetData<BfsNodeDistance>(n) = BfsImplementation::kDistanceInfinity;
  });

  galois::StatTimer execTime("BFS");
  execTime.start();

  RunAlgo<true>(algo, &graph, source);

  execTime.stop();

  return galois::ResultSuccess();
}

galois::Result<void>
galois::analytics::Bfs(
    galois::graphs::PropertyFileGraph* pfg, size_t start_node,
    const std::string& output_property_name, BfsPlan algo) {
  if (auto result = ConstructNodeProperties<std::tuple<BfsNodeDistance>>(
          pfg, {output_property_name});
      !result) {
    return result.error();
  }

  auto pg_result = Graph::Make(pfg, {output_property_name}, {});
  if (!pg_result) {
    return pg_result.error();
  }

  return BfsImpl(pg_result.value(), start_node, algo);
}

galois::Result<bool>
galois::analytics::BfsValidate(
    graphs::PropertyFileGraph* pfg, const std::string& property_name) {
  auto pg_result = BfsImplementation::Graph::Make(pfg, {property_name}, {});
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
    return false;
  }

  std::atomic<bool> not_consistent(false);
  do_all(
      iterate(graph),
      BfsImplementation::NotConsistent<BfsNodeDistance, BfsNodeDistance>(
          &graph, not_consistent));

  if (not_consistent) {
    return false;
  }

  return true;
}

galois::Result<BfsStatistics>
galois::analytics::BfsStatistics::Compute(
    galois::graphs::PropertyFileGraph* pfg, const std::string& property_name) {
  auto pg_result = BfsImplementation::Graph::Make(pfg, {property_name}, {});
  if (!pg_result) {
    return pg_result.error();
  }

  BfsImplementation::Graph graph = pg_result.value();

  uint32_t source_node;
  GReduceMax<uint32_t> max_dist;
  GAccumulator<uint64_t> sum_dist;
  GAccumulator<uint32_t> num_visited;
  max_dist.reset();
  sum_dist.reset();
  num_visited.reset();

  do_all(
      iterate(graph),
      [&](uint64_t i) {
        uint32_t my_distance = graph.GetData<BfsNodeDistance>(i);

        if (my_distance == 0) {
          source_node = i;
        }
        if (my_distance != BfsImplementation::kDistanceInfinity) {
          max_dist.update(my_distance);
          sum_dist += my_distance;
          num_visited += 1;
        }
      },
      loopname("Sanity check"), no_stats());

  return BfsStatistics{
      source_node, max_dist.reduce(), sum_dist.reduce(), num_visited.reduce()};
}

void
galois::analytics::BfsStatistics::Print(std::ostream& os) {
  os << "Source node = " << source_node << std::endl;
  os << "Number of reached nodes = " << n_reached_nodes << std::endl;
  os << "Maximum distance = " << max_distance << std::endl;
  os << "Sum of distances = " << total_distance << std::endl;
  os << "Average distance = " << average_distance() << std::endl;
}
