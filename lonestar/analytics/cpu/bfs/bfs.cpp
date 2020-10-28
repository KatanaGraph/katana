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
#include <iostream>
#include <type_traits>

#include "Lonestar/BFS_SSSP.h"
#include "Lonestar/BoilerPlate.h"

namespace cll = llvm::cl;

static const char* name = "Breadth-first Search";

static const char* desc =
    "Computes the shortest path from a source node to all nodes in a directed "
    "graph using a modified Bellman-Ford algorithm";

static const char* url = "breadth_first_search";

static cll::opt<std::string> inputFile(
    cll::Positional, cll::desc("<input file>"), cll::Required);
static cll::opt<unsigned int> startNode(
    "startNode", cll::desc("Node to start search from (default value 0)"),
    cll::init(0));
static cll::opt<unsigned int> reportNode(
    "reportNode", cll::desc("Node to report distance to (default value 1)"),
    cll::init(1));

// static cll::opt<unsigned int> stepShiftw("delta",
// cll::desc("Shift value for the deltastep"),
// cll::init(10));

enum Exec { SERIAL, PARALLEL };

enum Algo { AsyncTile = 0, Async, SyncTile, Sync };

const char* const ALGO_NAMES[] = {"AsyncTile", "Async", "SyncTile", "Sync"};

static cll::opt<Exec> execution(
    "exec",
    cll::desc("Choose SERIAL or PARALLEL execution (default value PARALLEL):"),
    cll::values(clEnumVal(SERIAL, "SERIAL"), clEnumVal(PARALLEL, "PARALLEL")),
    cll::init(PARALLEL));

static cll::opt<Algo> algo(
    "algo", cll::desc("Choose an algorithm (default value SyncTile):"),
    cll::values(
        clEnumVal(AsyncTile, "AsyncTile"), clEnumVal(Async, "Async"),
        clEnumVal(SyncTile, "SyncTile"), clEnumVal(Sync, "Sync")),
    cll::init(SyncTile));

struct NodeDistCurrent : public galois::PODProperty<uint32_t> {};

using NodeData = std::tuple<NodeDistCurrent>;
using EdgeData = std::tuple<>;

typedef galois::graphs::PropertyGraph<NodeData, EdgeData> Graph;
typedef typename Graph::Node GNode;

constexpr static const bool TRACK_WORK = false;
constexpr static const unsigned CHUNK_SIZE = 256U;
constexpr static const ptrdiff_t EDGE_TILE_SIZE = 256;

using BFS = BFS_SSSP<Graph, unsigned int, false, EDGE_TILE_SIZE>;

using UpdateRequest = BFS::UpdateRequest;
using Dist = BFS::Dist;
using SrcEdgeTile = BFS::SrcEdgeTile;
using SrcEdgeTileMaker = BFS::SrcEdgeTileMaker;
using SrcEdgeTilePushWrap = BFS::SrcEdgeTilePushWrap;
using ReqPushWrap = BFS::ReqPushWrap;
using OutEdgeRangeFn = BFS::OutEdgeRangeFn;
using TileRangeFn = BFS::TileRangeFn;

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
  void operator()(C& cont, const GNode& n, const char* const) const {
    (*this)(cont, n);
  }

  template <typename C>
  void operator()(C& cont, const GNode& n) const {
    cont.push(n);
  }
};

struct EdgeTilePushWrap {
  Graph* graph;

  template <typename C>
  void operator()(C& cont, const GNode& n, const char* const) const {
    BFS::pushEdgeTilesParallel(cont, graph, n, EdgeTileMaker{});
  }

  template <typename C>
  void operator()(C& cont, const GNode& n) const {
    BFS::pushEdgeTiles(cont, graph, n, EdgeTileMaker{});
  }
};

struct OneTilePushWrap {
  Graph* graph;

  template <typename C>
  void operator()(C& cont, const GNode& n, const char* const) const {
    (*this)(cont, n);
  }

  template <typename C>
  void operator()(C& cont, const GNode& n) const {
    EdgeTile t{graph->edge_begin(n), graph->edge_end(n)};

    cont.push(t);
  }
};

template <bool CONCURRENT, typename T, typename P, typename R>
void
asyncAlgo(Graph* graph, GNode source, const P& pushWrap, const R& edgeRange) {
  namespace gwl = galois::worklists;
  // typedef PerSocketChunkFIFO<CHUNK_SIZE> dFIFO;
  using FIFO = gwl::PerSocketChunkFIFO<CHUNK_SIZE>;
  using BSWL = gwl::BulkSynchronous<gwl::PerSocketChunkLIFO<CHUNK_SIZE>>;
  using WL = FIFO;

  using Loop = typename std::conditional<
      CONCURRENT, galois::ForEach, galois::WhileQ<galois::SerFIFO<T>>>::type;

  GALOIS_GCC7_IGNORE_UNUSED_BUT_SET
  constexpr bool useCAS = CONCURRENT && !std::is_same<WL, BSWL>::value;
  GALOIS_END_GCC7_IGNORE_UNUSED_BUT_SET

  Loop loop;

  galois::GAccumulator<size_t> BadWork;
  galois::GAccumulator<size_t> WLEmptyWork;

  graph->GetData<NodeDistCurrent>(source) = 0;
  galois::InsertBag<T> init_bag;

  if (CONCURRENT) {
    pushWrap(init_bag, source, 1, "parallel");
  } else {
    pushWrap(init_bag, source, 1);
  }

  loop(
      galois::iterate(init_bag),
      [&](const T& item, auto& ctx) {
        const auto& sdist = graph->GetData<NodeDistCurrent>(item.src);

        if (TRACK_WORK) {
          if (item.dist != sdist) {
            WLEmptyWork += 1;
            return;
          }
        }

        const auto new_dist = item.dist;

        for (auto ii : edgeRange(item)) {
          auto dest = graph->GetEdgeDest(ii);
          auto& ddata = graph->GetData<NodeDistCurrent>(*dest);

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

              if (TRACK_WORK) {
                if (old_dist != BFS::DIST_INFINITY) {
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

  if (TRACK_WORK) {
    galois::ReportStatSingle("BFS", "BadWork", BadWork.reduce());
    galois::ReportStatSingle("BFS", "EmptyWork", WLEmptyWork.reduce());
  }
}

template <bool CONCURRENT, typename T, typename P, typename R>
void
syncAlgo(Graph* graph, GNode source, const P& pushWrap, const R& edgeRange) {
  using Cont = typename std::conditional<
      CONCURRENT, galois::InsertBag<T>, galois::SerStack<T>>::type;
  using Loop = typename std::conditional<
      CONCURRENT, galois::DoAll, galois::StdForEach>::type;

  Loop loop;

  auto curr = std::make_unique<Cont>();
  auto next = std::make_unique<Cont>();

  Dist next_level = 0U;
  graph->GetData<NodeDistCurrent>(source) = 0U;

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
            auto& dest_data = graph->GetData<NodeDistCurrent>(*dest);

            if (dest_data == BFS::DIST_INFINITY) {
              dest_data = next_level;
              pushWrap(*next, *dest);
            }
          }
        },
        galois::steal(), galois::chunk_size<CHUNK_SIZE>(),
        galois::loopname("Sync"));
  }
}

template <bool CONCURRENT>
void
runAlgo(Graph* graph, const GNode& source) {
  switch (algo) {
  case AsyncTile:
    asyncAlgo<CONCURRENT, SrcEdgeTile>(
        graph, source, SrcEdgeTilePushWrap{graph}, TileRangeFn());
    break;
  case Async:
    asyncAlgo<CONCURRENT, UpdateRequest>(
        graph, source, ReqPushWrap(), OutEdgeRangeFn{graph});
    break;
  case SyncTile:
    syncAlgo<CONCURRENT, EdgeTile>(
        graph, source, EdgeTilePushWrap{graph}, TileRangeFn());
    break;
  case Sync:
    syncAlgo<CONCURRENT, GNode>(
        graph, source, NodePushWrap(), OutEdgeRangeFn{graph});
    break;
  default:
    std::cerr << "ERROR: unkown algo type\n";
  }
}

int
main(int argc, char** argv) {
  std::unique_ptr<galois::SharedMemSys> G =
      LonestarStart(argc, argv, name, desc, url, &inputFile);

  galois::StatTimer totalTime("TimerTotal");
  totalTime.start();

  std::cout << "Reading from file: " << inputFile << "\n";
  std::unique_ptr<galois::graphs::PropertyFileGraph> pfg =
      MakeFileGraph(inputFile, edge_property_name);
  auto result = ConstructNodeProperties<NodeData>(pfg.get());
  if (!result) {
    GALOIS_LOG_FATAL("failed to construct node properties: {}", result.error());
  }
  auto pg_result =
      galois::graphs::PropertyGraph<NodeData, EdgeData>::Make(pfg.get());
  if (!pg_result) {
    GALOIS_LOG_FATAL("could not make property graph: {}", pg_result.error());
  }
  Graph graph = pg_result.value();

  std::cout << "Read " << graph.num_nodes() << " nodes, " << graph.num_edges()
            << " edges\n";

  std::cout << "Running " << ALGO_NAMES[algo] << "\n";
  if (startNode >= graph.size() || reportNode >= graph.size()) {
    std::cerr << "failed to set report: " << reportNode
              << " or failed to set source: " << startNode << "\n";
    abort();
  }

  auto it = graph.begin();
  std::advance(it, startNode.getValue());
  GNode source = *it;
  it = graph.begin();
  std::advance(it, reportNode.getValue());
  GNode report = *it;

  size_t approxNodeData = 4 * (graph.num_nodes() + graph.num_edges());
  galois::Prealloc(8, approxNodeData);

  galois::reportPageAlloc("MeminfoPre");

  galois::do_all(galois::iterate(graph.begin(), graph.end()), [&graph](auto n) {
    graph.GetData<NodeDistCurrent>(n) = BFS::DIST_INFINITY;
  });

  std::cout << "Running " << ALGO_NAMES[algo] << " algorithm with "
            << (bool(execution) ? "PARALLEL" : "SERIAL") << " execution\n";

  galois::StatTimer execTime("Timer_0");
  execTime.start();

  if (execution == SERIAL) {
    runAlgo<false>(&graph, source);
  } else if (execution == PARALLEL) {
    runAlgo<true>(&graph, source);
  } else {
    std::cerr << "ERROR: unknown type of execution passed to -exec\n";
  }

  execTime.stop();

  galois::reportPageAlloc("MeminfoPost");

  std::cout << "Node " << reportNode << " has distance "
            << graph.GetData<NodeDistCurrent>(report) << "\n";

  // Sanity checking code
  galois::GReduceMax<uint64_t> max_dist;
  galois::GAccumulator<uint64_t> sum_dist;
  galois::GAccumulator<uint32_t> num_visited;
  max_dist.reset();
  sum_dist.reset();
  num_visited.reset();

  galois::do_all(
      galois::iterate(graph),
      [&](uint64_t i) {
        uint32_t my_distance = graph.GetData<NodeDistCurrent>(i);

        if (my_distance != BFS::DIST_INFINITY) {
          max_dist.update(my_distance);
          sum_dist += my_distance;
          num_visited += 1;
        }
      },
      galois::loopname("Sanity check"), galois::no_stats());

  // report sanity stats
  galois::gInfo("# visited nodes is ", num_visited.reduce());
  galois::gInfo("Max distance is ", max_dist.reduce());
  galois::gInfo("Sum of visited distances is ", sum_dist.reduce());

  if (!skipVerify) {
    if (BFS::verify<NodeDistCurrent>(&graph, source)) {
      std::cout << "Verification successful.\n";
    } else {
      GALOIS_DIE("verification failed");
    }
  }

  totalTime.stop();

  return 0;
}
