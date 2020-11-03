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

#include <iostream>

#include "Lonestar/BFS_SSSP.h"
#include "Lonestar/BoilerPlate.h"
#include "galois/AtomicHelpers.h"

namespace cll = llvm::cl;

static const char* name = "Single Source Shortest Path";
static const char* desc =
    "Computes the shortest path from a source node to all nodes in a directed "
    "graph using a modified chaotic iteration algorithm";
static const char* url = "single_source_shortest_path";

static cll::opt<std::string> inputFile(
    cll::Positional, cll::desc("<input file>"), cll::Required);
static cll::opt<unsigned int> startNode(
    "startNode", cll::desc("Node to start search from (default value 0)"),
    cll::init(0));
static cll::opt<unsigned int> reportNode(
    "reportNode", cll::desc("Node to report distance to(default value 1)"),
    cll::init(1));
static cll::opt<unsigned int> stepShift(
    "delta", cll::desc("Shift value for the deltastep (default value 13)"),
    cll::init(13));

enum Algo {
  deltaTile = 0,
  deltaStep,
  deltaStepBarrier,
  serDeltaTile,
  serDelta,
  dijkstraTile,
  dijkstra,
  topo,
  topoTile,
  AutoAlgo
};

const char* const ALGO_NAMES[] = {
    "deltaTile", "deltaStep",    "deltaStepBarrier", "serDeltaTile",
    "serDelta",  "dijkstraTile", "dijkstra",         "topo",
    "topoTile",  "Auto"};

static cll::opt<Algo> algo(
    "algo", cll::desc("Choose an algorithm (default value auto):"),
    cll::values(
        clEnumVal(deltaTile, "deltaTile"), clEnumVal(deltaStep, "deltaStep"),
        clEnumVal(deltaStepBarrier, "deltaStepBarrier"),
        clEnumVal(serDeltaTile, "serDeltaTile"),
        clEnumVal(serDelta, "serDelta"),
        clEnumVal(dijkstraTile, "dijkstraTile"),
        clEnumVal(dijkstra, "dijkstra"), clEnumVal(topo, "topo"),
        clEnumVal(topoTile, "topoTile"),
        clEnumVal(AutoAlgo, "auto: choose among the algorithms automatically")),
    cll::init(AutoAlgo));

//TODO (gill) Remove snippets from documentation
//! [withnumaalloc]
//! [withnumaalloc]

struct NodeDistCurrent {
  using ArrowType = arrow::CTypeTraits<uint32_t>::ArrowType;
  using ViewType = galois::PODPropertyView<std::atomic<uint32_t>>;
};

struct EdgeWeight : public galois::PODProperty<uint32_t> {};

using NodeData = std::tuple<NodeDistCurrent>;
using EdgeData = std::tuple<EdgeWeight>;

typedef galois::graphs::PropertyGraph<NodeData, EdgeData> Graph;
typedef typename Graph::Node GNode;

constexpr static const bool TRACK_WORK = false;
constexpr static const unsigned CHUNK_SIZE = 64U;
constexpr static const ptrdiff_t EDGE_TILE_SIZE = 512;

using SSSP = BFS_SSSP<Graph, uint32_t, true, EDGE_TILE_SIZE>;
using Dist = SSSP::Dist;
using UpdateRequest = SSSP::UpdateRequest;
using UpdateRequestIndexer = SSSP::UpdateRequestIndexer;
using SrcEdgeTile = SSSP::SrcEdgeTile;
using SrcEdgeTileMaker = SSSP::SrcEdgeTileMaker;
using SrcEdgeTilePushWrap = SSSP::SrcEdgeTilePushWrap;
using ReqPushWrap = SSSP::ReqPushWrap;
using OutEdgeRangeFn = SSSP::OutEdgeRangeFn;
using TileRangeFn = SSSP::TileRangeFn;

namespace gwl = galois::worklists;
using PSchunk = gwl::PerSocketChunkFIFO<CHUNK_SIZE>;
using OBIM = gwl::OrderedByIntegerMetric<UpdateRequestIndexer, PSchunk>;
using OBIM_Barrier = gwl::OrderedByIntegerMetric<
    UpdateRequestIndexer, PSchunk>::with_barrier<true>::type;

template <typename T, typename OBIMTy = OBIM, typename P, typename R>
void
deltaStepAlgo(
    Graph* graph, const GNode& source, const P& pushWrap, const R& edgeRange) {
  //! [reducible for self-defined stats]
  galois::GAccumulator<size_t> BadWork;
  //! [reducible for self-defined stats]
  galois::GAccumulator<size_t> WLEmptyWork;

  graph->GetData<NodeDistCurrent>(source) = 0;

  galois::InsertBag<T> init_bag;
  pushWrap(init_bag, source, 0, "parallel");

  galois::for_each(
      galois::iterate(init_bag),
      [&](const T& item, auto& ctx) {
        const auto& sdata = graph->GetData<NodeDistCurrent>(item.src);

        if (sdata < item.dist) {
          if (TRACK_WORK)
            WLEmptyWork += 1;
          return;
        }

        for (auto ii : edgeRange(item)) {
          auto dest = graph->GetEdgeDest(ii);
          auto& ddist = graph->GetData<NodeDistCurrent>(*dest);
          Dist ew = graph->GetEdgeData<EdgeWeight>(ii);
          const Dist new_dist = sdata + ew;
          Dist old_dist = galois::atomicMin(ddist, new_dist);
          if (new_dist < old_dist) {
            if (TRACK_WORK) {
              //! [per-thread contribution of self-defined stats]
              if (old_dist != SSSP::DIST_INFINITY) {
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

  if (TRACK_WORK) {
    //! [report self-defined stats]
    galois::ReportStatSingle("SSSP", "BadWork", BadWork.reduce());
    //! [report self-defined stats]
    galois::ReportStatSingle("SSSP", "WLEmptyWork", WLEmptyWork.reduce());
  }
}

template <typename T, typename P, typename R>
void
serDeltaAlgo(
    Graph* graph, const GNode& source, const P& pushWrap, const R& edgeRange) {
  SerialBucketWL<T, UpdateRequestIndexer> wl(UpdateRequestIndexer{stepShift});
  ;
  graph->GetData<NodeDistCurrent>(source) = 0;

  pushWrap(wl, source, 0);

  size_t iter = 0UL;
  while (!wl.empty()) {
    auto& curr = wl.minBucket();

    while (!curr.empty()) {
      ++iter;
      auto item = curr.front();
      curr.pop_front();

      if (graph->GetData<NodeDistCurrent>(item.src) < item.dist) {
        // empty work
        continue;
      }

      for (auto e : edgeRange(item)) {
        auto dest = graph->GetEdgeDest(e);
        auto& ddata = graph->GetData<NodeDistCurrent>(*dest);

        const auto new_dist = item.dist + graph->GetEdgeData<EdgeWeight>(e);

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
void
dijkstraAlgo(
    Graph* graph, const GNode& source, const P& pushWrap, const R& edgeRange) {
  using WL = galois::MinHeap<T>;

  graph->GetData<NodeDistCurrent>(source) = 0;

  WL wl;
  pushWrap(wl, source, 0);

  size_t iter = 0;

  while (!wl.empty()) {
    ++iter;

    T item = wl.pop();

    if (graph->GetData<NodeDistCurrent>(item.src) < item.dist) {
      // empty work
      continue;
    }

    for (auto e : edgeRange(item)) {
      auto dest = graph->GetEdgeDest(e);
      auto& ddata = graph->GetData<NodeDistCurrent>(*dest);

      const auto new_dist = item.dist + graph->GetEdgeData<EdgeWeight>(e);

      if (new_dist < ddata) {
        ddata = new_dist;
        pushWrap(wl, *dest, new_dist);
      }
    }
  }

  galois::ReportStatSingle("SSSP-Dijkstra", "Iterations", iter);
}

void
topoAlgo(Graph* graph, const GNode& source) {
  galois::LargeArray<Dist> old_dist;
  old_dist.allocateInterleaved(graph->size());

  constexpr Dist INFTY = SSSP::DIST_INFINITY;
  galois::do_all(
      galois::iterate(size_t{0}, graph->size()),
      [&](size_t i) { old_dist.constructAt(i, INFTY); }, galois::no_stats(),
      galois::loopname("initDistArray"));

  graph->GetData<NodeDistCurrent>(source) = 0;

  galois::GReduceLogicalOr changed;
  size_t rounds = 0;

  do {
    ++rounds;
    changed.reset();

    galois::do_all(
        galois::iterate(*graph),
        [&](const GNode& n) {
          const auto& sdata = graph->GetData<NodeDistCurrent>(n);

          if (old_dist[n] > sdata) {
            old_dist[n] = sdata;
            changed.update(true);

            for (auto e : graph->edges(n)) {
              const uint32_t new_dist =
                  sdata + graph->GetEdgeData<EdgeWeight>(e);
              auto dest = graph->GetEdgeDest(e);
              auto& ddata = graph->GetData<NodeDistCurrent>(*dest);
              galois::atomicMin(ddata, new_dist);
            }
          }
        },
        galois::steal(), galois::loopname("Update"));

  } while (changed.reduce());

  galois::ReportStatSingle("SSSP-topo", "rounds", rounds);
}

void
topoTileAlgo(Graph* graph, const GNode& source) {
  galois::InsertBag<SrcEdgeTile> tiles;

  graph->GetData<NodeDistCurrent>(source) = 0;

  galois::do_all(
      galois::iterate(*graph),
      [&](const GNode& n) {
        SSSP::pushEdgeTiles(
            tiles, graph, n, SrcEdgeTileMaker{n, SSSP::DIST_INFINITY});
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
          const auto& sdata = graph->GetData<NodeDistCurrent>(t.src);

          if (t.dist > sdata) {
            t.dist = sdata;
            changed.update(true);

            for (auto e = t.beg; e != t.end; ++e) {
              const uint32_t new_dist =
                  sdata + graph->GetEdgeData<EdgeWeight>(e);
              auto dest = graph->GetEdgeDest(e);
              auto& ddata = graph->GetData<NodeDistCurrent>(*dest);
              galois::atomicMin(ddata, new_dist);
            }
          }
        },
        galois::steal(), galois::loopname("Update"));

  } while (changed.reduce());

  galois::ReportStatSingle("SSSP-topo", "rounds", rounds);
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

  if (startNode >= graph.size() || reportNode >= graph.size()) {
    std::cerr << "failed to set report: " << reportNode
              << " or failed to set source: " << startNode << "\n";
    assert(0);
    abort();
  }

  auto it = graph.begin();
  std::advance(it, startNode.getValue());
  GNode source = *it;
  it = graph.begin();
  std::advance(it, reportNode.getValue());
  GNode report = *it;

  size_t approxNodeData = graph.size() * 64;
  galois::Prealloc(1, approxNodeData);
  galois::reportPageAlloc("MeminfoPre");

  if (algo == deltaStep || algo == deltaTile || algo == serDelta ||
      algo == serDeltaTile) {
    std::cout << "INFO: Using delta-step of " << (1 << stepShift) << "\n";
    std::cout
        << "WARNING: Performance varies considerably due to delta parameter.\n";
    std::cout
        << "WARNING: Do not expect the default to be good for your graph.\n";
  }

  galois::do_all(galois::iterate(graph), [&graph](const GNode& n) {
    graph.GetData<NodeDistCurrent>(n) = SSSP::DIST_INFINITY;
  });

  graph.GetData<NodeDistCurrent>(source) = 0;

  std::cout << "Running " << ALGO_NAMES[algo] << " algorithm\n";

  galois::StatTimer autoAlgoTimer("AutoAlgo_0");
  galois::StatTimer execTime("Timer_0");
  execTime.start();

  if (algo == AutoAlgo) {
    autoAlgoTimer.start();
    if (isApproximateDegreeDistributionPowerLaw(graph)) {
      algo = deltaStep;
    } else {
      algo = deltaStepBarrier;
    }
    autoAlgoTimer.stop();
    galois::gInfo("Choosing ", ALGO_NAMES[algo], " algorithm");
  }

  switch (algo) {
  case deltaTile:
    deltaStepAlgo<SrcEdgeTile>(
        &graph, source, SrcEdgeTilePushWrap{&graph}, TileRangeFn());
    break;
  case deltaStep:
    deltaStepAlgo<UpdateRequest>(
        &graph, source, ReqPushWrap(), OutEdgeRangeFn{&graph});
    break;
  case serDeltaTile:
    serDeltaAlgo<SrcEdgeTile>(
        &graph, source, SrcEdgeTilePushWrap{&graph}, TileRangeFn());
    break;
  case serDelta:
    serDeltaAlgo<UpdateRequest>(
        &graph, source, ReqPushWrap(), OutEdgeRangeFn{&graph});
    break;
  case dijkstraTile:
    dijkstraAlgo<SrcEdgeTile>(
        &graph, source, SrcEdgeTilePushWrap{&graph}, TileRangeFn());
    break;
  case dijkstra:
    dijkstraAlgo<UpdateRequest>(
        &graph, source, ReqPushWrap(), OutEdgeRangeFn{&graph});
    break;
  case topo:
    topoAlgo(&graph, source);
    break;
  case topoTile:
    topoTileAlgo(&graph, source);
    break;

  case deltaStepBarrier:
    deltaStepAlgo<UpdateRequest, OBIM_Barrier>(
        &graph, source, ReqPushWrap(), OutEdgeRangeFn{&graph});
    break;

  default:
    std::abort();
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

        if (my_distance != SSSP::DIST_INFINITY) {
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
    if (SSSP::verify<NodeDistCurrent, EdgeWeight>(&graph, source)) {
      std::cout << "Verification successful.\n";
    } else {
      GALOIS_DIE("verification failed");
    }
  }

  totalTime.stop();

  return 0;
}
