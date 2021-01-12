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
#include <map>

#include "Lonestar/BoilerPlate.h"
#include "Lonestar/K_SSSP.h"
#include "katana/AtomicHelpers.h"

namespace cll = llvm::cl;

static const char* name = "Single Source k Shortest Paths";
static const char* desc =
    "Computes the k shortest paths from a source node to all nodes in a "
    "directed "
    "graph using a modified chaotic iteration algorithm";
static const char* url = "k_shortest_paths";

static cll::opt<std::string> inputFile(
    cll::Positional, cll::desc("<input file>"), cll::Required);
static cll::opt<unsigned int> startNode(
    "startNode", cll::desc("Node to start search from (default value 0)"),
    cll::init(0));
static cll::opt<unsigned int> reportNode(
    "reportNode", cll::desc("Node to report distance to (default value 1)"),
    cll::init(0));
static cll::opt<unsigned int> stepShift(
    "delta", cll::desc("Shift value for the deltastep (default value 13)"),
    cll::init(13));
static cll::opt<unsigned int> numPaths(
    "numPaths",
    cll::desc("Number of paths to compute from source to report node (default "
              "value 1)"),
    cll::init(1));
enum Algo { deltaTile = 0, deltaStep, deltaStepBarrier };

const char* const ALGO_NAMES[] = {"deltaTile", "deltaStep", "deltaStepBarrier"};

static cll::opt<Algo> algo(
    "algo", cll::desc("Choose an algorithm:"),
    cll::values(
        clEnumVal(deltaTile, "deltaTile"), clEnumVal(deltaStep, "deltaStep"),
        clEnumVal(deltaStepBarrier, "deltaStepBarrier")),
    cll::init(deltaTile));

struct Path {
  uint32_t parent;
  const Path* last;
};

struct NodeCount {
  using ArrowType = arrow::CTypeTraits<uint32_t>::ArrowType;
  using ViewType = katana::PODPropertyView<std::atomic<uint32_t>>;
};

struct NodeMax {
  using ArrowType = arrow::CTypeTraits<uint32_t>::ArrowType;
  using ViewType = katana::PODPropertyView<std::atomic<uint32_t>>;
};

struct EdgeWeight : public katana::PODProperty<uint32_t> {};

using NodeData = std::tuple<NodeCount, NodeMax>;
using EdgeData = std::tuple<EdgeWeight>;

typedef katana::PropertyGraph<NodeData, EdgeData> Graph;
typedef typename Graph::Node GNode;

constexpr static const bool kTrackWork = false;
constexpr static const unsigned kChunkSize = 64U;
constexpr static const ptrdiff_t kEdgeTileSize = 512;

using Distance = uint32_t;
using SSSP = K_SSSP<Graph, Distance, const Path, true, kEdgeTileSize>;
using UpdateRequest = SSSP::UpdateRequest;
using UpdateRequestIndexer = SSSP::UpdateRequestIndexer;
using SrcEdgeTile = SSSP::SrcEdgeTile;
using SrcEdgeTileMaker = SSSP::SrcEdgeTileMaker;
using SrcEdgeTilePushWrap = SSSP::SrcEdgeTilePushWrap;
using ReqPushWrap = SSSP::ReqPushWrap;
using OutEdgeRangeFn = SSSP::OutEdgeRangeFn;
using TileRangeFn = SSSP::TileRangeFn;

using PSchunk = katana::PerSocketChunkFIFO<kChunkSize>;
using OBIM = katana::OrderedByIntegerMetric<UpdateRequestIndexer, PSchunk>;
using OBIM_Barrier = katana::OrderedByIntegerMetric<
    UpdateRequestIndexer, PSchunk>::with_barrier<true>::type;

bool
CheckIfReachable(Graph* graph, const GNode& source) {
  katana::InsertBag<GNode> current_bag;
  katana::InsertBag<GNode> next_bag;

  current_bag.push(source);
  graph->GetData<NodeCount>(source) = 1;

  while (true) {
    if (current_bag.begin() == current_bag.end())
      break;

    katana::do_all(
        katana::iterate(current_bag),
        [&](GNode n) {
          for (auto edge : graph->edges(n)) {
            auto dest = *(graph->GetEdgeDest(edge));
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
template <typename Item, typename OBIMTy, typename PushWrap, typename EdgeRange>
void
DeltaStepAlgo(
    Graph* graph, const GNode& source, const PushWrap& pushWrap,
    const EdgeRange& edgeRange,
    katana::InsertBag<std::pair<uint32_t, Path*>>* bag,
    katana::InsertBag<Path*>* path_pointers) {
  //! [reducible for self-defined stats]
  katana::GAccumulator<size_t> bad_work;
  //! [reducible for self-defined stats]
  katana::GAccumulator<size_t> wl_empty_work;

  graph->GetData<NodeCount>(source) = 1;

  katana::InsertBag<Item> init_bag;

  Path* path = new Path();
  path->last = NULL;
  path_pointers->push(path);

  pushWrap(init_bag, source, 0, path, "parallel");

  katana::for_each(
      katana::iterate(init_bag),
      [&](const Item& item, auto& ctx) {
        for (auto ii : edgeRange(item)) {
          GNode dst = *(graph->GetEdgeDest(ii));
          auto& ddata_count = graph->GetData<NodeCount>(dst);
          auto& ddata_max = graph->GetData<NodeMax>(dst);

          Distance ew = graph->GetEdgeData<EdgeWeight>(ii);
          const Distance new_dist = item.distance + ew;

          if ((ddata_count >= numPaths) && (ddata_max <= new_dist))
            continue;

          Path* path;
          path = new Path();
          path->parent = item.src;
          path->last = item.path;
          path_pointers->push(path);

          if (ddata_count < numPaths) {
            katana::atomicAdd<uint32_t>(ddata_count, (uint32_t)1);
            katana::atomicMax<uint32_t>(ddata_max, new_dist);
          }

          if (dst == reportNode)
            bag->push(std::make_pair(new_dist, path));

          if ((graph->GetData<NodeCount>(reportNode) < numPaths) ||
              ((graph->GetData<NodeCount>(reportNode) >= numPaths) &&
               (graph->GetData<NodeMax>(reportNode) > new_dist))) {
            const Path* const_path = path;
            pushWrap(ctx, dst, new_dist, const_path);
          }
        }
      },
      katana::wl<OBIM>(UpdateRequestIndexer{stepShift}),
      katana::disable_conflict_detection(), katana::loopname("SSSP"));

  if (kTrackWork) {
    //! [report self-defined stats]
    katana::ReportStatSingle("SSSP", "BadWork", bad_work.reduce());
    //! [report self-defined stats]
    katana::ReportStatSingle("SSSP", "WLEmptyWork", wl_empty_work.reduce());
  }
}

int
main(int argc, char** argv) {
  std::unique_ptr<katana::SharedMemSys> G =
      LonestarStart(argc, argv, name, desc, url, &inputFile);

  katana::StatTimer totalTime("TimerTotal");
  totalTime.start();

  katana::gInfo("Reading from file: ", inputFile, "\n");
  std::unique_ptr<katana::PropertyFileGraph> pfg =
      MakeFileGraph(inputFile, edge_property_name);

  auto result = ConstructNodeProperties<NodeData>(pfg.get());
  if (!result) {
    KATANA_LOG_FATAL("failed to construct node properties: {}", result.error());
  }

  auto pg_result =
      katana::PropertyGraph<NodeData, EdgeData>::Make(pfg.get());
  if (!pg_result) {
    KATANA_LOG_FATAL("could not make property graph: {}", pg_result.error());
  }
  Graph graph = pg_result.value();

  katana::gPrint(
      "Read ", graph.num_nodes(), " nodes, ", graph.num_edges(), " edges\n");

  if (startNode >= graph.size() || reportNode >= graph.size()) {
    KATANA_LOG_ERROR(
        "failed to set report: ", reportNode,
        " or failed to set source: ", startNode, "\n");
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
  katana::Prealloc(1, approxNodeData);
  katana::reportPageAlloc("MeminfoPre");

  if (algo == deltaStep || algo == deltaTile) {
    katana::gInfo("Using delta-step of ", (1 << stepShift), "\n");
    KATANA_LOG_WARN(
        "Performance varies considerably due to delta parameter.\n");
    KATANA_LOG_WARN("Do not expect the default to be good for your graph.\n");
  }

  katana::do_all(katana::iterate(graph), [&graph](GNode n) {
    graph.GetData<NodeMax>(n) = 0;
    graph.GetData<NodeCount>(n) = 0;
  });

  katana::gInfo("Running ", ALGO_NAMES[algo], " algorithm\n");

  katana::StatTimer execTime("Timer_0");
  execTime.start();

  katana::InsertBag<std::pair<uint32_t, Path*>> paths;
  katana::InsertBag<Path*> path_pointers;

  bool reachable = CheckIfReachable(&graph, source);

  if (reachable) {
    switch (algo) {
    case deltaTile:
      DeltaStepAlgo<SrcEdgeTile, OBIM>(
          &graph, source, SrcEdgeTilePushWrap{&graph}, TileRangeFn(), &paths,
          &path_pointers);
      break;
    case deltaStep:
      DeltaStepAlgo<UpdateRequest, OBIM>(
          &graph, source, ReqPushWrap(), OutEdgeRangeFn{&graph}, &paths,
          &path_pointers);
      break;
    case deltaStepBarrier:
      katana::gInfo("Using OBIM with barrier\n");
      DeltaStepAlgo<UpdateRequest, OBIM_Barrier>(
          &graph, source, ReqPushWrap(), OutEdgeRangeFn{&graph}, &paths,
          &path_pointers);
      break;

    default:
      std::abort();
    }
  }

  execTime.stop();

  if (reachable) {
    std::multimap<uint32_t, Path*> paths_map;

    for (auto pair : paths) {
      paths_map.insert(std::make_pair(pair.first, pair.second));
    }

    katana::reportPageAlloc("MeminfoPost");

    katana::gPrint("Node ", report, " has these k paths:\n");

    uint32_t num = (paths_map.size() > numPaths) ? numPaths : paths_map.size();

    auto it_report = paths_map.begin();

    for (uint32_t iter = 0; iter < num; iter++) {
      katana::gPrint(" ", report);
      GNode parent = report;

      const Path* path = it_report->second;
      it_report++;

      while (path->last != NULL) {
        parent = path->parent;
        katana::gPrint(" ", parent);
        path = path->last;
      }
      katana::gPrint("\n");
    }

    katana::do_all(
        katana::iterate(path_pointers), [&](Path* p) { delete (p); });
  }
  totalTime.stop();

  return 0;
}
