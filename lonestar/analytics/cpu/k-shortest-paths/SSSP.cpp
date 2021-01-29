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

#include <map>

#include "Lonestar/BoilerPlate.h"
#include "Lonestar/K_SSSP.h"
#include "katana/AtomicHelpers.h"
#include "katana/TypedPropertyGraph.h"
#include "katana/analytics/BfsSsspImplementationBase.h"

using namespace katana::analytics;

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
enum AlgoSSSP { deltaTile = 0, deltaStep, deltaStepBarrier };

const char* const ALGO_NAMES_SSSP[] = {
    "deltaTile", "deltaStep", "deltaStepBarrier"};

static cll::opt<AlgoSSSP> algoSSSP(
    "algoSSSP", cll::desc("Choose an algorithm for SSSP:"),
    cll::values(
        clEnumVal(deltaTile, "deltaTile"), clEnumVal(deltaStep, "deltaStep"),
        clEnumVal(deltaStepBarrier, "deltaStepBarrier")),
    cll::init(deltaTile));

enum AlgoReachability { async = 0, syncLevel };

static cll::opt<AlgoReachability> algoReachability(
    "algoReachability", cll::desc("Choose an algorithm for reachability:"),
    cll::values(clEnumVal(async, "async"), clEnumVal(syncLevel, "syncLevel")),
    cll::init(syncLevel));

struct Path {
  uint32_t parent;
  const Path* last{nullptr};
};

struct NodeCount {
  using ArrowType = arrow::CTypeTraits<uint32_t>::ArrowType;
  using ViewType = katana::PODPropertyView<std::atomic<uint32_t>>;
};

struct NodeMax {
  using ArrowType = arrow::CTypeTraits<uint32_t>::ArrowType;
  using ViewType = katana::PODPropertyView<std::atomic<uint32_t>>;
};

using EdgeWeight = katana::UInt32Property;

using NodeData = std::tuple<NodeCount, NodeMax>;
using EdgeData = std::tuple<EdgeWeight>;

typedef katana::TypedPropertyGraph<NodeData, EdgeData> Graph;
typedef typename Graph::Node GNode;

constexpr static const bool kTrackWork = false;
constexpr static const unsigned kChunkSize = 64U;
constexpr static const ptrdiff_t kEdgeTileSize = 512;

using Distance = uint32_t;
using SSSP = K_SSSP<Graph, Distance, const Path, true, kEdgeTileSize>;
using SSSPUpdateRequest = SSSP::UpdateRequest;
using SSSPUpdateRequestIndexer = SSSP::UpdateRequestIndexer;
using SSSPSrcEdgeTile = SSSP::SrcEdgeTile;
using SSSPSrcEdgeTileMaker = SSSP::SrcEdgeTileMaker;
using SSSPSrcEdgeTilePushWrap = SSSP::SrcEdgeTilePushWrap;
using SSSPReqPushWrap = SSSP::ReqPushWrap;
using SSSPOutEdgeRangeFn = SSSP::OutEdgeRangeFn;
using SSSPTileRangeFn = SSSP::TileRangeFn;

using PSchunk = katana::PerSocketChunkFIFO<kChunkSize>;
using OBIM = katana::OrderedByIntegerMetric<SSSPUpdateRequestIndexer, PSchunk>;
using OBIM_Barrier = katana::OrderedByIntegerMetric<
    SSSPUpdateRequestIndexer, PSchunk>::with_barrier<true>::type;

using BFS = BfsSsspImplementationBase<Graph, unsigned int, false>;
using BFSUpdateRequest = BFS::UpdateRequest;
using BFSSrcEdgeTile = BFS::SrcEdgeTile;
using BFSSrcEdgeTileMaker = BFS::SrcEdgeTileMaker;
using BFSSrcEdgeTilePushWrap = BFS::SrcEdgeTilePushWrap;
using BFSReqPushWrap = BFS::ReqPushWrap;
using BFSOutEdgeRangeFn = BFS::OutEdgeRangeFn;
using BFSTileRangeFn = BFS::TileRangeFn;

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
bool
CheckReachabilityAsync(
    Graph* graph, const GNode& source, const PushWrap& pushWrap,
    const EdgeRange& edgeRange) {
  using FIFO = katana::PerSocketChunkFIFO<kChunkSize>;
  using WL = FIFO;

  using Loop = typename std::conditional<
      true, katana::ForEach, katana::WhileQ<katana::SerFIFO<Item>>>::type;

  Loop loop;

  graph->GetData<NodeCount>(source) = 1;
  katana::InsertBag<Item> initBag;

  pushWrap(initBag, source, 1, "parallel");

  loop(
      katana::iterate(initBag),
      [&](const Item& item, auto& ctx) {
        for (auto ii : edgeRange(item)) {
          GNode dst = *(graph->GetEdgeDest(ii));
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

bool
CheckReachabilitySync(Graph* graph, const GNode& source) {
  katana::InsertBag<GNode> current_bag;
  katana::InsertBag<GNode> next_bag;

  current_bag.push(source);
  graph->GetData<NodeCount>(source) = 1;

  while (current_bag.begin() != current_bag.end()) {
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
    katana::InsertBag<std::pair<uint32_t, Path*>>* report_paths_bag,
    katana::InsertBag<Path*>* path_pointers, PathAlloc& path_alloc) {
  //! [reducible for self-defined stats]
  katana::GAccumulator<size_t> bad_work;
  //! [reducible for self-defined stats]
  katana::GAccumulator<size_t> wl_empty_work;

  graph->GetData<NodeCount>(source) = 1;

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
          GNode dst = *(graph->GetEdgeDest(ii));
          auto& ddata_count = graph->GetData<NodeCount>(dst);
          auto& ddata_max = graph->GetData<NodeMax>(dst);

          Distance ew = graph->GetEdgeData<EdgeWeight>(ii);
          const Distance new_dist = item.distance + ew;

          if ((ddata_count >= numPaths) && (ddata_max <= new_dist))
            continue;

          Path* path = path_alloc.NewPath();
          path->parent = item.src;
          path->last = item.path;
          path_pointers->push(path);

          if (ddata_count < numPaths) {
            katana::atomicAdd<uint32_t>(ddata_count, (uint32_t)1);
            katana::atomicMax<uint32_t>(ddata_max, new_dist);
          }

          if (dst == reportNode) {
            report_paths_bag->push(std::make_pair(new_dist, path));
          }

          //check if this new extended path needs to be added to the worklist
          bool should_add =
              (graph->GetData<NodeCount>(reportNode) < numPaths) ||
              ((graph->GetData<NodeCount>(reportNode) >= numPaths) &&
               (graph->GetData<NodeMax>(reportNode) > new_dist));

          if (should_add) {
            const Path* const_path = path;
            pushWrap(ctx, dst, new_dist, const_path);
          }
        }
      },
      katana::wl<OBIM>(SSSPUpdateRequestIndexer{stepShift}),
      katana::disable_conflict_detection(), katana::loopname("SSSP"));

  if (kTrackWork) {
    //! [report self-defined stats]
    katana::ReportStatSingle("SSSP", "BadWork", bad_work.reduce());
    //! [report self-defined stats]
    katana::ReportStatSingle("SSSP", "WLEmptyWork", wl_empty_work.reduce());
  }
}

void
PrintPath(const Path* path) {
  if (path->last != nullptr) {
    PrintPath(path->last);
  }

  katana::gPrint(" ", path->parent);
}

void
Initialize(Graph* graph) {
  katana::do_all(katana::iterate(*graph), [&graph](GNode n) {
    graph->GetData<NodeMax>(n) = 0;
    graph->GetData<NodeCount>(n) = 0;
  });
}

int
main(int argc, char** argv) {
  std::unique_ptr<katana::SharedMemSys> G =
      LonestarStart(argc, argv, name, desc, url, &inputFile);

  katana::StatTimer totalTime("TimerTotal");
  totalTime.start();

  katana::gInfo("Reading from file: ", inputFile, "\n");
  std::unique_ptr<katana::PropertyGraph> pg =
      MakeFileGraph(inputFile, edge_property_name);

  auto result = ConstructNodeProperties<NodeData>(pg.get());
  if (!result) {
    KATANA_LOG_FATAL("failed to construct node properties: {}", result.error());
  }

  auto pg_result =
      katana::TypedPropertyGraph<NodeData, EdgeData>::Make(pg.get());
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

  if (algoSSSP == deltaStep || algoSSSP == deltaTile) {
    katana::gInfo("Using delta-step of ", (1 << stepShift), "\n");
    KATANA_LOG_WARN(
        "Performance varies considerably due to delta parameter.\n");
    KATANA_LOG_WARN("Do not expect the default to be good for your graph.\n");
  }

  Initialize(&graph);

  katana::gInfo("Running ", ALGO_NAMES_SSSP[algoSSSP], " algorithm\n");

  katana::StatTimer execTime("Timer_0");
  execTime.start();

  katana::InsertBag<std::pair<uint32_t, Path*>> paths;
  katana::InsertBag<Path*> path_pointers;

  bool reachable = true;

  switch (algoReachability) {
  case async:
    reachable = CheckReachabilityAsync<BFSUpdateRequest>(
        &graph, source, BFSReqPushWrap(), BFSOutEdgeRangeFn{&graph});
    break;
  case syncLevel:
    reachable = CheckReachabilitySync(&graph, source);
    break;
  default:
    std::abort();
  }

  PathAlloc path_alloc;

  if (reachable) {
    switch (algoSSSP) {
    case deltaTile:
      DeltaStepAlgo<SSSPSrcEdgeTile, OBIM>(
          &graph, source, SSSPSrcEdgeTilePushWrap{&graph}, SSSPTileRangeFn(),
          &paths, &path_pointers, path_alloc);
      break;
    case deltaStep:
      DeltaStepAlgo<SSSPUpdateRequest, OBIM>(
          &graph, source, SSSPReqPushWrap(), SSSPOutEdgeRangeFn{&graph}, &paths,
          &path_pointers, path_alloc);
      break;
    case deltaStepBarrier:
      katana::gInfo("Using OBIM with barrier\n");
      DeltaStepAlgo<SSSPUpdateRequest, OBIM_Barrier>(
          &graph, source, SSSPReqPushWrap(), SSSPOutEdgeRangeFn{&graph}, &paths,
          &path_pointers, path_alloc);
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
  totalTime.stop();

  return 0;
}
