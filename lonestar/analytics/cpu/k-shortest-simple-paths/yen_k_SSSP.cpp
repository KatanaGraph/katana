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
#include "katana/Properties.h"
#include "katana/TypedPropertyGraph.h"

namespace cll = llvm::cl;

static const char* name = "Yen k Simple Shortest Paths";
static const char* desc =
    "Computes the k shortest simple paths from a source to a sink node in a "
    "directed "
    "graph";
static const char* url = "yen_k_simple_shortest_paths";

static cll::opt<std::string> inputFile(
    cll::Positional, cll::desc("<input file>"), cll::Required);
static cll::opt<unsigned int> startNode(
    "startNode", cll::desc("Node to start search from (default value 0)"),
    cll::init(1));
static cll::opt<unsigned int> reportNode(
    "reportNode", cll::desc("Node to report distance to(default value 1)"),
    cll::init(1));
static cll::opt<unsigned int> stepShift(
    "delta", cll::desc("Shift value for the deltastep (default value 13)"),
    cll::init(13));
static cll::opt<unsigned int> numPaths(
    "numPaths",
    cll::desc("Number of paths to compute from source to report node (default "
              "value 10)"),
    cll::init(10));

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
  uint32_t w;
  const Path* last;
};

struct NodeDist : public katana::AtomicPODProperty<uint32_t> {};

struct NodeAlive : public katana::PODProperty<uint8_t> {};

struct EdgeWeight : public katana::PODProperty<uint32_t> {};

using NodeData = std::tuple<NodeDist, NodeAlive>;
using EdgeData = std::tuple<EdgeWeight>;

typedef katana::TypedPropertyGraph<NodeData, EdgeData> Graph;
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

namespace gwl = katana;
using PSchunk = gwl::PerSocketChunkFIFO<kChunkSize>;
using OBIM = gwl::OrderedByIntegerMetric<UpdateRequestIndexer, PSchunk>;
using OBIM_Barrier = gwl::OrderedByIntegerMetric<
    UpdateRequestIndexer, PSchunk>::with_barrier<true>::type;

//delta stepping implementation for finding a shortest path from source to report node
template <typename Item, typename OBIMTy, typename PushWrap, typename EdgeRange>
bool
DeltaStepAlgo(
    Graph* graph, const GNode& source, const GNode& report,
    const PushWrap& pushWrap, const EdgeRange& edgeRange,
    std::vector<std::pair<GNode, uint32_t>>& cur_path, uint32_t prefix_wt,
    std::set<GNode>& remove_edges) {
  katana::do_all(katana::iterate(*graph), [&graph](const GNode n) {
    graph->GetData<NodeDist>(n) = SSSP::kDistInfinity;
  });

  //! [reducible for self-defined stats]
  katana::GAccumulator<size_t> bad_work;
  //! [reducible for self-defined stats]
  katana::GAccumulator<size_t> wl_empty_work;

  graph->GetData<NodeDist>(source) = 0;

  katana::InsertBag<Item> init_bag;
  katana::InsertBag<Path*> paths_bag;

  Path* path = new Path();
  path->last = NULL;
  path->w = 0;
  paths_bag.push(path);

  pushWrap(init_bag, source, 0, path, "parallel");

  katana::InsertBag<std::pair<uint32_t, const Path*>> report_paths;

  //add candidate paths corresponding to the neighbors of the source node
  for (auto edge : graph->OutEdges(source)) {
    auto dest = graph->OutEdgeDst(edge);

    //check if dest is alive; otherwise dest is already in the root-path
    if (graph->GetData<NodeAlive>(dest) == (uint8_t)0) {
      continue;
    }

    //check if edge to dest has been removed or not
    //this ensures that we do not output an already computed path
    if (remove_edges.find(dest) == remove_edges.end()) {
      auto wt = graph->GetEdgeData<EdgeWeight>(edge);
      Path* path_dest;
      path_dest = new Path();
      path_dest->parent = source;
      path_dest->last = path;
      path_dest->w = wt;

      paths_bag.push(path_dest);

      pushWrap(init_bag, dest, wt, path_dest);

      graph->GetData<NodeDist>(dest) = wt;
      if (dest == report) {
        report_paths.push(std::make_pair(wt, path_dest));
      }
    }
  }

  //find shortest distances from source to every node
  katana::for_each(
      katana::iterate(init_bag),
      [&](const Item& item, auto& ctx) {
        if (item.src == source) {
          return;
        }

        const auto& src_dist = graph->GetData<NodeDist>(item.src);

        //check if this source already has a better shortest path distance value
        if (src_dist < item.distance) {
          if (kTrackWork) {
            wl_empty_work += 1;
          }
          return;
        }

        for (auto ii : edgeRange(item)) {
          auto dest = graph->OutEdgeDst(ii);
          auto& ddist = graph->GetData<NodeDist>(dest);

          if (graph->GetData<NodeAlive>(dest) == (uint8_t)0) {
            continue;
          }

          Distance ew = graph->GetEdgeData<EdgeWeight>(ii);
          const Distance new_dist = item.distance + ew;
          Distance old_dist = katana::atomicMin<uint32_t>(ddist, new_dist);

          if (new_dist < old_dist) {
            if (kTrackWork) {
              if (old_dist != SSSP::kDistInfinity) {
                bad_work += 1;
              }
            }

            Path* path;
            path = new Path();
            path->parent = item.src;
            path->last = item.path;
            path->w = new_dist;

            paths_bag.push(path);

            const Path* const_path = path;
            pushWrap(ctx, dest, new_dist, const_path);

            if (dest == report) {
              report_paths.push(std::make_pair(new_dist, const_path));
            }
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

  bool path_exists = false;

  //if a shortest path exists to the report node, then append it to cur_path
  if (report_paths.begin() != report_paths.end()) {
    path_exists = true;

    std::map<uint32_t, const Path*> map_paths;

    for (auto pair : report_paths) {
      map_paths.insert(std::make_pair(pair.first, pair.second));
    }

    const Path* path = (map_paths.begin())->second;

    std::vector<std::pair<GNode, uint32_t>> nodes;
    nodes.push_back(std::make_pair(report, prefix_wt + path->w));

    while (path->last != NULL) {
      nodes.push_back(std::make_pair(path->parent, prefix_wt + path->last->w));
      path = path->last;
    }

    uint32_t len = nodes.size();
    for (int32_t i = (len - 1); i >= 0; i--) {
      cur_path.push_back(nodes[i]);
    }
  }

  katana::do_all(katana::iterate(paths_bag), [&](Path* path) {
    if (path != NULL) {
      delete (path);
    }
  });

  return path_exists;
}

//finds a shortest path from source to report node
bool
FindShortestPath(
    Graph* graph, const GNode& source, const GNode& report,
    std::vector<std::pair<GNode, uint32_t>>& shortest_path, uint32_t prefix_wt,
    std::set<GNode>& remove_edges) {
  bool path_exists = true;

  switch (algo) {
  case deltaTile:
    path_exists = DeltaStepAlgo<SrcEdgeTile, OBIM>(
        graph, source, report, SrcEdgeTilePushWrap{graph}, TileRangeFn(),
        shortest_path, prefix_wt, remove_edges);
    break;
  case deltaStep:
    path_exists = DeltaStepAlgo<UpdateRequest, OBIM>(
        graph, source, report, ReqPushWrap(), OutEdgeRangeFn{graph},
        shortest_path, prefix_wt, remove_edges);
    break;
  case deltaStepBarrier:
    path_exists = DeltaStepAlgo<UpdateRequest, OBIM_Barrier>(
        graph, source, report, ReqPushWrap(), OutEdgeRangeFn{graph},
        shortest_path, prefix_wt, remove_edges);
    break;

  default:
    std::abort();
  }

  return path_exists;
}

//find the next shortest simple path from source to report node
bool
FindNextPath(
    std::multimap<uint32_t, std::vector<std::pair<GNode, uint32_t>>>*
        candidates,
    std::vector<std::vector<std::pair<GNode, uint32_t>>>* k_paths) {
  bool next_path = true;

  while (true) {
    if (candidates->begin() == candidates->end()) {
      next_path = false;
      break;
    }

    auto candidate_pair = *(candidates->begin());

    uint32_t candidate_wt = candidate_pair.first;
    std::vector<std::pair<GNode, uint32_t>>* candidate_path =
        &(candidate_pair.second);

    uint32_t candidate_len = candidate_path->size();

    // need to check if this candidate path has not been picked before
    bool is_same = false;

    katana::do_all(
        katana::iterate(*k_paths),
        [&](std::vector<std::pair<GNode, uint32_t>> path) {
          uint32_t wt_path = (path.rbegin())->second;
          if (candidate_wt != wt_path) {
            return;
          }

          if (candidate_len != path.size()) {
            return;
          }

          // check if path is also same or not
          for (uint32_t i = 0; i < candidate_len; i++) {
            if ((*candidate_path)[i].first != path[i].first) {
              return;
            }
          }

          is_same = true;
        });

    if (is_same) {
      auto it = candidates->begin();
      candidates->erase(it);
    } else {
      k_paths->push_back(*candidate_path);
      auto it = candidates->begin();
      candidates->erase(it);
      break;
    }  //end if-else
  }    // end while

  return next_path;
}

//find k simple shortest paths from source to report node
void
YenKSP(
    Graph* graph, const GNode& source, const GNode& report,
    std::vector<std::vector<std::pair<GNode, uint32_t>>>& k_paths) {
  std::vector<std::pair<GNode, uint32_t>> shortest_path;

  std::set<GNode> remove_edges;

  //find the shortest path first
  bool path_exists =
      FindShortestPath(graph, source, report, shortest_path, 0, remove_edges);

  if (!path_exists) {
    katana::gPrint("no shortest path exists from source to sink \n");
    return;
  }

  k_paths.push_back(shortest_path);

  // store candidate paths
  std::vector<std::vector<std::pair<GNode, uint32_t>>> candidate_paths;
  std::multimap<uint32_t, std::vector<std::pair<GNode, uint32_t>>> candidates;

  //find k paths one by one
  for (uint32_t k = 1; k < numPaths; k++) {
    uint32_t len = k_paths[k - 1].size();

    for (uint32_t i = 0; i <= (len - 2); i++) {
      // Remove the links that are part of the previous shortest paths which
      // share the same root path;
      remove_edges.clear();

      for (auto path : k_paths) {
        bool is_same = true;
        katana::do_all(
            katana::iterate((uint32_t)0, (uint32_t)(i + 1)), [&](uint32_t l) {
              if (path[l].first != k_paths[k - 1][l].first) {
                is_same = false;
              }
            });

        if (is_same) {
          remove_edges.insert(path[i + 1].first);
        }
      }

      katana::do_all(katana::iterate((uint32_t)0, i), [&](uint32_t l) {
        graph->GetData<NodeAlive>(k_paths[k - 1][l].first) = (uint8_t)0;
      });

      // Calculate the spur path from the i-th node to the report node.
      std::vector<std::pair<GNode, uint32_t>> cur_path;

      for (uint32_t l = 0; l < i; l++) {
        cur_path.push_back(k_paths[k - 1][l]);
      }

      GNode new_source = k_paths[k - 1][i].first;
      uint32_t prefix_wt = k_paths[k - 1][i].second;

      //find a shortest path from i-th node to the report node
      path_exists = FindShortestPath(
          graph, new_source, report, cur_path, prefix_wt, remove_edges);

      //add this new path to the candidates set
      if (path_exists) {
        candidate_paths.push_back(cur_path);
        uint32_t wt = (cur_path.rbegin())->second;

        candidates.insert(std::make_pair(wt, cur_path));
      }
    }

    katana::do_all(katana::iterate((uint32_t)0, len), [&](uint32_t l) {
      graph->GetData<NodeAlive>(k_paths[k - 1][l].first) = true;
    });

    // pick a new path and add it to k
    bool next_path = FindNextPath(&candidates, &k_paths);

    if (!next_path) {
      break;
    }
  }  // end for
}

//print k paths
void
PrintKPaths(std::vector<std::vector<std::pair<GNode, uint32_t>>>& k_paths) {
  uint32_t len = k_paths.size();

  katana::gPrint("k paths: \n");

  for (uint32_t i = 0; i < len; i++) {
    uint32_t path_len = k_paths[i].size();

    for (uint32_t j = 0; j < path_len; j++) {
      katana::gPrint(" ", k_paths[i][j].first);
    }

    katana::gPrint(" weight: ", k_paths[i][path_len - 1].second, "\n");
  }
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

  katana::TxnContext txn_ctx;
  auto result = ConstructNodeProperties<NodeData>(pg.get(), &txn_ctx);
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
      "Read ", graph.NumNodes(), " nodes, ", graph.NumEdges(), " edges\n");

  if (startNode >= graph.size() || reportNode >= graph.size()) {
    KATANA_LOG_ERROR(
        "failed to set report: ", reportNode,
        " or failed to set source: ", startNode, "\n");
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
  katana::ReportPageAllocGuard page_alloc;

  if (algo == deltaStep || algo == deltaTile) {
    katana::gInfo("Using delta-step of ", (1 << stepShift), "\n");
    KATANA_LOG_WARN(
        "Performance varies considerably due to delta parameter.\n");
    KATANA_LOG_WARN("Do not expect the default to be good for your graph.\n");
  }

  katana::do_all(katana::iterate(graph), [&graph](GNode n) {
    graph.GetData<NodeDist>(n) = SSSP::kDistInfinity;
    graph.GetData<NodeAlive>(n) = (uint8_t)1;
  });

  katana::gInfo("Running ", ALGO_NAMES[algo], " algorithm\n");

  katana::StatTimer execTime("SSSP");
  execTime.start();

  std::vector<std::vector<std::pair<GNode, uint32_t>>> k_paths;
  YenKSP(&graph, source, report, k_paths);

  execTime.stop();
  page_alloc.Report();

  PrintKPaths(k_paths);

  totalTime.stop();

  return 0;
}
