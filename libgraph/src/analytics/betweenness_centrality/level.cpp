#include "betweenness_centrality_impl.h"
#include "katana/AtomicHelpers.h"
#include "katana/DynamicBitset.h"
#include "katana/NUMAArray.h"
#include "katana/Properties.h"
#include "katana/TypedPropertyGraph.h"

using namespace katana::analytics;

namespace {

// type of the num shortest paths variable
using LevelShortPathType = double;

constexpr static uint32_t kInfinity = std::numeric_limits<uint32_t>::max();

// NOTE: types assume that these values will not reach uint64_t: it may
// need to be changed for very large graphs
struct BCLevelNodeDataTy {
  std::atomic<uint32_t> current_dist;
  std::atomic<LevelShortPathType> num_shortest_paths;
  float dependency;
  float bc;
};
struct NodeBC : public katana::PODProperty<float> {};
using NodeDataLevel = std::tuple<>;
using EdgeDataLevel = std::tuple<>;

using LevelGraph = katana::TypedPropertyGraphView<
    katana::PropertyGraphViews::Default, NodeDataLevel, EdgeDataLevel>;
using LevelGNode = typename LevelGraph::Node;

using BCLevelNodeDataArray = katana::NUMAArray<BCLevelNodeDataTy>;

using LevelWorklistType = katana::InsertBag<LevelGNode, 4096>;

constexpr static const unsigned kLevelChunkSize = 256u;

/******************************************************************************/
/* Functions for running the algorithm */
/******************************************************************************/
/**
 * Initialize node fields all to 0
 * @param graph LevelGraph to initialize
 */
void
LevelInitializeGraph(
    LevelGraph* graph, BCLevelNodeDataArray* graph_data,
    katana::DynamicBitset* active_edges) {
  graph_data->allocateBlocked(graph->size());
  katana::do_all(
      katana::iterate(*graph),
      [&](LevelGNode n) {
        auto& node_data = (*graph_data)[n];
        node_data.current_dist = 0;
        node_data.num_shortest_paths = 0;
        node_data.dependency = 0;
        node_data.bc = 0;
      },
      katana::no_stats(), katana::loopname("InitializeGraph"));
  active_edges->resize(graph->NumEdges());
}

/**
 * Resets data associated to start a new SSSP with a new source.
 *
 * @param graph LevelGraph to reset iteration data
 */
void
LevelInitializeIteration(
    LevelGraph* graph, LevelGNode src_node, BCLevelNodeDataArray* graph_data,
    katana::DynamicBitset* active_edges) {
  katana::do_all(
      katana::iterate(*graph),
      [&](LevelGNode n) {
        bool is_source = (n == src_node);
        auto& node_data = (*graph_data)[n];

        // source nodes have distance 0 and initialize short paths to 1, else
        // distance is kInfinity with 0 short paths
        if (!is_source) {
          node_data.current_dist = kInfinity;
          node_data.num_shortest_paths = 0;
        } else {
          node_data.current_dist = 0;
          node_data.num_shortest_paths = 1;
        }
        // dependency reset for new source
        node_data.dependency = 0;
      },
      katana::no_stats(), katana::loopname("InitializeIteration"));
  active_edges->reset();
}

/**
 * Forward phase: SSSP to determine DAG and get shortest path counts.
 *
 * Worklist-based push. Save worklists on a stack for reuse in backward
 * Brandes dependency propagation.
 */
katana::gstl::Vector<LevelWorklistType>
LevelSSSP(
    LevelGraph* graph, LevelGNode src_node, BCLevelNodeDataArray* graph_data,
    katana::DynamicBitset* active_edges) {
  katana::gstl::Vector<LevelWorklistType> vector_of_worklists;
  uint32_t current_level = 0;

  // construct first level worklist which consists only of source
  vector_of_worklists.emplace_back();
  vector_of_worklists[0].emplace(src_node);

  // loop as long as current level's worklist is non-empty
  while (!vector_of_worklists[current_level].empty()) {
    // create worklist for next level
    vector_of_worklists.emplace_back();
    uint32_t next_level = current_level + 1;

    katana::do_all(
        katana::iterate(vector_of_worklists[current_level]),
        [&](LevelGNode n) {
          auto& src_data = (*graph_data)[n];
          KATANA_LOG_ASSERT(src_data.current_dist == current_level);

          for (auto e : graph->OutEdges(n)) {
            auto dest = graph->OutEdgeDst(e);
            auto& dst_data = (*graph_data)[dest];

            if (dst_data.current_dist == kInfinity) {
              auto expected = kInfinity;
              // only 1 thread adds to worklist
              bool performed_set =
                  dst_data.current_dist.compare_exchange_strong(
                      expected, next_level);
              if (performed_set) {
                vector_of_worklists[next_level].emplace(dest);
              }

              active_edges->set(e);
              katana::atomicAdd(
                  dst_data.num_shortest_paths,
                  src_data.num_shortest_paths.load());
            } else if (dst_data.current_dist == next_level) {
              active_edges->set(e);
              katana::atomicAdd(
                  dst_data.num_shortest_paths,
                  src_data.num_shortest_paths.load());
            }
          }
        },
        katana::steal(), katana::chunk_size<kLevelChunkSize>(),
        katana::no_stats(), katana::loopname("LevelSSSP"));

    // move on to next level
    current_level++;
  }
  return vector_of_worklists;
}

/**
 * Backward phase: use worklist of nodes at each level to back-propagate
 * dependency values.
 *
 * @param graph LevelGraph to do backward Brandes dependency prop on
 */
void
LevelBackwardBrandes(
    LevelGraph* graph,
    katana::gstl::Vector<LevelWorklistType>* vector_of_worklists,
    BCLevelNodeDataArray* graph_data, katana::DynamicBitset* active_edges) {
  // minus 3 because last one is empty, one after is leaf nodes, and one
  // to correct indexing to 0 index
  if (vector_of_worklists->size() >= 3) {
    uint32_t current_level = vector_of_worklists->size() - 3;

    // last level is ignored since it's just the source
    while (current_level > 0) {
      LevelWorklistType& current_worklist =
          (*vector_of_worklists)[current_level];

      katana::do_all(
          katana::iterate(current_worklist),
          [&](LevelGNode n) {
            auto& src_data = (*graph_data)[n];
            KATANA_LOG_ASSERT(src_data.current_dist == current_level);

            for (auto e : graph->OutEdges(n)) {
              if (active_edges->test(e)) {
                // note: distance check not required because an edge
                // will never be revisited in a BFS DAG, meaning it
                // will only ever be activated once
                auto dest = graph->OutEdgeDst(e);
                auto& dst_data = (*graph_data)[dest];

                // grab dependency, add to self
                float contrib = ((float)1 + dst_data.dependency) /
                                dst_data.num_shortest_paths;
                src_data.dependency += contrib;
              }
            }

            // multiply at end to get final dependency value
            src_data.dependency *= src_data.num_shortest_paths;
            // accumulate dependency into bc
            src_data.bc += src_data.dependency;
          },
          katana::steal(), katana::chunk_size<kLevelChunkSize>(),
          katana::no_stats(), katana::loopname("Brandes"));

      // move on to next level lower
      current_level--;
    }
  }
}

//! Gets the BC value from the AoS node data in the graph and adds it to the
//! property graph for use by stats/output verification
katana::Result<void>
ExtractBC(
    const std::shared_ptr<katana::PropertyGraph>& pg,
    const LevelGraph& array_of_struct_graph,
    const BCLevelNodeDataArray& graph_data,
    const std::string& output_property_name, katana::TxnContext* txn_ctx) {
  // construct the new property
  if (auto result =
          katana::analytics::ConstructNodeProperties<std::tuple<NodeBC>>(
              pg.get(), txn_ctx, {output_property_name});
      !result) {
    return result.error();
  }

  using NewGraph = katana::TypedPropertyGraphView<
      katana::PropertyGraphViews::Default, std::tuple<NodeBC>, std::tuple<>>;
  auto new_graph =
      KATANA_CHECKED(NewGraph::Make(pg, {output_property_name}, {}));

  // extract bc to property
  katana::do_all(
      katana::iterate(array_of_struct_graph),
      [&](LevelGNode node_id) {
        float bc_value = graph_data[node_id].bc;
        new_graph.GetData<NodeBC>(node_id) = bc_value;
      },
      katana::loopname("ExtractBC"), katana::no_stats());
  return katana::ResultSuccess();
}

}  // namespace

katana::Result<void>
BetweennessCentralityLevel(
    const std::shared_ptr<katana::PropertyGraph>& pg,
    katana::analytics::BetweennessCentralitySources sources,
    const std::string& output_property_name,
    katana::analytics::BetweennessCentralityPlan plan [[maybe_unused]],
    katana::TxnContext* txn_ctx) {
  katana::ReportStatSingle(
      "BetweennessCentrality", "ChunkSize", kLevelChunkSize);
  // LevelGraph construction
  katana::StatTimer graph_construct_timer(
      "TimerConstructGraph", "BetweennessCentrality");
  graph_construct_timer.start();

  LevelGraph graph = KATANA_CHECKED(LevelGraph::Make(pg, {}, {}));

  graph_construct_timer.stop();

  // preallocate pages in memory so allocation doesn't occur during compute
  katana::StatTimer prealloc_time("PreAllocTime", "BetweennessCentrality");
  prealloc_time.start();
  katana::EnsurePreallocated(std::max(
      size_t{katana::getActiveThreads()} * (graph.size() / 1350000),
      std::max(10U, katana::getActiveThreads()) * size_t{10}));
  prealloc_time.stop();
  katana::ReportPageAllocGuard page_alloc;

  // If particular set of sources was specified, use them
  std::vector<uint32_t> source_vector;
  if (std::holds_alternative<std::vector<uint32_t>>(sources)) {
    source_vector = std::get<std::vector<uint32_t>>(sources);
  }

  uint64_t loop_end;

  if (std::holds_alternative<uint32_t>(sources)) {
    if (sources == kBetweennessCentralityAllNodes) {
      loop_end = pg->NumNodes();
    } else {
      loop_end = std::get<uint32_t>(sources);
    }
  } else {
    loop_end = source_vector.size();
  }

  BCLevelNodeDataArray graph_data;
  katana::DynamicBitset active_edges;
  // graph initialization, then main loop
  LevelInitializeGraph(&graph, &graph_data, &active_edges);

  katana::StatTimer exec_time("Level", "BetweennessCentrality");

  // loop over all specified sources for SSSP/Brandes calculation
  for (uint64_t i = 0; i < loop_end; i++) {
    LevelGNode src_node;
    if (!source_vector.empty()) {
      if (i > source_vector.size()) {
        return katana::ErrorCode::AssertionFailed;
      }
      src_node = source_vector[i];
    } else {
      // all sources
      src_node = i;
    }

    // here begins main computation
    exec_time.start();
    LevelInitializeIteration(&graph, src_node, &graph_data, &active_edges);
    // worklist; last one will be empty
    katana::gstl::Vector<LevelWorklistType> worklists =
        LevelSSSP(&graph, src_node, &graph_data, &active_edges);
    LevelBackwardBrandes(&graph, &worklists, &graph_data, &active_edges);
    exec_time.stop();
  }

  // Get the BC proporty into the property graph by extracting from AoS
  return ExtractBC(pg, graph, graph_data, output_property_name, txn_ctx);
}
