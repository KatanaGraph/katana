#include "betweenness_centrality_impl.h"
#include "katana/AtomicHelpers.h"
#include "katana/Properties.h"
#include "katana/PropertyGraph.h"

using namespace katana::analytics;

namespace {

// type of the num shortest paths variable
using LevelShortPathType = double;

constexpr static uint32_t kInfinity = std::numeric_limits<uint32_t>::max();

// NOTE: types assume that these values will not reach uint64_t: it may
// need to be changed for very large graphs
struct NodeCurrentDist {
  using ArrowType = arrow::CTypeTraits<uint32_t>::ArrowType;
  using ViewType = katana::PODPropertyView<std::atomic<uint32_t>>;
};
struct NodeNumShortestPaths {
  using ArrowType = arrow::CTypeTraits<LevelShortPathType>::ArrowType;
  using ViewType = katana::PODPropertyView<std::atomic<LevelShortPathType>>;
};
struct NodeDependency : public katana::PODProperty<float> {};
struct NodeBC : public katana::PODProperty<float> {};

using NodeDataLevel =
    std::tuple<NodeBC, NodeCurrentDist, NodeNumShortestPaths, NodeDependency>;
using EdgeDataLevel = std::tuple<>;

typedef katana::PropertyGraph<NodeDataLevel, EdgeDataLevel> LevelGraph;
typedef typename LevelGraph::Node LevelGNode;

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
LevelInitializeGraph(LevelGraph* graph) {
  katana::do_all(
      katana::iterate(*graph),
      [&](LevelGNode n) {
        graph->GetData<NodeCurrentDist>(n) = 0;
        graph->GetData<NodeNumShortestPaths>(n) = 0;
        graph->GetData<NodeDependency>(n) = 0;
        graph->GetData<NodeBC>(n) = 0;
      },
      katana::no_stats(), katana::loopname("InitializeGraph"));
}

/**
 * Resets data associated to start a new SSSP with a new source.
 *
 * @param graph LevelGraph to reset iteration data
 */
void
LevelInitializeIteration(LevelGraph* graph, LevelGNode src_node) {
  katana::do_all(
      katana::iterate(*graph),
      [&](LevelGNode n) {
        bool is_source = (n == src_node);
        // source nodes have distance 0 and initialize short paths to 1, else
        // distance is kInfinity with 0 short paths
        if (!is_source) {
          graph->GetData<NodeCurrentDist>(n) = kInfinity;
          graph->GetData<NodeNumShortestPaths>(n) = 0;
        } else {
          graph->GetData<NodeCurrentDist>(n) = 0;
          graph->GetData<NodeNumShortestPaths>(n) = 1;
        }
        // dependency reset for new source
        graph->GetData<NodeDependency>(n) = 0;
      },
      katana::no_stats(), katana::loopname("InitializeIteration"));
}

/**
 * Forward phase: SSSP to determine DAG and get shortest path counts.
 *
 * Worklist-based push. Save worklists on a stack for reuse in backward
 * Brandes dependency propagation.
 */
katana::gstl::Vector<LevelWorklistType>
LevelSSSP(LevelGraph* graph, LevelGNode src_node) {
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
          KATANA_LOG_ASSERT(
              graph->GetData<NodeCurrentDist>(n) == current_level);

          for (auto e : graph->edges(n)) {
            auto dest = graph->GetEdgeDest(e);

            if (graph->GetData<NodeCurrentDist>(dest) == kInfinity) {
              auto expected = kInfinity;
              bool performed_set =
                  graph->GetData<NodeCurrentDist>(dest).compare_exchange_strong(
                      expected, next_level);
              // only 1 thread should add to worklist
              if (performed_set) {
                vector_of_worklists[next_level].emplace(*dest);
              }

              katana::atomicAdd(
                  graph->GetData<NodeNumShortestPaths>(dest),
                  graph->GetData<NodeNumShortestPaths>(n).load());
            } else if (graph->GetData<NodeCurrentDist>(dest) == next_level) {
              katana::atomicAdd(
                  graph->GetData<NodeNumShortestPaths>(dest),
                  graph->GetData<NodeNumShortestPaths>(n).load());
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
    katana::gstl::Vector<LevelWorklistType>* vector_of_worklists) {
  // minus 3 because last one is empty, one after is leaf nodes, and one
  // to correct indexing to 0 index
  if (vector_of_worklists->size() >= 3) {
    uint32_t current_level = vector_of_worklists->size() - 3;

    // last level is ignored since it's just the source
    while (current_level > 0) {
      LevelWorklistType& current_worklist =
          (*vector_of_worklists)[current_level];
      uint32_t successor_Level = current_level + 1;

      katana::do_all(
          katana::iterate(current_worklist),
          [&](LevelGNode n) {
            KATANA_LOG_ASSERT(
                graph->GetData<NodeCurrentDist>(n) == current_level);

            for (auto e : graph->edges(n)) {
              auto dest = graph->GetEdgeDest(e);

              if (graph->GetData<NodeCurrentDist>(dest) == successor_Level) {
                // grab dependency, add to self
                float contrib =
                    ((float)1 + graph->GetData<NodeDependency>(dest)) /
                    graph->GetData<NodeNumShortestPaths>(dest);
                graph->GetData<NodeDependency>(n) += contrib;
              }
            }

            // multiply at end to get final dependency value
            graph->GetData<NodeDependency>(n) *=
                graph->GetData<NodeNumShortestPaths>(n);
            // accumulate dependency into bc
            graph->GetData<NodeBC>(n) += graph->GetData<NodeDependency>(n);
          },
          katana::steal(), katana::chunk_size<kLevelChunkSize>(),
          katana::no_stats(), katana::loopname("Brandes"));

      // move on to next level lower
      current_level--;
    }
  }
}

}  // namespace

katana::Result<void>
BetweennessCentralityLevel(
    katana::PropertyFileGraph* pfg,
    katana::analytics::BetweennessCentralitySources sources,
    const std::string& output_property_name,
    katana::analytics::BetweennessCentralityPlan plan [[maybe_unused]]) {
  katana::ReportStatSingle(
      "BetweennessCentrality", "ChunkSize", kLevelChunkSize);
  katana::reportPageAlloc("MemAllocPre");

  // LevelGraph construction
  katana::StatTimer graph_construct_timer(
      "TimerConstructGraph", "BetweennessCentrality");
  graph_construct_timer.start();

  TemporaryPropertyGuard node_current_dist{pfg};
  TemporaryPropertyGuard node_num_shortest_paths{pfg};
  TemporaryPropertyGuard node_dependency{pfg};

  auto result = ConstructNodeProperties<NodeDataLevel>(
      pfg, {output_property_name, node_current_dist.name(),
            node_num_shortest_paths.name(), node_dependency.name()});
  if (!result) {
    return result.error();
  }

  auto pg_result = katana::PropertyGraph<NodeDataLevel, EdgeDataLevel>::Make(
      pfg,
      {output_property_name, node_current_dist.name(),
       node_num_shortest_paths.name(), node_dependency.name()},
      {});
  if (!pg_result) {
    return pg_result.error();
  }
  LevelGraph graph = pg_result.value();

  graph_construct_timer.stop();

  // preallocate pages in memory so allocation doesn't occur during compute
  katana::StatTimer prealloc_time("PreAllocTime", "BetweennessCentrality");
  prealloc_time.start();
  katana::Prealloc(std::max(
      size_t{katana::getActiveThreads()} * (graph.size() / 2000000),
      std::max(10U, katana::getActiveThreads()) * size_t{10}));
  prealloc_time.stop();
  katana::reportPageAlloc("MemAllocMid");

  // If particular set of sources was specified, use them
  std::vector<uint32_t> source_vector;
  if (std::holds_alternative<std::vector<uint32_t>>(sources)) {
    source_vector = std::get<std::vector<uint32_t>>(sources);
  }

  uint64_t loop_end;

  if (std::holds_alternative<uint32_t>(sources)) {
    if (sources == kBetweennessCentralityAllNodes) {
      loop_end = pfg->num_nodes();
    } else {
      loop_end = std::get<uint32_t>(sources);
    }
  } else {
    loop_end = source_vector.size();
  }

  // graph initialization, then main loop
  LevelInitializeGraph(&graph);
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
    LevelInitializeIteration(&graph, src_node);
    // worklist; last one will be empty
    katana::gstl::Vector<LevelWorklistType> worklists =
        LevelSSSP(&graph, src_node);
    LevelBackwardBrandes(&graph, &worklists);
    exec_time.stop();
  }

  katana::reportPageAlloc("MemAllocPost");

  return katana::ResultSuccess();
}
