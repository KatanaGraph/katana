#ifndef GALOIS_BC_LEVEL
#define GALOIS_BC_LEVEL

#include <fstream>
#include <limits>

#include "galois/AtomicHelpers.h"
#include "galois/Reduction.h"
#include "galois/gstl.h"

////////////////////////////////////////////////////////////////////////////////

static uint64_t kLevelCurrentSrcNode = 0;
// type of the num shortest paths variable
using LevelShortPathType = double;

// NOTE: types assume that these values will not reach uint64_t: it may
// need to be changed for very large graphs
struct NodeCurrentDist : public galois::PODProperty<uint32_t> {};
struct NodeNumShortestPaths {
  using ArrowType = arrow::CTypeTraits<LevelShortPathType>::ArrowType;
  using ViewType = galois::PODPropertyView<std::atomic<LevelShortPathType>>;
};
struct NodeDependency : public galois::PODProperty<float> {};
struct NodeBC : public galois::PODProperty<float> {};

using NodeDataLevel =
    std::tuple<NodeCurrentDist, NodeNumShortestPaths, NodeDependency, NodeBC>;
using EdgeDataLevel = std::tuple<>;

typedef galois::graphs::PropertyGraph<NodeDataLevel, EdgeDataLevel> LevelGraph;
typedef typename LevelGraph::Node LevelGNode;

using LevelWorklistType = galois::InsertBag<LevelGNode, 4096>;

constexpr static const unsigned LEVEL_CHUNK_SIZE = 256u;

/******************************************************************************/
/* Functions for running the algorithm */
/******************************************************************************/
/**
 * Initialize node fields all to 0
 * @param graph LevelGraph to initialize
 */
void
LevelInitializeGraph(LevelGraph* graph) {
  galois::do_all(
      galois::iterate(*graph),
      [&](LevelGNode n) {
        graph->GetData<NodeCurrentDist>(n) = 0;
        graph->GetData<NodeNumShortestPaths>(n) = 0;
        graph->GetData<NodeDependency>(n) = 0;
        graph->GetData<NodeBC>(n) = 0;
      },
      galois::no_stats(), galois::loopname("InitializeGraph"));
}

/**
 * Resets data associated to start a new SSSP with a new source.
 *
 * @param graph LevelGraph to reset iteration data
 */
void
LevelInitializeIteration(LevelGraph* graph) {
  galois::do_all(
      galois::iterate(*graph),
      [&](LevelGNode n) {
        bool is_source = (n == kLevelCurrentSrcNode);
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
      galois::no_stats(), galois::loopname("InitializeIteration"));
};

/**
 * Forward phase: SSSP to determine DAG and get shortest path counts.
 *
 * Worklist-based push. Save worklists on a stack for reuse in backward
 * Brandes dependency propagation.
 */
galois::gstl::Vector<LevelWorklistType>
LevelSSSP(LevelGraph* graph) {
  galois::gstl::Vector<LevelWorklistType> vector_of_worklists;
  uint32_t current_level = 0;

  // construct first level worklist which consists only of source
  vector_of_worklists.emplace_back();
  vector_of_worklists[0].emplace(kLevelCurrentSrcNode);

  // loop as long as current level's worklist is non-empty
  while (!vector_of_worklists[current_level].empty()) {
    // create worklist for next level
    vector_of_worklists.emplace_back();
    uint32_t next_level = current_level + 1;

    galois::do_all(
        galois::iterate(vector_of_worklists[current_level]),
        [&](LevelGNode n) {
          GALOIS_ASSERT(graph->GetData<NodeCurrentDist>(n) == current_level);

          for (auto e : graph->edges(n)) {
            LevelGNode dest = *graph->GetEdgeDest(e);

            if (graph->GetData<NodeCurrentDist>(dest) == kInfinity) {
              uint32_t oldVal = __sync_val_compare_and_swap(
                  &graph->GetData<NodeCurrentDist>(dest), kInfinity,
                  next_level);
              // only 1 thread should add to worklist
              if (oldVal == kInfinity) {
                vector_of_worklists[next_level].emplace(dest);
              }

              galois::atomicAdd(
                  graph->GetData<NodeNumShortestPaths>(dest),
                  graph->GetData<NodeNumShortestPaths>(n).load());
            } else if (graph->GetData<NodeCurrentDist>(dest) == next_level) {
              galois::atomicAdd(
                  graph->GetData<NodeNumShortestPaths>(dest),
                  graph->GetData<NodeNumShortestPaths>(n).load());
            }
          }
        },
        galois::steal(), galois::chunk_size<LEVEL_CHUNK_SIZE>(),
        galois::no_stats(), galois::loopname("SSSP"));

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
    galois::gstl::Vector<LevelWorklistType>* vector_of_worklists) {
  // minus 3 because last one is empty, one after is leaf nodes, and one
  // to correct indexing to 0 index
  if (vector_of_worklists->size() >= 3) {
    uint32_t current_level = vector_of_worklists->size() - 3;

    // last level is ignored since it's just the source
    while (current_level > 0) {
      LevelWorklistType& current_worklist =
          (*vector_of_worklists)[current_level];
      uint32_t successor_Level = current_level + 1;

      galois::do_all(
          galois::iterate(current_worklist),
          [&](LevelGNode n) {
            GALOIS_ASSERT(graph->GetData<NodeCurrentDist>(n) == current_level);

            for (auto e : graph->edges(n)) {
              LevelGNode dest = *graph->GetEdgeDest(e);

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
          galois::steal(), galois::chunk_size<LEVEL_CHUNK_SIZE>(),
          galois::no_stats(), galois::loopname("Brandes"));

      // move on to next level lower
      current_level--;
    }
  }
}

/******************************************************************************/
/* Sanity check */
/******************************************************************************/

/**
 * Get some sanity numbers (max, min, sum of BC)
 *
 * @param graph LevelGraph to sanity check
 */
void
LevelSanity(const LevelGraph& graph) {
  galois::GReduceMax<float> accum_max;
  galois::GReduceMin<float> accum_min;
  galois::GAccumulator<float> accum_sum;
  accum_max.reset();
  accum_min.reset();
  accum_sum.reset();

  // get max, min, sum of BC values using accumulators and reducers
  galois::do_all(
      galois::iterate(graph),
      [&](LevelGNode n) {
        accum_max.update(graph.GetData<NodeBC>(n));
        accum_min.update(graph.GetData<NodeBC>(n));
        accum_sum += graph.GetData<NodeBC>(n);
      },
      galois::no_stats(), galois::loopname("LevelSanity"));

  galois::gPrint("Max BC is ", accum_max.reduce(), "\n");
  galois::gPrint("Min BC is ", accum_min.reduce(), "\n");
  galois::gPrint("BC sum is ", accum_sum.reduce(), "\n");
}

/******************************************************************************/
/* Running */
/******************************************************************************/

void
DoLevelBC() {
  // reading in list of sources to operate on if provided
  std::ifstream source_file;
  std::vector<uint64_t> source_vector;

  // some initial stat reporting
  galois::gInfo(
      "Worklist chunk size of ", LEVEL_CHUNK_SIZE,
      ": best size may depend on input.");
  galois::runtime::reportStat_Single(
      REGION_NAME, "ChunkSize", LEVEL_CHUNK_SIZE);
  galois::reportPageAlloc("MemAllocPre");

  // LevelGraph construction
  galois::StatTimer graphConstructTimer("TimerConstructGraph", "BFS");
  graphConstructTimer.start();

  std::cout << "Reading from file: " << inputFile << "\n";
  std::unique_ptr<galois::graphs::PropertyFileGraph> pfg =
      MakeFileGraph(inputFile, edge_property_name);

  auto result = ConstructNodeProperties<NodeDataLevel>(pfg.get());
  if (!result) {
    GALOIS_LOG_FATAL("failed to construct node properties: {}", result.error());
  }

  auto pg_result =
      galois::graphs::PropertyGraph<NodeDataLevel, EdgeDataLevel>::Make(
          pfg.get());
  if (!pg_result) {
    GALOIS_LOG_FATAL("could not make property graph: {}", pg_result.error());
  }
  LevelGraph graph = pg_result.value();

  std::cout << "Read " << graph.num_nodes() << " nodes, " << graph.num_edges()
            << " edges\n";

  graphConstructTimer.stop();
  galois::gInfo("Graph construction complete");

  // preallocate pages in memory so allocation doesn't occur during compute
  galois::StatTimer preallocTime("PreAllocTime", REGION_NAME);
  preallocTime.start();
  galois::Prealloc(std::max(
      size_t{galois::getActiveThreads()} * (graph.size() / 2000000),
      std::max(10U, galois::getActiveThreads()) * size_t{10}));
  preallocTime.stop();
  galois::reportPageAlloc("MemAllocMid");

  // If particular set of sources was specified, use them
  if (sourcesToUse != "") {
    source_file.open(sourcesToUse);
    std::vector<uint64_t> t(
        std::istream_iterator<uint64_t>{source_file},
        std::istream_iterator<uint64_t>{});
    source_vector = t;
    source_file.close();
  }

  // determine how many sources to loop over based on command line args
  uint64_t loop_end = 1;
  bool s_sources = false;
  if (!singleSourceBC) {
    if (!numOfSources) {
      loop_end = graph.size();
    } else {
      loop_end = numOfSources;
    }

    // if provided a file of sources to work with, use that
    if (source_vector.size() != 0) {
      if (loop_end > source_vector.size()) {
        loop_end = source_vector.size();
      }
      s_sources = true;
    }
  }

  // graph initialization, then main loop
  LevelInitializeGraph(&graph);

  galois::gInfo("Beginning main computation");
  galois::StatTimer execTime("Timer_0");

  // loop over all specified sources for SSSP/Brandes calculation
  for (uint64_t i = 0; i < loop_end; i++) {
    if (singleSourceBC) {
      // only 1 source; specified start source in command line
      assert(loop_end == 1);
      galois::gDebug("This is single source node BC");
      kLevelCurrentSrcNode = startSource;
    } else if (s_sources) {
      kLevelCurrentSrcNode = source_vector[i];
    } else {
      // all sources
      kLevelCurrentSrcNode = i;
    }

    // here begins main computation
    execTime.start();
    LevelInitializeIteration(&graph);
    // worklist; last one will be empty
    galois::gstl::Vector<LevelWorklistType> worklists = LevelSSSP(&graph);
    LevelBackwardBrandes(&graph, &worklists);
    execTime.stop();
  }

  galois::reportPageAlloc("MemAllocPost");

  // sanity checking numbers
  LevelSanity(graph);

  // Verify, i.e. print out graph data for examination
  // @todo print to file instead of stdout
  if (output) {
    char* v_out = (char*)malloc(40);
    for (auto ii = graph.begin(); ii != graph.end(); ++ii) {
      // outputs betweenness centrality
      sprintf(v_out, "%u %.9f\n", (*ii), graph.GetData<NodeBC>(*ii));
      galois::gPrint(v_out);
    }
    free(v_out);
  }
}
#endif
