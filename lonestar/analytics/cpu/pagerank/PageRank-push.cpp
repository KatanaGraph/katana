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

#include "Lonestar/BoilerPlate.h"
#include "PageRank-constants.h"

/**
 * These implementations are based on the Push-based PageRank computation
 * (Algorithm 4) as described in the PageRank Europar 2015 paper.
 *
 * WHANG, Joyce Jiyoung, et al. Scalable data-driven pagerank: Algorithms,
 * system issues, and lessons learned. In: European Conference on Parallel
 * Processing. Springer, Berlin, Heidelberg, 2015. p. 438-450.
 */

const char* desc =
    "Computes page ranks a la Page and Brin. This is a push-style algorithm.";

constexpr static const unsigned CHUNK_SIZE = 16U;

enum Algo { Async, Sync };  ///< Async has better asbolute performance.

static cll::opt<Algo> algo(
    "algo", cll::desc("Choose an algorithm:"),
    cll::values(clEnumVal(Async, "Async"), clEnumVal(Sync, "Sync")),
    cll::init(Async));

struct NodeValue : public galois::PODProperty<PRTy> {};
struct NodeResidual {
  using ArrowType = arrow::CTypeTraits<PRTy>::ArrowType;
  using ViewType = galois::PODPropertyView<std::atomic<PRTy>>;
};

using NodeData = std::tuple<NodeValue, NodeResidual>;
using EdgeData = std::tuple<>;
typedef galois::graphs::PropertyGraph<NodeData, EdgeData> Graph;
typedef typename Graph::Node GNode;

void
asyncPageRank(Graph* graph) {
  typedef galois::worklists::PerSocketChunkFIFO<CHUNK_SIZE> WL;
  galois::for_each(
      galois::iterate(*graph),
      [&](const GNode& src, auto& ctx) {
        auto& src_residual = graph->GetData<NodeResidual>(src);
        if (src_residual > tolerance) {
          PRTy old_residual = src_residual.exchange(0.0);
          auto& src_value = graph->GetData<NodeValue>(src);
          src_value += old_residual;
          int src_nout =
              std::distance(graph->edge_begin(src), graph->edge_end(src));
          if (src_nout > 0) {
            PRTy delta = old_residual * ALPHA / src_nout;
            //! For each out-going neighbors.
            for (const auto& jj : graph->edges(src)) {
              auto dest = graph->GetEdgeDest(jj);
              auto& dest_residual = graph->GetData<NodeResidual>(dest);
              if (delta > 0) {
                auto old = atomicAdd(dest_residual, delta);
                if ((old < tolerance) && (old + delta >= tolerance)) {
                  ctx.push(*dest);
                }
              }
            }
          }
        }
      },
      galois::loopname("PushResidualAsync"),
      galois::disable_conflict_detection(), galois::no_stats(),
      galois::wl<WL>());
}

void
syncPageRank(Graph* graph) {
  struct Update {
    PRTy delta;
    Graph::edge_iterator beg;
    Graph::edge_iterator end;
  };

  constexpr ptrdiff_t EDGE_TILE_SIZE = 128;

  galois::InsertBag<Update> updates;
  galois::InsertBag<GNode> active_nodes;

  galois::do_all(
      galois::iterate(*graph), [&](const auto& src) { active_nodes.push(src); },
      galois::no_stats());

  size_t iter = 0;
  for (; !active_nodes.empty() && iter < maxIterations; ++iter) {
    galois::do_all(
        galois::iterate(active_nodes),
        [&](const GNode& src) {
          auto& sdata_residual = graph->GetData<NodeResidual>(src);

          if (sdata_residual > tolerance) {
            PRTy old_residual = sdata_residual;
            graph->GetData<NodeValue>(src) += old_residual;
            sdata_residual = 0.0;

            int src_nout =
                std::distance(graph->edge_begin(src), graph->edge_end(src));
            PRTy delta = old_residual * ALPHA / src_nout;

            auto beg = graph->edge_begin(src);
            const auto end = graph->edge_end(src);

            assert(beg <= end);

            //! Edge tiling for large outdegree nodes.
            if ((end - beg) > EDGE_TILE_SIZE) {
              for (; beg + EDGE_TILE_SIZE < end;) {
                auto ne = beg + EDGE_TILE_SIZE;
                updates.push(Update{delta, beg, ne});
                beg = ne;
              }
            }

            if ((end - beg) > 0) {
              updates.push(Update{delta, beg, end});
            }
          }
        },
        galois::steal(), galois::chunk_size<CHUNK_SIZE>(),
        galois::loopname("CreateEdgeTiles"), galois::no_stats());

    active_nodes.clear();

    galois::do_all(
        galois::iterate(updates),
        [&](const Update& up) {
          //! For each out-going neighbors.
          for (auto jj = up.beg; jj != up.end; ++jj) {
            auto dest = graph->GetEdgeDest(jj);
            auto& ddata_residual = graph->GetData<NodeResidual>(dest);
            auto old = atomicAdd(ddata_residual, up.delta);
            //! If fabs(old) is greater than tolerance, then it would
            //! already have been processed in the previous do_all
            //! loop.
            if ((old <= tolerance) && (old + up.delta >= tolerance)) {
              active_nodes.push(*dest);
            }
          }
        },
        galois::steal(), galois::chunk_size<CHUNK_SIZE>(),
        galois::loopname("PushResidualSync"), galois::no_stats());

    updates.clear();
  }

  if (iter >= maxIterations) {
    std::cerr << "ERROR: failed to converge in " << iter << " iterations\n";
  }
}

/******************************************************************************/
/* Make results */
/******************************************************************************/

std::vector<PRTy>
makeResults(const Graph& graph) {
  std::vector<PRTy> values;

  values.reserve(graph.num_nodes());
  for (auto node : graph) {
    auto& value = graph.GetData<NodeValue>(node);
    values.push_back(value);
  }

  return values;
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

  galois::Prealloc(5, 5 * graph.size() * sizeof(NodeData));
  galois::reportPageAlloc("MeminfoPre");

  std::cout << "tolerance:" << tolerance << ", maxIterations:" << maxIterations
            << "\n";

  galois::do_all(
      galois::iterate(graph),
      [&graph](const GNode& n) {
        graph.GetData<NodeResidual>(n) = INIT_RESIDUAL;
        graph.GetData<NodeValue>(n) = 0;
      },
      galois::no_stats(), galois::loopname("Initialize"));

  galois::StatTimer execTime("Timer_0");
  execTime.start();

  switch (algo) {
  case Async:
    std::cout << "Running Edge Async push version,";
    asyncPageRank(&graph);
    break;

  case Sync:
    std::cout << "Running Edge Sync push version,";
    syncPageRank(&graph);
    break;

  default:
    std::abort();
  }

  execTime.stop();

  galois::reportPageAlloc("MeminfoPost");

  if (!skipVerify) {
    printTop<Graph, NodeValue>(&graph);
  }

  if (output) {
    std::vector<PRTy> results = makeResults(graph);
    assert(results.size() == graph.size());

    writeOutput(outputLocation, results.data(), results.size());
  }

#if DEBUG
  printPageRank(graph);
#endif

  totalTime.stop();

  return 0;
}
