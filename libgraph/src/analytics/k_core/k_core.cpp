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

#include "katana/analytics/k_core/k_core.h"

#include "katana/ArrowRandomAccessBuilder.h"
#include "katana/Statistics.h"
#include "katana/TypedPropertyGraph.h"

using namespace katana::analytics;

const int KCorePlan::kChunkSize = 64;

/*******************************************************************************
 * Functions for running the algorithm
 ******************************************************************************/
//! Node deadness can be derived from current degree and k value, so no field
//! necessary.
struct KCoreNodeCurrentDegree : public katana::AtomicPODProperty<uint32_t> {};

struct KCoreNodeAlive : public katana::PODProperty<uint32_t> {};

using NodeData = std::tuple<KCoreNodeCurrentDegree>;
using EdgeData = std::tuple<>;

/**
 * Initialize degree fields in graph with current degree. Since symmetric,
 * out edge count is equivalent to in-edge count.
 *
 * @param graph Graph to initialize degrees in
 */
template <typename GraphTy>
void
DegreeCounting(GraphTy* graph) {
  using GNode = typename GraphTy::Node;
  katana::do_all(
      katana::iterate(*graph),
      [&](const GNode& node) {
        auto& node_current_degree =
            graph->template GetData<KCoreNodeCurrentDegree>(node);
        node_current_degree.store(Degree(*graph, node));
      },
      katana::loopname("DegreeCounting"), katana::no_stats());
}

/**
 * Setup initial worklist of dead nodes.
 *
 * @param graph Graph to operate on
 * @param initial_worklist Empty worklist to be filled with dead nodes.
 * @param k_core_number Each node in the core is expected to have degree <= k_core_number.
 */
template <typename GraphTy>
void
SetupInitialWorklist(
    const GraphTy& graph,
    katana::InsertBag<typename GraphTy::Node>& initial_worklist,
    uint32_t k_core_number) {
  using GNode = typename GraphTy::Node;
  katana::do_all(
      katana::iterate(graph),
      [&](const GNode& node) {
        const auto& node_current_degree =
            graph.template GetData<KCoreNodeCurrentDegree>(node);
        if (node_current_degree < k_core_number) {
          //! Dead node, add to initial_worklist for processing later.
          initial_worklist.emplace(node);
        }
      },
      katana::loopname("InitialWorklistSetup"), katana::no_stats());
}

/**
 * Starting with initial dead nodes as current worklist; decrement degree;
 * add to next worklist; switch next with current and repeat until worklist
 * is empty (i.e. no more dead nodes).
 *
 * @param graph Graph to operate on
 * @param k_core_number Each node in the core is expected to have degree <= k_core_number
 */
template <typename GraphTy>
void
SyncCascadeKCore(GraphTy* graph, uint32_t k_core_number) {
  using GNode = typename GraphTy::Node;
  auto current = std::make_unique<katana::InsertBag<GNode>>();
  auto next = std::make_unique<katana::InsertBag<GNode>>();

  //! Setup worklist.
  SetupInitialWorklist(*graph, *next, k_core_number);

  while (!next->empty()) {
    //! Make "next" into current.
    std::swap(current, next);
    next->clear();

    katana::do_all(
        katana::iterate(*current),
        [&](const GNode& dead_node) {
          //! Decrement degree of all neighbors.
          for (auto e : Edges(*graph, dead_node)) {
            auto dest = EdgeDst(*graph, e);
            auto& dest_current_degree =
                graph->template GetData<KCoreNodeCurrentDegree>(dest);
            uint32_t old_degree = katana::atomicSub(dest_current_degree, 1u);

            if (old_degree == k_core_number) {
              //! This thread was responsible for putting degree of destination
              //! below threshold; add to worklist.
              next->emplace(dest);
            }
          }
        },
        katana::steal(), katana::chunk_size<KCorePlan::kChunkSize>(),
        katana::loopname("KCore Synchronous"));
  }
}

/**
 * Starting with initial dead nodes, decrement degree and add to worklist
 * as they drop below 'k' threshold until worklist is empty (i.e. no more dead
 * nodes).
 *
 * @param graph Graph to operate on
 * @param k_core_number Each node in the core is expected to have degree <= k_core_number.
 */
template <typename GraphTy>
void
AsyncCascadeKCore(GraphTy* graph, uint32_t k_core_number) {
  using GNode = typename GraphTy::Node;
  katana::InsertBag<GNode> initial_worklist;
  //! Setup worklist.
  SetupInitialWorklist(*graph, initial_worklist, k_core_number);

  katana::for_each(
      katana::iterate(initial_worklist),
      [&](const GNode& dead_node, auto& ctx) {
        //! Decrement degree of all neighbors.
        for (auto e : Edges(*graph, dead_node)) {
          auto dest = EdgeDst(*graph, e);
          auto& dest_current_degree =
              graph->template GetData<KCoreNodeCurrentDegree>(dest);
          uint32_t old_degree = katana::atomicSub(dest_current_degree, 1u);

          if (old_degree == k_core_number) {
            //! This thread was responsible for putting degree of destination
            //! below threshold: add to worklist.
            ctx.push(dest);
          }
        }
      },
      katana::disable_conflict_detection(),
      katana::chunk_size<KCorePlan::kChunkSize>(),
      katana::loopname("KCore Asynchronous"));
}

/**
 * After computation is finished, the nodes left in the core
 * are marked as alive.
 *
 * @param graph Graph to operate on
 * @param k_core_number Each node in the core is expected to have degree <= k_core_number.
 */
template <typename GraphTy>
katana::Result<void>
KCoreMarkAliveNodes(GraphTy* graph, uint32_t k_core_number) {
  using GNode = typename GraphTy::Node;
  katana::do_all(
      katana::iterate(*graph),
      [&](const GNode& node) {
        auto& node_current_degree =
            graph->template GetData<KCoreNodeCurrentDegree>(node);
        auto& node_flag = graph->template GetData<KCoreNodeAlive>(node);
        node_flag = 1;
        if (node_current_degree < k_core_number) {
          node_flag = 0;
        }
      },
      katana::loopname("KCore Mark Nodes in Core"));
  return katana::ResultSuccess();
}

template <typename GraphTy>
static katana::Result<void>
KCoreImpl(GraphTy* graph, KCorePlan algo, uint32_t k_core_number) {
  size_t approxNodeData = 4 * (graph->NumNodes() + graph->NumEdges());
  katana::EnsurePreallocated(8, approxNodeData);
  katana::ReportPageAllocGuard page_alloc;

  //! Intialization of degrees.
  DegreeCounting(graph);

  //! Begins main computation.
  katana::StatTimer exec_time("KCore");

  exec_time.start();

  switch (algo.algorithm()) {
  case KCorePlan::kSynchronous:
    SyncCascadeKCore(graph, k_core_number);
    break;
  case KCorePlan::kAsynchronous:
    AsyncCascadeKCore(graph, k_core_number);
    break;
  default:
    return katana::ErrorCode::AssertionFailed;
  }
  exec_time.stop();

  return katana::ResultSuccess();
}

katana::Result<void>
katana::analytics::KCore(
    katana::PropertyGraph* pg, uint32_t k_core_number,
    const std::string& output_property_name, katana::TxnContext* txn_ctx,
    const bool& is_symmetric, KCorePlan plan) {
  katana::analytics::TemporaryPropertyGuard temporary_property{
      pg->NodeMutablePropertyView()};

  KATANA_CHECKED(
      pg->ConstructNodeProperties<std::tuple<KCoreNodeCurrentDegree>>(
          txn_ctx, {temporary_property.name()}));

  if (is_symmetric) {
    using Graph = katana::TypedPropertyGraphView<
        katana::PropertyGraphViews::Default, NodeData, EdgeData>;
    Graph graph =
        KATANA_CHECKED(Graph::Make(pg, {temporary_property.name()}, {}));

    KATANA_CHECKED(KCoreImpl(&graph, plan, k_core_number));
  } else {
    using Graph = katana::TypedPropertyGraphView<
        katana::PropertyGraphViews::Undirected, NodeData, EdgeData>;

    Graph graph =
        KATANA_CHECKED(Graph::Make(pg, {temporary_property.name()}, {}));

    KATANA_CHECKED(KCoreImpl(&graph, plan, k_core_number));
  }
  // Post processing. Mark alive nodes.
  KATANA_CHECKED(pg->ConstructNodeProperties<std::tuple<KCoreNodeAlive>>(
      txn_ctx, {output_property_name}));

  using GraphTy = katana::TypedPropertyGraph<
      std::tuple<KCoreNodeAlive, KCoreNodeCurrentDegree>, std::tuple<>>;
  auto graph_final = KATANA_CHECKED(
      GraphTy::Make(pg, {output_property_name, temporary_property.name()}, {}));

  return KCoreMarkAliveNodes(&graph_final, k_core_number);
}

// Doxygen doesn't correctly handle implementation annotations that do not
// appear in the declaration.
/// \cond DO_NOT_DOCUMENT
// TODO (gill) Add a validity routine.
katana::Result<void>
katana::analytics::KCoreAssertValid(
    [[maybe_unused]] katana::PropertyGraph* pg,
    [[maybe_unused]] uint32_t k_core_number,
    [[maybe_unused]] const std::string& property_name) {
  return katana::ResultSuccess();
}

katana::Result<KCoreStatistics>
katana::analytics::KCoreStatistics::Compute(
    katana::PropertyGraph* pg, [[maybe_unused]] uint32_t k_core_number,
    const std::string& property_name) {
  using Graph =
      katana::TypedPropertyGraph<std::tuple<KCoreNodeAlive>, std::tuple<>>;
  using GNode = Graph::Node;
  auto pg_result = Graph::Make(pg, {property_name}, {});
  if (!pg_result) {
    return pg_result.error();
  }

  auto graph = pg_result.value();

  katana::GAccumulator<uint32_t> alive_nodes;
  alive_nodes.reset();

  katana::do_all(
      katana::iterate(graph),
      [&](const GNode& node) {
        auto& node_alive = graph.GetData<KCoreNodeAlive>(node);
        if (node_alive) {
          alive_nodes += 1;
        }
      },
      katana::loopname("KCore sanity check"), katana::no_stats());

  return KCoreStatistics{alive_nodes.reduce()};
}
/// \endcond DO_NOT_DOCUMENT

void
katana::analytics::KCoreStatistics::Print(std::ostream& os) const {
  os << "Number of nodes in the core = " << number_of_nodes_in_kcore
     << std::endl;
}
