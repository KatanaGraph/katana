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

#include <arrow/type.h>

#include "katana/TypedPropertyGraph.h"
#include "katana/analytics/Utils.h"
#include "pagerank-impl.h"

namespace {

struct PagerankValueAndOutDegreeTy {
  uint32_t out;
  PRTy value;
};

using PagerankValueAndOutDegree =
    katana::StructProperty<PagerankValueAndOutDegreeTy>;

using DeltaArray = katana::NUMAArray<PRTy>;
using ResidualArray = katana::NUMAArray<PRTy>;
using NodeOutDegreeArray = katana::NUMAArray<uint32_t>;
using PagerankValueAndOutDegreeArray =
    katana::NUMAArray<PagerankValueAndOutDegreeTy>;

using NodeData = std::tuple<NodeValue>;
using EdgeData = std::tuple<>;

using Graph = katana::TypedPropertyGraphView<
    katana::PropertyGraphViews::Transposed, NodeData, EdgeData>;

//! Initialize nodes for the topological algorithm.
katana::Result<void>
InitNodeDataTopological(
    const Graph& graph, PagerankValueAndOutDegreeArray* node_data) {
  using GNode = typename Graph::Node;
  PRTy init_value = 1.0f / graph.size();
  katana::do_all(
      katana::iterate(graph),
      [&](const GNode& n) {
        (*node_data)[n].value = init_value;
        (*node_data)[n].out = 0;
      },
      katana::loopname("initNodeData"));
  return katana::ResultSuccess();
}

//! Initialize nodes for the residual algorithm.
katana::Result<void>
InitNodeDataResidual(
    Graph* graph, DeltaArray* delta, ResidualArray* residual,
    NodeOutDegreeArray* node_out_degree, katana::analytics::PagerankPlan plan) {
  using GNode = typename Graph::Node;
  katana::do_all(
      katana::iterate(*graph),
      [&](const GNode& n) {
        auto& sdata = graph->template GetData<NodeValue>(n);
        sdata = 0;
        (*node_out_degree)[n] = 0;
        (*delta)[n] = 0;
        (*residual)[n] = plan.initial_residual();
      },
      katana::loopname("initNodeData"));
  return katana::ResultSuccess();
}

//! Computing outdegrees in the tranpose graph is equivalent to computing the
//! indegrees in the original graph.
katana::Result<void>
ComputeOutDeg(const Graph& graph, PagerankValueAndOutDegreeArray* node_data) {
  using GNode = typename Graph::Node;
  katana::StatTimer out_degree_timer("computeOutDegFunc");
  out_degree_timer.start();

  katana::NUMAArray<std::atomic<size_t>> vec;
  vec.allocateInterleaved(graph.size());

  katana::do_all(
      katana::iterate(graph),
      [&](const GNode& src) { vec.constructAt(src, 0ul); },
      katana::loopname("InitDegVec"));

  katana::do_all(
      katana::iterate(graph),
      [&](const GNode& src) {
        for (auto nbr : graph.OutEdges(src)) {
          auto dest = graph.OutEdgeDst(nbr);
          vec[dest].fetch_add(1ul);
        }
      },
      katana::steal(),
      katana::chunk_size<katana::analytics::PagerankPlan::kChunkSize>(),
      katana::loopname("ComputeOutDeg"));

  katana::do_all(
      katana::iterate(graph),
      [&](const GNode& src) { (*node_data)[src].out = vec[src]; },
      katana::loopname("CopyDeg"));

  out_degree_timer.stop();
  return katana::ResultSuccess();
}

katana::Result<void>
ComputeOutDeg(const Graph& graph, NodeOutDegreeArray* node_data) {
  using GNode = typename Graph::Node;
  katana::StatTimer out_degree_timer("computeOutDegFunc");
  out_degree_timer.start();

  katana::NUMAArray<std::atomic<size_t>> vec;
  vec.allocateInterleaved(graph.size());

  katana::do_all(
      katana::iterate(graph),
      [&](const GNode& src) { vec.constructAt(src, 0ul); },
      katana::loopname("InitDegVec"));

  katana::do_all(
      katana::iterate(graph),
      [&](const GNode& src) {
        for (auto nbr : graph.OutEdges(src)) {
          auto dest = graph.OutEdgeDst(nbr);
          vec[dest].fetch_add(1ul);
        }
      },
      katana::steal(),
      katana::chunk_size<katana::analytics::PagerankPlan::kChunkSize>(),
      katana::loopname("ComputeOutDeg"));

  katana::do_all(
      katana::iterate(graph),
      [&](const GNode& src) { (*node_data)[src] = vec[src]; },
      katana::loopname("CopyDeg"));

  out_degree_timer.stop();
  return katana::ResultSuccess();
}

/**
 * It does not calculate the pagerank for each iteration,
 * but only calculate the residual to be added from the previous pagerank to
 * the current one.
 * If the residual is smaller than the tolerance, that is not reflected to
 * the next pagerank.
 */
//! [scalarreduction]
katana::Result<void>
ComputePRResidual(
    Graph* graph, DeltaArray* delta, ResidualArray* residual,
    const NodeOutDegreeArray& node_out_degree,
    katana::analytics::PagerankPlan plan) {
  katana::StatTimer exec_time("PagerankPullResidual");
  exec_time.start();
  using GNode = typename Graph::Node;
  unsigned int iterations = 0;
  katana::GAccumulator<unsigned int> accum;

  while (true) {
    katana::do_all(
        katana::iterate(*graph),
        [&](const GNode& src) {
          auto& sdata = graph->template GetData<NodeValue>(src);
          (*delta)[src] = 0;

          //! Only the residual higher than tolerance will be reflected
          //! to the pagerank.
          if ((*residual)[src] > plan.tolerance()) {
            PRTy old_residual = (*residual)[src];
            (*residual)[src] = 0.0;
            sdata += old_residual;
            if (node_out_degree[src] > 0) {
              (*delta)[src] =
                  old_residual * plan.alpha() / node_out_degree[src];
              accum += 1;
            }
          }
        },
        katana::loopname("PageRank_delta"));

    katana::do_all(
        katana::iterate(*graph),
        [&](const GNode& src) {
          float sum = 0;
          for (auto nbr : graph->OutEdges(src)) {
            auto dest = graph->OutEdgeDst(nbr);
            if ((*delta)[dest] > 0) {
              sum += (*delta)[dest];
            }
          }
          if (sum > 0) {
            (*residual)[src] = sum;
          }
        },
        katana::steal(),
        katana::chunk_size<katana::analytics::PagerankPlan::kChunkSize>(),
        katana::loopname("PageRank"));

#if DEBUG
    std::cout << "iteration: " << iterations << "\n";
#endif
    iterations++;
    if (iterations >= plan.max_iterations() || !accum.reduce()) {
      break;
    }
    accum.reset();
  }  ///< End while(true).
  //! [scalarreduction]
  exec_time.start();
  return katana::ResultSuccess();
}

/**
 * PageRank pull topological.
 * Always calculate the new pagerank for each iteration.
 */
katana::Result<void>
ComputePRTopological(
    Graph* graph, katana::analytics::PagerankPlan plan,
    PagerankValueAndOutDegreeArray* node_data) {
  katana::StatTimer exec_time("PagerankPullTopological");
  exec_time.start();

  using GNode = typename Graph::Node;
  unsigned int iteration = 0;
  katana::GAccumulator<float> accum;

  float base_score = (1.0f - plan.alpha());
  while (true) {
    katana::do_all(
        katana::iterate(*graph),
        [&](const GNode& src) {
          float sum = 0.0;

          for (auto jj : graph->OutEdges(src)) {
            auto dest = graph->OutEdgeDst(jj);
            auto& ddata = (*node_data)[dest];
            sum += ddata.value / ddata.out;
          }

          //! New value of pagerank after computing contributions from
          //! incoming edges in the original graph.
          float value = sum * plan.alpha() + base_score;
          //! Find the delta in new and old pagerank values.
          //float diff = std::fabs(value - sdata.value);
          float diff = std::fabs(value - (*node_data)[src].value);

          //! Do not update pagerank before the diff is computed since
          //! there is a data dependence on the pagerank value.
          auto& sdata = (*node_data)[src].value;
          sdata = value;
          accum += diff;
        },
        katana::steal(),
        katana::chunk_size<katana::analytics::PagerankPlan::kChunkSize>(),
        katana::loopname("Pagerank Topological"));

#if DEBUG
    std::cout << "iteration: " << iteration << " max delta: " << delta << "\n";
#endif
    iteration += 1;
    if (accum.reduce() <= plan.tolerance() ||
        iteration >= plan.max_iterations()) {
      break;
    }
    accum.reset();

  }  ///< End while(true).

  katana::ReportStatSingle("PageRank", "Iterations", iteration);

  /// Assign values back to the property graph.
  katana::do_all(
      katana::iterate(*graph),
      [&](uint32_t i) {
        graph->template GetData<NodeValue>(i) = (*node_data)[i].value;
      },
      katana::loopname("Extract pagerank"), katana::no_stats());

  exec_time.stop();
  return katana::ResultSuccess();
}

}  // namespace

katana::Result<void>
PagerankPullTopological(
    katana::PropertyGraph* pg, const std::string& output_property_name,
    katana::analytics::PagerankPlan plan, katana::TxnContext* txn_ctx) {
  KATANA_CHECKED(
      katana::analytics::ConstructNodeProperties<std::tuple<NodeValue>>(
          pg, txn_ctx, {output_property_name}));

  Graph graph = KATANA_CHECKED(Graph::Make(pg, {output_property_name}, {}));

  katana::EnsurePreallocated(2, 3 * graph.size() * sizeof(NodeData));
  katana::ReportPageAllocGuard page_alloc;

  // NUMA-aware temporary node data
  PagerankValueAndOutDegreeArray node_data;
  node_data.allocateInterleaved(graph.size());

  KATANA_CHECKED(InitNodeDataTopological(graph, &node_data));
  KATANA_CHECKED(ComputeOutDeg(graph, &node_data));

  return ComputePRTopological(&graph, plan, &node_data);
}

katana::Result<void>
PagerankPullResidual(
    katana::PropertyGraph* pg, const std::string& output_property_name,
    katana::analytics::PagerankPlan plan, katana::TxnContext* txn_ctx) {
  KATANA_CHECKED(katana::analytics::ConstructNodeProperties<NodeData>(
      pg, txn_ctx, {output_property_name}));

  Graph graph = KATANA_CHECKED(Graph::Make(pg, {output_property_name}, {}));

  katana::EnsurePreallocated(2, 3 * graph.size() * sizeof(NodeData));
  katana::ReportPageAllocGuard page_alloc;

  // NUMA-aware temporary node data
  NodeOutDegreeArray node_out_degree;
  node_out_degree.allocateInterleaved(graph.size());

  DeltaArray delta;
  delta.allocateInterleaved(graph.size());
  ResidualArray residual;
  residual.allocateInterleaved(graph.size());

  KATANA_CHECKED(
      InitNodeDataResidual(&graph, &delta, &residual, &node_out_degree, plan));
  KATANA_CHECKED(ComputeOutDeg(graph, &node_out_degree));

  return ComputePRResidual(&graph, &delta, &residual, node_out_degree, plan);
}
