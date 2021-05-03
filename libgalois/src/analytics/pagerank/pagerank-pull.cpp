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

#include "katana/TypedPropertyGraph.h"
#include "katana/analytics/Utils.h"
#include "pagerank-impl.h"

namespace {
struct NodeNout : public katana::PODProperty<uint32_t> {};

using NodeData = std::tuple<NodeValue, NodeNout>;
using EdgeData = std::tuple<>;

typedef katana::TypedPropertyGraph<NodeData, EdgeData> Graph;
typedef typename Graph::Node GNode;

using DeltaArray = katana::LargeArray<PRTy>;
using ResidualArray = katana::LargeArray<PRTy>;

//! Initialize nodes for the topological algorithm.
void
InitNodeDataTopological(Graph* graph) {
  PRTy init_value = 1.0f / graph->size();
  katana::do_all(
      katana::iterate(*graph),
      [&](const GNode& n) {
        auto& sdata_value = graph->GetData<NodeValue>(n);
        auto& sdata_nout = graph->GetData<NodeNout>(n);
        sdata_value = init_value;
        sdata_nout = 0;
      },
      katana::no_stats(), katana::loopname("initNodeData"));
}

//! Initialize nodes for the residual algorithm.
void
InitNodeDataResidual(
    Graph* graph, DeltaArray& delta, ResidualArray& residual,
    katana::analytics::PagerankPlan plan) {
  katana::do_all(
      katana::iterate(*graph),
      [&](const GNode& n) {
        auto& sdata_value = graph->GetData<NodeValue>(n);
        auto& sdata_nout = graph->GetData<NodeNout>(n);
        sdata_value = 0;
        sdata_nout = 0;
        delta[n] = 0;
        residual[n] = plan.initial_residual();
      },
      katana::no_stats(), katana::loopname("initNodeData"));
}

//! Computing outdegrees in the tranpose graph is equivalent to computing the
//! indegrees in the original graph.
void
ComputeOutDeg(Graph* graph) {
  katana::StatTimer out_degree_timer("computeOutDegFunc");
  out_degree_timer.start();

  katana::LargeArray<std::atomic<size_t>> vec;
  vec.allocateInterleaved(graph->size());

  katana::do_all(
      katana::iterate(*graph),
      [&](const GNode& src) { vec.constructAt(src, 0ul); }, katana::no_stats(),
      katana::loopname("InitDegVec"));

  katana::do_all(
      katana::iterate(*graph),
      [&](const GNode& src) {
        for (auto nbr : graph->edges(src)) {
          auto dest = graph->GetEdgeDest(nbr);
          vec[*dest].fetch_add(1ul);
        }
      },
      katana::steal(),
      katana::chunk_size<katana::analytics::PagerankPlan::kChunkSize>(),
      katana::no_stats(), katana::loopname("ComputeOutDeg"));

  katana::do_all(
      katana::iterate(*graph),
      [&](const GNode& src) {
        auto& src_nout = graph->GetData<NodeNout>(src);
        src_nout = vec[src];
      },
      katana::no_stats(), katana::loopname("CopyDeg"));

  out_degree_timer.stop();
}

/**
 * It does not calculate the pagerank for each iteration,
 * but only calculate the residual to be added from the previous pagerank to
 * the current one.
 * If the residual is smaller than the tolerance, that is not reflected to
 * the next pagerank.
 */
//! [scalarreduction]
void
ComputePRResidual(
    Graph* graph, DeltaArray& delta, ResidualArray& residual,
    katana::analytics::PagerankPlan plan) {
  unsigned int iterations = 0;
  katana::GAccumulator<unsigned int> accum;

  while (true) {
    katana::do_all(
        katana::iterate(*graph),
        [&](const GNode& src) {
          auto& sdata_value = graph->GetData<NodeValue>(src);
          auto& sdata_nout = graph->GetData<NodeNout>(src);
          delta[src] = 0;

          //! Only the residual higher than tolerance will be reflected
          //! to the pagerank.
          if (residual[src] > plan.tolerance()) {
            PRTy old_residual = residual[src];
            residual[src] = 0.0;
            sdata_value += old_residual;
            if (sdata_nout > 0) {
              delta[src] = old_residual * plan.alpha() / sdata_nout;
              accum += 1;
            }
          }
        },
        katana::no_stats(), katana::loopname("PageRank_delta"));

    katana::do_all(
        katana::iterate(*graph),
        [&](const GNode& src) {
          float sum = 0;
          for (auto nbr : graph->edges(src)) {
            auto dest = graph->GetEdgeDest(nbr);
            if (delta[*dest] > 0) {
              sum += delta[*dest];
            }
          }
          if (sum > 0) {
            residual[src] = sum;
          }
        },
        katana::steal(),
        katana::chunk_size<katana::analytics::PagerankPlan::kChunkSize>(),
        katana::no_stats(), katana::loopname("PageRank"));

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
}

/**
 * PageRank pull topological.
 * Always calculate the new pagerank for each iteration.
 */
void
ComputePRTopological(Graph* graph, katana::analytics::PagerankPlan plan) {
  unsigned int iteration = 0;
  katana::GAccumulator<float> accum;

  float base_score = (1.0f - plan.alpha()) / graph->size();
  while (true) {
    katana::do_all(
        katana::iterate(*graph),
        [&](const GNode& src) {
          auto& sdata_value = graph->GetData<NodeValue>(src);
          float sum = 0.0;

          for (auto jj : graph->edges(src)) {
            auto dest = graph->GetEdgeDest(jj);
            auto& ddata_value = graph->GetData<NodeValue>(dest);
            auto& ddata_nout = graph->GetData<NodeNout>(dest);
            sum += ddata_value / ddata_nout;
          }

          //! New value of pagerank after computing contributions from
          //! incoming edges in the original graph.
          float value = sum * plan.alpha() + base_score;
          //! Find the delta in new and old pagerank values.
          float diff = std::fabs(value - sdata_value);

          //! Do not update pagerank before the diff is computed since
          //! there is a data dependence on the pagerank value.
          sdata_value = value;
          accum += diff;
        },
        katana::no_stats(), katana::steal(),
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
}

}  // namespace

katana::Result<void>
PagerankPullTopological(
    katana::PropertyGraph* pg, const std::string& output_property_name,
    katana::analytics::PagerankPlan plan) {
  katana::EnsurePreallocated(2, 3 * pg->num_nodes() * sizeof(NodeData));

  katana::analytics::TemporaryPropertyGuard temporary_property{pg};

  if (auto result = katana::analytics::ConstructNodeProperties<NodeData>(
          pg, {output_property_name, temporary_property.name()});
      !result) {
    return result.error();
  }

  auto graph_result =
      Graph::Make(pg, {output_property_name, temporary_property.name()}, {});
  if (!graph_result) {
    return graph_result.error();
  }
  Graph graph = graph_result.value();

  InitNodeDataTopological(&graph);
  ComputeOutDeg(&graph);

  katana::StatTimer exec_time("PagerankPullTopological");
  exec_time.start();
  ComputePRTopological(&graph, plan);
  exec_time.stop();

  return katana::ResultSuccess();
}

katana::Result<void>
PagerankPullResidual(
    katana::PropertyGraph* pg, const std::string& output_property_name,
    katana::analytics::PagerankPlan plan) {
  katana::EnsurePreallocated(2, 3 * pg->num_nodes() * sizeof(NodeData));

  katana::analytics::TemporaryPropertyGuard temporary_property{pg};

  if (auto result = katana::analytics::ConstructNodeProperties<NodeData>(
          pg, {output_property_name, temporary_property.name()});
      !result) {
    return result.error();
  }

  auto graph_result =
      Graph::Make(pg, {output_property_name, temporary_property.name()}, {});
  if (!graph_result) {
    return graph_result.error();
  }
  Graph graph = graph_result.value();

  DeltaArray delta;
  delta.allocateInterleaved(pg->num_nodes());
  ResidualArray residual;
  residual.allocateInterleaved(pg->num_nodes());

  InitNodeDataResidual(&graph, delta, residual, plan);
  ComputeOutDeg(&graph);

  katana::StatTimer exec_time("PagerankPullResidual");
  exec_time.start();
  ComputePRResidual(&graph, delta, residual, plan);
  exec_time.stop();

  return katana::ResultSuccess();
}
