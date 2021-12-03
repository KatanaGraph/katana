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

#include "katana/AtomicHelpers.h"
#include "katana/Properties.h"
#include "katana/TypedPropertyGraph.h"
#include "katana/analytics/Utils.h"
#include "pagerank-impl.h"

using katana::atomicAdd;

namespace {

struct NodeResidual : katana::AtomicPODProperty<PRTy> {};

using NodeData = std::tuple<NodeValue, NodeResidual>;
using EdgeData = std::tuple<>;
typedef katana::TypedPropertyGraph<NodeData, EdgeData> Graph;
typedef typename Graph::Node GNode;

void
InitializeNodeResidual(Graph& graph, katana::analytics::PagerankPlan plan) {
  katana::do_all(
      katana::iterate(graph),
      [&](const GNode& n) {
        graph.GetData<NodeResidual>(n) = plan.initial_residual();
        graph.GetData<NodeValue>(n) = 0;
      },
      katana::no_stats(), katana::loopname("Initialize"));
}

}  // namespace

katana::Result<void>
PagerankPushAsynchronous(
    tsuba::TxnContext* txn_ctx, katana::PropertyGraph* pg,
    const std::string& output_property_name,
    katana::analytics::PagerankPlan plan) {
  katana::EnsurePreallocated(5, 5 * pg->num_nodes() * sizeof(NodeData));
  katana::ReportPageAllocGuard page_alloc;

  katana::analytics::TemporaryPropertyGuard temporary_property{
      pg->NodeMutablePropertyView()};

  if (auto result = katana::analytics::ConstructNodeProperties<NodeData>(
          txn_ctx, pg, {output_property_name, temporary_property.name()});
      !result) {
    return result.error();
  }

  auto graph_result =
      Graph::Make(pg, {output_property_name, temporary_property.name()}, {});
  if (!graph_result) {
    return graph_result.error();
  }
  Graph graph = graph_result.value();

  InitializeNodeResidual(graph, plan);

  typedef katana::PerSocketChunkFIFO<
      katana::analytics::PagerankPlan::kChunkSize>
      WL;
  katana::for_each(
      katana::iterate(graph),
      [&](const GNode& src, auto& ctx) {
        auto& src_residual = graph.GetData<NodeResidual>(src);
        if (src_residual > plan.tolerance()) {
          PRTy old_residual = src_residual.exchange(0.0);
          auto& src_value = graph.GetData<NodeValue>(src);
          src_value += old_residual;
          int src_nout = graph.edges(src).size();
          if (src_nout > 0) {
            PRTy delta = old_residual * plan.alpha() / src_nout;
            //! For each out-going neighbors.
            for (const auto& jj : graph.edges(src)) {
              auto dest = graph.GetEdgeDest(jj);
              auto& dest_residual = graph.GetData<NodeResidual>(dest);
              if (delta > 0) {
                auto old = atomicAdd(dest_residual, delta);
                if ((old < plan.tolerance()) &&
                    (old + delta >= plan.tolerance())) {
                  ctx.push(*dest);
                }
              }
            }
          }
        }
      },
      katana::loopname("PushResidualAsynchronous"),
      katana::disable_conflict_detection(), katana::wl<WL>());

  return katana::ResultSuccess();
}

katana::Result<void>
PagerankPushSynchronous(
    tsuba::TxnContext* txn_ctx, katana::PropertyGraph* pg,
    const std::string& output_property_name,
    katana::analytics::PagerankPlan plan) {
  katana::EnsurePreallocated(5, 5 * pg->num_nodes() * sizeof(NodeData));
  katana::ReportPageAllocGuard page_alloc;

  katana::analytics::TemporaryPropertyGuard temporary_property{
      pg->NodeMutablePropertyView()};

  if (auto result = katana::analytics::ConstructNodeProperties<NodeData>(
          txn_ctx, pg, {output_property_name, temporary_property.name()});
      !result) {
    return result.error();
  }

  auto graph_result =
      Graph::Make(pg, {output_property_name, temporary_property.name()}, {});
  if (!graph_result) {
    return graph_result.error();
  }
  Graph graph = graph_result.value();

  InitializeNodeResidual(graph, plan);

  struct Update {
    PRTy delta;
    Graph::edge_iterator beg;
    Graph::edge_iterator end;
  };

  constexpr ptrdiff_t kEdgeTileSize = 128;

  katana::InsertBag<Update> updates;
  katana::InsertBag<GNode> active_nodes;

  katana::do_all(
      katana::iterate(graph), [&](const auto& src) { active_nodes.push(src); },
      katana::no_stats());

  size_t iter = 0;
  for (; !active_nodes.empty() && iter < plan.max_iterations(); ++iter) {
    katana::do_all(
        katana::iterate(active_nodes),
        [&](const GNode& src) {
          auto& sdata_residual = graph.GetData<NodeResidual>(src);

          if (sdata_residual > plan.tolerance()) {
            PRTy old_residual = sdata_residual;
            graph.GetData<NodeValue>(src) += old_residual;
            sdata_residual = 0.0;

            int src_nout = graph.edges(src).size();
            PRTy delta = old_residual * plan.alpha() / src_nout;

            auto beg = graph.edge_begin(src);
            const auto end = graph.edge_end(src);

            KATANA_LOG_ASSERT(beg <= end);

            //! Edge tiling for large outdegree nodes.
            if ((end - beg) > kEdgeTileSize) {
              for (; beg + kEdgeTileSize < end;) {
                auto ne = beg + kEdgeTileSize;
                updates.push(Update{delta, beg, ne});
                beg = ne;
              }
            }

            if ((end - beg) > 0) {
              updates.push(Update{delta, beg, end});
            }
          }
        },
        katana::steal(),
        katana::chunk_size<katana::analytics::PagerankPlan::kChunkSize>(),
        katana::loopname("CreateEdgeTiles"), katana::no_stats());

    active_nodes.clear();

    katana::do_all(
        katana::iterate(updates),
        [&](const Update& up) {
          //! For each out-going neighbors.
          for (auto jj = up.beg; jj != up.end; ++jj) {
            auto dest = graph.GetEdgeDest(jj);
            auto& ddata_residual = graph.GetData<NodeResidual>(dest);
            auto old = atomicAdd(ddata_residual, up.delta);
            //! If fabs(old) is greater than tolerance, then it would
            //! already have been processed in the previous do_all
            //! loop.
            if ((old <= plan.tolerance()) &&
                (old + up.delta >= plan.tolerance())) {
              active_nodes.push(*dest);
            }
          }
        },
        katana::steal(),
        katana::chunk_size<katana::analytics::PagerankPlan::kChunkSize>(),
        katana::loopname("PushResidualSynchronous"));

    updates.clear();
  }
  return katana::ResultSuccess();
}
