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
#include "pagerank-impl.h"

katana::Result<void>
katana::analytics::Pagerank(
    const std::shared_ptr<katana::PropertyGraph>& pg,
    const std::string& output_property_name, katana::TxnContext* txn_ctx,
    katana::analytics::PagerankPlan plan) {
  switch (plan.algorithm()) {
  case PagerankPlan::kPullResidual:
    return PagerankPullResidual(pg, output_property_name, plan, txn_ctx);
  case PagerankPlan::kPullTopological:
    return PagerankPullTopological(pg, output_property_name, plan, txn_ctx);
  case PagerankPlan::kPushAsynchronous:
    return PagerankPushAsynchronous(pg, output_property_name, plan, txn_ctx);
  case PagerankPlan::kPushSynchronous:
    return PagerankPushSynchronous(pg, output_property_name, plan, txn_ctx);
  default:
    return katana::ErrorCode::InvalidArgument;
  }
}

/// \cond DO_NOT_DOCUMENT
katana::Result<void>
katana::analytics::PagerankAssertValid(
    [[maybe_unused]] const std::shared_ptr<katana::PropertyGraph>&,
    [[maybe_unused]] const std::string& property_name) {
  // TODO(gill): This should have real checks. amp has no idea what to check.
  return katana::ResultSuccess();
}
/// \endcond

void
katana::analytics::PagerankStatistics::Print(std::ostream& os) {
  os << "Maximum rank = " << max_rank << std::endl;
  os << "Minimum rank = " << min_rank << std::endl;
  os << "Average rank = " << average_rank << std::endl;
}

katana::Result<katana::analytics::PagerankStatistics>
katana::analytics::PagerankStatistics::Compute(
    const std::shared_ptr<katana::PropertyGraph>& pg,
    const std::string& property_name) {
  auto graph_result =
      TypedPropertyGraph<std::tuple<NodeValue>, std::tuple<>>::Make(
          pg, {property_name}, {});
  if (!graph_result) {
    return graph_result.error();
  }
  auto graph = graph_result.value();
  katana::GReduceMax<PRTy> max_rank;
  katana::GReduceMin<PRTy> min_rank;
  katana::GAccumulator<PRTy> distance_sum;

  //! [example of no_stats]
  katana::do_all(
      katana::iterate(graph),
      [&](uint32_t i) {
        PRTy rank = graph.GetData<NodeValue>(i);

        max_rank.update(rank);
        min_rank.update(rank);
        distance_sum += rank;
      },
      katana::loopname("Sanity check"), katana::no_stats());
  //! [example of no_stats]

  return PagerankStatistics{
      max_rank.reduce(), min_rank.reduce(),
      distance_sum.reduce() / graph.size()};
}
