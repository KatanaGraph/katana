/*
 * This file belongs to the Galois project, a C++ library for exploiting
 * parallelism. The code is being released under the terms of the 3-Clause BSD
 * License (a copy is located in LICENSE.txt at the top-level directory).
 *
 * Copyright (C) 2020, The University of Texas at Austin. All rights reserved.
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

#include "betweenness_centrality_impl.h"

using namespace katana::analytics;

const BetweennessCentralitySources
    katana::analytics::kBetweennessCentralityAllNodes =
        std::numeric_limits<uint32_t>::max();

katana::Result<void>
katana::analytics::BetweennessCentrality(
    katana::PropertyGraph* pg, const std::string& output_property_name,
    katana::TxnContext* txn_ctx, const BetweennessCentralitySources& sources,
    BetweennessCentralityPlan plan) {
  switch (plan.algorithm()) {
    //TODO (gill) Needs bidirectional graph (CSR_CSC)
    //   case Asynchronous:
    //     // see AsyncStructs.h
    //     katana::gInfo("Running async BC");
    //     doAsyncBC();
    //     break;
  case BetweennessCentralityPlan::kLevel:
    return BetweennessCentralityLevel(
        pg, sources, output_property_name, plan, txn_ctx);
  case BetweennessCentralityPlan::kOuter:
    return BetweennessCentralityOuter(
        pg, sources, output_property_name, plan, txn_ctx);
  default:
    return katana::ErrorCode::InvalidArgument;
  }
}

void
BetweennessCentralityStatistics::Print(std::ostream& os) {
  os << "Maximum centrality = " << max_centrality << std::endl;
  os << "Minimum centrality = " << min_centrality << std::endl;
  os << "Average centrality = " << average_centrality << std::endl;
}

katana::Result<BetweennessCentralityStatistics>
BetweennessCentralityStatistics::Compute(
    katana::PropertyGraph* pg, const std::string& output_property_name) {
  auto values_result = pg->GetNodePropertyTyped<float>(output_property_name);
  if (!values_result) {
    return values_result.error();
  }
  auto values = values_result.value();

  katana::GReduceMax<float> accum_max;
  katana::GReduceMin<float> accum_min;
  katana::GAccumulator<float> accum_sum;

  // get max, min, sum of BC values using accumulators and reducers
  katana::do_all(
      katana::iterate((uint64_t)0, pg->num_nodes()),
      [&](uint32_t n) {
        accum_max.update(values->Value(n));
        accum_min.update(values->Value(n));
        accum_sum += values->Value(n);
      },
      katana::no_stats(),
      katana::loopname("Betweenness Centrality Statistics"));

  return BetweennessCentralityStatistics{
      accum_max.reduce(), accum_min.reduce(),
      accum_sum.reduce() / pg->num_nodes()};
}
