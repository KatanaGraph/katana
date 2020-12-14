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

#include "galois/analytics/sssp/sssp.h"

using namespace galois::analytics;

// Explicit instantiations for extern templates
// These are actually already used by the functions below, but having them
// explicit means that changes to the functions below cannot cause accidentally
// remove an instantiation.
/// \cond DO_NOT_DOCUMENT
template galois::Result<void> galois::analytics::Sssp<uint32_t>(
    galois::graphs::PropertyGraph<
        std::tuple<SsspNodeDistance<uint32_t>>,
        std::tuple<SsspEdgeWeight<uint32_t>>>& pg,
    size_t start_node, SsspPlan plan);
template galois::Result<void> galois::analytics::Sssp<int32_t>(
    galois::graphs::PropertyGraph<
        std::tuple<SsspNodeDistance<int32_t>>,
        std::tuple<SsspEdgeWeight<int32_t>>>& pg,
    size_t start_node, SsspPlan plan);
template galois::Result<void> galois::analytics::Sssp<uint64_t>(
    galois::graphs::PropertyGraph<
        std::tuple<SsspNodeDistance<uint64_t>>,
        std::tuple<SsspEdgeWeight<uint64_t>>>& pg,
    size_t start_node, SsspPlan plan);
template galois::Result<void> galois::analytics::Sssp<int64_t>(
    galois::graphs::PropertyGraph<
        std::tuple<SsspNodeDistance<int64_t>>,
        std::tuple<SsspEdgeWeight<int64_t>>>& pg,
    size_t start_node, SsspPlan plan);
template galois::Result<void> galois::analytics::Sssp<float>(
    galois::graphs::PropertyGraph<
        std::tuple<SsspNodeDistance<float>>, std::tuple<SsspEdgeWeight<float>>>&
        pg,
    size_t start_node, SsspPlan plan);
template galois::Result<void> galois::analytics::Sssp<double>(
    galois::graphs::PropertyGraph<
        std::tuple<SsspNodeDistance<double>>,
        std::tuple<SsspEdgeWeight<double>>>& pg,
    size_t start_node, SsspPlan plan);
/// \endcond DO_NOT_DOCUMENT

template <typename Weight>
static galois::Result<void>
SSSPWithWrap(
    galois::graphs::PropertyFileGraph* pfg, size_t start_node,
    std::string edge_weight_property_name, std::string output_property_name,
    SsspPlan plan) {
  if (auto r = ConstructNodeProperties<std::tuple<SsspNodeDistance<Weight>>>(
          pfg, {output_property_name});
      !r) {
    return r.error();
  }
  auto graph = galois::graphs::PropertyGraph<
      std::tuple<SsspNodeDistance<Weight>>,
      std::tuple<SsspEdgeWeight<Weight>>>::
      Make(pfg, {output_property_name}, {edge_weight_property_name});
  if (!graph && graph.error() == galois::ErrorCode::TypeError) {
    GALOIS_LOG_DEBUG(
        "Incorrect edge property type: {}",
        pfg->edge_table()
            ->GetColumnByName(edge_weight_property_name)
            ->type()
            ->ToString());
  }
  if (!graph) {
    return graph.error();
  }

  return galois::analytics::Sssp(graph.value(), start_node, plan);
}

galois::Result<void>
galois::analytics::Sssp(
    graphs::PropertyFileGraph* pfg, size_t start_node,
    std::string edge_weight_property_name, std::string output_property_name,
    SsspPlan plan) {
  switch (pfg->EdgeProperty(edge_weight_property_name)->type()->id()) {
  // TODO: Consider lifting these repetitive clauses into a macro or template
  //  function. For each type we get something like:
  //  case arrow::CTypeTraits<type>::ArrowType::type_id:
  //    return func<type> args;
  case arrow::UInt32Type::type_id:
    return SSSPWithWrap<uint32_t>(
        pfg, start_node, edge_weight_property_name, output_property_name, plan);
  case arrow::Int32Type::type_id:
    return SSSPWithWrap<int32_t>(
        pfg, start_node, edge_weight_property_name, output_property_name, plan);
  case arrow::UInt64Type::type_id:
    return SSSPWithWrap<uint64_t>(
        pfg, start_node, edge_weight_property_name, output_property_name, plan);
  case arrow::Int64Type::type_id:
    return SSSPWithWrap<int64_t>(
        pfg, start_node, edge_weight_property_name, output_property_name, plan);
  case arrow::FloatType::type_id:
    return SSSPWithWrap<float>(
        pfg, start_node, edge_weight_property_name, output_property_name, plan);
  case arrow::DoubleType::type_id:
    return SSSPWithWrap<double>(
        pfg, start_node, edge_weight_property_name, output_property_name, plan);
  default:
    return galois::ErrorCode::TypeError;
  }
}
