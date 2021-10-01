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

#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>

#include <arrow/type_traits.h>

#include "katana/ArrowVisitor.h"
#include "katana/Galois.h"
#include "katana/Logging.h"
#include "katana/OfflineGraph.h"
#include "llvm/Support/CommandLine.h"

namespace cll = llvm::cl;
static cll::opt<std::string> inputfilename(
    cll::Positional, cll::desc("graph-file"), cll::Required);

static cll::opt<std::string> outputfilename(
    cll::Positional, cll::desc("out-file"), cll::Required);

using map_element = std::unordered_map<std::string, int64_t>;
using map_string_element = std::unordered_map<std::string, std::string>;
using memory_map = std::unordered_map<
    std::string, std::variant<map_element, map_string_element>>;

inline std::shared_ptr<arrow::DataType>
GetArrowType(const arrow::Scalar& scalar) {
  return scalar.type;
}

inline std::shared_ptr<arrow::DataType>
GetArrowType(const arrow::Array& array) {
  return array.type();
}

inline std::shared_ptr<arrow::DataType>
GetArrowType(const arrow::ArrayBuilder* builder) {
  return builder->type();
}

struct Visitor : public katana::ArrowVisitor {
  using ResultType = katana::Result<int64_t>;
  using AcceptTypes = std::tuple<katana::AcceptAllFlatTypes>;

  template <typename ArrowType, typename ArrayType>
  arrow::enable_if_null<ArrowType, ResultType> Call(const ArrayType& scalars) {
    std::cout << scalars.total_values_length() << "\n";
    return 0;
  }

  template <typename ArrowType, typename ArrayType>
  std::enable_if_t<
      arrow::is_number_type<ArrowType>::value ||
          arrow::is_boolean_type<ArrowType>::value ||
          arrow::is_temporal_type<ArrowType>::value,
      ResultType>
  Call(const ArrayType& scalars) {
    // ResultType width = 0;
    std::cout << scalars.total_values_length() << "\n";
    return 0;
  }

  template <typename ArrowType, typename ArrayType>
  arrow::enable_if_string_like<ArrowType, ResultType> Call(
      const ArrayType& scalars) {
    std::cout << scalars.total_values_length() << "\n";

    return 0;
  }

  template <typename Param>
  ResultType AcceptFailed(Param&& param) {
    return KATANA_ERROR(
        "Instant functions do not accept {}", GetArrowType(param)->ToString());
  }
};

// struct Visitor : public katana::ArrowVisitor {
//   const std::shared_ptr<arrow::Scalar>& scalar;
//   Visitor(const std::shared_ptr<arrow::Scalar>& input) : scalar(input) {}
//   using ResultType = katana::Result<int64_t>;
//   using AcceptTypes = std::tuple<katana::AcceptAllArrowTypes>;

//   template <typename ArrowType, typename WidthType>
//   arrow::enable_if_null<ArrowType, ResultType> Call(
//       const WidthType& width_tracker) {
//     width_tracker = 0;
//     return width_tracker;
//   }

//   template <typename ArrowType, typename WidthType>
//   std::enable_if_t<
//       arrow::is_number_type<ArrowType>::value ||
//           arrow::is_boolean_type<ArrowType>::value ||
//           arrow::is_temporal_type<ArrowType>::value,
//       ResultType>
//   Call(const WidthType& width_tracker) {
//     using ScalarType = typename arrow::TypeTraits<ArrowType>::ScalarType;
//     if (scalar != nullptr && scalar->is_valid) {
//       const ScalarType* typed_scalar = static_cast<ScalarType*>(scalar.get());
//       return typed_scalar->value;
//     } else {
//       return KATANA_ERROR(
//           katana::ErrorCode::ArrowError, "arrow visitor failed to read: NULL");
//     }
//   }

//   template <typename ArrowType, typename WidthType>
//   arrow::enable_if_string_like<ArrowType, ResultType> Call(
//       const WidthType& width_tracker) {
//     using ScalarType = typename arrow::TypeTraits<ArrowType>::ScalarType;
//     if (scalar != nullptr && scalar->is_valid) {
//       // ->value->ToString() works, scalar->ToString() yields "..."
//       const ScalarType* typed_scalar = static_cast<ScalarType*>(scalar.get());
//       auto res = (arrow::util::string_view)(*typed_scalar->value);
//       return res;
//     } else {
//       return KATANA_ERROR(
//           katana::ErrorCode::ArrowError, "arrow visitor failed to read: NULL");
//     }
//   }

//   template <typename ArrowType, typename WidthType>
//   std::enable_if_t<
//       arrow::is_list_type<ArrowType>::value ||
//           arrow::is_struct_type<ArrowType>::value,
//       ResultType>
//   Call(const WidthType& width_tracker) {
//     using ScalarType = typename arrow::TypeTraits<ArrowType>::ScalarType;
//     // use a visitor to traverse more complex types
//     Visitor visitor(scalar);
//     if (scalar != nullptr && scalar->is_valid) {
//       const ScalarType* typed_scalar = static_cast<ScalarType*>(scalar.get());
//       KATANA_CHECKED(visitor.Call<ArrowType>(*typed_scalar));
//     }
//   }

//   ResultType AcceptFailed(const arrow::Scalar& scalar) {
//     return KATANA_ERROR(
//         katana::ErrorCode::ArrowError, "no matching type {}",
//         scalar.type->name());
//   }
// };

void
PrintAtomicTypes(const std::vector<std::string>& atomic_types) {
  for (auto atype : atomic_types) {
    std::cout << atype << "\n";
  }
}

void
PrintMapping(const std::unordered_map<std::string, int64_t>& u) {
  std::cout << "\n";
  for (const auto& n : u) {
    std::cout << n.first << " : " << n.second << "\n";
  }
  std::cout << "\n";
}

void
PrintStringMapping(const std::unordered_map<std::string, std::string>& u) {
  std::cout << "\n";
  for (const auto& n : u) {
    std::cout << n.first << " : " << n.second << "\n";
  }
  std::cout << "\n";
}

void
RunVisit(const std::shared_ptr<arrow::Array> scalars) {
  int64_t total = 0;
  Visitor v;
  arrow::Array* arr = scalars.get();
  auto res = katana::VisitArrow(v, *arr);
  KATANA_LOG_VASSERT(res, "unexpected errror {}", res.error());
  total += res.value();

  // KATANA_LOG_VASSERT(
  //     total == scalars->length(), "{} != {}", total, scalars->length());
}

void
InsertPropertyTypeMemoryData(
    const std::unique_ptr<katana::PropertyGraph>& g,
    const std::unordered_map<std::string, int64_t>& u,
    const std::vector<std::string>& list_type_names) {
  std::cout << g->num_nodes() << "\n";
  for (auto prop_name : list_type_names) {
    if (g->HasAtomicEdgeType(prop_name)) {
      auto prop_type = g->GetEdgeEntityTypeID(prop_name);
      std::cout << prop_name << " : " << prop_type << "\n";
      std::cout << prop_name
                << " Has Atomic Type : " << g->HasAtomicNodeType(prop_name)
                << "\n";
    }
  }
  PrintMapping(u);
}

void
GatherMemoryAllocation(
    const std::shared_ptr<arrow::Schema> schema,
    const std::unique_ptr<katana::PropertyGraph>& g, map_element& allocations,
    map_element& usage, map_element& width, map_string_element& types,
    bool node_or_edge) {
  std::shared_ptr<arrow::Array> prop_field;
  int total_alloc = 0;
  int total_usage = 0;
  int alloc_size = 0;
  int prop_size = 0;

  for (int32_t i = 0; i < schema->num_fields(); ++i) {
    std::string prop_name = schema->field(i)->name();
    auto dtype = schema->field(i)->type();
    if (node_or_edge) {
      prop_field = g->GetNodeProperty(prop_name).value()->chunk(0);
    } else {
      prop_field = g->GetEdgeProperty(prop_name).value()->chunk(0);
    }
    alloc_size = 0;
    prop_size = 0;
    auto bit_width = arrow::bit_width(dtype->id());
    RunVisit(prop_field);
    for (auto j = 0; j < prop_field->length(); j++) {
      if (prop_field->IsValid(j)) {
        auto scal_ptr = *prop_field->GetScalar(j);
        auto data = *scal_ptr;
        prop_size += sizeof(data) / 8;
      }
      alloc_size += bit_width / 8;
    }

    total_alloc += alloc_size;
    total_usage += prop_size;

    allocations.insert(std::pair(prop_name, alloc_size));
    usage.insert(std::pair(prop_name, prop_size));
    width.insert(std::pair(prop_name, bit_width));
    types.insert(std::pair(prop_name, dtype->name()));
    // std::cout << "Size: " << visited_arr->value().size() << "\n";
  }
  allocations.insert(std::pair("Total-Alloc", total_alloc));
  usage.insert(std::pair("Total-Usage", total_usage));
}

void
doMemoryAnalysis(const std::unique_ptr<katana::PropertyGraph> graph) {
  memory_map mem_map = {};
  map_element basic_raw_stats = {};
  auto node_schema = graph->full_node_schema();
  auto edge_schema = graph->full_edge_schema();
  int64_t total_num_node_props = node_schema->num_fields();
  int64_t total_num_edge_props = edge_schema->num_fields();

  basic_raw_stats.insert(std::pair("Node-Schema-Size", total_num_node_props));
  basic_raw_stats.insert(std::pair("Edge-Schema-Size", total_num_edge_props));
  basic_raw_stats.insert(
      std::pair("Number-Node-Atomic-Types", graph->GetNumNodeAtomicTypes()));
  basic_raw_stats.insert(
      std::pair("Number-Edge-Atomic-Types", graph->GetNumEdgeAtomicTypes()));
  basic_raw_stats.insert(
      std::pair("Number-Node-Entity-Types", graph->GetNumNodeEntityTypes()));
  basic_raw_stats.insert(
      std::pair("Number-Edge-Entity-Types", graph->GetNumNodeEntityTypes()));
  basic_raw_stats.insert(std::pair("Number-Nodes", graph->num_nodes()));
  basic_raw_stats.insert(std::pair("Number-Edges", graph->num_edges()));

  auto atomic_node_types = graph->ListAtomicNodeTypes();

  auto atomic_edge_types = graph->ListAtomicEdgeTypes();

  map_string_element all_node_prop_stats;
  map_string_element all_edge_prop_stats;
  map_element all_node_width_stats;
  map_element all_edge_width_stats;
  map_element all_node_alloc;
  map_element all_edge_alloc;
  map_element all_node_usage;
  map_element all_edge_usage;

  all_node_prop_stats.insert(std::pair("kUnknownName", "uint8"));
  all_edge_prop_stats.insert(std::pair("kUnknownName", "uint8"));

  all_node_width_stats.insert(std::pair("kUnknownName", sizeof(uint8_t) * 8));
  all_edge_width_stats.insert(std::pair("kUnknownName", sizeof(uint8_t) * 8));

  GatherMemoryAllocation(
      node_schema, graph, all_node_alloc, all_node_usage, all_node_width_stats,
      all_node_prop_stats, true);

  std::cout << "Node Memory Stats"
            << "\n";
  std::cout << "---------------------------------------------------"
            << "\n";
  std::cout << "Type Statistics"
            << "\n";
  PrintStringMapping(all_node_prop_stats);

  std::cout << "Width Statstics"
            << "\n";
  PrintMapping(all_node_width_stats);

  std::cout << "Node Memory Allocation Statistics"
            << "\n";
  PrintMapping(all_node_alloc);

  std::cout << "Node Actual Memory Usage"
            << "\n";
  PrintMapping(all_node_usage);

  mem_map.insert(std::pair("Node-Types", all_node_prop_stats));

  GatherMemoryAllocation(
      edge_schema, graph, all_edge_alloc, all_edge_usage, all_edge_width_stats,
      all_edge_prop_stats, false);

  std::cout << "Edge Memory Stats"
            << "\n";
  std::cout << "---------------------------------------------------"
            << "\n";
  std::cout << "Type Statistics"
            << "\n";
  PrintStringMapping(all_edge_prop_stats);

  std::cout << "Width Statstics"
            << "\n";
  PrintMapping(all_edge_width_stats);
  std::cout << "Edge Memory Allocation Statistics"
            << "\n";
  PrintMapping(all_edge_alloc);

  std::cout << "Edge Actual Memory Usage"
            << "\n";
  PrintMapping(all_edge_usage);
  mem_map.insert(std::pair("Edge-Types", all_edge_prop_stats));

  mem_map.insert(std::pair("General-Stats", basic_raw_stats));
  PrintMapping(basic_raw_stats);
}

int
main(int argc, char** argv) {
  katana::SharedMemSys sys;
  llvm::cl::ParseCommandLineOptions(argc, argv);
  auto g = katana::PropertyGraph::Make(inputfilename, tsuba::RDGLoadOptions());
  doMemoryAnalysis(std::move(g.value()));
  return 1;
}