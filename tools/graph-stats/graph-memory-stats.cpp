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

#include "katana/Galois.h"
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
    // int64_t prop_size = 1;
    // u.insert(std::pair(prop_name, prop_size));
  }
  PrintMapping(u);
}

void
GatherMemoryAllocation(
    const std::shared_ptr<arrow::Schema> schema,
    const std::unique_ptr<katana::PropertyGraph>& g, map_element& allocations,
    map_element& usage, map_element& width, map_string_element& types) {
  for (int32_t i = 0; i < schema->num_fields(); ++i) {
    std::string prop_name = schema->field(i)->name();
    auto dtype = schema->field(i)->type();
    auto prop_field = g->GetNodeProperty(prop_name).value()->chunk(0);
    int64_t alloc_size = 0;
    int64_t prop_size = 0;
    auto bit_width = arrow::bit_width(dtype->id());

    for (auto j = 0; j < prop_field->length(); j++) {
      if (prop_field->IsValid(j)) {
        auto scal_ptr = *prop_field->GetScalar(j);
        auto data = *scal_ptr;
        prop_size += sizeof(data);
      }
      alloc_size += bit_width;
    }
    allocations.insert(std::pair(prop_name, alloc_size));
    usage.insert(std::pair(prop_name, prop_size));
    width.insert(std::pair(prop_name, bit_width));
    types.insert(std::pair(prop_name, dtype->name()));
  }
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

  const katana::GraphTopology& g_topo = graph->topology();

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
      all_node_prop_stats);

  PrintStringMapping(all_node_prop_stats);
  PrintMapping(all_node_width_stats);
  PrintMapping(all_node_alloc);
  PrintMapping(all_node_usage);
  mem_map.insert(std::pair("Node-Types", all_node_prop_stats));

  GatherMemoryAllocation(
      edge_schema, graph, all_edge_alloc, all_edge_usage, all_edge_width_stats,
      all_edge_prop_stats);

  PrintStringMapping(all_edge_prop_stats);
  PrintMapping(all_edge_width_stats);
  mem_map.insert(std::pair("Edge-Types", all_edge_prop_stats));

  auto node_iterator = g_topo.all_nodes();
  auto edge_iterator = g_topo.all_edges();

  int64_t width;
  int64_t node_size = 0;
  map_element node_dist;
  map_element edge_dist;

  for (auto node : node_iterator) {
    std::string node_type = *graph->GetNodeAtomicTypeName(node);
    width = all_node_width_stats.find(node_type)->second;
    node_dist[node_type]++;
    node_size += width / 8;
  }

  PrintMapping(node_dist);
  mem_map.insert(std::pair("Node-Type-Distribution", node_dist));

  int64_t edge_size = 0;
  for (auto edge : edge_iterator) {
    std::string edge_type = *graph->GetEdgeAtomicTypeName(edge);
    width = all_edge_width_stats.find(edge_type)->second;
    edge_dist[edge_type]++;
    edge_size += width / 8;
  }
  PrintMapping(edge_dist);
  mem_map.insert(std::pair("Edge-Type-Distribution", edge_dist));

  basic_raw_stats.insert(std::pair("Node-Memory-Consumption", node_size));
  basic_raw_stats.insert(std::pair("Edge-Memory-Consumption", edge_size));

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