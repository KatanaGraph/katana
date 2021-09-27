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

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>
// Our special new must allocate memory as expected…
#include <cstdio>
// …but also inspect the stack and print some results.
#include <execinfo.h>
#include <unistd.h>

// Import bad_alloc, expected in case of errors.
#include <stdlib.h>

#include <type_traits>
#include <typeinfo>
#ifndef _MSC_VER
#include <cxxabi.h>
#endif
#include <memory>

#include "katana/Galois.h"
#include "katana/LCGraph.h"
#include "katana/OfflineGraph.h"
#include "llvm/Support/CommandLine.h"

template <class T>
std::string
type_name() {
  typedef typename std::remove_reference<T>::type TR;
  std::unique_ptr<char, void (*)(void*)> own(
#ifndef _MSC_VER
      abi::__cxa_demangle(typeid(TR).name(), nullptr, nullptr, nullptr),
#else
      nullptr,
#endif
      std::free);
  std::string r = own != nullptr ? own.get() : typeid(TR).name();
  if (std::is_const<TR>::value)
    r += " const";
  if (std::is_volatile<TR>::value)
    r += " volatile";
  if (std::is_lvalue_reference<T>::value)
    r += "&";
  else if (std::is_rvalue_reference<T>::value)
    r += "&&";
  return r;
}

namespace cll = llvm::cl;
static cll::opt<std::string> inputfilename(
    cll::Positional, cll::desc("graph-file"), cll::Required);

static cll::opt<std::string> outputfilename(
    cll::Positional, cll::desc("out-file"), cll::Required);

// std::cout << "decltype(i) is " << type_name<decltype(i)>() << '\n';
void
PrintAtomicTypes(const std::vector<std::string>& atomic_types) {
  for (auto atype : atomic_types) {
    std::cout << atype << "\n";
  }
}

void
PrintMapping(const std::unordered_map<std::string, int64_t>& u) {
  for (const auto& n : u) {
    std::cout << n.first << " : " << n.second << "\n";
  }
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
doNonGroupingAnalysis(const std::unique_ptr<katana::PropertyGraph> graph) {
  using map_element = std::unordered_map<std::string, int64_t>;
  using memory_map = std::unordered_map<std::string, map_element>;
  memory_map mem_map = {};
  map_element basic_raw_stats = {};
  auto node_schema = graph->full_node_schema();
  auto edge_schema = graph->full_edge_schema();
  int64_t total_num_node_props = node_schema->num_fields();
  int64_t total_num_edge_props = edge_schema->num_fields();

  std::cout << "\n";

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

  PrintMapping(basic_raw_stats);

  auto atomic_node_types = graph->ListAtomicNodeTypes();

  auto atomic_edge_types = graph->ListAtomicEdgeTypes();

  // std::cout << "Node Types<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n";
  // PrintAtomicTypes(atomic_node_types);
  // std::cout << "Edge Types<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n";
  // PrintAtomicTypes(atomic_edge_types);

  const katana::GraphTopology& g_topo = graph->topology();

  auto node_iterator = g_topo.all_nodes();
  auto edge_iterator = g_topo.all_edges();
  map_element all_node_prop_stats;
  map_element all_edge_prop_stats;

  std::cout << "\n";
  std::cout << "Node Schema\n";
  std::cout << "---------------------------------------\n";

  for (int32_t i = 0; i < node_schema->num_fields(); ++i) {
    std::string prop_name = node_schema->field(i)->name();
    auto dtype = node_schema->field(i)->type()->name();
    int64_t prop_size = sizeof(node_schema->field(i)->type()->name());
    std::cout << prop_name << " : " << dtype << "\n";
    all_node_prop_stats.insert(std::pair(prop_name, prop_size));
  }

  // PrintMapping(all_node_prop_stats);

  std::cout << "\n";
  std::cout << "Edge Schema\n";
  std::cout << static_cast<arrow::Type::type>(0) << "\n";
  std::cout << "----------------------------------------\n";

  for (int32_t i = 0; i < edge_schema->num_fields(); ++i) {
    std::string prop_name = edge_schema->field(i)->name();
    auto dtype = edge_schema->field(i)->type()->name();
    int64_t prop_size = sizeof(edge_schema->field(i)->type()->name());
    std::cout << prop_name << " : " << dtype << "\n";
    all_node_prop_stats.insert(std::pair(prop_name, prop_size));
  }
  PrintMapping(all_edge_prop_stats);

  std::cout << "\n";
  int64_t node_size = 0;
  for (auto node : node_iterator) {
    auto node_type = graph->GetTypeOfNode(node);
    node_size += sizeof(node_type);
  }
  std::cout << "Total Number of bytes taken up by Nodes: " << node_size << "\n";

  int64_t edge_size = 0;
  for (auto edge : edge_iterator) {
    auto edge_type = graph->GetTypeOfEdge(edge);
    edge_size += sizeof(edge_type);
  }
  std::cout << "Total Number of bytes taken up by Edges: " << edge_size << "\n";
}

int
main(int argc, char** argv) {
  katana::SharedMemSys sys;
  llvm::cl::ParseCommandLineOptions(argc, argv);

  // ofstream memeory_file("example.txt");
  // memeory_file.open();
  // memeory_file << "File containing memory analysis of a graph.\n";
  // memeory_file.close();

  auto g = katana::PropertyGraph::Make(inputfilename, tsuba::RDGLoadOptions());
  std::cout << "Graph Sizeof is: " << sizeof(g) << "\n";
  doNonGroupingAnalysis(std::move(g.value()));
  return 1;
}