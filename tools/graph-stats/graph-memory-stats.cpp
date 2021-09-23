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
#include <string>

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

// void
// printSchema(
//     const std::unique_ptr<katana::PropertyGraph> graph,
//     const std::shared_ptr<arrow::Schema>& schema) {
//   for (int32_t i = 0; i < schema->num_fields(); ++i) {
//     auto prop_name = schema->field(i)->name();
//     auto field = schema->GetFieldByName(prop_name);
//     int prop_size = sizeof(field);
//     std::cout << prop_name << "type is: " << field->type()
//               << " size is: " << prop_size << "\n";
//   }
// }

void
doNonGroupingAnalysis(const std::unique_ptr<katana::PropertyGraph> graph) {
  auto node_schema = graph->full_node_schema();
  auto edge_schema = graph->full_edge_schema();

  for (int32_t i = 0; i < node_schema->num_fields(); ++i) {
    auto prop_name = node_schema->field(i)->name();
    int prop_size = sizeof(edge_schema->field(i));
    std::cout << prop_name << "type is: " << node_schema->field(i)->type()
              << " size is: " << prop_size << "\n";
  }

  std::cout << "Edge Schema\n";

  for (int32_t i = 0; i < edge_schema->num_fields(); ++i) {
    auto prop_name = edge_schema->field(i)->name();
    int prop_size = sizeof(edge_schema->field(i));
    std::cout << prop_name << "type is: " << edge_schema->field(i)->type()
              << " size is: " << prop_size << "\n";
  }
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