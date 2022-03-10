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

#include "Lonestar/BoilerPlate.h"

#include <sstream>

#include "katana/SharedMemSys.h"

//! standard global options to the benchmarks
llvm::cl::opt<bool> skipVerify(
    "noverify", llvm::cl::desc("Skip verification step (default value false)"),
    llvm::cl::init(false));
llvm::cl::opt<int> numThreads(
    "t", llvm::cl::desc("Number of threads (default value 1)"),
    llvm::cl::init(1));
llvm::cl::opt<std::string> statFile(
    "statFile",
    llvm::cl::desc("ouput file to print stats to (default value empty)"),
    llvm::cl::init(""));

//! Flag that forces user to be aware that they should be passing in a
//! symmetric graph.
llvm::cl::opt<bool> symmetricGraph(
    "symmetricGraph",
    llvm::cl::desc("Specify that the input graph is symmetric"),
    llvm::cl::init(false));

llvm::cl::opt<std::string> edge_property_name(
    "edgePropertyName",
    llvm::cl::desc("name of the edge property to the loaded"),
    llvm::cl::init(""));

llvm::cl::opt<std::string> outputLocation(
    "outputLocation",
    llvm::cl::desc(
        "Location (directory) to write results to when output is true"));

llvm::cl::opt<bool> output(
    "output", llvm::cl::desc("Write result (default false)"),
    llvm::cl::init(false));

llvm::cl::opt<std::string> node_types(
    "node_types", llvm::cl::desc("<node types to project>"));

llvm::cl::opt<std::string> edge_types(
    "edge_types", llvm::cl::desc("<edge types to project>"));

static void
LonestarPrintVersion(llvm::raw_ostream& out) {
  out << "LoneStar Benchmark Suite v" << katana::getVersion() << " ("
      << katana::getRevision() << ")\n";
  out.flush();
}

//! initialize lonestar benchmark
std::unique_ptr<katana::SharedMemSys>
LonestarStart(int argc, char** argv) {
  return LonestarStart(argc, argv, nullptr, nullptr, nullptr, nullptr);
}

//! initialize lonestar benchmark
std::unique_ptr<katana::SharedMemSys>
LonestarStart(
    int argc, char** argv, const char* app, const char* desc, const char* url,
    llvm::cl::opt<std::string>* input) {
  llvm::cl::SetVersionPrinter(LonestarPrintVersion);
  llvm::cl::ParseCommandLineOptions(argc, argv);

  auto shared_mem_sys = std::make_unique<katana::SharedMemSys>();

  numThreads = katana::setActiveThreads(numThreads);

  katana::SetStatFile(statFile);

  LonestarPrintVersion(llvm::outs());
  llvm::outs() << "Copyright (C) " << katana::getCopyrightYear()
               << " The University of Texas at Austin\n";
  llvm::outs() << "http://iss.ices.utexas.edu/katana/\n\n";
  llvm::outs() << "application: " << (app ? app : "unspecified") << "\n";
  if (desc) {
    llvm::outs() << desc << "\n";
  }
  if (url) {
    llvm::outs() << "http://iss.ices.utexas.edu/?p=projects/katana/benchmarks/"
                 << url << "\n";
  }
  llvm::outs() << "\n";
  llvm::outs().flush();

  std::ostringstream cmdout;
  for (int i = 0; i < argc; ++i) {
    cmdout << argv[i];
    if (i != argc - 1) {
      cmdout << " ";
    }
  }

  katana::ReportParam("(NULL)", "CommandLine", cmdout.str());
  katana::ReportParam("(NULL)", "Threads", numThreads);
  katana::ReportParam("(NULL)", "Hosts", 1);
  if (input) {
    katana::ReportParam("(NULL)", "Input", input->getValue());
  }

  char name[256];
  gethostname(name, 256);
  katana::ReportParam("(NULL)", "Hostname", name);
  return shared_mem_sys;
}

std::shared_ptr<katana::PropertyGraph>
ProjectPropertyGraphForArguments(
    const std::shared_ptr<katana::PropertyGraph>& pg) {
  std::vector<std::string> vec_node_types;
  if (node_types != "") {
    katana::analytics::SplitStringByComma(node_types, &vec_node_types);
  }

  std::vector<std::string> vec_edge_types;
  if (edge_types != "") {
    katana::analytics::SplitStringByComma(edge_types, &vec_edge_types);
  }

  auto pg_view_res = katana::PropertyGraph::MakeProjectedGraph(
      pg,
      vec_node_types.empty() ? std::nullopt
                             : std::make_optional(vec_node_types),
      vec_edge_types.empty() ? std::nullopt
                             : std::make_optional(vec_edge_types));
  if (!pg_view_res) {
    KATANA_LOG_FATAL("Failed to construct projection: {}", pg_view_res.error());
  }

  return std::move(pg_view_res.value());
}
