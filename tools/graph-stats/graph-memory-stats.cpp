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
#include <cstdlib>
// …but also inspect the stack and print some results.
#include <execinfo.h>
#include <unistd.h>

#include <fstream>
// Import bad_alloc, expected in case of errors.
#include <new>

#include "katana/Galois.h"
#include "katana/LCGraph.h"
#include "katana/OfflineGraph.h"
#include "llvm/Support/CommandLine.h"

namespace cll = llvm::cl;
static cll::opt<std::string> inputfilename(
    cll::Positional, cll::desc("graph-file"), cll::Required);

static cll::opt<std::string> outputfilename(
    cll::Positional, cll::desc("out-file"), cll::Required);

typedef katana::OfflineGraph Graph;
typedef Graph::GraphNode GNode;

static void
dumpStackTrace(std::ofstream& memoryProfile) {
  // Record 150 pointers to stack frame - enough for the example program.
  const int maximumStackSize = 150;
  void* callStack[maximumStackSize];
  size_t framesInUse = backtrace(callStack, maximumStackSize);
  // Now callStack is full of pointers. Request the names of the functions matching each frame.
  char** mangledFunctionNames = backtrace_symbols(callStack, framesInUse);
  // Writes all the function names in the stream.
  for (size_t i = 0; i < framesInUse; ++i)
    memoryProfile << mangledFunctionNames[i] << std::endl;
  // To be fair, we should release mangledFunctionNames with free…
}

static std::ofstream&
resultFile() {
  static std::ofstream memoryProfile;
  static bool open = false;  // Init on 1st use, as usual.
  if (!open) {
    memoryProfile.open(outputfilename);
    open = true;
  }
  // Else, handle errors, close the file…
  // We won’t do it, to keep the example simple.
  return memoryProfile;
}

void*
operator new(std::size_t sz) {
  // Allocate the requested memory for the caller.
  void* requestedMemory = std::malloc(sz);
  if (!requestedMemory)
    throw std::bad_alloc();
  // Share our allocations with the world.
  std::ofstream& memoryProfile = resultFile();
  memoryProfile << "Allocation, size = " << sz << " at "
                << static_cast<void*>(requestedMemory) << std::endl;
  dumpStackTrace(memoryProfile);
  memoryProfile << "-----------" << std::endl;  // Poor man’s separator.
  return requestedMemory;
}

// void
// doNonGroupingAnalysis(Graph& graph, ofstream& myfile) {}

// void
// doGroupingAnalysis(Graph& graph, ofstream& myfile) {}

int
main(int argc, char** argv) {
  llvm::cl::ParseCommandLineOptions(argc, argv);
  std::ofstream myfile("example.txt");
  auto g = katana::PropertyGraph::Make(inputfilename);
  // try {
  //   Graph graph(inputfilename);
  //   return 0;
  // } catch (...) {
  //   std::cerr << "failed\n";
  //   return 1;
  // }

  return 1;
}