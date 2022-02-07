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

#ifndef LONESTAR_BOILERPLATE_H
#define LONESTAR_BOILERPLATE_H

#include "Lonestar/Utils.h"
#include "katana/Galois.h"
#include "katana/SharedMemSys.h"
#include "katana/Version.h"
#include "katana/analytics/Utils.h"
#include "llvm/Support/CommandLine.h"

//! standard global options to the benchmarks
extern llvm::cl::opt<bool> skipVerify;
extern llvm::cl::opt<int> numThreads;
extern llvm::cl::opt<std::string> statFile;
extern llvm::cl::opt<bool> symmetricGraph;
extern llvm::cl::opt<std::string> edge_property_name;
//! Where to write output if output is set
extern llvm::cl::opt<std::string> outputLocation;
extern llvm::cl::opt<bool> output;
//! Node and edge types for native projections
extern llvm::cl::opt<std::string> node_types;
extern llvm::cl::opt<std::string> edge_types;

//! initialize lonestar benchmark
std::unique_ptr<katana::SharedMemSys> LonestarStart(
    int argc, char** argv, const char* app, const char* desc, const char* url,
    llvm::cl::opt<std::string>* input);
std::unique_ptr<katana::SharedMemSys> LonestarStart(int argc, char** argv);
#endif
