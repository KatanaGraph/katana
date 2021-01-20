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

#include "katana/BufferedGraph.h"
#include "katana/FileGraph.h"
#include "katana/Galois.h"
#include "llvm/Support/CommandLine.h"

namespace cll = llvm::cl;

static cll::opt<std::string> inputFilename(
    cll::Positional, cll::desc("<input file>"), cll::Required);
static cll::opt<std::string> mappingFilename(
    cll::Positional, cll::desc("<mapping file>"), cll::Required);
static cll::opt<std::string> outputFilename(
    cll::Positional, cll::desc("<output file>"), cll::Required);

using Writer = katana::FileGraphWriter;

/**
 * Create node map from file
 */
std::map<uint32_t, uint32_t>
createNodeMap() {
  katana::gInfo("Creating node map");
  // read new mapping
  std::ifstream mapFile(mappingFilename);
  mapFile.seekg(0, std::ios_base::end);

  int64_t endOfFile = mapFile.tellg();
  if (!mapFile) {
    KATANA_DIE("failed to read file");
  }

  mapFile.seekg(0, std::ios_base::beg);
  if (!mapFile) {
    KATANA_DIE("failed to read file");
  }

  // remap node listed on line n in the mapping to node n
  std::map<uint32_t, uint32_t> remapper;
  uint64_t counter = 0;
  while (((int64_t)mapFile.tellg() + 1) != endOfFile) {
    uint64_t nodeID;
    mapFile >> nodeID;
    if (!mapFile) {
      KATANA_DIE("failed to read file");
    }
    remapper[nodeID] = counter++;
  }

  KATANA_LOG_ASSERT(remapper.size() == counter);
  katana::gInfo("Remapping ", counter, " nodes");

  katana::gInfo("Node map created");

  return remapper;
}

int
main(int argc, char** argv) {
  katana::SharedMemSys G;
  llvm::cl::ParseCommandLineOptions(argc, argv);

  std::map<uint32_t, uint32_t> remapper = createNodeMap();

  katana::gInfo("Loading graph to remap");
  katana::BufferedGraph<void> graphToRemap;
  graphToRemap.loadGraph(inputFilename);
  katana::gInfo("Graph loaded");

  Writer graphWriter;
  graphWriter.setNumNodes(remapper.size());
  graphWriter.setNumEdges(graphToRemap.sizeEdges());

  // phase 1: count degrees
  graphWriter.phase1();
  katana::gInfo("Starting degree counting");
  size_t prevNumNodes = graphToRemap.size();
  size_t nodeIDCounter = 0;
  for (size_t i = 0; i < prevNumNodes; i++) {
    // see if current node is to be remapped, i.e. exists in the map
    if (remapper.find(i) != remapper.end()) {
      KATANA_LOG_ASSERT(nodeIDCounter == remapper[i]);
      for (auto e = graphToRemap.edgeBegin(i); e < graphToRemap.edgeEnd(i);
           e++) {
        graphWriter.incrementDegree(nodeIDCounter);
      }
      nodeIDCounter++;
    }
  }
  KATANA_LOG_ASSERT(nodeIDCounter == remapper.size());

  // phase 2: edge construction
  graphWriter.phase2();
  katana::gInfo("Starting edge construction");
  nodeIDCounter = 0;
  for (size_t i = 0; i < prevNumNodes; i++) {
    // see if current node is to be remapped, i.e. exists in the map
    if (remapper.find(i) != remapper.end()) {
      KATANA_LOG_ASSERT(nodeIDCounter == remapper[i]);
      for (auto e = graphToRemap.edgeBegin(i); e < graphToRemap.edgeEnd(i);
           e++) {
        uint32_t dst = graphToRemap.edgeDestination(*e);
        KATANA_LOG_ASSERT(remapper.find(dst) != remapper.end());
        graphWriter.addNeighbor(nodeIDCounter, remapper[dst]);
      }
      nodeIDCounter++;
    }
  }
  KATANA_LOG_ASSERT(nodeIDCounter == remapper.size());

  katana::gInfo("Finishing up: outputting graph shortly");

  graphWriter.finish<void>();
  graphWriter.toFile(outputFilename);

  katana::gInfo(
      "new size is ", graphWriter.size(), " num edges ",
      graphWriter.sizeEdges());

  return 0;
}
