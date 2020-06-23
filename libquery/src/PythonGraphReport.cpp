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

#include "querying/PythonGraph.h"

#include <iostream>
#include <fstream>

unsigned rightmostSetBitPos(uint32_t n) {
  assert(n != 0);
  if (n & 1)
    return 0;

  // unset rightmost bit and xor with itself
  n = n ^ (n & (n - 1));

  unsigned pos = 0;
  while (n) {
    n >>= 1;
    pos++;
  }
  return pos - 1;
}


void reportGraphStats(AttributedGraph& g) {
  galois::gPrint("GRAPH STATS\n");
  galois::gPrint("-------------------------------------------------------------"
                 "---------\n");
  galois::gPrint("Number of Nodes: ", g.graph.size(), "\n");
  galois::gPrint("Number of Edges: ", g.graph.sizeEdges(), "\n\n");

  // print all node label names
  galois::gPrint("Node Labels:\n");
  galois::gPrint("------------------------------\n");
  for (std::string nLabel : g.nodeLabelNames) {
    galois::gPrint(nLabel, ", ");
  }

  galois::gPrint("\n\n");

  // print all edge label names
  galois::gPrint("Edge Labels:\n");
  galois::gPrint("------------------------------\n");
  for (std::string eLabel : g.edgeLabelNames) {
    galois::gPrint(eLabel, ", ");
  }
  galois::gPrint("\n\n");

  // print all node attribute names
  galois::gPrint("Node Attributes:\n");
  galois::gPrint("------------------------------\n");
  for (auto& nLabel : g.nodeAttributes) {
    galois::gPrint(nLabel.first, ", ");
  }
  galois::gPrint("\n\n");

  // print all edge attribute names
  galois::gPrint("Edge Attributes:\n");
  galois::gPrint("------------------------------\n");
  for (auto& eLabel : g.edgeAttributes) {
    galois::gPrint(eLabel.first, ", ");
  }
  galois::gPrint("\n");

  galois::gPrint("-------------------------------------------------------------"
                 "---------\n");
}
