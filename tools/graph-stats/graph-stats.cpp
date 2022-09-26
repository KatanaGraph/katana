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
#include <iostream>
#include <vector>

#include "katana/Galois.h"
#include "katana/LCGraph.h"
#include "katana/OfflineGraph.h"
#include "llvm/Support/CommandLine.h"
#include "tsuba/RDGInspection.h"

namespace cll = llvm::cl;

static cll::opt<std::string> inputfilename(
    cll::Positional, cll::desc("<graph file>"), cll::Required);
static cll::list<tsuba::StatMode> statModeList(
    cll::desc("Available stats:"),
    cll::values(
        clEnumVal(tsuba::degreehist, "Histogram of degrees"),
        clEnumVal(tsuba::degrees, "Node degrees"),
        clEnumVal(tsuba::maxDegreeNode, "Max Degree Node"),
        clEnumVal(tsuba::dsthist, "Histogram of destinations"),
        clEnumVal(tsuba::indegreehist, "Histogram of indegrees"),
        clEnumVal(
            tsuba::sortedlogoffsethist,
            "Histogram of neighbor offsets with sorted edges"),
        clEnumVal(
            tsuba::sparsityPattern,
            "Pattern of non-zeros when graph is "
            "interpreted as a sparse matrix"),
        clEnumVal(tsuba::summary, "Graph summary")));
static cll::opt<int> numBins(
    "numBins", cll::desc("Number of bins"), cll::init(-1));
static cll::opt<int> columns(
    "columns", cll::desc("Columns for sparsity"), cll::init(80));

typedef katana::OfflineGraph Graph;
typedef Graph::GraphNode GNode;

int
main(int argc, char** argv) {
  llvm::cl::ParseCommandLineOptions(argc, argv);
  try {
    Graph graph(inputfilename);
    for (unsigned i = 0; i != statModeList.size(); ++i) {
      switch (statModeList[i]) {
      case tsuba::degreehist:
        tsuba::doDegreeHistogram(graph, numBins);
        break;
      case tsuba::degrees:
        tsuba::doDegrees(graph);
        break;
      case tsuba::maxDegreeNode:
        tsuba::findMaxDegreeNode(graph);
        break;
      case tsuba::dsthist:
        tsuba::doDestinationHistogram(graph, numBins);
        break;
      case tsuba::indegreehist:
        tsuba::doInDegreeHistogram(graph, numBins);
        break;
      case tsuba::sortedlogoffsethist:
        tsuba::doSortedLogOffsetHistogram(graph);
        break;
      case tsuba::sparsityPattern: {
        unsigned lastrow = ~0;
        tsuba::doSparsityPattern(
            graph, columns, [&lastrow](unsigned, unsigned y, bool val) {
              if (y != lastrow) {
                lastrow = y;
                std::cout << '\n';
              }
              std::cout << (val ? 'x' : '.');
            });
        std::cout << '\n';
        break;
      }
      case tsuba::summary:
        tsuba::doSummary(graph);
        break;
      default:
        std::cerr << "Unknown stat requested\n";
        break;
      }
    }
    return 0;
  } catch (...) {
    std::cerr << "failed\n";
    return 1;
  }
}
