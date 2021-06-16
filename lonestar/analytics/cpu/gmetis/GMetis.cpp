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

#include <stdlib.h>
#include <string.h>

#include <algorithm>
#include <cmath>
#include <fstream>
#include <iostream>
#include <map>
#include <numeric>
#include <set>
#include <vector>

#include "Metis.h"
#include "katana/ReadGraph.h"
#include "katana/Statistics.h"
#include "katana/Timer.h"
//#include "GraphReader.h"
#include "Lonestar/BoilerPlate.h"
#include "katana/FileGraph.h"
#include "katana/LargeArray.h"

namespace cll = llvm::cl;

static const char* name = "GMetis";
static const char* desc =
    "Partitions a graph into K parts and minimizing the graph cut";
static const char* url = "gMetis";

static cll::opt<std::string> inputFile(
    cll::Positional, cll::desc("<input file>"), cll::Required);
static cll::opt<InitialPartMode> partMode(
    cll::desc("Choose a inital part mode:"),
    cll::values(
        clEnumVal(GGP, "GGP"), clEnumVal(GGGP, "GGGP (default)"),
        clEnumVal(MGGGP, "MGGGP")),
    cll::init(GGGP));
static cll::opt<refinementMode> refineMode(
    cll::desc("Choose a refinement mode:"),
    cll::values(
        clEnumVal(BKL, "BKL"), clEnumVal(BKL2, "BKL2 (default)"),
        clEnumVal(ROBO, "ROBO"), clEnumVal(GRACLUS, "GRACLUS")),
    cll::init(BKL2));

static cll::opt<bool> mtxInput(
    "mtxinput",
    cll::desc("Use text mtx files instead of binary galois gr files"),
    cll::init(false));
static cll::opt<bool> weighted(
    "weighted", cll::desc("weighted"), cll::init(false));
static cll::opt<bool> verbose(
    "verbose", cll::desc("verbose output (debugging mode, takes extra time)"),
    cll::init(false));
static cll::opt<std::string> outputFilename(
    "outputFilename", cll::desc("output partition file name"));
static cll::opt<std::string> orderedfile(
    "ordered", cll::desc("output ordered graph file name"));
static cll::opt<std::string> permutationfile(
    "permutation", cll::desc("output permutation file name"));
static cll::opt<int> numPartitions(
    "numPartitions", cll::desc("<Number of partitions>"), cll::Required);
static cll::opt<double> imbalance(
    "balance",
    cll::desc("Fraction deviated from mean partition size (default 0.01)"),
    cll::init(0.01));

// const double COARSEN_FRACTION = 0.9;

/**
 * KMetis Algorithm
 */
void
Partition(MetisGraph* metisGraph, unsigned nparts) {
  unsigned fineMetisGraphWeight = metisGraph->getTotalWeight();
  unsigned meanWeight = ((double)fineMetisGraphWeight) / (double)nparts;
  unsigned coarsenTo = 20 * nparts;

  if (verbose)
    std::cout << "Starting coarsening: \n";
  katana::StatTimer T("Coarsen");
  T.start();
  auto mcg =
      std::unique_ptr<MetisGraph>(coarsen(metisGraph, coarsenTo, verbose));
  T.stop();
  if (verbose)
    std::cout << "Time coarsen: " << T.get() << "\n";

  katana::StatTimer T2("Partition");
  T2.start();
  std::vector<partInfo> parts;
  parts = partition(mcg.get(), fineMetisGraphWeight, nparts, partMode);
  T2.stop();

  if (verbose)
    std::cout << "Init edge cut : " << computeCut(*mcg->getGraph()) << "\n\n";

  std::vector<partInfo> initParts = parts;
  if (verbose)
    std::cout << "Time clustering:  " << T2.get() << '\n';

  if (verbose) {
    switch (refineMode) {
    case BKL2:
      std::cout << "Sorting refinnement with BKL2\n";
      break;
    case BKL:
      std::cout << "Sorting refinnement with BKL\n";
      break;
    case ROBO:
      std::cout << "Sorting refinnement with ROBO\n";
      break;
    case GRACLUS:
      std::cout << "Sorting refinnement with GRACLUS\n";
      break;
    default:
      abort();
    }
  }

  katana::StatTimer T3("Refine");
  T3.start();
  refine(
      mcg.get(), parts, meanWeight - (unsigned)(meanWeight * imbalance),
      meanWeight + (unsigned)(meanWeight * imbalance), refineMode, verbose);
  T3.stop();
  if (verbose)
    std::cout << "Time refinement: " << T3.get() << "\n";

  std::cout << "Initial dist\n";
  printPartStats(initParts);
  std::cout << "\n";

  std::cout << "Refined dist\n";
  printPartStats(parts);
  std::cout << "\n";
}

typedef katana::FileGraph FG;
typedef FG::GraphNode FN;
template <typename GNode, typename Weights>
struct order_by_degree {
  GGraph& graph;
  Weights& weights;
  order_by_degree(GGraph& g, Weights& w) : graph(g), weights(w) {}
  bool operator()(const GNode& a, const GNode& b) {
    uint64_t wa = weights[a];
    uint64_t wb = weights[b];
    int pa = graph.getData(a, katana::MethodFlag::UNPROTECTED).getPart();
    int pb = graph.getData(b, katana::MethodFlag::UNPROTECTED).getPart();
    if (pa != pb) {
      return pa < pb;
    }
    return wa < wb;
  }
};

typedef katana::PerThreadStorage<std::map<GNode, uint64_t>> PerThreadDegInfo;

int
main(int argc, char** argv) {
  std::unique_ptr<katana::SharedMemSys> G =
      LonestarStart(argc, argv, name, desc, url, &inputFile);

  katana::StatTimer totalTime("TimerTotal");
  totalTime.start();

  srand(-1);
  MetisGraph metisGraph;
  GGraph& graph = *metisGraph.getGraph();

  katana::readGraph(graph, inputFile);

  katana::do_all(
      katana::iterate(graph),
      [&](GNode node) {
        for (auto jj : graph.edges(node)) {
          graph.getEdgeData(jj) = 1;
          // weight+=1;
        }
      },
      katana::loopname("initMorphGraph"));

  graphStat(graph);
  std::cout << "\n";

  katana::Prealloc(katana::numPagePoolAllocTotal() * 5);
  katana::ReportPageAllocGuard page_alloc;

  katana::StatTimer execTime("Timer_0");
  execTime.start();
  Partition(&metisGraph, numPartitions);
  execTime.stop();

  page_alloc.Report();

  std::cout << "Total edge cut: " << computeCut(graph) << "\n";

  if (outputFilename != "") {
    MetisGraph* coarseGraph = &metisGraph;
    while (coarseGraph->getCoarserGraph())
      coarseGraph = coarseGraph->getCoarserGraph();
    std::ofstream outFile(outputFilename.c_str());
    for (auto it = graph.begin(), ie = graph.end(); it != ie; it++) {
      unsigned gPart = graph.getData(*it).getPart();
      outFile << gPart << '\n';
    }
  }

  if (orderedfile != "" || permutationfile != "") {
    katana::FileGraph g;
    g.fromFile(inputFile);
    typedef katana::LargeArray<GNode> Permutation;
    Permutation perm;
    perm.create(g.size());
    std::copy(graph.begin(), graph.end(), perm.begin());
    PerThreadDegInfo threadDegInfo;
    std::vector<int> parts(numPartitions);
    for (unsigned int i = 0; i < parts.size(); i++) {
      parts[i] = i;
    }

    using WL = katana::PerSocketChunkFIFO<16>;

    katana::for_each(
        katana::iterate(parts),
        [&](int part, auto& lwl) {
          constexpr auto flag = katana::MethodFlag::UNPROTECTED;
          typedef std::vector<
              std::pair<unsigned, GNode>,
              katana::PerIterAllocTy::rebind<std::pair<unsigned, GNode>>::other>
              GD;
          // copy and translate all edges
          GD orderedNodes(GD::allocator_type(lwl.getPerIterAlloc()));
          for (auto n : graph) {
            auto& nd = graph.getData(n, flag);
            if (static_cast<int>(nd.getPart()) == part) {
              int edges = std::distance(
                  graph.edge_begin(n, flag), graph.edge_end(n, flag));
              orderedNodes.push_back(std::make_pair(edges, n));
            }
          }
          std::sort(orderedNodes.begin(), orderedNodes.end());
          int index = 0;
          std::map<GNode, uint64_t>& threadMap(*threadDegInfo.getLocal());
          for (auto p : orderedNodes) {
            GNode n = p.second;
            threadMap[n] += index;
            for (auto eb : graph.edges(n, flag)) {
              GNode neigh = graph.getEdgeDst(eb);
              auto& nd = graph.getData(neigh, flag);
              if (static_cast<int>(nd.getPart()) == part) {
                threadMap[neigh] += index;
              }
            }
            index++;
          }
        },
        katana::wl<WL>(), katana::per_iter_alloc(),
        katana::loopname("Order Graph"));

    std::map<GNode, uint64_t> globalMap;
    for (unsigned int i = 0; i < threadDegInfo.size(); i++) {
      std::map<GNode, uint64_t>& localMap(*threadDegInfo.getRemote(i));
      for (auto mb = localMap.begin(), me = localMap.end(); mb != me; mb++) {
        globalMap[mb->first] = mb->second;
      }
    }
    order_by_degree<GNode, std::map<GNode, uint64_t>> fn(graph, globalMap);
    std::map<GNode, int> nodeIdMap;
    int id = 0;
    for (auto nb = graph.begin(), ne = graph.end(); nb != ne; nb++) {
      nodeIdMap[*nb] = id;
      id++;
    }
    // compute inverse
    std::stable_sort(perm.begin(), perm.end(), fn);
    katana::LargeArray<uint64_t> perm2;
    perm2.create(g.size());
    // compute permutation
    id = 0;
    for (auto pb = perm.begin(), pe = perm.end(); pb != pe; pb++) {
      int prevId = nodeIdMap[*pb];
      perm2[prevId] = id;
      id++;
    }
    katana::FileGraph out;
    katana::permute<int>(g, perm2, out);
    if (orderedfile != "")
      out.toFile(orderedfile);
    if (permutationfile != "") {
      std::ofstream file(permutationfile.c_str());
      katana::LargeArray<uint64_t> transpose;
      transpose.create(g.size());
      uint64_t id = 0;
      for (auto& ii : perm2) {
        transpose[ii] = id++;
      }
      for (auto& ii : transpose) {
        file << ii << "\n";
      }
    }
  }

  totalTime.stop();

  return 0;
}
