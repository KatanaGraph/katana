#ifndef KATANA_LIBTSUBA_RDGINSPECTION_H_
#define KATANA_LIBTSUBA_RDGINSPECTION_H_

#include <cstdlib>
#include <iostream>
#include <vector>

#include "katana/BufferedGraph.h"
#include "katana/FileGraph.h"
#include "katana/Galois.h"
#include "katana/LCGraph.h"
#include "katana/OfflineGraph.h"
#include "llvm/Support/CommandLine.h"

namespace tsuba {

namespace cll = llvm::cl;

enum StatMode {
  degreehist,
  degrees,
  maxDegreeNode,
  dsthist,
  indegreehist,
  sortedlogoffsethist,
  sparsityPattern,
  summary
};

typedef katana::OfflineGraph Graph;
typedef Graph::GraphNode GNode;

using Writer = katana::FileGraphWriter;

void
doSummary(Graph& graph) {
  std::cout << "NumNodes: " << graph.size() << "\n";
  std::cout << "NumEdges: " << graph.sizeEdges() << "\n";
  std::cout << "SizeofEdge: " << graph.edgeSize() << "\n";
}

void
doDegrees(Graph& graph) {
  for (auto n : graph) {
    std::cout << graph.edges(n).size() << "\n";
  }
}

void
findMaxDegreeNode(Graph& graph) {
  uint64_t nodeID = 0;
  size_t MaxDegree = 0;
  uint64_t MaxDegreeNode = 0;
  for (auto n : graph) {
    size_t degree = graph.edges(n).size();
    if (MaxDegree < degree) {
      MaxDegree = degree;
      MaxDegreeNode = nodeID;
    }
    ++nodeID;
  }
  std::cout << "MaxDegreeNode : " << MaxDegreeNode
            << " , MaxDegree : " << MaxDegree << "\n";
}

void
printHistogram(
    const std::string& name, std::map<uint64_t, uint64_t>& hists,
    const uint64_t& number_of_bins) {
  auto max = hists.rbegin()->first;
  if (number_of_bins <= 0) {
    std::cout << name << "Bin,Start,End,Count\n";
    for (unsigned x = 0; x <= max; ++x) {
      std::cout << x << ',' << x << ',' << x + 1 << ',';
      if (hists.count(x)) {
        std::cout << hists[x] << '\n';
      } else {
        std::cout << "0\n";
      }
    }
  } else {
    std::vector<uint64_t> bins(number_of_bins);
    auto bwidth = (max + 1) / number_of_bins;
    if ((max + 1) % number_of_bins) {
      ++bwidth;
    }
    // std::cerr << "* " << max << " " << number_of_bins << " " << bwidth << "\n";
    for (auto p : hists) {
      bins.at(p.first / bwidth) += p.second;
    }
    std::cout << name << "Bin,Start,End,Count\n";
    for (unsigned x = 0; x < bins.size(); ++x) {
      std::cout << x << ',' << x * bwidth << ',' << (x * bwidth + bwidth) << ','
                << bins[x] << '\n';
    }
  }
}

void
doSparsityPattern(
    Graph& graph, const int64_t& columns,
    std::function<void(unsigned, unsigned, bool)> printFn) {
  unsigned blockSize = (graph.size() + columns - 1) / columns;

  for (int i = 0; i < columns; ++i) {
    std::vector<bool> row(columns);
    auto p = katana::block_range(graph.begin(), graph.end(), i, columns);
    for (auto ii = p.first, ei = p.second; ii != ei; ++ii) {
      for (auto jj : graph.edges(*ii)) {
        row[graph.getEdgeDst(jj) / blockSize] = true;
      }
    }
    for (int x = 0; x < columns; ++x) {
      printFn(x, i, row[x]);
    }
  }
}

void
doDegreeHistogram(Graph& graph, const uint64_t& numBins) {
  std::map<uint64_t, uint64_t> hist;
  for (auto ii : graph) {
    ++hist[graph.edges(ii).size()];
  }
  printHistogram("Degree", hist, numBins);
}

void
doInDegreeHistogram(Graph& graph, const uint64_t& numBins) {
  std::vector<uint64_t> inv(graph.size());
  std::map<uint64_t, uint64_t> hist;
  for (auto ii : graph) {
    for (auto jj : graph.edges(ii)) {
      ++inv[graph.getEdgeDst(jj)];
    }
  }
  for (uint64_t n : inv) {
    ++hist[n];
  }
  printHistogram("InDegree", hist, numBins);
}

struct EdgeComp {
  typedef katana::EdgeSortValue<GNode, void> Edge;

  bool operator()(const Edge& a, const Edge& b) const { return a.dst < b.dst; }
};

int
getLogIndex(ptrdiff_t x) {
  int logvalue = 0;
  int sign = x < 0 ? -1 : 1;

  if (x < 0) {
    x = -x;
  }

  while ((x >>= 1) != 0) {
    ++logvalue;
  }
  return sign * logvalue;
}

void
doSortedLogOffsetHistogram([[maybe_unused]] Graph& graph) {
  // Graph copy;
  // {
  //   // Original FileGraph is immutable because it is backed by a file
  //   copy = graph;
  // }

  // std::vector<std::map<int, size_t> > hists;
  // hists.emplace_back();
  // auto hist = &hists.back();
  // int curHist = 0;
  // auto p = katana::block_range(
  //     boost::counting_iterator<size_t>(0),
  //     boost::counting_iterator<size_t>(graph.sizeEdges()),
  //     curHist,
  //     numHist);
  // for (auto ii = graph.begin(), ei = graph.end(); ii != ei; ++ii) {
  //   copy.sortEdges<void>(*ii, EdgeComp());

  //   GNode last = 0;
  //   bool first = true;
  //   for (auto jj = copy.edge_begin(*ii), ej = copy.edge_end(*ii); jj != ej;
  //   ++jj) {
  //     GNode dst = copy.getEdgeDst(jj);
  //     ptrdiff_t diff = dst - (ptrdiff_t) last;

  //     if (!first) {
  //       int index = getLogIndex(diff);
  //       ++(*hist)[index];
  //     }
  //     first = false;
  //     last = dst;
  //     if (++p.first == p.second) {
  //       hists.emplace_back();
  //       hist = &hists.back();
  //       curHist += 1;
  //       p = katana::block_range(
  //           boost::counting_iterator<size_t>(0),
  //           boost::counting_iterator<size_t>(graph.sizeEdges()),
  //           curHist,
  //           numHist);
  //     }
  //   }
  // }

  // printHistogram("LogOffset", hists);
}

void
doDestinationHistogram(Graph& graph, const uint64_t& numBins) {
  std::map<uint64_t, uint64_t> hist;
  for (auto ii : graph) {
    for (auto jj : graph.edges(ii)) {
      ++hist[graph.getEdgeDst(jj)];
    }
  }
  printHistogram("DestinationBin", hist, numBins);
}

/**
 * Create node map from file
 */
std::map<uint32_t, uint32_t>
createNodeMap(const std::string& mappingFilename) {
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

}  // namespace tsuba
#endif
