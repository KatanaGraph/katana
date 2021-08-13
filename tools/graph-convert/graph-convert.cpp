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

#include <fcntl.h>

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <fstream>
#include <iostream>
#include <limits>
#include <random>
#include <string>
#include <vector>

#include <boost/mpl/if.hpp>
#include <llvm/Support/CommandLine.h>

#include "katana/ErrorCode.h"
#include "katana/FileGraph.h"
#include "katana/Galois.h"
#include "katana/NUMAArray.h"
#include "katana/Strings.h"
#include "tsuba/CSRTopology.h"
#include "tsuba/Errors.h"
#include "tsuba/file.h"

// TODO: move these enums to a common location for all graph convert tools
enum ConvertMode {
  bipartitegr2bigpetsc,
  bipartitegr2littlepetsc,
  bipartitegr2sorteddegreegr,
  dimacs2gr,
  edgelist2gr,
  csv2gr,
  gr2biggr,
  gr2binarypbbs32,
  gr2binarypbbs64,
  gr2bsml,
  gr2cgr,
  gr2dimacs,
  gr2adjacencylist,
  gr2edgelist,
  gr2edgelist1ind,
  gr2linegr,
  gr2lowdegreegr,
  gr2mtx,
  gr2partdstgr,
  gr2partsrcgr,
  gr2pbbs,
  gr2pbbsedges,
  gr2randgr,
  gr2randomweightgr,
  gr2ringgr,
  gr2rmat,
  gr2metis,
  gr2sgr,
  gr2sorteddegreegr,
  gr2sorteddstgr,
  gr2sortedparentdegreegr,
  gr2sortedweightgr,
  gr2sortedbfsgr,
  gr2streegr,
  gr2tgr,
  gr2treegr,
  gr2trigr,
  gr2totem,
  gr2neo4j,
  gr2kg,
  mtx2gr,
  nodelist2gr,
  pbbs2gr,
  svmlight2gr,
  edgelist2binary
};

enum EdgeType { float32_, float64_, int32_, int64_, uint32_, uint64_, void_ };

namespace cll = llvm::cl;

static std::string kCommandLine;

static cll::opt<std::string> inputFilename(
    cll::Positional, cll::desc("<input file>"), cll::Required);
static cll::opt<std::string> outputFilename(
    cll::Positional, cll::desc("<output file>"), cll::Required);
static cll::opt<std::string> transposeFilename(
    "graphTranspose", cll::desc("transpose graph file"), cll::init(""));
static cll::opt<std::string> outputPermutationFilename(
    "outputNodePermutation", cll::desc("output node permutation file"),
    cll::init(""));
static cll::opt<std::string> labelsFilename(
    "labels", cll::desc("labels file for svmlight2gr"), cll::init(""));
static cll::opt<EdgeType> edgeType(
    "edgeType", cll::desc("Input/Output edge type:"),
    cll::values(
        clEnumValN(
            EdgeType::float32_, "float32", "32 bit floating point edge values"),
        clEnumValN(
            EdgeType::float64_, "float64", "64 bit floating point edge values"),
        clEnumValN(EdgeType::int32_, "int32", "32 bit int edge values"),
        clEnumValN(EdgeType::int64_, "int64", "64 bit int edge values"),
        clEnumValN(
            EdgeType::uint32_, "uint32", "32 bit unsigned int edge values"),
        clEnumValN(
            EdgeType::uint64_, "uint64", "64 bit unsigned int edge values"),
        clEnumValN(EdgeType::void_, "void", "no edge values")),
    cll::init(EdgeType::void_));
static cll::opt<ConvertMode> convertMode(
    cll::desc("Conversion mode:"),
    cll::values(
        clEnumVal(
            bipartitegr2bigpetsc,
            "Convert bipartite binary gr to big-endian PETSc format"),
        clEnumVal(
            bipartitegr2littlepetsc,
            "Convert bipartite binary gr to little-endian PETSc format"),
        clEnumVal(
            bipartitegr2sorteddegreegr,
            "Sort nodes of bipartite binary gr by degree"),
        clEnumVal(dimacs2gr, "Convert dimacs to binary gr"),
        clEnumVal(edgelist2gr, "Convert edge list to binary gr"),
        clEnumVal(csv2gr, "Convert csv to binary gr"),
        clEnumVal(
            gr2biggr,
            "Convert binary gr with little-endian edge data to "
            "big-endian edge data"),
        clEnumVal(
            gr2binarypbbs32,
            "Convert binary gr to unweighted binary pbbs graph"),
        clEnumVal(
            gr2binarypbbs64,
            "Convert binary gr to unweighted binary pbbs graph"),
        clEnumVal(gr2bsml, "Convert binary gr to binary sparse MATLAB matrix"),
        clEnumVal(
            gr2cgr, "Clean up binary gr: remove self edges and multi-edges"),
        clEnumVal(gr2dimacs, "Convert binary gr to dimacs"),
        clEnumVal(gr2adjacencylist, "Convert binary gr to adjacency list"),
        clEnumVal(gr2edgelist, "Convert binary gr to edgelist"),
        clEnumVal(gr2edgelist1ind, "Convert binary gr to edgelist, 1-indexed"),
        clEnumVal(gr2linegr, "Overlay line graph"),
        clEnumVal(gr2lowdegreegr, "Remove high degree nodes from binary gr"),
        clEnumVal(gr2mtx, "Convert binary gr to matrix market format"),
        clEnumVal(
            gr2partdstgr,
            "Partition binary gr in N pieces by destination nodes"),
        clEnumVal(
            gr2partsrcgr, "Partition binary gr in N pieces by source nodes"),
        clEnumVal(gr2pbbs, "Convert binary gr to pbbs graph"),
        clEnumVal(gr2pbbsedges, "Convert binary gr to pbbs edge list"),
        clEnumVal(gr2randgr, "Randomly permute nodes of binary gr"),
        clEnumVal(gr2randomweightgr, "Add or Randomize edge weights"),
        clEnumVal(
            gr2ringgr,
            "Convert binary gr to strongly connected graph by "
            "adding ring overlay"),
        clEnumVal(gr2rmat, "Convert binary gr to RMAT graph"),
        clEnumVal(gr2metis, "Convert binary gr to METIS graph (unweighted)"),
        clEnumVal(
            gr2sgr,
            "Convert binary gr to symmetric graph by adding reverse edges"),
        clEnumVal(gr2sorteddegreegr, "Sort nodes by degree"),
        clEnumVal(
            gr2sorteddstgr,
            "Sort outgoing edges of binary gr by edge destination"),
        clEnumVal(gr2sortedparentdegreegr, "Sort nodes by degree of parent"),
        clEnumVal(
            gr2sortedweightgr,
            "Sort outgoing edges of binary gr by edge weight"),
        clEnumVal(
            gr2sortedbfsgr,
            "Sort nodes by a BFS traversal from the source (greedy)"),
        clEnumVal(
            gr2streegr,
            "Convert binary gr to strongly connected graph "
            "by adding symmetric tree overlay"),
        clEnumVal(gr2tgr, "Transpose binary gr"),
        clEnumVal(gr2treegr, "Overlay tree"),
        clEnumVal(
            gr2trigr,
            "Convert symmetric binary gr to triangular form by "
            "removing reverse edges"),
        clEnumVal(gr2totem, "Convert binary gr totem input format"),
        clEnumVal(gr2neo4j, "Convert binary gr to a vertex/edge csv for neo4j"),
        clEnumVal(
            gr2kg, "Convert binary gr to a property graph for katana graph"),
        clEnumVal(mtx2gr, "Convert matrix market format to binary gr"),
        clEnumVal(nodelist2gr, "Convert node list to binary gr"),
        clEnumVal(pbbs2gr, "Convert pbbs graph to binary gr"),
        clEnumVal(svmlight2gr, "Convert svmlight file to binary gr"),
        clEnumVal(
            edgelist2binary,
            "Convert edge list to binary edgelist "
            "format (assumes vertices of type uin32_t)")),
    cll::Required);
static cll::opt<uint32_t> sourceNode(
    "sourceNode", cll::desc("Source node ID for BFS traversal"), cll::init(0));
static cll::opt<int> numParts(
    "numParts", cll::desc("number of parts to partition graph into"),
    cll::init(64));
static cll::opt<int> maxValue(
    "maxValue",
    cll::desc("maximum weight to add for tree, line, "
              "ring and random weight conversions"),
    cll::init(100));
static cll::opt<int> minValue(
    "minValue",
    cll::desc("minimum weight to add for random weight conversions"),
    cll::init(1));
static cll::opt<size_t> maxDegree(
    "maxDegree", cll::desc("maximum degree to keep"), cll::init(2 * 1024));

struct Conversion {};
struct HasOnlyVoidSpecialization {};
struct HasNoVoidSpecialization {};

template <typename EdgeTy, typename C>
void
convert(C& c, Conversion) {
  c.template convert<EdgeTy>(inputFilename, outputFilename);
}

template <typename EdgeTy, typename C>
void
convert(
    C& c, HasOnlyVoidSpecialization,
    typename std::enable_if<std::is_same<EdgeTy, void>::value>::type* = 0) {
  c.template convert<EdgeTy>(inputFilename, outputFilename);
}

template <typename EdgeTy, typename C>
void
convert(
    C&, HasOnlyVoidSpecialization,
    typename std::enable_if<!std::is_same<EdgeTy, void>::value>::type* = 0) {
  KATANA_DIE("conversion undefined for non-void graphs");
}

template <typename EdgeTy, typename C>
void
convert(
    C& c, HasNoVoidSpecialization,
    typename std::enable_if<!std::is_same<EdgeTy, void>::value>::type* = 0) {
  c.template convert<EdgeTy>(inputFilename, outputFilename);
}

template <typename EdgeTy, typename C>
void
convert(
    C&, HasNoVoidSpecialization,
    typename std::enable_if<std::is_same<EdgeTy, void>::value>::type* = 0) {
  KATANA_DIE("conversion undefined for void graphs");
}

static std::string
edgeTypeToName(EdgeType e) {
  switch (e) {
  case EdgeType::float32_:
    return "float32";
  case EdgeType::float64_:
    return "float64";
  case EdgeType::int32_:
    return "int32";
  case EdgeType::int64_:
    return "int64";
  case EdgeType::uint32_:
    return "uint32";
  case EdgeType::uint64_:
    return "uint64";
  case EdgeType::void_:
    return "void";
  default:
    abort();
  }
}

template <typename C>
void
convert() {
  C c;
  std::cout << "Graph type: " << edgeTypeToName(edgeType) << "\n";
  switch (edgeType) {
  case EdgeType::float32_:
    convert<float>(c, c);
    break;
  case EdgeType::float64_:
    convert<double>(c, c);
    break;
  case EdgeType::int32_:
    convert<int32_t>(c, c);
    break;
  case EdgeType::int64_:
    convert<int64_t>(c, c);
    break;
  case EdgeType::uint32_:
    convert<uint32_t>(c, c);
    break;
  case EdgeType::uint64_:
    convert<uint64_t>(c, c);
    break;
  case EdgeType::void_:
    convert<void>(c, c);
    break;
  default:
    abort();
  };
}

static void
printStatus(size_t inNodes, size_t inEdges, size_t outNodes, size_t outEdges) {
  std::cout << "InGraph : |V| = " << inNodes << ", |E| = " << inEdges << "\n";
  std::cout << "OutGraph: |V| = " << outNodes << ", |E| = " << outEdges << "\n";
}

static void
printStatus(size_t inNodes, size_t inEdges) {
  printStatus(inNodes, inEdges, inNodes, inEdges);
}

template <typename EdgeValues, bool Enable>
void
setEdgeValue(
    EdgeValues& edgeValues, int value,
    typename std::enable_if<Enable>::type* = 0) {
  edgeValues.set(0, static_cast<typename EdgeValues::value_type>(value));
}

template <typename EdgeValues, bool Enable>
void
setEdgeValue(EdgeValues&, int, typename std::enable_if<!Enable>::type* = 0) {}

template <typename EdgeTy, bool Enable>
EdgeTy
getEdgeValue(
    katana::FileGraph& g, katana::FileGraph::edge_iterator ii,
    typename std::enable_if<Enable>::type* = 0) {
  return g.getEdgeData<EdgeTy>(ii);
}

template <typename EdgeTy, bool Enable>
int
getEdgeValue(
    katana::FileGraph&, katana::FileGraph::edge_iterator,
    typename std::enable_if<!Enable>::type* = 0) {
  return 1;
}

template <typename T>
void
outputPermutation(const T& perm) {
  size_t oid = 0;
  std::ofstream out(outputPermutationFilename);
  for (auto ii = perm.begin(), ei = perm.end(); ii != ei; ++ii, ++oid) {
    out << oid << "," << *ii << "\n";
  }
}

void
skipLine(std::ifstream& infile) {
  infile.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
}

/**
 * Common parsing for edgelist style text files.
 *
 * src dst [weight]
 * ...
 *
 * If delim is set, this function expects that each entry is separated by delim
 * surrounded by optional whitespace.
 */
template <typename EdgeTy>
void
convertEdgelist(
    const std::string& infilename, const std::string& outfilename,
    const bool skipFirstLine, std::optional<char> delim) {
  typedef katana::FileGraphWriter Writer;
  typedef katana::NUMAArray<EdgeTy> EdgeData;
  typedef typename EdgeData::value_type edge_value_type;

  Writer p;
  EdgeData edgeData;
  std::ifstream infile(infilename.c_str());

  size_t numNodes = 0;
  size_t numEdges = 0;
  size_t lineNumber = 0;

  if (skipFirstLine) {
    katana::gWarn(
        "first line is assumed to contain labels and will be ignored\n");
    skipLine(infile);
    ++lineNumber;
  }

  const bool hasDelim = static_cast<bool>(delim);
  std::optional<size_t> skippedLine;
  std::string line;
  char readDelim;

  for (; std::getline(infile, line); ++lineNumber) {
    std::stringstream iss(line);

    size_t src;
    if (!(iss >> src)) {
      skippedLine = lineNumber;
      continue;
    }

    if (hasDelim) {
      if (!(iss >> readDelim) || readDelim != delim) {
        skippedLine = lineNumber;
        continue;
      }
    }

    size_t dst;
    if (!(iss >> dst)) {
      skippedLine = lineNumber;
      continue;
    }

    edge_value_type data{};
    if (EdgeData::has_value) {
      if (hasDelim) {
        if (!(iss >> readDelim) || readDelim != delim) {
          skippedLine = lineNumber;
          continue;
        }
      }

      if (!(iss >> data)) {
        skippedLine = lineNumber;
        continue;
      }
    }

    if (infile) {
      ++numEdges;
      if (src > numNodes)
        numNodes = src;
      if (dst > numNodes)
        numNodes = dst;
    }
  }

  if (skippedLine) {
    katana::gWarn(
        "ignored at least one line (line ", *skippedLine,
        ") because it did not match the expected format\n");
  }

  numNodes++;
  p.setNumNodes(numNodes);
  p.setNumEdges(numEdges);
  p.setSizeofEdgeData(EdgeData::size_of::value);
  edgeData.create(numEdges);

  infile.clear();
  infile.seekg(0, std::ios::beg);
  p.phase1();

  if (skipFirstLine) {
    skipLine(infile);
  }

  while (std::getline(infile, line)) {
    std::stringstream iss(line);

    size_t src;
    if (!(iss >> src)) {
      continue;
    }

    if (hasDelim) {
      if (!(iss >> readDelim) || readDelim != delim) {
        continue;
      }
    }

    size_t dst;
    if (!(iss >> dst)) {
      continue;
    }

    edge_value_type data{};
    if (EdgeData::has_value) {
      if (hasDelim) {
        if (!(iss >> readDelim) || readDelim != delim) {
          continue;
        }
      }

      if (!(iss >> data)) {
        continue;
      }
    }

    if (infile) {
      p.incrementDegree(src);
    }
  }

  infile.clear();
  infile.seekg(0, std::ios::beg);
  p.phase2();

  if (skipFirstLine) {
    skipLine(infile);
  }

  while (std::getline(infile, line)) {
    std::stringstream iss(line);

    size_t src;
    if (!(iss >> src)) {
      continue;
    }

    if (hasDelim) {
      if (!(iss >> readDelim) || readDelim != delim) {
        continue;
      }
    }

    size_t dst;
    if (!(iss >> dst)) {
      continue;
    }

    edge_value_type data{};
    if (EdgeData::has_value) {
      if (hasDelim) {
        if (!(iss >> readDelim) || readDelim != delim) {
          continue;
        }
      }

      if (!(iss >> data)) {
        continue;
      }
    }

    if (infile) {
      edgeData.set(p.addNeighbor(src, dst), data);
    }
  }

  edge_value_type* rawEdgeData = p.finish<edge_value_type>();
  if (EdgeData::has_value)
    std::uninitialized_copy(
        std::make_move_iterator(edgeData.begin()),
        std::make_move_iterator(edgeData.end()), rawEdgeData);

  p.toFile(outfilename);
  printStatus(numNodes, numEdges);
}

template <typename EdgeTy>
void
convertEdgelist(
    const std::string& infilename, const std::string& outfilename,
    const bool skipFirstLine) {
  convertEdgelist<EdgeTy>(
      infilename, outfilename, skipFirstLine, std::optional<char>());
}

/**
 * Assumption: First line has labels
 * Just a bunch of pairs or triples:
 * src dst weight?
 */
struct CSV2Gr : public Conversion {
  template <typename EdgeTy>
  void convert(const std::string& infilename, const std::string& outfilename) {
    convertEdgelist<EdgeTy>(infilename, outfilename, true, ',');
  }
};

/**
 * Just a bunch of pairs or triples:
 * src dst weight?
 */
struct Edgelist2Gr : public Conversion {
  template <typename EdgeTy>
  void convert(const std::string& infilename, const std::string& outfilename) {
    convertEdgelist<EdgeTy>(infilename, outfilename, false);
  }
};

/**
 * Convert edgelist to binary edgelist format
 * Assumes no edge data.
 */
struct Edgelist2Binary : public Conversion {
  template <typename EdgeTy>
  void convert(const std::string& infilename, const std::string& outfilename) {
    std::ifstream infile(infilename.c_str());
    std::ofstream outfile(outfilename.c_str());

    size_t numNodes = 0;
    size_t numEdges = 0;

    std::vector<uint32_t> buffer(10000);
    uint32_t counter = 0;
    bool skippedLine = false;
    while (infile) {
      uint32_t src;
      if (!(infile >> src)) {
        skipLine(infile);
        skippedLine = true;
        continue;
      }

      uint32_t dst;
      if (!(infile >> dst)) {
        skipLine(infile);
        skippedLine = true;
        continue;
      }

      buffer[counter++] = src;
      buffer[counter++] = dst;
      if (counter == buffer.size()) {
        // flush it to the output file.
        outfile.write(
            reinterpret_cast<char*>(&buffer[0]), sizeof(uint32_t) * counter);
        counter = 0;
      }

      if (infile) {
        ++numEdges;
        if (src > numNodes)
          numNodes = src;
        if (dst > numNodes)
          numNodes = dst;
      } else {
        counter -= 2;
      }
    }

    if (counter) {
      // flush it to the output file.
      outfile.write(
          reinterpret_cast<char*>(&buffer[0]), sizeof(uint32_t) * counter);
    }

    if (skippedLine) {
      katana::gWarn(
          "ignored at least one line because it did not match the "
          "expected format\n");
    }

    printStatus(numNodes, numEdges);
  }
};

/**
 * Convert matrix market matrix to binary graph.
 *
 * %% comments
 * % ...
 * <num nodes> <num nodes> <num edges>
 * <src> <dst> <float>
 *
 * src and dst start at 1.
 */
struct Mtx2Gr : public HasNoVoidSpecialization {
  template <typename EdgeTy>
  void convert(const std::string& infilename, const std::string& outfilename) {
    typedef katana::FileGraphWriter Writer;
    typedef katana::NUMAArray<EdgeTy> EdgeData;
    typedef typename EdgeData::value_type edge_value_type;

    Writer p;
    EdgeData edgeData;
    uint32_t nnodes;
    size_t nedges;

    for (int phase = 0; phase < 2; ++phase) {
      std::ifstream infile(infilename.c_str());
      if (!infile) {
        KATANA_DIE("failed to open input file");
      }

      // Skip comments
      while (infile) {
        if (infile.peek() != '%') {
          break;
        }
        skipLine(infile);
      }

      // Read header
      char header[256];
      infile.getline(header, 256);
      std::istringstream line(header, std::istringstream::in);
      std::vector<std::string> tokens;
      while (line) {
        std::string tmp;
        line >> tmp;
        if (line) {
          tokens.push_back(tmp);
        }
      }
      if (tokens.size() != 3) {
        KATANA_DIE("unknown problem specification line: ", line.str());
      }
      // Prefer C functions for maximum compatibility
      // nnodes = std::stoull(tokens[0]);
      // nedges = std::stoull(tokens[2]);
      nnodes = strtoull(tokens[0].c_str(), NULL, 0);
      nedges = strtoull(tokens[2].c_str(), NULL, 0);

      // Parse edges
      if (phase == 0) {
        p.setNumNodes(nnodes);
        p.setNumEdges(nedges);
        p.setSizeofEdgeData(EdgeData::size_of::value);
        edgeData.create(nedges);
        p.phase1();
      } else {
        p.phase2();
      }

      for (size_t edge_num = 0; edge_num < nedges; ++edge_num) {
        if ((edge_num % (nedges / 500)) == 0) {
          printf(
              "Phase %d: current edge progress %lf%%\n", phase,
              ((double)edge_num / nedges) * 100);
        }
        uint32_t cur_id, neighbor_id;
        double weight = 1;

        infile >> cur_id >> neighbor_id >> weight;
        if (cur_id == 0 || cur_id > nnodes) {
          KATANA_DIE("node id out of range: ", cur_id);
        }
        if (neighbor_id == 0 || neighbor_id > nnodes) {
          KATANA_DIE("neighbor id out of range: ", neighbor_id);
        }

        // 1 indexed
        if (phase == 0) {
          p.incrementDegree(cur_id - 1);
        } else {
          edgeData.set(
              p.addNeighbor(cur_id - 1, neighbor_id - 1),
              static_cast<edge_value_type>(weight));
        }

        skipLine(infile);
      }

      infile.peek();
      if (!infile.eof()) {
        KATANA_DIE("additional lines in file");
      }
    }
    // this is for the progress print

    edge_value_type* rawEdgeData = p.finish<edge_value_type>();
    if (EdgeData::has_value)
      std::uninitialized_copy(
          std::make_move_iterator(edgeData.begin()),
          std::make_move_iterator(edgeData.end()), rawEdgeData);

    p.toFile(outfilename);
    printStatus(p.size(), p.sizeEdges());
  }
};

struct Gr2Mtx : public HasNoVoidSpecialization {
  template <typename EdgeTy>
  void convert(const std::string& infilename, const std::string& outfilename) {
    typedef katana::FileGraph Graph;
    typedef Graph::GraphNode GNode;

    Graph graph;
    graph.fromFile(infilename);

    std::ofstream file(outfilename.c_str());
    file << graph.size() << " " << graph.size() << " " << graph.sizeEdges()
         << "\n";
    for (Graph::iterator ii = graph.begin(), ei = graph.end(); ii != ei; ++ii) {
      GNode src = *ii;
      for (Graph::edge_iterator jj = graph.edge_begin(src),
                                ej = graph.edge_end(src);
           jj != ej; ++jj) {
        GNode dst = graph.getEdgeDst(jj);
        double v = static_cast<double>(graph.getEdgeData<EdgeTy>(jj));
        file << src + 1 << " " << dst + 1 << " " << v << "\n";
      }
    }
    file.close();

    printStatus(graph.size(), graph.sizeEdges());
  }
};

/**
 * List of node adjacencies:
 *
 * <node id> <num neighbors> <neighbor id>*
 * ...
 */
struct Nodelist2Gr : public HasOnlyVoidSpecialization {
  template <typename EdgeTy>
  void convert(const std::string& infilename, const std::string& outfilename) {
    static_assert(
        std::is_same<EdgeTy, void>::value,
        "conversion undefined for non-void graphs");
    typedef katana::FileGraphWriter Writer;

    Writer p;
    std::ifstream infile(infilename.c_str());

    size_t numNodes = 0;
    size_t numEdges = 0;

    while (infile) {
      size_t src;
      size_t numNeighbors;

      infile >> src >> numNeighbors;

      if (infile) {
        if (src > numNodes)
          numNodes = src;
        numEdges += numNeighbors;
      }
      skipLine(infile);
    }

    numNodes++;
    p.setNumNodes(numNodes);
    p.setNumEdges(numEdges);

    infile.clear();
    infile.seekg(0, std::ios::beg);
    p.phase1();
    while (infile) {
      size_t src;
      size_t numNeighbors;

      infile >> src >> numNeighbors;

      if (infile) {
        p.incrementDegree(src, numNeighbors);
      }
      skipLine(infile);
    }

    infile.clear();
    infile.seekg(0, std::ios::beg);
    p.phase2();
    while (infile) {
      size_t src;
      size_t numNeighbors;

      infile >> src >> numNeighbors;

      for (; infile && numNeighbors > 0; --numNeighbors) {
        size_t dst;
        infile >> dst;
        if (infile)
          p.addNeighbor(src, dst);
      }

      skipLine(infile);
    }

    p.finish<void>();

    p.toFile(outfilename);
    printStatus(numNodes, numEdges);
  }
};

struct Gr2Adjacencylist : public Conversion {
  template <typename EdgeTy>
  void convert(const std::string& infilename, const std::string& outfilename) {
    typedef katana::FileGraph Graph;
    typedef Graph::GraphNode GNode;

    Graph graph;
    graph.fromFile(infilename);

    std::ofstream file(outfilename.c_str());
    for (Graph::iterator ii = graph.begin(), ei = graph.end(); ii != ei; ++ii) {
      GNode src = *ii;
      file << src;
      for (Graph::edge_iterator jj = graph.edge_begin(src),
                                ej = graph.edge_end(src);
           jj != ej; ++jj) {
        GNode dst = graph.getEdgeDst(jj);
        file << " " << dst;
      }
      file << "\n";
    }
    file.close();

    printStatus(graph.size(), graph.sizeEdges());
  }
};

struct Gr2Edgelist : public Conversion {
  template <typename EdgeTy>
  void convert(const std::string& infilename, const std::string& outfilename) {
    typedef katana::FileGraph Graph;
    typedef Graph::GraphNode GNode;
    typedef katana::NUMAArray<EdgeTy> EdgeData;
    typedef typename EdgeData::value_type edge_value_type;

    Graph graph;
    graph.fromFile(infilename);

    std::ofstream file(outfilename.c_str());
    for (Graph::iterator ii = graph.begin(), ei = graph.end(); ii != ei; ++ii) {
      GNode src = *ii;
      for (Graph::edge_iterator jj = graph.edge_begin(src),
                                ej = graph.edge_end(src);
           jj != ej; ++jj) {
        GNode dst = graph.getEdgeDst(jj);
        if (EdgeData::has_value) {
          file << src << " " << dst << " "
               << graph.getEdgeData<edge_value_type>(jj) << "\n";
        } else {
          file << src << " " << dst << "\n";
        }
      }
    }
    file.close();

    printStatus(graph.size(), graph.sizeEdges());
  }
};

/**
 * Edge list conversion from gr except all ids are incremented by 1 (i.e.
 * 1 indexing).
 */
struct Gr2Edgelist1Ind : public Conversion {
  template <typename EdgeTy>
  void convert(const std::string& infilename, const std::string& outfilename) {
    using Graph = katana::FileGraph;
    using GNode = Graph::GraphNode;
    using EdgeData = katana::NUMAArray<EdgeTy>;
    using edge_value_type = typename EdgeData::value_type;

    Graph graph;
    graph.fromFile(infilename);

    std::ofstream file(outfilename.c_str());
    for (Graph::iterator ii = graph.begin(), ei = graph.end(); ii != ei; ++ii) {
      GNode src = *ii;
      for (Graph::edge_iterator jj = graph.edge_begin(src),
                                ej = graph.edge_end(src);
           jj != ej; ++jj) {
        GNode dst = graph.getEdgeDst(jj);
        if (EdgeData::has_value) {
          file << (src + 1) << " " << (dst + 1) << " "
               << graph.getEdgeData<edge_value_type>(jj) << "\n";
        } else {
          file << (src + 1) << " " << (dst + 1) << "\n";
        }
      }
    }
    file.close();

    printStatus(graph.size(), graph.sizeEdges());
  }
};

template <bool LittleEndian, typename T>
void
writeEndian(T* out, T value) {
  static_assert(sizeof(T) == 4 || sizeof(T) == 8, "unknown data size");
  switch ((sizeof(T) == 4 ? 0 : 2) + (LittleEndian ? 0 : 1)) {
  case 3:
    value = katana::convert_htobe64(value);
    break;
  case 2:
    value = katana::convert_htole64(value);
    break;
  case 1:
    value = katana::convert_htobe32(value);
    break;
  case 0:
    value = katana::convert_htole32(value);
    break;
  default:
    abort();
  }

  *out = value;
}

template <bool LittleEndian, typename T>
void
writeEndian(std::ofstream& out, T value) {
  static_assert(sizeof(T) == 4 || sizeof(T) == 8, "unknown data size");
  switch ((sizeof(T) == 4 ? 0 : 2) + (LittleEndian ? 0 : 1)) {
  case 3:
    value = katana::convert_htobe64(value);
    break;
  case 2:
    value = katana::convert_htole64(value);
    break;
  case 1:
    value = katana::convert_htobe32(value);
    break;
  case 0:
    value = katana::convert_htole32(value);
    break;
  default:
    abort();
  }

  out.write(reinterpret_cast<char*>(&value), sizeof(value));
}

template <typename OutEdgeTy, bool LittleEndian>
struct Bipartitegr2Petsc : public HasNoVoidSpecialization {
  template <typename InEdgeTy>
  void convert(const std::string& infilename, const std::string& outfilename) {
    typedef katana::FileGraph Graph;
    typedef Graph::GraphNode GNode;

    Graph graph;
    graph.fromFile(infilename);

    size_t partition = 0;
    for (Graph::iterator ii = graph.begin(), ei = graph.end(); ii != ei;
         ++ii, ++partition) {
      GNode src = *ii;
      if (graph.edge_begin(src) == graph.edge_end(src)) {
        break;
      }
    }

    std::ofstream file(outfilename.c_str());
    writeEndian<LittleEndian, int32_t>(file, 1211216);
    writeEndian<LittleEndian, int32_t>(file, partition);  // rows
    writeEndian<LittleEndian, int32_t>(
        file,
        graph.size() - partition);  // columns
    writeEndian<LittleEndian, int32_t>(file, graph.sizeEdges());

    // number of nonzeros in each row
    for (Graph::iterator ii = graph.begin(), ei = ii + partition; ii != ei;
         ++ii) {
      GNode src = *ii;
      writeEndian<LittleEndian, int32_t>(file, graph.edges(src).size());
    }

    // column indices
    for (Graph::iterator ii = graph.begin(), ei = ii + partition; ii != ei;
         ++ii) {
      GNode src = *ii;
      for (Graph::edge_iterator jj = graph.edge_begin(src),
                                ej = graph.edge_end(src);
           jj != ej; ++jj) {
        GNode dst = graph.getEdgeDst(jj);
        writeEndian<LittleEndian, int32_t>(file, dst - partition);
      }
    }

    // values
    for (Graph::iterator ii = graph.begin(), ei = ii + partition; ii != ei;
         ++ii) {
      GNode src = *ii;
      for (Graph::edge_iterator jj = graph.edge_begin(src),
                                ej = graph.edge_end(src);
           jj != ej; ++jj) {
        writeEndian<LittleEndian, OutEdgeTy>(
            file, graph.getEdgeData<InEdgeTy>(jj));
      }
    }
    file.close();

    printStatus(graph.size(), graph.sizeEdges());
  }
};

//! Wrap generator into form form std::random_shuffle
template <typename T, typename Gen, template <typename> class Dist>
struct UniformDist {
  Gen& gen;

  UniformDist(Gen& g) : gen(g) {}
  T operator()(T m) {
    Dist<T> r(0, m - 1);
    return r(gen);
  }
};

struct RandomizeNodes : public Conversion {
  template <typename EdgeTy>
  void convert(const std::string& infilename, const std::string& outfilename) {
    typedef katana::FileGraph Graph;
    typedef Graph::GraphNode GNode;
    typedef katana::NUMAArray<GNode> Permutation;

    Graph graph;
    graph.fromFile(infilename);

    Permutation perm;
    perm.create(graph.size());
    std::copy(
        boost::counting_iterator<GNode>(0),
        boost::counting_iterator<GNode>(graph.size()), perm.begin());
    std::random_device rng;
    std::mt19937 urng(rng());
    std::shuffle(perm.begin(), perm.end(), urng);

    Graph out;
    katana::permute<EdgeTy>(graph, perm, out);
    outputPermutation(perm);

    out.toFile(outfilename);
    printStatus(out.size(), out.sizeEdges());
  }
};

struct SortByBFS : public Conversion {
  template <typename EdgeTy>
  void convert(const std::string& infilename, const std::string& outfilename) {
    typedef katana::FileGraph Graph;
    typedef Graph::GraphNode GNode;
    typedef katana::NUMAArray<GNode> Permutation;

    Graph graph;
    graph.fromFile(infilename);

    Permutation perm;
    perm.create(graph.size());
    GNode perm_index = 0;

    // perform a BFS traversal
    std::vector<GNode> curr, next;
    katana::NUMAArray<bool> visited;
    visited.create(graph.size());
    for (Graph::iterator ii = graph.begin(), ei = graph.end(); ii != ei; ++ii) {
      GNode node = *ii;
      visited[node] = false;
    }
    GNode src = sourceNode;
    visited[src] = true;
    next.push_back(src);
    while (!next.empty()) {
      size_t wl_size = next.size();
      curr.resize(wl_size);
      std::copy(next.begin(), next.end(), curr.begin());
      next.clear();
      for (size_t i = 0; i < wl_size; ++i) {
        GNode node = curr[i];
        perm[node] = perm_index++;
        for (Graph::edge_iterator jj = graph.edge_begin(node),
                                  ej = graph.edge_end(node);
             jj != ej; ++jj) {
          GNode dst = graph.getEdgeDst(jj);
          if (visited[dst] == false) {
            visited[dst] = true;
            next.push_back(dst);
          }
        }
      }
    }
    for (Graph::iterator ii = graph.begin(), ei = graph.end(); ii != ei; ++ii) {
      GNode node = *ii;
      if (visited[node] == false) {
        perm[node] = perm_index++;
      }
    }
    KATANA_LOG_DEBUG_ASSERT(perm_index == graph.size());

    Graph out;
    katana::permute<EdgeTy>(graph, perm, out);
    outputPermutation(perm);

    out.toFile(outfilename);
    printStatus(out.size(), out.sizeEdges());
  }
};

template <typename T, bool IsInteger = std::numeric_limits<T>::is_integer>
struct UniformDistribution {};

template <typename T>
struct UniformDistribution<T, true> {
  std::uniform_int_distribution<T> dist;

  UniformDistribution(int a, int b) : dist(a, b) {}
  template <typename Gen>
  T operator()(Gen& g) {
    return dist(g);
  }
};

template <typename T>
struct UniformDistribution<T, false> {
  std::uniform_real_distribution<T> dist;

  UniformDistribution(int a, int b) : dist(a, b) {}
  template <typename Gen>
  T operator()(Gen& g) {
    return dist(g);
  }
};

struct RandomizeEdgeWeights : public HasNoVoidSpecialization {
  template <typename OutEdgeTy>
  void convert(const std::string& infilename, const std::string& outfilename) {
    typedef katana::FileGraph Graph;

    Graph graph;
    Graph outgraph;

    graph.fromFile(infilename);
    OutEdgeTy* edgeData = outgraph.fromGraph<OutEdgeTy>(graph);
    OutEdgeTy* edgeDataEnd = edgeData + graph.sizeEdges();

    std::mt19937 gen;
    UniformDistribution<OutEdgeTy> dist(minValue, maxValue);
    for (; edgeData != edgeDataEnd; ++edgeData) {
      *edgeData = dist(gen);
    }

    outgraph.toFile(outfilename);
    printStatus(
        graph.size(), graph.sizeEdges(), outgraph.size(), outgraph.sizeEdges());
  }
};

/**
 * Add edges (i, i-1) for all i \in V.
 */
template <bool AddLine>
struct AddRing : public Conversion {
  template <typename EdgeTy>
  void convert(const std::string& infilename, const std::string& outfilename) {
    typedef katana::FileGraph Graph;
    typedef katana::FileGraphWriter Writer;
    typedef typename Graph::GraphNode GNode;
    typedef katana::NUMAArray<EdgeTy> EdgeData;
    typedef typename EdgeData::value_type edge_value_type;

    Graph graph;
    graph.fromFile(infilename);

    Writer p;
    EdgeData edgeData;
    EdgeData edgeValue;

    uint64_t size = graph.size();
    uint64_t newEdges = AddLine ? size - 1 : size;
    p.setNumNodes(size);
    p.setNumEdges(graph.sizeEdges() + newEdges);
    p.setSizeofEdgeData(EdgeData::size_of::value);
    edgeData.create(graph.sizeEdges() + newEdges);
    edgeValue.create(1);
    setEdgeValue<EdgeData, EdgeData::has_value>(edgeValue, maxValue);

    p.phase1();
    for (Graph::iterator ii = graph.begin(), ei = graph.end(); ii != ei; ++ii) {
      GNode src = *ii;
      auto d = graph.edges(src).size();
      if (AddLine && src == 0)
        p.incrementDegree(src, d);
      else
        p.incrementDegree(src, d + 1);
    }

    p.phase2();
    for (Graph::iterator ii = graph.begin(), ei = graph.end(); ii != ei; ++ii) {
      GNode src = *ii;

      for (Graph::edge_iterator jj = graph.edge_begin(src),
                                ej = graph.edge_end(src);
           jj != ej; ++jj) {
        GNode dst = graph.getEdgeDst(jj);
        if (EdgeData::has_value) {
          edgeData.set(
              p.addNeighbor(src, dst), graph.getEdgeData<edge_value_type>(jj));
        } else {
          p.addNeighbor(src, dst);
        }
      }

      if (AddLine && src == 0)
        continue;

      GNode dst = src == 0 ? size - 1 : src - 1;
      if (EdgeData::has_value) {
        edgeData.set(p.addNeighbor(src, dst), edgeValue.at(0));
      } else {
        p.addNeighbor(src, dst);
      }
    }

    edge_value_type* rawEdgeData = p.finish<edge_value_type>();
    if (EdgeData::has_value)
      std::uninitialized_copy(
          std::make_move_iterator(edgeData.begin()),
          std::make_move_iterator(edgeData.end()), rawEdgeData);
    p.toFile(outfilename);
    printStatus(graph.size(), graph.sizeEdges(), p.size(), p.sizeEdges());
  }
};

/**
 * Add edges (i, i*2+1), (i, i*2+2) and their complement.
 */
template <bool AddComplement>
struct AddTree : public Conversion {
  template <typename EdgeTy>
  void convert(const std::string& infilename, const std::string& outfilename) {
    typedef katana::FileGraph Graph;
    typedef katana::FileGraphWriter Writer;
    typedef Graph::GraphNode GNode;
    typedef katana::NUMAArray<EdgeTy> EdgeData;
    typedef typename EdgeData::value_type edge_value_type;

    Graph graph;
    graph.fromFile(infilename);

    Writer p;
    EdgeData edgeData;
    EdgeData edgeValue;

    uint64_t size = graph.size();
    uint64_t newEdges = 0;
    if (size >= 2) {
      // Closed form counts for the loop below
      newEdges = (size - 1 + (2 - 1)) / 2;   // (1) rounded up
      newEdges += (size - 2 + (2 - 1)) / 2;  // (2) rounded up
    } else if (size >= 1)
      newEdges = 1;
    if (AddComplement)
      newEdges *= 2;  // reverse edges

    p.setNumNodes(size);
    p.setNumEdges(graph.sizeEdges() + newEdges);
    p.setSizeofEdgeData(EdgeData::size_of::value);
    edgeData.create(graph.sizeEdges() + newEdges);
    edgeValue.create(1);
    // edgeValue.set(0, maxValue + 1);
    setEdgeValue<EdgeData, EdgeData::has_value>(edgeValue, maxValue);

    p.phase1();
    for (Graph::iterator ii = graph.begin(), ei = graph.end(); ii != ei; ++ii) {
      GNode src = *ii;
      p.incrementDegree(src, graph.edges(src).size());
      if (src * 2 + 1 < size) {  // (1)
        p.incrementDegree(src);
        if (AddComplement)
          p.incrementDegree(src * 2 + 1);
      }
      if (src * 2 + 2 < size) {  // (2)
        p.incrementDegree(src);
        if (AddComplement)
          p.incrementDegree(src * 2 + 2);
      }
    }

    p.phase2();
    for (Graph::iterator ii = graph.begin(), ei = graph.end(); ii != ei; ++ii) {
      GNode src = *ii;

      for (Graph::edge_iterator jj = graph.edge_begin(src),
                                ej = graph.edge_end(src);
           jj != ej; ++jj) {
        GNode dst = graph.getEdgeDst(jj);
        if (EdgeData::has_value) {
          edgeData.set(
              p.addNeighbor(src, dst), graph.getEdgeData<edge_value_type>(jj));
        } else {
          p.addNeighbor(src, dst);
        }
      }
      if (src * 2 + 1 < size) {
        if (EdgeData::has_value) {
          edgeData.set(p.addNeighbor(src, src * 2 + 1), edgeValue.at(0));
          if (AddComplement)
            edgeData.set(p.addNeighbor(src * 2 + 1, src), edgeValue.at(0));
        } else {
          p.addNeighbor(src, src * 2 + 1);
          if (AddComplement)
            p.addNeighbor(src * 2 + 1, src);
        }
      }
      if (src * 2 + 2 < size) {
        if (EdgeData::has_value) {
          edgeData.set(p.addNeighbor(src, src * 2 + 2), edgeValue.at(0));
          if (AddComplement)
            edgeData.set(p.addNeighbor(src * 2 + 2, src), edgeValue.at(0));
        } else {
          p.addNeighbor(src, src * 2 + 2);
          if (AddComplement)
            p.addNeighbor(src * 2 + 2, src);
        }
      }
    }

    edge_value_type* rawEdgeData = p.finish<edge_value_type>();
    if (EdgeData::has_value)
      std::uninitialized_copy(
          std::make_move_iterator(edgeData.begin()),
          std::make_move_iterator(edgeData.end()), rawEdgeData);
    p.toFile(outfilename);
    printStatus(graph.size(), graph.sizeEdges(), p.size(), p.sizeEdges());
  }
};

//! Make graph symmetric by blindly adding reverse entries
struct MakeSymmetric : public Conversion {
  template <typename EdgeTy>
  void convert(const std::string& infilename, const std::string& outfilename) {
    typedef katana::FileGraph Graph;

    Graph ingraph;
    Graph outgraph;
    ingraph.fromFile(infilename);
    katana::makeSymmetric<EdgeTy>(ingraph, outgraph);

    outgraph.toFile(outfilename);
    printStatus(
        ingraph.size(), ingraph.sizeEdges(), outgraph.size(),
        outgraph.sizeEdges());
  }
};

/**
 * Like SortByDegree but (1) take into account bipartite representation splits
 * symmetric relation over two graphs (a graph and its transpose) and (2)
 * normalize representation by placing all nodes from bipartite graph set A
 * before set B.
 */
struct BipartiteSortByDegree : public Conversion {
  template <typename EdgeTy>
  void convert(const std::string& infilename, const std::string& outfilename) {
    typedef katana::FileGraph Graph;
    typedef Graph::GraphNode GNode;
    typedef katana::NUMAArray<GNode> Permutation;

    Graph ingraph, outgraph, transposegraph;
    ingraph.fromFile(infilename);
    transposegraph.fromFile(transposeFilename);

    Permutation perm;
    perm.create(ingraph.size());

    auto hasOutEdge = [&](GNode x) {
      return ingraph.edge_begin(x) != ingraph.edge_end(x);
    };
    ptrdiff_t numSetA =
        std::count_if(ingraph.begin(), ingraph.end(), hasOutEdge);
    auto getDistance = [&](GNode x) -> ptrdiff_t {
      if (ingraph.edge_begin(x) == ingraph.edge_end(x))
        return numSetA +
               std::distance(
                   transposegraph.edge_begin(x), transposegraph.edge_end(x));
      else
        return ingraph.edges(x).size();
    };

    std::copy(ingraph.begin(), ingraph.end(), perm.begin());
    std::sort(perm.begin(), perm.end(), [&](GNode lhs, GNode rhs) -> bool {
      return getDistance(lhs) < getDistance(rhs);
    });

    // Finalize by taking the transpose/inverse
    Permutation inverse;
    inverse.create(ingraph.size());
    size_t idx = 0;
    for (auto n : perm) {
      inverse[n] = idx++;
    }

    katana::permute<EdgeTy>(ingraph, inverse, outgraph);
    outputPermutation(inverse);
    outgraph.toFile(outfilename);
    printStatus(ingraph.size(), ingraph.sizeEdges());
  }
};

struct SortByDegree : public Conversion {
  template <typename EdgeTy>
  void convert(const std::string& infilename, const std::string& outfilename) {
    typedef katana::FileGraph Graph;
    typedef Graph::GraphNode GNode;
    typedef katana::NUMAArray<GNode> Permutation;

    Graph ingraph, outgraph;
    ingraph.fromFile(infilename);

    Permutation perm;
    perm.create(ingraph.size());

    std::copy(ingraph.begin(), ingraph.end(), perm.begin());
    std::sort(perm.begin(), perm.end(), [&](GNode lhs, GNode rhs) -> bool {
      return ingraph.edges(lhs).size() < ingraph.edges(rhs).size();
    });

    // Finalize by taking the transpose/inverse
    Permutation inverse;
    inverse.create(ingraph.size());
    size_t idx = 0;
    for (auto n : perm) {
      inverse[n] = idx++;
    }

    katana::permute<EdgeTy>(ingraph, inverse, outgraph);
    outputPermutation(inverse);
    outgraph.toFile(outfilename);
    printStatus(ingraph.size(), ingraph.sizeEdges());
  }
};

struct ToBigEndian : public HasNoVoidSpecialization {
  template <typename EdgeTy>
  void convert(const std::string& infilename, const std::string& outfilename) {
    typedef katana::FileGraph Graph;

    Graph ingraph, outgraph;
    ingraph.fromFile(infilename);
    EdgeTy* out = outgraph.fromGraph<EdgeTy>(ingraph);

    for (auto ii = ingraph.edge_data_begin<EdgeTy>(),
              ei = ingraph.edge_data_end<EdgeTy>();
         ii != ei; ++ii, ++out) {
      writeEndian<false>(out, *ii);
    }
    outgraph.toFile(outfilename);
    printStatus(ingraph.size(), ingraph.sizeEdges());
  }
};

struct SortByHighDegreeParent : public Conversion {
  template <typename EdgeTy>
  void convert(const std::string& infilename, const std::string& outfilename) {
    typedef katana::FileGraph Graph;
    typedef Graph::GraphNode GNode;
    typedef katana::NUMAArray<GNode> Permutation;

    Graph graph;
    // get file graph
    graph.fromFile(infilename);

    // get the number of vertices
    auto sz = graph.size();

    Permutation perm;
    perm.create(sz);
    // fill the perm array with 0 through # vertices
    std::copy(
        boost::counting_iterator<GNode>(0), boost::counting_iterator<GNode>(sz),
        perm.begin());

    std::cout << "Done setting up perm\n";

    std::deque<std::deque<std::pair<unsigned, GNode>>> inv(sz);
    unsigned count = 0;

    // loop through all vertices
    for (auto ii = graph.begin(), ee = graph.end(); ii != ee; ++ii) {
      // progress indicator print
      if (!(++count % 1024))
        std::cerr << static_cast<double>(count * 100) / sz << "\r";

      // get the number of edges this vertex has
      unsigned dist = graph.edges(*ii).size();

      // for each edge, get destination, and on that destination vertex save
      // the source id (i.e. this is a transpose)
      for (auto dsti = graph.edge_begin(*ii), dste = graph.edge_end(*ii);
           dsti != dste; ++dsti)
        inv[graph.getEdgeDst(dsti)].push_back(std::make_pair(dist, *ii));
    }

    std::cout << "Found inverse\n";

    count = 0;
    // looping through deques with incoming edges
    // TODO this can probably be parallelized since each deque is disjoint
    for (auto ii = inv.begin(), ee = inv.end(); ii != ee; ++ii) {
      // progress tracker
      if (!(++count % 1024)) {
        std::cerr << count << " of " << sz << "\r";
      }

      // sort each deque
      std::sort(
          ii->begin(), ii->end(), std::greater<std::pair<unsigned, GNode>>());
    }

    std::cout << "Beginning perm sort\n";

    // sort the 0 -> # vertices array
    std::sort(perm.begin(), perm.end(), [&inv](GNode lhs, GNode rhs) -> bool {
      const auto& leftBegin = inv[lhs].begin();
      const auto& leftEnd = inv[lhs].end();
      const auto& rightBegin = inv[rhs].begin();
      const auto& rightEnd = inv[rhs].end();
      // not less-than and not equal => greater-than
      return (
          !std::lexicographical_compare(
              leftBegin, leftEnd, rightBegin, rightEnd) &&
          !(std::distance(leftBegin, leftEnd) ==
                std::distance(rightBegin, rightEnd) &&
            std::equal(leftBegin, leftEnd, rightBegin)));
    });

    std::cout << "Done sorting\n";

    Permutation perm2;
    perm2.create(sz);
    // perm2 stores the new ordering of a particular vertex
    for (unsigned x = 0; x < perm.size(); ++x)
      perm2[perm[x]] = x;

    std::cout << "Done inverting\n";

    // sanity check; this should print the same thing
    for (unsigned x = 0; x < perm2.size(); ++x) {
      if (perm[x] == 0) {
        std::cout << "Zero is at " << x << "\n";
        break;
      }
    }
    std::cout << "Zero is at " << perm2[0] << "\n";

    // do actual permutation of the graph
    Graph out;
    katana::permute<EdgeTy>(graph, perm2, out);
    outputPermutation(perm2);

    // std::cout << "Biggest was " << first << " now " << perm2[first] << " with
    // "
    //           << std::distance(out.edge_begin(perm2[first]),
    //           out.edge_end(perm2[first]))
    //           << "\n";

    out.toFile(outfilename);
    printStatus(graph.size(), graph.sizeEdges());
  }
};

struct RemoveHighDegree : public Conversion {
  template <typename EdgeTy>
  void convert(const std::string& infilename, const std::string& outfilename) {
    typedef katana::FileGraph Graph;
    typedef Graph::GraphNode GNode;
    typedef katana::FileGraphWriter Writer;
    typedef katana::NUMAArray<EdgeTy> EdgeData;
    typedef typename EdgeData::value_type edge_value_type;

    Graph graph;
    graph.fromFile(infilename);

    Writer p;
    EdgeData edgeData;

    std::vector<GNode> nodeTable;
    nodeTable.resize(graph.size());
    uint64_t numNodes = 0;
    uint64_t numEdges = 0;
    for (Graph::iterator ii = graph.begin(), ei = graph.end(); ii != ei; ++ii) {
      GNode src = *ii;
      Graph::edge_iterator jj = graph.edge_begin(src), ej = graph.edge_end(src);
      if (graph.edges(src).size() > maxDegree)
        continue;
      nodeTable[src] = numNodes++;
      for (; jj != ej; ++jj) {
        GNode dst = graph.getEdgeDst(jj);
        if (graph.edges(dst).size() > maxDegree)
          continue;
        ++numEdges;
      }
    }

    if (numEdges == graph.sizeEdges() && numNodes == graph.size()) {
      std::cout << "Graph already simplified; copy input to output\n";
      printStatus(graph.size(), graph.sizeEdges());
      graph.toFile(outfilename);
      return;
    }

    p.setNumNodes(numNodes);
    p.setNumEdges(numEdges);
    p.setSizeofEdgeData(EdgeData::size_of::value);
    edgeData.create(numEdges);

    p.phase1();
    for (Graph::iterator ii = graph.begin(), ei = graph.end(); ii != ei; ++ii) {
      GNode src = *ii;
      Graph::edge_iterator jj = graph.edge_begin(src), ej = graph.edge_end(src);
      if (graph.edges(src).size() > maxDegree)
        continue;
      for (; jj != ej; ++jj) {
        GNode dst = graph.getEdgeDst(jj);
        if (graph.edges(dst).size() > maxDegree)
          continue;
        p.incrementDegree(nodeTable[src]);
      }
    }

    p.phase2();
    for (Graph::iterator ii = graph.begin(), ei = graph.end(); ii != ei; ++ii) {
      GNode src = *ii;
      Graph::edge_iterator jj = graph.edge_begin(src), ej = graph.edge_end(src);
      if (graph.edges(src).size() > maxDegree)
        continue;
      for (; jj != ej; ++jj) {
        GNode dst = graph.getEdgeDst(jj);
        if (graph.edges(dst).size() > maxDegree)
          continue;
        if (EdgeData::has_value) {
          edgeData.set(
              p.addNeighbor(nodeTable[src], nodeTable[dst]),
              graph.getEdgeData<edge_value_type>(jj));
        } else {
          p.addNeighbor(nodeTable[src], nodeTable[dst]);
        }
      }
    }

    edge_value_type* rawEdgeData = p.finish<edge_value_type>();
    if (EdgeData::has_value)
      std::uninitialized_copy(
          std::make_move_iterator(edgeData.begin()),
          std::make_move_iterator(edgeData.end()), rawEdgeData);

    p.toFile(outfilename);
    printStatus(graph.size(), graph.sizeEdges(), p.size(), p.sizeEdges());
  }
};

//! Partition graph into balanced number of edges by source node
struct PartitionBySource : public Conversion {
  template <typename EdgeTy>
  void convert(const std::string& infilename, const std::string& outfilename) {
    typedef katana::FileGraph Graph;
    typedef Graph::GraphNode GNode;
    typedef katana::FileGraphWriter Writer;
    typedef katana::NUMAArray<EdgeTy> EdgeData;
    typedef typename EdgeData::value_type edge_value_type;

    Graph graph;
    graph.fromFile(infilename);

    for (int i = 0; i < numParts; ++i) {
      Writer p;
      EdgeData edgeData;

      auto r = graph.divideByNode(0, 1, i, numParts).first;

      size_t numEdges = 0;
      if (r.first != r.second)
        numEdges = std::distance(
            graph.edge_begin(*r.first), graph.edge_end(*(r.second - 1)));

      p.setNumNodes(graph.size());
      p.setNumEdges(numEdges);
      p.setSizeofEdgeData(EdgeData::size_of::value);
      edgeData.create(numEdges);

      p.phase1();
      for (Graph::iterator ii = r.first, ei = r.second; ii != ei; ++ii) {
        GNode src = *ii;
        p.incrementDegree(src, graph.edges(src).size());
      }

      p.phase2();
      for (Graph::iterator ii = r.first, ei = r.second; ii != ei; ++ii) {
        GNode src = *ii;
        for (Graph::edge_iterator jj = graph.edge_begin(src),
                                  ej = graph.edge_end(src);
             jj != ej; ++jj) {
          GNode dst = graph.getEdgeDst(jj);
          if (EdgeData::has_value)
            edgeData.set(
                p.addNeighbor(src, dst),
                graph.getEdgeData<edge_value_type>(jj));
          else
            p.addNeighbor(src, dst);
        }
      }

      edge_value_type* rawEdgeData = p.finish<edge_value_type>();
      if (EdgeData::has_value)
        std::uninitialized_copy(
            std::make_move_iterator(edgeData.begin()),
            std::make_move_iterator(edgeData.end()), rawEdgeData);

      std::ostringstream partname;
      partname << outfilename << "." << i << ".of." << numParts;

      p.toFile(partname.str());
      printStatus(graph.size(), graph.sizeEdges(), p.size(), p.sizeEdges());
    }
  }
};

template <typename InDegree, typename It = typename InDegree::iterator>
static std::pair<It, It>
divide_by_destination(InDegree& inDegree, int id, int total) {
  if (inDegree.begin() == inDegree.end())
    return std::make_pair(inDegree.begin(), inDegree.end());

  size_t size = inDegree[inDegree.size() - 1];
  size_t block = (size + total - 1) / total;

  It bb = std::lower_bound(inDegree.begin(), inDegree.end(), id * block);
  It eb;
  if (id + 1 == total)
    eb = inDegree.end();
  else
    eb = std::upper_bound(bb, inDegree.end(), (id + 1) * block);
  return std::make_pair(bb, eb);
}

template <typename GraphTy, typename InDegree>
static void
compute_indegree(GraphTy& graph, InDegree& inDegree) {
  inDegree.create(graph.size());

  for (auto nn = graph.begin(), en = graph.end(); nn != en; ++nn) {
    for (auto jj = graph.edge_begin(*nn), ej = graph.edge_end(*nn); jj != ej;
         ++jj) {
      auto dst = graph.getEdgeDst(jj);
      inDegree[dst] += 1;
    }
  }

  for (size_t i = 1; i < inDegree.size(); ++i)
    inDegree[i] = inDegree[i - 1] + inDegree[i];
}

//! Partition graph into balanced number of edges by destination node
struct PartitionByDestination : public Conversion {
  template <typename EdgeTy>
  void convert(const std::string& infilename, const std::string& outfilename) {
    typedef katana::FileGraph Graph;
    typedef Graph::GraphNode GNode;
    typedef katana::FileGraphWriter Writer;
    typedef katana::NUMAArray<EdgeTy> EdgeData;
    typedef katana::NUMAArray<size_t> InDegree;
    typedef typename EdgeData::value_type edge_value_type;

    Graph graph;
    graph.fromFile(infilename);
    InDegree inDegree;
    compute_indegree(graph, inDegree);

    for (int i = 0; i < numParts; ++i) {
      Writer p;
      EdgeData edgeData;

      auto r = divide_by_destination(inDegree, i, numParts);
      size_t bb = std::distance(inDegree.begin(), r.first);
      size_t eb = std::distance(inDegree.begin(), r.second);

      size_t numEdges = 0;
      if (bb != eb) {
        size_t begin = bb == 0 ? 0 : inDegree[bb - 1];
        size_t end = eb == 0 ? 0 : inDegree[eb - 1];
        numEdges = end - begin;
      }

      p.setNumNodes(graph.size());
      p.setNumEdges(numEdges);
      p.setSizeofEdgeData(EdgeData::size_of::value);
      edgeData.create(numEdges);

      p.phase1();
      for (Graph::iterator ii = graph.begin(), ei = graph.end(); ii != ei;
           ++ii) {
        GNode src = *ii;
        for (Graph::edge_iterator jj = graph.edge_begin(src),
                                  ej = graph.edge_end(src);
             jj != ej; ++jj) {
          GNode dst = graph.getEdgeDst(jj);
          if (dst < bb)
            continue;
          if (dst >= eb)
            continue;
          p.incrementDegree(src);
        }
      }

      p.phase2();
      for (Graph::iterator ii = graph.begin(), ei = graph.end(); ii != ei;
           ++ii) {
        GNode src = *ii;
        for (Graph::edge_iterator jj = graph.edge_begin(src),
                                  ej = graph.edge_end(src);
             jj != ej; ++jj) {
          GNode dst = graph.getEdgeDst(jj);
          if (dst < bb)
            continue;
          if (dst >= eb)
            continue;
          if (EdgeData::has_value)
            edgeData.set(
                p.addNeighbor(src, dst),
                graph.getEdgeData<edge_value_type>(jj));
          else
            p.addNeighbor(src, dst);
        }
      }

      edge_value_type* rawEdgeData = p.finish<edge_value_type>();
      if (EdgeData::has_value)
        std::uninitialized_copy(
            std::make_move_iterator(edgeData.begin()),
            std::make_move_iterator(edgeData.end()), rawEdgeData);

      std::ostringstream partname;
      partname << outfilename << "." << i << ".of." << numParts;

      p.toFile(partname.str());
      printStatus(graph.size(), graph.sizeEdges(), p.size(), p.sizeEdges());
    }
  }
};

//! Transpose graph
struct Transpose : public Conversion {
  template <typename EdgeTy>
  void convert(const std::string& infilename, const std::string& outfilename) {
    typedef katana::FileGraph Graph;
    typedef Graph::GraphNode GNode;
    typedef katana::FileGraphWriter Writer;
    typedef katana::NUMAArray<EdgeTy> EdgeData;
    typedef typename EdgeData::value_type edge_value_type;

    Graph graph;
    graph.fromFile(infilename);

    Writer p;
    EdgeData edgeData;

    p.setNumNodes(graph.size());
    p.setNumEdges(graph.sizeEdges());
    p.setSizeofEdgeData(EdgeData::size_of::value);
    edgeData.create(graph.sizeEdges());

    p.phase1();
    for (Graph::iterator ii = graph.begin(), ei = graph.end(); ii != ei; ++ii) {
      GNode src = *ii;

      for (Graph::edge_iterator jj = graph.edge_begin(src),
                                ej = graph.edge_end(src);
           jj != ej; ++jj) {
        GNode dst = graph.getEdgeDst(jj);
        p.incrementDegree(dst);
      }
    }

    p.phase2();
    for (Graph::iterator ii = graph.begin(), ei = graph.end(); ii != ei; ++ii) {
      GNode src = *ii;

      for (Graph::edge_iterator jj = graph.edge_begin(src),
                                ej = graph.edge_end(src);
           jj != ej; ++jj) {
        GNode dst = graph.getEdgeDst(jj);
        if (EdgeData::has_value) {
          edgeData.set(
              p.addNeighbor(dst, src), graph.getEdgeData<edge_value_type>(jj));
        } else {
          p.addNeighbor(dst, src);
        }
      }
    }

    edge_value_type* rawEdgeData = p.finish<edge_value_type>();
    if (EdgeData::has_value)
      std::uninitialized_copy(
          std::make_move_iterator(edgeData.begin()),
          std::make_move_iterator(edgeData.end()), rawEdgeData);

    p.toFile(outfilename);
    printStatus(graph.size(), graph.sizeEdges(), p.size(), p.sizeEdges());
  }
};

template <typename GraphNode, typename EdgeTy>
struct IdLess {
  bool operator()(
      const katana::EdgeSortValue<GraphNode, EdgeTy>& e1,
      const katana::EdgeSortValue<GraphNode, EdgeTy>& e2) const {
    return e1.dst < e2.dst;
  }
};

template <typename GraphNode, typename EdgeTy>
struct WeightLess {
  bool operator()(
      const katana::EdgeSortValue<GraphNode, EdgeTy>& e1,
      const katana::EdgeSortValue<GraphNode, EdgeTy>& e2) const {
    return e1.get() < e2.get();
  }
};

/**
 * Removes self and multi-edges from a graph.
 */
struct Cleanup : public Conversion {
  template <typename EdgeTy>
  void convert(const std::string& infilename, const std::string& outfilename) {
    typedef katana::FileGraph Graph;
    typedef Graph::GraphNode GNode;

    Graph orig, graph;
    {
      // Original FileGraph is immutable because it is backed by a file
      orig.fromFile(infilename);
      graph = orig;
    }

    size_t numEdges = 0;

    for (Graph::iterator ii = graph.begin(), ei = graph.end(); ii != ei; ++ii) {
      GNode src = *ii;

      graph.sortEdges<EdgeTy>(src, IdLess<GNode, EdgeTy>());

      Graph::edge_iterator prev = graph.edge_end(src);
      for (Graph::edge_iterator jj = graph.edge_begin(src),
                                ej = graph.edge_end(src);
           jj != ej; ++jj) {
        GNode dst = graph.getEdgeDst(jj);
        if (src == dst) {
        } else if (prev != ej && graph.getEdgeDst(prev) == dst) {
        } else {
          numEdges += 1;
        }
        prev = jj;
      }
    }

    if (numEdges == graph.sizeEdges()) {
      std::cout << "Graph already simplified; copy input to output\n";
      printStatus(graph.size(), graph.sizeEdges());
      graph.toFile(outfilename);
      return;
    }

    typedef katana::FileGraphWriter Writer;
    typedef katana::NUMAArray<EdgeTy> EdgeData;
    typedef typename EdgeData::value_type edge_value_type;

    Writer p;
    EdgeData edgeData;

    p.setNumNodes(graph.size());
    p.setNumEdges(numEdges);
    p.setSizeofEdgeData(EdgeData::size_of::value);
    edgeData.create(numEdges);

    p.phase1();
    for (Graph::iterator ii = graph.begin(), ei = graph.end(); ii != ei; ++ii) {
      GNode src = *ii;

      Graph::edge_iterator prev = graph.edge_end(src);
      for (Graph::edge_iterator jj = graph.edge_begin(src),
                                ej = graph.edge_end(src);
           jj != ej; ++jj) {
        GNode dst = graph.getEdgeDst(jj);
        if (src == dst) {
        } else if (prev != ej && graph.getEdgeDst(prev) == dst) {
        } else {
          p.incrementDegree(src);
        }
        prev = jj;
      }
    }

    p.phase2();
    for (Graph::iterator ii = graph.begin(), ei = graph.end(); ii != ei; ++ii) {
      GNode src = *ii;

      Graph::edge_iterator prev = graph.edge_end(src);
      for (Graph::edge_iterator jj = graph.edge_begin(src),
                                ej = graph.edge_end(src);
           jj != ej; ++jj) {
        GNode dst = graph.getEdgeDst(jj);
        if (src == dst) {
        } else if (prev != ej && graph.getEdgeDst(prev) == dst) {
        } else if (EdgeData::has_value) {
          edgeData.set(
              p.addNeighbor(src, dst), graph.getEdgeData<edge_value_type>(jj));
        } else {
          p.addNeighbor(src, dst);
        }
        prev = jj;
      }
    }

    edge_value_type* rawEdgeData = p.finish<edge_value_type>();
    if (EdgeData::has_value)
      std::uninitialized_copy(
          std::make_move_iterator(edgeData.begin()),
          std::make_move_iterator(edgeData.end()), rawEdgeData);

    p.toFile(outfilename);
    printStatus(graph.size(), graph.sizeEdges(), p.size(), p.sizeEdges());
  }
};

template <template <typename, typename> class SortBy, bool NeedsEdgeData>
struct SortEdges
    : public boost::mpl::if_c<
          NeedsEdgeData, HasNoVoidSpecialization, Conversion>::type {
  template <typename EdgeTy>
  void convert(const std::string& infilename, const std::string& outfilename) {
    typedef katana::FileGraph Graph;
    typedef Graph::GraphNode GNode;

    Graph orig, graph;
    {
      // Original FileGraph is immutable because it is backed by a file
      orig.fromFile(infilename);
      graph = orig;
    }

    for (typename Graph::iterator ii = graph.begin(), ei = graph.end();
         ii != ei; ++ii) {
      GNode src = *ii;

      graph.sortEdges<EdgeTy>(src, SortBy<GNode, EdgeTy>());
    }

    graph.toFile(outfilename);
    printStatus(graph.size(), graph.sizeEdges());
  }
};

/**
 * Removes edges such that src > dst
 */
struct MakeUnsymmetric : public Conversion {
  template <typename EdgeTy>
  void convert(const std::string& infilename, const std::string& outfilename) {
    typedef katana::FileGraph Graph;
    typedef Graph::GraphNode GNode;

    Graph graph;
    graph.fromFile(infilename);

    size_t numEdges = 0;

    for (Graph::iterator ii = graph.begin(), ei = graph.end(); ii != ei; ++ii) {
      GNode src = *ii;

      for (Graph::edge_iterator jj = graph.edge_begin(src),
                                ej = graph.edge_end(src);
           jj != ej; ++jj) {
        GNode dst = graph.getEdgeDst(jj);
        if (src > dst) {
        } else {
          numEdges += 1;
        }
      }
    }

    if (numEdges == graph.sizeEdges()) {
      std::cout << "Graph already simplified; copy input to output\n";
      printStatus(graph.size(), graph.sizeEdges());
      graph.toFile(outfilename);
      return;
    }

    typedef katana::FileGraphWriter Writer;
    typedef katana::NUMAArray<EdgeTy> EdgeData;
    typedef typename EdgeData::value_type edge_value_type;

    Writer p;
    EdgeData edgeData;

    p.setNumNodes(graph.size());
    p.setNumEdges(numEdges);
    p.setSizeofEdgeData(EdgeData::size_of::value);
    edgeData.create(numEdges);

    p.phase1();
    for (Graph::iterator ii = graph.begin(), ei = graph.end(); ii != ei; ++ii) {
      GNode src = *ii;

      for (Graph::edge_iterator jj = graph.edge_begin(src),
                                ej = graph.edge_end(src);
           jj != ej; ++jj) {
        GNode dst = graph.getEdgeDst(jj);
        if (src > dst) {
        } else {
          p.incrementDegree(src);
        }
      }
    }

    p.phase2();
    for (Graph::iterator ii = graph.begin(), ei = graph.end(); ii != ei; ++ii) {
      GNode src = *ii;

      for (Graph::edge_iterator jj = graph.edge_begin(src),
                                ej = graph.edge_end(src);
           jj != ej; ++jj) {
        GNode dst = graph.getEdgeDst(jj);
        if (src > dst) {
        } else if (EdgeData::has_value) {
          edgeData.set(
              p.addNeighbor(src, dst), graph.getEdgeData<edge_value_type>(jj));
        } else {
          p.addNeighbor(src, dst);
        }
      }
    }

    edge_value_type* rawEdgeData = p.finish<edge_value_type>();
    if (EdgeData::has_value)
      std::uninitialized_copy(
          std::make_move_iterator(edgeData.begin()),
          std::make_move_iterator(edgeData.end()), rawEdgeData);

    p.toFile(outfilename);
    printStatus(graph.size(), graph.sizeEdges(), p.size(), p.sizeEdges());
  }
};

// Example:
//  c Some file
//  c Comments
//  p XXX* <num nodes> <num edges>
//  a <src id> <dst id> <weight>
//  ....
struct Dimacs2Gr : public HasNoVoidSpecialization {
  template <typename EdgeTy>
  void convert(const std::string& infilename, const std::string& outfilename) {
    typedef katana::FileGraphWriter Writer;
    typedef katana::NUMAArray<EdgeTy> EdgeData;
    typedef typename EdgeData::value_type edge_value_type;

    Writer p;
    EdgeData edgeData;
    uint32_t nnodes;
    size_t nedges;

    for (int phase = 0; phase < 2; ++phase) {
      std::ifstream infile(infilename.c_str());

      // Skip comments
      while (infile) {
        if (infile.peek() == 'p') {
          break;
        }
        skipLine(infile);
      }

      // Read header
      char header[256];
      infile.getline(header, 256);
      std::istringstream line(header, std::istringstream::in);
      std::vector<std::string> tokens;
      while (line) {
        std::string tmp;
        line >> tmp;
        if (line) {
          tokens.push_back(tmp);
        }
      }
      if (tokens.size() < 3 || tokens[0].compare("p") != 0) {
        KATANA_DIE("unknown problem specification line: ", line.str());
      }
      // Prefer C functions for maximum compatibility
      // nnodes = std::stoull(tokens[tokens.size() - 2]);
      // nedges = std::stoull(tokens[tokens.size() - 1]);
      nnodes = strtoull(tokens[tokens.size() - 2].c_str(), NULL, 0);
      nedges = strtoull(tokens[tokens.size() - 1].c_str(), NULL, 0);

      // Parse edges
      if (phase == 0) {
        p.setNumNodes(nnodes);
        p.setNumEdges(nedges);
        p.setSizeofEdgeData(EdgeData::size_of::value);
        edgeData.create(nedges);
        p.phase1();
      } else {
        p.phase2();
      }

      for (size_t edge_num = 0; edge_num < nedges; ++edge_num) {
        uint32_t cur_id;
        uint32_t neighbor_id;
        int32_t weight;
        std::string tmp;
        infile >> tmp;

        if (tmp.compare("a") != 0) {
          --edge_num;
          skipLine(infile);
          continue;
        }

        infile >> cur_id >> neighbor_id >> weight;
        if (cur_id == 0 || cur_id > nnodes) {
          KATANA_DIE("node id out of range: ", cur_id);
        }
        if (neighbor_id == 0 || neighbor_id > nnodes) {
          KATANA_DIE("neighbor id out of range: ", neighbor_id);
        }

        // 1 indexed
        if (phase == 0) {
          p.incrementDegree(cur_id - 1);
        } else {
          edgeData.set(p.addNeighbor(cur_id - 1, neighbor_id - 1), weight);
        }

        skipLine(infile);
      }

      infile.peek();
      if (!infile.eof()) {
        KATANA_DIE("additional lines in file");
      }
    }

    edge_value_type* rawEdgeData = p.finish<edge_value_type>();
    if (EdgeData::has_value)
      std::uninitialized_copy(
          std::make_move_iterator(edgeData.begin()),
          std::make_move_iterator(edgeData.end()), rawEdgeData);

    p.toFile(outfilename);
    printStatus(p.size(), p.sizeEdges());
  }
};

/**
 * PBBS input is an ASCII file of tokens that serialize a CSR graph. I.e.,
 * elements in brackets are non-literals:
 *
 * AdjacencyGraph
 * <num nodes>
 * <num edges>
 * <offset node 0>
 * <offset node 1>
 * ...
 * <edge 0>
 * <edge 1>
 * ...
 */
struct Pbbs2Gr : public HasOnlyVoidSpecialization {
  template <typename EdgeTy>
  void convert(const std::string& infilename, const std::string& outfilename) {
    static_assert(
        std::is_same<EdgeTy, void>::value,
        "conversion undefined for non-void graphs");
    typedef katana::FileGraphWriter Writer;

    Writer p;

    std::ifstream infile(infilename.c_str());
    std::string header;
    uint32_t nnodes;
    size_t nedges;

    infile >> header >> nnodes >> nedges;
    if (header != "AdjacencyGraph") {
      KATANA_DIE("unknown file format");
    }

    p.setNumNodes(nnodes);
    p.setNumEdges(nedges);

    size_t* offsets = new size_t[nnodes];
    for (size_t i = 0; i < nnodes; ++i) {
      infile >> offsets[i];
    }

    size_t* edges = new size_t[nedges];
    for (size_t i = 0; i < nedges; ++i) {
      infile >> edges[i];
    }

    p.phase1();
    for (uint32_t i = 0; i < nnodes; ++i) {
      size_t begin = offsets[i];
      size_t end = (i == nnodes - 1) ? nedges : offsets[i + 1];
      p.incrementDegree(i, end - begin);
    }

    p.phase2();
    for (uint32_t i = 0; i < nnodes; ++i) {
      size_t begin = offsets[i];
      size_t end = (i == nnodes - 1) ? nedges : offsets[i + 1];
      for (size_t j = begin; j < end; ++j) {
        size_t dst = edges[j];
        p.addNeighbor(i, dst);
      }
    }

    p.finish<void>();

    p.toFile(outfilename);
    printStatus(p.size(), p.sizeEdges());
  }
};

// TODO
// gr Version 2 support doesn't exist
struct Gr2Pbbsedges : public HasNoVoidSpecialization {
  template <typename EdgeTy>
  void convert(const std::string& infilename, const std::string& outfilename) {
    // Use FileGraph because it is basically in CSR format needed for pbbs
    typedef katana::FileGraph Graph;
    typedef Graph::GraphNode GNode;

    Graph graph;
    graph.fromFile(infilename);

    std::ofstream file(outfilename.c_str());
    file << "WeightedEdgeArray\n";
    for (Graph::iterator ii = graph.begin(), ei = graph.end(); ii != ei; ++ii) {
      GNode src = *ii;
      for (Graph::edge_iterator jj = graph.edge_begin(src),
                                ej = graph.edge_end(src);
           jj != ej; ++jj) {
        GNode dst = graph.getEdgeDst(jj);
        EdgeTy& weight = graph.getEdgeData<EdgeTy>(jj);
        file << src << " " << dst << " " << weight << "\n";
      }
    }
    file.close();

    printStatus(graph.size(), graph.sizeEdges());
  }
};

/**
 * PBBS input is an ASCII file of tokens that serialize a CSR graph. I.e.,
 * elements in brackets are non-literals:
 *
 * [Weighted]AdjacencyGraph
 * <num nodes>
 * <num edges>
 * <offset node 0>
 * <offset node 1>
 * ...
 * <edge 0>
 * <edge 1>
 * ...
 * [
 * <edge weight 0>
 * <edge weight 1>
 * ...
 * ]
 */
// TODO
// gr Version 2 support doesn't exist
struct Gr2Pbbs : public Conversion {
  template <typename EdgeTy>
  void convert(const std::string& infilename, const std::string& outfilename) {
    typedef katana::FileGraph Graph;
    typedef katana::NUMAArray<EdgeTy> EdgeData;
    typedef typename EdgeData::value_type edge_value_type;

    Graph graph;
    graph.fromFile(infilename);

    std::ofstream file(outfilename.c_str());
    if (EdgeData::has_value)
      file << "Weighted";
    file << "AdjacencyGraph\n"
         << graph.size() << "\n"
         << graph.sizeEdges() << "\n";
    // edgeid[i] is the end of i in FileGraph while it is the beginning of i in
    // pbbs graph
    size_t last = std::distance(graph.edge_id_begin(), graph.edge_id_end());
    size_t count = 0;
    file << "0\n";
    for (Graph::edge_id_iterator ii = graph.edge_id_begin(),
                                 ei = graph.edge_id_end();
         ii != ei; ++ii, ++count) {
      if (count < last - 1)
        file << *ii << "\n";
    }
    for (Graph::node_id_iterator ii = graph.node_id_begin(),
                                 ei = graph.node_id_end();
         ii != ei; ++ii) {
      file << *ii << "\n";
    }
    if (EdgeData::has_value) {
      for (edge_value_type *ii = graph.edge_data_begin<edge_value_type>(),
                           *ei = graph.edge_data_end<edge_value_type>();
           ii != ei; ++ii) {
        file << *ii << "\n";
      }
    }
    file.close();

    printStatus(graph.size(), graph.sizeEdges());
  }
};

/**
 * Binary PBBS format is three files.
 *
 * <base>.config - ASCII file with number of vertices
 * <base>.adj - Binary adjacencies
 * <base>.idx - Binary offsets for adjacencies
 */
template <typename NodeIdx, typename Offset>
struct Gr2BinaryPbbs : public HasOnlyVoidSpecialization {
  template <typename EdgeTy>
  void convert(const std::string& infilename, const std::string& outfilename) {
    static_assert(
        std::is_same<EdgeTy, void>::value,
        "conversion undefined for non-void graphs");
    typedef katana::FileGraph Graph;

    Graph graph;
    graph.fromFile(infilename);

    {
      std::string configName = outfilename + ".config";
      std::ofstream configFile(configName.c_str());
      configFile << graph.size() << "\n";
    }

    {
      std::string idxName = outfilename + ".idx";
      std::ofstream idxFile(idxName.c_str());
      // edgeid[i] is the end of i in FileGraph while it is the beginning of i
      // in pbbs graph
      size_t last = std::distance(graph.edge_id_begin(), graph.edge_id_end());
      size_t count = 0;
      Offset offset = 0;
      idxFile.write(reinterpret_cast<char*>(&offset), sizeof(offset));
      for (Graph::edge_id_iterator ii = graph.edge_id_begin(),
                                   ei = graph.edge_id_end();
           ii != ei; ++ii, ++count) {
        offset = *ii;
        if (count < last - 1)
          idxFile.write(reinterpret_cast<char*>(&offset), sizeof(offset));
      }
      idxFile.close();
    }

    {
      std::string adjName = outfilename + ".adj";
      std::ofstream adjFile(adjName.c_str());
      for (Graph::node_id_iterator ii = graph.node_id_begin(),
                                   ei = graph.node_id_end();
           ii != ei; ++ii) {
        NodeIdx nodeIdx = *ii;
        adjFile.write(reinterpret_cast<char*>(&nodeIdx), sizeof(nodeIdx));
      }
      adjFile.close();
    }

    printStatus(graph.size(), graph.sizeEdges());
  }
};

struct Gr2Dimacs : public HasNoVoidSpecialization {
  template <typename EdgeTy>
  void convert(const std::string& infilename, const std::string& outfilename) {
    typedef katana::FileGraph Graph;
    typedef Graph::GraphNode GNode;

    Graph graph;
    graph.fromFile(infilename);

    std::ofstream file(outfilename.c_str());
    file << "p sp " << graph.size() << " " << graph.sizeEdges() << "\n";
    for (Graph::iterator ii = graph.begin(), ei = graph.end(); ii != ei; ++ii) {
      GNode src = *ii;
      for (Graph::edge_iterator jj = graph.edge_begin(src),
                                ej = graph.edge_end(src);
           jj != ej; ++jj) {
        GNode dst = graph.getEdgeDst(jj);
        EdgeTy& weight = graph.getEdgeData<EdgeTy>(jj);
        file << "a " << src + 1 << " " << dst + 1 << " " << weight << "\n";
      }
    }
    file.close();

    printStatus(graph.size(), graph.sizeEdges());
  }
};

/**
 * RMAT format (zero indexed):
 *  %%% Comment1
 *  %%% Comment2
 *  %%% Comment3
 *  <num nodes> <num edges>
 *  <node id> <num edges> [<neighbor id> <neighbor weight>]*
 *  ...
 */
template <typename OutEdgeTy>
struct Gr2Rmat : public HasNoVoidSpecialization {
  template <typename InEdgeTy>
  void convert(const std::string& infilename, const std::string& outfilename) {
    typedef katana::FileGraph Graph;
    typedef Graph::GraphNode GNode;

    Graph graph;
    graph.fromFile(infilename);

    std::ofstream file(outfilename.c_str());
    file << "%%%\n";
    file << "%%%\n";
    file << "%%%\n";
    file << graph.size() << " " << graph.sizeEdges() << "\n";
    for (Graph::iterator ii = graph.begin(), ei = graph.end(); ii != ei; ++ii) {
      GNode src = *ii;
      file << *ii << " " << graph.edges(src).size();
      for (Graph::edge_iterator jj = graph.edge_begin(src),
                                ej = graph.edge_end(src);
           jj != ej; ++jj) {
        GNode dst = graph.getEdgeDst(jj);
        OutEdgeTy weight = graph.getEdgeData<InEdgeTy>(jj);
        file << " " << dst << " " << weight;
      }
      file << "\n";
    }
    file.close();

    printStatus(graph.size(), graph.sizeEdges());
  }
};
template <template <typename, typename> class SortBy>
struct Gr2Totem : public HasNoVoidSpecialization {
  template <typename EdgeTy>
  void convert(const std::string& infilename, const std::string& outfilename) {
    typedef katana::FileGraph Graph;
    typedef Graph::GraphNode GNode;

    Graph orig, graph;
    {
      // Original FileGraph is immutable because it is backed by a file
      orig.fromFile(infilename);
      graph = orig;
    }

    const uint32_t BINARY_MAGIC_WORD = 0x10102048;
    FILE* outfile;
    outfile = fopen(outfilename.c_str(), "wr");

    typedef uint32_t vid_t;
    typedef uint32_t eid_t;
    typedef uint32_t weight_t;
    fwrite(&BINARY_MAGIC_WORD, sizeof(uint32_t), 1, outfile);

    uint32_t vid_size = sizeof(vid_t);
    fwrite(&vid_size, sizeof(uint32_t), 1, outfile);
    uint32_t eid_size = sizeof(vid_t);
    fwrite(&eid_size, sizeof(uint32_t), 1, outfile);

    vid_t vertex_count = graph.size();
    fwrite(&vertex_count, sizeof(vid_t), 1, outfile);
    eid_t edge_count = graph.sizeEdges();
    fwrite(&edge_count, sizeof(eid_t), 1, outfile);

    bool valued = false;
    fwrite(&valued, sizeof(bool), 1, outfile);
    bool weighted = true;
    fwrite(&weighted, sizeof(bool), 1, outfile);
    bool directed = true;
    fwrite(&directed, sizeof(bool), 1, outfile);

    vid_t* nodes = (vid_t*)malloc(sizeof(vid_t) * (vertex_count + 1));
    eid_t* edges = (eid_t*)malloc(sizeof(vid_t) * edge_count);
    weight_t* weights = (weight_t*)malloc(sizeof(vid_t) * edge_count);
    memset(nodes, 0, sizeof(vid_t) * (vertex_count + 1));
    memset(edges, 0, sizeof(vid_t) * eid_size);
    memset(weights, 0, sizeof(vid_t) * eid_size);
    vid_t vid = 0;
    eid_t eid = 0;

    Graph::iterator e_start = graph.edge_begin(*graph.begin());

    for (Graph::iterator ii = graph.begin(), ei = graph.end(); ii != ei;
         ++ii, vid++) {
      GNode src = *ii;
      nodes[vid] = std::distance(e_start, graph.edge_begin(src));
      graph.sortEdges<EdgeTy>(src, SortBy<GNode, EdgeTy>());
      for (Graph::edge_iterator jj = graph.edge_begin(src),
                                ej = graph.edge_end(src);
           jj != ej; ++jj, eid++) {
        GNode dst = graph.getEdgeDst(jj);
        edges[eid] = (vid_t)dst;
        weights[eid] = (uint32_t)graph.getEdgeData<EdgeTy>(jj);
        // printf("%d %d %u \n", vid, edges[eid], weights[eid]);
      }
    }
    nodes[vertex_count] = graph.sizeEdges();
    fwrite(nodes, sizeof(vid_t), vertex_count + 1, outfile);
    fwrite(edges, sizeof(eid_t), edge_count, outfile);
    fwrite(weights, sizeof(weight_t), edge_count, outfile);
    // printf("nodes: %d %d %d\n", nodes[0],nodes[1],nodes[2]);

    // printf("nodes: %d %d %d\n", edges[0],edges[1],edges[2]);
    // printf("nodes: %d %d %d\n", weights[0],weights[1],weights[2]);

    fclose(outfile);

    printStatus(graph.size(), graph.sizeEdges());
  }
};

struct Gr2Neo4j : public Conversion {
  /**
   * This conversion produces 4 files to use to import into neo4j.
   *
   * <output>.nodesheader: column names for columns in .nodes file
   * <output>.nodes: a list of numbers from 0 -> number of nodes - 1 to signify
   * IDs of nodes to be imported into neo4j
   * <output>.edgesheader: column names for edges, which is just source, dest,
   * and the name of the edge weight property ("value")
   * <output>.edges: edgelist in text format with <src> <dst> <weight> (only if
   * type is specified by user)
   *
   * These files can be used to import a GR into neo4j with the following
   * command.
   *
   * ./neo4j-admin import --nodes=Vertex="<nodesheader>,<nodes>"
   * --relationships=EDGE="<edgesheader>,<edges>" --delimiter ','
   *
   * The label on nodes will be "Vertex", and the label on edges will be "EDGE".
   * The weight, if it exists, will be in the property "value" on the edge.
   *
   * @tparam EdgeTy type of the edge on the gr. Right now, integer is the only
   * type that is supported for sure
   * @param infilename gr file to convert
   * @param outfilename Prefix of output files, i.e. <outfilename>.nodesheader,
   * <outfilename>.nodes, etc.
   */
  template <typename EdgeTy>
  void convert(const std::string& infilename, const std::string& outfilename) {
    using Graph = katana::FileGraph;
    using GNode = Graph::GraphNode;
    using EdgeData = katana::NUMAArray<EdgeTy>;
    using edge_value_type = typename EdgeData::value_type;

    Graph graph;
    graph.fromFile(infilename);

    // output node csv for node creation

    // first is header
    std::string nodeHFile = outfilename + ".nodesheader";
    std::ofstream fileH(nodeHFile.c_str());
    fileH << ":ID\n";
    fileH.close();

    // then nodes
    std::string nodeFile = outfilename + ".nodes";
    std::ofstream fileN(nodeFile.c_str());
    // pretty inefficient: literally just printing 0 -> n in a text file, but
    // sadly it's what neo4j requires to my knowledge
    for (size_t i = 0; i < graph.size(); i++) {
      fileN << i << "\n";
    }
    fileN.close();

    // output edge CSV with or without data for edge creation
    std::string edgeHFile = outfilename + ".edgesheader";
    std::ofstream fileHE(edgeHFile.c_str());

    if (EdgeData::has_value) {
      // TODO right now assumes integer output
      fileHE << ":START_ID,:END_ID,value:int\n";
    } else {
      fileHE << ":START_ID,:END_ID\n";
    }
    fileHE.close();

    // output edge CSV with or without data for edge creation
    std::string edgeFile = outfilename + ".edges";
    std::ofstream fileE(edgeFile.c_str());

    // write edges
    for (Graph::iterator ii = graph.begin(), ei = graph.end(); ii != ei; ++ii) {
      GNode src = *ii;
      for (Graph::edge_iterator jj = graph.edge_begin(src),
                                ej = graph.edge_end(src);
           jj != ej; ++jj) {
        GNode dst = graph.getEdgeDst(jj);
        if (EdgeData::has_value) {
          fileE << src << "," << dst << ","
                << graph.getEdgeData<edge_value_type>(jj) << "\n";
        } else {
          fileE << src << "," << dst << "\n";
        }
      }
    }
    fileE.close();

    printStatus(graph.size(), graph.sizeEdges());
  }
};

/**
 * This is Required gr to kg conversion to append edge data
 * as the edge property.
 */
template <typename EdgeTy>
katana::Result<void>
AppendEdgeData(
    katana::PropertyGraph* pg, const katana::NUMAArray<EdgeTy>& edge_data) {
  using Builder = typename arrow::CTypeTraits<EdgeTy>::BuilderType;
  using ArrowType = typename arrow::CTypeTraits<EdgeTy>::ArrowType;
  Builder builder;
  if (auto r = builder.AppendValues(edge_data.begin(), edge_data.end());
      !r.ok()) {
    KATANA_LOG_DEBUG("arrow error: {}", r);
    return katana::ErrorCode::ArrowError;
  }

  std::shared_ptr<arrow::Array> ret;
  if (auto r = builder.Finish(&ret); !r.ok()) {
    KATANA_LOG_DEBUG("arrow error: {}", r);
    return katana::ErrorCode::ArrowError;
  }
  std::vector<std::shared_ptr<arrow::Field>> fields;
  std::vector<std::shared_ptr<arrow::Array>> columns;
  fields.emplace_back(arrow::field(("value"), std::make_shared<ArrowType>()));
  columns.emplace_back(ret);
  auto edge_data_table = arrow::Table::Make(arrow::schema(fields), columns);
  if (auto r = pg->AddEdgeProperties(edge_data_table); !r) {
    KATANA_LOG_DEBUG("could not add edge property: {}", r.error());
    return r;
  }
  return katana::ResultSuccess();
}

template <>
katana::Result<void>
AppendEdgeData<void>(katana::PropertyGraph*, const katana::NUMAArray<void>&) {
  return katana::ResultSuccess();
}

/**
 * Gr2Kg reads in the binary csr (.gr) files and produces
 * katana graph property graphs.
 */
struct Gr2Kg : public Conversion {
  katana::Result<void> OutOfCoreConvert(
      const std::string& in_file_name, const std::string& out_file_name) {
    tsuba::CSRTopologyHeader header;
    if (auto res = tsuba::FileGet(in_file_name, &header); !res) {
      return res.error();
    }

    if (header.version != 1) {
      KATANA_LOG_ERROR("Out of core not possible, katana expects GR v1");
      return katana::ErrorCode::NotImplemented;
    }

    if (header.edge_type_size != 0) {
      KATANA_LOG_WARN("ignoring existing edge property in conversion");
      header.edge_type_size = 0;
    }

    tsuba::StatBuf stat_buf;
    if (auto res = tsuba::FileStat(in_file_name, &stat_buf); !res) {
      KATANA_LOG_DEBUG("could not stat {}", out_file_name);
      return res.error();
    }

    uint64_t new_size = tsuba::CSRTopologyFileSize(header);
    if (stat_buf.size < new_size) {
      KATANA_LOG_ERROR(
          "{} does not appear to be well formed (too small)", in_file_name);
      return katana::ErrorCode::InvalidArgument;
    }

    if (auto res = tsuba::Create(out_file_name); !res) {
      return res.error();
    }

    auto handle_res = tsuba::Open(out_file_name, tsuba::kReadWrite);
    if (!handle_res) {
      return handle_res.error();
    }

    tsuba::RDGFile handle(std::move(handle_res.value()));

    katana::Uri top_file_name = tsuba::MakeTopologyFileName(handle);
    if (auto res = tsuba::FileRemoteCopy(
            in_file_name, top_file_name.string(), 0, new_size);
        !res) {
      return res.error();
    }

    tsuba::RDG rdg;
    rdg.set_rdg_dir(tsuba::GetRDGDir(handle));
    if (auto res = rdg.SetTopologyFile(top_file_name); !res) {
      return res.error();
    }
    return rdg.Store(handle, kCommandLine);
  }

  template <typename EdgeTy>
  void convert(
      const std::string& in_file_name, const std::string& out_file_name) {
    using Graph = katana::FileGraph;
    using GNode = Graph::GraphNode;
    using EdgeData = katana::NUMAArray<EdgeTy>;
    using edge_value_type = typename EdgeData::value_type;

    if constexpr (std::is_same<EdgeTy, void>::value) {
      // the property graph topology file format is very close to gr, so we can
      // use this shortcut. This shortcut also avoids reading the graph on one
      // host so it's important to do this to support large graphs
      fmt::print(stderr, "attempting out-of-core conversion\n");

      auto res = OutOfCoreConvert(in_file_name, out_file_name);
      if (res) {
        return;
      }
      if (res.error() != katana::ErrorCode::NotImplemented) {
        KATANA_LOG_FATAL("Failed out-of-core conversion: {}", res.error());
      }
      fmt::print(stderr, "out-of-core not supported for input trying in-core");
    }

    Graph graph;
    graph.fromFile(in_file_name);

    katana::NUMAArray<uint64_t> out_indices;
    out_indices.allocateBlocked(graph.size());

    katana::NUMAArray<uint32_t> out_dests;
    out_dests.allocateBlocked(graph.sizeEdges());

    katana::NUMAArray<EdgeTy> out_dests_data;
    if (EdgeData::has_value) {
      out_dests_data.allocateBlocked(graph.sizeEdges());
    }

    // write edges
    for (Graph::iterator ii = graph.begin(), ei = graph.end(); ii != ei; ++ii) {
      GNode src = *ii;
      out_indices[src] = *graph.edge_end(src);
      for (Graph::edge_iterator jj = graph.edge_begin(src),
                                ej = graph.edge_end(src);
           jj != ej; ++jj) {
        GNode dst = graph.getEdgeDst(jj);
        out_dests[*jj] = dst;
        if (EdgeData::has_value) {
          out_dests_data.set(*jj, graph.getEdgeData<edge_value_type>(jj));
        }
      }
    }

    katana::GraphTopology topo{std::move(out_indices), std::move(out_dests)};
    auto pg_res = katana::PropertyGraph::Make(std::move(topo));
    if (!pg_res) {
      KATANA_LOG_FATAL("Failed to create PropertyGraph");
    }
    std::unique_ptr<katana::PropertyGraph> pg = std::move(pg_res.value());

    if (EdgeData::has_value) {
      if (auto r = AppendEdgeData<EdgeTy>(pg.get(), out_dests_data); !r) {
        KATANA_LOG_FATAL("could not add edge property: {}", r.error());
      }
    }

    katana::gPrint("Edge Schema : ", pg->edge_schema()->ToString(), "\n");
    katana::gPrint("Node Schema : ", pg->node_schema()->ToString(), "\n");

    if (auto r = pg->Write(out_file_name, "cmd"); !r) {
      KATANA_LOG_FATAL("Failed to write property file graph: {}", r.error());
    }
    printStatus(graph.size(), graph.sizeEdges());
  }
};

/**
 * METIS format (1-indexed). See METIS 4.10 manual, section 4.5.
 *  % comment prefix
 *  <num nodes> <num edges> [<data format> [<weights per vertex>]]
 *  [<vertex data>] [<destination> [<edge data>]]*
 *  ...
 * vertex weights must be integers >= 0; edge weights must be > 0.
 * Input graph must be symmetric. Does not write self-edges.
 * FIXME: implement weights.
 */
struct Gr2Metis : public HasOnlyVoidSpecialization {
  template <typename InEdgeTy>
  void convert(const std::string& infilename, const std::string& outfilename) {
    typedef katana::FileGraph Graph;
    typedef Graph::GraphNode GNode;

    Graph graph;
    graph.fromFile(infilename);

    /* Skip self-edges */
    unsigned int nedges = graph.sizeEdges();
    for (Graph::iterator ii = graph.begin(), ei = graph.end(); ii != ei; ++ii) {
      GNode src = *ii;
      for (Graph::edge_iterator jj = graph.edge_begin(src),
                                ej = graph.edge_end(src);
           jj != ej; ++jj) {
        GNode dst = graph.getEdgeDst(jj);
        if (dst == src)
          nedges--;
      }
    }
    KATANA_LOG_DEBUG_ASSERT((nedges % 2) == 0);
    nedges /= 2;  // Do not double-count edges

    std::ofstream file(outfilename.c_str());
    file << graph.size() << " " << nedges << "\n";
    for (Graph::iterator ii = graph.begin(), ei = graph.end(); ii != ei; ++ii) {
      GNode src = *ii;
      for (Graph::edge_iterator jj = graph.edge_begin(src),
                                ej = graph.edge_end(src);
           jj != ej; ++jj) {
        GNode dst = graph.getEdgeDst(jj);
        // OutEdgeTy weight = graph.getEdgeData<InEdgeTy>(jj);
        if (dst != src)
          file << dst + 1 << " ";
      }
      file << "\n";
    }
    file.close();

    printStatus(graph.size(), nedges);
  }
};

/**
 * GR to Binary Sparse MATLAB matrix.
 * [i, j, v] = find(A);
 * fwrite(f, size(A,1), 'uint32');
 * fwrite(f, size(A,2), 'uint32');
 * fwrite(f, nnz(A), 'uint32');
 * fwrite(f, (i-1), 'uint32');     % zero-indexed
 * fwrite(f, (j-1), 'uint32');
 * fwrite(f, v, 'double');
 */
struct Gr2Bsml : public Conversion {
  template <typename EdgeTy>
  void convert(const std::string& infilename, const std::string& outfilename) {
    typedef katana::FileGraph Graph;
    typedef typename Graph::GraphNode GNode;
    typedef typename katana::NUMAArray<EdgeTy> EdgeData;

    Graph graph;
    graph.fromFile(infilename);

    uint32_t nnodes = graph.size();
    uint32_t nedges = graph.sizeEdges();

    std::ofstream file(outfilename.c_str());

    // Write header
    file.write(reinterpret_cast<char*>(&nnodes), sizeof(nnodes));
    file.write(reinterpret_cast<char*>(&nnodes), sizeof(nnodes));
    file.write(reinterpret_cast<char*>(&nedges), sizeof(nedges));

    // Write row adjacency
    for (typename Graph::iterator ii = graph.begin(), ei = graph.end();
         ii != ei; ++ii) {
      GNode src = *ii;
      uint32_t sid = src;
      for (typename Graph::edge_iterator jj = graph.edge_begin(src),
                                         ej = graph.edge_end(src);
           jj != ej; ++jj) {
        file.write(reinterpret_cast<char*>(&sid), sizeof(sid));
      }
    }

    // Write column adjacency
    for (typename Graph::iterator ii = graph.begin(), ei = graph.end();
         ii != ei; ++ii) {
      GNode src = *ii;
      for (typename Graph::edge_iterator jj = graph.edge_begin(src),
                                         ej = graph.edge_end(src);
           jj != ej; ++jj) {
        GNode dst = graph.getEdgeDst(jj);
        uint32_t did = dst;
        file.write(reinterpret_cast<char*>(&did), sizeof(did));
      }
    }

    // Write data
    for (typename Graph::iterator ii = graph.begin(), ei = graph.end();
         ii != ei; ++ii) {
      GNode src = *ii;
      for (typename Graph::edge_iterator jj = graph.edge_begin(src),
                                         ej = graph.edge_end(src);
           jj != ej; ++jj) {
        double weight = static_cast<double>(
            getEdgeValue<EdgeTy, EdgeData::has_value>(graph, jj));
        file.write(reinterpret_cast<char*>(&weight), sizeof(weight));
      }
    }

    file.close();
    printStatus(nnodes, nedges);
  }
};

/**
 * SVMLight format.
 *
 * <line> .=. <target> <feature>:<value> <feature>:<value> ... <feature>:<value>
 * # <info> <target> .=. +1 | -1 | 0 | <float> <feature> .=. <integer> | "qid"
 * <value> .=. <float>
 * <info> .=. <string>
 *
 */
struct Svmlight2Gr : public HasNoVoidSpecialization {
  template <typename EdgeTy>
  void convert(const std::string& infilename, const std::string& outfilename) {
    typedef katana::FileGraphWriter Writer;
    typedef katana::NUMAArray<EdgeTy> EdgeData;
    typedef typename EdgeData::value_type edge_value_type;

    Writer p;
    EdgeData edgeData;
    std::ifstream infile(infilename.c_str());
    std::ofstream outlabels(labelsFilename.c_str());

    if (!outlabels) {
      KATANA_DIE("unable to create labels file");
    }

    size_t featureOffset = 0;
    size_t numEdges = 0;
    long maxFeature = -1;

    for (int phase = 0; phase < 3; ++phase) {
      infile.clear();
      infile.seekg(0, std::ios::beg);
      size_t numNodes = 0;

      while (infile) {
        if (phase == 2) {
          float label;
          infile >> label;
          if (!infile)
            break;
          outlabels << numNodes << " " << label << "\n";
        } else {
          infile.ignore(std::numeric_limits<std::streamsize>::max(), ' ');
          if (!infile)
            break;
        }

        const int maxLength = 1024;
        char buffer[maxLength];
        int idx = 0;

        while (infile) {
          char c = infile.get();
          if (!infile)
            break;
          if (c == ' ' || c == '\n' || c == '#') {
            buffer[idx] = '\0';
            // Parse "feature:value" pairs
            if (idx) {
              char* delim = strchr(buffer, ':');
              if (!delim)
                KATANA_DIE(
                    "unknown feature format: '", buffer,
                    "' on line: ", numNodes + 1);
              *delim = '\0';
              double value = strtod(delim + 1, NULL);
              if (value == 0.0) {
                ;  // pass
              } else if (phase == 0) {
                long feature = strtol(buffer, NULL, 10);
                maxFeature = std::max(maxFeature, feature);
                numEdges += 1;
              } else if (phase == 1) {
                p.incrementDegree(numNodes);
              } else {
                long feature = strtol(buffer, NULL, 10);
                edge_value_type data = value;
                edgeData.set(
                    p.addNeighbor(numNodes, feature + featureOffset), data);
              }
            }

            idx = 0;
          } else {
            buffer[idx++] = c;
            if (idx == maxLength)
              KATANA_DIE("token too long");
            continue;
          }
          if (c == '#') {
            skipLine(infile);
          }
          if (c == '#' || c == '\n') {
            break;
          }
        }

        numNodes += 1;
      }

      if (phase == 0) {
        featureOffset = numNodes;
        numNodes += maxFeature + 1;
        p.setNumNodes(numNodes);
        p.setNumEdges(numEdges);
        p.setSizeofEdgeData(EdgeData::size_of::value);
        edgeData.create(numEdges);
        p.phase1();
      } else if (phase == 1) {
        p.phase2();
      } else {
        edge_value_type* rawEdgeData = p.finish<edge_value_type>();
        if (EdgeData::has_value)
          std::uninitialized_copy(
              std::make_move_iterator(edgeData.begin()),
              std::make_move_iterator(edgeData.end()), rawEdgeData);
        numNodes += maxFeature + 1;
        p.toFile(outfilename);
        printStatus(numNodes, numEdges);
      }
    }
  }
};

int
main(int argc, char** argv) {
  kCommandLine = katana::Join(" ", argv, argv + argc);
  katana::SharedMemSys G;
  llvm::cl::ParseCommandLineOptions(
      argc, argv,
      "Converter for old graphs to gr formats for galois\n\n"
      "  For converting property graphs use graph-properties-convert\n");
  std::ios_base::sync_with_stdio(false);
  switch (convertMode) {
  case bipartitegr2bigpetsc:
    convert<Bipartitegr2Petsc<double, false>>();
    break;
  case bipartitegr2littlepetsc:
    convert<Bipartitegr2Petsc<double, true>>();
    break;
  case bipartitegr2sorteddegreegr:
    convert<BipartiteSortByDegree>();
    break;
  case dimacs2gr:
    convert<Dimacs2Gr>();
    break;
  case edgelist2gr:
    convert<Edgelist2Gr>();
    break;
  case csv2gr:
    convert<CSV2Gr>();
    break;
  case gr2biggr:
    convert<ToBigEndian>();
    break;
  case gr2binarypbbs32:
    convert<Gr2BinaryPbbs<uint32_t, uint32_t>>();
    break;
  case gr2binarypbbs64:
    convert<Gr2BinaryPbbs<uint32_t, uint64_t>>();
    break;
  case gr2bsml:
    convert<Gr2Bsml>();
    break;
  case gr2cgr:
    convert<Cleanup>();
    break;
  case gr2dimacs:
    convert<Gr2Dimacs>();
    break;
  case gr2adjacencylist:
    convert<Gr2Adjacencylist>();
    break;
  case gr2edgelist:
    convert<Gr2Edgelist>();
    break;
  case gr2edgelist1ind:
    convert<Gr2Edgelist1Ind>();
    break;
  case gr2linegr:
    convert<AddRing<true>>();
    break;
  case gr2lowdegreegr:
    convert<RemoveHighDegree>();
    break;
  case gr2mtx:
    convert<Gr2Mtx>();
    break;
  case gr2partdstgr:
    convert<PartitionByDestination>();
    break;
  case gr2partsrcgr:
    convert<PartitionBySource>();
    break;
  case gr2pbbs:
    convert<Gr2Pbbs>();
    break;
  case gr2pbbsedges:
    convert<Gr2Pbbsedges>();
    break;
  case gr2randgr:
    convert<RandomizeNodes>();
    break;
  case gr2randomweightgr:
    convert<RandomizeEdgeWeights>();
    break;
  case gr2ringgr:
    convert<AddRing<false>>();
    break;
  case gr2rmat:
    convert<Gr2Rmat<int32_t>>();
    break;
  case gr2metis:
    convert<Gr2Metis>();
    break;
  case gr2sgr:
    convert<MakeSymmetric>();
    break;
  case gr2sorteddegreegr:
    convert<SortByDegree>();
    break;
  case gr2sorteddstgr:
    convert<SortEdges<IdLess, false>>();
    break;
  case gr2sortedparentdegreegr:
    convert<SortByHighDegreeParent>();
    break;
  case gr2sortedweightgr:
    convert<SortEdges<WeightLess, true>>();
    break;
  case gr2sortedbfsgr:
    convert<SortByBFS>();
    break;
  case gr2streegr:
    convert<AddTree<true>>();
    break;
  case gr2tgr:
    convert<Transpose>();
    break;
  case gr2treegr:
    convert<AddTree<false>>();
    break;
  case gr2trigr:
    convert<MakeUnsymmetric>();
    break;
  case gr2totem:
    convert<Gr2Totem<IdLess>>();
    break;
  case gr2neo4j:
    convert<Gr2Neo4j>();
    break;
  case gr2kg:
    convert<Gr2Kg>();
    break;
  case mtx2gr:
    convert<Mtx2Gr>();
    break;
  case nodelist2gr:
    convert<Nodelist2Gr>();
    break;
  case pbbs2gr:
    convert<Pbbs2Gr>();
    break;
  case svmlight2gr:
    convert<Svmlight2Gr>();
    break;
  case edgelist2binary:
    convert<Edgelist2Binary>();
    break;
  default:
    abort();
  }
  return 0;
}
