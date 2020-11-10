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

#ifndef LONESTAR_UTILS_H
#define LONESTAR_UTILS_H

#include <algorithm>
#include <random>
#include <vector>

#include <arrow/stl.h>
#include <boost/filesystem.hpp>

#include "galois/ErrorCode.h"
#include "galois/Result.h"
#include "galois/Traits.h"
#include "galois/graphs/PropertyGraph.h"

//! Used to pick random non-zero degree starting points for search algorithms
//! This code has been copied from GAP benchmark suite
//! (https://github.com/sbeamer/gapbs/blob/master/src/benchmark.h)
template <typename Graph>
class SourcePicker {
  static const uint32_t kRandSeed;
  std::mt19937 rng;
  std::uniform_int_distribution<typename Graph::Node> udist;
  const Graph& graph;

public:
  explicit SourcePicker(const Graph& g)
      : rng(kRandSeed), udist(0, g.size() - 1), graph(g) {}

  auto PickNext() {
    typename Graph::Node source;
    do {
      source = udist(rng);
    } while (
        std::distance(graph.edges(source).begin(), graph.edges(source).end()));
    return source;
  }
};
template <typename Graph>
const uint32_t SourcePicker<Graph>::kRandSeed = 27491095;

//! Used to determine if a graph has power-law degree distribution or not
//! by sampling some of the vertices in the graph randomly
//! This code has been copied from GAP benchmark suite
//! (https://github.com/sbeamer/gapbs/blob/master/src/tc.cc WorthRelabelling())
template <typename Graph>
bool
isApproximateDegreeDistributionPowerLaw(const Graph& graph) {
  uint32_t averageDegree = graph.num_edges() / graph.num_nodes();
  if (averageDegree < 10)
    return false;
  SourcePicker<Graph> sp(graph);
  uint32_t num_samples = 1000;
  if (num_samples > graph.size())
    num_samples = graph.size();
  uint32_t sample_total = 0;
  std::vector<uint32_t> samples(num_samples);
  for (uint32_t trial = 0; trial < num_samples; trial++) {
    typename Graph::Node node = sp.PickNext();
    samples[trial] =
        std::distance(graph.edges(node).begin(), graph.edges(node).end());
    sample_total += samples[trial];
  }
  std::sort(samples.begin(), samples.end());
  double sample_average = static_cast<double>(sample_total) / num_samples;
  double sample_median = samples[num_samples / 2];
  return sample_average / 1.3 > sample_median;
}

inline std::unique_ptr<galois::graphs::PropertyFileGraph>
MakeFileGraph(
    const std::string& rdg_name, const std::string& edge_property_name) {
  std::vector<std::string> edge_properties;
  std::vector<std::string> node_properties;
  if (!edge_property_name.empty())
    edge_properties.emplace_back(edge_property_name);

  auto pfg_result = galois::graphs::PropertyFileGraph::Make(
      rdg_name, node_properties, edge_properties);
  if (!pfg_result) {
    GALOIS_LOG_FATAL("cannot make graph: {}", pfg_result.error());
  }
  return std::move(pfg_result.value());
}

template <typename NodeProps>
inline galois::Result<void>
ConstructNodeProperties(galois::graphs::PropertyFileGraph* pfg) {
  auto res_table =
      galois::AllocateTable<NodeProps>(pfg->topology().num_nodes());
  if (!res_table) {
    GALOIS_LOG_FATAL(
        "failed to allocate node properties table: {}", res_table.error());
  }

  auto result = pfg->AddNodeProperties(res_table.value());
  if (!result) {
    GALOIS_LOG_FATAL("failed to add node properties: {}", result.error());
  }
  return galois::ResultSuccess();
}

template <typename EdgeProps>
inline galois::Result<void>
ConstructEdgeProperties(galois::graphs::PropertyFileGraph* pfg) {
  auto res_table =
      galois::AllocateTable<EdgeProps>(pfg->topology().num_edges());
  if (!res_table) {
    GALOIS_LOG_FATAL(
        "failed to allocate edge properties table: {}", res_table.error());
  }

  auto result = pfg->AddEdgeProperties(res_table.value());
  if (!result) {
    GALOIS_LOG_FATAL("failed to add edge properties: {}", result.error());
  }
  return galois::ResultSuccess();
}

template <typename T>
void
writeOutput(const std::string& outputDir, T* values, size_t length) {
  namespace fs = boost::filesystem;
  fs::path filename{outputDir};
  filename = filename.append("output");

  std::ofstream outputFile(filename.string().c_str());

  if (!outputFile) {
    GALOIS_LOG_FATAL("could not open file: {}", filename);
  }

  for (size_t i = 0; i < length; i++) {
    outputFile << i << " " << *(values++) << "\n";
  }

  if (!outputFile) {
    GALOIS_LOG_FATAL("failed to write file: {}", filename);
  }
}

#endif
