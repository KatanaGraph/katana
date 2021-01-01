#ifndef GALOIS_LIBGALOIS_GALOIS_ANALYTICS_UTILS_H_
#define GALOIS_LIBGALOIS_GALOIS_ANALYTICS_UTILS_H_

#include <algorithm>
#include <random>

#include "galois/ErrorCode.h"
#include "galois/Result.h"
#include "galois/graphs/PropertyGraph.h"

namespace galois::analytics {

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

template <typename Props>
std::vector<std::string>
DefaultPropertyNames() {
  auto num_tuple_elem = std::tuple_size<Props>::value;
  std::vector<std::string> names(num_tuple_elem);

  for (size_t i = 0; i < names.size(); ++i) {
    names[i] = "Column_" + std::to_string(i);
  }
  return names;
}

template <typename NodeProps>
inline galois::Result<void>
ConstructNodeProperties(
    galois::graphs::PropertyFileGraph* pfg,
    const std::vector<std::string>& names = DefaultPropertyNames<NodeProps>()) {
  auto res_table =
      galois::AllocateTable<NodeProps>(pfg->topology().num_nodes(), names);
  if (!res_table) {
    return res_table.error();
  }

  return pfg->AddNodeProperties(res_table.value());
}

template <typename EdgeProps>
inline galois::Result<void>
ConstructEdgeProperties(
    galois::graphs::PropertyFileGraph* pfg,
    const std::vector<std::string>& names = DefaultPropertyNames<EdgeProps>()) {
  auto res_table =
      galois::AllocateTable<EdgeProps>(pfg->topology().num_edges(), names);
  if (!res_table) {
    return res_table.error();
  }

  return pfg->AddEdgeProperties(res_table.value());
}

}  // namespace galois::analytics

#endif
