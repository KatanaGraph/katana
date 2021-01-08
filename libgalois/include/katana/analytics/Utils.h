#ifndef KATANA_LIBGALOIS_KATANA_ANALYTICS_UTILS_H_
#define KATANA_LIBGALOIS_KATANA_ANALYTICS_UTILS_H_

#include <algorithm>
#include <random>

#include "katana/ErrorCode.h"
#include "katana/PropertyGraph.h"
#include "katana/Result.h"

namespace katana::analytics {

// TODO(amp): This file should be disbanded and it's functions moved to
//  PropertyFileGraph.h or other more specific places.

//! Used to pick random non-zero degree starting points for search algorithms
//! This code has been copied from GAP benchmark suite
//! (https://github.com/sbeamer/gapbs/blob/master/src/benchmark.h)
class KATANA_EXPORT SourcePicker {
  const PropertyFileGraph& graph;

public:
  explicit SourcePicker(const PropertyFileGraph& g) : graph(g) {}

  uint32_t PickNext();
};

//! Used to determine if a graph has power-law degree distribution or not
//! by sampling some of the vertices in the graph randomly
//! This code has been copied from GAP benchmark suite
//! (https://github.com/sbeamer/gapbs/blob/master/src/tc.cc WorthRelabelling())
KATANA_EXPORT bool IsApproximateDegreeDistributionPowerLaw(
    const PropertyFileGraph& graph);

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
inline katana::Result<void>
ConstructNodeProperties(
    PropertyFileGraph* pfg,
    const std::vector<std::string>& names = DefaultPropertyNames<NodeProps>()) {
  auto res_table = katana::AllocateTable<NodeProps>(pfg->num_nodes(), names);
  if (!res_table) {
    return res_table.error();
  }

  return pfg->AddNodeProperties(res_table.value());
}

template <typename EdgeProps>
inline katana::Result<void>
ConstructEdgeProperties(
    PropertyFileGraph* pfg,
    const std::vector<std::string>& names = DefaultPropertyNames<EdgeProps>()) {
  auto res_table =
      katana::AllocateTable<EdgeProps>(pfg->topology().num_edges(), names);
  if (!res_table) {
    return res_table.error();
  }

  return pfg->AddEdgeProperties(res_table.value());
}

}  // namespace katana::analytics

#endif
