#ifndef KATANA_LIBGALOIS_KATANA_ANALYTICS_SUBGRAPHEXTRACTION_SUBGRAPHEXTRACTION_H_
#define KATANA_LIBGALOIS_KATANA_ANALYTICS_SUBGRAPHEXTRACTION_SUBGRAPHEXTRACTION_H_

#include "katana/PropertyGraph.h"
#include "katana/analytics/Plan.h"

// API

namespace katana::analytics {

/// A computational plan to for SubGraph Extraction.
class SubGraphExtractionPlan : public Plan {
public:
  enum Algorithm {
    kNodeSet,
  };

private:
  Algorithm algorithm_;

  SubGraphExtractionPlan(Architecture architecture, Algorithm algorithm)
      : Plan(architecture), algorithm_(algorithm) {}

public:
  SubGraphExtractionPlan() : SubGraphExtractionPlan{kCPU, kNodeSet} {}

  Algorithm algorithm() const { return algorithm_; }

  /**
   * The node-set algorithm:
   *    Given a set of node ids, this algorithm constructs a new sub-graph
   *    connecting all the nodes in the set along with the properties requested.
   */
  static SubGraphExtractionPlan NodeSet() { return {kCPU, kNodeSet}; }
};

/**
 * Construct a new sub-graph from the original graph.
 *
 * By default only topology of the sub-graph is constructed.
 * The new sub-graph is independent of the original graph.
 *
 * @param pg The graph to process.
 * @param node_vec Set of node ids
 * @param plan
 */
KATANA_EXPORT katana::Result<std::unique_ptr<katana::PropertyGraph>>
SubGraphExtraction(
    katana::PropertyGraph* pg, const std::vector<uint32_t>& node_vec,
    SubGraphExtractionPlan plan = {});
// const std::vector<std::string>& node_properties_to_copy, const std::vector<std::string>& edge_properties_to_copy);

}  // namespace katana::analytics

#endif
