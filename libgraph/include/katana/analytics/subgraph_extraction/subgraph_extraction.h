#ifndef KATANA_LIBGRAPH_KATANA_ANALYTICS_SUBGRAPHEXTRACTION_SUBGRAPHEXTRACTION_H_
#define KATANA_LIBGRAPH_KATANA_ANALYTICS_SUBGRAPHEXTRACTION_SUBGRAPHEXTRACTION_H_

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

  // TODO(amp): This algorithm defines the semantics of the call. If there were
  //  an algorithm that, for instance, took a list of edges, that would need to
  //  be a different function, not just a different plan, since it takes
  //  semantically different arguments. I do think this should have a plan, even
  //  if there is only one concrete algorithm, but it should be defined and
  //  documented in terms of the concrete algorithm, not the semantics of the
  //  function (which is described well below).

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
 * @param node_vec Set of node IDs
 * @param plan
 */
KATANA_EXPORT katana::Result<std::unique_ptr<katana::PropertyGraph>>
SubGraphExtraction(
    const std::shared_ptr<katana::PropertyGraph>& pg,
    const std::vector<katana::PropertyGraph::Node>& node_vec,
    SubGraphExtractionPlan plan = {});
// const std::vector<std::string>& node_properties_to_copy, const std::vector<std::string>& edge_properties_to_copy);

}  // namespace katana::analytics

#endif
