#ifndef KATANA_LIBGRAPH_KATANA_ANALYTICS_LOCALCLUSTERINGCOEFFICIENT_LOCALCLUSTERINGCOEFFICIENT_H_
#define KATANA_LIBGRAPH_KATANA_ANALYTICS_LOCALCLUSTERINGCOEFFICIENT_LOCALCLUSTERINGCOEFFICIENT_H_

#include "katana/PropertyGraph.h"
#include "katana/TypedPropertyGraph.h"
#include "katana/analytics/Plan.h"
#include "katana/analytics/Utils.h"

// API

namespace katana::analytics {

/// A computational plan to for computing Local
/// Clustering Coefficient of the nodes in the graph.
class LocalClusteringCoefficientPlan : public Plan {
public:
  enum Algorithm { kOrderedCountAtomics, kOrderedCountPerThread };

  enum Relabeling {
    kRelabel,
    kNoRelabel,
    kAutoRelabel,
  };

  static const Relabeling kDefaultRelabeling = kAutoRelabel;
  static const bool kDefaultEdgesSorted = false;

private:
  Algorithm algorithm_;
  bool edges_sorted_;
  Relabeling relabeling_;

  LocalClusteringCoefficientPlan(
      Architecture architecture, Algorithm algorithm, bool edges_sorted,
      Relabeling relabeling)
      : Plan(architecture),
        algorithm_(algorithm),
        edges_sorted_(edges_sorted),
        relabeling_(relabeling) {}

public:
  LocalClusteringCoefficientPlan()
      : LocalClusteringCoefficientPlan{
            kCPU, kOrderedCountPerThread, kDefaultEdgesSorted,
            kDefaultRelabeling} {}

  Algorithm algorithm() const { return algorithm_; }
  // TODO(amp): These parameters should be documented.
  bool edges_sorted() const { return edges_sorted_; }
  Relabeling relabeling() const { return relabeling_; }

  /**
   * An ordered count algorithm that sorts the nodes by degree before
   * execution. This has been found to give good performance. We implement the
   * ordered count algorithm from the following:
   * http://gap.cs.berkeley.edu/benchmark.html
   *
   * @param edges_sorted Are the edges of the graph already sorted.
   * @param relabeling Should the algorithm relabel the nodes.
   */
  static LocalClusteringCoefficientPlan OrderedCountAtomics(
      bool edges_sorted = kDefaultEdgesSorted,
      Relabeling relabeling = kDefaultRelabeling) {
    return {kCPU, kOrderedCountAtomics, edges_sorted, relabeling};
  }

  static LocalClusteringCoefficientPlan OrderedCountPerThread(
      bool edges_sorted = kDefaultEdgesSorted,
      Relabeling relabeling = kDefaultRelabeling) {
    return {kCPU, kOrderedCountPerThread, edges_sorted, relabeling};
  }
};

/**
 * Compute the local clustering coefficient for each node in the graph.
 * The graph must be symmetric!
 *
 * @param pg The graph to process.
 * @param output_property_name name of the output property
 * @param plan
 *
 * @warning This algorithm will reorder nodes and edges in the graph.
 */
KATANA_EXPORT Result<void> LocalClusteringCoefficient(
    const std::shared_ptr<katana::PropertyGraph>& pg,
    const std::string& output_property_name, katana::TxnContext* txn_ctx,
    LocalClusteringCoefficientPlan plan = {});

}  // namespace katana::analytics

#endif
