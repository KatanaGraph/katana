#ifndef KATANA_LIBGALOIS_KATANA_ANALYTICS_LOCALCLUSTERINGCOEFFICIENT_LOCALCLUSTERINGCOEFFICIENT_H_
#define KATANA_LIBGALOIS_KATANA_ANALYTICS_LOCALCLUSTERINGCOEFFICIENT_LOCALCLUSTERINGCOEFFICIENT_H_

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
  static const bool kDefaultEdgeSorted = false;

private:
  Algorithm algorithm_;
  Relabeling relabeling_;
  bool edges_sorted_;

  LocalClusteringCoefficientPlan(
      Architecture architecture, Algorithm algorithm, bool edges_sorted,
      Relabeling relabeling)
      : Plan(architecture),
        algorithm_(algorithm),
        relabeling_(relabeling),
        edges_sorted_(edges_sorted) {}

public:
  LocalClusteringCoefficientPlan()
      : LocalClusteringCoefficientPlan{
            kCPU, kOrderedCountPerThread, kDefaultEdgeSorted,
            kDefaultRelabeling} {}

  Algorithm algorithm() const { return algorithm_; }
  // TODO(amp): These parameters should be documented.
  Relabeling relabeling() const { return relabeling_; }
  bool edges_sorted() const { return edges_sorted_; }

  /**
   * An ordered count algorithm that sorts the nodes by degree before
   * execution. This has been found to give good performance. We implement the
   * ordered count algorithm from the following:
   * http://gap.cs.berkeley.edu/benchmark.html
   *
   * @param edges_sorted Are the edges of the graph already sorted.
   * @param relabeling Should the algorithm relabel the nodes.
   */
  static LocalClusteringCoefficientPlan LocalClusteringCoefficientAtomics(
      bool edges_sorted = kDefaultEdgeSorted,
      Relabeling relabeling = kDefaultRelabeling) {
    return {kCPU, kOrderedCountAtomics, edges_sorted, relabeling};
  }

  static LocalClusteringCoefficientPlan LocalClusteringCoefficientPerThread(
      bool edges_sorted = kDefaultEdgeSorted,
      Relabeling relabeling = kDefaultRelabeling) {
    return {kCPU, kOrderedCountPerThread, edges_sorted, relabeling};
  }
};

// TODO(amp): The doc string was not updated.
/**
 * Count the total number of triangles in the graph. The graph must be
 * symmetric!
 *
 * This algorithm copies the graph internally.
 *
 * @param pg The graph to process.
 * @param output_property_name name of the output property
 * @param plan
 */
KATANA_EXPORT Result<void> LocalClusteringCoefficient(
    PropertyGraph* pg, const std::string& output_property_name,
    LocalClusteringCoefficientPlan plan = {});

}  // namespace katana::analytics

#endif
