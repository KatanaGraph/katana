#ifndef GALOIS_LIBGALOIS_GALOIS_ANALYTICS_JACCARD_JACCARD_H_
#define GALOIS_LIBGALOIS_GALOIS_ANALYTICS_JACCARD_JACCARD_H_

#include <iostream>

#include "galois/Properties.h"
#include "galois/analytics/Plan.h"
#include "galois/graphs/PropertyFileGraph.h"
#include "galois/graphs/PropertyGraph.h"

namespace galois::analytics {

/// A computational plan to for Jaccard, specifying the algorithm and any parameters
/// associated with it.
class JaccardPlan : public Plan {
public:
  enum EdgeSorting {
    /// The edges may be sorted, but may not.
    /// Jaccard may optimistically use a sorted algorithm and fail over to an
    /// unsorted one if unsorted edges are detected.
    kUnknown,
    /// The edges are known to be sorted by destination.
    /// Use faster sorted intersection algorithm.
    kSorted,
    /// The edges are known to be unsorted.
    /// Use slower hash-table intersection algorithm.
    kUnsorted,
  };

private:
  EdgeSorting edge_sorting_;

  JaccardPlan(Architecture architecture, EdgeSorting edge_sorting)
      : Plan(architecture), edge_sorting_(edge_sorting) {}

public:
  /// Automatically choose an algorithm.
  /// May either use the unsorted algorithm, or use an algorithm that attempts
  /// the sorted algorithm, but checks for out of order edges.
  JaccardPlan() : JaccardPlan(kCPU, kUnknown) {}

  JaccardPlan& operator=(const JaccardPlan&) = default;

  EdgeSorting edge_sorting() const { return edge_sorting_; }

  /// The graph's edge lists are not sorted; use an algorithm that handles that.
  static JaccardPlan Unsorted() { return {kCPU, kUnsorted}; }

  /// The graph's edge lists are sorted; optimize based on this.
  static JaccardPlan Sorted() { return {kCPU, kSorted}; }
};

/// The tag for the output property of Jaccard in PropertyGraphs.
using JaccardSimilarity = galois::PODProperty<double>;

/// Compute the Jaccard similarity between each node and compare_node. The
/// result is stored in a property named by output_property_name. The plan
/// controls the assumptions made about edge list ordering.
/// The property named output_property_name is created by this function and may
/// not exist before the call.
GALOIS_EXPORT Result<void> Jaccard(
    graphs::PropertyFileGraph* pfg, uint32_t compare_node,
    const std::string& output_property_name, JaccardPlan plan = {});

GALOIS_EXPORT Result<void> JaccardAssertValid(
    graphs::PropertyFileGraph* pfg, uint32_t compare_node,
    const std::string& property_name);

struct GALOIS_EXPORT JaccardStatistics {
  /// The maximum similarity excluding the comparison node.
  double max_similarity;
  /// The minimum similarity
  double min_similarity;
  /// The average similarity excluding the comparison node.
  double average_similarity;

  /// Print the statistics in a human readable form.
  void Print(std::ostream& os = std::cout);

  static galois::Result<JaccardStatistics> Compute(
      galois::graphs::PropertyFileGraph* pfg, uint32_t compare_node,
      const std::string& property_name);
};

}  // namespace galois::analytics

#endif
