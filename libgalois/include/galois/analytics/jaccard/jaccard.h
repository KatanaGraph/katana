#ifndef GALOIS_LIBGALOIS_GALOIS_ANALYTICS_JACCARD_JACCARD_H_
#define GALOIS_LIBGALOIS_GALOIS_ANALYTICS_JACCARD_JACCARD_H_

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
  JaccardPlan() : JaccardPlan(kCPU, kUnknown) {}
  JaccardPlan& operator=(const JaccardPlan&) = default;

  EdgeSorting edge_sorting() const { return edge_sorting_; }

  /// The graph's edge lists are not sorted; use an algorithm that handles that.
  static JaccardPlan Unsorted() { return {kCPU, kUnsorted}; }

  /// The graph's edge lists are sorted; optimize based on this.
  static JaccardPlan Sorted() { return {kCPU, kSorted}; }

  /// Automatically choose an algorithm.
  /// May either use the unsorted algorithm, or use an algorithm that attempts
  /// the sorted algorithm, but checks for out of order edges.
  static JaccardPlan Automatic() { return {}; }
};

/// The tag for the output property of Jaccard in PropertyGraphs.
using JaccardSimilarity = galois::PODProperty<double>;

// TODO: Do we need to support float output? (For large graphs that want to use
//  less memory, maybe)

/// Compute the Jaccard similarity between each node and compare_node. The
/// result is stored in a property named by output_property_name. The plan
/// controls the assumptions made about edge list ordering.
/// The property named output_property_name is created by this function and may
/// not exist before the call.
GALOIS_EXPORT Result<void> Jaccard(
    graphs::PropertyFileGraph* pfg, size_t compare_node,
    const std::string& output_property_name,
    JaccardPlan plan = JaccardPlan::Automatic());

}  // namespace galois::analytics

#endif
