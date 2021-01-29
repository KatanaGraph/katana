#ifndef KATANA_LIBGALOIS_KATANA_ANALYTICS_PAGERANK_PAGERANK_H_
#define KATANA_LIBGALOIS_KATANA_ANALYTICS_PAGERANK_PAGERANK_H_

#include <iostream>

#include "katana/Properties.h"
#include "katana/PropertyGraph.h"
#include "katana/analytics/Plan.h"

namespace katana::analytics {

/// A computational plan to for Page Rank, specifying the algorithm and any
/// parameters associated with it.
class PagerankPlan : public Plan {
public:
  enum Algorithm {
    kPullTopological,
    kPullResidual,
    kPushSynchronous,
    kPushAsynchronous,
  };

private:
  Algorithm algorithm_;
  float tolerance_;
  unsigned int max_iterations_;
  float alpha_;

public:
  PagerankPlan(
      Architecture architecture, Algorithm algorithm, float tolerance,
      unsigned int max_iterations, float alpha)
      : Plan(architecture),
        algorithm_(algorithm),
        tolerance_(tolerance),
        max_iterations_(max_iterations),
        alpha_(alpha) {}

  constexpr static const unsigned kChunkSize = 16U;

  /// Automatically choose an algorithm.
  PagerankPlan() : PagerankPlan(kCPU, kPushAsynchronous, 1.0e-3, 0, 0.85) {}

  PagerankPlan& operator=(const PagerankPlan&) = default;

  Algorithm algorithm() const { return algorithm_; }
  float tolerance() const { return tolerance_; }
  unsigned int max_iterations() const { return max_iterations_; }
  float alpha() const { return alpha_; }
  float initial_residual() const { return 1 - alpha_; }

  /// Topological pull algorithm
  ///
  /// The graph must be transposed to use this algorithm.
  static PagerankPlan PullTopological(
      float tolerance = 1.0e-3, unsigned int max_iterations = 1000,
      float alpha = 0.85) {
    return {kCPU, kPullTopological, tolerance, max_iterations, alpha};
  }

  /// Delta-residual pull algorithm
  ///
  /// The graph must be transposed to use this algorithm.
  static PagerankPlan PullResidual(
      float tolerance = 1.0e-3, unsigned int max_iterations = 1000,
      float alpha = 0.85) {
    return {kCPU, kPullResidual, tolerance, max_iterations, alpha};
  }

  /// Asynchronous push algorithm
  ///
  /// This implementation is based on the Push-based PageRank computation
  /// (Algorithm 4) as described in the PageRank Europar 2015 paper.
  ///
  /// WHANG, Joyce Jiyoung, et al. Scalable data-driven pagerank: Algorithms,
  /// system issues, and lessons learned. In: European Conference on Parallel
  /// Processing. Springer, Berlin, Heidelberg, 2015. p. 438-450.
  static PagerankPlan PushAsynchronous(
      float tolerance = 1.0e-3, float alpha = 0.85) {
    return {kCPU, kPushAsynchronous, tolerance, 0, alpha};
  }

  /// Synchronous push algorithm
  ///
  /// This implementation is based on the Push-based PageRank computation
  /// (Algorithm 4) as described in the PageRank Europar 2015 paper.
  ///
  /// WHANG, Joyce Jiyoung, et al. Scalable data-driven pagerank: Algorithms,
  /// system issues, and lessons learned. In: European Conference on Parallel
  /// Processing. Springer, Berlin, Heidelberg, 2015. p. 438-450.
  static PagerankPlan PushSynchronous(
      float tolerance = 1.0e-3, unsigned int max_iterations = 1000,
      float alpha = 0.85) {
    return {kCPU, kPushSynchronous, tolerance, max_iterations, alpha};
  }
};

/// Compute the Page Rank of each node in the graph.
/// The property named output_property_name is created by this function and may
/// not exist before the call.
KATANA_EXPORT Result<void> Pagerank(
    PropertyGraph* pg, const std::string& output_property_name,
    PagerankPlan plan = {});

KATANA_EXPORT Result<void> PagerankAssertValid(
    PropertyGraph* pg, const std::string& property_name);

struct KATANA_EXPORT PagerankStatistics {
  /// The maximum similarity excluding the comparison node.
  float max_rank;
  /// The minimum similarity
  float min_rank;
  /// The average similarity excluding the comparison node.
  float average_rank;

  /// Print the statistics in a human readable form.
  void Print(std::ostream& os = std::cout);

  static katana::Result<PagerankStatistics> Compute(
      katana::PropertyGraph* pg, const std::string& property_name);
};

}  // namespace katana::analytics

#endif
