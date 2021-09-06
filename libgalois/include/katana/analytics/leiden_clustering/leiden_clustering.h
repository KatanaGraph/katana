#ifndef KATANA_LIBGALOIS_KATANA_ANALYTICS_LEIDENCLUSTERING_LEIDENCLUSTERING_H_
#define KATANA_LIBGALOIS_KATANA_ANALYTICS_LEIDENCLUSTERING_LEIDENCLUSTERING_H_

#include <iostream>

#include "katana/AtomicHelpers.h"
#include "katana/analytics/Plan.h"
#include "katana/analytics/Utils.h"

namespace katana::analytics {

/// A computational plan to for Leiden Clustering, specifying the algorithm and any
/// parameters associated with it.
class LeidenClusteringPlan : public Plan {
public:
  enum Algorithm {
    kDoAll,
    kDeterministic,
  };

  static const bool kDefaultEnableVF = false;
  static constexpr double kDefaultModularityThresholdPerRound = 0.01;
  static constexpr double kDefaultModularityThresholdTotal = 0.01;
  static const uint32_t kDefaultMaxIterations = 10;
  static const uint32_t kDefaultMinGraphSize = 100;
  static constexpr double kDefaultResolution = 1.0;
  static constexpr double kDefaultRandomness = 0.01;

  // Don't allow people to directly construct these, so as to have only one
  // consistent way to configure.
private:
  Algorithm algorithm_;
  bool enable_vf_;
  double modularity_threshold_per_round_;
  double modularity_threshold_total_;
  uint32_t max_iterations_;
  uint32_t min_graph_size_;
  double resolution_;
  double randomness_;

  LeidenClusteringPlan(
      Architecture architecture, Algorithm algorithm, bool enable_vf,
      double modularity_threshold_per_round, double modularity_threshold_total,
      uint32_t max_iterations, uint32_t min_graph_size, double resolution,
      double randomness)
      : Plan(architecture),
        algorithm_(algorithm),
        enable_vf_(enable_vf),
        modularity_threshold_per_round_(modularity_threshold_per_round),
        modularity_threshold_total_(modularity_threshold_total),
        max_iterations_(max_iterations),
        min_graph_size_(min_graph_size),
        resolution_(resolution),
        randomness_(randomness) {}

public:
  LeidenClusteringPlan()
      : LeidenClusteringPlan{
            kCPU,
            kDoAll,
            kDefaultEnableVF,
            kDefaultModularityThresholdPerRound,
            kDefaultModularityThresholdTotal,
            kDefaultMaxIterations,
            kDefaultMinGraphSize,
            kDefaultResolution,
            kDefaultRandomness} {}

  Algorithm algorithm() const { return algorithm_; }
  /// Enable vertex following optimization
  bool enable_vf() const { return enable_vf_; }
  /// Threshold for modularity gain per round.
  double modularity_threshold_per_round() const {
    return modularity_threshold_per_round_;
  }
  /// Threshold for overall modularity gain.
  double modularity_threshold_total() const {
    return modularity_threshold_total_;
  }
  /// Maximum number of iterations to execute.
  uint32_t max_iterations() const { return max_iterations_; }
  /// Minimum coarsened graph size
  uint32_t min_graph_size() const { return min_graph_size_; }

  /// Resolution for calculating the modularity
  double resolution() const { return resolution_; }

  /// Randomness for picking subcommunities
  double randomness() const { return randomness_; }

  /// Nondeterministic algorithm for louvain clustering
  /// usign katana do_all
  static LeidenClusteringPlan DoAll(
      bool enable_vf = kDefaultEnableVF,
      double modularity_threshold_per_round =
          kDefaultModularityThresholdPerRound,
      double modularity_threshold_total = kDefaultModularityThresholdTotal,
      uint32_t max_iterations = kDefaultMaxIterations,
      uint32_t min_graph_size = kDefaultMinGraphSize,
      double resolution = kDefaultResolution,
      double randomness = kDefaultRandomness) {
    return {
        kCPU,
        kDoAll,
        enable_vf,
        modularity_threshold_per_round,
        modularity_threshold_total,
        max_iterations,
        min_graph_size,
        resolution,
        randomness};
  }

  /// Deterministic algorithm for louvain clustering
  /// using delayed updates
  static LeidenClusteringPlan Deterministic(
      bool enable_vf = kDefaultEnableVF,
      double modularity_threshold_per_round =
          kDefaultModularityThresholdPerRound,
      double modularity_threshold_total = kDefaultModularityThresholdTotal,
      uint32_t max_iterations = kDefaultMaxIterations,
      uint32_t min_graph_size = kDefaultMinGraphSize,
      double resolution = kDefaultResolution,
      double randomness = kDefaultRandomness) {
    return {
        kCPU,
        kDeterministic,
        enable_vf,
        modularity_threshold_per_round,
        modularity_threshold_total,
        max_iterations,
        min_graph_size,
        resolution,
        randomness};
  }
};

/// Compute the Leiden Clustering for pg.
/// The edge weights are taken from the property named
/// edge_weight_property_name (which may be a 32- or 64-bit sign or unsigned
/// int), and the computed cluster IDs are stored in the property named
/// output_property_name (as uint32_t).
/// The property named output_property_name is created by this function and may
/// not exist before the call.
KATANA_EXPORT Result<void> LeidenClustering(
    PropertyGraph* pg, const std::string& edge_weight_property_name,
    const std::string& output_property_name, LeidenClusteringPlan plan = {});

KATANA_EXPORT Result<void> LeidenClusteringAssertValid(
    PropertyGraph* pg, const std::string& edge_weight_property_name,
    const std::string& output_property_name);

struct KATANA_EXPORT LeidenClusteringStatistics {
  /// Total number of unique clusters in the graph.
  uint64_t n_clusters;
  /// Total number of clusters with more than 1 node.
  uint64_t n_non_trivial_clusters;
  /// The number of nodes present in the largest cluster.
  uint64_t largest_cluster_size;
  /// The proportion of nodes present in the largest cluster.
  double largest_cluster_proportion;
  /// Leiden modularity of the graph
  double modularity;

  /// Print the statistics in a human readable form.
  void Print(std::ostream& os = std::cout) const;

  static katana::Result<LeidenClusteringStatistics> Compute(
      PropertyGraph* pg, const std::string& edge_weight_property_name,
      const std::string& output_property_name);
};

}  // namespace katana::analytics

#endif
