#ifndef KATANA_LIBGALOIS_KATANA_ANALYTICS_LOUVAINCLUSTERING_LOUVAINCLUSTERING_H_
#define KATANA_LIBGALOIS_KATANA_ANALYTICS_LOUVAINCLUSTERING_LOUVAINCLUSTERING_H_

#include <iostream>

#include "katana/AtomicHelpers.h"
#include "katana/analytics/Plan.h"
#include "katana/analytics/Utils.h"

namespace katana::analytics {

/// A computational plan to for Louvain Clustering, specifying the algorithm and any
/// parameters associated with it.
class LouvainClusteringPlan : public Plan {
public:
  /// Algorithm selectors for Single-Source Shortest Path
  enum Algorithm {
    kDoAll,
  };

  static const bool kEnableVF = false;
  static constexpr double kModularityThresholdPerRound = 0.01;
  static constexpr double kModularityThresholdTotal = 0.01;
  static const uint32_t kMaxIterations = 10;
  static const uint32_t kMinGraphSize = 100;

  // Don't allow people to directly construct these, so as to have only one
  // consistent way to configure.
private:
  Algorithm algorithm_;
  //Flag to enable vertex following optimization.
  bool enable_vf_;
  //Threshold for modularity gain per round.
  double modularity_threshold_per_round_;
  //Threshold for overall modularity gain.
  double modularity_threshold_total_;
  //Maximum number of iterations to execute.
  uint32_t max_iterations_;
  //Minimum coarsened graph size
  uint32_t min_graph_size_;

  LouvainClusteringPlan(
      Architecture architecture, Algorithm algorithm, bool enable_vf,
      double modularity_threshold_per_round, double modularity_threshold_total,
      uint32_t max_iterations, uint32_t min_graph_size)
      : Plan(architecture),
        algorithm_(algorithm),
        enable_vf_(enable_vf),
        modularity_threshold_per_round_(modularity_threshold_per_round),
        modularity_threshold_total_(modularity_threshold_total),
        max_iterations_(max_iterations),
        min_graph_size_(min_graph_size) {}

public:
  LouvainClusteringPlan()
      : LouvainClusteringPlan{kCPU, kDoAll, false, 0.01, 0.01, 10, 100} {}

  Algorithm algorithm() const { return algorithm_; }
  bool is_enable_vf() const { return enable_vf_; }
  double modularity_threshold_per_round() const {
    return modularity_threshold_per_round_;
  }
  double modularity_threshold_total() const {
    return modularity_threshold_total_;
  }
  uint32_t max_iterations() const { return max_iterations_; }
  uint32_t min_graph_size() const { return min_graph_size_; }

  static LouvainClusteringPlan DoAll(
      bool enable_vf = kEnableVF,
      double modularity_threshold_per_round = kModularityThresholdPerRound,
      double modularity_threshold_total = kModularityThresholdTotal,
      uint32_t max_iterations = kMaxIterations,
      uint32_t min_graph_size = kMinGraphSize) {
    return {
        kCPU,
        kDoAll,
        enable_vf,
        modularity_threshold_per_round,
        modularity_threshold_total,
        max_iterations,
        min_graph_size};
  }
};

/// Compute the Louvain Clustering for pg.
/// The edge weights are taken from the property named
/// edge_weight_property_name (which may be a 32- or 64-bit sign or unsigned
/// int), and the computed cluster ids are stored in the property named
/// output_property_name (as uint32_t).
/// The property named output_property_name is created by this function and may
/// not exist before the call.
KATANA_EXPORT Result<void> LouvainClustering(
    PropertyGraph* pg, const std::string& edge_weight_property_name,
    const std::string& output_property_name, LouvainClusteringPlan plan = {});

KATANA_EXPORT Result<void> LouvainClusteringAssertValid(
    PropertyGraph* pg, const std::string& edge_weight_property_name,
    const std::string& output_property_name);

struct KATANA_EXPORT LouvainClusteringStatistics {
  /// Total number of unique clusters in the graph.
  uint64_t n_clusters;
  /// Total number of clusters with more than 1 node.
  uint64_t n_non_trivial_clusters;
  /// The number of nodes present in the largest cluster.
  uint64_t largest_cluster_size;
  /// The proportion of nodes present in the largest cluster.
  double largest_cluster_proportion;
  /// Louvain modularity of the graph
  double modularity;

  /// Print the statistics in a human readable form.
  void Print(std::ostream& os = std::cout) const;

  static katana::Result<LouvainClusteringStatistics> Compute(
      PropertyGraph* pg, const std::string& edge_weight_property_name,
      const std::string& output_property_name);
};

}  // namespace katana::analytics

#endif
