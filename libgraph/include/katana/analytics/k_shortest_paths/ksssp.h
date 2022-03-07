#ifndef KATANA_LIBGRAPH_KATANA_ANALYTICS_KSHORTESTPATHS_KSSSP_H_
#define KATANA_LIBGRAPH_KATANA_ANALYTICS_KSHORTESTPATHS_KSSSP_H_

#include <iostream>

#include "katana/AtomicHelpers.h"
#include "katana/analytics/Plan.h"
#include "katana/analytics/Utils.h"

namespace katana::analytics {

/// A computational plan for KSSSP, specifying the algorithm,
/// path reachability, and any parametrs associated with it.
class KssspPlan : public Plan {
public:
  /// Algorithm selectros for K Shortest Paths
  enum Algorithm {
    kDeltaTile,
    kDeltaStep,
    kDeltaStepBarrier,
  };

  /// Specifices algorithm used for path reachability
  enum Reachability {
    asyncLevel,
    syncLevel,
  };

  static const Reachability kDefaultReach = syncLevel;
  static const int kDefaultDelta = 13;
  static const int kDefaultEdgeTileSize = 512;

  // Don't allow people to directly construct these, so as to have only one
  // consistent way to configure.
private:
  Algorithm algorithm_;
  Reachability reachability_;
  unsigned delta_;
  ptrdiff_t edge_tile_size_;

  KssspPlan(
      Architecture architecture, Algorithm algorithm, Reachability reachability,
      unsigned delta, ptrdiff_t edge_tile_size)
      : Plan(architecture),
        algorithm_(algorithm),
        reachability_(reachability),
        delta_(delta),
        edge_tile_size_(edge_tile_size) {}

public:
  KssspPlan() : KssspPlan{kCPU, kDeltaTile, kDefaultReach, 0, 0} {}

  KssspPlan(const katana::PropertyGraph* pg) : Plan(kCPU) {
    bool isPowerLaw = IsApproximateDegreeDistributionPowerLaw(*pg);
    if (isPowerLaw) {
      *this = DeltaStep();
    } else {
      *this = DeltaStepBarrier();
    }
  }

  Algorithm algorithm() const { return algorithm_; }
  Reachability reachability() const { return reachability_; }

  /// The exponent of the delta step size (2 based). A delta of 4 will produce a real delta step size of 16.
  unsigned delta() const { return delta_; }
  ptrdiff_t edge_tile_size() const { return edge_tile_size_; }

  static KssspPlan DeltaTile(
      Reachability reachability = kDefaultReach, unsigned delta = kDefaultDelta,
      ptrdiff_t edge_tile_size = kDefaultEdgeTileSize) {
    return {kCPU, kDeltaTile, reachability, delta, edge_tile_size};
  }

  static KssspPlan DeltaStep(
      Reachability reachability = kDefaultReach,
      unsigned delta = kDefaultDelta) {
    return {kCPU, kDeltaStep, reachability, delta, 0};
  }

  static KssspPlan DeltaStepBarrier(
      Reachability reachability = kDefaultReach,
      unsigned delta = kDefaultDelta) {
    return {kCPU, kDeltaStepBarrier, reachability, delta, 0};
  }
};

/// Compute the K Shortest Path for pg starting from start_node.
/// The algorithm and delta stepping
/// parameter can be specified, but have reasonable defaults.
KATANA_EXPORT Result<std::shared_ptr<arrow::Table>> Ksssp(
    katana::PropertyGraph* pg, const std::string& edge_weight_property_name,
    size_t start_node, size_t report_node, size_t num_paths,
    const bool& is_symmetric, katana::TxnContext* txn_ctx, KssspPlan plan = {});

/// TODO: Add KssspAssertValid(?)

struct KATANA_EXPORT KssspStatistics {
  std::vector<std::vector<<uint64_t>> paths;

  void Print(std::ostream& os = std::cout) const;

  static katana::Result<KssspStatistics> Compute(
      std::shared_ptr<arrow::Table> table, size_t report_node);
};

}  // namespace katana::analytics

#endif
