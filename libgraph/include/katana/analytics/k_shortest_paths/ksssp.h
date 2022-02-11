#ifndef KATANA_LIBGRAPH_KATANA_ANALYTICS_KSHORTESTPATHS_KSSSP_H_
#define KATANA_LIBGRAPH_KATANA_ANALYTICS_KSHORTESTPATHS_KSSSP_H_

#include "katana/analytics/sssp/sssp.h"

namespace katana::analytics {
using kSsspPlan = SsspPlan;

/// Specifies algorithm used to check path reachability
class AlgoReachability {
public:
  /// Algorithm selector for K-Shortest Path
  enum Algorithm {
    asyncLevel, 
    syncLevel
  };

private:
  Algorithm algorithm_;

  AlgoReachability(
      Algorithm algorithm)
      : algorithm_(algorithm) {}

public:
  AlgoReachability() : AlgoReachability{syncLevel} {}

  Algorithm algorithm() const { return algorithm_; }

  static AlgoReachability AsyncLevel() {
    return {asyncLevel};
  }

  static AlgoReachability SyncLevel() {
    return {syncLevel};
  }
};

/// Compute the K Shortest Path for pg starting from start_node.
/// The algorithm and delta stepping
/// parameter can be specified, but have reasonable defaults.
KATANA_EXPORT Result<void> Ksssp(
    katana::PropertyGraph* pg, const std::string& edge_weight_property_name,
    size_t start_node, size_t report_node, size_t num_paths, 
    const bool& is_symmetric, katana::TxnContext* txn_ctx, 
    AlgoReachability algo_reachability, kSsspPlan plan = {});
}  // namespace katana::analytics

#endif
