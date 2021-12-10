#ifndef KATANA_LIBGRAPH_KATANA_ANALYTICS_KCORE_KCORE_H_
#define KATANA_LIBGRAPH_KATANA_ANALYTICS_KCORE_KCORE_H_

#include <iostream>

#include <katana/analytics/Plan.h>

#include "katana/AtomicHelpers.h"
#include "katana/analytics/Utils.h"

// API

namespace katana::analytics {

/// A computational plan to for k-core, specifying the algorithm and any
/// parameters associated with it.
class KCorePlan : public Plan {
public:
  /// Algorithm selectors for KCore
  enum Algorithm { kSynchronous, kAsynchronous };

  // Don't allow people to directly construct these, so as to have only one
  // consistent way to configure.
private:
  Algorithm algorithm_;

  KCorePlan(Architecture architecture, Algorithm algorithm)
      : Plan(architecture), algorithm_(algorithm) {}

public:
  // kChunkSize is a fixed const int (default value: 64)
  static const int kChunkSize;

  KCorePlan() : KCorePlan{kCPU, kSynchronous} {}

  Algorithm algorithm() const { return algorithm_; }

  /// Synchronous k-core algorithm.
  static KCorePlan Synchronous() { return {kCPU, kSynchronous}; }

  /// Asynchronous k-core algorithm.
  static KCorePlan Asynchronous() { return {kCPU, kAsynchronous}; }
};

/// Compute the k-core for pg. The pg must be symmetric.
/// The algorithm, and k_core_number parameters can be specified,
/// but have reasonable defaults.
/// The property named output_property_name is created by this function and may
/// not exist before the call.
KATANA_EXPORT Result<void> KCore(
    PropertyGraph* pg, uint32_t k_core_number,
    const std::string& output_property_name, tsuba::TxnContext* txn_ctx,
    KCorePlan plan = KCorePlan());

KATANA_EXPORT Result<void> KCoreAssertValid(
    PropertyGraph* pg, uint32_t k_core_number,
    const std::string& property_name);

struct KATANA_EXPORT KCoreStatistics {
  /// Total number of node left in the core.
  uint64_t number_of_nodes_in_kcore;

  /// Print the statistics in a human readable form.
  void Print(std::ostream& os = std::cout) const;

  static katana::Result<KCoreStatistics> Compute(
      katana::PropertyGraph* pg, uint32_t k_core_number,
      const std::string& property_name);
};

}  // namespace katana::analytics
#endif
