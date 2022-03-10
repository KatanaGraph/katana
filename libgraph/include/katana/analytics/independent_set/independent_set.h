#ifndef KATANA_LIBGRAPH_KATANA_ANALYTICS_INDEPENDENTSET_INDEPENDENTSET_H_
#define KATANA_LIBGRAPH_KATANA_ANALYTICS_INDEPENDENTSET_INDEPENDENTSET_H_

#include <iostream>

#include "katana/Properties.h"
#include "katana/PropertyGraph.h"
#include "katana/analytics/Plan.h"

namespace katana::analytics {

/// A computational plan to for IndependentSet, specifying the algorithm and any parameters
/// associated with it.
class IndependentSetPlan : public Plan {
public:
  enum Algorithm {
    kSerial,
    kPull,
    // TODO(gill): These algorithms need locks and cautious operator.
    // kNondeterministic,
    // kDeterministicBase,
    kPriority,
    kEdgeTiledPriority
  };

private:
  Algorithm algorithm_;

  IndependentSetPlan(Architecture architecture, Algorithm algorithm)
      : Plan(architecture), algorithm_(algorithm) {}

public:
  IndependentSetPlan() : IndependentSetPlan(kCPU, kPriority) {}

  IndependentSetPlan& operator=(const IndependentSetPlan&) = default;

  Algorithm algorithm() const { return algorithm_; }

  static IndependentSetPlan Serial() { return {kCPU, kSerial}; }

  static IndependentSetPlan Pull() { return {kCPU, kPull}; }

  static IndependentSetPlan Priority() { return {kCPU, kPriority}; }

  static IndependentSetPlan EdgeTiledPriority() {
    return {kCPU, kEdgeTiledPriority};
  }

  static IndependentSetPlan FromAlgorithm(Algorithm algorithm) {
    return {kCPU, algorithm};
  }
};

/// Find a maximal (not the maximum) independent set in the graph and create an
/// indicator property that is true for elements of the independent set.
/// The graph must be symmetric.
/// The property named output_property_name is created by this function and may
/// not exist before the call. The created property has type uint8_t.
KATANA_EXPORT Result<void> IndependentSet(
    const std::shared_ptr<PropertyGraph>& pg,
    const std::string& output_property_name, katana::TxnContext* txn_ctx,
    IndependentSetPlan plan = {});

KATANA_EXPORT Result<void> IndependentSetAssertValid(
    const std::shared_ptr<PropertyGraph>& pg, const std::string& property_name);

struct KATANA_EXPORT IndependentSetStatistics {
  /// The number of nodes in the independent set.
  uint32_t cardinality;

  /// Print the statistics in a human readable form.
  void Print(std::ostream& os = std::cout) const;

  static katana::Result<IndependentSetStatistics> Compute(
      const std::shared_ptr<PropertyGraph>& pg,
      const std::string& property_name);
};

}  // namespace katana::analytics

#endif
