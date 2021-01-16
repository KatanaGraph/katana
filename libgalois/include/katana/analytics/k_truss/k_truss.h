#ifndef KATANA_LIBGALOIS_KATANA_ANALYTICS_KTRUSS_KTRUSS_H_
#define KATANA_LIBGALOIS_KATANA_ANALYTICS_KTRUSS_KTRUSS_H_

#include <iostream>

#include <katana/analytics/Plan.h>

#include "katana/analytics/Utils.h"

// API

namespace katana::analytics {

/// A computational plan to for k-truss, specifying the algorithm and any
/// parameters associated with it.
class KTrussPlan : Plan {
public:
  /// Algorithm selectors for KCore
  enum Algorithm { kBsp, kBspJacobi, kBspCoreThenTruss };

  // Don't allow people to directly construct these, so as to have only one
  // consistent way to configure.
private:
  Algorithm algorithm_;

  KTrussPlan(Architecture architecture, Algorithm algorithm)
      : Plan(architecture), algorithm_(algorithm) {}

public:
  KTrussPlan() : KTrussPlan{kCPU, kBsp} {}

  Algorithm algorithm() const { return algorithm_; }

  /// Bulk-synchronous parallel algorithm.
  static KTrussPlan Bsp() { return {kCPU, kBsp}; }

  /// Bulk-synchronous parallel with separated edge removal algorithm.
  static KTrussPlan BspJacobi() { return {kCPU, kBspJacobi}; }

  /// Compute k-1 core and then k-truss algorithm.
  static KTrussPlan BspCoreThenTruss() { return {kCPU, kBspCoreThenTruss}; }
};

/// Compute the k-truss for pfg. The pfg is expected to be
/// symmetric.
/// The algorithm parameters can be specified,
/// but have reasonable defaults.
/// The property named output_property_name is created by this function and may
/// not exist before the call.
KATANA_EXPORT Result<void> KTruss(
    PropertyFileGraph* pfg, uint32_t k_truss_number,
    const std::string& output_property_name, KTrussPlan plan = KTrussPlan());

KATANA_EXPORT Result<void> KTrussAssertValid(
    [[maybe_unused]] PropertyFileGraph* pfg,
    [[maybe_unused]] uint32_t k_truss_number,
    [[maybe_unused]] const std::string& property_name);

struct KATANA_EXPORT KTrussStatistics {
  /// k-truss number
  uint32_t k_truss_number;
  /// Total number of edges left in the truss.
  uint64_t number_of_edges_left;

  /// Print the statistics in a human readable form.
  void Print(std::ostream& os = std::cout);

  static katana::Result<KTrussStatistics> Compute(
      katana::PropertyFileGraph* pfg, uint32_t k_truss_number,
      const std::string& property_name);
};

}  // namespace katana::analytics
#endif
