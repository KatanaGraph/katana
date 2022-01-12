#include "katana/SharedMemSys.h"
#include "katana/TopologyGeneration.h"
#include "katana/analytics/cdlp/cdlp.h"

using namespace katana::analytics;

void
RunCdlp(
    std::unique_ptr<katana::PropertyGraph>&& pg,
    const bool& is_symmetric, const CdlpStatistics cdlp_expected_statistics) noexcept {
  using Plan = CdlpPlan;
  Plan plan = Plan::Synchronous();
  const std::string property_name = "community";

  katana::TxnContext txn_ctx;
  auto cdlp = Cdlp(pg.get(), property_name, 10, &txn_ctx, is_symmetric, plan);
  KATANA_LOG_VASSERT(cdlp, " CDLP failed and returned error {}", cdlp.error());

  auto stats_result = CdlpStatistics::Compute(pg.get(), property_name);
  KATANA_LOG_VASSERT(
      stats_result, "Failed to compute Cdlp statistics: {}",
      stats_result.error());

  CdlpStatistics stats = stats_result.value();
  KATANA_LOG_VASSERT(
      stats.total_communities == cdlp_expected_statistics.total_communities,
      "Wrong total number of communities. Found: {}, Expected: {}",
      stats.total_communities, cdlp_expected_statistics.total_communities);

  KATANA_LOG_VASSERT(
      stats.total_non_trivial_communities ==
          cdlp_expected_statistics.total_non_trivial_communities,
      "Wrong total number of non-trivial communities. Found: {}, Expected: {}",
      stats.total_non_trivial_communities,
      cdlp_expected_statistics.total_non_trivial_communities);

  KATANA_LOG_VASSERT(
      stats.largest_community_size ==
          cdlp_expected_statistics.largest_community_size,
      "Wrong size for the largest community. Found: {}, Expected: {}",
      stats.largest_community_size,
      cdlp_expected_statistics.largest_community_size);

  KATANA_LOG_VASSERT(
      stats.largest_community_ratio ==
          cdlp_expected_statistics.largest_community_ratio,
      "Wrong ratio of nodes presented in the largest community. Found: {}, "
      "Expected: {}",
      stats.largest_community_ratio,
      cdlp_expected_statistics.largest_community_ratio);
}

int
main() {
  katana::SharedMemSys S;

  // Pass true as the third argument to RunCdlp if the generated
  // graph is already symmetric

  // Grid tests
  RunCdlp(katana::MakeGrid(2, 2, true), true, CdlpStatistics{1, 1, 4, 1});
  RunCdlp(katana::MakeGrid(2, 2, false), true, CdlpStatistics{2, 2, 2, 0.5});

  // Triangular array tests
  RunCdlp(katana::MakeTriangle(1), true, CdlpStatistics{1, 1, 3, 1});

  return 0;
}
