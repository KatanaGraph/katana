#ifndef KATANA_LIBGRAPH_KATANA_ANALYTICS_KSHORTESTPATHS_KSSSP_H_
#define KATANA_LIBGRAPH_KATANA_ANALYTICS_KSHORTESTPATHS_KSSSP_H_

#include "katana/analytics/sssp/sssp.h"

namespace katana::analytics {
using kSsspPlan = SsspPlan;

enum AlgoReachability { async = 0, syncLevel };

/// Compute the K Shortest Path for pg starting from start_node.
/// The algorithm and delta stepping
/// parameter can be specified, but have reasonable defaults.
KATANA_EXPORT Result<void> Ksp(
    katana::PropertyGraph* pg, unsigned int start_node,
    unsigned int report_node, katana::TxnContext* txn_ctx,
    AlgoReachability algo_reachability, unsigned int num_paths,
    unsigned int step_shift, kSsspPlan plan = {});
}  // namespace katana::analytics

#endif
