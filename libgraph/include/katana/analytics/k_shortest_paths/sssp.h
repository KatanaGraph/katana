#ifndef KATANA_LIBGRAPH_KATANA_ANALYTICS_KSHORTESTPATHS_SSSP_H_
#define KATANA_LIBGRAPH_KATANA_ANALYTICS_KSHORTESTPATHS_SSSP_H_

#include "katana/analytics/sssp/sssp.h"

namespace katana::analytics {
enum AlgoReachability { async = 0, syncLevel };

/// Compute the Single-Source Shortest Path for pg starting from start_node.
/// The edge weights are taken from the property named
/// edge_weight_property_name (which may be a 32- or 64-bit sign or unsigned
/// int), and the computed path lengths are stored in the property named
/// output_property_name (as uint32_t). The algorithm and delta stepping
/// parameter can be specified, but have reasonable defaults.
/// The property named output_property_name is created by this function and may
/// not exist before the call.
KATANA_EXPORT Result<void> Ksp(
    PropertyGraph* pg, unsigned int start_node, unsigned int reportNode,
    const std::string& edge_weight_property_name, katana::TxnContext* txn_ctx,
    AlgoReachability algoReachability, unsigned int numPaths,
    unsigned int stepShift, SsspPlan plan = {});
}  // namespace katana::analytics

#endif
