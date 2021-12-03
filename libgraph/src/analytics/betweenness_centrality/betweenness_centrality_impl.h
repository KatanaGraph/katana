#ifndef KATANA_LIBGRAPH_ANALYTICS_BETWEENNESSCENTRALITY_BETWEENNESSCENTRALITYIMPL_H_
#define KATANA_LIBGRAPH_ANALYTICS_BETWEENNESSCENTRALITY_BETWEENNESSCENTRALITYIMPL_H_

#include "katana/analytics/Utils.h"
#include "katana/analytics/betweenness_centrality/betweenness_centrality.h"

katana::Result<void> BetweennessCentralityOuter(
    tsuba::TxnContext* txn_ctx, katana::PropertyGraph* pg,
    katana::analytics::BetweennessCentralitySources sources,
    const std::string& output_property_name,
    [[maybe_unused]] katana::analytics::BetweennessCentralityPlan plan);

katana::Result<void> BetweennessCentralityLevel(
    tsuba::TxnContext* txn_ctx, katana::PropertyGraph* pg,
    katana::analytics::BetweennessCentralitySources sources,
    const std::string& output_property_name,
    katana::analytics::BetweennessCentralityPlan plan);

#endif
