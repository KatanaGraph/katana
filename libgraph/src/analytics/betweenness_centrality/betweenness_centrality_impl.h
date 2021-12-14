#ifndef KATANA_LIBGRAPH_ANALYTICS_BETWEENNESSCENTRALITY_BETWEENNESSCENTRALITYIMPL_H_
#define KATANA_LIBGRAPH_ANALYTICS_BETWEENNESSCENTRALITY_BETWEENNESSCENTRALITYIMPL_H_

#include "katana/analytics/Utils.h"
#include "katana/analytics/betweenness_centrality/betweenness_centrality.h"

katana::Result<void> BetweennessCentralityOuter(
    katana::PropertyGraph* pg,
    katana::analytics::BetweennessCentralitySources sources,
    const std::string& output_property_name,
    [[maybe_unused]] katana::analytics::BetweennessCentralityPlan plan,
    tsuba::TxnContext* txn_ctx);

katana::Result<void> BetweennessCentralityLevel(
    katana::PropertyGraph* pg,
    katana::analytics::BetweennessCentralitySources sources,
    const std::string& output_property_name,
    katana::analytics::BetweennessCentralityPlan plan,
    tsuba::TxnContext* txn_ctx);

#endif
