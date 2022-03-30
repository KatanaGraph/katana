#ifndef KATANA_LIBGRAPH_ANALYTICS_BETWEENNESSCENTRALITY_BETWEENNESSCENTRALITYIMPL_H_
#define KATANA_LIBGRAPH_ANALYTICS_BETWEENNESSCENTRALITY_BETWEENNESSCENTRALITYIMPL_H_

#include "katana/analytics/Utils.h"
#include "katana/analytics/betweenness_centrality/betweenness_centrality.h"

katana::Result<void> BetweennessCentralityOuter(
    const std::shared_ptr<katana::PropertyGraph>& pg,
    katana::analytics::BetweennessCentralitySources sources,
    const std::string& output_property_name,
    [[maybe_unused]] katana::analytics::BetweennessCentralityPlan plan,
    katana::TxnContext* txn_ctx);

katana::Result<void> BetweennessCentralityLevel(
    const std::shared_ptr<katana::PropertyGraph>& pg,
    katana::analytics::BetweennessCentralitySources sources,
    const std::string& output_property_name,
    katana::analytics::BetweennessCentralityPlan plan,
    katana::TxnContext* txn_ctx);

#endif
