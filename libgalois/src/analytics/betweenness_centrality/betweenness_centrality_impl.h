#ifndef KATANA_LIBGALOIS_ANALYTICS_BETWEENNESSCENTRALITY_BETWEENNESSCENTRALITYIMPL_H_
#define KATANA_LIBGALOIS_ANALYTICS_BETWEENNESSCENTRALITY_BETWEENNESSCENTRALITYIMPL_H_

#include "katana/analytics/Utils.h"
#include "katana/analytics/betweenness_centrality/betweenness_centrality.h"

katana::Result<void> BetweennessCentralityOuter(
    katana::PropertyFileGraph* pfg,
    katana::analytics::BetweennessCentralitySources sources,
    const std::string& output_property_name,
    [[maybe_unused]] katana::analytics::BetweennessCentralityPlan plan);

katana::Result<void> BetweennessCentralityLevel(
    katana::PropertyFileGraph* pfg,
    katana::analytics::BetweennessCentralitySources sources,
    const std::string& output_property_name,
    katana::analytics::BetweennessCentralityPlan plan);

#endif
