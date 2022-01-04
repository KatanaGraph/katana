#ifndef KATANA_LIBGRAPH_KATANA_ANALYTICS_BETWEENNESSCENTRALITY_BETWEENNESSCENTRALITY_H_
#define KATANA_LIBGRAPH_KATANA_ANALYTICS_BETWEENNESSCENTRALITY_BETWEENNESSCENTRALITY_H_

#include <iostream>
#include <variant>

#include "katana/PropertyGraph.h"
#include "katana/analytics/Plan.h"

// API

namespace katana::analytics {

/// A computational plan to for Betweenness Centrality, specifying the algorithm
/// and any parameters associated with it.
class BetweennessCentralityPlan : public Plan {
public:
  enum Algorithm {
    kLevel,
    kOuter,
    // TODO(gill): Reinstate async and auto once we have bidirectional graphs.
    // kAsynchronous,
    // kAutomatic,
  };

private:
  Algorithm algorithm_;

  BetweennessCentralityPlan(Architecture architecture, Algorithm algorithm)
      : Plan(architecture), algorithm_(algorithm) {}

public:
  BetweennessCentralityPlan() : BetweennessCentralityPlan{kCPU, kLevel} {}

  BetweennessCentralityPlan(const katana::PropertyGraph* pg [[maybe_unused]])
      : BetweennessCentralityPlan() {
    // TODO(gill): Reinstate automation once we reinstate async
    // if (algo == AutoAlgo) {
    //   katana::FileGraph degreeGraph;
    //   degreeGraph.fromFile(inputFile);
    //   degreeGraph.initNodeDegrees();
    //   autoAlgoTimer.start();
    //   if (isApproximateDegreeDistributionPowerLaw(degreeGraph)) {
    //     algo = Asynchronous;
    //   } else {
    //     algo = Level;
    //   }
    //   autoAlgoTimer.stop();
    // }
  }

  Algorithm algorithm() const { return algorithm_; }

  static BetweennessCentralityPlan Level() { return {kCPU, kLevel}; }

  static BetweennessCentralityPlan Outer() { return {kCPU, kOuter}; }

  static BetweennessCentralityPlan FromAlgorithm(Algorithm algo) {
    return BetweennessCentralityPlan(kCPU, algo);
  }
};

/// Either a vector of node IDs or a number of nodes to use as sources.
using BetweennessCentralitySources =
    std::variant<std::vector<uint32_t>, uint32_t>;

/// Use all sources instead of a subset.
KATANA_EXPORT extern const BetweennessCentralitySources
    kBetweennessCentralityAllNodes;

/// Compute the betweenness centrality of each node in the graph.
///
/// The property named output_property_name is created by this function and may
/// not exist before the call.
///
/// @param pg The graph to process.
/// @param output_property_name The parameter to create with the computed value.
/// @param sources Only process some sources, producing an approximate
///          betweenness centrality. If this is a vector process those source
///          nodes; if this is an int process that number of source nodes.
/// @param plan
KATANA_EXPORT Result<void> BetweennessCentrality(
    PropertyGraph* pg, const std::string& output_property_name,
    katana::TxnContext* txn_ctx,
    const BetweennessCentralitySources& sources =
        kBetweennessCentralityAllNodes,
    BetweennessCentralityPlan plan = {});

// TODO(gill): It's not clear how to check these results.
//KATANA_EXPORT Result<void> BetweennessCentralityAssertValid(
//    PropertyGraph* pg, const std::string& output_property_name);

struct KATANA_EXPORT BetweennessCentralityStatistics {
  /// The maximum centrality across all nodes.
  float max_centrality;
  /// The minimum centrality across all nodes.
  float min_centrality;
  /// The average centrality across all nodes.
  float average_centrality;

  /// Print the statistics in a human readable form.
  void Print(std::ostream& os = std::cout);

  static katana::Result<BetweennessCentralityStatistics> Compute(
      PropertyGraph* pg, const std::string& output_property_name);
};

}  // namespace katana::analytics

#endif
