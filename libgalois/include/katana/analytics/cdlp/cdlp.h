#ifndef KATANA_LIBGALOIS_KATANA_ANALYTICS_CDLP_CDLP_H_
#define KATANA_LIBGALOIS_KATANA_ANALYTICS_CDLP_CDLP_H_

#include <iostream>

#include "katana/AtomicHelpers.h"
#include "katana/analytics/Plan.h"
#include "katana/analytics/Utils.h"

// API

namespace katana::analytics {

/// A computational plan for Compunity Detection Using Label Propagation
class CdlpPlan : public Plan {
public:
  /// Algorithm selectors for CDLP
  enum Algorithm {
    kSynchronous,
    kAsynchronous,
  };

  // Don't allow people to directly construct these, so as to have only one
  // consistent way to configure.
private:
  Algorithm algorithm_;

  CdlpPlan(
      Architecture architecture, Algorithm algorithm)
      : Plan(architecture),
        algorithm_(algorithm) {}

public:

  CdlpPlan()
      : CdlpPlan{
            kCPU, kSynchronous} {}

  Algorithm algorithm() const { return algorithm_; }

  /// Community Detection using Label Propagation.
  /// [1] U. N. Raghavan, R. Albert and S. Kumara, "Near linear time algorithm
  /// to detect community structures in large-scale networks,"  In: Physical 
  /// Review E 76.3 (2007), p. 036106.
  
  /// Initially, all nodes are in their own community IDs (same as their
  /// node IDs). Then, the community IDs are iteratively set to the most
  /// frequent community ID in their immediate neighborhood. It continues
  /// untill the community ID of all nodes in graph become the same as 
  /// the most frequent ID in their immediate neighborhood.

  /// Synchronous community detection algorithm. This algorithm is based on 
  /// Graphalytics benchmark that has two key differences from the original algorithm 
  /// proposed in [1]. First, it is deterministic: if there are multiple
  /// labels with their frequency equalling the maximum, it selects the smallest 
  /// one while the original algorithm selects randomly. Second, it is synchronous,
  /// i.e., each iteration is computed based on the labels obtained as a result of
  /// the previous iteration. 
  ///
  /// FIXME: As remarked in [1], this can cause the oscillation 
  /// of labels in bipartite or nearly bipartite subgraphs. This is especially true
  /// in cases where communities take the form of a star graph

  static CdlpPlan Synchronous() {
    return {kCPU, kSynchronous};
  }

  /// Unlike Synchronous algorithm, Asynchronous can use the current iteration 
  /// updated community IDs for some of the neighbors that have been already 
  /// updated in the current iteration and use the old values for the other neighbors

  /// TODO: The order in which all the n nodes in the network are updated
  /// at each iteration is chosen randomly vs in order.
  /// if there are multiple labels with their frequency equalling the maximum, it
  /// selects one randomly.
  /// TODO: the output is not deterministic so it is impossible to test; 
  ///
  /// [1] aggregates multiple solutions to get most useful information.
  ///
  /// FIXME: When the algorithm terminates it is possible that two or more disconnected
  /// groups of nodes have the same label (the groups are connected in the network via 
  /// other nodes of different labels). This happens when two or more neighborsof a 
  /// node receive its label and pass the labels in different directions, which ultimately
  /// leads to different communities adopting the same label. In such cases, after the 
  /// algorithm terminates one can run a simple breadth-first search on the sub-networks
  /// of each individual groups to separate the disconnected communities. This requires
  /// an overall time of O(m + n). When aggregating solutions however, we rarely find 
  /// disconnected groups within communities [1].
  ///
  /// The stop Criterion is: If every node has a label that the maximum number of
  /// their neighbors have, then stop the algorithm (TODO: maybe we can use the same stop
  /// criterion for the Synchronous algorithm as well.)

  static CdlpPlan Asynchronous() {
    return {kCPU, kAsynchronous};
  }

};

/// Compute the Community Detection for pg. The pg can be both 
/// directed or undirected based our my discussion with Amber,
/// I can use undirected view which is going to be commited soon.
/// in this stage I am using the bidrectview
/// TODO: not sure about symmetric graphs? 
/// The property named output_property_name is created by this function and may
/// not exist before the call.
KATANA_EXPORT Result<void> Cdlp(
    PropertyGraph* pg, const std::string& output_property_name,
    size_t max_iterations, CdlpPlan plan = CdlpPlan());

KATANA_EXPORT Result<void> CdlpAssertValid(
    PropertyGraph* pg, const std::string& property_name);

struct KATANA_EXPORT CdlpStatistics {
  /// Total number of unique communities in the graph.
  uint64_t total_communities;
  /// Total number of communities with more than 1 node.
  uint64_t total_non_trivial_communities;
  /// The number of nodes present in the largest community.
  uint64_t largest_community_size;
  /// The ratio of nodes present in the largest community.
  double largest_community_ratio;

  /// Print the statistics in a human readable form.
  void Print(std::ostream& os = std::cout) const;

  static katana::Result<CdlpStatistics> Compute(
      katana::PropertyGraph* pg, const std::string& property_name);
};

}  // namespace katana::analytics
#endif
