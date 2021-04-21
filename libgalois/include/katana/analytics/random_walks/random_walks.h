#ifndef KATANA_LIBGALOIS_KATANA_ANALYTICS_RANDOMWALKS_RANDOMWALKS_H_
#define KATANA_LIBGALOIS_KATANA_ANALYTICS_RANDOMWALKS_RANDOMWALKS_H_

#include <iostream>

#include <katana/analytics/Plan.h>

#include "katana/AtomicHelpers.h"
#include "katana/analytics/Utils.h"

// API

namespace katana::analytics {

/// A computational plan to for random walks, specifying the algorithm and any
/// parameters associated with it.
class RandomWalksPlan : public Plan {
public:
  /// Algorithm selectors for random walks
  enum Algorithm { kNode2Vec, kEdge2Vec };

  static const Algorithm kDefaultAlgorithm = kNode2Vec;
  static const uint32_t kDefaultWalkLength = 1;
  static const uint32_t kDefaultNumberOfWalks = 1;
  constexpr static const double kDefaultBackwardProbability = 1.0;
  constexpr static const double kDefaultForwardProbability = 1.0;
  static const uint32_t kDefaultMaxIterations = 10;
  static const uint32_t kDefaultNumberOfEdgeTypes = 1;

  // Don't allow people to directly construct these, so as to have only one
  // consistent way to configure.
private:
  Algorithm algorithm_;
  uint32_t walk_length_;
  uint32_t number_of_walks_;
  double backward_probability_;
  double forward_probability_;
  // Only need for edge2vec
  uint32_t max_iterations_;
  // Only need for edge2vec
  // TODO(gill) Find number of edge types automatically
  uint32_t number_of_edge_types_;

  RandomWalksPlan(
      Architecture architecture, Algorithm algorithm, uint32_t walk_length,
      uint32_t number_of_walks, double backward_probability,
      double forward_probability, uint32_t max_iterations,
      uint32_t number_of_edge_types)
      : Plan(architecture),
        algorithm_(algorithm),
        walk_length_(walk_length),
        number_of_walks_(number_of_walks),
        backward_probability_(backward_probability),
        forward_probability_(forward_probability),
        max_iterations_(max_iterations),
        number_of_edge_types_(number_of_edge_types) {}

public:
  // kChunkSize is fixed at 1
  static const int kChunkSize;

  RandomWalksPlan()
      : RandomWalksPlan{
            kCPU,
            kDefaultAlgorithm,
            kDefaultWalkLength,
            kDefaultNumberOfWalks,
            kDefaultBackwardProbability,
            kDefaultForwardProbability,
            kDefaultMaxIterations,
            kDefaultNumberOfEdgeTypes} {}

  Algorithm algorithm() const { return algorithm_; }

  // TODO(amp): The parameters walk_length, number_of_walks,
  //  backward_probability, and forward_probability control the expected output,
  //  not the algorithm used to compute the output. So they need to be parameters
  //  on the algorithm function, not in the plan. The plan should be parameters
  //  which do not change the expected output (though they may cause selecting a
  //  different correct output).

  /// Length of random walks.
  uint32_t walk_length() const { return walk_length_; }

  /// Number of walks per node.
  uint32_t number_of_walks() const { return number_of_walks_; }

  /// Probability of moving back to parent.
  double backward_probability() const { return backward_probability_; }

  /// Probability of moving forward (2-hops).
  double forward_probability() const { return forward_probability_; }

  uint32_t max_iterations() const { return max_iterations_; }

  uint32_t number_of_edge_types() const { return number_of_edge_types_; }

  /// Node2Vec algorithm to generate random walks on the graph
  static RandomWalksPlan Node2Vec(
      uint32_t walk_length = kDefaultWalkLength,
      uint32_t number_of_walks = kDefaultNumberOfWalks,
      double backward_probability = kDefaultBackwardProbability,
      double forward_probability = kDefaultBackwardProbability) {
    return {
        kCPU,
        kNode2Vec,
        walk_length,
        number_of_walks,
        backward_probability,
        forward_probability,
        0,
        1};
  }

  /// Edge2Vec algorithm to generate random walks on the graph.
  /// Takes the heterogeneity of the edges into account
  static RandomWalksPlan Edge2Vec(
      uint32_t walk_length = kDefaultWalkLength,
      uint32_t number_of_walks = kDefaultNumberOfWalks,
      double backward_probability = kDefaultBackwardProbability,
      double forward_probability = kDefaultBackwardProbability,
      uint32_t max_iterations = kDefaultMaxIterations,
      uint32_t number_of_edge_types = kDefaultNumberOfEdgeTypes) {
    return {
        kCPU,
        kNode2Vec,
        walk_length,
        number_of_walks,
        backward_probability,
        forward_probability,
        max_iterations,
        number_of_edge_types};
  }
};

/// Compute the random-walks for pg. The pg is expected to be symmetric. The
/// parameters can be specified, but have reasonable defaults. Not all
/// parameters are used by the algorithms. The generated random-walks generated
/// are returned as a vector of vectors.
KATANA_EXPORT Result<std::vector<std::vector<uint32_t>>> RandomWalks(
    PropertyGraph* pg, RandomWalksPlan plan = RandomWalksPlan());

KATANA_EXPORT Result<void> RandomWalksAssertValid(PropertyGraph* pg);

}  // namespace katana::analytics
#endif
