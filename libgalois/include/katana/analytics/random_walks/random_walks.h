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
class RandomWalksPlan : Plan {
  enum Algo { node2vec, edge2vec };

public:
  /// Algorithm selectors for Connected-components
  enum Algorithm { kNode2Vec, kEdge2Vec };

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
  // kChunkSize is a fixed const int (default value: 1)
  static const int kChunkSize;

  RandomWalksPlan() : RandomWalksPlan{kCPU, kNode2Vec, 1, 1, 1.0, 1.0, 10, 1} {}

  Algorithm algorithm() const { return algorithm_; }
  uint32_t walk_length() const { return walk_length_; }
  uint32_t number_of_walks() const { return number_of_walks_; }
  double backward_probability() const { return backward_probability_; }
  double forward_probability() const { return forward_probability_; }
  uint32_t max_iterations() const { return max_iterations_; }
  uint32_t number_of_edge_types() const { return number_of_edge_types_; }

  /// Node2Vec algorithm to generate random walks on the graph
  static RandomWalksPlan Node2Vec(
      uint32_t walk_length, uint32_t number_of_walks,
      double backward_probability, double forward_probability) {
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
      uint32_t walk_length, uint32_t number_of_walks,
      double backward_probability, double forward_probability,
      uint32_t max_iterations, uint32_t number_of_edge_types) {
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

/// Compute the random-walks for pg. The pg is expected to be
/// symmetric.
/// The parameters can be specified, but have reasonable defaults. Not all parameters
/// are used by the algorithms.
/// The generated random-walks generated are return in Katana::InsertBag.
KATANA_EXPORT Result<std::vector<std::vector<uint32_t>>> RandomWalks(
    PropertyGraph* pg, RandomWalksPlan plan = RandomWalksPlan());

KATANA_EXPORT Result<void> RandomWalksAssertValid(PropertyGraph* pg);

}  // namespace katana::analytics
#endif
