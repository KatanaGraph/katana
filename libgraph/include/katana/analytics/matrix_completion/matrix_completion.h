#ifndef KATANA_LIBGRAPH_KATANA_ANALYTICS_MATRIXCOMPLETION_MATRIXCOMPLETION_H_
#define KATANA_LIBGRAPH_KATANA_ANALYTICS_MATRIXCOMPLETION_MATRIXCOMPLETION_H_

#include <fstream>
#include <iostream>

#include "katana/Properties.h"
#include "katana/PropertyGraph.h"
#include "katana/analytics/Plan.h"

namespace katana::analytics {

/// A computational plan to for MatrixCompletion, specifying the algorithm and any parameters
/// associated with it.
class MatrixCompletionPlan : public Plan {
public:
  enum Algorithm {
    kSGDByItems,
  };

  enum Step { kBold, kBottou, kIntel, kInverse, kPurdue };

  static constexpr double kDefaultLearningRate = 0.012;
  static constexpr double kDefaultDecayRate = 0.015;
  static constexpr double kDefaultLambda = 0.05;
  static constexpr double kDefaultTolerance = 0.01;
  static constexpr bool kDefaultUseSameLatentVector = false;
  static constexpr uint32_t kDefaultMaxUpdates = 100;
  static constexpr uint32_t kDefaultUpdatesPerEdge = 1;
  static constexpr uint32_t kDefaultFixedRounds = 0;
  static constexpr bool kDefaultUseExactError = false;
  static constexpr bool kDefaultUseDetInit = false;
  static constexpr Step kDefaultLearningRateFunction = kBold;

private:
  Algorithm algorithm_;
  double learning_rate_;
  double decay_rate_;
  double lambda_;
  double tolerance_;
  bool use_same_latent_vector_;
  uint32_t max_updates_;
  uint32_t updates_per_edge_;
  uint32_t fixed_rounds_;
  bool use_exact_error_;
  bool use_det_init_;
  Step learning_rate_function_;

  MatrixCompletionPlan(
      Architecture architecture, Algorithm algorithm, double learning_rate,
      double decay_rate, double lambda, double tolerance,
      bool use_same_latent_vector, uint32_t max_updates,
      uint32_t updates_per_edge, uint32_t fixed_rounds, bool use_exact_error,
      bool use_det_init, Step learning_rate_function)
      : Plan(architecture),
        algorithm_(algorithm),
        learning_rate_(learning_rate),
        decay_rate_(decay_rate),
        lambda_(lambda),
        tolerance_(tolerance),
        use_same_latent_vector_(use_same_latent_vector),
        max_updates_(max_updates),
        updates_per_edge_(updates_per_edge),
        fixed_rounds_(fixed_rounds),
        use_exact_error_(use_exact_error),
        use_det_init_(use_det_init),
        learning_rate_function_(learning_rate_function) {}

public:
  MatrixCompletionPlan()
      : MatrixCompletionPlan{
            kCPU,
            kSGDByItems,
            kDefaultLearningRate,
            kDefaultDecayRate,
            kDefaultLambda,
            kDefaultTolerance,
            kDefaultUseSameLatentVector,
            kDefaultMaxUpdates,
            kDefaultUpdatesPerEdge,
            kDefaultFixedRounds,
            kDefaultUseExactError,
            kDefaultUseDetInit,
            kDefaultLearningRateFunction} {}

  Algorithm algorithm() const { return algorithm_; }
  double learningRate() const { return learning_rate_; }
  double decayRate() const { return decay_rate_; }
  double lambda() const { return lambda_; }
  double tolerance() const { return tolerance_; }
  bool useSameLatentVector() const { return use_same_latent_vector_; }
  uint32_t maxUpdates() const { return max_updates_; }
  uint32_t updatesPerEdge() const { return updates_per_edge_; }
  uint32_t fixedRounds() const { return fixed_rounds_; }
  bool useExactError() const { return use_exact_error_; }
  bool useDetInit() const { return use_det_init_; }
  Step learningRateFunction() const { return learning_rate_function_; }

  static MatrixCompletionPlan SGDByItems(
      double learning_rate = kDefaultLearningRate,
      double decay_rate = kDefaultDecayRate, double lambda = kDefaultLambda,
      double tolerance = kDefaultTolerance,
      bool use_same_latent_vector = kDefaultUseSameLatentVector,
      uint32_t max_updates = kDefaultMaxUpdates,
      uint32_t updates_per_edge = kDefaultUpdatesPerEdge,
      uint32_t fixed_rounds = kDefaultFixedRounds,
      bool use_exact_error = kDefaultUseExactError,
      bool use_det_init = kDefaultUseDetInit,
      Step learning_rate_function = kDefaultLearningRateFunction) {
    return {
        kCPU,
        kSGDByItems,
        learning_rate,
        decay_rate,
        lambda,
        tolerance,
        use_same_latent_vector,
        max_updates,
        updates_per_edge,
        fixed_rounds,
        use_exact_error,
        use_det_init,
        learning_rate_function};
  }
};

/// Performs matrix completion using stochastic gradient descent (SGD) algortihm
/// on a bipartite graph and learns latent vectors for each node that is stored in
/// an ArrayProperty.
/// The plan controls the algorithm and parameters used to compute the latent vectors.
KATANA_EXPORT Result<void> MatrixCompletion(
    const std::shared_ptr<katana::PropertyGraph>& pg,
    katana::TxnContext* txn_ctx, MatrixCompletionPlan plan = {});

}  // namespace katana::analytics

#endif
