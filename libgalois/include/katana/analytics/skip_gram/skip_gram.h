#ifndef KATANA_LIBGALOIS_KATANA_ANALYTICS_SKIPGRAM_SKIPGRAM_H_
#define KATANA_LIBGALOIS_KATANA_ANALYTICS_SKIPGRAM_SKIPGRAM_H_

#include <fstream>
#include <iostream>
#include <map>

#include "katana/AtomicHelpers.h"
#include "katana/AtomicWrapper.h"
#include "katana/PropertyGraph.h"
#include "katana/analytics/Plan.h"

// API

namespace katana::analytics {

/// A computational plan to for Skip Gram Embedding
class SkipGramPlan : public Plan {
public:
  enum Algorithm {
    kSkipGram,
  };

  static const uint32_t kEmbeddingSize = 100;
  static constexpr double kAlpha = 0.025;
  static const uint32_t kWindow = 5;
  static constexpr double kDownSampleRate = 0.001;
  static const bool kHierarchicalSoftmax = false;
  static const uint32_t kNumNegSamples = 5;
  static const uint32_t kNumIterations = 5;
  static const uint32_t KMinimumFrequency = 5;

private:
  Algorithm algorithm_;
  //Size of the embedding vector
  uint32_t embedding_size_;
  //alpha
  double alpha_;
  //window size
  uint32_t window_;
  //down-sampling rate
  double down_sample_rate_;
  //Enable/disable hierarchical softmax
  bool hierarchical_softmax_;
  //Number of negative samples
  uint32_t num_neg_samples_;
  //Number of Training Iterations
  uint32_t num_iterations_;
  //Mininum Frequency
  uint32_t minimum_frequency_;

  SkipGramPlan(
      Architecture architecture, Algorithm algorithm, uint32_t embedding_size,
      double alpha, uint32_t window, double down_sample_rate,
      bool hierarchical_softmax, uint32_t num_neg_samples,
      uint32_t num_iterations, uint32_t minimum_frequency)
      : Plan(architecture),
        algorithm_(algorithm),
        embedding_size_(embedding_size),
        alpha_(alpha),
        window_(window),
        down_sample_rate_(down_sample_rate),
        hierarchical_softmax_(hierarchical_softmax),
        num_neg_samples_(num_neg_samples),
        num_iterations_(num_iterations),
        minimum_frequency_(minimum_frequency) {}

public:
  SkipGramPlan()
      : SkipGramPlan{kCPU, kSkipGram, 100, 0.025, 5, 0.001, false, 5, 5, 5} {}

  Algorithm algorithm() const { return algorithm_; }
  uint32_t embedding_size() const { return embedding_size_; }
  double alpha() const { return alpha_; }
  uint32_t window() const { return window_; }
  double down_sample_rate() const { return down_sample_rate_; }
  bool hierarchical_softmax() const { return hierarchical_softmax_; }
  uint32_t num_neg_samples() const { return num_neg_samples_; }
  uint32_t num_iterations() const { return num_iterations_; }
  uint32_t minimum_frequency() const { return minimum_frequency_; }

  static SkipGramPlan SkipGram(
      uint32_t embedding_size = kEmbeddingSize, double alpha = kAlpha,
      uint32_t window = kWindow, double down_sample_rate = kDownSampleRate,
      bool hierarchical_softmax = kHierarchicalSoftmax,
      uint32_t num_neg_samples = kNumNegSamples,
      uint32_t num_iterations = kNumIterations,
      uint32_t minimum_frequency = KMinimumFrequency) {
    return {
        kCPU,           kSkipGram,        embedding_size,       alpha,
        window,         down_sample_rate, hierarchical_softmax, num_neg_samples,
        num_iterations, minimum_frequency};
  }
};

/// Compute the embeddings for the random walks stored in inputFile
KATANA_EXPORT Result<std::vector<std::pair<uint32_t, std::vector<double>>>>
SkipGram(const std::string& input_file, SkipGramPlan plan = {});

}  // namespace katana::analytics
#endif
