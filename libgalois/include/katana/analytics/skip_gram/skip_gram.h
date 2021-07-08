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

  SkipGramPlan(Architecture architecture, Algorithm algorithm)
      : Plan(architecture), algorithm_(algorithm) {}

public:
  SkipGramPlan() : SkipGramPlan{kCPU, kSkipGram} {}

  Algorithm algorithm() const { return algorithm_; }

  static SkipGramPlan SkipGram() { return {kCPU, kSkipGram}; }
};

/// Compute the embeddings for the random walks stored in inputFile
KATANA_EXPORT Result<std::vector<std::pair<uint32_t, std::vector<double>>>>
SkipGram(
    const std::string& input_file, SkipGramPlan plan = {},
    uint32_t embedding_size = 100, double alpha = 0.025, uint32_t window = 5,
    double down_sample_rate = 0.001, bool hierarchical_softmax = false,
    uint32_t num_neg_samples = 5, uint32_t num_iterations = 5,
    uint32_t minimum_frequency = 5);

}  // namespace katana::analytics
#endif
