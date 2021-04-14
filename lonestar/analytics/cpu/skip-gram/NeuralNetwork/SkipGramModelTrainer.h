#pragma once

#include <math.h>

#include <algorithm>

#include "../Huffman/HuffmanCoding.h"
#include "katana/AtomicHelpers.h"
#include "katana/AtomicWrapper.h"

/** Parent class for training word2vec's neural network */
class SkipGramModelTrainer {
private:
  /** Boundary for maximum exponent allowed */
  const static uint32_t kMaxExp = 6;

  const static uint32_t kMaxQw = 100000000;
  /** Size of the pre-cached exponent table */
  const static uint32_t kExpTableSize = 1000;

  std::vector<double> exp_table_;

  const static uint32_t kTableSize = 100000000;

  uint32_t vocab_size_;

  uint32_t embedding_size_;

  /**
         * In the C version, this includes the </s> token that replaces a newline character
         */
  uint32_t num_trained_tokens_;

  /** 
         * To be precise, this is the number of words in the training data that exist in the vocabulary
         * which have been processed so far.  It includes words that are discarded from sampling.
         * Note that each word is processed once per iteration.
         */
  long actual_word_count_;

  double alpha_;

  /** Learning rate, affects how fast values in the layers get updated */
  double initial_learning_rate_;

  /** 
         * This contains the outer layers of the neural network
         * First dimension is the vocab, second is the layer
         */
  std::vector<std::vector<katana::CopyableAtomic<double>>> syn0_;

  /** This contains hidden layers of the neural network */
  std::vector<std::vector<katana::CopyableAtomic<double>>> syn1_;

  /** This is used for negative sampling */
  std::vector<std::vector<katana::CopyableAtomic<double>>> syn1_neg_;

  /** Used for negative sampling */
  std::vector<int32_t> table_;

  long start_nano_;

  uint32_t negative_samples_;
  /** 
        ** The number of words observed in the training data for this worker that exist
        ** in the vocabulary.  It includes words that are discarded from sampling.
        **/
  uint32_t word_count_;

  /** Value of wordCount the last time alpha was updated */
  uint32_t last_word_count_;

  uint32_t iterations_;

  double down_sample_rate_;

  unsigned long long next_random_;

  const static uint32_t kLearningRateUpdateFrequency = 10000;

  uint32_t current_actual_;

  uint32_t window_;

  bool hierarchical_softmax_;

public:
  SkipGramModelTrainer(
      uint32_t embedding_size, double alpha, uint32_t window,
      double down_sample_rate, bool hierarchical_softmax,
      uint32_t num_neg_samples, uint32_t num_iterations, uint32_t vocab_size,
      uint32_t num_trained_tokens,
      std::map<uint32_t, HuffmanCoding::HuffmanNode*>& huffman_nodes_map);

  double GetSyn0(uint32_t node_idx, uint32_t idx) {
    return syn0_[node_idx][idx];
  }

  //initialize exp_table
  void InitExpTable();

  //initialize table_
  void InitializeUnigramTable(
      std::map<unsigned int, HuffmanCoding::HuffmanNode*>& huffman_nodes_map);

  //randomly initializes the embeddings
  void InitializeSyn0();

  /** @return Next random value to use */
  static unsigned long long IncrementRandom(unsigned long long r);

  /** 
	  * Degrades the learning rate (alpha) steadily towards 0
	  * @param iter Only used for debugging
	  */
  void UpdateAlpha();

  //Train a pair of target and sample nodes
  void TrainSample(
      unsigned int target, unsigned int sample,
      std::map<uint32_t, HuffmanCoding::HuffmanNode*>& huffman_nodes_map,
      unsigned long long* next_random);

  //Train random walks
  void Train(
      std::vector<std::vector<uint32_t>>& random_walks,
      std::map<uint32_t, HuffmanCoding::HuffmanNode*>& huffman_nodes_map,
      katana::gstl::Map<uint32_t, uint32_t>& vocab_multiset);

  //generate random negative samples
  void HandleNegativeSampling(
      HuffmanCoding::HuffmanNode& huffman_node, uint32_t l1,
      std::vector<double>* neu1e, unsigned long long* next_random);

  //construct a new walk/sentence by downsampling
  //most frequently occurring nodes
  void RefineWalk(
      std::vector<uint32_t>& walk, std::vector<uint32_t>* refined_walk,
      katana::gstl::Map<uint32_t, uint32_t>& vocab_multiset,
      unsigned long long* next_random);
};
