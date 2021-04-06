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

  const static uint32_t kLayer1Size = 100;

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
  constexpr static double kInitialLearningRate = 0.025f;

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

  const static uint32_t kNegativeSamples = 1;
  /** 
        ** The number of words observed in the training data for this worker that exist
        ** in the vocabulary.  It includes words that are discarded from sampling.
        **/
  uint32_t word_count_;

  /** Value of wordCount the last time alpha was updated */
  uint32_t last_word_count_;

  const static uint32_t kIterations = 5;

  constexpr static double kDownSampleRate = 0.001f;

  unsigned long long next_random_;

  const static uint32_t kLearningRateUpdateFrequency = 10000;

  uint32_t current_actual_;

  const static uint32_t kWindow = 5;

public:
  SkipGramModelTrainer(
      uint32_t vocab_size, uint32_t num_trained_tokens,
      std::map<unsigned int, HuffmanCoding::HuffmanNode*>& huffman_nodes_map);

  double GetSyn0(uint32_t node_idx, uint32_t idx) {
    return syn0_[node_idx][idx];
  }
  static uint32_t GetLayer1Size() { return kLayer1Size; }
  void InitExpTable();

  void InitializeUnigramTable(
      std::map<unsigned int, HuffmanCoding::HuffmanNode*>& huffman_nodes_map);

  void InitializeSyn0();

  /** @return Next random value to use */
  static unsigned long long IncrementRandom(unsigned long long r);

  void TrainSample(uint32_t target, uint32_t sample);

  /** 
	  * Degrades the learning rate (alpha) steadily towards 0
	  * @param iter Only used for debugging
	  */
  void UpdateAlpha();

  void TrainSample(
      unsigned int target, unsigned int sample,
      std::map<uint32_t, HuffmanCoding::HuffmanNode*>& huffman_nodes_map,
      unsigned long long* next_random);

  void Train(
      std::vector<std::vector<uint32_t>>& random_walks,
      std::map<uint32_t, HuffmanCoding::HuffmanNode*>& huffman_nodes_map);

  //generate random negative samples
  void HandleNegativeSampling(
      HuffmanCoding::HuffmanNode& huffman_node, uint32_t l1,
      std::vector<double>* neu1e, unsigned long long* next_random);
};
