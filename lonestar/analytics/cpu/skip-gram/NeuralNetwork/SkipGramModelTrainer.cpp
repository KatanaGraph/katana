//#pragma once

#include "SkipGramModelTrainer.h"

#include <math.h>

#include <algorithm>

#include "../Huffman/HuffmanCoding.h"
#include "katana/AtomicWrapper.h"

void
SkipGramModelTrainer::InitExpTable() {
  for (uint32_t i = 0; i < kExpTableSize; i++) {
    // Precompute the exp() table
    exp_table_[i] = std::exp((i / (double)kExpTableSize * 2 - 1) * kMaxExp);
    // Precompute f(x) = x / (x + 1)
    exp_table_[i] /= exp_table_[i] + 1;
  }
}

SkipGramModelTrainer::SkipGramModelTrainer(
    uint32_t vocab_size, uint32_t num_trained_tokens,
    std::map<uint32_t, HuffmanCoding::HuffmanNode*>& huffman_nodes_map) {
  vocab_size_ = vocab_size;
  num_trained_tokens_ = num_trained_tokens;
  word_count_ = 0;
  current_actual_ = 0;

  exp_table_.reserve(kExpTableSize);
  table_.reserve(kTableSize);

  syn0_.resize(vocab_size_ + 1);
  syn1_.resize(vocab_size_ + 1);
  syn1_neg_.resize(vocab_size_ + 1);

  katana::do_all(
      katana::iterate((uint32_t)0, (uint32_t)(vocab_size_ + 1)),
      [&](uint32_t idx) {
        syn0_[idx].resize(kLayer1Size, 0.0f);
        syn1_[idx].resize(kLayer1Size, 0.0f);
        syn1_neg_[idx].resize(kLayer1Size, 0.0f);
      });

  alpha_ = kInitialLearningRate;

  InitializeSyn0();
  InitializeUnigramTable(huffman_nodes_map);
}

void
SkipGramModelTrainer::InitializeUnigramTable(
    std::map<uint32_t, HuffmanCoding::HuffmanNode*>& huffman_nodes_map) {
  katana::GAccumulator<long long> train_words_pow;
  double power = 0.75f;

  //katana::GAccumulator<uint32_t> count;

  katana::do_all(
      katana::iterate(huffman_nodes_map),
      [&](std::pair<uint32_t, HuffmanCoding::HuffmanNode*> pair) {
        if (pair.first > vocab_size_) {
          return;
        }
        HuffmanCoding::HuffmanNode* node = pair.second;
        train_words_pow += std::pow(node->GetCount(), power);
        //count += 1;
      });

  auto iter = huffman_nodes_map.begin();
  HuffmanCoding::HuffmanNode* last_node = iter->second;
  iter++;
  double d1 =
      pow(last_node->GetCount(), power) / ((double)train_words_pow.reduce());
  uint32_t i = 0;

  for (uint32_t a = 0; a < kTableSize; a++) {
    table_[a] = i;

    if (a / (double)kTableSize > d1) {
      i++;
      HuffmanCoding::HuffmanNode* next_node = last_node;
      if (iter != huffman_nodes_map.end()) {
        next_node = iter->second;
        iter++;
      }

      d1 += std::pow(next_node->GetCount(), power) /
            ((double)train_words_pow.reduce());
      last_node = next_node;
    }

    if (i >= vocab_size_) {
      i = vocab_size_ - 1;
    }
  }
}

void
SkipGramModelTrainer::InitializeSyn0() {
  //unsigned long long next_random = 1;
  next_random_ = 1;
  for (uint32_t a = 0; a < vocab_size_; a++) {
    // Consume a random for fun
    // Actually we do this to use up the injected </s> token
    next_random_ = IncrementRandom(next_random_);
    for (uint32_t b = 0; b < kLayer1Size; b++) {
      next_random_ = IncrementRandom(next_random_);
      syn0_[a][b] =
          (((next_random_ & 0xFFFF) / (double)65536) - 0.5f) / kLayer1Size;
    }
  }
}

/** @return Next random value to use */
unsigned long long
SkipGramModelTrainer::IncrementRandom(unsigned long long r) {
  return r * (unsigned long long)25214903917L + 11;
}

/** 
		 * Degrades the learning rate (alpha) steadily towards 0
		 * @param iter Only used for debugging
		 */
void
SkipGramModelTrainer::UpdateAlpha() {
  current_actual_ += word_count_ - last_word_count_;
  last_word_count_ = word_count_;

  // Degrade the learning rate linearly towards 0 but keep a minimum
  alpha_ =
      kInitialLearningRate *
      std::max(
          1 - current_actual_ / (double)(kIterations * num_trained_tokens_),
          (double)0.0001f);

  katana::gPrint("current:", current_actual_);
  katana::gPrint("alpha:", alpha_);
}

//generate random negative samples
void
SkipGramModelTrainer::HandleNegativeSampling(
    HuffmanCoding::HuffmanNode& huffman_node, uint32_t l1,
    std::vector<double>* neu1e, unsigned long long* next_random) {
  for (uint32_t d = 0; d <= kNegativeSamples; d++) {
    uint32_t target;
    uint32_t label;
    if (d == 0) {
      target = huffman_node.GetIdx();
      label = 1;
    } else {
      (*next_random) = IncrementRandom(*next_random);
      target = table_[(uint32_t)(
          ((((*next_random) >> 16) % kTableSize) + kTableSize) % kTableSize)];

      if (target == 0) {
        target =
            (uint32_t)(
                (((*next_random) % (vocab_size_ - 1)) + (vocab_size_ - 1)) %
                (vocab_size_ - 1)) +
            1;
      }
      if (target == huffman_node.GetIdx()) {
        continue;
      }
      label = 0;
    }

    KATANA_LOG_VASSERT(target < vocab_size_, "target exceeds vocab size");

    uint32_t l2 = target;
    double f = 0.0;
    for (uint32_t c = 0; c < kLayer1Size; c++) {
      f += syn0_[l1][c] * syn1_neg_[l2][c];
    }
    double g;
    if (f > kMaxExp) {
      g = ((double)(label - 1)) * alpha_;
    } else if (f < -kMaxExp) {
      g = ((double)(label - 0)) * alpha_;
    } else {
      g = ((double)label -
           exp_table_[(uint32_t)(
               (f + (double)kMaxExp) *
               ((double)kExpTableSize / ((double)kMaxExp) / 2))]) *
          alpha_;
    }

    for (uint32_t c = 0; c < kLayer1Size; c++) {
      (*neu1e)[c] += g * syn1_neg_[l2][c];
    }
    for (uint32_t c = 0; c < kLayer1Size; c++) {
      katana::atomicAdd(syn1_neg_[l2][c], g * syn0_[l1][c]);
    }
  }
}

void
SkipGramModelTrainer::TrainSample(
    unsigned int target, unsigned int sample,
    std::map<uint32_t, HuffmanCoding::HuffmanNode*>& huffman_nodes_map,
    unsigned long long* next_random) {
  HuffmanCoding::HuffmanNode* huffman_node =
      huffman_nodes_map.find(target)->second;

  std::vector<double> neu1e;
  neu1e.resize(kLayer1Size, 0.0);

  uint32_t l1 = huffman_nodes_map.find(sample)->second->GetIdx();

  /*uint32_t huffman_node_code_len = huffman_node->GetCodeLen();

  for (uint32_t d = 0; d < huffman_node_code_len; d++) {
    double f = 0.0f;
    uint32_t l2 = huffman_node->GetPoint(d);

    for (uint32_t e = 0; e < kLayer1Size; e++) {
      f += syn0_[l1][e] * syn1_[l2][e];
    }

    if ((f <= -kMaxExp) || (f >= kMaxExp)) {
      continue;
    } else {
      f = exp_table_[(uint32_t)((f + (double)kMaxExp) * (((double)kExpTableSize) / ((double)kMaxExp) / 2))];
    }

    double g = (1.0 - huffman_node->GetCode(d) - f) * alpha_;

    for (uint32_t e = 0; e < kLayer1Size; e++) {
      neu1e[e] += g * syn1_[l2][e];
    }

    // Learn weights hidden -> output

    for (uint32_t e = 0; e < kLayer1Size; e++) {
      katana::atomicAdd(syn1_[l2][e], g * syn0_[l1][e]);
    }
  }*/

  HandleNegativeSampling(*huffman_node, l1, &neu1e, next_random);

  //katana::gPrint("next:", next_random);

  // Learn weights input -> hidden
  for (uint32_t d = 0; d < kLayer1Size; d++) {
    katana::atomicAdd(syn0_[l1][d], neu1e[d]);
  }
}

void
SkipGramModelTrainer::RefineWalk(
    std::vector<uint32_t>& walk, std::vector<uint32_t>* refined_walk,
    std::map<uint32_t, HuffmanCoding::HuffmanNode*>& huffman_nodes_map,
    unsigned long long* next_random) {
  for (auto val : walk) {
    HuffmanCoding::HuffmanNode* huffman_node =
        huffman_nodes_map.find(val)->second;
    uint32_t count = huffman_node->GetCount();
    if (kDownSampleRate > 0) {
      double ran =
          (std::sqrt(
               count / (kDownSampleRate * ((double)num_trained_tokens_))) +
           1) *
          (kDownSampleRate * ((double)num_trained_tokens_)) / ((double)count);
      (*next_random) = IncrementRandom(*next_random);
      if (ran < ((*next_random) & 0xFFFF) / (double)65536) {
        continue;
      }
    }
    refined_walk->push_back(val);
  }
}

void
SkipGramModelTrainer::Train(
    std::vector<std::vector<uint32_t>>& random_walks,
    std::map<uint32_t, HuffmanCoding::HuffmanNode*>& huffman_nodes_map) {
  katana::GAccumulator<uint64_t> accum;
  katana::do_all(
      katana::iterate(random_walks), [&](std::vector<uint32_t>& walk) {
        unsigned long long next_random = next_random_;
        std::vector<uint32_t> refined_walk;
        refined_walk.reserve(walk.size());
        accum += walk.size();

        RefineWalk(walk, &refined_walk, huffman_nodes_map, &next_random);

        uint32_t sentence_position = 0;
        uint32_t walk_length = refined_walk.size();

        while (sentence_position < walk_length) {
          uint32_t target = refined_walk[sentence_position];
          next_random = IncrementRandom(next_random);

          uint32_t b = next_random % kWindow;
          for (uint32_t a = b; a < (kWindow * 2 + 1 - b); a++) {
            if (a != kWindow) {
              int32_t c = sentence_position - kWindow + a;
              if (c < 0) {
                continue;
              }
              if (c >= (int32_t)walk_length) {
                continue;
              }
              uint32_t sample = refined_walk[c];
              TrainSample(target, sample, huffman_nodes_map, &next_random);
            }
          }

          sentence_position++;
        }

        next_random_ = next_random;
      });

  word_count_ += accum.reduce();
  if (word_count_ - last_word_count_ > kLearningRateUpdateFrequency) {
    UpdateAlpha();
  }
}
