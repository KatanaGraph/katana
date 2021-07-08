/*
 * This file belongs to the Galois project, a C++ library for exploiting
 * parallelism. The code is being released under the terms of the 3-Clause BSD
 * License (a copy is located in LICENSE.txt at the top-level directory).
 *
 * Copyright (C) 2018, The University of Texas at Austin. All rights reserved.
 * UNIVERSITY EXPRESSLY DISCLAIMS ANY AND ALL WARRANTIES CONCERNING THIS
 * SOFTWARE AND DOCUMENTATION, INCLUDING ANY WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR ANY PARTICULAR PURPOSE, NON-INFRINGEMENT AND WARRANTIES OF
 * PERFORMANCE, AND ANY WARRANTY THAT MIGHT OTHERWISE ARISE FROM COURSE OF
 * DEALING OR USAGE OF TRADE.  NO WARRANTY IS EITHER EXPRESS OR IMPLIED WITH
 * RESPECT TO THE USE OF THE SOFTWARE OR DOCUMENTATION. Under no circumstances
 * shall University be liable for incidental, special, indirect, direct or
 * consequential damages or loss of profits, interruption of business, or
 * related expenses which may arise from use of Software or Documentation,
 * including but not limited to those resulting from defects in Software and/or
 * Documentation, or loss or inaccuracy of data of any kind.
 */

#include "katana/analytics/skip_gram/skip_gram.h"

#include "katana/analytics/Utils.h"

using namespace katana::analytics;

class HuffmanCoding {
public:
  /** Node */
  class HuffmanNode {
  public:
    HuffmanNode() : idx_(0), count_(0), code_len_(0), token_(0) {}

    HuffmanNode(uint32_t idx, uint32_t count, uint32_t code_len, uint32_t token)
        : idx_(idx), count_(count), code_len_(code_len), token_(token) {}

    void InitCode(std::vector<uint32_t>& code) {
      code_.resize(code_len_);
      for (uint32_t i = 0; i < code_len_; i++) {
        code_[code_len_ - i - 1] = code[i];
      }
    }

    void InitPoints(std::vector<int32_t>& points, uint32_t num_tokens) {
      point_.resize(code_len_ + 1);
      point_[0] = num_tokens - 2;

      for (uint32_t i = 0; i < code_len_; i++) {
        point_[code_len_ - i] = points[i] - num_tokens;
      }
    }

    void InitVars(
        uint32_t idx, uint32_t count, uint32_t code_len, uint32_t token) {
      idx_ = idx;
      count_ = count;
      code_len_ = code_len;
      token_ = token;
    }

    uint32_t GetIdx() { return idx_; }

    uint32_t GetCount() { return count_; }

    uint32_t GetCodeLen() { return code_len_; }

    int32_t GetPoint(uint32_t idx) { return point_[idx]; }

    uint32_t GetCode(uint32_t idx) { return code_[idx]; }

  private:
    /** vector of 0's and 1's */
    std::vector<uint32_t> code_;
    /** vector of parent node index offsets */
    std::vector<int32_t> point_;
    /** Index of the Huffman node */
    uint32_t idx_;
    /** Frequency of the token */
    uint32_t count_;

    uint32_t code_len_;

    uint32_t token_;
  };

  HuffmanCoding(
      std::set<uint32_t>* vocab,
      katana::gstl::Map<uint32_t, uint32_t>* vocab_multiset)
      : vocab_(vocab), vocab_multiset_(vocab_multiset) {}

  /**
         * @return Map from each given token to a  HuffmanNode
         */
  void Encode(
      std::map<uint32_t, HuffmanNode*>* huffman_node_map,
      std::vector<HuffmanCoding::HuffmanNode>* huffman_nodes) {
    num_tokens_ = vocab_->size();

    parent_node_.reserve(num_tokens_ * 2 + 1);
    binary_.reserve(num_tokens_ * 2 + 1);
    count_.resize(num_tokens_ * 2 + 1, (unsigned long)100000000000000);

    uint32_t idx = 0;

    for (auto item : *vocab_) {
      count_[idx] = (*vocab_multiset_)[item];
      idx++;
    }

    CreateTree();

    EncodeTree(huffman_node_map, huffman_nodes);
  }

  /**
         * Populate the count, binary, and parentNode arrays with the Huffman tree
         * This uses the linear time method assuming that the count array is sorted
         */
  void CreateTree() {
    uint32_t min1i;
    uint32_t min2i;
    int32_t pos1 = num_tokens_ - 1;
    int32_t pos2 = num_tokens_;

    uint32_t new_node_idx;

    // Construct the Huffman tree by adding one node at a time
    for (uint32_t idx = 0; idx < (num_tokens_ - 1); idx++) {
      // First, find two smallest nodes 'min1, min2'
      if (pos1 >= 0) {
        if (count_[pos1] < count_[pos2]) {
          min1i = pos1;
          pos1--;
        } else {
          min1i = pos2;
          pos2++;
        }
      } else {
        min1i = pos2;
        pos2++;
      }

      if (pos1 >= 0) {
        if (count_[pos1] < count_[pos2]) {
          min2i = pos1;
          pos1--;
        } else {
          min2i = pos2;
          pos2++;
        }
      } else {
        min2i = pos2;
        pos2++;
      }

      new_node_idx = num_tokens_ + idx;
      count_[new_node_idx] = count_[min1i] + count_[min2i];
      parent_node_[min1i] = new_node_idx;
      parent_node_[min2i] = new_node_idx;
      binary_[min2i] = 1;
    }
  }

  /** @return Ordered map from each token to its {@link HuffmanNode}, ordered by frequency descending */
  void EncodeTree(
      std::map<uint32_t, HuffmanNode*>* huffman_nodes_map,
      std::vector<HuffmanCoding::HuffmanNode>* huffman_nodes) {
    uint32_t node_idx = 0;
    uint32_t cur_node_idx;

    std::vector<uint32_t> code;
    std::vector<int32_t> points;

    uint32_t code_len;
    uint32_t count;

    for (auto e : *vocab_) {
      cur_node_idx = node_idx;
      code.clear();
      points.clear();

      while (true) {
        code.push_back(binary_[cur_node_idx]);
        points.push_back(cur_node_idx);
        cur_node_idx = parent_node_[cur_node_idx];
        if (cur_node_idx == (num_tokens_ * 2 - 2)) {
          break;
        }
      }
      code_len = code.size();
      count = (*vocab_multiset_)[e];

      HuffmanNode* huffman_node = &((*huffman_nodes)[node_idx]);

      huffman_node->InitVars(node_idx, count, code_len, e);
      huffman_node->InitCode(code);
      huffman_node->InitPoints(points, num_tokens_);

      huffman_nodes_map->insert(std::make_pair(e, huffman_node));

      node_idx++;
    }
  }

private:
  std::set<uint32_t>* vocab_;

  katana::gstl::Map<uint32_t, uint32_t>* vocab_multiset_;

  uint32_t num_tokens_;

  std::vector<uint32_t> parent_node_;

  std::vector<uint32_t> binary_;

  std::vector<unsigned long> count_;
};

class SkipGramModelTrainer {
private:
  /** Boundary for maximum exponent allowed */
  const static int32_t kMaxExp = 6;

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
      std::map<uint32_t, HuffmanCoding::HuffmanNode*>& huffman_nodes_map) {
    embedding_size_ = embedding_size;
    initial_learning_rate_ = alpha;
    window_ = window;
    down_sample_rate_ = down_sample_rate;
    hierarchical_softmax_ = hierarchical_softmax;
    negative_samples_ = num_neg_samples;
    iterations_ = num_iterations;
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
          syn0_[idx].resize(embedding_size_, 0.0f);
          syn1_[idx].resize(embedding_size_, 0.0f);
          syn1_neg_[idx].resize(embedding_size_, 0.0f);
        });

    alpha_ = initial_learning_rate_;
    InitializeSyn0();
    InitializeUnigramTable(huffman_nodes_map);
  }

  double GetSyn0(uint32_t node_idx, uint32_t idx) {
    return syn0_[node_idx][idx];
  }

  //initialize exp_table
  void InitExpTable() {
    for (uint32_t i = 0; i < kExpTableSize; i++) {
      // Precompute the exp() table
      exp_table_[i] = std::exp((i / (double)kExpTableSize * 2 - 1) * kMaxExp);
      // Precompute f(x) = x / (x + 1)
      exp_table_[i] /= exp_table_[i] + 1;
    }
  }

  //initialize table_
  void InitializeUnigramTable(
      std::map<unsigned int, HuffmanCoding::HuffmanNode*>& huffman_nodes_map) {
    katana::GAccumulator<double> train_words_pow;
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
    double d1 = pow(last_node->GetCount(), power) / train_words_pow.reduce();
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

        d1 += std::pow(next_node->GetCount(), power) / train_words_pow.reduce();
        last_node = next_node;
      }

      if (i >= vocab_size_) {
        i = vocab_size_ - 1;
      }
    }
  }

  //randomly initializes the embeddings
  void InitializeSyn0() {
    next_random_ = 1;
    for (uint32_t a = 0; a < vocab_size_; a++) {
      for (uint32_t b = 0; b < embedding_size_; b++) {
        next_random_ = IncrementRandom(next_random_);
        syn0_[a][b] = (((next_random_ & 0xFFFF) / (double)65536) - 0.5f) /
                      embedding_size_;
      }
    }
  }

  /** @return Next random value to use */
  static unsigned long long IncrementRandom(unsigned long long r) {
    return r * (unsigned long long)25214903917L + 11;
  }

  /**
	  * Degrades the learning rate (alpha) steadily towards 0
	  */
  void UpdateAlpha() {
    current_actual_ += word_count_ - last_word_count_;
    last_word_count_ = word_count_;

    // Degrade the learning rate linearly towards 0 but keep a minimum
    alpha_ =
        initial_learning_rate_ *
        std::max(
            1 - current_actual_ / (double)(iterations_ * num_trained_tokens_),
            (double)0.0001f);
  }

  //Train a pair of target and sample nodes
  void TrainSample(
      unsigned int target, unsigned int sample,
      std::map<uint32_t, HuffmanCoding::HuffmanNode*>& huffman_nodes_map,
      unsigned long long* next_random) {
    HuffmanCoding::HuffmanNode* huffman_node =
        huffman_nodes_map.find(target)->second;

    std::vector<double> neu1e;
    neu1e.resize(embedding_size_, 0.0);

    uint32_t l1 = huffman_nodes_map.find(sample)->second->GetIdx();

    if (hierarchical_softmax_) {
      uint32_t huffman_node_code_len = huffman_node->GetCodeLen();

      for (uint32_t d = 0; d < huffman_node_code_len; d++) {
        double f = 0.0f;
        uint32_t l2 = huffman_node->GetPoint(d);

        for (uint32_t e = 0; e < embedding_size_; e++) {
          f += syn0_[l1][e] * syn1_[l2][e];
        }

        if ((f <= -kMaxExp) || (f >= kMaxExp)) {
          continue;
        } else {
          f = exp_table_[(uint32_t)(
              (f + (double)kMaxExp) *
              (((double)kExpTableSize) / ((double)kMaxExp) / 2))];
        }

        double g = (1.0 - huffman_node->GetCode(d) - f) * alpha_;

        for (uint32_t e = 0; e < embedding_size_; e++) {
          neu1e[e] += g * syn1_[l2][e];
        }

        // Learn weights hidden -> output

        for (uint32_t e = 0; e < embedding_size_; e++) {
          katana::atomicAdd(syn1_[l2][e], g * syn0_[l1][e]);
        }
      }
    }

    HandleNegativeSampling(*huffman_node, l1, &neu1e, next_random);

    // Learn weights input -> hidden
    for (uint32_t d = 0; d < embedding_size_; d++) {
      katana::atomicAdd(syn0_[l1][d], neu1e[d]);
    }
  }

  //Train random walks
  void Train(
      std::vector<std::vector<uint32_t>>& random_walks,
      std::map<uint32_t, HuffmanCoding::HuffmanNode*>& huffman_nodes_map,
      katana::gstl::Map<uint32_t, uint32_t>& vocab_multiset) {
    katana::GAccumulator<uint64_t> accum;
    uint32_t word_count = word_count_;
    katana::do_all(
        katana::iterate(random_walks), [&](std::vector<uint32_t>& walk) {
          if (katana::ThreadPool::getTID() == 0) {
            word_count_ = word_count + accum.reduce();
            if (word_count_ - last_word_count_ > kLearningRateUpdateFrequency) {
              UpdateAlpha();
            }
          }

          unsigned long long next_random = next_random_;
          std::vector<uint32_t> refined_walk;
          refined_walk.reserve(walk.size());
          accum += walk.size();

          RefineWalk(walk, &refined_walk, vocab_multiset, &next_random);

          uint32_t sentence_position = 0;
          uint32_t walk_length = refined_walk.size();

          while (sentence_position < walk_length) {
            uint32_t target = refined_walk[sentence_position];
            next_random = IncrementRandom(next_random);

            uint32_t b = next_random % window_;
            for (uint32_t a = b; a < (window_ * 2 + 1 - b); a++) {
              if (a != window_) {
                int32_t c = sentence_position - window_ + a;
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

    word_count_ = word_count + accum.reduce();
  }

  //generate random negative samples
  void HandleNegativeSampling(
      HuffmanCoding::HuffmanNode& huffman_node, uint32_t l1,
      std::vector<double>* neu1e, unsigned long long* next_random) {
    for (uint32_t d = 0; d <= negative_samples_; d++) {
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

      uint32_t l2 = target;
      double f = 0.0;
      for (uint32_t c = 0; c < embedding_size_; c++) {
        f += syn0_[l1][c] * syn1_neg_[l2][c];
      }
      double g;
      if (f > kMaxExp) {
        if (label == 0) {
          g = -alpha_;
        } else {
          g = 0.0;
        }
        // g = ((double)(label - 1)) * alpha_;

      } else if (f < -kMaxExp) {
        if (label == 0) {
          g = 0.0;
        } else {
          g = alpha_;
        }
        //g = ((double)(label - 0)) * alpha_;
      } else {
        g = ((double)label -
             exp_table_[(uint32_t)(
                 (f + (double)kMaxExp) *
                 ((double)kExpTableSize / ((double)kMaxExp * 2.0)))]) *
            alpha_;
      }

      for (uint32_t c = 0; c < embedding_size_; c++) {
        (*neu1e)[c] += g * syn1_neg_[l2][c];
      }
      for (uint32_t c = 0; c < embedding_size_; c++) {
        katana::atomicAdd(syn1_neg_[l2][c], g * syn0_[l1][c]);
      }
    }
  }

  //construct a new walk/sentence by downsampling
  //most frequently occurring nodes
  void RefineWalk(
      std::vector<uint32_t>& walk, std::vector<uint32_t>* refined_walk,
      katana::gstl::Map<uint32_t, uint32_t>& vocab_multiset,
      unsigned long long* next_random) {
    for (auto val : walk) {
      uint32_t count = vocab_multiset[val];
      if (down_sample_rate_ > 0) {
        double ran =
            (std::sqrt(
                 count / (down_sample_rate_ * ((double)num_trained_tokens_))) +
             1) *
            (down_sample_rate_ * ((double)num_trained_tokens_)) /
            ((double)count);
        (*next_random) = IncrementRandom(*next_random);
        if (ran < ((*next_random) & 0xFFFF) / (double)65536) {
          continue;
        }
      }
      refined_walk->push_back(val);
    }
  }
};

void
ReadRandomWalks(
    std::ifstream& input_file,
    std::vector<std::vector<uint32_t>>* random_walks) {
  std::string line;

  while (std::getline(input_file, line)) {
    std::vector<uint32_t> walk;
    std::stringstream ss(line);

    uint32_t val;

    while (ss >> val) {
      walk.push_back(val);
    }

    random_walks->push_back(std::move(walk));
  }
}

//builds a vocabulary of nodes using the
//provided random walks
void
BuildVocab(
    std::vector<std::vector<uint32_t>>& random_walks, std::set<uint32_t>* vocab,
    katana::gstl::Map<uint32_t, uint32_t>& vocab_multiset,
    uint32_t* num_trained_tokens, uint32_t minimum_frequency) {
  for (auto walk : random_walks) {
    for (auto val : walk) {
      vocab->insert(val);
      (*num_trained_tokens)++;
    }
  }
  using Map = katana::gstl::Map<uint32_t, uint32_t>;

  auto reduce = [](Map& lhs, Map&& rhs) -> Map& {
    Map v{std::move(rhs)};

    for (auto& kv : v) {
      if (lhs.count(kv.first) == 0) {
        lhs[kv.first] = 0;
      }
      lhs[kv.first] += kv.second;
    }

    return lhs;
  };

  auto mapIdentity = []() { return Map(); };

  auto accumMap = katana::make_reducible(reduce, mapIdentity);

  katana::do_all(
      katana::iterate(random_walks),
      [&](const std::vector<uint32_t>& walk) {
        for (auto val : walk) {
          accumMap.update(Map{std::make_pair(val, 1)});
        }
      },
      katana::loopname("countFrequency"));

  vocab_multiset = accumMap.reduce();

  std::vector<uint32_t> to_remove;
  std::set<uint32_t>::iterator iter = vocab->begin();

  //remove nodes occurring less than minCount times
  while (iter != vocab->end()) {
    uint32_t node = *iter;
    if (vocab_multiset[node] < minimum_frequency) {
      to_remove.push_back(node);
    }

    iter++;
  }

  for (auto node : to_remove) {
    vocab->erase(node);
    vocab_multiset.erase(node);
  }
}

//constructs a new set of random walks
//by pruning nodes (from the walks)
//that are not in the vocabulary
void
RefineRandomWalks(
    std::vector<std::vector<uint32_t>>& random_walks,
    std::vector<std::vector<uint32_t>>* refined_random_walks,
    std::set<uint32_t>& vocab) {
  for (auto walk : random_walks) {
    std::vector<uint32_t> w;
    for (auto val : walk) {
      if (vocab.find(val) != vocab.end()) {
        w.push_back(val);
      }
    }
    refined_random_walks->push_back(std::move(w));
  }
}

//outputs the embedding to a vector
std::vector<std::pair<uint32_t, std::vector<double>>>
StoreEmbeddings(
    std::map<unsigned int, HuffmanCoding::HuffmanNode*>& huffman_nodes,
    SkipGramModelTrainer& skip_gram_model_trainer, uint32_t max_id,
    uint32_t embedding_size) {
  HuffmanCoding::HuffmanNode* node;
  uint32_t node_idx;

  std::vector<std::pair<uint32_t, std::vector<double>>> out_vector;

  for (uint32_t id = 1; id <= max_id; id++) {
    if (huffman_nodes.find(id) != huffman_nodes.end()) {
      node = huffman_nodes.find(id)->second;
      node_idx = node->GetIdx();

      std::vector<double> embedding_vector;
      embedding_vector.resize(embedding_size, 0.0);

      for (uint32_t i = 0; i < embedding_size; i++) {
        embedding_vector[i] = skip_gram_model_trainer.GetSyn0(node_idx, i);
      }

      out_vector.push_back(std::make_pair(id, std::move(embedding_vector)));
    }
  }

  return out_vector;
}

katana::Result<std::vector<std::pair<uint32_t, std::vector<double>>>>
katana::analytics::SkipGram(
    const std::string& input_file, SkipGramPlan plan, uint32_t embedding_size,
    double alpha, uint32_t window, double down_sample_rate,
    bool hierarchical_softmax, uint32_t num_neg_samples,
    uint32_t num_iterations, uint32_t minimum_frequency) {
  if (plan.algorithm() != SkipGramPlan::kSkipGram) {
    return katana::ErrorCode::InvalidArgument;
  }
  std::ifstream input(input_file.c_str());

  std::vector<std::vector<uint32_t>> random_walks;

  ReadRandomWalks(input, &random_walks);

  std::set<uint32_t> vocab;
  katana::gstl::Map<uint32_t, uint32_t> vocab_multiset;

  uint32_t num_trained_tokens;

  BuildVocab(
      random_walks, &vocab, vocab_multiset, &num_trained_tokens,
      minimum_frequency);

  std::vector<std::vector<uint32_t>> refined_random_walks;

  RefineRandomWalks(random_walks, &refined_random_walks, vocab);

  HuffmanCoding huffman_coding(&vocab, &vocab_multiset);

  std::vector<HuffmanCoding::HuffmanNode> huffman_nodes;
  huffman_nodes.resize(vocab.size());

  std::map<uint32_t, HuffmanCoding::HuffmanNode*> huffman_nodes_map;
  huffman_coding.Encode(&huffman_nodes_map, &huffman_nodes);

  SkipGramModelTrainer skip_gram_model_trainer(
      embedding_size, alpha, window, down_sample_rate, hierarchical_softmax,
      num_neg_samples, num_iterations, vocab.size(), num_trained_tokens,
      huffman_nodes_map);

  skip_gram_model_trainer.InitExpTable();

  for (uint32_t iter = 0; iter < num_iterations; iter++) {
    skip_gram_model_trainer.Train(
        refined_random_walks, huffman_nodes_map, vocab_multiset);
  }

  uint32_t max_id = *(vocab.crbegin());

  return StoreEmbeddings(
      huffman_nodes_map, skip_gram_model_trainer, max_id, embedding_size);
}
