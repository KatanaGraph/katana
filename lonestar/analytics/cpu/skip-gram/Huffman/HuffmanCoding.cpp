#include "HuffmanCoding.h"

#include <iostream>
#include <map>
#include <set>
#include <string>
#include <vector>

/**
	 * @return {@link Map} from each given token to a {@link HuffmanNode} 
	 */
void
HuffmanCoding::Encode(
    std::map<uint32_t, HuffmanCoding::HuffmanNode*>* huffman_node_map,
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
void
HuffmanCoding::CreateTree() {
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

void
HuffmanCoding::HuffmanNode::InitVars(
    uint32_t idx, uint32_t count, uint32_t code_len, uint32_t token) {
  idx_ = idx;
  count_ = count;
  code_len_ = code_len;
  token_ = token;
}

void
HuffmanCoding::HuffmanNode::InitCode(std::vector<uint32_t>& code) {
  code_.resize(code_len_);
  for (uint32_t i = 0; i < code_len_; i++) {
    code_[code_len_ - i - 1] = code[i];
  }
}

void
HuffmanCoding::HuffmanNode::InitPoints(
    std::vector<int32_t>& points, uint32_t num_tokens) {
  point_.resize(code_len_ + 1);
  point_[0] = num_tokens - 2;

  for (uint32_t i = 0; i < code_len_; i++) {
    point_[code_len_ - i] = points[i] - num_tokens;
  }
}

/** @return Ordered map from each token to its {@link HuffmanNode}, ordered by frequency descending */
void
HuffmanCoding::EncodeTree(
    std::map<uint32_t, HuffmanCoding::HuffmanNode*>* huffman_node_map,
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

    HuffmanNode* huffman_node = &((*huffman_nodes)[cur_node_idx]);

    huffman_node->InitVars(node_idx, count, code_len, e);
    huffman_node->InitCode(code);
    huffman_node->InitPoints(points, num_tokens_);

    huffman_node_map->insert(std::make_pair(e, huffman_node));

    node_idx++;
  }
}
