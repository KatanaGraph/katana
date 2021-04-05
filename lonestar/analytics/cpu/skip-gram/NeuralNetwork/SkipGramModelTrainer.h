#pragma once
#include <math.h>

//#include "../Huffman/HuffmanCoding.h"
#include "NeuralNetworkTrainer.h"


class SkipGramModelTrainer : public NeuralNetworkTrainer {
	private:
		const static uint32_t kWindow = 5;
	public:

	SkipGramModelTrainer(uint32_t vocab_size, uint32_t num_trained_tokens, std::map<uint32_t, HuffmanCoding::HuffmanNode*>& huffman_nodes_map) : NeuralNetworkTrainer(vocab_size, num_trained_tokens, huffman_nodes_map);
		
	void TrainSample(unsigned int target, unsigned int sample, std::map<uint32_t, HuffmanCoding::HuffmanNode*>& huffman_nodes_map);

	void Train(std::vector<std::vector<uint32_t>>& random_walks, std::map<uint32_t, uint32_t>& vocab_multiset, std::map<uint32_t, HuffmanCoding::HuffmanNode*>& huffman_nodes_map);
};
