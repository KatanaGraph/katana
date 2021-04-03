//#pragma once

#include <math.h>

#include <algorithm>

#include "../Huffman/HuffmanCoding.h"
#include "NeuralNetworkTrainer.h"
#include "katana/AtomicWrapper.h"

	NeuralNetworkTrainer(uint32_t vocab_size){
		vocab_size_ = vocab_size;
	}


	void NeuralNetworkTrainer::InitExpTable(){
		for (uint32_t i = 0; i < kExpTableSize; i++) {
			// Precompute the exp() table
			exp_table_[i] = std::exp((i / (double)kExpTableSize * 2 - 1) * kMaxExp);
			// Precompute f(x) = x / (x + 1)
			exp_table_[i] /= exp_table_[i] + 1;
		}
	}
	
	NeuralNetworkTrainer(uint32_t vocab_size, uint32_t num_trained_tokens, std::map<uint32_t, HuffmanCoding::HuffmanNode*>& huffman_nodes_map) {

		vocab_size_ = vocab_size;
		num_trained_tokens_ = num_trained_tokens;
		
		syn0_.resize(vocab_size_ + 1);
		syn1_.resize(vocab_size_ + 1);
		syn1_neg_.resize(vocab_size_ + 1);

		katana::do_all(
			katana::iterate((uint32_t)0, (uint32_t)(vocab_size_+1)),
			[&](uint32_t idx) {
			
				syn0_[idx].resize(kLayer1Size, 0.0f);
				syn1_[idx].resize(kLayer1Size, 0.0f);
				syn1_neg_[idx].resize(kLayer1Size, 0.0f);

			});
		
		alpha_ = kInitialLearningRate;

		InitializeSyn0();
		InitializeUnigramTable(huffman_nodes_map);
	}
	
	void NeuralNetworkTrainer::InitializeUnigramTable(std::map<uint32_t, HuffmanCoding::HuffmanNode*>& huffman_nodes_map) {
		katana::GAccumulator<long> train_words_pow;
		double power = 0.75f;

		katana::GAccumulator<uint32_t> count;

		katana::do_all(
				katana::iterate(huffman_nodes_map),
				[&](std::pair<uint32_t, HuffmanCoding::HuffmanNode*> pair) {
					if(pair.first > vocab_size_) {
						return;
					}
					HuffmanCoding::HuffmanNode* node = pair.second;
					train_words_pow += std::pow(node->GetCount(), power);
					count++;
				});
		
		auto iter = huffman_nodes_map.begin();
		HuffmanCoding::HuffmanNode* last_node = iter->second;
		iter++;
		double d1 = pow(last_node->GetCount(), power)/((double) train_words_pow.reduce());
		uint32_t i = 0;

		for (uint32_t a = 0; a < kTableSize; a++) {
			table_[a] = i;
	
			if (a / (double)kTableSize > d1) {
				i++;
				HuffmanCoding::HuffmanNode* next = last;
				if(iter != huffman_nodes_map.end()){
					next = iter->second;
					iter++;
				}
				
				d1 += std::pow(next->GetCount(), power)/((double) train_words_pow.reduce());
				last = next;
			}
	
			if(i > vocab_size_){ 
				i = vocab_size_ ;
			}
		}
	}

	void NeuralNetworkTrainer::InitializeSyn0() {
		unsigned long long next_random = 1;
		for (uint32_t a = 0; a < vocab_size_; a++) {
			// Consume a random for fun
			// Actually we do this to use up the injected </s> token
			next_random = IncrementRandom(next_random);
			for (uint32_t b = 0; b < kLayer1Size; b++) {
				next_random = IncrementRandom(next_random);
				syn0_[a][b] = (((next_random & 0xFFFF) / (double)65536) - 0.5f)/kLayer1Size;
			}
		}
	}
	
	/** @return Next random value to use */
	unsigned long long NeuralNetworkTrainer::IncrementRandom(unsigned long long r) {
		return r * (unsigned long long) 25214903917L + 11;
	}
	
		/** 
		 * Degrades the learning rate (alpha) steadily towards 0
		 * @param iter Only used for debugging
		 */
		void NeuralNetworkTrainer::UpdateAlpha(int iter) {
			current_actual_ = word_count_ - last_word_count_;
			last_word_count_ = word_count_;
			
			// Degrade the learning rate linearly towards 0 but keep a minimum
			alpha_ = kInitialLearningRate * std::max(
					1 - current_actual_/ (double)(kIterations * num_trained_tokens_),
					(double) 0.0001f
				);
		}

		//generate random negative samples
		void NeuralNetworkTrainer::HandleNegativeSampling(HuffmanCoding::HuffmanNode* huffman_node, uint32_t l1, std::vector<double>* neu1e) {
			for (uint32_t d = 0; d <= kNegativeSamples; d++) {
				uint32_t target;
				uint32_t label;
				if (d == 0) {
					target = huffman_node.GetIdx();
					label = 1;
				} else {
					next_random = IncrementRandom(next_random);
					target = table_[(uint32_t)((next_random >> 16) % kTableSize)];
					
					if (target == 0){
						target = (uint32_t)(next_random % (vocab_size_ - 1)) + 1;
					}
					if (target == huffman_node->GetIdx()){
						continue;
					}
					label = 0;
				}
				uint32_t l2 = target;
				long double f = 0;
				for (uint32_t c = 0; c < kLayer1Size; c++) {
					f += syn0_[l1][c] * syn1_neg_[l2][c];
				}
				double g;
				if (f > kMaxExp) {
					g = ((double)(label - 1)) * alpha_;
				}
				else if (f < -kMaxEx) {
					g = ((double)(label - 0)) * alpha_;
				}
				else{
					g = ((double)label - exp_table_[(uint32_t)((f + (double) kMaxExp) * ((double) kExpTableSize / ((double) kMaxExp * 2)))]) * alpha_;
				}

				for (uint32_t c = 0; c < kLayer1Size; c++) {
					(*neu1e)[c] += g * syn1_neg_[l2][c];
				}
				for (uint32_t c = 0; c < kLayer1Size; c++) {
					katana::atomicAdd(syn1_neg_[l2][c], g * syn0_[l1][c]);
				}
			}
		}
		
