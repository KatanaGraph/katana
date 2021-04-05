#include <math.h>

//#include "../Huffman/HuffmanCoding.h"
#include "NeuralNetworkTrainer.h"
#include "SkipGramModelTrainer.h"

//#include "katana/Util.h"
#include "katana/Timer.h"
#include "Lonestar/BoilerPlate.h"
#include "katana/FileGraph.h"
#include "katana/LargeArray.h"

#include "katana/AtomicHelpers.h"
#include "katana/AtomicWrapper.h"

		
/*	std::array<unsigned int, LAYER1_SIZE> index;
	
	void initArray(){

		for(int i=0;i<LAYER1_SIZE;i++)
			index[i] = i;
	}		
*/
	void SkipGramModelTrainer::TrainSample(unsigned int target, unsigned int sample, std::map<uint32_t, HuffmanCoding::HuffmanNode*>& huffman_nodes_mapi, unsigned long long* next_random) {
				
				HuffmanCoding::HuffmanNode*	huffman_node =  huffman_nodes_map->find(target)->second;

				std::vector<double> neu1e;
				neu1e.resize(kLayer1Size, 0.0);
					
				uint32_t l1 = huffman_nodes_map->find(sample)->second->idx;
		
				uint32_t huffman_node_code_len = huffman_node->GetCodeLen();

				for(uint32_t d = 0; d < huffman_node_code_len; d++) {
							
					double f = 0.0f;
					uint32_t l2 = huffman_node->GetPoint(d);
			
					for(uint32_t e=0; e<kLayer1Size;e++) {
						f += syn0_[l1][e] * syn1_[l2][e];
					}
					
				
					if ((f <= -kMaxExp) || (f >= kMaxExp)) {
						continue;
					}
					else{
							f = exp_table_[(uint32_t)((f + kMaxExp) * (kExpTableSize / kMaxExp / 2))];
					}
					
					double g = (1.0 - huffman_node->GetCode(d) - f) * alpha_;
				
					for(uint32_t e=0; e<kLayer1Size ; e++){
						neu1e[e] += g * syn1_[l2][e];
					}
							
					// Learn weights hidden -> output
					
					for(uint32_t e=0 ; e<kLayer1Size ;e++){
						katana::atomicAdd(syn1_[l2][e],g * syn0_[l1][e]);		
					}
				
				}
				
					
				HandleNegativeSampling(huffman_node, l1, next_random);
					
				// Learn weights input -> hidden
				for(uint32_t d=0; d<kLayer1Size ;d++) {
					katana::atomicAdd(syn0_[l1][d], neu1e[d]);
				}
		
			
		
	}

	void SkipGramModelTrainer::Train(std::vector<std::vector<uint32_t>>& random_walks, std::map<uint32_t, uint32_t>& vocab_multiset, std::map<uint32_t, HuffmanCoding::HuffmanNode*>& huffman_nodes_map) {

             
								katana::GAccumulator<uint64_t> accum;
								katana::do_all(katana::iterate(random_walks),
								[&] (std::vector<uint32_t>& walk){

									uint32_t sentence_position = 0;
									uint32_t walk_length = walk.size();

									unsigned long long next_random = next_random_;

									while(sentence_position < walk_length) {
										accum += 1;
										uint32_t target = walk[sentence_position];
									next_random = 	IncrementRandom(next_random);

									uint32_t b = next_random % kWindow;							
									for (uint32_t a = b; a < (kWindow * 2 + 1 - b); a++) {
									
										if (a != kWindow) {
											c = sentence_position - kWindow + a;
											if (c < 0){
										       		continue;
												}
											if (c >= walk_length) {
												continue;
											}
											uint32_t sample = walk[c];
											TrainSample(target, sample, huffman_nodes_map, &next_random);
										}
									}

									sentence_position++;
									}
									
										/* if(downSampleRate > 0){
                                std::cout << "down sampling\n";
                
                                HuffmanCoding::HuffmanNode huffmanNode = *huffmanNodes[target];
                                double random = (sqrt(huffmanNode.count/(downSampleRate * numTrainedTokens)) + 1) * (downSampleRate * numTrainedTokens)/huffmanNode.count;
                                nextRandom = incrementRandom(nextRandom);
                                if (random < (nextRandom & 0xFFFF) / (double)65536){
                                    continue;
                                }
                        }*/
								});
								

									word_count_ += accum.reduce();
								if (word_count_ - last_word_count_ > kLearningRateUpdateFrequency) {
                                					UpdateAlpha(0);
								}
        }
