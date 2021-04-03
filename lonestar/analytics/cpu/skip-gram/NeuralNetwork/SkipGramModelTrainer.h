#pragma once
#include <math.h>

//#include "../Huffman/HuffmanCoding.h"
#include "NeuralNetworkTrainer.h"


class SkipGramModelTrainer : public NeuralNetworkTrainer {
public:

	SkipGramModelTrainer(uint32_t vocab_size, uint32_t num_trained_tokens, std::map<uint32_t, HuffmanCoding::HuffmanNode*>& huffman_nodes_map) : NeuralNetworkTrainer(vocab_size, num_trained_tokens, huffman_nodes_map) {}
		
	void trainSample(unsigned int target, unsigned int sample) {
				
				huffmanNode =  huffmanNodes->find(target)->second;

				for (c = 0; c < LAYER1_SIZE; c++)
					neu1e[c] = 0;
					
				l1 = huffmanNodes->find(sample)->second->idx;
				if(target == 4)
					std::cout << "size code: " << huffmanNode->codeLen << std::endl;					
				for (d = 0; d < huffmanNode->codeLen; d++) {
							
					f = 0.0f;
					l2 = huffmanNode->point[d];
					
					for (e = 0; e < LAYER1_SIZE; e++)
						f += syn0[l1][e] * syn1[l2][e];
				
					if (f <= -MAX_EXP || f >= MAX_EXP)
						continue;
					else{
							f = EXP_TABLE[(int)((f + MAX_EXP) * (EXP_TABLE_SIZE / MAX_EXP / 2))];
					}
					
					g = (1 - huffmanNode->code[d] - f) * alpha;
					
					for (e = 0; e < LAYER1_SIZE; e++)
						neu1e[e] += g * syn1[l2][e];
							
					// Learn weights hidden -> output
					for (e = 0; e < LAYER1_SIZE; e++)
						syn1[l2][e] += g * syn0[l1][e];
				}
				
					
				//handleNegativeSampling(huffmanNode, l1);
					
				// Learn weights input -> hidden
				for (d = 0; d < LAYER1_SIZE; d++) {
					syn0[l1][d] += neu1e[d];
				}
				//counts[target][sample]++;
			
		
	}

	double** train(std::vector<std::pair<unsigned int, unsigned int>>* samples){

                int numSamples = samples->size();
                numTrainedTokens += numSamples;

								unsigned int target, sample;

                for(auto entry: *samples){
                        target = entry.first;
                        sample = entry.second;

                        wordCount++;

												//not using at the moment
                       /* if(downSampleRate > 0){
				std::cout << "down sampling\n";
		
                                HuffmanCoding::HuffmanNode huffmanNode = *huffmanNodes[target];
                                double random = (sqrt(huffmanNode.count/(downSampleRate * numTrainedTokens)) + 1) * (downSampleRate * numTrainedTokens)/huffmanNode.count;
                                nextRandom = incrementRandom(nextRandom);
                                if (random < (nextRandom & 0xFFFF) / (double)65536){
                                    continue;
                                }
                        }*/
			//if(target == 5)
			//	std::cout << "5:\n";
			//if(sample == 5)
			//	std::cout << "5samples: \n";

                        trainSample(target, sample);

                       /* if (wordCount - lastWordCount > LEARNING_RATE_UPDATE_FREQUENCY) {
                                updateAlpha(0);
                        }*/
                }

                return syn0;
        }

};
