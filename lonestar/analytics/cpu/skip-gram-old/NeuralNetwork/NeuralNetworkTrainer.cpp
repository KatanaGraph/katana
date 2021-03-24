//#pragma once

#include <math.h>
#include <algorithm>
#include "../Huffman/HuffmanCoding.h"
#include "NeuralNetworkTrainer.h"
#include "galois/AtomicWrapper.h"

	void NeuralNetworkTrainer::initExpTable(){
		for (int i = 0; i < EXP_TABLE_SIZE; i++) {
			// Precompute the exp() table
			EXP_TABLE[i] = exp((i / (double)EXP_TABLE_SIZE * 2 - 1) * MAX_EXP);
			// Precompute f(x) = x / (x + 1)
//			std::cout <<"exp table: " << EXP_TABLE[i] << std::endl;
			EXP_TABLE[i] /= EXP_TABLE[i] + 1;
//			std::cout <<"exp table: " << EXP_TABLE[i] << std::endl;
		}
	}
	
	NeuralNetworkTrainer::NeuralNetworkTrainer(std::multiset<unsigned int>* vocab, std::map<unsigned int, HuffmanCoding::HuffmanNode*>* huffmanNodes) {
		
		this->huffmanNodes = huffmanNodes;
		this->vocabSize = huffmanNodes->size();
		this->numTrainedTokens = vocab->size();
		
		this->syn0 = new galois::CopyableAtomic<double>*[vocabSize+1];
		this->syn1 = new galois::CopyableAtomic<double>*[vocabSize+1];
		this->syn1neg = new double*[vocabSize+1];

		for(int i=0;i<=vocabSize; i++){
					
			this->syn0[i] = new galois::CopyableAtomic<double>[LAYER1_SIZE];
			this->syn1[i] = new galois::CopyableAtomic<double>[LAYER1_SIZE];
			this->syn1neg[i] = new double[LAYER1_SIZE];

			for(int j=0;j<LAYER1_SIZE;j++){
				this->syn0[i][j] = (double) 0.0f;
				this->syn1[i][j] = (double) 0.0f;
				this->syn1neg[i][j] = (double) 0.0f;

			}
		}
		
		this->alpha = this->initialLearningRate;

	/*	for(int i=0;i<LAYER1_SIZE;i++){
			neu1[i] = (double) 0.0f;
			neu1e[i] = (double) 0.0f;
		}*/
		initializeSyn0();
		initializeUnigramTable();

//		for(int i=0;i<2000;i++)
//			for(int j=0;j<2000;j++)
//				counts[i][j] = 0;
	}
	
	void NeuralNetworkTrainer::initializeUnigramTable() {
		long trainWordsPow = 0;
		double power = 0.75f;

		int count = 0;		
		for(auto entry : *huffmanNodes){
			if(entry.first > vocabSize)	
				break;
			HuffmanCoding::HuffmanNode node = *(entry.second);
			trainWordsPow += pow(node.count, power);
			count++;
		}

		std::cout << count << " count \n";
		
		std::map<unsigned int, HuffmanCoding::HuffmanNode*>::iterator it = huffmanNodes->begin();
		HuffmanCoding::HuffmanNode last = *((*it).second);
		it++;
		double d1 = pow(last.count, power)/((double) trainWordsPow);
		int i = 0;
		for (int a = 0; a < TABLE_SIZE; a++) {
			table[a] = i;
		//	std::cout << "d1: " << d1 << std::endl;
			if (a / (double)TABLE_SIZE > d1) {
				i++;
				HuffmanCoding::HuffmanNode next = last;
				if(it != huffmanNodes->end()){
					next = *((*it).second);
					it++;
				}
				
				d1 += pow(next.count, power)/((double) trainWordsPow);
				
				last = next;
			}
	
			if(i > vocabSize){ 
				i = vocabSize ;
				std::cout << "hello \n";

			}
		}
	}

	void NeuralNetworkTrainer::initializeSyn0() {
		unsigned long long nextRandom = 1;
		for (int a = 0; a < huffmanNodes->size(); a++) {
			// Consume a random for fun
			// Actually we do this to use up the injected </s> token
			nextRandom = incrementRandom(nextRandom);
			for (int b = 0; b < LAYER1_SIZE; b++) {
				nextRandom = incrementRandom(nextRandom);
				syn0[a][b] = (((nextRandom & 0xFFFF) / (double)65536) - 0.5f)/LAYER1_SIZE;
				//std::cout << syn0[a][b] <<std::endl; 
				if(syn0[a][b] > 10)	std::cout << "hello \n";
				if(a==3)
					std::cout << syn0[a][b] <<  " ";
				if(a == 4)	
					std::cout << syn0[a][b] <<  " ";
				//syn0[a][b] = (double) 0.0f;
			}
		}
	}
	
	/** @return Next random value to use */
	unsigned long long NeuralNetworkTrainer::incrementRandom(unsigned long long r) {
		return r * (unsigned long long) 25214903917L + 11;
	}
	/*void setDownSampleRate(double downSampleRate){
		this.downSampleRate = downSampleR*/

		
		/*void setIterations(int iterations){
			this.iterations = iterations;
		}*/

		/** 
		 * Degrades the learning rate (alpha) steadily towards 0
		 * @param iter Only used for debugging
		 */
		void NeuralNetworkTrainer::updateAlpha(int iter) {
			currentActual = wordCount - lastWordCount;
			lastWordCount = wordCount;
			
			// Degrade the learning rate linearly towards 0 but keep a minimum
			alpha = initialLearningRate * std::max(
					1 - currentActual / (double)(iterations * numTrainedTokens),
					(double) 0.0001f
				);
		}
		

		/*void setNegativeSamples(int negativeSamples){
			this->negativeSamples = negativeSamples;
		}*/

		//generate random negative samples
		void NeuralNetworkTrainer::handleNegativeSampling(HuffmanCoding::HuffmanNode huffmanNode, int l1) {
			for (int d = 0; d <= negativeSamples; d++) {
				int target;
				int label;
				if (d == 0) {
					target = huffmanNode.idx;
					label = 1;
				} else {
					nextRandom = incrementRandom(nextRandom);
					target = table[(int) (((nextRandom >> 16) % TABLE_SIZE) + TABLE_SIZE) % TABLE_SIZE];
					std::cout << "target table: " << target << std::endl;
					if (target == 0){
						target = (int)(((nextRandom % (vocabSize - 1)) + vocabSize - 1) % (vocabSize - 1)) + 1;
						std::cout << "target random: " << target << std::endl;
					}
					if (target == huffmanNode.idx)
						continue;
					label = 0;
				}
				int l2 = target;
				//std::cout << "l2: " << l2  << std::endl;
				long double f = 0;
				for (int c = 0; c < LAYER1_SIZE; c++)
					f += syn0[l1][c] * syn1neg[l2][c];
				double g;
				if (f > MAX_EXP)
					g = (label - 1) * alpha;
				else if (f < -MAX_EXP)
					g = (label - 0) * alpha;
				else{
					//std::cout << "f: " << f << std::endl;
					//std::cout << "inside else: " << (int)((f + (double) MAX_EXP) * ((double) EXP_TABLE_SIZE / ((double) MAX_EXP * 2))) << std::endl;
					g = (label - EXP_TABLE[(int)((f + (double) MAX_EXP) * ((double) EXP_TABLE_SIZE / ((double) MAX_EXP * 2)))]) * alpha;
					
				}

				double neu1e[300];
				for (int c = 0; c < LAYER1_SIZE; c++)
					neu1e[c] += g * syn1neg[l2][c];
				for (int c = 0; c < LAYER1_SIZE; c++)
					syn1neg[l2][c] += g * syn0[l1][c];
			}
		}
		
