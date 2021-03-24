#include <math.h>

//#include "../Huffman/HuffmanCoding.h"
#include "NeuralNetworkTrainer.h"


#include "galois/graphs/Util.h"
#include "galois/Timer.h"
#include "Lonestar/BoilerPlate.h"
#include "galois/graphs/FileGraph.h"
#include "galois/LargeArray.h"

#include "galois/AtomicHelpers.h"
#include "galois/AtomicWrapper.h"

class SkipGramModelTrainer : public NeuralNetworkTrainer {
public:

	//	HuffmanCoding::HuffmanNode* huffmanNode;
//int c, l1, d, l2, e;
//double f, g;


	galois::GAccumulator<double>** sum_syn0;
	galois::GAccumulator<double>** sum_syn1;	
	SkipGramModelTrainer(std::multiset<unsigned int>* counts, std::map<unsigned int, HuffmanCoding::HuffmanNode*>* huffmanNodes) : NeuralNetworkTrainer(counts, huffmanNodes) {

	/*	this->sum_syn0 = new galois::GAccumulator<double>*[vocabSize+1];
    this->sum_syn1 = new galois::GAccumulator<double>*[vocabSize+1];

		for(int i=0;i<=vocabSize; i++){

      this->sum_syn0[i] = new galois::GAccumulator<double>[LAYER1_SIZE];
      this->sum_syn1[i] = new galois::GAccumulator<double>[LAYER1_SIZE];
		}
*/
//		createAccumulator();
	}

	void createAccumulator(){

		this->sum_syn0 = new galois::GAccumulator<double>*[vocabSize+1];
    this->sum_syn1 = new galois::GAccumulator<double>*[vocabSize+1];

    for(int i=0;i<=vocabSize; i++){

      this->sum_syn0[i] = new galois::GAccumulator<double>[LAYER1_SIZE];
      this->sum_syn1[i] = new galois::GAccumulator<double>[LAYER1_SIZE];
    }
	}

	void freeAccumulator(){

		for(int i=0;i<=vocabSize; i++){

      free (this->sum_syn0[i]);
			free (this->sum_syn1[i]);
    }	

		free (this->sum_syn0);
		free (this->sum_syn1);
	}
		
	std::array<unsigned int, LAYER1_SIZE> index;
	
	void initArray(){

		for(int i=0;i<LAYER1_SIZE;i++)
			index[i] = i;
	}		

	void trainSample(unsigned int target, unsigned int sample) {
				
				HuffmanCoding::HuffmanNode*	huffmanNode =  huffmanNodes->find(target)->second;

				double neu1e[LAYER1_SIZE];
		
				for(int c=0;c<LAYER1_SIZE;c++)
					neu1e[c] = 0;
					
				int l1 = huffmanNodes->find(sample)->second->idx;
	//			if(target == 4)
//					std::cout << "size code: " << huffmanNode->codeLen << std::endl;					
				for(int d = 0; d < huffmanNode->codeLen; d++) {
							
					double f = 0.0f;
					int l2 = huffmanNode->point[d];
			
					for(int e=0;e<LAYER1_SIZE;e++)
						f += syn0[l1][e] * syn1[l2][e];
					
				
					if (f <= -MAX_EXP || f >= MAX_EXP)
						continue;
					else{
							f = EXP_TABLE[(int)((f + MAX_EXP) * (EXP_TABLE_SIZE / MAX_EXP / 2))];
					}
					
					double g = (1 - huffmanNode->code[d] - f) * alpha;
				
					for(int e=0;e<LAYER1_SIZE;e++)
						neu1e[e] += g * syn1[l2][e];
							
					// Learn weights hidden -> output
					
					for(int e=0;e<LAYER1_SIZE;e++)
						galois::atomicAdd(syn1[l2][e],g * syn0[l1][e]);		
				
				}
				
					
				//handleNegativeSampling(huffmanNode, l1);
					
				// Learn weights input -> hidden
				for(int d=0;d<LAYER1_SIZE;d++)
					galois::atomicAdd(syn0[l1][d], neu1e[d]);
		
			
		
	}

	/*void sync(){
		
		for(int i=0;i<=vocabSize;i++){

			for(int j=0;j<LAYER1_SIZE;j++){
				
				syn0[i][j] += sum_syn0[i][j].reduce();
				syn1[i][j] += sum_syn1[i][j].reduce();			
			}
		}		
	}
*/
	/*void resetAccumulator(){

    //this->sum_syn0 = new galois::GAccumulator<double>*[vocabSize+1];
    //this->sum_syn1 = new galois::GAccumulator<double>*[vocabSize+1];

    for(int i=0;i<=vocabSize; i++){

			for(int j=0;j<LAYER1_SIZE; j++){
      this->sum_syn0[i][j].reset();
      this->sum_syn1[i][j].reset();
			}
    }
  }
	*/
	galois::CopyableAtomic<double>** train(std::vector<std::pair<unsigned int, unsigned int>>* samples){

								//createAccumulator();
//								resetAccumulator();
                int numSamples = samples->size();
                numTrainedTokens += numSamples;

								galois::GAccumulator<unsigned int> accum;
								galois::do_all(galois::iterate(*samples),
								[&] (std::pair<unsigned int, unsigned int> p){
								
										unsigned int target, sample;
										target = p.first;
                    sample = p.second;

										accum += 1;
		
										trainSample(target, sample);
								}								
								);
								

									wordCount += accum.reduce();
								if (wordCount - lastWordCount > LEARNING_RATE_UPDATE_FREQUENCY) {
                                updateAlpha(0);
								}
	
	//								sync();
								
							//	freeAccumulator();
                return syn0;
        }

};
