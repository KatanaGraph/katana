
#include <math.h>
#include <algorithm>

#include "../Huffman/HuffmanCoding.h"
#include "galois/AtomicHelpers.h"
#include "galois/AtomicWrapper.h"

/** Parent class for training word2vec's neural network */
class NeuralNetworkTrainer {	
public:
	/** Boundary for maximum exponent allowed */
	const static int MAX_EXP = 6;

	const static int MAX_QW = 100000000;	
	/** Size of the pre-cached exponent table */
	const  static int EXP_TABLE_SIZE = 1000;

	double EXP_TABLE[EXP_TABLE_SIZE];
	
	void initExpTable();
	
	const static int TABLE_SIZE = (int) 100000000;
	
	std::map<unsigned int, HuffmanCoding::HuffmanNode*>* huffmanNodes;
	int vocabSize;
	const static int LAYER1_SIZE = 200;
	
	/** 
	 * In the C version, this includes the </s> token that replaces a newline character
	 */
	int numTrainedTokens;

	//int counts[2000][2000];	
	/* The following includes shared state that is updated per worker thread */
	
	/** 
	 * To be precise, this is the number of words in the training data that exist in the vocabulary
	 * which have been processed so far.  It includes words that are discarded from sampling.
	 * Note that each word is processed once per iteration.
	 */
	long int actualWordCount;

	double alpha;
	/** Learning rate, affects how fast values in the layers get updated */
	constexpr static double initialLearningRate = 0.025f;
	//constexpr static double initialLearningRate = 0.05f;
	/** 
	 * This contains the outer layers of the neural network
	 * First dimension is the vocab, second is the layer
	 */
	galois::CopyableAtomic<double>** syn0;
	/** This contains hidden layers of the neural network */
	galois::CopyableAtomic<double>** syn1;
	/** This is used for negative sampling */
	double** syn1neg;
	/** Used for negative sampling */
	int table[TABLE_SIZE];
	long startNano;

	const static int negativeSamples = 0;
	                
//	double neu1[LAYER1_SIZE];
// 	double neu1e[LAYER1_SIZE];
	
	/** 
 	** The number of words observed in the training data for this worker that exist
 	** in the vocabulary.  It includes words that are discarded from sampling.
 	**/
       	int wordCount;
       	/** Value of wordCount the last time alpha was updated */
        int lastWordCount;	

	const static int iterations = 0;

	constexpr static double downSampleRate = -0.001f;

	unsigned long long nextRandom;

	const static int LEARNING_RATE_UPDATE_FREQUENCY = 100000;

	int currentActual;

	NeuralNetworkTrainer(std::multiset<unsigned int>* vocab, std::map<unsigned int, HuffmanCoding::HuffmanNode*>* huffmanNodes);
	
	void initializeUnigramTable();

	void initializeSyn0();
	
	/** @return Next random value to use */
	static unsigned long long incrementRandom(unsigned long long r);
	
	void trainSample (unsigned int target, unsigned int sample);

	/** @return Trained NN model */
	std::array<double*, LAYER1_SIZE> train(std::vector<std::pair<unsigned int, unsigned int>>* samples);

	/*void setDownSampleRate(double downSampleRate){
		this.downSampleRate = downSampleRate;
	}*/

		
		/*void setIterations(int iterations){
			this.iterations = iterations;
		}*/

		/** 
		 * Degrades the learning rate (alpha) steadily towards 0
		 * @param iter Only used for debugging
		 */
		void updateAlpha(int iter);
		

		/*void setNegativeSamples(int negativeSamples){
			this->negativeSamples = negativeSamples;
		}*/

		//generate random negative samples
		void handleNegativeSampling(HuffmanCoding::HuffmanNode huffmanNode, int l1);
		
};
