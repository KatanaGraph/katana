
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <sstream>
#include <set>
#include <array> 
#include <map>
#include <algorithm>
#include "Huffman/HuffmanCoding.h"
//#include "NeuralNetwork/NeuralNetworkTrainer.h"
#include "NeuralNetwork/SkipGramModelTrainer.cpp"

#include "galois/graphs/Util.h"
#include "galois/Timer.h"
#include "Lonestar/BoilerPlate.h"
#include "galois/graphs/FileGraph.h"
#include "galois/LargeArray.h"

#include "galois/AtomicHelpers.h"
#include "galois/AtomicWrapper.h"

using namespace std;

//namespace cll = llvm::cl;

static const char* name = "Embeddings";
static const char* desc =
    "Generate embeddings";
static const char* url = "embeddings";

int main(int argc, char** argv){

	galois::SharedMemSys G;
  LonestarStart(argc, argv, name, desc, url);

	ifstream myfile("samples.csv");
	
	std::vector<std::pair<unsigned int,unsigned int>>* samples = new std::vector<std::pair<unsigned int,unsigned int>>; 
	std::set<unsigned int>* vocab = new std::set<unsigned int>;
	std::multiset<unsigned int>* vocabMultiset = new std::multiset<unsigned int>;

	std::string line;

	unsigned int target;
 	unsigned int sample;

	unsigned int maxId = 0;
	//reading samples
	while(getline(myfile, line)){
		std::stringstream ss(line);
		
		ss >> target;
		ss >> sample;
		
		samples->push_back(std::make_pair(target, sample));
		
		vocab->insert(target);
		vocab->insert(sample);

		vocabMultiset->insert(target);
		vocabMultiset->insert(sample);
		
		if(target > maxId)	maxId = target;
		if(sample > maxId) 	maxId = sample;

	}
	std::cout << "read all samples\n";

	HuffmanCoding* huffmanCoding = new HuffmanCoding(vocab, vocabMultiset);
	std::cout << "huffman Coding init done\n";

	std::map<unsigned int, HuffmanCoding::HuffmanNode*>* huffmanNodes = huffmanCoding->encode();

	std::cout << "huffman encoding done \n";

/*HuffmanCoding::HuffmanNode* node;
  int l, i;

	for(unsigned int id=1;id<=vocab->size();id++){

		if(huffmanNodes->find(id) != huffmanNodes->end()){
    node = huffmanNodes->find(id)->second;
    l = node->idx;

    std::cout << id <<" " << node->idx << std::endl;
		}
		else
			std::cout << "broken: " << id << std::endl;
	}*/

	std::cout << "testing done\n";

	SkipGramModelTrainer* skipGramModelTrainer = new SkipGramModelTrainer(vocabMultiset, huffmanNodes);

	std::cout << "skip gram trainer init done \n";
	
	skipGramModelTrainer->initArray();

	skipGramModelTrainer->initExpTable();	

	std::cout << "skip gram init exp table \n";
	galois::CopyableAtomic<double>**  syn0;

	int num_iterations = 50;
//	int num_iterations = 1;

	for(int iter =0; iter<num_iterations;iter++){
		//std::random_shuffle(samples.begin(), samples.end());
		syn0 = skipGramModelTrainer->train(samples);		
	}

	ofstream of("embeddings.csv");

	HuffmanCoding::HuffmanNode* node;
  int l, i;
	for(unsigned int id=1;id<=maxId;id++){
	
		if(huffmanNodes->find(id) != huffmanNodes->end()){
			node = huffmanNodes->find(id)->second;
			l = node->idx;

			of << id;
			for(i=0;i<NeuralNetworkTrainer::LAYER1_SIZE;i++)
				of << " " << syn0[l][i];
	
			of << std::endl;
		}
		else
		{
			of << id;
      for(i=0;i<NeuralNetworkTrainer::LAYER1_SIZE;i++)
        of << " " << 0.0f;

      of << std::endl;
		}
	}

	of.close();

/*	
	std::cout <<"4,3:" << skipGramModelTrainer->counts[4][3] << std::endl;
std::cout <<"4,5:" << skipGramModelTrainer->counts[4][5] << std::endl;
std::cout <<"5,4:" << skipGramModelTrainer->counts[5][4] << std::endl;
std::cout <<"5,6:" << skipGramModelTrainer->counts[5][6] << std::endl;
*/	return 0;
}

