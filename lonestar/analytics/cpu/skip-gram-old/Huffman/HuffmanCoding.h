#ifndef HUFFMANCODING_H
#define HUFFMANCODING_H

#include <iostream>
#include <string>
#include <set>
#include <vector>
#include <map>

class HuffmanCoding {
public:
	/** Node */
	class HuffmanNode {
	public:
		/** vector of 0's and 1's */
		unsigned int* code;
		/** vector of parent node index offsets */
		int* point;
		/** Index of the Huffman node */
		int idx;
		/** Frequency of the token */
		int count;

		int codeLen;
	
		int token;

		HuffmanNode(unsigned int* code, int* point, int idx, int count, int codeLen, int token);
	};

	std::set<unsigned int>* vocab;
	std::multiset<unsigned int>* vocabMultiset;
	
  HuffmanCoding(std::set<unsigned int>* vocab, std::multiset<unsigned int>* vocabMultiset);
	
	/**
	 * @return {@link Map} from each given token to a {@link HuffmanNode} 
	 */
	std::map<unsigned int, HuffmanNode*>* encode();
	
	/** 
	 * Populate the count, binary, and parentNode arrays with the Huffman tree
	 * This uses the linear time method assuming that the count array is sorted 
	 */
	void createTree(int numTokens, long* count, int* binary, int* parentNode);
	
	/** @return Ordered map from each token to its {@link HuffmanNode}, ordered by frequency descending */
	std::map<unsigned int, HuffmanNode*>* encode(int* binary, int* parentNode);
};

#endif
