#include <iostream>
#include <string>
#include <set>
#include <vector>
#include <map>
#include "HuffmanCoding.h"

 HuffmanCoding::HuffmanNode::HuffmanNode(unsigned int* code, int* point, int idx, int count, int codeLen, int token) {
			this->code = code;
			this->point = point;
			this->idx = idx;
			this->count = count;
			this->codeLen = codeLen;
			this->token = token;
		}
	
	
  HuffmanCoding::HuffmanCoding(std::set<unsigned int>* vocab, std::multiset<unsigned int>* vocabMultiset) {
		this->vocab = vocab;
		this->vocabMultiset = vocabMultiset;
	}
	
	/**
	 * @return {@link Map} from each given token to a {@link HuffmanNode} 
	 */
	std::map<unsigned int,HuffmanCoding::HuffmanNode*>* HuffmanCoding::encode(){
		
		int numTokens = vocab->size();
		
		int* parentNode = new int[numTokens * 2 + 1];
		int* binary = new int[numTokens * 2 + 1];
		long* count = new long[numTokens * 2 + 1];
		int i = 0;
		for (auto e : *vocab) {
			count[i] = vocabMultiset->count(e);
			i++;
		}

		for (i = numTokens; i < numTokens * 2 + 1; i++)
			count[i] = (long)100000000000000;
		
		createTree(numTokens, count, binary, parentNode);
		
		return encode(binary, parentNode);
	}
	
	/** 
	 * Populate the count, binary, and parentNode arrays with the Huffman tree
	 * This uses the linear time method assuming that the count array is sorted 
	 */
	void HuffmanCoding::createTree(int numTokens, long* count, int* binary, int* parentNode) {
	
		int min1i;
		int min2i;
		int pos1 = numTokens - 1;
		int pos2 = numTokens;
	
		int newNodeIdx;
	
		// Construct the Huffman tree by adding one node at a time
		for (int a = 0; a < numTokens - 1; a++) {
	
//			int newNodeIdx;	

		// First, find two smallest nodes 'min1, min2'
			if (pos1 >= 0) { 
				if (count[pos1] < count[pos2]) {
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
				if (count[pos1] < count[pos2]) {
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
			
			newNodeIdx = numTokens + a;
			count[newNodeIdx] = count[min1i] + count[min2i];
			parentNode[min1i] = newNodeIdx;
			parentNode[min2i] = newNodeIdx;
			binary[min2i] = 1;
		}
	}
	
	/** @return Ordered map from each token to its {@link HuffmanNode}, ordered by frequency descending */
	std::map<unsigned int, HuffmanCoding::HuffmanNode*>* HuffmanCoding::encode(int* binary, int* parentNode) {
		int numTokens = vocab->size();
		
		// Now assign binary code to each unique token
		std::map<unsigned int, HuffmanNode*>* result = new std::map<unsigned int, HuffmanNode*>;

		int nodeIdx = 0;
		int curNodeIdx;
	
		std::vector<unsigned int> code;	
		std::vector<int> points;

		int codeLen;
		int count;

		unsigned int* rawCode;
    int* rawPoints;

		int i;
		unsigned int token;

		std::cout << "numtokens: " << numTokens << std::endl;

		for (auto e : *vocab) {
			curNodeIdx = nodeIdx;
			code.clear();
			points.clear();

			while (true) {
				code.push_back(binary[curNodeIdx]);
				points.push_back(curNodeIdx);
				curNodeIdx = parentNode[curNodeIdx];
				if (curNodeIdx == numTokens * 2 - 2)
					break;
			}
			codeLen = code.size();
			count = vocabMultiset->count(e);
			
			rawCode = new unsigned int[codeLen];
			rawPoints = new int[codeLen+1];
			
			//rawCode->resize(codeLen, 0);
			//rawPoints->resize(codeLen+1, 0);

			rawPoints[0] = numTokens - 2;

			for (i = 0; i < codeLen; i++) {
				rawCode[codeLen - i - 1] = code[i];
				rawPoints[codeLen - i] = points[i] - numTokens;
			}
			
			token = e;
			result->insert(std::make_pair(token, new HuffmanNode(rawCode, rawPoints, nodeIdx, count,codeLen, token)));
		
			nodeIdx++;
		
		
		}
		
		return result;
	}
