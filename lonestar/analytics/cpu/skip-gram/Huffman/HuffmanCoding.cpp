#include <iostream>
#include <map>
#include <set>
#include <string>
#include <vector>
#include <map>
	
	/**
	 * @return {@link Map} from each given token to a {@link HuffmanNode} 
	 */
	void HuffmanCoding::Encode(std::map<uint32_t,HuffmanCoding::HuffmanNode*>* huffman_node_map){
		
		num_tokens_ = vocab_->size();
		
		parent_node_->reserve(num_tokens * 2 + 1);
		binary_->reserve(num_tokens * 2 + 1);
		count_->allocateBlocked(num_tokens * 2 + 1);

		katana::do_all(
				katana::iterate((uint32_t) 0, (num_tokens * 2 + 1)),
				[&](uint32_t idx){
				
					count_[idx] = (unsigned long) 100000000000000;
				}, katana::steal());

		uint32_t idx = 0;

		for (auto item : *vocab) {
			count[i] = vocab_multiset_[item];
			idx++;
		}
		
		CreateTree();
		
		Encode(huffman_node_map);
	}
	
	/** 
	 * Populate the count, binary, and parentNode arrays with the Huffman tree
	 * This uses the linear time method assuming that the count array is sorted 
	 */
	void HuffmanCoding::CreateTree() {
	
		uint32_t min1i;
		uint32_t min2i;
		uint32_t pos1 = num_tokens_ - 1;
		uint32_t pos2 = num_tokens_;
	
		uint32_t new_node_idx;
	
		// Construct the Huffman tree by adding one node at a time
		for (uint32_t idx = 0; idx < (num_tokens_ - 1); idx++) {

		// First, find two smallest nodes 'min1, min2'
			if (pos1 >= 0) { 
				if (count_[pos1] < count_[pos2]) {
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
				if (count_[pos1] < count_[pos2]) {
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
			
			new_node_idx = num_tokens_ + idx;
			count_[new_node_idx] = count_[min1i] + count_[min2i];
			parent_node_[min1i] = new_node_idx;
			parent_node_[min2i] = new_node_idx;
			binary_[min2i] = 1;
		}
	}
	
	/** @return Ordered map from each token to its {@link HuffmanNode}, ordered by frequency descending */
	void HuffmanCoding::Encode(std::map<unsigned int, HuffmanCoding::HuffmanNode*>* huffman_node_map) {
	
		uint32_t node_idx = 0;
		uint32_t cur_node_idx;
	
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
			huffman_node->insert(std::make_pair(token, new HuffmanNode(rawCode, rawPoints, nodeIdx, count,codeLen, token)));
		
			nodeIdx++;
		
		
		}
	}
