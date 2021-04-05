#ifndef HUFFMANCODING_H
#define HUFFMANCODING_H

#include <iostream>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "katana/LargeArray.h"

class HuffmanCoding {
	public:
	/** Node */
	class HuffmanNode {
	public:
		HuffmanNode(uint32_t idx, uint32_t count, uint32_t code_len, uint32_t token): 
			idx_(idx),
			count_(count),
			code_len_(code_len),
			token_(token) {}
	
		void InitCode(std::vector<uint32_t>& code);

		void InitPoints(std::vector<int32_t>& point);

		void InitVars(uint32_t idx, uint32_t count, uint32_t code_len, uint32_t token);
		uint32_t GetIdx() {return idx_;}

		uint32_t GetCount(){ return count_;}

		uint32_t GetCodeLen() { return code_len_;}

		int32_t GetPoint(uint32_t idx) { return point_[idx];}

		uint32_t GetCode(uint32_t idx) { return code_[idx]; }
	private	:
		/** vector of 0's and 1's */
		std::vector<uint32_t> code_;
		/** vector of parent node index offsets */
	        std::vector<int32_t> point_;
		/** Index of the Huffman node */
		uint32_t idx_;
		/** Frequency of the token */
		uint32_t count_;

		uint32_t code_len_;
	
		uint32_t token_;
	};

	HuffmanCoding(std::set<uint32_t>* vocab, std::map<uint32_t, uint32_t>* vocab_multiset):
		vocab_(vocab),
		vocab_multiset_(vocab_multiset) {}

	/**
         * @return {@link Map} from each given token to a {@link HuffmanNode}
         */
        void Encode(std::map<uint32_t, HuffmanNode*>*, std::vector<HuffmanCoding::HuffmanNode>* );

	/**
         * Populate the count, binary, and parentNode arrays with the Huffman tree
         * This uses the linear time method assuming that the count array is sorted
         */
        void CreateTree();

	/** @return Ordered map from each token to its {@link HuffmanNode}, ordered by frequency descending */
        void EncodeTree(std::map<uint32_t, HuffmanNode*>* huffman_nodes_map, std::vector<HuffmanCoding::HuffmanNode>*);

	private:
	std::set<uint32_t>* vocab_;

	std::map<uint32_t, uint32_t>* vocab_multiset_;

	uint32_t num_tokens_;

	std::vector<uint32_t> parent_node_;
        
	std::vector<uint32_t> binary_;
       	
	std::vector<unsigned long> count_; 
};

#endif
