#ifndef BIPART_HELPER_H_
#define BIPART_HELPER_H_

#include "Bipart.h"
/**
 * Initialize the nodes in the graph
 *
 * @param graph Graph
 * @param num_hedges Number of hyperedges in the specified param graph
 */
void InitNodes(GGraph& graph, uint32_t num_hedges);

/**
 * Calculate the prefix sum for CSR graph construction
 *
 * @param prefix_sum A LargeArray to store prefix sum of nodes
 */
template <typename T>
uint64_t ParallelPrefixSum(galois::LargeArray<T>& prefix_sum);

/**
 * Constructs LC_CSR graph from the input file
 *
 * @param graph Graph to be constructed
 * @param filename Input graph file name
 */
void ConstructGraph(
    GGraph& graph, const std::string filename, const bool skip_isolated_hedges);
/**
 * Priority assinging functions.
 */
void PrioritizeHigherDegree(GNode node, GGraph* fine_graph);
void PrioritizeRandom(GNode node, GGraph* fine_graph);
void PrioritizeLowerDegree(GNode node, GGraph* fine_graph);
void PrioritizeHigherWeight(GNode node, GGraph* fine_graph);
void PrioritizeDegree(GNode node, GGraph* fine_graph);
void SortNodesByGainAndWeight(
    GGraph& graph, std::vector<GNode>& nodes, uint32_t end_offset);
void InitGain(GGraph& g);
void InitGain(
    std::vector<std::pair<uint32_t, uint32_t>>& combined_edgelist,
    std::vector<std::pair<uint32_t, uint32_t>>& combined_nodelist,
    std::vector<GGraph*>& g);
#endif
