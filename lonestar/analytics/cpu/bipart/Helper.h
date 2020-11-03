#ifndef BIPART_HELPER_H_
#define BIPART_HELPER_H_

#include "Bipart.h"
#include "galois/ParallelSTL.h"

/**
 * Initialize the nodes in the graph
 *
 * @param graph Graph
 * @param num_hedges Number of hyperedges in the specified param graph
 */
void InitNodes(HyperGraph* graph, uint32_t num_hedges);

/**
 * Constructs LC_CSR graph from the input file
 *
 * @param graph Graph to be constructed
 * @param filename Input graph file name
 */
void ConstructGraph(
    HyperGraph* graph, const std::string filename,
    const bool skip_isolated_hedges);
/**
 * Priority assinging functions.
 */
void PrioritizeHigherDegree(GNode node, HyperGraph* fine_graph);
void PrioritizeRandom(GNode node, HyperGraph* fine_graph);
void PrioritizeLowerDegree(GNode node, HyperGraph* fine_graph);
void PrioritizeHigherWeight(GNode node, HyperGraph* fine_graph);
void PrioritizeDegree(GNode node, HyperGraph* fine_graph);
void SortNodesByGainAndWeight(
    HyperGraph* graph, std::vector<GNode>* nodes, uint32_t end_offset);
void InitGain(HyperGraph* g);
void InitGain(
    const std::vector<std::pair<uint32_t, uint32_t>>& combined_edgelist,
    const std::vector<std::pair<uint32_t, uint32_t>>& combined_nodelist,
    const std::vector<HyperGraph*>& g);
#endif
