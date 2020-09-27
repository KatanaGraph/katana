#ifndef BIPART_HELPER_H_
#define BIPART_HELPER_H_

#include "Bipart.h"
/**
 * Initialize the nodes in the graph
 *
 * @param graph Graph
 * @param num_hedges Number of hyperedges in the specified param graph
 */
void InitNodes(HyperGraph& graph, uint32_t num_hedges);

/**
 * Calculate the prefix sum for CSR graph construction
 *
 * @param prefix_sum A LargeArray to store prefix sum of nodes
 */
template <typename T>
uint64_t
ParallelPrefixSum(galois::LargeArray<T>& prefix_sum) {
  uint32_t num_threads = galois::getActiveThreads();
  uint32_t size = prefix_sum.size();

  galois::LargeArray<uint64_t> interm_sums;
  interm_sums.allocateInterleaved(num_threads);

  //! Local summation.
  galois::on_each([&](uint32_t tid, uint32_t total_threads) {
    uint32_t block_size = size / total_threads;
    if ((size % num_threads) > 0) {
      ++block_size;
    }
    uint32_t start = tid * block_size;
    uint32_t end = (tid + 1) * block_size;
    if (end > size) {
      end = size;
    }

    for (uint32_t idx = start + 1; idx < end; ++idx) {
      prefix_sum[idx] += prefix_sum[idx - 1];
    }

    interm_sums[tid] = prefix_sum[end - 1];
  });

  //! Compute global prefix sum.
  for (uint32_t tid = 1; tid < num_threads; ++tid) {
    interm_sums[tid] += interm_sums[tid - 1];
  }

  galois::on_each([&](uint32_t tid, uint32_t total_threads) {
    if (tid == 0) {
      return;
    }
    uint32_t block_size = size / total_threads;
    if ((size % num_threads) > 0) {
      ++block_size;
    }
    uint32_t start = tid * block_size;
    uint32_t end = (tid + 1) * block_size;
    if (end > size) {
      end = size;
    }

    for (uint32_t idx = start; idx < end; ++idx) {
      prefix_sum[idx] += interm_sums[tid - 1];
    }
  });

  return prefix_sum[size - 1];
}

/**
 * Constructs LC_CSR graph from the input file
 *
 * @param graph Graph to be constructed
 * @param filename Input graph file name
 */
void ConstructGraph(
    HyperGraph& graph, const std::string filename,
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
    HyperGraph& graph, std::vector<GNode>& nodes, uint32_t end_offset);
void InitGain(HyperGraph& g);
void InitGain(
    std::vector<std::pair<uint32_t, uint32_t>>& combined_edgelist,
    std::vector<std::pair<uint32_t, uint32_t>>& combined_nodelist,
    std::vector<HyperGraph*>& g);
#endif
