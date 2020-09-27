/*
 * This file belongs to the Galois project, a C++ library for exploiting
 * parallelism. The code is being released under the terms of the 3-Clause BSD
 * License (a copy is located in LICENSE.txt at the top-level directory).
 *
 * Copyright (C) 2018, The University of Texas at Austin. All rights reserved.
 * UNIVERSITY EXPRESSLY DISCLAIMS ANY AND ALL WARRANTIES CONCERNING THIS
 * SOFTWARE AND DOCUMENTATION, INCLUDING ANY WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR ANY PARTICULAR PURPOSE, NON-INFRINGEMENT AND WARRANTIES OF
 * PERFORMANCE, AND ANY WARRANTY THAT MIGHT OTHERWISE ARISE FROM COURSE OF
 * DEALING OR USAGE OF TRADE.  NO WARRANTY IS EITHER EXPRESS OR IMPLIED WITH
 * RESPECT TO THE USE OF THE SOFTWARE OR DOCUMENTATION. Under no circumstances
 * shall University be liable for incidental, special, indirect, direct or
 * consequential damages or loss of profits, interruption of business, or
 * related expenses which may arise from use of Software or Documentation,
 * including but not limited to those resulting from defects in Software and/or
 * Documentation, or loss or inaccuracy of data of any kind.
 */

#ifndef BIPART_H_
#define BIPART_H_

#include "galois/DynamicBitset.h"
#include "galois/graphs/HyperGraph.h"

constexpr static const uint32_t kChunkSize = 512u;
constexpr static const uint32_t kInfPartition =
    std::numeric_limits<uint32_t>::max();

using EdgeDstVecTy = galois::gstl::Vector<galois::PODResizeableArray<uint32_t>>;
using LargeArrayUint64Ty = galois::LargeArray<uint64_t>;
using HyperGraph = galois::graphs::HyperGraph;
using MetisGraph = galois::graphs::MetisGraph;
using MetisNode = galois::graphs::MetisNode;
using GNode = HyperGraph::GraphNode;
using GNodeBag = galois::InsertBag<GNode>;
using GainTy = MetisNode::GainTy;
using NetvalTy = MetisNode::NetvalTy;
using NetnumTy = MetisNode::NetnumTy;
using WeightTy = MetisNode::WeightTy;

// Algorithm types.
enum MatchingPolicy {
  HigherDegree,
  LowerDegree,
  HigherWeight,
  LowerWeight,
  Random
};

// Metrics
unsigned GraphStat(HyperGraph& graph);
// Coarsening
void Coarsen(std::vector<MetisGraph*>&, unsigned, MatchingPolicy);

// Partitioning
void PartitionCoarsestGraphs(std::vector<MetisGraph*>&, std::vector<unsigned>&);
// Refinement
void Refine(std::vector<MetisGraph*>&);

void ConstructCombinedLists(
    std::vector<MetisGraph*>& metis_graphs,
    std::vector<std::pair<uint32_t, uint32_t>>& combined_edge_list,
    std::vector<std::pair<uint32_t, uint32_t>>& combined_node_list);

#endif
