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
#include "galois/graphs/LC_CSR_Graph.h"

constexpr static const uint32_t kChunkSize = 512u;
constexpr static const uint32_t kInfPartition =
    std::numeric_limits<uint32_t>::max();

using EdgeDstVecTy = galois::gstl::Vector<galois::PODResizeableArray<uint32_t>>;
using LargeArrayUint64Ty = galois::LargeArray<uint64_t>;

class MetisNode;
typedef void EdgeTy;

struct GGraph
    : public galois::graphs::LC_CSR_Graph<MetisNode, EdgeTy>::with_no_lockable<
          true>::type::with_numa_alloc<true>::type {
  uint32_t hedges;
  uint32_t hnodes;
};

using GNode = GGraph::GraphNode;
using GNodeBag = galois::InsertBag<GNode>;
using GainTy = int;
using NetvalTy = int;
using NetnumTy = uint32_t;
using WeightTy = uint32_t;

template <typename T>
using GCopyableAtomic = galois::CopyableAtomic<T>;

template <typename T>
using GCopyableAtomicRef = GCopyableAtomic<T>&;

// Algorithm types.
enum MatchingPolicy {
  HigherDegree,
  LowerDegree,
  HigherWeight,
  LowerWeight,
  Random
};
enum coarseModeII { HMETISII, PAIRII };
enum pairScheduleModeII { FIRSTII, MAXWII, ECII };

// Nodes in the metis graph.
class MetisNode {
  uint32_t partition_;
  GNode parent_;
  GNode node_id_;
  GNode child_id_;
  uint32_t graph_index_;
  uint32_t counter_;
  uint32_t list_index_;

  bool not_alone_;
  bool matched_;

  WeightTy weight_;
  GCopyableAtomic<GainTy> positive_gain_;
  GCopyableAtomic<GainTy> negative_gain_;
  GCopyableAtomic<uint32_t> degree_;
  // Net-val and -rand have the same type.
  GCopyableAtomic<NetvalTy> netrand_;
  GCopyableAtomic<NetvalTy> netval_;
  GCopyableAtomic<NetnumTy> netnum_;

  void InitCoarsen() {
    matched_ = false;
    parent_ = 0;
    netval_ = 0;
  }

public:
  inline GCopyableAtomicRef<GainTy> GetPositiveGain() { return positive_gain_; }
  inline void SetPositiveGain(GainTy pg) { positive_gain_ = pg; }

  GCopyableAtomicRef<GainTy> GetNegativeGain() { return negative_gain_; }
  void SetNegativeGain(GainTy ng) { negative_gain_ = ng; }

  inline GCopyableAtomicRef<NetvalTy> GetNetrand() { return netrand_; }
  inline void SetNetrand(NetvalTy nr) { netrand_ = nr; }

  inline GCopyableAtomicRef<NetvalTy> GetNetval() { return netval_; }
  inline void SetNetval(NetvalTy nv) { netval_ = nv; }

  inline GCopyableAtomicRef<NetnumTy> GetNetnum() { return netnum_; }
  inline void SetNetnum(NetnumTy nn) { netnum_ = nn; }

  inline GCopyableAtomicRef<uint32_t> GetDegree() { return degree_; }
  inline void SetDegree(uint32_t dg) { degree_ = dg; }

  inline GNode GetChildId() const { return child_id_; }
  inline void SetChildId(GNode ci) { child_id_ = ci; }

  inline uint32_t GetGraphIndex() const { return graph_index_; }
  inline void SetGraphIndex(uint32_t gi) { graph_index_ = gi; }

  inline uint32_t GetListIndex() const { return list_index_; }
  inline void SetListIndex(uint32_t li) { list_index_ = li; }

  inline GNode GetNodeId() const { return node_id_; }
  inline void SetNodeId(GNode nid) { node_id_ = nid; }

  inline WeightTy GetWeight() const { return weight_; }
  inline void SetWeight(WeightTy w) { weight_ = w; }

  inline GNode GetParent() const { return parent_; }
  inline void SetParent(GNode p) { parent_ = p; }

  inline GainTy GetGain() {
    return (positive_gain_ - (negative_gain_ + counter_));
  }

  inline void SetMatched() { matched_ = true; }
  inline void UnsetMatched() { matched_ = false; }
  inline bool IsMatched() const { return matched_; }

  inline uint32_t GetPartition() const { return partition_; }
  inline void SetPartition(uint32_t p) { partition_ = p; }

  inline bool IsNotAlone() { return not_alone_; }
  inline void SetNotAlone() { not_alone_ = true; }
  inline void UnsetNotAlone() { not_alone_ = false; }

  inline uint32_t GetCounter() const { return counter_; }
  inline void ResetCounter() { counter_ = 0; }
  inline void IncCounter() { counter_++; }

  explicit MetisNode(WeightTy weight) : weight_(weight) {
    InitCoarsen();
    counter_ = 0;
    partition_ = 0;
  }

  MetisNode() : weight_(1) {
    InitCoarsen();
    counter_ = 0;
    partition_ = 0;
    matched_ = false;
  }

  void InitRefine(uint32_t p = 0) {
    partition_ = p;
    counter_ = 0;
  }
}; /* Metis Node Done. */

// Structure to keep track of graph hirarchy.
class MetisGraph {
  // Coarse root: leaf.
  MetisGraph* coarsened_graph_;
  MetisGraph* parent_graph_;

  GGraph graph_;

public:
  MetisGraph() : coarsened_graph_(nullptr), parent_graph_(nullptr) {}

  explicit MetisGraph(MetisGraph* fg)
      : coarsened_graph_(nullptr), parent_graph_(fg) {
    parent_graph_->coarsened_graph_ = this;
  }

  const GGraph* GetGraph() const { return &graph_; }
  GGraph* GetGraph() { return &graph_; }
  MetisGraph* GetParentGraph() const { return parent_graph_; }
  MetisGraph* GetCoarsenedGraph() const { return coarsened_graph_; }
};

// Metrics
unsigned GraphStat(GGraph& graph);
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
