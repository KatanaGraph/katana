#ifndef BIPART_METISGRAPH_H_
#define BIPART_METISGRAPH_H_

#include "katana/HyperGraph.h"

struct MetisNode;

using HyperGraph = katana::HyperGraph<MetisNode>;
using GNode = HyperGraph::GraphNode;
using GNodeBag = katana::InsertBag<GNode>;

// Nodes in the metis graph.
struct MetisNode {
  using GainTy = int;
  using NetvalTy = int;
  using NetnumTy = uint32_t;
  using WeightTy = uint32_t;

  uint32_t partition;
  GNode parent;
  GNode node_id;
  GNode child_id;
  uint32_t graph_index;
  uint32_t counter;
  uint32_t list_index;
  bool not_alone;
  bool matched;
  WeightTy weight;
  GainTy positive_gain;
  GainTy negative_gain;
  katana::CopyableAtomic<uint32_t> degree;
  // Net-val and -rand have the same type.
  katana::CopyableAtomic<NetvalTy> netrand;
  katana::CopyableAtomic<NetvalTy> netval;
  katana::CopyableAtomic<NetnumTy> netnum;

  inline GainTy GetGain() const {
    return (positive_gain - (negative_gain + counter));
  }

  inline void SetMatched() { matched = true; }
  inline void UnsetMatched() { matched = false; }
  inline bool IsMatched() const { return matched; }

  inline bool IsNotAlone() const { return not_alone; }
  inline void SetNotAlone() { not_alone = true; }
  inline void UnsetNotAlone() { not_alone = false; }

  inline uint32_t GetCounter() const { return counter; }
  inline void ResetCounter() { counter = 0; }
  inline void IncCounter() { counter++; }

  explicit MetisNode(WeightTy weight) : weight(weight) { Init(); }

  MetisNode() : weight(1) { Init(); }

  void InitRefine(uint32_t p = 0) {
    partition = p;
    counter = 0;
  }

  void Init() {
    matched = false;
    parent = 0;
    netval = 0;
    counter = 0;
    partition = 0;
  }
}; /* Metis Node Done. */

// Structure to keep track of graph hirarchy.
struct MetisGraph {
  // Coarse root: leaf.
  MetisGraph* coarsened_graph;
  MetisGraph* parent_graph;
  HyperGraph graph;

  MetisGraph() : coarsened_graph(nullptr), parent_graph(nullptr) {}

  explicit MetisGraph(MetisGraph* fg)
      : coarsened_graph(nullptr), parent_graph(fg) {
    parent_graph->coarsened_graph = this;
  }
};

#endif
