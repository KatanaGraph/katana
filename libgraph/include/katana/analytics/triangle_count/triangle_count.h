#ifndef KATANA_LIBGRAPH_KATANA_ANALYTICS_TRIANGLECOUNT_TRIANGLECOUNT_H_
#define KATANA_LIBGRAPH_KATANA_ANALYTICS_TRIANGLECOUNT_TRIANGLECOUNT_H_

#include "katana/PropertyGraph.h"
#include "katana/analytics/Plan.h"

// API

namespace katana::analytics {

/// A computational plan to for Total Triangle Counting.
class TriangleCountPlan : public Plan {
public:
  enum Algorithm {
    kNodeIteration,
    kEdgeIteration,
    kOrderedCount,
  };

  enum Relabeling {
    kRelabel,
    kNoRelabel,
    kAutoRelabel,
  };

  static const Relabeling kDefaultRelabeling = kAutoRelabel;
  static const bool kDefaultEdgeSorted = false;

private:
  Algorithm algorithm_;
  Relabeling relabeling_;
  bool edges_sorted_;

  TriangleCountPlan(
      Architecture architecture, Algorithm algorithm, bool edges_sorted,
      Relabeling relabeling)
      : Plan(architecture),
        algorithm_(algorithm),
        relabeling_(relabeling),
        edges_sorted_(edges_sorted) {}

public:
  TriangleCountPlan()
      : TriangleCountPlan{
            kCPU, kOrderedCount, kDefaultEdgeSorted, kDefaultRelabeling} {}

  Algorithm algorithm() const { return algorithm_; }
  Relabeling relabeling() const { return relabeling_; }
  bool edges_sorted() const { return edges_sorted_; }

  /**
   * The node-iterator algorithm from the following:
   *   Thomas Schank. Algorithmic Aspects of Triangle-Based Network Analysis. PhD
   *   Thesis. Universitat Karlsruhe. 2007.
   *
   * @param edges_sorted Are the edges of the graph already sorted.
   * @param relabeling Should the algorithm relabel the nodes.
   */
  static TriangleCountPlan NodeIteration(
      bool edges_sorted = kDefaultEdgeSorted,
      Relabeling relabeling = kDefaultRelabeling) {
    return {kCPU, kNodeIteration, edges_sorted, relabeling};
  }

  /**
   * The edge-iterator algorithm from the following:
   *   Thomas Schank. Algorithmic Aspects of Triangle-Based Network Analysis. PhD
   *   Thesis. Universitat Karlsruhe. 2007.
   *
   * @param edges_sorted Are the edges of the graph already sorted.
   * @param relabeling Should the algorithm relabel the nodes.
   */
  static TriangleCountPlan EdgeIteration(
      bool edges_sorted = kDefaultEdgeSorted,
      Relabeling relabeling = kDefaultRelabeling) {
    return {kCPU, kEdgeIteration, edges_sorted, relabeling};
  }

  /**
   * An ordered count algorithm that sorts the nodes by degree before
   * execution. This has been found to give good performance. We implement the
   * ordered count algorithm from the following:
   * http://gap.cs.berkeley.edu/benchmark.html
   *
   * @param edges_sorted Are the edges of the graph already sorted.
   * @param relabeling Should the algorithm relabel the nodes.
   */
  static TriangleCountPlan OrderedCount(
      bool edges_sorted = kDefaultEdgeSorted,
      Relabeling relabeling = kDefaultRelabeling) {
    return {kCPU, kOrderedCount, edges_sorted, relabeling};
  }
};

/**
 * Count the total number of triangles in the graph. The graph must be
 * symmetric!
 *
 * This algorithm copies the graph internally.
 *
 * @param pg The graph to process.
 * @param plan
 */
KATANA_EXPORT katana::Result<uint64_t> TriangleCount(
    PropertyGraph* pg, TriangleCountPlan plan = {});

}  // namespace katana::analytics

#endif
