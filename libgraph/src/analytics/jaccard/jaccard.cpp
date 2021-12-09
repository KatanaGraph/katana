/*
 * This file belongs to the Galois project, a C++ library for exploiting
 * parallelism. The code is being released under the terms of the 3-Clause BSD
 * License (a copy is located in LICENSE.txt at the top-level directory).
 *
 * Copyright (C) 2019, The University of Texas at Austin. All rights reserved.
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

#include "katana/analytics/jaccard/jaccard.h"

#include "katana/Statistics.h"
#include "katana/TypedPropertyGraph.h"
#include "katana/analytics/Utils.h"

using namespace katana::analytics;

using NodeData = std::tuple<JaccardSimilarity>;
using EdgeData = std::tuple<>;

using Graph = katana::TypedPropertyGraphView<
    katana::PropertyGraphViews::Default, NodeData, EdgeData>;
using GNode = typename Graph::Node;

namespace {

struct IntersectWithSortedEdgeList {
private:
  const GNode base_;
  const Graph& graph_;

public:
  IntersectWithSortedEdgeList(const Graph& graph, GNode base)
      : base_(base), graph_(graph) {}

  uint32_t operator()(GNode n2) {
    uint32_t intersection_size = 0;
    // Iterate over the edges of both n2 and base in sync, based on the
    // assumption that edges lists are sorted.
    auto edges_n2_iter = graph_.edges(n2).begin();
    auto edges_n2_end = graph_.edges(n2).end();
    auto edges_base_iter = graph_.edges(base_).begin();
    auto edges_base_end = graph_.edges(base_).end();
    while (edges_n2_iter != edges_n2_end && edges_base_iter != edges_base_end) {
      auto edge_n2_dst = graph_.edge_dest(*edges_n2_iter);
      auto edge_base_dst = graph_.edge_dest(*edges_base_iter);
      if (edge_n2_dst == edge_base_dst) {
        intersection_size++;
        edges_n2_iter++;
        edges_base_iter++;
      } else if (edge_n2_dst > edge_base_dst) {
        edges_base_iter++;
      } else if (edge_n2_dst < edge_base_dst) {
        edges_n2_iter++;
      }
    }
    return intersection_size;
  }
};

struct IntersectWithUnsortedEdgeList {
private:
  std::unordered_set<GNode> base_neighbors;
  const Graph& graph_;

public:
  IntersectWithUnsortedEdgeList(const Graph& graph, GNode base)
      : graph_(graph) {
    // Collect all the neighbors of the base node into a hash set.
    for (const auto& e : graph.edges(base)) {
      auto dest = graph.edge_dest(e);
      base_neighbors.emplace(dest);
    }
  }

  uint32_t operator()(GNode n2) {
    uint32_t intersection_size = 0;
    for (const auto& e : graph_.edges(n2)) {
      auto neighbor = graph_.edge_dest(e);
      if (base_neighbors.count(neighbor) > 0)
        intersection_size++;
    }
    return intersection_size;
  }
};

template <typename IntersectAlgorithm>
katana::Result<void>
JaccardImpl(Graph& graph, size_t compare_node, JaccardPlan /*plan*/) {
  if (compare_node >= graph.size()) {
    return katana::ErrorCode::InvalidArgument;
  }

  katana::ReportPageAllocGuard page_alloc;

  katana::StatTimer exec_time("Jaccard");
  exec_time.start();

  auto it = graph.begin();
  std::advance(it, compare_node);
  Graph::Node base = *it;

  uint32_t base_size = graph.edges(base).size();

  IntersectAlgorithm intersect_with_base{graph, base};

  // Compute the similarity for each node
  katana::do_all(katana::iterate(graph), [&](const GNode& n2) {
    double& n2_data = graph.GetData<JaccardSimilarity>(n2);
    uint32_t n2_size = graph.edges(n2).size();
    // Count the number of neighbors of n2 and the number that are shared
    // with base
    uint32_t intersection_size = intersect_with_base(n2);
    // Compute the similarity
    uint32_t union_size = base_size + n2_size - intersection_size;
    double similarity =
        union_size > 0 ? (double)intersection_size / union_size : 1;
    // Store the similarity back into the graph.
    n2_data = similarity;
  });

  exec_time.stop();

  return katana::ResultSuccess();
}

}  // namespace

katana::Result<void>
katana::analytics::Jaccard(
    PropertyGraph* pg, uint32_t compare_node,
    const std::string& output_property_name, JaccardPlan plan) {
  if (auto result =
          ConstructNodeProperties<NodeData>(pg, {output_property_name});
      !result) {
    return result.error();
  }

  Graph graph = KATANA_CHECKED(Graph::Make(pg, {output_property_name}, {}));

  katana::Result<void> r = katana::ResultSuccess();
  switch (plan.edge_sorting()) {
  case JaccardPlan::kUnknown:
    // TODO(amp): It would be possible to start with the sorted case and then
    //  fail to the unsorted case if unsorted nodes are detected.
  case JaccardPlan::kUnsorted:
    r = JaccardImpl<IntersectWithUnsortedEdgeList>(graph, compare_node, plan);
    break;
  case JaccardPlan::kSorted:
    r = JaccardImpl<IntersectWithSortedEdgeList>(graph, compare_node, plan);
    break;
  }

  return r;
}

constexpr static const double EPSILON = 1e-6;

katana::Result<void>
katana::analytics::JaccardAssertValid(
    katana::PropertyGraph* pg, uint32_t compare_node,
    const std::string& property_name) {
  Graph graph = KATANA_CHECKED(Graph::Make(pg, {property_name}, {}));
  ;

  if (abs(graph.GetData<JaccardSimilarity>(compare_node) - 1.0) > EPSILON) {
    return katana::ErrorCode::AssertionFailed;
  }

  auto is_bad = [&graph](const GNode& n) {
    auto& similarity = graph.template GetData<JaccardSimilarity>(n);
    if (similarity > 1 || similarity < 0) {
      return true;
    }
    return false;
  };

  if (katana::ParallelSTL::find_if(graph.begin(), graph.end(), is_bad) !=
      graph.end()) {
    return katana::ErrorCode::AssertionFailed;
  }

  return katana::ResultSuccess();
}

katana::Result<JaccardStatistics>
katana::analytics::JaccardStatistics::Compute(
    katana::PropertyGraph* pg, uint32_t compare_node,
    const std::string& property_name) {
  Graph graph = KATANA_CHECKED(Graph::Make(pg, {property_name}, {}));
  ;

  katana::GReduceMax<double> max_similarity;
  katana::GReduceMin<double> min_similarity;
  katana::GAccumulator<double> total_similarity;

  katana::do_all(
      katana::iterate(graph),
      [&](const GNode& i) {
        double similarity = graph.GetData<JaccardSimilarity>(i);
        if ((unsigned int)i != (unsigned int)compare_node) {
          max_similarity.update(similarity);
          min_similarity.update(similarity);
          total_similarity += similarity;
        }
      },
      katana::loopname("Jaccard Statistics"), katana::no_stats());

  return JaccardStatistics{
      max_similarity.reduce(), min_similarity.reduce(),
      total_similarity.reduce() / (graph.size() - 1)};
}

void
katana::analytics::JaccardStatistics::Print(std::ostream& os) {
  os << "Maximum similarity = " << max_similarity << std::endl;
  os << "Minimum similarity = " << min_similarity << std::endl;
  os << "Average similarity = " << average_similarity << std::endl;
}
