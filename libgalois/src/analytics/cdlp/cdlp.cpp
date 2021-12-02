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

#include "katana/analytics/cdlp/cdlp.h"

#include <boost/unordered_map.hpp>

#include "katana/ArrowRandomAccessBuilder.h"
#include "katana/TypedPropertyGraph.h"

using namespace katana::analytics;

namespace {
/// Limited number of iterations to limit the oscillation of the label
/// in Synchronous algorithm. We dont need to limit it in Asynchronous algorithm.
/// Set to 10 same as Graphalytics benchmark.
const unsigned int kMaxIterations = 10;

struct CdlpAlgo {
  using CommunityType = uint64_t;
  struct NodeCommunity : public katana::PODProperty<CommunityType> {};

  using NodeData = std::tuple<NodeCommunity>;
  using EdgeData = std::tuple<>;
  using Graph = katana::TypedPropertyGraphView<
      katana::PropertyGraphViews::Undirected, NodeData, EdgeData>;
  using GNode = typename Graph::Node;

  void Initialize(Graph* graph) {
    katana::do_all(katana::iterate(*graph), [&](const GNode& node) {
      graph->GetData<NodeCommunity>(node) = node;
    });
  }
  virtual void operator()(Graph* graph, size_t max_iterations) = 0;
};

struct CdlpSynchronousAlgo : CdlpAlgo {
  void operator()(Graph* graph, size_t max_iterations = kMaxIterations) {
    if (max_iterations == 0)
      return;

    struct NodeDataPair {
      GNode node;
      CommunityType data;
      NodeDataPair(GNode node, CommunityType data) : node(node), data(data) {}
    };

    size_t iterations = 0;
    katana::InsertBag<NodeDataPair> apply_bag;

    /// TODO (Yasin): in this implementation, in each iteration, all the nodes are active
    /// for gather phase. If InsertBag does not accept duplicate items then this
    /// can be improved to have only the affected nodes to be active in next iteration
    while (iterations < max_iterations) {
      // Gather Phase
      katana::do_all(
          katana::iterate(*graph),
          [&](const GNode& node) {
            const auto ndata_current_comm = graph->GetData<NodeCommunity>(node);
            using Histogram_type = boost::unordered_map<CommunityType, size_t>;
            Histogram_type histogram;
            // Iterate over all neighbors (this is undirected view)
            for (auto e : graph->edges(node)) {
              auto neighbor = graph->edge_dest(e);
              const auto neighbor_data =
                  graph->GetData<NodeCommunity>(neighbor);
              histogram[neighbor_data]++;
            }

            // Pick the most frequent communtiy as the new community for node
            // pick the smallest one if more than one max frequent exist.
            auto ndata_new_comm = ndata_current_comm;
            size_t best_freq = 0;
            for (const auto& [comm, freq] : histogram) {
              if (freq > best_freq ||
                  (freq == best_freq && comm < ndata_new_comm)) {
                ndata_new_comm = comm;
                best_freq = freq;
              }
            }

            if (ndata_new_comm != ndata_current_comm)
              apply_bag.push(NodeDataPair(node, (CommunityType)ndata_new_comm));
          },
          katana::loopname("CDLP_Gather"));

      // No change! break!
      if (apply_bag.empty())
        break;

      // Apply Phase
      katana::do_all(
          katana::iterate(apply_bag),
          [&](const NodeDataPair node_data) {
            GNode node = node_data.node;
            graph->GetData<NodeCommunity>(node) = node_data.data;
          },
          katana::loopname("CDLP_Apply"));

      apply_bag.clear();
      iterations += 1;
    }
    katana::ReportStatSingle("CDLP_Synchronous", "iterations", iterations);
  }
};

struct CdlpAsynchronousAlgo : CdlpAlgo {
  void operator()(Graph*, size_t) {}
};

}  //namespace

template <typename Algorithm>
static katana::Result<void>
CdlpWithWrap(
    katana::PropertyGraph* pg, std::string output_property_name,
    size_t max_iterations) {
  katana::EnsurePreallocated(
      2,
      pg->topology().num_nodes() * sizeof(typename Algorithm::NodeCommunity));
  katana::ReportPageAllocGuard page_alloc;

  if (auto r = ConstructNodeProperties<
          std::tuple<typename Algorithm::NodeCommunity>>(
          pg, {output_property_name});
      !r) {
    return r.error();
  }
  auto pg_result = Algorithm::Graph::Make(pg, {output_property_name}, {});
  if (!pg_result) {
    return pg_result.error();
  }
  auto graph = pg_result.value();

  Algorithm algo;

  algo.Initialize(&graph);

  katana::StatTimer execTime("CDLP");

  execTime.start();
  algo(&graph, max_iterations);
  execTime.stop();

  return katana::ResultSuccess();
}

katana::Result<void>
katana::analytics::Cdlp(
    PropertyGraph* pg, const std::string& output_property_name,
    size_t max_iterations, CdlpPlan plan) {
  switch (plan.algorithm()) {
  case CdlpPlan::kSynchronous:
    return CdlpWithWrap<CdlpSynchronousAlgo>(
        pg, output_property_name, max_iterations);
  /// TODO (Yasin): Asynchronous Algorithm will be implemented later after Synchronous
  /// is done for both shared and distributed versions.
  /*
  case CdlpPlan::kAsynchronous:
    return CdlpWithWrap<CdlpAsynchronousAlgo>(
        pg, output_property_name, max_iterations);
  */
  default:
    return ErrorCode::InvalidArgument;
  }
}

/// TODO (Yasin): This function is now being used by louvain,
/// cc, and cdlp, basically everything which is calculating communities. Explore
/// possiblity of moving it to some common .h file in libgalois/include/analytics
/// to avoid code duplication.
katana::Result<CdlpStatistics>
katana::analytics::CdlpStatistics::Compute(
    katana::PropertyGraph* pg, const std::string& property_name) {
  using CommunityType = uint64_t;
  struct NodeCommunity : public katana::PODProperty<CommunityType> {};

  using NodeData = std::tuple<NodeCommunity>;
  using EdgeData = std::tuple<>;
  using Graph = katana::TypedPropertyGraph<NodeData, EdgeData>;
  using GNode = typename Graph::Node;

  auto pg_result = Graph::Make(pg, {property_name}, {});
  if (!pg_result) {
    return pg_result.error();
  }

  auto graph = pg_result.value();

  using Map = katana::gstl::Map<CommunityType, int>;

  auto reduce = [](Map& lhs, Map&& rhs) -> Map& {
    Map v{std::move(rhs)};

    for (auto& kv : v) {
      if (lhs.count(kv.first) == 0) {
        lhs[kv.first] = 0;
      }
      lhs[kv.first] += kv.second;
    }

    return lhs;
  };

  auto mapIdentity = []() { return Map(); };

  auto accumMap = katana::make_reducible(reduce, mapIdentity);

  katana::do_all(
      katana::iterate(graph),
      [&](const GNode& x) {
        auto& n = graph.template GetData<NodeCommunity>(x);
        accumMap.update(Map{std::make_pair(n, 1)});
      },
      katana::loopname("CountLargest"));

  Map& map = accumMap.reduce();
  size_t reps = map.size();

  using CommunitySizePair = std::pair<CommunityType, int>;

  auto sizeMax = [](const CommunitySizePair& a, const CommunitySizePair& b) {
    if (a.second > b.second) {
      return a;
    }
    return b;
  };

  auto identity = []() { return CommunitySizePair{}; };

  auto maxComm = katana::make_reducible(sizeMax, identity);

  katana::GAccumulator<uint64_t> non_trivial_communities;
  katana::do_all(katana::iterate(map), [&](const CommunitySizePair& x) {
    maxComm.update(x);
    if (x.second > 1) {
      non_trivial_communities += 1;
    }
  });

  CommunitySizePair largest = maxComm.reduce();

  size_t largest_community_size = largest.second;
  double largest_community_ratio = 0;
  if (!graph.empty()) {
    largest_community_ratio = double(largest_community_size) / graph.size();
  }

  return CdlpStatistics{
      reps, non_trivial_communities.reduce(), largest_community_size,
      largest_community_ratio};
}

void
katana::analytics::CdlpStatistics::Print(std::ostream& os) const {
  os << "Total number of communities = " << total_communities << std::endl;
  os << "Total number of non trivial communities = "
     << total_non_trivial_communities << std::endl;
  os << "Number of nodes in the largest community = " << largest_community_size
     << std::endl;
  os << "Ratio of nodes in the largest community = " << largest_community_ratio
     << std::endl;
}
