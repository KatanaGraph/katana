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

#include "katana/analytics/subgraph_extraction/subgraph_extraction.h"

#include <iostream>

#include "katana/PropertyGraph.h"
#include "katana/TypedPropertyGraph.h"
#include "katana/analytics/Utils.h"

namespace {

using namespace katana::analytics;
using edge_iterator = boost::counting_iterator<uint64_t>;

using SortedGraphView = katana::PropertyGraphViews::EdgesSortedByDestID;
using Node = SortedGraphView::Node;
using Edge = SortedGraphView::Edge;

katana::Result<std::unique_ptr<katana::PropertyGraph>>
SubGraphNodeSet(
    const SortedGraphView& graph, const std::vector<Node>& node_set) {
  uint64_t num_nodes = node_set.size();
  // Subgraph topology : out indices
  katana::NUMAArray<Edge> out_indices;
  out_indices.allocateInterleaved(num_nodes);

  katana::gstl::Vector<katana::gstl::Vector<Node>> subgraph_edges;
  subgraph_edges.resize(num_nodes);

  katana::do_all(
      katana::iterate(Node(0), Node(num_nodes)),
      [&](const Node& n) {
        Node src = node_set[n];

        auto last = graph.OutEdges(src).end();
        for (Node m = 0; m < num_nodes; ++m) {
          auto dest = node_set[m];
          // Binary search on the edges sorted by destination id
          for (auto edge_it = graph.FindEdge(src, dest);
               edge_it != last && graph.OutEdgeDst(*edge_it) == dest;
               ++edge_it) {
            subgraph_edges[n].push_back(m);
          }
        }
        out_indices[n] = subgraph_edges[n].size();
      },
      katana::steal(), katana::loopname("SubgraphExtraction"));

  // Prefix sum
  katana::ParallelSTL::partial_sum(
      out_indices.begin(), out_indices.end(), out_indices.begin());
  uint64_t num_edges = out_indices[num_nodes - 1];

  // Subgraph topology : out dests
  katana::NUMAArray<Node> out_dests;
  out_dests.allocateInterleaved(num_edges);

  katana::do_all(
      katana::iterate(Node(0), Node(num_nodes)),
      [&](const Node& n) {
        uint64_t offset = n == 0 ? 0 : out_indices[n - 1];
        for (Node dest : subgraph_edges[n]) {
          out_dests[offset] = dest;
          offset++;
        }
      },
      katana::steal(), katana::loopname("ConstructTopology"));

  katana::GraphTopology sub_g_topo{
      std::move(out_indices), std::move(out_dests)};
  auto sub_g_res = katana::PropertyGraph::Make(std::move(sub_g_topo));

  return sub_g_res;
}
}  // namespace

katana::Result<std::unique_ptr<katana::PropertyGraph>>
katana::analytics::SubGraphExtraction(
    katana::PropertyGraph* pg, const std::vector<Node>& node_vec,
    SubGraphExtractionPlan plan) {
  // Remove duplicates from the node vector
  std::unordered_set<uint32_t> set;
  std::vector<uint32_t> dedup_node_vec;
  for (auto n : node_vec) {
    if (set.insert(n).second) {  // If n wasn't already present.
      dedup_node_vec.push_back(n);
    }
  }

  if (dedup_node_vec.empty()) {
    return std::make_unique<katana::PropertyGraph>();
  }

  SortedGraphView sg = pg->BuildView<SortedGraphView>();

  katana::StatTimer execTime("SubGraph-Extraction");
  switch (plan.algorithm()) {
  case SubGraphExtractionPlan::kNodeSet: {
    execTime.start();
    auto subgraph = SubGraphNodeSet(sg, dedup_node_vec);
    execTime.stop();
    KATANA_LOG_DEBUG_ASSERT(subgraph);
    return std::move(subgraph.value());
  }
  default:
    return katana::ErrorCode::InvalidArgument;
  }
}
