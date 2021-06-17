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

katana::Result<std::unique_ptr<katana::PropertyGraph>>
SubGraphNodeSet(
    katana::PropertyGraph* graph, const std::vector<uint32_t>& node_set) {
  auto subgraph = std::make_unique<katana::PropertyGraph>();
  if (node_set.empty()) {
    return std::unique_ptr<katana::PropertyGraph>(std::move(subgraph));
  }

  uint64_t num_nodes = node_set.size();
  // Subgraph topology : out indices
  katana::LargeArray<uint64_t> out_indices;
  out_indices.allocateInterleaved(num_nodes);

  katana::gstl::Vector<katana::gstl::Vector<uint32_t>> subgraph_edges;
  subgraph_edges.resize(num_nodes);

  katana::do_all(
      katana::iterate(uint32_t(0), uint32_t(num_nodes)),
      [&](const uint32_t& n) {
        uint32_t src = node_set[n];

        auto last = graph->edges(src).end();
        for (uint32_t m = 0; m < num_nodes; ++m) {
          auto dest = node_set[m];
          // Binary search on the edges sorted by destination id
          auto edge_id = katana::FindEdgeSortedByDest(graph, src, dest);
          while (edge_id != *last && *graph->GetEdgeDest(edge_id) == dest) {
            subgraph_edges[n].push_back(m);
            edge_id++;
          }
        }
        out_indices[n] = subgraph_edges[n].size();
      },
      katana::steal(), katana::no_stats(),
      katana::loopname("SubgraphExtraction"));

  // Prefix sum
  for (uint64_t i = 1; i < num_nodes; ++i) {
    out_indices[i] += out_indices[i - 1];
  }
  uint64_t num_edges = out_indices[num_nodes - 1];

  // Subgraph topology : out dests
  katana::LargeArray<uint32_t> out_dests;
  out_dests.allocateInterleaved(num_edges);

  katana::do_all(
      katana::iterate(uint32_t(0), uint32_t(num_nodes)),
      [&](const uint32_t& n) {
        uint64_t offset = n == 0 ? 0 : out_indices[n - 1];
        for (uint32_t dest : subgraph_edges[n]) {
          out_dests[offset] = dest;
          offset++;
        }
      },
      katana::no_stats(), katana::loopname("ConstructTopology"));

  // TODO(amp): The pattern out_indices.release()->data() is leaking both the
  //  LargeBuffer instance (just a few bytes), AND the buffer itself. The
  //  instance is leaking because of the call to release without passing
  //  ownership of the instance to some other object. The buffer is leaking
  //  because arrow::MutableBuffer does not own it's data, so it will never
  //  deallocate the buffer passed to arrow::MutableBuffer::Wrap.
  //  This pattern probably exists elsewhere in the code.

  // Set new topology

  auto newTopo = std::make_unique<katana::GraphTopology>(
      std::move(out_indices), std::move(out_dests));

  if (auto r = subgraph->SetTopology(std::move(newTopo)); !r) {
    return r.error();
  }
  KATANA_LOG_DEBUG_ASSERT(&subgraph->topology());

  return std::unique_ptr<katana::PropertyGraph>(std::move(subgraph));
}
}  // namespace

katana::Result<std::unique_ptr<katana::PropertyGraph>>
katana::analytics::SubGraphExtraction(
    katana::PropertyGraph* pg, const std::vector<uint32_t>& node_vec,
    SubGraphExtractionPlan plan) {
  if (auto r = katana::SortAllEdgesByDest(pg); !r) {
    return r.error();
  }

  // Remove duplicates from the node vector
  std::unordered_set<uint32_t> set;
  std::vector<uint32_t> dedup_node_vec;
  for (auto n : node_vec) {
    if (set.insert(n).second) {  // If n wasn't already present.
      dedup_node_vec.push_back(n);
    }
  }

  katana::StatTimer execTime("SubGraph-Extraction");
  switch (plan.algorithm()) {
  case SubGraphExtractionPlan::kNodeSet: {
    execTime.start();
    auto subgraph = SubGraphNodeSet(pg, dedup_node_vec);
    execTime.stop();
    KATANA_LOG_DEBUG_ASSERT(subgraph);
    return std::move(subgraph.value());
  }
  default:
    return katana::ErrorCode::InvalidArgument;
  }
}
