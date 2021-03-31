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
  auto out_indices = std::make_unique<katana::LargeArray<uint64_t>>();
  out_indices->allocateInterleaved(num_nodes);

  katana::gstl::Vector<katana::gstl::Vector<uint32_t>> subgraph_edges;
  subgraph_edges.resize(num_nodes);

  katana::do_all(
      katana::iterate(uint32_t(0), uint32_t(num_nodes)),
      [&](const uint32_t& n) {
        uint32_t src = node_set[n];
        auto edge_range = graph->topology().edge_range(src);
        for (auto dest : node_set) {
          // Binary search on the edges sorted by destination id
          auto edge_matched = std::lower_bound(
              edge_iterator(edge_range.first), edge_iterator(edge_range.second),
              dest);
          if (*edge_matched != edge_range.second) {
            while (*edge_matched == dest) {
              subgraph_edges[n].push_back(dest);
              edge_matched++;
            }
          }
          (*out_indices)[n] = subgraph_edges[n].size();
        }
      },
      katana::steal(), katana::no_stats(),
      katana::loopname("SubgraphExtraction"));

  // Prefix sum
  for (uint64_t i = 1; i < num_nodes; ++i) {
    (*out_indices)[i] += (*out_indices)[i - 1];
  }
  uint64_t num_edges = (*out_indices)[num_nodes - 1];
  // Subgraph topology : out dests
  auto out_dests = std::make_unique<katana::LargeArray<uint32_t>>();
  out_dests->allocateInterleaved(num_edges);

  katana::do_all(
      katana::iterate(uint32_t(0), uint32_t(num_nodes)),
      [&](const uint32_t& n) {
        uint64_t offset = (*out_indices)[n];
        for (uint32_t dest : subgraph_edges[n]) {
          (*out_dests)[offset] = dest;
          offset++;
        }
      },
      katana::no_stats(), katana::loopname("ConstructTopology"));

  // Set new topology
  auto numeric_array_out_indices =
      std::make_shared<arrow::NumericArray<arrow::UInt64Type>>(
          static_cast<int64_t>(num_nodes),
          arrow::MutableBuffer::Wrap(out_indices.release()->data(), num_nodes));

  auto numeric_array_out_dests =
      std::make_shared<arrow::NumericArray<arrow::UInt32Type>>(
          static_cast<int64_t>(num_edges),
          arrow::MutableBuffer::Wrap(out_dests.release()->data(), num_edges));

  if (auto r = subgraph->SetTopology(katana::GraphTopology{
          .out_indices = std::move(numeric_array_out_indices),
          .out_dests = std::move(numeric_array_out_dests),
      });
      !r) {
    return r.error();
  }

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
  for (auto i : node_vec) {
    set.insert(i);
  }
  dedup_node_vec.assign(set.begin(), set.end());

  katana::StatTimer execTime("SubGraph-Extraction");
  switch (plan.algorithm()) {
  case SubGraphExtractionPlan::kNodeSet: {
    execTime.start();
    auto subgraph = SubGraphNodeSet(pg, dedup_node_vec);
    execTime.stop();
    return std::move(subgraph.value());
  }
  default:
    return katana::ErrorCode::InvalidArgument;
  }
}
