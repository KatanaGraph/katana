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

#include "Helper.h"
#include "katana/AtomicHelpers.h"

/**
 * Computes the degrees of the nodes
 *
 * @param graph Vector of graphs
 * @param combined_edge_list Concatenated list of hyperedges of the graphs in
 * specified param graph
 * @param combined_node_list Concatenated list of nodes of the graphs in
 * specified param graph
 */
void
ComputeDegrees(
    std::vector<HyperGraph*>* graph,
    const std::vector<std::pair<uint32_t, uint32_t>>& combined_edge_list,
    const std::vector<std::pair<uint32_t, uint32_t>>& combined_node_list) {
  uint32_t total_nodes = combined_node_list.size();

  katana::do_all(
      katana::iterate(uint32_t{0}, total_nodes),
      [&](uint32_t n) {
        auto node_index_pair = combined_node_list[n];
        uint32_t index = node_index_pair.second;
        GNode node = node_index_pair.first;
        graph->at(index)->getData(node).degree = 0;
      },
      katana::loopname("Partitioning-Init-Degrees"));

  uint32_t total_hedges = combined_edge_list.size();

  katana::do_all(
      katana::iterate(uint32_t{0}, total_hedges),
      [&](GNode hedge) {
        auto hedge_index_pair = combined_edge_list[hedge];
        uint32_t index = hedge_index_pair.second;
        GNode h = hedge_index_pair.first;
        HyperGraph& cur_graph = *graph->at(index);
        auto edges = cur_graph.edges(h);

        uint32_t degree = std::distance(edges.begin(), edges.end());

        // No need to add degree for lone hedges.
        if (degree <= 1) {
          return;
        }

        for (auto& fedge : edges) {
          GNode member_node = cur_graph.getEdgeDst(fedge);
          katana::atomicAdd(cur_graph.getData(member_node).degree, uint32_t{1});
        }
      },
      katana::loopname("Partitioning-Calculate-Degrees"));
}

/**
 * Finds an initial partition of the coarsest graphs
 *
 * @param metis_graphs Vector of metis graphs
 * @param K Vector corresponding to the number of target partitions that needs
 * to be created for the graphs in specified param metis_graphs
 */
void
PartitionCoarsestGraphs(
    const std::vector<MetisGraph*>& metis_graphs,
    const std::vector<unsigned>& target_partitions) {
  assert(metis_graphs.size() == target_partitions.size());
  uint32_t num_partitions = metis_graphs.size();
  std::vector<katana::GAccumulator<WeightTy>> nzero_accum(num_partitions);
  std::vector<katana::GAccumulator<WeightTy>> zero_accum(num_partitions);
  std::vector<GNodeBag> zero_partition_nodes(num_partitions);
  std::vector<GNodeBag> nzero_partition_nodes(num_partitions);
  std::vector<HyperGraph*> graph(num_partitions, nullptr);
  uint32_t total_hedges{0};
  uint32_t total_nodes{0};

  for (uint32_t i = 0; i < num_partitions; i++) {
    if (metis_graphs[i] != nullptr) {
      graph[i] = &metis_graphs[i]->graph;
    }
  }

  for (uint32_t i = 0; i < num_partitions; i++) {
    if (graph[i] != nullptr) {
      total_hedges += graph[i]->GetHedges();
    }
  }

  for (uint32_t i = 0; i < num_partitions; i++) {
    if (graph[i] != nullptr) {
      total_nodes += graph[i]->GetHnodes();
    }
  }

  std::vector<std::pair<uint32_t, uint32_t>> combined_edge_list(total_hedges);
  std::vector<std::pair<uint32_t, uint32_t>> combined_node_list(total_nodes);

  ConstructCombinedLists(
      metis_graphs, &combined_edge_list, &combined_node_list);

  katana::do_all(
      katana::iterate(uint32_t{0}, total_nodes),
      [&](uint32_t n) {
        auto node_index_pair = combined_node_list[n];
        uint32_t index = node_index_pair.second;
        GNode item = node_index_pair.first;

        MetisNode& node_data = graph[index]->getData(item);
        nzero_accum[index] += node_data.weight;
        node_data.InitRefine(1);
      },
      katana::loopname("Partitioning-Init-PartitionOne"));

  katana::do_all(
      katana::iterate(uint32_t{0}, total_hedges),
      [&](uint32_t hedge) {
        auto hedge_index_pair = combined_edge_list[hedge];
        uint32_t index = hedge_index_pair.second;
        HyperGraph& sub_graph = *graph[index];
        GNode item = hedge_index_pair.first;

        for (auto& fedge : sub_graph.edges(item)) {
          GNode node = sub_graph.getEdgeDst(fedge);
          MetisNode& node_data = sub_graph.getData(node);
          node_data.partition = 0;
        }
      },
      katana::steal(), katana::loopname("Partitioning-Init-PartitionZero"));

  katana::do_all(
      katana::iterate(uint32_t{0}, total_nodes),
      [&](uint32_t node) {
        auto node_index_pair = combined_node_list[node];
        uint32_t index = node_index_pair.second;
        GNode item = node_index_pair.first;
        MetisNode& node_data = graph[index]->getData(item);

        if (node_data.partition == 0) {
          zero_partition_nodes[index].push(item);
          zero_accum[index] += node_data.weight;
        } else {
          nzero_partition_nodes[index].push(item);
        }
      },
      katana::loopname("Partitioning-Aggregate-Nodes"));

  // first compute degree of every node
  ComputeDegrees(&graph, combined_edge_list, combined_node_list);

  for (uint32_t i = 0; i < num_partitions; i++) {
    if (graph[i] == nullptr) {
      continue;
    }
    HyperGraph* cur_graph = graph[i];

    WeightTy total_weights = nzero_accum[i].reduce();
    WeightTy zero_partition_weights = zero_accum[i].reduce();
    WeightTy first_partition_weights = total_weights - zero_partition_weights;
    bool process_zero_partition =
        (zero_partition_weights > first_partition_weights);
    WeightTy sqrt_size = sqrt(total_weights);
    uint32_t curr_partition = (process_zero_partition) ? 0 : 1;
    uint32_t k_val = (target_partitions[i] + 1) / 2;
    WeightTy target_weight = (total_weights * k_val) / target_partitions[i];
    if (process_zero_partition) {
      target_weight = total_weights - target_weight;
    }
    GNodeBag& node_bag = (process_zero_partition) ? zero_partition_nodes[i]
                                                  : nzero_partition_nodes[i];
    uint32_t node_bag_size = std::distance(node_bag.begin(), node_bag.end());
    std::vector<GNode> node_vec(node_bag_size);
    uint32_t idx{0};
    WeightTy moved_weight = (process_zero_partition) ? first_partition_weights
                                                     : zero_partition_weights;

    for (auto& item : node_bag) {
      node_vec[idx++] = item;
    }

    katana::StatTimer init_gain_timer("Partitioning-Init-Gains");
    katana::StatTimer aggregate_node_timer("Partitioning-Aggregate-Nodes");
    katana::StatTimer sort_timer("Partitioning-Sort");
    katana::StatTimer find_partitionone_timer("Partitioning-Find-PartitionOne");
    while (true) {
      init_gain_timer.start();
      InitGain(cur_graph);
      init_gain_timer.stop();

      node_bag.clear();

      katana::do_all(
          katana::iterate(uint32_t{0}, idx),
          [&](uint32_t node_id) {
            GNode node = node_vec[node_id];
            uint32_t partition = cur_graph->getData(node).partition;
            if ((process_zero_partition && partition == 0) ||
                (!process_zero_partition && partition == 1)) {
              node_bag.emplace(node);
            }
          },
          katana::loopname("Partitioning-Aggregate-Nodes"));

      aggregate_node_timer.start();
      idx = 0;
      for (auto& item : node_bag) {
        node_vec[idx++] = item;
      }
      aggregate_node_timer.stop();

      sort_timer.start();
      SortNodesByGainAndWeight(cur_graph, &node_vec, idx);
      sort_timer.stop();

      find_partitionone_timer.start();
      uint32_t node_size{0};
      for (uint32_t node_id = 0; node_id < idx; node_id++) {
        GNode node = node_vec[node_id];
        MetisNode& node_data = cur_graph->getData(node);
        node_data.partition = 1 - curr_partition;
        moved_weight += node_data.weight;

        // Check if node is a lone hedge.
        uint32_t degree = node_data.degree;

        if (degree >= 1) {
          node_size++;
        }
        if (moved_weight >= target_weight) {
          break;
        }
        if (node_size > sqrt_size) {
          break;
        }
      }
      find_partitionone_timer.stop();

      if (moved_weight >= target_weight) {
        break;
      }
    }
  }
}
