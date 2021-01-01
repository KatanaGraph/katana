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

void
ProjectPart(MetisGraph* metis_graph) {
  HyperGraph* fine_graph = &metis_graph->parent_graph->graph;
  HyperGraph* coarse_graph = &metis_graph->graph;
  katana::do_all(
      katana::iterate(
          fine_graph->GetHedges(), static_cast<uint32_t>(fine_graph->size())),
      [&](GNode n) {
        GNode parent = fine_graph->getData(n).parent;
        auto& cn = coarse_graph->getData(parent);
        uint32_t partition = cn.partition;
        fine_graph->getData(n).partition = partition;
      },
      katana::loopname("Refining-Project-Partition"));
}

void
ResetCounter(HyperGraph* g) {
  katana::do_all(
      katana::iterate(g->GetHedges(), static_cast<uint32_t>(g->size())),
      [&](GNode n) { g->getData(n).ResetCounter(); },
      katana::loopname("Refining-Reset-Counter"));
}

void
ParallelSwaps(
    const std::vector<std::pair<uint32_t, uint32_t>>& combined_edgelist,
    const std::vector<std::pair<uint32_t, uint32_t>>& combined_nodelist,
    std::vector<HyperGraph*>* g, const uint32_t refine_max_levels) {
  uint32_t num_partitions = g->size();
  std::vector<katana::GAccumulator<WeightTy>> accum(num_partitions);
  std::vector<katana::GAccumulator<WeightTy>> node_size(num_partitions);

  uint32_t total_nodes = combined_nodelist.size();

  katana::do_all(
      katana::iterate(uint32_t{0}, total_nodes),
      [&](uint32_t v) {
        auto node_index_pair = combined_nodelist[v];
        uint32_t index = node_index_pair.second;
        GNode n = node_index_pair.first;
        MetisNode& node_data = g->at(index)->getData(n);
        WeightTy weight = node_data.weight;

        node_size[index] += weight;
        if (node_data.partition > 0) {
          accum[index] += weight;
        }
      },
      katana::loopname("Refining-Make-Balance-Swap"));

  GNodeBag partition_zero_nodes;
  GNodeBag partition_one_nodes;
  GNodeBag swap_bag;
  std::vector<GNode> partition_zero_nodes_bag;
  std::vector<GNode> partition_one_nodes_bag;
  uint32_t num_partition_zero_nodes{0};
  uint32_t num_partition_one_nodes{0};

  katana::StatTimer init_gain_timer("Refining-Init-Gains");
  katana::StatTimer sort_timer("Refining-Sort");

  for (uint32_t pass = 0; pass < refine_max_levels; pass++) {
    init_gain_timer.start();
    InitGain(combined_edgelist, combined_nodelist, *g);
    init_gain_timer.stop();

    for (uint32_t i = 0; i < num_partitions; i++) {
      if (g->at(i) == nullptr) {
        continue;
      }
      HyperGraph& cur_graph = *g->at(i);

      partition_zero_nodes.clear();
      partition_one_nodes.clear();
      partition_zero_nodes_bag.clear();
      partition_one_nodes_bag.clear();
      swap_bag.clear();

      katana::do_all(
          katana::iterate(
              cur_graph.GetHedges(), static_cast<uint32_t>(cur_graph.size())),
          [&](uint32_t n) {
            MetisNode& node_data = cur_graph.getData(n);

            if (!node_data.positive_gain && !node_data.negative_gain) {
              return;
            }

            GainTy gain = node_data.GetGain();

            if (gain < 0) {
              return;
            }

            uint32_t partition = node_data.partition;

            if (partition == 0) {
              partition_zero_nodes.push(n);
            } else {
              partition_one_nodes.push(n);
            }
          },
          katana::loopname("Refining-Find-Partition-Nodes"));

      num_partition_zero_nodes = std::distance(
          partition_zero_nodes.begin(), partition_zero_nodes.end());
      num_partition_one_nodes =
          std::distance(partition_one_nodes.begin(), partition_one_nodes.end());

      for (uint32_t n : partition_zero_nodes) {
        partition_zero_nodes_bag.push_back(n);
      }
      for (uint32_t n : partition_one_nodes) {
        partition_one_nodes_bag.push_back(n);
      }

      sort_timer.start();
      for (uint32_t iter = 0; iter < 2; iter++) {
        auto& cur_bag =
            (iter == 0) ? partition_zero_nodes_bag : partition_one_nodes_bag;
        std::sort(
            cur_bag.begin(), cur_bag.end(),
            [&cur_graph](GNode& l_opr, GNode& r_opr) {
              MetisNode& l_data = cur_graph.getData(l_opr);
              MetisNode& r_data = cur_graph.getData(r_opr);
              GainTy l_gain = l_data.GetGain();
              GainTy r_gain = r_data.GetGain();

              if (l_gain == r_gain) {
                uint32_t l_nid = l_data.node_id;
                uint32_t r_nid = r_data.node_id;

                return l_nid < r_nid;
              }

              return l_gain > r_gain;
            });
      }
      sort_timer.stop();

      uint32_t num_swap_nodes =
          (num_partition_zero_nodes <= num_partition_one_nodes)
              ? num_partition_zero_nodes
              : num_partition_one_nodes;
      for (uint32_t i = 0; i < num_swap_nodes; i++) {
        swap_bag.push(partition_one_nodes_bag[i]);
        swap_bag.push(partition_zero_nodes_bag[i]);
      }
      katana::do_all(
          katana::iterate(swap_bag),
          [&](GNode n) {
            MetisNode& node_data = cur_graph.getData(n);
            uint32_t partition = node_data.partition;
            node_data.partition = 1 - partition;
            node_data.IncCounter();
          },
          katana::loopname("Refining-Swap"));
    }
  }
  for (uint32_t i = 0; i < num_partitions; i++) {
    HyperGraph* graph = g->at(i);
    if (graph != nullptr) {
      ResetCounter(graph);
    }
  }
}

void
ParallelMakingbalance(HyperGraph* g, const float tol) {
  uint32_t total_hnodes = g->GetHnodes();
  uint32_t total_hedges = g->GetHedges();
  uint32_t graph_size = g->size();
  uint32_t sqrt_hnodes = sqrt(total_hnodes);

  katana::GAccumulator<WeightTy> accum;
  katana::GAccumulator<WeightTy> node_size;
  katana::do_all(
      katana::iterate(total_hedges, graph_size),
      [&](GNode n) {
        MetisNode& node_data = g->getData(n);
        WeightTy weight = node_data.weight;
        node_size += weight;
        if (node_data.partition > 0) {
          accum += weight;
        }
      },
      katana::loopname("Refining-Make-Balance"));

  const WeightTy hi = (1 + tol) * node_size.reduce() / (2 + tol);
  const WeightTy lo = node_size.reduce() - hi;
  WeightTy balance = accum.reduce();

  katana::StatTimer init_gain_timer("Refining-Init-Gains");
  katana::StatTimer sort_timer("Refining-Sort");
  katana::StatTimer make_balance_timer("Refining-Make-Balance");

  while (true) {
    if (balance >= lo && balance <= hi) {
      break;
    }

    init_gain_timer.start();
    InitGain(g);
    init_gain_timer.stop();

    // Creating buckets.
    std::array<std::vector<GNode>, 101> pz_nodes_vec_arr;
    std::array<std::vector<GNode>, 101> po_nodes_vec_arr;

    std::array<GNodeBag, 101> pz_nodes_bag_arr;
    std::array<GNodeBag, 101> po_nodes_bag_arr;

    // Bucket for nodes with gan by weight ratio <= -9.0f.
    std::vector<GNode> neg_pz_nodes_vec;
    std::vector<GNode> neg_po_nodes_vec;

    GNodeBag neg_pz_nodes_bag;
    GNodeBag neg_po_nodes_bag;

    bool process_zero_partition = (balance < lo) ? true : false;
    auto& cand_nodes_vec_arr =
        (process_zero_partition) ? pz_nodes_vec_arr : po_nodes_vec_arr;
    auto& cand_nodes_bag_arr =
        (process_zero_partition) ? pz_nodes_bag_arr : po_nodes_bag_arr;
    auto& neg_cand_nodes_vec =
        (process_zero_partition) ? neg_pz_nodes_vec : neg_po_nodes_vec;
    auto& neg_cand_nodes_bag =
        (process_zero_partition) ? neg_pz_nodes_bag : neg_pz_nodes_bag;

    // Placing each node in an appropriate bucket using the gain by weight
    // ratio.
    katana::do_all(
        katana::iterate(total_hedges, graph_size),
        [&](GNode n) {
          MetisNode& node_data = g->getData(n);
          float gain =
              static_cast<float>(node_data.GetGain()) / node_data.weight;
          uint32_t partition = g->getData(n).partition;

          if ((process_zero_partition && partition != 0) ||
              (!process_zero_partition && partition != 1)) {
            return;
          }

          // Nodes with gain >= 1.0f are in one bucket.
          if (gain >= 1.0f) {
            cand_nodes_bag_arr[0].emplace(n);
          } else if (gain >= 0.0f) {
            int32_t d = gain * 10.0f;
            uint32_t idx = 10 - d;
            cand_nodes_bag_arr[idx].emplace(n);
          } else if (gain > -9.0f) {
            int32_t d = gain * 10.0f - 1;
            uint32_t idx = 10 - d;
            cand_nodes_bag_arr[idx].emplace(n);
          } else { /* NODES with gain by weight ratio <= -9.0f are in one
                      bucket */
            neg_cand_nodes_bag.emplace(n);
          }
        },
        katana::loopname("Refining-Bucket-Gain"));

    // Sorting each bucket in parallel.
    katana::do_all(
        katana::iterate(cand_nodes_bag_arr),
        [&](GNodeBag& cand_nodes_bag) {
          if (cand_nodes_bag.empty()) {
            return;
          }

          GNode n = *cand_nodes_bag.begin();
          MetisNode& node_data = g->getData(n);
          float gain =
              static_cast<float>(node_data.GetGain()) / node_data.weight;
          uint32_t idx{0};
          if (gain < 1.0f) {
            GainTy weighted_gain{static_cast<int32_t>(gain * 10.0f)};
            if (gain < 0.0f) {
              weighted_gain -= 1;
            }
            idx = 10 - weighted_gain;
          }
          for (GNode cand_node : cand_nodes_bag) {
            cand_nodes_vec_arr[idx].push_back(cand_node);
          }

          SortNodesByGainAndWeight(g, &cand_nodes_vec_arr[idx], 0);
        },
        katana::loopname("Refining-Sort-Bucket"));

    uint32_t i{0}, j{0};

    make_balance_timer.start();
    // Now moving nodes from partition 0 to 1.
    while (j <= 100) {
      if (cand_nodes_vec_arr[j].size() == 0) {
        j++;
        continue;
      }

      for (GNode cand_node : cand_nodes_vec_arr[j]) {
        MetisNode& cand_node_data = g->getData(cand_node);
        uint32_t partition = cand_node_data.partition;
        cand_node_data.partition = 1 - partition;

        if (process_zero_partition) {
          balance += cand_node_data.weight;
          if (balance >= lo) {
            break;
          }
        } else {
          balance -= cand_node_data.weight;
          if (balance <= hi) {
            break;
          }
        }
        i++;
        if (i > sqrt_hnodes) {
          break;
        }
      }

      if ((process_zero_partition && balance >= lo) ||
          (!process_zero_partition && balance <= hi) || i > sqrt_hnodes) {
        break;
      }
      j++;
    }
    make_balance_timer.stop();

    if ((process_zero_partition && balance >= lo) ||
        (!process_zero_partition && balance <= hi)) {
      break;
    }

    if (i > sqrt_hnodes || neg_cand_nodes_bag.empty()) {
      continue;
    }

    for (GNode cand_node : neg_cand_nodes_bag) {
      neg_cand_nodes_vec.push_back(cand_node);
    }

    sort_timer.start();
    SortNodesByGainAndWeight(g, &neg_cand_nodes_vec, 0);
    sort_timer.stop();

    make_balance_timer.start();
    for (GNode cand_node : neg_cand_nodes_vec) {
      MetisNode& cand_node_data = g->getData(cand_node);
      uint32_t partition = cand_node_data.partition;

      cand_node_data.partition = 1 - partition;
      if (process_zero_partition) {
        balance += cand_node_data.weight;
        if (balance >= lo) {
          break;
        }
      } else {
        balance -= cand_node_data.weight;
        if (balance <= hi) {
          break;
        }
      }
      i++;
      if (i > sqrt_hnodes) {
        break;
      }
    }
    make_balance_timer.stop();

    if ((process_zero_partition && balance >= lo) ||
        (!process_zero_partition && balance <= hi)) {
      break;
    }
  }
}

void
Refine(std::vector<MetisGraph*>* coarse_graph) {
  uint32_t num_partitions = coarse_graph->size();

  std::vector<float> ratio(num_partitions, 0.0f);
  std::vector<float> tol(num_partitions, 0.0f);
  std::vector<MetisGraph*> fine_graph(num_partitions, nullptr);
  std::vector<HyperGraph*> gg(num_partitions, nullptr);

  katana::StatTimer construct_timer("Refining-Total-Construct-Lists");
  katana::StatTimer parallel_swap_timer("Refining-Total-Parallel-Swap");
  katana::StatTimer make_balance_timer("Refining-Total-Make-Balance");
  katana::StatTimer project_partition_timer("Refining-Total-Project-Partition");

  for (uint32_t i = 0; i < num_partitions; i++) {
    if (coarse_graph->at(i) == nullptr) {
      continue;
    }

    ratio[i] = 52.5 / 47.5;  // change if needed
    tol[i] = std::max(ratio[i], 1 - ratio[i]) - 1;
  }

  uint32_t total_hnodes{0}, total_hedges{0};

  while (true) {
    for (uint32_t i = 0; i < num_partitions; i++) {
      MetisGraph* graph = coarse_graph->at(i);
      if (graph != nullptr) {
        fine_graph[i] = graph->parent_graph;
        gg[i] = &graph->graph;
        total_hnodes += gg[i]->GetHnodes();
        total_hedges += gg[i]->GetHedges();
      } else {
        gg[i] = nullptr;
      }
    }

    construct_timer.start();
    std::vector<std::pair<uint32_t, uint32_t>> combined_edgelist(total_hedges);
    std::vector<std::pair<uint32_t, uint32_t>> combined_nodelist(total_hnodes);

    ConstructCombinedLists(
        *coarse_graph, &combined_edgelist, &combined_nodelist);

    construct_timer.stop();
    ParallelSwaps(combined_edgelist, combined_nodelist, &gg, 2);

    make_balance_timer.start();
    // Not maiking it further parallel since it only takes 4% of the total
    // running time.
    for (uint32_t i = 0; i < num_partitions; i++) {
      if (gg[i] != nullptr) {
        ParallelMakingbalance(gg[i], tol[i]);
      }
    }
    make_balance_timer.stop();

    project_partition_timer.start();
    for (uint32_t i = 0; i < num_partitions; i++) {
      if (fine_graph[i] != nullptr) {
        ProjectPart(coarse_graph->at(i));
      }
    }
    project_partition_timer.stop();

    bool all_done{true};
    for (uint32_t i = 0; i < num_partitions; i++) {
      MetisGraph** graph = &coarse_graph->at(i);
      if (*graph != nullptr) {
        *graph = (*graph)->parent_graph;
        all_done = false;
      }
    }

    if (all_done) {
      break;
    }

    total_hnodes = 0;
    total_hedges = 0;
  }
}
