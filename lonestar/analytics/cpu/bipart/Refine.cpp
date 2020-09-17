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
  GGraph* fine_graph = metis_graph->GetParentGraph()->GetGraph();
  GGraph* coarse_graph = metis_graph->GetGraph();
  galois::do_all(
      galois::iterate(
          fine_graph->hedges, static_cast<uint32_t>(fine_graph->size())),
      [&](GNode n) {
        auto parent = fine_graph->getData(n).GetParent();
        auto& cn = coarse_graph->getData(parent);
        uint32_t partition = cn.GetPartition();
        fine_graph->getData(n).SetPartition(partition);
      },
      galois::loopname("Refining-Project-Partition"));
}

void
ResetCounter(GGraph& g) {
  galois::do_all(
      galois::iterate(g.hedges, static_cast<uint32_t>(g.size())),
      [&](GNode n) { g.getData(n).ResetCounter(); },
      galois::loopname("Refining-Reset-Counter"));
}

void
ParallelSwaps(
    std::vector<std::pair<uint32_t, uint32_t>>& combined_edgelist,
    std::vector<std::pair<uint32_t, uint32_t>>& combined_nodelist,
    std::vector<GGraph*>& g, uint32_t refine_max_levels) {
  uint32_t num_partitions = g.size();
  std::vector<galois::GAccumulator<WeightTy>> accum(num_partitions);
  std::vector<galois::GAccumulator<WeightTy>> node_size(num_partitions);

  uint32_t total_nodes = combined_nodelist.size();

  galois::do_all(
      galois::iterate(uint32_t{0}, total_nodes),
      [&](uint32_t v) {
        auto node_index_pair = combined_nodelist[v];
        uint32_t index = node_index_pair.second;
        GNode n = node_index_pair.first;
        MetisNode& node_data = g[index]->getData(n);
        WeightTy weight = node_data.GetWeight();

        node_size[index] += weight;
        if (node_data.GetPartition() > 0) {
          accum[index] += weight;
        }
      },
      galois::loopname("Refining-Make-Balance-Swap"));

  GNodeBag partition_zero_nodes;
  GNodeBag partition_one_nodes;
  GNodeBag swap_bag;
  std::vector<GNode> partition_zero_nodes_bag;
  std::vector<GNode> partition_one_nodes_bag;
  uint32_t num_partition_zero_nodes{0};
  uint32_t num_partition_one_nodes{0};

  galois::StatTimer init_gain_timer("Refining-Init-Gains");
  galois::StatTimer sort_timer("Refining-Sort");

  for (uint32_t pass = 0; pass < refine_max_levels; pass++) {
    init_gain_timer.start();
    InitGain(combined_edgelist, combined_nodelist, g);
    init_gain_timer.stop();

    for (uint32_t i = 0; i < num_partitions; i++) {
      if (g[i] == nullptr) {
        continue;
      }
      GGraph& cur_graph = *g[i];

      partition_zero_nodes.clear();
      partition_one_nodes.clear();
      partition_zero_nodes_bag.clear();
      partition_one_nodes_bag.clear();
      swap_bag.clear();

      galois::do_all(
          galois::iterate(
              cur_graph.hedges, static_cast<uint32_t>(cur_graph.size())),
          [&](uint32_t n) {
            MetisNode& node_data = cur_graph.getData(n);

            if (!node_data.GetPositiveGain() && !node_data.GetNegativeGain()) {
              return;
            }

            GainTy gain = node_data.GetGain();

            if (gain < 0) {
              return;
            }

            uint32_t partition = node_data.GetPartition();

            if (partition == 0) {
              partition_zero_nodes.push(n);
            } else {
              partition_one_nodes.push(n);
            }
          },
          galois::loopname("Refining-Find-Partition-Nodes"));

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
                uint32_t l_nid = l_data.GetNodeId();
                uint32_t r_nid = r_data.GetNodeId();

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
      galois::do_all(
          galois::iterate(swap_bag),
          [&](GNode n) {
            MetisNode& node_data = cur_graph.getData(n);
            uint32_t partition = node_data.GetPartition();
            node_data.SetPartition(1 - partition);
            node_data.IncCounter();
          },
          galois::loopname("Refining-Swap"));
    }
  }
  for (uint32_t i = 0; i < num_partitions; i++) {
    if (g[i] != nullptr) {
      ResetCounter(*g[i]);
    }
  }
}

void
ParallelMakingbalance(GGraph& g, float tol) {
  uint32_t total_hnodes = g.hnodes;
  uint32_t total_hedges = g.hedges;
  uint32_t graph_size = g.size();
  uint32_t sqrt_hnodes = sqrt(total_hnodes);

  galois::GAccumulator<WeightTy> accum;
  galois::GAccumulator<WeightTy> node_size;
  galois::do_all(
      galois::iterate(total_hedges, graph_size),
      [&](GNode n) {
        MetisNode& node_data = g.getData(n);
        WeightTy weight = node_data.GetWeight();
        node_size += weight;
        if (node_data.GetPartition() > 0) {
          accum += weight;
        }
      },
      galois::loopname("Refining-Make-Balance"));

  const WeightTy hi = (1 + tol) * node_size.reduce() / (2 + tol);
  const WeightTy lo = node_size.reduce() - hi;
  WeightTy balance = accum.reduce();

  galois::StatTimer init_gain_timer("Refining-Init-Gains");
  galois::StatTimer sort_timer("Refining-Sort");
  galois::StatTimer make_balance_timer("Refining-Make-Balance");

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
    galois::do_all(
        galois::iterate(total_hedges, graph_size),
        [&](GNode n) {
          MetisNode& node_data = g.getData(n);
          float gain =
              static_cast<float>(node_data.GetGain()) / node_data.GetWeight();
          uint32_t partition = g.getData(n).GetPartition();

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
        galois::loopname("Refining-Bucket-Gain"));

    // Sorting each bucket in parallel.
    galois::do_all(
        galois::iterate(cand_nodes_bag_arr),
        [&](GNodeBag& cand_nodes_bag) {
          if (cand_nodes_bag.empty()) {
            return;
          }

          GNode n = *cand_nodes_bag.begin();
          MetisNode& node_data = g.getData(n);
          float gain =
              static_cast<float>(node_data.GetGain()) / node_data.GetWeight();
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

          SortNodesByGainAndWeight(g, cand_nodes_vec_arr[idx], 0);
        },
        galois::steal(), galois::loopname("Refining-Sort-Bucket"));

    uint32_t i{0}, j{0};

    make_balance_timer.start();
    // Now moving nodes from partition 0 to 1.
    while (j <= 100) {
      if (cand_nodes_vec_arr[j].size() == 0) {
        j++;
        continue;
      }

      for (GNode cand_node : cand_nodes_vec_arr[j]) {
        MetisNode& cand_node_data = g.getData(cand_node);
        uint32_t partition = cand_node_data.GetPartition();
        cand_node_data.SetPartition(1 - partition);

        if (process_zero_partition) {
          balance += cand_node_data.GetWeight();
          if (balance >= lo) {
            break;
          }
        } else {
          balance -= cand_node_data.GetWeight();
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
    SortNodesByGainAndWeight(g, neg_cand_nodes_vec, 0);
    sort_timer.stop();

    make_balance_timer.start();
    for (GNode cand_node : neg_cand_nodes_vec) {
      MetisNode& cand_node_data = g.getData(cand_node);
      uint32_t partition = cand_node_data.GetPartition();

      cand_node_data.SetPartition(1 - partition);
      if (process_zero_partition) {
        balance += cand_node_data.GetWeight();
        if (balance >= lo) {
          break;
        }
      } else {
        balance -= cand_node_data.GetWeight();
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
Refine(std::vector<MetisGraph*>& coarse_graph) {
  uint32_t num_partitions = coarse_graph.size();

  std::vector<float> ratio(num_partitions, 0.0f);
  std::vector<float> tol(num_partitions, 0.0f);
  std::vector<MetisGraph*> fine_graph(num_partitions, nullptr);
  std::vector<GGraph*> gg(num_partitions, nullptr);

  galois::StatTimer construct_timer("Refining-Total-Construct-Lists");
  galois::StatTimer parallel_swap_timer("Refining-Total-Parallel-Swap");
  galois::StatTimer make_balance_timer("Refining-Total-Make-Balance");
  galois::StatTimer project_partition_timer("Refining-Total-Project-Partition");

  for (uint32_t i = 0; i < num_partitions; i++) {
    if (coarse_graph[i] == nullptr) {
      continue;
    }

    ratio[i] = 52.5 / 47.5;  // change if needed
    tol[i] = std::max(ratio[i], 1 - ratio[i]) - 1;
  }

  uint32_t total_hnodes{0}, total_hedges{0};

  while (true) {
    for (uint32_t i = 0; i < num_partitions; i++) {
      if (coarse_graph[i] != nullptr) {
        fine_graph[i] = coarse_graph[i]->GetParentGraph();
        gg[i] = coarse_graph[i]->GetGraph();
        total_hnodes += gg[i]->hnodes;
        total_hedges += gg[i]->hedges;
      } else {
        gg[i] = nullptr;
      }
    }

    construct_timer.start();
    std::vector<std::pair<uint32_t, uint32_t>> combined_edgelist(total_hedges);

    std::vector<std::pair<uint32_t, uint32_t>> combined_nodelist(total_hnodes);

    ConstructCombinedLists(coarse_graph, combined_edgelist, combined_nodelist);

    construct_timer.stop();
    ParallelSwaps(combined_edgelist, combined_nodelist, gg, 2);

    make_balance_timer.start();
    // Not maiking it further parallel since it only takes 4% of the total
    // running time.
    for (uint32_t i = 0; i < num_partitions; i++) {
      if (gg[i] != nullptr) {
        ParallelMakingbalance(*gg[i], tol[i]);
      }
    }
    make_balance_timer.stop();

    project_partition_timer.start();
    for (uint32_t i = 0; i < num_partitions; i++) {
      if (fine_graph[i] != nullptr) {
        ProjectPart(coarse_graph[i]);
      }
    }
    project_partition_timer.stop();

    bool all_done{true};
    for (uint32_t i = 0; i < num_partitions; i++) {
      if (coarse_graph[i] != nullptr) {
        coarse_graph[i] = coarse_graph[i]->GetParentGraph();
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
