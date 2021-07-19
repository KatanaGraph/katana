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

/**
 *  @file Coarsening.cpp
 *
 *  Contains the implementation for the Coarsening phase of the multi-level
 *  partitioning algorithm
 */

#include "Helper.h"
#include "katana/AtomicHelpers.h"
#include "katana/DynamicBitset.h"

// maximum weight limit for a coarsened node
WeightTy kLimitWeights[100];

using MatchingPolicyFunction = void(GNode, HyperGraph*);

// maximum number of lone nodes that can be created in the coarsened graph
constexpr static const uint32_t kLoneNodesCoarsenFactor = 1000u;
// lower limit for the number of hyperedges in the coarsest graph
constexpr static const uint32_t kCoarsestSizeLimit = 1000u;
// lower limit for the number of nodes in the coarsest graph
constexpr static const uint32_t kCoarsestNodeLimit = 300u;
/**
 * Generates a hashed value
 *
 * @param val uint32 value
 *
 * @returns hashed value for the specified param val
 */
uint32_t
Hash(NetnumTy val) {
  int64_t seed = val * 1103515245 + 12345;
  return ((seed / 65536) % 32768);
}

/**
 * Assigns a netrand value to every hyperedge
 *
 * @param graph Vector containing finer-graphs
 * @param combined_edge_list Concatenated list of hyperedges of the
 * finer-graphs in specified param graph
 */
void
ParallelRand(
    const std::vector<MetisGraph*>& graph,
    const std::vector<std::pair<uint32_t, uint32_t>>& combined_edge_list) {
  katana::do_all(
      katana::iterate(
          uint32_t{0}, static_cast<uint32_t>(combined_edge_list.size())),
      [&](uint32_t h) {
        auto hedge_index_pair = combined_edge_list[h];
        uint32_t index = hedge_index_pair.second;
        if (graph[index] == nullptr) {
          return;
        }
        GNode src = hedge_index_pair.first;
        MetisNode& node = graph[index]->parent_graph->graph.getData(src);
        NetvalTy netrand = Hash(node.netnum);
        node.netrand = netrand;
      },
      katana::loopname("Coarsening-Assign-Rand"));
}

/**
 * Assigns a matching for every node to a hyperedge
 *
 * @param graph Vector containing finer-graphs
 * @param combined_edge_list Concatenated list of hyperedges of the
 * finer-graphs in specified param graph
 */
template <MatchingPolicyFunction Matcher>
void
ParallelPrioRand(
    const std::vector<MetisGraph*>& graph,
    const std::vector<std::pair<uint32_t, uint32_t>>& combined_edge_list) {
  ParallelRand(graph, combined_edge_list);

  uint32_t total_hedge_size = combined_edge_list.size();

  // Make partitioning deterministic.
  katana::do_all(
      katana::iterate(uint32_t{0}, total_hedge_size),
      [&](uint32_t h) {
        auto hedge_index_pair = combined_edge_list[h];
        uint32_t g_index = hedge_index_pair.second;

        if (graph[g_index] == nullptr) {
          return;
        }

        GNode hedge = hedge_index_pair.first;
        HyperGraph* fine_graph = &graph[g_index]->parent_graph->graph;

        Matcher(hedge, fine_graph);
        // Iterate inside normal edges of the hyper edge.
        for (auto& fedge : fine_graph->edges(hedge)) {
          GNode dst = fine_graph->getEdgeDst(fedge);
          katana::atomicMin(
              fine_graph->getData(dst).netval,
              fine_graph->getData(hedge).netval.load());
        }
      },
      katana::steal(), katana::chunk_size<kChunkSize>(),
      katana::loopname("Coarsening-PrioRand-netval"));

  katana::do_all(
      katana::iterate(uint32_t{0}, total_hedge_size),
      [&](uint32_t h) {
        auto hedge_index_pair = combined_edge_list[h];
        uint32_t g_index = hedge_index_pair.second;

        if (graph[g_index] == nullptr) {
          return;
        }

        GNode hedge = hedge_index_pair.first;
        HyperGraph* fine_graph = &graph[g_index]->parent_graph->graph;
        MetisNode& hedge_data = fine_graph->getData(hedge);
        for (auto& fedge : fine_graph->edges(hedge)) {
          GNode dst = fine_graph->getEdgeDst(fedge);
          MetisNode& dst_node_data = fine_graph->getData(dst);

          if (dst_node_data.netval == hedge_data.netval) {
            katana::atomicMin(dst_node_data.netrand, hedge_data.netrand.load());
          }
        }
      },
      katana::steal(), katana::chunk_size<kChunkSize>(),
      katana::loopname("Coarsening-PrioRand-netrand"));

  katana::do_all(
      katana::iterate(uint32_t{0}, total_hedge_size),
      [&](uint32_t h) {
        auto hedge_index_pair = combined_edge_list[h];
        uint32_t g_index = hedge_index_pair.second;

        if (graph[g_index] == nullptr) {
          return;
        }

        GNode hedge = hedge_index_pair.first;
        HyperGraph* fine_graph = &graph[g_index]->parent_graph->graph;
        MetisNode& hedge_data = fine_graph->getData(hedge);
        for (auto& fedge : fine_graph->edges(hedge)) {
          GNode dst = fine_graph->getEdgeDst(fedge);
          MetisNode& dst_node_data = fine_graph->getData(dst);

          if (dst_node_data.netrand == hedge_data.netrand) {
            katana::atomicMin(dst_node_data.netnum, hedge_data.netnum.load());
          }
        }
      },
      katana::steal(), katana::chunk_size<kChunkSize>(),
      katana::loopname("Coarsening-PrioRand-netnum"));
}

/**
 * Identifies hyperedges whose nodes are matched to different hyperedges.
 * This implies that such hyperedge should definitely be part of the
 * coarsened graph.
 *
 * @param graph Vector containing finer-graphs
 * @param combined_edge_list Concatenated list of hyperedges of the
 * finer-graphs in specified param graph
 * @param nodes Vector of GNodeBags containing node ids to be created in
 * the coarsened graphs
 * @param hedges Vector of DynamicBitset that represents whether a hyperedge
 * needs to be added to the coarsened graph
 * @param weight Vector of vectors containing the weight value of the
 * coarsened nodes
 */
template <MatchingPolicyFunction Matcher>
void
ParallelHMatchAndCreateNodes(
    const std::vector<MetisGraph*>& graph,
    const std::vector<std::pair<uint32_t, uint32_t>>& combined_edge_list,
    std::vector<GNodeBag>* nodes, std::vector<katana::DynamicBitset>* hedges,
    std::vector<std::vector<WeightTy>>* weight) {
  ParallelPrioRand<Matcher>(graph, combined_edge_list);

  uint32_t total_hedge_size = combined_edge_list.size();
  uint32_t num_partitions = graph.size();
  std::vector<katana::InsertBag<GNode>> hedge_bag(num_partitions);

  katana::do_all(
      katana::iterate(uint32_t{0}, total_hedge_size),
      [&](uint32_t h) {
        auto hedge_index_pair = combined_edge_list[h];
        uint32_t index = hedge_index_pair.second;

        if (graph[index] == nullptr) {
          return;
        }

        HyperGraph* fine_graph = &graph[index]->parent_graph->graph;

        GNode hedge = hedge_index_pair.first;
        MetisNode& hedge_data = fine_graph->getData(hedge);
        std::vector<GNode> edges;
        GNode node_id{std::numeric_limits<GNode>::max()};
        bool flag{false};
        WeightTy total_node_weight{0};

        // Flag is set if any node is in any match.
        // If the total weights of the currently added nodes
        // exceeds the limit, then the current node is not
        // included in the currently visiting match; so ignore it for now.
        for (auto& fedge : fine_graph->edges(hedge)) {
          GNode dst = fine_graph->getEdgeDst(fedge);
          MetisNode& dst_node_data = fine_graph->getData(dst);
          if (dst_node_data.IsMatched()) {
            flag = true;
            continue;
          }
          if (dst_node_data.netnum == hedge_data.netnum) {
            WeightTy dst_node_weight = dst_node_data.weight;
            if (total_node_weight + dst_node_weight > kLimitWeights[index]) {
              break;
            }
            edges.push_back(dst);
            total_node_weight += dst_node_weight;
            node_id = std::min(node_id, dst);
          } else { /* If the dst is in the different match. */
            flag = true;
          }
        }

        // If the edges bag is not empty,
        // then the item node can construct the new match.
        // (as the above code specified, netnum of the hyper edge and the
        // node should be the same)
        // In this case, the parent node is the node having the minimum
        // node id.
        if (!edges.empty()) {
          // Only one node is matched to this hyperedge
          // will be taken care of in a later phase.
          if (flag && edges.size() == 1) {
            return;
          }
          hedge_data.SetMatched();
          if (flag) { /* Consider this hedge as the separate match. */
            hedge_bag[index].push(hedge);
          }
          // A representative node is stored in the bag.
          nodes->at(index).push(node_id);
          // Confirm that the member edges of the match are matched.
          WeightTy total_member_node_weight{0};
          for (GNode member : edges) {
            MetisNode& member_node = fine_graph->getData(member);
            total_member_node_weight += member_node.weight;
            member_node.SetMatched();
            member_node.parent = node_id;
            member_node.netnum = hedge_data.netnum;
          }
          weight->at(index)[node_id - fine_graph->GetHedges()] =
              total_member_node_weight;
        }
      },
      katana::steal(), katana::chunk_size<kChunkSize>(),
      katana::loopname("Coarsening-EdgeMatching-phaseI"));

  katana::do_all(
      katana::iterate(uint32_t{0}, num_partitions),
      [&](uint32_t i) {
        if (graph[i] == nullptr) {
          return;
        }
        HyperGraph* fine_graph = &graph[i]->parent_graph->graph;
        if (fine_graph == nullptr) {
          return;
        }

        for (uint32_t hedge : hedge_bag[i]) {
          // This hedge needs to be added to the coarsened graph.
          hedges->at(i).set(hedge);
        }
      },
      katana::steal(), katana::loopname("Coarsening-Set-MatchedHEdge"));
}

/**
 * Merges/coarsen the lone nodes to one of the neighbor nodes that is already
 * coarsened
 *
 * @param graph Vector containing finer-graphs
 * @param combined_edge_list Concatenated list of hyperedges of the
 * finer-graphs in specified param graph
 * @param weight Vector of vectors containing the weight value of the
 * coarsened nodes
 */
void
MoreCoarse(
    const std::vector<MetisGraph*>& graph,
    const std::vector<std::pair<uint32_t, uint32_t>>& combined_edge_list,
    std::vector<std::vector<WeightTy>>* weight) {
  using VecTy = std::vector<GNode>;

  uint32_t num_partitions = graph.size();
  std::vector<GNodeBag> updated_node_bag(num_partitions);
  uint32_t total_hedge_size = combined_edge_list.size();
  NetvalTy min_netval = std::numeric_limits<NetvalTy>::min();

  katana::do_all(
      katana::iterate(uint32_t{0}, total_hedge_size),
      [&](uint32_t h) {
        auto hedge_index_pair = combined_edge_list[h];
        uint32_t g_index = hedge_index_pair.second;

        if (graph[g_index] == nullptr) {
          return;
        }

        GNode hedge = hedge_index_pair.first;
        HyperGraph* fine_graph = &graph[g_index]->parent_graph->graph;
        MetisNode& hedge_data = fine_graph->getData(hedge);

        if (hedge_data.IsMatched()) {
          return;
        }
        for (auto& fedge : fine_graph->edges(hedge)) {
          GNode dst = fine_graph->getEdgeDst(fedge);
          MetisNode& dst_node_data = fine_graph->getData(dst);
          if (dst_node_data.IsMatched()) {
            dst_node_data.netval = min_netval;
          }
        }
      },
      katana::steal(), katana::chunk_size<kChunkSize>(),
      katana::loopname("Coarsening-Find-MatchedNode-InsideHEdge"));

  katana::do_all(
      katana::iterate(uint32_t{0}, total_hedge_size),
      [&](uint32_t h) {
        auto hedge_index_pair = combined_edge_list[h];
        uint32_t g_index = hedge_index_pair.second;
        WeightTy max_weight = std::numeric_limits<WeightTy>::max();

        if (graph[g_index] == nullptr) {
          return;
        }

        GNode hedge = hedge_index_pair.first;
        HyperGraph* fine_graph = &graph[g_index]->parent_graph->graph;
        MetisNode& hedge_node = fine_graph->getData(hedge);
        WeightTy best_weight{max_weight};
        GNode best_node{0};
        VecTy cells;

        if (hedge_node.IsMatched()) {
          return;
        }

        for (auto& fedge : fine_graph->edges(hedge)) {
          GNode mem_node = fine_graph->getEdgeDst(fedge);
          MetisNode& mem_node_data = fine_graph->getData(mem_node);
          // If dst is already in a match,
          // then compares with the current minimum node.
          if (mem_node_data.IsMatched()) {
            if (mem_node_data.netval == min_netval) {
              WeightTy node_weight = mem_node_data.weight;
              if (node_weight < best_weight) {
                best_weight = node_weight;
                best_node = mem_node;
              } else if (node_weight == best_weight && mem_node < best_node) {
                best_node = mem_node;
              }
            }
            // If dst is not in a match, but has the same netnum,
          } else if (mem_node_data.netnum == hedge_node.netnum) {
            cells.push_back(mem_node);
          }
        }

        if (cells.size() > 0 && best_weight < max_weight) {
          MetisNode& best_node_data = fine_graph->getData(best_node);
          GNode b_parent = best_node_data.parent;
          // Iterate not yet matched nodes.
          for (GNode nym : cells) {
            MetisNode& nym_node = fine_graph->getData(nym);
            nym_node.SetMatched();
            nym_node.parent = b_parent;
            nym_node.netnum = best_node_data.netnum;

            // This node is now in a match.
            // To update weights of the match,
            // this node is appended to the bag.
            updated_node_bag[g_index].push(nym);
          }
        }
      },
      katana::steal(), katana::chunk_size<kChunkSize>(),
      katana::loopname("Coarsening-Update-MatchedNode-Info"));

  katana::do_all(
      katana::iterate(uint32_t{0}, num_partitions),
      [&](uint32_t i) {
        if (graph[i] == nullptr) {
          return;
        }
        HyperGraph* fine_graph = &graph[i]->parent_graph->graph;
        if (fine_graph == nullptr) {
          return;
        }

        for (GNode nym : updated_node_bag[i]) {
          MetisNode& nym_node = fine_graph->getData(nym);
          GNode nym_parent = nym_node.parent;
          weight->at(i)[nym_parent - fine_graph->GetHedges()] +=
              nym_node.weight;
        }
      },
      katana::loopname("Coarsening-Update-MatchedNode-Weights"));
}

/**
 * Identifies more hyperedges that needs to be added to the coarsened graphs,
 * after lone nodes are merged with one of the already coarsened nodes.
 *
 * @param graph Vector containing finer-graphs
 * @param combined_edge_list Concatenated list of hyperedges of the
 * finer-graphs in specified param graph
 * @param hedges Vector of DynamicBitset that represents whether a hyperedge
 * needs to be added to the coarsened graph
 * @param weight Vector of vectors containing the weight value of the
 * coarsened nodes
 */
void
CoarseUnmatchedNodes(
    const std::vector<MetisGraph*>& graph,
    const std::vector<std::pair<uint32_t, uint32_t>>& combined_edge_list,
    std::vector<katana::DynamicBitset>* hedges,
    std::vector<std::vector<WeightTy>>* weight) {
  MoreCoarse(graph, combined_edge_list, weight);

  uint32_t total_hedge_size = combined_edge_list.size();
  katana::InsertBag<std::pair<uint32_t, GNode>> hedge_bag;

  katana::do_all(
      katana::iterate(uint32_t{0}, total_hedge_size),
      [&](GNode h) {
        auto hedge_index_pair = combined_edge_list[h];
        uint32_t g_index = hedge_index_pair.second;

        if (graph[g_index] == nullptr) {
          return;
        }

        GNode hedge = hedge_index_pair.first;
        HyperGraph* fine_graph = &graph[g_index]->parent_graph->graph;
        MetisNode& hedge_data = fine_graph->getData(hedge);

        if (hedge_data.IsMatched()) {
          return;
        }

        GNode exp_parent{0};
        size_t count{0};
        // This loop filters hyperedges which have rooms for more improvements.
        // It considers below two cases.
        // First, check if there are nodes having different parents.
        // Second, check if there is any node which is still not in a match.
        for (auto& fedge : fine_graph->edges(hedge)) {
          GNode mem_node = fine_graph->getEdgeDst(fedge);
          MetisNode& mem_node_data = fine_graph->getData(mem_node);
          if (mem_node_data.IsMatched()) {
            GNode cur_parent = mem_node_data.parent;
            if (count == 0) {
              exp_parent = mem_node_data.parent;
              count++;
            } else if (exp_parent != cur_parent) {
              count++;
              break;
            }
          } else { /* Any node inside of the hyperedge is not in a match. */
            count = 0;
            break;
          }
        }
        if (count != 1) {
          hedge_bag.push(std::make_pair(g_index, hedge));
        }
      },
      katana::steal(), katana::loopname("Coarsening-Count-HEdges"));

  for (auto& pair : hedge_bag) {
    hedges->at(pair.first).set(pair.second);
  }
}

/**
 * Find nodes that are not connected to any hyperedge
 *
 * @param graph Vector containing finer-graphs
 * @param combined_edge_list Concatenated list of hyperedges of the
 * finer-graphs in specified param graph
 * @param combined_node_list Concatenated list of nodes of the
 * finer-graphs in specified param graph
 */
void
FindLoneNodes(
    std::vector<HyperGraph*>* graph,
    const std::vector<std::pair<uint32_t, uint32_t>>& combined_edge_list,
    const std::vector<std::pair<uint32_t, uint32_t>>& combined_node_list) {
  uint32_t total_nodes_size = combined_node_list.size();
  uint32_t total_hedge_size = combined_edge_list.size();

  // All nodes are initialized as 'lone' nodes,
  // which implies that they are not in hyper edges.
  katana::do_all(
      katana::iterate(uint32_t{0}, total_nodes_size),
      [&](uint32_t n_id) {
        auto node_index_pair = combined_node_list[n_id];
        uint32_t index = node_index_pair.second;
        GNode node = node_index_pair.first;
        graph->at(index)->getData(node).UnsetNotAlone();
      },
      katana::loopname("Coarsening-Initialize-LoneNodes"));

  // Now, nodes connected to hyper edges are set as
  // 'not lone' nodes.
  katana::do_all(
      katana::iterate(uint32_t{0}, total_hedge_size),
      [&](uint32_t hedge_id) {
        auto hedge_index_pair = combined_edge_list[hedge_id];
        uint32_t index = hedge_index_pair.second;
        GNode src = hedge_index_pair.first;
        HyperGraph* h_graph = graph->at(index);
        for (auto& e : h_graph->edges(src)) {
          GNode dst = h_graph->getEdgeDst(e);
          h_graph->getData(dst).SetNotAlone();
        }
      },
      katana::steal(), katana::loopname("Coarsening-Initialize-NotLoneEdges"));
}

/**
 * Constructs coarsened graphs
 *
 * @param coarse_metis_graph Vector containing coarse-graphs
 * @param combined_edge_list Concatenated list of hyperedges of the
 * finer-graphs
 * @param combined_node_list Concatenated list of nodes of the
 * finer-graphs
 * @param bag Vector of nodes to be added to the coarsened graphs
 * @param hedges Vector of DynamicBitset that represents whether a hyperedge
 * needs to be added to the coarsened graph
 * @param weight Vector of vectors containing the weight value of the
 * coarsened nodes
 */
void
ParallelCreateEdges(
    const std::vector<MetisGraph*>& coarse_metis_graph,
    const std::vector<std::pair<uint32_t, uint32_t>>& combined_edge_list,
    const std::vector<std::pair<uint32_t, uint32_t>>& combined_node_list,
    std::vector<GNodeBag>* nodes_bag,
    const std::vector<katana::DynamicBitset>& hedges,
    std::vector<std::vector<WeightTy>>* weight) {
  uint32_t num_partitions = coarse_metis_graph.size();
  NetnumTy max_netnum = std::numeric_limits<NetnumTy>::max();
  NetvalTy max_netval = std::numeric_limits<NetvalTy>::max();

  // For conveniences, construct pointer arrays pointing to graphs.
  std::vector<HyperGraph*> fine_graphs(num_partitions, nullptr);
  for (uint32_t i = 0; i < num_partitions; ++i) {
    if (coarse_metis_graph[i] != nullptr) {
      fine_graphs[i] = &coarse_metis_graph[i]->parent_graph->graph;
    }
  }

  std::vector<HyperGraph*> coarse_graphs(num_partitions, nullptr);
  for (uint32_t i = 0; i < num_partitions; ++i) {
    if (coarse_metis_graph[i] != nullptr) {
      coarse_graphs[i] = &coarse_metis_graph[i]->graph;
    }
  }

  // The number of hyperedges which are still in progress.
  std::vector<katana::GAccumulator<uint32_t>> num_wip_hg(num_partitions);

  uint32_t total_hedges = combined_edge_list.size();

  katana::do_all(
      katana::iterate(uint32_t{0}, total_hedges),
      [&](uint32_t h) {
        auto hedge_index_pair = combined_edge_list[h];
        uint32_t h_index = hedge_index_pair.second;
        GNode hedge = hedge_index_pair.first;
        if (hedges[h_index].test(hedge)) {
          num_wip_hg[h_index] += 1;
        }
      },
      katana::loopname("Coarsening-Count-HEdges"));

  // Find lone nodes.
  FindLoneNodes(&fine_graphs, combined_edge_list, combined_node_list);
  std::vector<katana::InsertBag<GNode>> postponded_nodes(num_partitions);
  uint32_t total_nodes_size = combined_node_list.size();

  katana::do_all(
      katana::iterate(uint32_t{0}, total_nodes_size),
      [&](uint32_t n) {
        auto node_index_pair = combined_node_list[n];
        uint32_t n_index = node_index_pair.second;
        GNode node = node_index_pair.first;
        MetisNode& node_data = fine_graphs[n_index]->getData(node);
        // If a node is not connected to a hyper edge,
        // then it can be considered as a coarsened node.
        if (!node_data.IsMatched() && node_data.IsNotAlone()) {
          nodes_bag->at(n_index).emplace(node);
          node_data.SetMatched();
          node_data.parent = node;  ///< self-edge.
          node_data.netnum = max_netnum;
          weight->at(n_index)[node - fine_graphs[n_index]->GetHedges()] =
              node_data.weight;
          // Otherwise, a node should consider neighbors connected
          // by the same hyper edge. Therefore, this is just appened
          // to the postponded_nodes and processed later.
          // (e.g. fine the representative node of the match, etc)
        } else if (!node_data.IsMatched() && !node_data.IsNotAlone()) {
          postponded_nodes[n_index].emplace(node);
        }
      },
      katana::loopname("Coarsening-Count-PostponedNodes"));

  // Process not matched `lone` nodes.
  // Merge lone nodes and create coarsened node.
  // The number of these nodes is less than 1000.
  katana::do_all(
      katana::iterate(uint32_t{0}, num_partitions),
      [&](uint32_t i) {
        if (fine_graphs[i] == nullptr || postponded_nodes[i].empty()) {
          return;
        }

        std::vector<GNode> repr_node_ids(
            kLoneNodesCoarsenFactor, std::numeric_limits<GNode>::max());
        katana::DynamicBitset new_coarsen_node_filter;
        new_coarsen_node_filter.resize(kLoneNodesCoarsenFactor);

        // 1) Find minimum node id from a match.
        for (GNode n : postponded_nodes[i]) {
          uint32_t index = n % kLoneNodesCoarsenFactor;
          new_coarsen_node_filter.set(index);
          if (repr_node_ids[index] > n) {
            repr_node_ids[index] = n;
          }
        }

        // 2) Push the processed nodes to the bag.
        for (uint32_t j = 0; j < kLoneNodesCoarsenFactor; j++) {
          if (new_coarsen_node_filter.test(j)) {
            nodes_bag->at(i).push(repr_node_ids[j]);
          }
        }

        // 3) Update the processed nodes information.
        for (GNode n : postponded_nodes[i]) {
          uint32_t n_index = n % kLoneNodesCoarsenFactor;
          GNode repr_node_id = repr_node_ids[n_index];
          MetisNode& node_data = fine_graphs[i]->getData(n);
          node_data.SetMatched();
          node_data.parent = repr_node_id;
          node_data.netnum = max_netnum;
          weight->at(i)[repr_node_id - fine_graphs[i]->GetHedges()] +=
              node_data.weight;
        }
      },
      katana::loopname("Coarsening-Process-LoneNodes"));

  std::vector<uint32_t> hnum(num_partitions);
  std::vector<uint32_t> nodes(num_partitions);
  std::vector<uint32_t> newval(num_partitions);
  std::vector<std::vector<uint32_t>> idmap(num_partitions);
  std::vector<std::vector<WeightTy>> new_weight(num_partitions);

  for (uint32_t i = 0; i < num_partitions; i++) {
    if (fine_graphs[i] == nullptr) {
      continue;
    }
    uint32_t num_hnodes = fine_graphs[i]->GetHnodes();
    hnum[i] = num_wip_hg[i].reduce();  ///< # of hedges.
    // # of the representative nodes of the
    // coarsened match inside of i-th hedge.
    nodes[i] = std::distance(nodes_bag->at(i).begin(), nodes_bag->at(i).end());
    newval[i] = hnum[i]; /* # of hedges. */
    idmap[i].resize(num_hnodes);
    new_weight[i].resize(nodes[i]);
  }

  katana::do_all(
      katana::iterate(uint32_t{0}, num_partitions),
      [&](uint32_t i) {
        if (fine_graphs[i] == nullptr) {
          return;
        }
        uint32_t num_hedges = fine_graphs[i]->GetHedges();
        uint32_t tot_size = fine_graphs[i]->size();
        katana::DynamicBitset new_coarsen_node_filter;
        new_coarsen_node_filter.resize(tot_size);

        // Set nodes which were newly included in a match.
        for (GNode n : nodes_bag->at(i)) {
          new_coarsen_node_filter.set(n);
        }

        // Update weights.
        for (uint32_t n = num_hedges; n < tot_size; n++) {
          if (new_coarsen_node_filter.test(n)) {
            // ID of the appended coarsened node.
            GNode current_id = newval[i]++;
            idmap[i][n - num_hedges] = current_id;
            // new_weight: sparse array of the coarsened nodes.
            // weight: dense array of the nodes.
            new_weight[i][current_id - hnum[i]] = weight->at(i)[n - num_hedges];
          }
        }
      },
      katana::steal(),
      katana::loopname("Coarsening-Update-MatchedNode-Weights"));

  // Update parents of the coarsened node.
  katana::do_all(
      katana::iterate(uint32_t{0}, total_nodes_size),
      [&](uint32_t n) {
        auto node_index_pair = combined_node_list[n];
        uint32_t g_index = node_index_pair.second;
        GNode node = node_index_pair.first;
        MetisNode& node_data = fine_graphs[g_index]->getData(node);

        GNode par_id = node_data.parent;
        node_data.parent =
            idmap[g_index][par_id - fine_graphs[g_index]->GetHedges()];
      },
      katana::loopname("Coarsening-Update-Parents"));

  std::vector<katana::gstl::Vector<katana::PODVector<uint32_t>>> edges_id(
      num_partitions);
  std::vector<std::vector<NetnumTy>> old_id(num_partitions);
  std::vector<uint32_t> num_nodes_next(num_partitions);

  katana::do_all(
      katana::iterate(uint32_t{0}, num_partitions),
      [&](uint32_t i) {
        if (fine_graphs[i] == nullptr) {
          return;
        }
        uint32_t i_num_hedge = hnum[i];
        uint32_t i_num_fedge = nodes[i];
        uint32_t num_iedges = i_num_fedge + i_num_hedge;
        num_nodes_next[i] = i_num_hedge + i_num_fedge;

        edges_id[i].resize(num_iedges);
        old_id[i].resize(i_num_hedge);

        GNode h_id{0};
        for (uint32_t n = 0; n < fine_graphs[i]->GetHedges(); n++) {
          MetisNode& node_data = fine_graphs[i]->getData(n);
          if (hedges[i].test(n)) {
            // This netnum will be reused in the new graph.
            old_id[i][h_id] = node_data.netnum;
            node_data.node_id = h_id++;
          }
        }
      },
      katana::steal(), katana::loopname("Coarsening-Set-NodeIds"));

  katana::do_all(
      katana::iterate(uint32_t{0}, total_hedges),
      [&](uint32_t v) {
        auto hedge_index_pair = combined_edge_list[v];
        uint32_t index = hedge_index_pair.second;
        GNode n = hedge_index_pair.first;

        if (!hedges[index].test(n)) {
          return;
        }

        HyperGraph* f_graph = fine_graphs[index];

        GNode id = f_graph->getData(n).node_id;

        for (auto& fedge : f_graph->edges(n)) {
          GNode dst = f_graph->getEdgeDst(fedge);
          MetisNode& dst_data = f_graph->getData(dst);
          GNode pid = dst_data.parent;
          auto parent_iter = std::find(
              edges_id[index][id].begin(), edges_id[index][id].end(), pid);
          // If a parent does not exist in the edge id array,
          // then add it.
          if (parent_iter == edges_id[index][id].end()) {
            edges_id[index][id].push_back(pid);
          }
        }
      },
      katana::steal(), katana::chunk_size<kChunkSize>(),
      katana::loopname("Coarsening-Build-EdgeIds"));

  std::vector<katana::NUMAArray<uint64_t>> edges_prefixsum(num_partitions);
  std::vector<katana::GAccumulator<uint64_t>> num_edges_acc(num_partitions);

  for (uint32_t i = 0; i < num_partitions; ++i) {
    if (fine_graphs[i] == nullptr) {
      continue;
    }
    uint32_t num_ith_nodes = num_nodes_next[i];
    edges_prefixsum[i].allocateInterleaved(num_ith_nodes);

    katana::do_all(
        katana::iterate(uint32_t{0}, num_ith_nodes),
        [&](uint32_t c) {
          edges_prefixsum[i][c] = edges_id[i][c].size();
          num_edges_acc[i] += edges_prefixsum[i][c];
        },
        katana::loopname("Coarsening-PrefixSum"));
  }

  for (uint32_t i = 0; i < num_partitions; ++i) {
    if (fine_graphs[i] == nullptr) {
      continue;
    }

    uint32_t num_ith_nodes = num_nodes_next[i];
    uint64_t num_edges_next = num_edges_acc[i].reduce();

    katana::ParallelSTL::partial_sum(
        edges_prefixsum[i].begin(), edges_prefixsum[i].end(),
        edges_prefixsum[i].begin());

    HyperGraph* c_graph = coarse_graphs[i];
    c_graph->constructFrom(
        num_ith_nodes, num_edges_next, std::move(edges_prefixsum[i]),
        edges_id[i]);
    c_graph->SetHedges(hnum[i]);
    c_graph->SetHnodes(nodes[i]);
    katana::do_all(
        katana::iterate(*c_graph),
        [&](GNode n) {
          MetisNode& node_data = c_graph->getData(n);
          node_data.netval = max_netval;
          if (n < hnum[i]) {
            node_data.netnum = old_id[i][n];
          } else {
            node_data.netnum = max_netnum;
            node_data.netrand = max_netval;
            node_data.node_id = n;
            node_data.weight = new_weight[i][n - c_graph->GetHedges()];
          }
        },
        katana::loopname("Coarsening-Construct-Graph"));
  }
}

/**
 * This function first finds a multi-node matching, and then
 * construct coarsened graphs based on this matching
 *
 * @param coarse_mgraph Vector containing coarse-graphs
 * @param fine_mgraph Vector containing finer-graphs
 * @param combined_edge_list Concatenated list of hyperedges of the
 * finer-graphs
 * @param combined_node_list Concatenated list of nodes of the
 * finer-graphs
 * @param matching_policy matching policy to be used
 */
void
FindMatching(
    const std::vector<MetisGraph*>& coarse_mgraph,
    const std::vector<MetisGraph*>& fine_mgraph,
    const std::vector<std::pair<uint32_t, uint32_t>>& combined_edge_list,
    const std::vector<std::pair<uint32_t, uint32_t>>& combined_node_list,
    const MatchingPolicy matching_policy) {
  KATANA_LOG_DEBUG_ASSERT(coarse_mgraph.size() == fine_mgraph.size());
  uint32_t num_fine_hedges = fine_mgraph.size();
  std::vector<GNodeBag> nodes(num_fine_hedges);
  std::vector<katana::DynamicBitset> hedges(num_fine_hedges);
  // Maintain total weights of nodes inside of a match.
  std::vector<std::vector<WeightTy>> weight(num_fine_hedges);

  for (uint32_t i = 0; i < num_fine_hedges; ++i) {
    if (coarse_mgraph[i] == nullptr) {
      continue;
    }

    HyperGraph* f_graph = &fine_mgraph[i]->graph;
    uint32_t num_valid_hedges = f_graph->GetHedges();
    uint32_t num_valid_nodes = f_graph->GetHnodes();
    hedges[i].resize(num_valid_hedges);
    weight[i].resize(num_valid_nodes);
  }

  switch (matching_policy) {
  case HigherDegree:
    ParallelHMatchAndCreateNodes<PrioritizeHigherDegree>(
        coarse_mgraph, combined_edge_list, &nodes, &hedges, &weight);
    break;
  case Random:
    ParallelHMatchAndCreateNodes<PrioritizeRandom>(
        coarse_mgraph, combined_edge_list, &nodes, &hedges, &weight);
    break;
  case LowerDegree:
    ParallelHMatchAndCreateNodes<PrioritizeLowerDegree>(
        coarse_mgraph, combined_edge_list, &nodes, &hedges, &weight);
    break;
  case HigherWeight:
    ParallelHMatchAndCreateNodes<PrioritizeHigherWeight>(
        coarse_mgraph, combined_edge_list, &nodes, &hedges, &weight);
    break;
  case LowerWeight:
    ParallelHMatchAndCreateNodes<PrioritizeDegree>(
        coarse_mgraph, combined_edge_list, &nodes, &hedges, &weight);
    break;
  default:
    abort();
  }

  CoarseUnmatchedNodes(coarse_mgraph, combined_edge_list, &hedges, &weight);
  ParallelCreateEdges(
      coarse_mgraph, combined_edge_list, combined_node_list, &nodes, hedges,
      &weight);
}

/**
 * Creates coarsened graphs
 *
 * @param next_coarse_graph Vector containing coarse-graphs
 * @param fine_metis_graph Vector containing finer-graphs
 * @param combined_edge_list Concatenated list of hyperedges of the
 * finer-graphs
 * @param combined_node_list Concatenated list of nodes of the
 * finer-graphs
 * @param matching_policy matching policy to be used
 */
void
CoarsenOnce(
    std::vector<MetisGraph*>* next_coarse_graph,
    const std::vector<MetisGraph*>& fine_metis_graph,
    const std::vector<std::pair<uint32_t, uint32_t>>& combined_edge_list,
    const std::vector<std::pair<uint32_t, uint32_t>>& combined_node_list,
    const MatchingPolicy matching_policy) {
  uint32_t num_partitions = fine_metis_graph.size();
  KATANA_LOG_DEBUG_ASSERT(next_coarse_graph->size() == num_partitions);
  for (uint32_t i = 0; i < num_partitions; ++i) {
    MetisGraph* graph = fine_metis_graph[i];
    if (graph != nullptr) {
      // A fine metis graph points to next coarse graph
      // as the coarser graph (e.g. parent node).
      next_coarse_graph->at(i) = new MetisGraph(graph);
    }
  }

  FindMatching(
      *next_coarse_graph, fine_metis_graph, combined_edge_list,
      combined_node_list, matching_policy);
}

/**
 * Main Coarsening Function
 *
 * @param metis_graphs Vector containing original finer-graphs
 * @param max_coarsen_level maximum number of coarsening levels allowed
 * @param matching_policy matching policy to be used
 */
void
Coarsen(
    std::vector<MetisGraph*>* metis_graphs, const uint32_t max_coarsen_level,
    const MatchingPolicy matching_policy) {
  uint32_t num_partitions = metis_graphs->size();
  std::vector<uint32_t> current_num_nodes(num_partitions);
  std::vector<uint32_t> new_num_nodes(num_partitions);
  std::vector<MetisGraph*> final_graph(num_partitions, nullptr);
  katana::DynamicBitset graph_is_done;

  graph_is_done.resize(num_partitions);
  graph_is_done.reset();

  for (uint32_t i = 0; i < num_partitions; ++i) {
    if (metis_graphs->at(i) == nullptr) {
      continue;
    }
    new_num_nodes[i] = current_num_nodes[i] =
        metis_graphs->at(i)->graph.GetHnodes();
  }

  std::vector<uint32_t> num_hedges(num_partitions);

  const float ratio = 52.5 / 47.5;
  const float tol = ratio - 1;

  for (uint32_t i = 0; i < num_partitions; ++i) {
    if (metis_graphs->at(i) == nullptr) {
      continue;
    }
    const WeightTy hi = (1 + tol) * current_num_nodes[i] / (2 + tol);
    kLimitWeights[i] = hi / 4;
  }

  uint32_t iter_num{0};

  while (true) {
    if (iter_num > max_coarsen_level) {
      break;
    }

    if (iter_num > 2) {
      for (uint32_t i = 0; i < num_partitions; ++i) {
        MetisGraph** graph = &metis_graphs->at(i);
        if ((*graph != nullptr) && !graph_is_done.test(i) &&
            (current_num_nodes[i] - new_num_nodes[i] <= 0)) {
          graph_is_done.set(i);
          final_graph[i] = *graph;
          *graph = nullptr;
        }
      }
    }

    bool all_is_done{true};

    // If coarseGraph still exists and graph_is_done is still false.
    for (uint32_t i = 0; i < num_partitions; ++i) {
      if ((metis_graphs->at(i) != nullptr) && !graph_is_done.test(i)) {
        all_is_done = false;
        break;
      }
    }

    //! If no coarse Graph and graph_is_done all is set to true.
    if (all_is_done) {
      break;
    }

    for (uint32_t i = 0; i < num_partitions; ++i) {
      MetisGraph* graph = metis_graphs->at(i);
      if ((graph != nullptr) && !graph_is_done.test(i)) {
        new_num_nodes[i] = graph->graph.GetHnodes();
      }
    }

    std::vector<MetisGraph*> next_coarse_graph(num_partitions, nullptr);

    uint32_t total_nodes{0};
    uint32_t total_edges{0};

    for (uint32_t i = 0; i < num_partitions; ++i) {
      MetisGraph* graph = metis_graphs->at(i);
      if ((graph != nullptr) && !graph_is_done.test(i)) {
        total_nodes += graph->graph.GetHnodes();
        total_edges += graph->graph.GetHedges();
      }
    }

    std::vector<std::pair<uint32_t, uint32_t>> combined_edgelist(total_edges);
    std::vector<std::pair<uint32_t, uint32_t>> combined_nodelist(total_nodes);

    ConstructCombinedLists(
        *metis_graphs, &combined_edgelist, &combined_nodelist);

    CoarsenOnce(
        &next_coarse_graph, *metis_graphs, combined_edgelist, combined_nodelist,
        matching_policy);

    for (uint32_t i = 0; i < num_partitions; ++i) {
      if (!graph_is_done.test(i)) {
        metis_graphs->at(i) = next_coarse_graph[i];
        current_num_nodes[i] = metis_graphs->at(i)->graph.GetHnodes();
        num_hedges[i] = metis_graphs->at(i)->graph.GetHedges();
        //! If the number of hyper edge is less than 1000,
        //! then the graph is already very small,
        //! so no need to coarsen more.
        if (num_hedges[i] < kCoarsestSizeLimit ||
            current_num_nodes[i] < kCoarsestNodeLimit) {
          graph_is_done.set(i);
          metis_graphs->at(i) = nullptr;
        }
      }

      // Overwrite the newly constructed graph.
      if (next_coarse_graph[i] != nullptr) {
        final_graph[i] = next_coarse_graph[i];
      }
    }

    ++iter_num;
  }

  // Copy new to coarse.
  for (size_t i = 0; i < num_partitions; ++i) {
    metis_graphs->at(i) = final_graph[i];
  }
}
// #endif
