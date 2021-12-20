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

#ifndef KATANA_LIBGRAPH_KATANA_ANALYTICS_CLUSTERINGIMPLEMENTATIONBASE_H_
#define KATANA_LIBGRAPH_KATANA_ANALYTICS_CLUSTERINGIMPLEMENTATIONBASE_H_

#include <fstream>
#include <iostream>
#include <random>
#include <set>
#include <vector>

#include "katana/AtomicHelpers.h"
#include "katana/Galois.h"
#include "katana/NUMAArray.h"
#include "katana/analytics/Utils.h"

namespace katana::analytics {

// Maintain community information
template <typename EdgeWeightType>
struct CommunityType {
  std::atomic<uint64_t> size;
  std::atomic<EdgeWeightType> degree_wt;
  EdgeWeightType internal_edge_wt;
};

template <typename EdgeWeightType>
struct LeidenCommunityType {
  std::atomic<uint64_t> size;
  std::atomic<EdgeWeightType> degree_wt;
  std::atomic<uint64_t> node_wt;
  EdgeWeightType internal_edge_wt;
  uint64_t num_internal_edges;
  uint64_t num_sub_communities;
};

struct PreviousCommunityID : public katana::PODProperty<uint64_t> {};
struct CurrentCommunityID : public katana::PODProperty<uint64_t> {};

template <typename EdgeWeightType>
using DegreeWeight = katana::PODProperty<EdgeWeightType>;

template <typename EdgeWeightType>
using EdgeWeight = katana::PODProperty<EdgeWeightType>;

/* Leiden specific properties */
struct CurrentSubCommunityID : public katana::PODProperty<uint64_t> {};
struct NodeWeight : public katana::PODProperty<uint64_t> {};

template <typename _Graph, typename _EdgeType, typename _CommunityType>
struct ClusteringImplementationBase {
  using Graph = _Graph;
  using GNode = typename Graph::Node;
  using EdgeTy = _EdgeType;
  using CommunityType = _CommunityType;

  constexpr static const GNode UNASSIGNED = std::numeric_limits<GNode>::max();
  constexpr static const double INFINITY_DOUBLE =
      std::numeric_limits<double>::max() / 4;

  using CommunityArray = katana::NUMAArray<CommunityType>;

  /**
   * Algorithm to find the best cluster for the node
   * to move to among its neighbors in the graph and moves.
   *
   * It updates the mapping of neighboring nodes clusters
   * in cluster_local_map, total unique cluster edge weights
   * in counter as well as total weight of self edges in self_loop_wt.
   */
  template <typename EdgeWeightType>
  static void FindNeighboringClusters(
      const Graph& graph, const GNode& n,
      std::map<uint64_t, uint64_t>& cluster_local_map,
      std::vector<EdgeTy>& counter, EdgeTy& self_loop_wt) {
    uint64_t num_unique_clusters = 0;

    // Add the node's current cluster to be considered
    // for movement as well
    cluster_local_map[graph.template GetData<CurrentCommunityID>(n)] =
        0;                 // Add n's current cluster
    counter.push_back(0);  // Initialize the counter to zero (no edges incident
                           // yet)
    num_unique_clusters++;

    // Assuming we have grabbed lock on all the neighbors
    for (auto e : graph.edges(n)) {
      auto dst = graph.edge_dest(e);
      // Self loop weights is recorded
      auto edge_wt = graph.template GetEdgeData<EdgeWeight<EdgeWeightType>>(e);
      if (dst == n) {
        self_loop_wt += edge_wt;  // Self loop weights is recorded
      }
      auto stored_already =
          cluster_local_map.find(graph.template GetData<CurrentCommunityID>(
              dst));  // Check if it already exists
      if (stored_already != cluster_local_map.end()) {
        counter[stored_already->second] += edge_wt;
      } else {
        cluster_local_map[graph.template GetData<CurrentCommunityID>(dst)] =
            num_unique_clusters;
        counter.push_back(edge_wt);
        num_unique_clusters++;
      }
    }  // End edge loop
  }

  /**
   * Enables the filtering optimization to remove the
   * node with out-degree 0 (isolated) and 1 before the clustering
   * algorithm begins.
   */
  static uint64_t VertexFollowing(Graph* graph) {
    // Initialize each node to its own cluster
    katana::do_all(katana::iterate(*graph), [&](GNode n) {
      graph->template GetData<CurrentCommunityID>(n) = n;
    });

    // Remove isolated and degree-one nodes
    katana::GAccumulator<uint64_t> isolated_nodes;
    katana::do_all(katana::iterate(*graph), [&](GNode n) {
      auto& n_data_curr_comm_id =
          graph->template GetData<CurrentCommunityID>(n);
      uint64_t degree = graph->degree(n);
      if (degree == 0) {
        isolated_nodes += 1;
        n_data_curr_comm_id = UNASSIGNED;
      } else {
        if (degree == 1) {
          // Check if the destination has degree greater than one
          auto dst = graph->edge_dest(*graph->edges(n).begin());
          uint64_t dst_degree = graph->degree(dst);
          if ((dst_degree > 1 || (n > dst))) {
            isolated_nodes += 1;
            n_data_curr_comm_id =
                graph->template GetData<CurrentCommunityID>(dst);
          }
        }
      }
    });
    // The number of isolated nodes that can be removed
    return isolated_nodes.reduce();
  }

  /**
   * Sums up the degree weight for all
   * the unique clusters.
   */
  template <typename EdgeWeightType>
  static void SumVertexDegreeWeight(Graph* graph, CommunityArray& c_info) {
    katana::do_all(katana::iterate(*graph), [&](GNode n) {
      EdgeTy total_weight = 0;
      auto& n_degree_wt =
          graph->template GetData<DegreeWeight<EdgeWeightType>>(n);
      for (auto e : graph->edges(n)) {
        total_weight +=
            graph->template GetEdgeData<EdgeWeight<EdgeWeightType>>(e);
      }
      n_degree_wt = total_weight;
      c_info[n].degree_wt = total_weight;
      c_info[n].size = 1;
    });
  }

  /**
   * Sums up the internal degree weight for all
   * the unique clusters.
   * This is required for finding subcommunities.
   */
  template <typename EdgeWeightType>
  static void SumVertexDegreeWeightCommunity(Graph* graph) {
    katana::do_all(katana::iterate(*graph), [&](GNode n) {
      EdgeTy total_weight = 0;
      auto& n_degree_wt =
          graph->template GetData<DegreeWeight<EdgeWeightType>>(n);
      auto comm_id = graph->template GetData<CurrentCommunityID>(n);

      for (auto e : graph->edges(n)) {
        auto dst = graph->edge_dest(e);

        if (graph->template GetData<CurrentCommunityID>(dst) != comm_id) {
          continue;
        }
        total_weight +=
            graph->template GetEdgeData<EdgeWeight<EdgeWeightType>>(e);
      }

      n_degree_wt = total_weight;
    });
  }

  /**
   * Computes the constant term 1/(2 * total internal edge weight)
   * of the current coarsened graph.
   */
  template <typename EdgeWeightType>
  static double CalConstantForSecondTerm(const Graph& graph) {
    //Using double to avoid overflow
    katana::GAccumulator<double> local_weight;
    katana::do_all(katana::iterate(graph), [&graph, &local_weight](GNode n) {
      local_weight += graph.template GetData<DegreeWeight<EdgeWeightType>>(n);
    });
    //This is twice since graph is symmetric
    double total_edge_weight_twice = local_weight.reduce();
    return 1 / total_edge_weight_twice;
  }

  /**
   * Computes the constant term 1/(2 * total internal edge weight)
   * of the current coarsened graph. Takes the optional NUMAArray
   * with edge weight. To be used if edge weight is missing in the
   * property graph.
   */
  template <typename EdgeWeightType>
  static double CalConstantForSecondTerm(
      const Graph& graph,
      katana::NUMAArray<EdgeWeightType>& degree_weight_array) {
    // Using double to avoid overflow
    katana::GAccumulator<double> local_weight;
    katana::do_all(katana::iterate(graph), [&](GNode n) {
      local_weight += degree_weight_array[n];
    });
    // This is twice since graph is symmetric
    double total_edge_weight_twice = local_weight.reduce();
    return 1 / total_edge_weight_twice;
  }

  /**
   * Computes the constant term 1/(2 * total internal edge weight)
   * for subgraphs corresponding to each individual community.
   * This is required for finding subcommunities.
   */
  template <typename EdgeWeightType>
  static void CalConstantForSecondTerm(
      const Graph& graph,
      katana::NUMAArray<std::atomic<double>>* comm_constant_term_array) {
    katana::do_all(katana::iterate(graph), [&](GNode n) {
      (*comm_constant_term_array)[n] = 0.0;
    });

    katana::do_all(katana::iterate(graph), [&](GNode n) {
      auto comm_id = graph.template GetData<CurrentCommunityID>(n);
      katana::atomicAdd(
          (*comm_constant_term_array)[comm_id],
          (double)graph.template GetData<DegreeWeight<EdgeWeightType>>(n));
    });

    katana::do_all(katana::iterate(graph), [&](GNode n) {
      if ((*comm_constant_term_array)[n] != 0) {
        (*comm_constant_term_array)[n] = 1.0 / (*comm_constant_term_array)[n];
      }
    });
  }

  /**
   * Computes the modularity gain of the current cluster assignment
   * without swapping the cluster assignment.
   */
  static uint64_t MaxModularityWithoutSwaps(
      std::map<uint64_t, uint64_t>& cluster_local_map,
      std::vector<EdgeTy>& counter, uint64_t self_loop_wt,
      CommunityArray& c_info, EdgeTy degree_wt, uint64_t sc, double constant) {
    uint64_t max_index = sc;  // Assign the intial value as self community
    double cur_gain = 0;
    double max_gain = 0;
    double eix = counter[0] - self_loop_wt;
    double ax = c_info[sc].degree_wt - degree_wt;
    double eiy = 0;
    double ay = 0;

    auto stored_already = cluster_local_map.begin();
    do {
      if (sc != stored_already->first) {
        ay = c_info[stored_already->first].degree_wt;  // Degree wt of cluster y

        if (ay < (ax + degree_wt)) {
          stored_already++;
          continue;
        } else if (ay == (ax + degree_wt) && stored_already->first > sc) {
          stored_already++;
          continue;
        }

        eiy = counter[stored_already
                          ->second];  // Total edges incident on cluster y
        cur_gain = 2 * constant * (eiy - eix) +
                   2 * degree_wt * ((ax - ay) * constant * constant);

        if ((cur_gain > max_gain) ||
            ((cur_gain == max_gain) && (cur_gain != 0) &&
             (stored_already->first < max_index))) {
          max_gain = cur_gain;
          max_index = stored_already->first;
        }
      }
      stored_already++;  // Explore next cluster
    } while (stored_already != cluster_local_map.end());

    if ((c_info[max_index].size == 1 && c_info[sc].size == 1 &&
         max_index > sc)) {
      max_index = sc;
    }

    KATANA_LOG_DEBUG_ASSERT(max_gain >= 0);
    return max_index;
  }

  template <
      typename EdgeWeightType, typename CommunityIDType,
      typename NodeWeightFunc>
  static double ModularityImpl(
      const Graph& graph, const NodeWeightFunc& node_wt_func, double& e_xx,
      double& a2_x, const double constant_for_second_term) {
    katana::NUMAArray<EdgeTy> cluster_wt_internal;
    cluster_wt_internal.allocateBlocked(graph.num_nodes());
    katana::ParallelSTL::fill(
        cluster_wt_internal.begin(), cluster_wt_internal.end(), 0);

    /* Calculate the overall modularity */
    katana::GAccumulator<double> acc_e_xx;
    katana::GAccumulator<double> acc_a2_x;

    katana::do_all(katana::iterate(graph), [&](GNode n) {
      auto n_data_current_comm = graph.template GetData<CommunityIDType>(n);
      for (auto e : graph.edges(n)) {
        if (graph.template GetData<CommunityIDType>(graph.edge_dest(e)) ==
            n_data_current_comm) {
          cluster_wt_internal[n] +=
              graph.template GetEdgeData<EdgeWeight<EdgeWeightType>>(e);
        }
      }
    });

    katana::do_all(katana::iterate(graph), [&](GNode n) {
      acc_e_xx += cluster_wt_internal[n];
      double degree_wt = node_wt_func(n);
      acc_a2_x += degree_wt * degree_wt * constant_for_second_term;
    });

    e_xx = acc_e_xx.reduce();
    a2_x = acc_a2_x.reduce();

    return (e_xx - a2_x) * constant_for_second_term;
  }

  /**
   * Computes the modularity gain of the current cluster assignment.
   */
  template <
      typename EdgeWeightType, typename CommunityIDType = CurrentCommunityID>
  static double CalModularity(
      const Graph& graph, CommunityArray& c_info, double& e_xx, double& a2_x,
      const double constant_for_second_term) {
    auto node_wt_func = [&](GNode n) {
      return static_cast<double>(c_info[n].degree_wt);
    };
    return ModularityImpl<EdgeWeightType, CommunityIDType>(
        graph, node_wt_func, e_xx, a2_x, constant_for_second_term);
  }

  template <typename EdgeWeightType, typename NodePropType>
  static void SumClusterWeight(
      Graph& graph, CommunityArray& c_info,
      katana::NUMAArray<EdgeWeightType>& degree_weight_array) {
    using GNode = typename Graph::Node;

    katana::do_all(katana::iterate(graph), [&](GNode n) {
      EdgeTy total_weight = 0;
      for (auto e : graph.edges(n)) {
        total_weight +=
            graph.template GetEdgeData<EdgeWeight<EdgeWeightType>>(e);
      }
      degree_weight_array[n] = total_weight;
      c_info[n].degree_wt = 0;
    });

    katana::do_all(katana::iterate(graph), [&](GNode n) {
      auto& n_data_comm_id = graph.template GetData<NodePropType>(n);
      if (n_data_comm_id != UNASSIGNED)
        katana::atomicAdd(
            c_info[n_data_comm_id].degree_wt, degree_weight_array[n]);
    });
  }

  /**
 * Computes the final modularity using prev cluster
 * assignments.
 */
  template <typename EdgeWeightType, typename CommunityIDType>
  static double CalModularityFinal(Graph& graph) {
    CommunityArray c_info;  // Community info

    /*** Initialization ***/
    c_info.allocateBlocked(graph.num_nodes());

    katana::NUMAArray<EdgeWeightType> degree_weight_array;
    degree_weight_array.allocateBlocked(graph.num_nodes());

    /* Calculate the weighted degree sum for each vertex */
    SumClusterWeight<EdgeWeightType, CommunityIDType>(
        graph, c_info, degree_weight_array);

    /* Compute the total weight (2m) and 1/2m terms */
    const double constant_for_second_term =
        CalConstantForSecondTerm<EdgeWeightType>(graph, degree_weight_array);

    double e_xx = 0.0;
    double a2_x = 0.0;

    return CalModularity<EdgeWeightType, CommunityIDType>(
        graph, c_info, e_xx, a2_x, constant_for_second_term);
  }

  /**
 * Renumbers the cluster to contiguous cluster ids
 * to fill the holes in the cluster id assignments.
 */
  template <typename CommunityIDType>
  static uint64_t RenumberClustersContiguously(Graph* graph) {
    std::map<uint64_t, uint64_t> cluster_local_map;
    uint64_t num_unique_clusters = 0;

    // TODO(amber): parallelize
    for (GNode n : graph->all_nodes()) {
      auto& n_data_curr_comm_id = graph->template GetData<CommunityIDType>(n);
      if (n_data_curr_comm_id != UNASSIGNED) {
        auto stored_already = cluster_local_map.find(n_data_curr_comm_id);
        if (stored_already == cluster_local_map.end()) {
          cluster_local_map[n_data_curr_comm_id] = num_unique_clusters;
          num_unique_clusters++;
        }
      }
    }

    uint64_t new_comm_id = 0;
    for (std::pair<uint64_t, uint64_t> old_comm_id : cluster_local_map) {
      cluster_local_map[old_comm_id.first] = new_comm_id;
      new_comm_id++;
    }

    katana::do_all(katana::iterate(*graph), [&](GNode n) {
      auto& n_data_curr_comm_id = graph->template GetData<CommunityIDType>(n);
      if (n_data_curr_comm_id != UNASSIGNED) {
        n_data_curr_comm_id = cluster_local_map[n_data_curr_comm_id];
      }
    });

    return num_unique_clusters;
  }

  template <typename EdgeWeightType>
  static void CheckModularity(
      Graph& graph, katana::NUMAArray<uint64_t>& clusters_orig) {
    katana::do_all(katana::iterate(graph), [&](GNode n) {
      graph.template GetData<CurrentCommunityID>(n).curr_comm_ass =
          clusters_orig[n];
    });

    [[maybe_unused]] uint64_t num_unique_clusters =
        RenumberClustersContiguously(graph);
    auto mod = CalModularityFinal<EdgeWeightType, CurrentCommunityID>(graph);
  }

  /**
 * Creates a duplicate of the graph by copying the
 * graph (pfg_from) topology
 * (read from the underlying RDG) to the in-memory
 * temporary graph (pfg_to).
 * TODO(gill) replace with ephemeral graph
 */
  static katana::Result<std::unique_ptr<katana::PropertyGraph>>
  DuplicateGraphWithSameTopo(const katana::PropertyGraph& pfg_from) {
    const katana::GraphTopology& topology_from = pfg_from.topology();

    katana::GraphTopology topo_copy = GraphTopology::Copy(topology_from);

    auto pfg_to_res = katana::PropertyGraph::Make(std::move(topo_copy));
    if (!pfg_to_res) {
      return pfg_to_res.error();
    }
    return std::unique_ptr<katana::PropertyGraph>(
        std::move(pfg_to_res.value()));
  }

  /**
 * Copy edge property from
 * property graph, pg_from to pg_to.
 */
  static katana::Result<void> CopyEdgeProperty(
      katana::PropertyGraph* pfg_from, katana::PropertyGraph* pfg_to,
      const std::string& edge_property_name,
      const std::string& new_edge_property_name, tsuba::TxnContext* txn_ctx) {
    // Remove the existing edge property
    if (pfg_to->HasEdgeProperty(new_edge_property_name)) {
      if (auto r = pfg_to->RemoveEdgeProperty(new_edge_property_name, txn_ctx);
          !r) {
        return r.error();
      }
    }
    // Copy edge properties
    using ArrowType = typename arrow::CTypeTraits<EdgeTy>::ArrowType;
    auto edge_property_result =
        pfg_from->GetEdgePropertyTyped<EdgeTy>(edge_property_name);
    if (!edge_property_result) {
      return edge_property_result.error();
    }
    auto edge_property = edge_property_result.value();
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<arrow::Array>> columns;
    fields.emplace_back(
        arrow::field((new_edge_property_name), std::make_shared<ArrowType>()));
    columns.emplace_back(edge_property);
    auto edge_data_table = arrow::Table::Make(arrow::schema(fields), columns);
    if (auto r = pfg_to->AddEdgeProperties(edge_data_table, txn_ctx); !r) {
      return r.error();
    }
    return katana::ResultSuccess();
  }

  /**
 * Creates a coarsened hierarchical graph for the next phase
 * of the clustering algorithm. It merges all the nodes within a
 * same cluster to form a super node for the coarsened graphs.
 * The total number of nodes in the coarsened graph are equal to
 * the number of unique clusters in the previous level of the graph.
 * All the edges inside a cluster are merged (edge weights are summed
 * up) to form the edges within super nodes.
 */
  template <
      typename NodeData, typename EdgeData, typename EdgeWeightType,
      typename CommunityIDType>
  static katana::Result<std::unique_ptr<katana::PropertyGraph>> GraphCoarsening(
      const Graph& graph, katana::PropertyGraph* pfg_mutable,
      uint64_t num_unique_clusters,
      const std::vector<std::string>& temp_node_property_names,
      const std::vector<std::string>& temp_edge_property_names,
      tsuba::TxnContext* txn_ctx) {
    using GNode = typename Graph::Node;

    katana::StatTimer TimerGraphBuild("Timer_Graph_build");
    TimerGraphBuild.start();

    const uint64_t num_nodes_next = num_unique_clusters;

    std::vector<std::vector<GNode>> cluster_bags(num_unique_clusters);
    // TODO(amber): This loop can be parallelized when using a concurrent container
    // for cluster_bags, but something like katana::InsertBag exhausts the
    // per-thread-storage memory
    for (GNode n = 0; n < graph.num_nodes(); ++n) {
      auto n_data_curr_comm_id = graph.template GetData<CommunityIDType>(n);
      if (n_data_curr_comm_id != UNASSIGNED) {
        cluster_bags[n_data_curr_comm_id].push_back(n);
      }
    }

    std::vector<katana::gstl::Vector<uint32_t>> edges_id(num_unique_clusters);
    std::vector<katana::gstl::Vector<EdgeTy>> edges_data(num_unique_clusters);

    /* First pass to find the number of edges */
    katana::do_all(
        katana::iterate(uint64_t{0}, num_unique_clusters),
        [&](uint64_t c) {
          katana::gstl::Map<uint64_t, uint64_t> cluster_local_map;
          uint64_t num_unique_clusters = 0;
          for (auto node : cluster_bags[c]) {
            KATANA_LOG_DEBUG_ASSERT(
                graph.template GetData<CommunityIDType>(node) ==
                c);  // All nodes in this bag must have same cluster id

            for (auto e : graph.edges(node)) {
              auto dst = graph.edge_dest(e);
              auto dst_data_curr_comm_id =
                  graph.template GetData<CommunityIDType>(dst);
              KATANA_LOG_DEBUG_ASSERT(dst_data_curr_comm_id != UNASSIGNED);
              auto stored_already = cluster_local_map.find(
                  dst_data_curr_comm_id);  // Check if it already exists
              if (stored_already != cluster_local_map.end()) {
                edges_data[c][stored_already->second] +=
                    graph.template GetEdgeData<EdgeWeight<EdgeWeightType>>(e);
              } else {
                cluster_local_map[dst_data_curr_comm_id] = num_unique_clusters;
                edges_id[c].push_back(dst_data_curr_comm_id);
                edges_data[c].push_back(
                    graph.template GetEdgeData<EdgeWeight<EdgeWeightType>>(e));
                num_unique_clusters++;
              }
            }  // End edge loop
          }
        },
        katana::steal(), katana::loopname("BuildGraph: Find edges"));

    /* Serial loop to reduce all the edge counts */
    katana::NUMAArray<uint64_t> prefix_edges_count;
    prefix_edges_count.allocateInterleaved(num_unique_clusters);

    katana::GAccumulator<uint64_t> num_edges_acc;
    katana::do_all(
        katana::iterate(uint64_t{0}, num_nodes_next), [&](uint64_t c) {
          prefix_edges_count[c] = edges_id[c].size();
          num_edges_acc += prefix_edges_count[c];
        });

    const uint64_t num_edges_next = num_edges_acc.reduce();

    katana::ParallelSTL::partial_sum(
        prefix_edges_count.begin(), prefix_edges_count.end(),
        prefix_edges_count.begin());

    KATANA_LOG_DEBUG_ASSERT(
        prefix_edges_count[num_unique_clusters - 1] == num_edges_next);
    katana::StatTimer TimerConstructFrom("Timer_Construct_From");
    TimerConstructFrom.start();

    // Remove all the existing node/edge properties
    for (auto property : temp_node_property_names) {
      if (pfg_mutable->HasNodeProperty(property)) {
        if (auto r = pfg_mutable->RemoveNodeProperty(property, txn_ctx); !r) {
          return r.error();
        }
      }
    }
    for (auto property : temp_edge_property_names) {
      if (pfg_mutable->HasEdgeProperty(property)) {
        if (auto r = pfg_mutable->RemoveEdgeProperty(property, txn_ctx); !r) {
          return r.error();
        }
      }
    }

    using Node = katana::GraphTopology::Node;
    using Edge = katana::GraphTopology::Edge;

    katana::NUMAArray<Node> out_dests_next;
    out_dests_next.allocateInterleaved(num_edges_next);

    katana::NUMAArray<EdgeWeightType> edge_data_next;
    edge_data_next.allocateInterleaved(num_edges_next);

    katana::do_all(
        katana::iterate(uint64_t{0}, num_nodes_next), [&](uint64_t n) {
          uint64_t number_of_edges =
              (n == 0) ? prefix_edges_count[0]
                       : (prefix_edges_count[n] - prefix_edges_count[n - 1]);
          uint64_t start_index = (n == 0) ? 0 : prefix_edges_count[n - 1];
          for (uint64_t k = 0; k < number_of_edges; ++k) {
            out_dests_next[start_index + k] = edges_id[n][k];
            edge_data_next[start_index + k] = edges_data[n][k];
          }
        });

    TimerConstructFrom.stop();

    // TODO(amber): This is a lame attempt at freeing the memory back to each
    // thread's pool of free pages and blocks. Due to stealing, the execution of
    // do_all above that populates these containers may be different from the
    // do_all below that frees them.
    katana::do_all(
        katana::iterate(uint64_t{0}, num_unique_clusters), [&](uint64_t c) {
          edges_id[c] = gstl::Vector<uint32_t>();
          edges_data[c] = gstl::Vector<EdgeTy>();
        });

    GraphTopology topo_next{
        std::move(prefix_edges_count), std::move(out_dests_next)};
    auto pfg_next_res = katana::PropertyGraph::Make(std::move(topo_next));

    if (!pfg_next_res) {
      return pfg_next_res.error();
    }
    std::unique_ptr<katana::PropertyGraph> pfg_next =
        std::move(pfg_next_res.value());

    if (auto result = katana::analytics::ConstructNodeProperties<NodeData>(
            pfg_next.get(), txn_ctx, temp_node_property_names);
        !result) {
      return result.error();
    }

    if (auto result = katana::analytics::ConstructEdgeProperties<EdgeData>(
            pfg_next.get(), txn_ctx, temp_edge_property_names);
        !result) {
      return result.error();
    }

    auto graph_result = Graph::Make(pfg_next.get());
    if (!graph_result) {
      return graph_result.error();
    }
    Graph graph_next = graph_result.value();
    // TODO(amber): figure out a better way to add/update the edge property
    katana::do_all(
        katana::iterate(graph_next.all_edges()),
        [&](Edge e) {
          graph_next.template GetEdgeData<EdgeWeight<EdgeWeightType>>(e) =
              edge_data_next[e];
        },
        katana::no_stats());

    TimerGraphBuild.stop();
    return std::unique_ptr<katana::PropertyGraph>(std::move(pfg_next));
  }

  /**
 * Functions specific to Leiden clustering
 */
  /**
   * Sums up the degree weight for all
   * the unique clusters.
   */
  template <typename EdgeWeightType>
  static void SumVertexDegreeWeightWithNodeWeight(Graph* graph) {
    katana::do_all(katana::iterate(*graph), [&](GNode n) {
      EdgeTy total_weight = 0;
      auto& n_degree_wt =
          graph->template GetData<DegreeWeight<EdgeWeightType>>(n);

      for (auto e : graph->edges(n)) {
        total_weight +=
            graph->template GetEdgeData<EdgeWeight<EdgeWeightType>>(e);
      }
      n_degree_wt = total_weight;
    });
  }

  template <typename ValTy>
  static double GenerateRandonNumber(ValTy min, ValTy max) {
    std::random_device
        rd;  // Will be used to obtain a seed for the random number engine
    std::mt19937 gen(
        rd());  // Standard mersenne_twister_engine seeded with rd()
    std::uniform_real_distribution<> dis(
        min,
        max);  // distribution in range [min, max]
    return dis(gen);
  }

  template <typename EdgeWeightType>
  static uint64_t GetSubcommunity(
      const Graph& graph, GNode n, CommunityArray& subcomm_info,
      uint64_t comm_id, double constant_for_second_term, double resolution,
      std::vector<GNode>& subcomms) {
    auto& n_current_subcomm_id =
        graph.template GetData<CurrentSubCommunityID>(n);
    std::vector<EdgeTy> counter(graph.size(), 0);

    EdgeTy self_loop_wt = 0;

    for (auto e : graph.edges(n)) {
      auto dst = graph.edge_dest(e);
      // Self loop weights is recorded
      EdgeWeightType edge_wt =
          graph.template GetEdgeData<EdgeWeight<EdgeWeightType>>(e);
      auto& n_current_comm = graph.template GetData<CurrentCommunityID>(dst);
      auto& n_current_subcomm =
          graph.template GetData<CurrentSubCommunityID>(dst);

      if (n_current_comm == comm_id) {
        if (dst == n) {
          self_loop_wt += edge_wt;  // Self loop weights is recorded
          continue;
        }
        counter[n_current_subcomm] += edge_wt;
      }
    }  // End edge loop

    uint64_t best_cluster = n_current_subcomm_id;
    double max_quality_value_increment = -INFINITY_DOUBLE;
    double quality_value_increment = 0;

    auto& n_degree_wt = graph.template GetData<DegreeWeight<EdgeWeightType>>(n);

    for (auto subcomm : subcomms) {
      if (n_current_subcomm_id == subcomm || subcomm_info[subcomm].size == 0) {
        continue;
      }

      double subcomm_degree_wt = subcomm_info[subcomm].degree_wt;

      quality_value_increment =
          counter[subcomm] - counter[n_current_subcomm_id] -
          n_degree_wt *
              (subcomm_degree_wt -
               subcomm_info[n_current_subcomm_id].degree_wt + n_degree_wt) *
              constant_for_second_term * resolution;

      if (quality_value_increment > max_quality_value_increment) {
        best_cluster = subcomm;
        max_quality_value_increment = quality_value_increment;
      }

      counter[subcomm] = 0;
    }

    return best_cluster;
  }

  /**
 * Finds a clustering of the nodes in a network using the local merging
 * algorithm.
 *
 * <p>
 * The local merging algorithm starts from a singleton partition. It
 * performs a single iteration over the nodes in a network. Each node
 * belonging to a singleton cluster is considered for merging with another
 * cluster. This cluster is chosen randomly from all clusters that do not
 * result in a decrease in the quality function. The larger the increase in
 * the quality function, the more likely a cluster is to be chosen. The
 * strength of this effect is determined by the randomness parameter. The
 * higher the value of the randomness parameter, the stronger the
 * randomness in the choice of a cluster. The lower the value of the
 * randomness parameter, the more likely the cluster resulting in the
 * largest increase in the quality function is to be chosen. A node is
 * merged with a cluster only if both are sufficiently well connected to
 * the rest of the network.
 * </p>
 *
 * @param
 *
 * @return : Number of unique subcommunities formed
 * DO NOT parallelize as it is called within Galois parallel loops
 *
 */
  template <typename EdgeWeightType>
  static void MergeNodesSubset(
      Graph* graph, std::vector<GNode>& cluster_nodes, uint64_t comm_id,
      CommunityArray& subcomm_info,
      katana::NUMAArray<std::atomic<double>>& constant_for_second_term,
      double resolution) {
    for (uint64_t i = 0; i < cluster_nodes.size(); ++i) {
      GNode n = cluster_nodes[i];
      const auto& n_degree_wt =
          graph->template GetData<DegreeWeight<EdgeWeightType>>(n);
      const auto& n_node_wt = graph->template GetData<NodeWeight>(n);
      /*
     * Initialize with singleton sub-communities
     */
      EdgeWeightType node_edge_weight_within_cluster = 0;
      uint64_t num_edges_within_cluster = 0;

      for (auto e : graph->edges(n)) {
        auto dst = graph->edge_dest(e);
        EdgeWeightType edge_wt =
            graph->template GetEdgeData<EdgeWeight<EdgeWeightType>>(e);
        /*
       * Must include the edge weight of all neighbors excluding self loops
       * belonging to the community comm_id
       */
        if (dst != n &&
            graph->template GetData<CurrentCommunityID>(dst) == comm_id) {
          node_edge_weight_within_cluster += edge_wt;
          num_edges_within_cluster++;
        }
      }

      uint64_t node_wt = n_node_wt;
      uint64_t degree_wt = n_degree_wt;
      /*
     * Additionally, only nodes that are well connected with
     * the rest of the network are considered for moving.
     * (externalEdgeWeightPerCluster[j] >= clusterWeights[j] * (totalNodeWeight
     * - clusterWeights[j]) * resolution
     */

      subcomm_info[n].node_wt = node_wt;
      subcomm_info[n].internal_edge_wt = node_edge_weight_within_cluster;
      subcomm_info[n].num_internal_edges = num_edges_within_cluster;
      subcomm_info[n].size = 1;
      subcomm_info[n].degree_wt = degree_wt;
    }

    std::vector<GNode> subcomms;
    for (uint64_t i = 0; i < cluster_nodes.size(); ++i) {
      GNode n = cluster_nodes[i];

      subcomms.push_back(graph->template GetData<CurrentSubCommunityID>(n));
    }

    for (GNode n : cluster_nodes) {
      const auto& n_degree_wt =
          graph->template GetData<DegreeWeight<EdgeWeightType>>(n);
      const auto& n_node_wt = graph->template GetData<NodeWeight>(n);
      auto& n_current_subcomm_id =
          graph->template GetData<CurrentSubCommunityID>(n);
      /*
     * Only consider singleton communities
     */
      if (subcomm_info[n_current_subcomm_id].size == 1) {
        uint64_t new_subcomm_ass = GetSubcommunity<EdgeWeightType>(
            *graph, n, subcomm_info, comm_id, constant_for_second_term[comm_id],
            resolution, subcomms);

        if (new_subcomm_ass != UNASSIGNED &&
            new_subcomm_ass != n_current_subcomm_id) {
          /*
         * Move the currently selected node to its new cluster and
         * update the clustering statistics.
         */
          katana::atomicAdd(subcomm_info[new_subcomm_ass].node_wt, n_node_wt);
          katana::atomicAdd(subcomm_info[new_subcomm_ass].size, uint64_t{1});
          katana::atomicAdd(
              subcomm_info[new_subcomm_ass].degree_wt, n_degree_wt);

          katana::atomicSub(
              subcomm_info[n_current_subcomm_id].node_wt, n_node_wt);
          katana::atomicSub(
              subcomm_info[n_current_subcomm_id].size, uint64_t{1});
          katana::atomicSub(
              subcomm_info[n_current_subcomm_id].degree_wt, n_degree_wt);

          for (auto e : graph->edges(n)) {
            auto dst = graph->edge_dest(e);
            auto edge_wt =
                graph->template GetEdgeData<EdgeWeight<EdgeWeightType>>(e);
            if (dst != n &&
                graph->template GetData<CurrentCommunityID>(dst) == comm_id) {
              if (graph->template GetData<CurrentSubCommunityID>(dst) ==
                  new_subcomm_ass) {
                subcomm_info[new_subcomm_ass].internal_edge_wt -= 2.0 * edge_wt;
                subcomm_info[new_subcomm_ass].num_internal_edges -= 2;
              } else {
                subcomm_info[new_subcomm_ass].internal_edge_wt += 2.0 * edge_wt;
                subcomm_info[new_subcomm_ass].num_internal_edges += 2;
              }
            }  //end outer if

            if (dst != n &&
                graph->template GetData<CurrentCommunityID>(dst) == comm_id) {
              if (graph->template GetData<CurrentSubCommunityID>(dst) ==
                  n_current_subcomm_id) {
                subcomm_info[n_current_subcomm_id].internal_edge_wt +=
                    2.0 * edge_wt;
                subcomm_info[n_current_subcomm_id].num_internal_edges += 2;
              } else {
                subcomm_info[n_current_subcomm_id].internal_edge_wt -=
                    2.0 * edge_wt;
                subcomm_info[n_current_subcomm_id].num_internal_edges -= 2;
              }
            }  //end outer if
          }
        }  //end for

        n_current_subcomm_id = new_subcomm_ass;
      }  // end outer if
    }
  }

  /*
 * Refine the clustering by iterating over the clusters and by
 * trying to split up each cluster into multiple clusters.
 */
  template <typename EdgeWeightType>
  static void RefinePartition(Graph* graph, double resolution) {
    [[maybe_unused]] double constant_for_second_term =
        CalConstantForSecondTerm<EdgeWeightType>(*graph);
    // set singleton subcommunities
    katana::do_all(katana::iterate(*graph), [&](GNode n) {
      graph->template GetData<CurrentSubCommunityID>(n) = n;
    });

    // populate nodes into communities
    std::vector<std::vector<GNode>> cluster_bags(graph->size());
    CommunityArray comm_info;

    comm_info.allocateBlocked(graph->size());

    katana::do_all(katana::iterate(size_t{0}, (graph->size())), [&](size_t n) {
      comm_info[n].node_wt = 0ull;
      comm_info[n].degree_wt = 0ull;
    });

    //TODO (gill): Can be parallelized using do_all.
    for (GNode n : *graph) {
      const auto& n_current_comm =
          graph->template GetData<CurrentCommunityID>(n);
      const auto& n_node_wt = graph->template GetData<NodeWeight>(n);
      const auto& n_degree_wt =
          graph->template GetData<DegreeWeight<EdgeWeightType>>(n);
      if (n_current_comm != UNASSIGNED) {
        cluster_bags[n_current_comm].push_back(n);

        katana::atomicAdd(comm_info[n_current_comm].node_wt, n_node_wt);
        katana::atomicAdd(comm_info[n_current_comm].degree_wt, n_degree_wt);
      }
    }
    uint64_t total = 0;
    for (size_t n = 0; n < graph->size(); ++n) {
      total += cluster_bags[n].size();
    }

    CommunityArray subcomm_info;

    subcomm_info.allocateBlocked(graph->size());

    SumVertexDegreeWeightCommunity<EdgeWeightType>(graph);

    katana::NUMAArray<std::atomic<double>> comm_constant_term;

    comm_constant_term.allocateBlocked(graph->size());

    CalConstantForSecondTerm<EdgeWeightType>(*graph, &comm_constant_term);

    // call MergeNodesSubset for each community in parallel
    katana::do_all(katana::iterate(size_t{0}, graph->size()), [&](size_t c) {
      /*
                    * Only nodes belonging to singleton clusters can be moved to
                    * a different cluster. This guarantees that clusters will
                    * never be split up.
                    */
      comm_info[c].num_sub_communities = 0;
      if (cluster_bags[c].size() > 1) {
        MergeNodesSubset<EdgeWeightType>(
            graph, cluster_bags[c], c, subcomm_info, comm_constant_term,
            resolution);
      }
    });
  }

  template <typename EdgeWeightType>
  uint64_t MaxCPMQualityWithoutSwaps(
      std::map<uint64_t, uint64_t>& cluster_local_map,
      std::vector<EdgeWeightType>& counter, EdgeWeightType self_loop_wt,
      CommunityArray& c_info, uint64_t node_wt, uint64_t sc,
      double resolution) {
    uint64_t max_index = sc;  // Assign the initial value as self community
    double cur_gain = 0;
    double max_gain = 0;
    double eix = counter[0] - self_loop_wt;
    double eiy = 0;
    auto size_x = static_cast<double>(c_info[sc].node_wt - node_wt);
    double size_y = 0;

    auto stored_already = cluster_local_map.begin();
    do {
      if (sc != stored_already->first) {
        eiy = counter[stored_already
                          ->second];  // Total edges incident on cluster y
        size_y = c_info[stored_already->first].node_wt;

        cur_gain = 2.0 * (eiy - eix) - resolution *
                                           static_cast<double>(node_wt) *
                                           (size_y - size_x);
        if ((cur_gain > max_gain) ||
            ((cur_gain == max_gain) && (cur_gain != 0) &&
             (stored_already->first < max_index))) {
          max_gain = cur_gain;
          max_index = stored_already->first;
        }
      }
      stored_already++;  // Explore next cluster
    } while (stored_already != cluster_local_map.end());

    if ((c_info[max_index].size == 1 && c_info[sc].size == 1 &&
         max_index > sc)) {
      max_index = sc;
    }
    assert(max_gain >= 0);
    return max_index;
  }

  template <typename EdgeWeightType>
  double CalCPMQuality(
      Graph& graph, CommunityArray& c_info, double& e_xx, double& a2_x,
      double& constant_for_second_term, double resolution) {
    auto node_wt_func = [&](GNode n) {
      return static_cast<double>(c_info[n].node_wt) * resolution;
    };
    return ModularityImpl<EdgeWeightType, CurrentCommunityID>(
        graph, node_wt_func, e_xx, a2_x, constant_for_second_term);
  }
};
}  // namespace katana::analytics
#endif  // CLUSTERING_H
