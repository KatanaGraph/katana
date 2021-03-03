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

#ifndef KATANA_LIBGALOIS_KATANA_ANALYTICS_CLUSTERINGIMPLEMENTATIONBASE_H_
#define KATANA_LIBGALOIS_KATANA_ANALYTICS_CLUSTERINGIMPLEMENTATIONBASE_H_

#include <fstream>
#include <iostream>
#include <random>

#include "katana/AtomicHelpers.h"
#include "katana/Galois.h"
#include "katana/LargeArray.h"
#include "katana/analytics/Utils.h"

namespace katana::analytics {

// Maintain community information
template <typename EdgeWeightType>
struct CommunityType {
  std::atomic<uint64_t> size;
  std::atomic<EdgeWeightType> degree_wt;
  EdgeWeightType internal_edge_wt;
};

using PreviousCommunityId = katana::PODProperty<uint64_t>;
using CurrentCommunityId = katana::PODProperty<uint64_t>;
// using ColorId = katana::PODProperty<int64_t>;
template <typename EdgeWeightType>
using DegreeWeight = katana::PODProperty<EdgeWeightType>;

template <typename EdgeWeightType>
using EdgeWeight = katana::PODProperty<EdgeWeightType>;

template <typename _Graph, typename _EdgeType, typename _CommunityType>
struct ClusteringImplementationBase {
  using Graph = _Graph;
  using GNode = typename Graph::Node;
  using EdgeTy = _EdgeType;
  using CommunityType = _CommunityType;

  constexpr static const uint64_t UNASSIGNED =
      std::numeric_limits<uint64_t>::max();

  using CommunityArray = katana::LargeArray<CommunityType>;

  /**
   * Algorithm to find the best cluster for the node
   * to move to among its neighbors in the graph and moves.
   *
   * It updates the mapping of neighboring nodes clusters
   * in cluster_local_map, total unique cluster edge weights
   * in counter as well as total weight of self edges in self_loop_wt.
   */
  template <typename EdgeWeightType>
  void FindNeighboringClusters(
      const Graph& graph, GNode& n,
      std::map<uint64_t, uint64_t>& cluster_local_map,
      std::vector<EdgeTy>& counter, EdgeTy& self_loop_wt) {
    uint64_t num_unique_clusters = 0;

    // Add the node's current cluster to be considered
    // for movement as well
    cluster_local_map[graph.template GetData<CurrentCommunityId>(n)] =
        0;                 // Add n's current cluster
    counter.push_back(0);  // Initialize the counter to zero (no edges incident
                           // yet)
    num_unique_clusters++;

    // Assuming we have grabbed lock on all the neighbors
    for (auto ii = graph.edge_begin(n); ii != graph.edge_end(n); ++ii) {
      auto dst = graph.GetEdgeDest(ii);
      auto edge_wt = graph.template GetEdgeData<EdgeWeight<EdgeWeightType>>(
          ii);  // Self loop weights is recorded
      if (*dst == n) {
        self_loop_wt += edge_wt;  // Self loop weights is recorded
      }
      auto stored_already =
          cluster_local_map.find(graph.template GetData<CurrentCommunityId>(
              dst));  // Check if it already exists
      if (stored_already != cluster_local_map.end()) {
        counter[stored_already->second] += edge_wt;
      } else {
        cluster_local_map[graph.template GetData<CurrentCommunityId>(dst)] =
            num_unique_clusters;
        counter.push_back(edge_wt);
        num_unique_clusters++;
      }
    }  // End edge loop
    return;
  }

  /**
   * Enables the filtering optimization to remove the
   * node with out-degree 0 (isolated) and 1 before the clustering
   * algorithm begins.
   */
  uint64_t VertexFollowing(Graph* graph) {
    using GNode = typename Graph::Node;
    // Initialize each node to its own cluster
    katana::do_all(katana::iterate(*graph), [&](GNode n) {
      graph->template GetData<CurrentCommunityId>(n) = n;
    });

    // Remove isolated and degree-one nodes
    katana::GAccumulator<uint64_t> isolated_nodes;
    katana::do_all(katana::iterate(*graph), [&](GNode n) {
      auto& n_data_curr_comm_id =
          graph->template GetData<CurrentCommunityId>(n);
      uint64_t degree = std::distance(graph->edge_begin(n), graph->edge_end(n));
      if (degree == 0) {
        isolated_nodes += 1;
        n_data_curr_comm_id = UNASSIGNED;
      } else {
        if (degree == 1) {
          // Check if the destination has degree greater than one
          auto dst = graph->GetEdgeDest(graph->edge_end(n));
          uint64_t dst_degree =
              std::distance(graph->edge_begin(*dst), graph->edge_end(*dst));
          if ((dst_degree > 1 || (n > *dst))) {
            isolated_nodes += 1;
            n_data_curr_comm_id =
                graph->template GetData<CurrentCommunityId>(dst);
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
  void SumVertexDegreeWeight(Graph* graph, CommunityArray& c_info) {
    katana::do_all(katana::iterate(*graph), [&](GNode n) {
      EdgeTy total_weight = 0;
      auto& n_degree_wt =
          graph->template GetData<DegreeWeight<EdgeWeightType>>(n);
      for (auto ii = graph->edge_begin(n); ii != graph->edge_end(n); ++ii) {
        total_weight +=
            graph->template GetEdgeData<EdgeWeight<EdgeWeightType>>(ii);
      }
      n_degree_wt = total_weight;
      c_info[n].degree_wt = total_weight;
      c_info[n].size = 1;
    });
    return;
  }

  /**
   * Computes the constant term 1/(2 * total internal edge weight)
   * of the current coarsened graph.
   */
  template <typename EdgeWeightType>
  double CalConstantForSecondTerm(const Graph& graph) {
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
   * of the current coarsened graph. Takes the optional LargeArray
   * with edge weight. To be used if edge weight is missing in the
   * property graph.
   */
  template <typename EdgeWeightType>
  static double CalConstantForSecondTerm(
      const Graph& graph,
      katana::LargeArray<EdgeWeightType>& degree_weight_array) {
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
   * Computes the modularity gain of the current cluster assignment
   * without swapping the cluster assignment.
   */
  uint64_t MaxModularityWithoutSwaps(
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

  /**
   * Computes the modularity gain of the current cluster assignment.
   */
  template <typename EdgeWeightType>
  double CalModularity(
      const Graph& graph, CommunityArray& c_info, double& e_xx, double& a2_x,
      double& constant_for_second_term) {
    /* Variables needed for Modularity calculation */
    double mod = -1;

    katana::LargeArray<EdgeTy> cluster_wt_internal;

    /*** Initialization ***/
    cluster_wt_internal.allocateBlocked(graph.num_nodes());

    /* Calculate the overall modularity */
    katana::GAccumulator<double> acc_e_xx;
    katana::GAccumulator<double> acc_a2_x;

    katana::do_all(
        katana::iterate(graph), [&](GNode n) { cluster_wt_internal[n] = 0; });

    katana::do_all(katana::iterate(graph), [&](GNode n) {
      auto n_data_current_comm_id =
          graph.template GetData<CurrentCommunityId>(n);
      for (auto ii = graph.edge_begin(n); ii != graph.edge_end(n); ++ii) {
        if (graph.template GetData<CurrentCommunityId>(graph.GetEdgeDest(ii)) ==
            n_data_current_comm_id) {
          cluster_wt_internal[n] +=
              graph.template GetEdgeData<EdgeWeight<EdgeWeightType>>(ii);
        }
      }
    });

    katana::do_all(katana::iterate(graph), [&](GNode n) {
      acc_e_xx += cluster_wt_internal[n];
      acc_a2_x +=
          (double)(c_info[n].degree_wt) *
          ((double)(c_info[n].degree_wt) * (double)constant_for_second_term);
    });

    e_xx = acc_e_xx.reduce();
    a2_x = acc_a2_x.reduce();

    mod = e_xx * (double)constant_for_second_term -
          a2_x * (double)constant_for_second_term;
    return mod;
  }

  template <typename EdgeWeightType>
  static void SumClusterWeight(
      Graph& graph, CommunityArray& c_info,
      katana::LargeArray<EdgeWeightType>& degree_weight_array) {
    using GNode = typename Graph::Node;

    katana::do_all(katana::iterate(graph), [&](GNode n) {
      EdgeTy total_weight = 0;
      for (auto ii = graph.edge_begin(n); ii != graph.edge_end(n); ++ii) {
        total_weight +=
            graph.template GetEdgeData<EdgeWeight<EdgeWeightType>>(ii);
      }
      degree_weight_array[n] = total_weight;
      c_info[n].degree_wt = 0;
    });

    katana::do_all(katana::iterate(graph), [&](GNode n) {
      auto& n_data_comm_id = graph.template GetData<CurrentCommunityId>(n);
      if (n_data_comm_id != UNASSIGNED)
        katana::atomicAdd(
            c_info[n_data_comm_id].degree_wt, degree_weight_array[n]);
    });
  }

  /**
 * Computes the final modularity using prev cluster
 * assignments.
 */
  template <typename GraphTy, typename EdgeWeightType>
  static double CalModularityFinal(GraphTy& graph) {
    CommunityArray c_info;    // Community info
    CommunityArray c_update;  // Used for updating community

    /* Variables needed for Modularity calculation */
    double constant_for_second_term;
    double mod = -1;

    katana::LargeArray<EdgeTy> cluster_wt_internal;

    /*** Initialization ***/
    c_info.allocateBlocked(graph.num_nodes());
    c_update.allocateBlocked(graph.num_nodes());
    cluster_wt_internal.allocateBlocked(graph.num_nodes());

    katana::LargeArray<EdgeWeightType> degree_weight_array;
    degree_weight_array.allocateBlocked(graph.num_nodes());

    /* Calculate the weighted degree sum for each vertex */
    SumClusterWeight<EdgeWeightType>(graph, c_info, degree_weight_array);

    /* Compute the total weight (2m) and 1/2m terms */
    constant_for_second_term =
        CalConstantForSecondTerm<EdgeWeightType>(graph, degree_weight_array);

    /* Calculate the overall modularity */
    double e_xx = 0;
    katana::GAccumulator<double> acc_e_xx;
    double a2_x = 0;
    katana::GAccumulator<double> acc_a2_x;

    katana::do_all(
        katana::iterate(graph), [&](GNode n) { cluster_wt_internal[n] = 0; });

    katana::do_all(katana::iterate(graph), [&](GNode n) {
      auto n_data_current_comm = graph.template GetData<CurrentCommunityId>(n);
      for (auto ii = graph.edge_begin(n); ii != graph.edge_end(n); ++ii) {
        if (graph.template GetData<CurrentCommunityId>(graph.GetEdgeDest(ii)) ==
            n_data_current_comm) {
          // if(graph.getData(graph.getEdgeDst(ii)).prev_comm_ass ==
          // n_data.prev_comm_ass) {
          cluster_wt_internal[n] +=
              graph.template GetEdgeData<EdgeWeight<EdgeWeightType>>(ii);
        }
      }
    });

    katana::do_all(katana::iterate(graph), [&](GNode n) {
      acc_e_xx += cluster_wt_internal[n];
      acc_a2_x +=
          (double)(c_info[n].degree_wt) *
          ((double)(c_info[n].degree_wt) * (double)constant_for_second_term);
    });

    e_xx = acc_e_xx.reduce();
    a2_x = acc_a2_x.reduce();

    mod = e_xx * (double)constant_for_second_term -
          a2_x * (double)constant_for_second_term;
    return mod;
  }

  /**
 * Renumbers the cluster to contiguous cluster ids
 * to fill the holes in the cluster id assignments.
 */
  uint64_t RenumberClustersContiguously(Graph* graph) {
    std::map<uint64_t, uint64_t> cluster_local_map;
    uint64_t num_unique_clusters = 0;

    for (GNode n = 0; n < graph->num_nodes(); ++n) {
      auto& n_data_curr_comm_id =
          graph->template GetData<CurrentCommunityId>(n);
      if (n_data_curr_comm_id != UNASSIGNED) {
        KATANA_LOG_DEBUG_ASSERT(n_data_curr_comm_id < graph->num_nodes());
        auto stored_already = cluster_local_map.find(n_data_curr_comm_id);
        if (stored_already != cluster_local_map.end()) {
          n_data_curr_comm_id = stored_already->second;
        } else {
          cluster_local_map[n_data_curr_comm_id] = num_unique_clusters;
          n_data_curr_comm_id = num_unique_clusters;
          num_unique_clusters++;
        }
      }
    }
    return num_unique_clusters;
  }

  template <typename EdgeWeightType>
  void CheckModularity(
      Graph& graph, katana::LargeArray<uint64_t>& clusters_orig) {
    katana::do_all(katana::iterate(graph), [&](GNode n) {
      graph.template GetData<CurrentCommunityId>(n).curr_comm_ass =
          clusters_orig[n];
    });

    [[maybe_unused]] uint64_t num_unique_clusters =
        RenumberClustersContiguously(graph);
    auto mod = CalModularityFinal<Graph, EdgeWeightType>(graph);
  }

  /**
 * Creates a duplicate of the graph by copying the
 * graph (pfg_from) topology as well as edge property
 * (read from the underlying RDG) to the in-memory
 * temporary graph (pfg_to).
 */
  katana::Result<void> CreateDuplicateGraph(
      katana::PropertyGraph* pfg_from, katana::PropertyGraph* pfg_to,
      const std::string& edge_property_name,
      const std::string& new_edge_property_name) {
    const katana::GraphTopology& topology_from = pfg_from->topology();
    const katana::GraphTopology& topology_to = pfg_to->topology();

    KATANA_ASSERT(
        (topology_from.num_nodes() == topology_to.num_nodes()),
        "num nodes of both property graph must be same");

    auto out_indices_view_from_result =
        katana::ConstructPropertyView<katana::UInt64Property>(
            topology_from.out_indices.get());
    if (!out_indices_view_from_result) {
      return out_indices_view_from_result.error();
    }
    auto out_indices_view_from =
        std::move(out_indices_view_from_result.value());

    auto out_dests_view_from_result =
        katana::ConstructPropertyView<katana::UInt32Property>(
            topology_from.out_dests.get());
    if (!out_dests_view_from_result) {
      return out_dests_view_from_result.error();
    }
    auto out_dests_view_from = std::move(out_dests_view_from_result.value());

    auto out_indices_view_to_result =
        katana::ConstructPropertyView<katana::UInt64Property>(
            topology_to.out_indices.get());
    if (!out_indices_view_to_result) {
      return out_indices_view_to_result.error();
    }
    auto out_indices_view_to = std::move(out_indices_view_to_result.value());

    auto out_dests_view_to_result =
        katana::ConstructPropertyView<katana::UInt32Property>(
            topology_to.out_dests.get());
    if (!out_dests_view_to_result) {
      return out_dests_view_to_result.error();
    }
    auto out_dests_view_to = std::move(out_dests_view_to_result.value());

    // First pass to find the number of edges
    katana::do_all(
        katana::iterate((uint64_t)0, topology_from.num_nodes()),
        [&](uint64_t n) { out_indices_view_to[n] = out_indices_view_from[n]; });
    katana::do_all(
        katana::iterate((uint64_t)0, topology_from.num_edges()),
        [&](uint64_t e) { out_dests_view_to[e] = out_dests_view_from[e]; });

    // Remove the existing edge property
    if (auto r = pfg_to->RemoveEdgeProperty(new_edge_property_name); !r) {
      return r.error();
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
    if (auto r = pfg_to->AddEdgeProperties(edge_data_table); !r) {
      return r;
    }
    return katana::ResultSuccess();
  }

  /**
 * Creates a coarsened hierarchical graph for the next phase
 * of the clustering algorithm. It merges all the nodes within a
 * same cluster to form a super node for the coursened graphs.
 * The total number of nodes in the coarsened graph are equal to
 * the number of unique clusters in the previous level of the graph.
 * All the edges inside a cluster are merged (edge weights are summed
 * up) to form the edges within super nodes.
 */
  template <typename NodeData, typename EdgeData, typename EdgeWeightType>
  katana::Result<std::unique_ptr<katana::PropertyGraph>> GraphCoarsening(
      const Graph& graph, katana::PropertyGraph* pfg_mutable,
      uint64_t num_unique_clusters,
      const std::vector<std::string>& temp_node_property_names,
      const std::vector<std::string>& temp_edge_property_names) {
    using GNode = typename Graph::Node;

    katana::StatTimer TimerGraphBuild("Timer_Graph_build");
    TimerGraphBuild.start();
    uint64_t num_nodes_next = num_unique_clusters;
    uint64_t num_edges_next = 0;  // Unknown right now

    std::vector<std::vector<GNode>> cluster_bags(num_unique_clusters);
    // Comment: Serial separation is better than do_all due to contention
    for (GNode n = 0; n < graph.num_nodes(); ++n) {
      auto n_data_curr_comm_id = graph.template GetData<CurrentCommunityId>(n);
      if (n_data_curr_comm_id != UNASSIGNED)
        cluster_bags[n_data_curr_comm_id].push_back(n);
    }

    std::vector<std::vector<uint32_t>> edges_id(num_unique_clusters);
    std::vector<std::vector<EdgeTy>> edges_data(num_unique_clusters);

    /* First pass to find the number of edges */
    katana::do_all(
        katana::iterate((uint64_t)0, num_unique_clusters),
        [&](uint64_t c) {
          std::map<uint64_t, uint64_t> cluster_local_map;
          uint64_t num_unique_clusters = 0;
          for (auto cb_ii = cluster_bags[c].begin();
               cb_ii != cluster_bags[c].end(); ++cb_ii) {
            KATANA_LOG_DEBUG_ASSERT(
                graph.template GetData<CurrentCommunityId>(*cb_ii) ==
                c);  // All nodes in this bag must have same cluster id

            for (auto ii = graph.edge_begin(*cb_ii);
                 ii != graph.edge_end(*cb_ii); ++ii) {
              auto dst = graph.GetEdgeDest(ii);
              auto dst_data_curr_comm_id =
                  graph.template GetData<CurrentCommunityId>(dst);
              KATANA_LOG_DEBUG_ASSERT(dst_data_curr_comm_id != UNASSIGNED);
              auto stored_already = cluster_local_map.find(
                  dst_data_curr_comm_id);  // Check if it already exists
              if (stored_already != cluster_local_map.end()) {
                edges_data[c][stored_already->second] +=
                    graph.template GetEdgeData<EdgeWeight<EdgeWeightType>>(ii);
              } else {
                cluster_local_map[dst_data_curr_comm_id] = num_unique_clusters;
                edges_id[c].push_back(dst_data_curr_comm_id);
                edges_data[c].push_back(
                    graph.template GetEdgeData<EdgeWeight<EdgeWeightType>>(ii));
                num_unique_clusters++;
              }
            }  // End edge loop
          }
        },
        katana::steal(), katana::loopname("BuildGraph: Find edges"));

    /* Serial loop to reduce all the edge counts */
    std::vector<uint64_t> prefix_edges_count(num_unique_clusters);
    katana::GAccumulator<uint64_t> num_edges_acc;
    katana::do_all(
        katana::iterate((uint64_t)0, num_nodes_next), [&](uint64_t c) {
          prefix_edges_count[c] = edges_id[c].size();
          num_edges_acc += prefix_edges_count[c];
        });

    num_edges_next = num_edges_acc.reduce();
    for (uint64_t c = 1; c < num_nodes_next; ++c) {
      prefix_edges_count[c] += prefix_edges_count[c - 1];
    }

    KATANA_LOG_DEBUG_ASSERT(
        prefix_edges_count[num_unique_clusters - 1] == num_edges_next);
    katana::StatTimer TimerConstructFrom("Timer_Construct_From");
    TimerConstructFrom.start();

    // Remove all the existing node/edge properties
    for (auto property : temp_node_property_names) {
      if (auto r = pfg_mutable->RemoveNodeProperty(property); !r) {
        return r.error();
      }
    }
    for (auto property : temp_edge_property_names) {
      if (auto r = pfg_mutable->RemoveEdgeProperty(property); !r) {
        return r.error();
      }
    }

    const katana::GraphTopology& topology = pfg_mutable->topology();
    auto out_indices_next =
        (topology.out_indices.get())
            ->Slice(0, static_cast<int64_t>(num_nodes_next));
    std::shared_ptr<arrow::Array> out_dests_next =
        (topology.out_dests.get())
            ->Slice(0, static_cast<int64_t>(num_edges_next));

    auto numeric_array_out_indices =
        std::make_shared<arrow::NumericArray<arrow::UInt64Type>>(
            out_indices_next->data());
    auto numeric_array_out_dests =
        std::make_shared<arrow::NumericArray<arrow::UInt32Type>>(
            out_dests_next->data());

    auto pfg_next = std::make_unique<katana::PropertyGraph>();
    if (auto r = pfg_next->SetTopology(katana::GraphTopology{
            .out_indices = std::move(numeric_array_out_indices),
            .out_dests = std::move(numeric_array_out_dests),
        });
        !r) {
      return r.error();
    }

    const katana::GraphTopology& topology_next = pfg_next->topology();

    auto out_indices_view_result =
        katana::ConstructPropertyView<katana::UInt64Property>(
            topology_next.out_indices.get());
    if (!out_indices_view_result) {
      return out_indices_view_result.error();
    }
    auto out_indices_view = std::move(out_indices_view_result.value());

    auto out_dests_view_result =
        katana::ConstructPropertyView<katana::UInt32Property>(
            topology_next.out_dests.get());
    if (!out_dests_view_result) {
      return out_dests_view_result.error();
    }
    auto out_dests_view = std::move(out_dests_view_result.value());

    if (auto result = katana::analytics::ConstructNodeProperties<NodeData>(
            pfg_next.get(), temp_node_property_names);
        !result) {
      return result.error();
    }

    if (auto result = katana::analytics::ConstructEdgeProperties<EdgeData>(
            pfg_next.get(), temp_edge_property_names);
        !result) {
      return result.error();
    }

    auto graph_result = Graph::Make(pfg_next.get());
    if (!graph_result) {
      return graph_result.error();
    }
    Graph graph_curr = graph_result.value();
    katana::do_all(
        katana::iterate((uint64_t)0, num_nodes_next), [&](uint64_t n) {
          out_indices_view[n] = prefix_edges_count[n];
          uint64_t number_of_edges =
              (n == 0) ? prefix_edges_count[0]
                       : (prefix_edges_count[n] - prefix_edges_count[n - 1]);
          uint64_t start_index = (n == 0) ? 0 : prefix_edges_count[n - 1];
          for (uint32_t k = 0; k < number_of_edges; ++k) {
            out_dests_view[start_index + k] = edges_id[n][k];
            graph_curr.template GetEdgeData<EdgeWeight<EdgeWeightType>>(
                start_index + k) = edges_data[n][k];
          }
        });

    TimerConstructFrom.stop();

    TimerGraphBuild.stop();
    return std::unique_ptr<katana::PropertyGraph>(std::move(pfg_next));
  }
};
}  // namespace katana::analytics
#endif  // CLUSTERING_H
