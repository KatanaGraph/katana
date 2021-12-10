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

#include "katana/analytics/louvain_clustering/louvain_clustering.h"

#include <cmath>
#include <deque>
#include <type_traits>

#include "katana/TypedPropertyGraph.h"
#include "katana/analytics/ClusteringImplementationBase.h"

using namespace katana::analytics;
namespace {

template <typename EdgeWeightType>
struct GraphTypes {
  using NodeData = std::tuple<
      PreviousCommunityID, CurrentCommunityID, DegreeWeight<EdgeWeightType>>;

  using EdgeData = std::tuple<EdgeWeight<EdgeWeightType>>;

  using Graph = katana::TypedPropertyGraphView<
      katana::PropertyGraphViews::Undirected, NodeData, EdgeData>;
};

template <typename EdgeWeightType>
struct LouvainClusteringImplementation
    : public katana::analytics::ClusteringImplementationBase<
          typename GraphTypes<EdgeWeightType>::Graph, EdgeWeightType,
          CommunityType<EdgeWeightType>> {
  using CommTy = CommunityType<EdgeWeightType>;
  using CommunityArray = katana::NUMAArray<CommTy>;

  using Graph = typename GraphTypes<EdgeWeightType>::Graph;
  using NodeData = typename GraphTypes<EdgeWeightType>::NodeData;
  using EdgeData = typename GraphTypes<EdgeWeightType>::EdgeData;
  using GNode = typename Graph::Node;

  using Base = katana::analytics::ClusteringImplementationBase<
      Graph, EdgeWeightType, CommTy>;

  katana::Result<double> LouvainWithoutLockingDoAll(
      katana::PropertyGraph* pg, double lower,
      double modularity_threshold_per_round, uint32_t& iter) {
    katana::StatTimer TimerClusteringTotal("Timer_Clustering_Total");
    TimerClusteringTotal.start();

    auto graph_result = Graph::Make(pg);
    if (!graph_result) {
      return graph_result.error();
    }
    Graph graph = graph_result.value();

    CommunityArray c_info;    // Community info
    CommunityArray c_update;  // Used for updating community

    /* Variables needed for Modularity calculation */
    double prev_mod = lower;
    double curr_mod = -1;
    uint32_t num_iter = iter;

    /*** Initialization ***/
    c_info.allocateBlocked(graph.num_nodes());
    c_update.allocateBlocked(graph.num_nodes());

    /* Initialization each node to its own cluster */
    katana::do_all(katana::iterate(graph), [&](GNode n) {
      graph.template GetData<CurrentCommunityID>(n) = n;
      graph.template GetData<PreviousCommunityID>(n) = n;
    });

    /* Calculate the weighted degree sum for each vertex */
    Base::template SumVertexDegreeWeight<EdgeWeightType>(&graph, c_info);

    /* Compute the total weight (2m) and 1/2m terms */
    const double constant_for_second_term =
        Base::template CalConstantForSecondTerm<EdgeWeightType>(graph);

    if (std::isinf(constant_for_second_term)) {
      KATANA_LOG_FATAL("constant_for_second_term is INFINITY\n");
    }

    katana::StatTimer TimerClusteringWhile("Timer_Clustering_While");
    TimerClusteringWhile.start();
    while (true) {
      num_iter++;

      katana::do_all(katana::iterate(graph), [&](GNode n) {
        c_update[n].degree_wt = 0;
        c_update[n].size = 0;
      });

      katana::do_all(
          katana::iterate(graph),
          [&](GNode n) {
            auto& n_data_curr_comm_id =
                graph.template GetData<CurrentCommunityID>(n);
            auto& n_data_degree_wt =
                graph.template GetData<DegreeWeight<EdgeWeightType>>(n);

            uint64_t degree = graph.degree(n);
            uint64_t local_target = Base::UNASSIGNED;
            // TODO(amber): use scalable allocator with these containers
            std::map<uint64_t, uint64_t>
                cluster_local_map;  // Map each neighbor's cluster to local number:
                                    // Community --> Index
            std::vector<EdgeWeightType>
                counter;  // Number of edges to each unique cluster
            EdgeWeightType self_loop_wt = 0;

            if (degree > 0) {
              Base::template FindNeighboringClusters<EdgeWeightType>(
                  graph, n, cluster_local_map, counter, self_loop_wt);
              // Find the max gain in modularity
              local_target = Base::MaxModularityWithoutSwaps(
                  cluster_local_map, counter, self_loop_wt, c_info,
                  n_data_degree_wt, n_data_curr_comm_id,
                  constant_for_second_term);

            } else {
              local_target = Base::UNASSIGNED;
            }

            /* Update cluster info */
            if (local_target != n_data_curr_comm_id &&
                local_target != Base::UNASSIGNED) {
              katana::atomicAdd(
                  c_info[local_target].degree_wt, n_data_degree_wt);
              katana::atomicAdd(c_info[local_target].size, uint64_t{1});
              katana::atomicSub(
                  c_info[n_data_curr_comm_id].degree_wt, n_data_degree_wt);
              katana::atomicSub(c_info[n_data_curr_comm_id].size, uint64_t{1});

              /* Set the new cluster id */
              n_data_curr_comm_id = local_target;
            }
          },
          katana::loopname("louvain algo: Phase 1"));

      /* Calculate the overall modularity */
      double e_xx = 0;
      double a2_x = 0;

      curr_mod = Base::template CalModularity<EdgeWeightType>(
          graph, c_info, e_xx, a2_x, constant_for_second_term);

      if (std::isnan(curr_mod)) {
        KATANA_LOG_FATAL("Modulary is NaN. num_iter = {}\n", num_iter);
      }

      if ((curr_mod - prev_mod) < modularity_threshold_per_round) {
        prev_mod = curr_mod;
        break;
      }

      prev_mod = curr_mod;

    }  // End while
    TimerClusteringWhile.stop();

    iter = num_iter;

    c_info.destroy();
    c_info.deallocate();

    c_update.destroy();
    c_update.deallocate();

    TimerClusteringTotal.stop();
    return prev_mod;
  }

  // TODO The function arguments are  similar to
  // the non-deterministic one. Need to figure how to
  // do remove duplication
  katana::Result<double> LouvainDeterministic(
      katana::PropertyGraph* pg, double lower,
      double modularity_threshold_per_round, uint32_t& iter) {
    katana::StatTimer TimerClusteringTotal("Timer_Clustering_Total");
    katana::TimerGuard TimerClusteringGuard(TimerClusteringTotal);

    auto graph_result = Graph::Make(pg);
    if (!graph_result) {
      return graph_result.error();
    }
    Graph graph = graph_result.value();

    CommunityArray c_info;        // Community info
    CommunityArray c_update_add;  // Used for updating community
    CommunityArray c_update_subtract;

    /* Variables needed for Modularity calculation */
    double constant_for_second_term;
    double prev_mod = lower;
    double curr_mod = -1;
    uint32_t num_iter = iter;

    /*** Initialization ***/
    c_info.allocateBlocked(graph.num_nodes());
    c_update_add.allocateBlocked(graph.num_nodes());
    c_update_subtract.allocateBlocked(graph.num_nodes());

    /* Initialization each node to its own cluster */
    katana::do_all(katana::iterate(graph), [&](GNode n) {
      graph.template GetData<CurrentCommunityID>(n) = n;
      graph.template GetData<PreviousCommunityID>(n) = n;
    });

    /* Calculate the weighted degree sum for each vertex */
    Base::template SumVertexDegreeWeight<EdgeWeightType>(&graph, c_info);

    /* Compute the total weight (2m) and 1/2m terms */
    constant_for_second_term =
        Base::template CalConstantForSecondTerm<EdgeWeightType>(graph);

    katana::NUMAArray<uint64_t> local_target;
    local_target.allocateBlocked(graph.num_nodes());

    // partition nodes
    std::vector<katana::InsertBag<GNode>> bag(16);

    katana::InsertBag<GNode> to_process;
    katana::NUMAArray<bool> in_bag;
    in_bag.allocateBlocked(graph.num_nodes());

    katana::do_all(katana::iterate(graph), [&](GNode n) {
      uint64_t idx = n % 16;
      bag[idx].push(n);
      in_bag[n] = false;
      local_target[n] = Base::UNASSIGNED;
    });

    katana::do_all(katana::iterate(graph), [&](GNode n) {
      c_update_add[n].degree_wt = 0;
      c_update_add[n].size = 0;
      c_update_subtract[n].degree_wt = 0;
      c_update_subtract[n].size = 0;
    });

    katana::StatTimer TimerClusteringWhile("Timer_Clustering_While");
    TimerClusteringWhile.start();

    while (true) {
      num_iter++;

      for (uint64_t idx = 0; idx <= 15; idx++) {
        katana::GAccumulator<uint32_t> sync_round;

        katana::do_all(
            katana::iterate(bag[idx]),
            [&](GNode n) {
              auto& n_data_curr_comm_id =
                  graph.template GetData<CurrentCommunityID>(n);
              auto& n_data_degree_wt =
                  graph.template GetData<DegreeWeight<EdgeWeightType>>(n);

              uint64_t degree = graph.degree(n);

              // TODO(amber): use scalable allocators
              std::map<uint64_t, uint64_t>
                  cluster_local_map;  // Map each neighbor's cluster to local number:
                                      // Community --> Index
              std::vector<EdgeWeightType>
                  counter;  // Number of edges to each unique cluster
              EdgeWeightType self_loop_wt = 0;

              if (degree > 0) {
                Base::template FindNeighboringClusters<EdgeWeightType>(
                    graph, n, cluster_local_map, counter, self_loop_wt);
                // Find the max gain in modularity
                local_target[n] = Base::MaxModularityWithoutSwaps(
                    cluster_local_map, counter, self_loop_wt, c_info,
                    n_data_degree_wt, n_data_curr_comm_id,
                    constant_for_second_term);

              } else {
                local_target[n] = Base::UNASSIGNED;
              }

              /* Update cluster info */
              if (local_target[n] != n_data_curr_comm_id &&
                  local_target[n] != Base::UNASSIGNED) {
                katana::atomicAdd(
                    c_update_add[local_target[n]].degree_wt, n_data_degree_wt);
                katana::atomicAdd(
                    c_update_add[local_target[n]].size, (uint64_t)1);
                katana::atomicAdd(
                    c_update_subtract[n_data_curr_comm_id].degree_wt,
                    n_data_degree_wt);
                katana::atomicAdd(
                    c_update_subtract[n_data_curr_comm_id].size, (uint64_t)1);

                if (!in_bag[local_target[n]]) {
                  to_process.push(local_target[n]);
                  in_bag[local_target[n]] = true;
                }

                if (!in_bag[n_data_curr_comm_id]) {
                  to_process.push(n_data_curr_comm_id);
                  in_bag[n_data_curr_comm_id] = true;
                }
              }
            },
            katana::loopname("louvain algo: Phase 1"));

        katana::do_all(katana::iterate(bag[idx]), [&](GNode n) {
          graph.template GetData<CurrentCommunityID>(n) = local_target[n];
        });

        for (auto n : to_process) {
          if (in_bag[n]) {
            katana::atomicAdd(c_info[n].size, c_update_add[n].size.load());
            katana::atomicAdd(
                c_info[n].degree_wt, c_update_add[n].degree_wt.load());

            katana::atomicSub(c_info[n].size, c_update_subtract[n].size.load());
            katana::atomicSub(
                c_info[n].degree_wt, c_update_subtract[n].degree_wt.load());
            c_update_add[n].size = 0;
            c_update_add[n].degree_wt = 0;
            c_update_subtract[n].size = 0;
            c_update_subtract[n].degree_wt = 0;
            in_bag[n] = false;
          }
        }

      }  // end for

      /* Calculate the overall modularity */
      double e_xx = 0;
      double a2_x = 0;

      curr_mod = Base::template CalModularity<EdgeWeightType>(
          graph, c_info, e_xx, a2_x, constant_for_second_term);

      if ((curr_mod - prev_mod) < modularity_threshold_per_round) {
        prev_mod = curr_mod;
        break;
      }

      prev_mod = curr_mod;

      if (prev_mod < lower)
        prev_mod = lower;

    }  // End while
    TimerClusteringWhile.stop();

    iter = num_iter;
    return prev_mod;
  }

public:
  katana::Result<void> LouvainClustering(
      katana::PropertyGraph* pg, const std::string& edge_weight_property_name,
      const std::vector<std::string>& temp_node_property_names,
      katana::NUMAArray<uint64_t>& clusters_orig, LouvainClusteringPlan plan,
      tsuba::TxnContext* txn_ctx) {
    TemporaryPropertyGuard temp_edge_property{pg->EdgeMutablePropertyView()};
    std::vector<std::string> temp_edge_property_names = {
        temp_edge_property.name()};

    /*
     * Construct temp property graph. This graph gets coarsened as the
     * computation proceeds.
     */
    std::unique_ptr<katana::PropertyGraph> pg_mutable;

    Graph graph_curr = KATANA_CHECKED(
        Graph::Make(pg, temp_node_property_names, {edge_weight_property_name}));

    /*
    * Vertex following optimization
    */
    if (plan.enable_vf()) {
      Base::VertexFollowing(&graph_curr);  // Find nodes that follow other nodes

      uint64_t num_unique_clusters =
          Base::template RenumberClustersContiguously<CurrentCommunityID>(
              &graph_curr);

      /*
     * Initialize node cluster id.
     */
      katana::do_all(katana::iterate(graph_curr), [&](GNode n) {
        clusters_orig[n] = graph_curr.template GetData<CurrentCommunityID>(n);
      });

      auto pg_empty = std::make_unique<katana::PropertyGraph>();

      // Build new graph to remove the isolated nodes
      auto coarsened_graph_result = Base::template GraphCoarsening<
          NodeData, EdgeData, EdgeWeightType, CurrentCommunityID>(
          graph_curr, pg_empty.get(), num_unique_clusters,
          temp_node_property_names, temp_edge_property_names, txn_ctx);
      if (!coarsened_graph_result) {
        return coarsened_graph_result.error();
      }

      pg_mutable = std::move(coarsened_graph_result.value());

    } else {
      /*
       * Initialize node cluster id.
       */
      katana::do_all(katana::iterate(graph_curr), [&](GNode n) {
        clusters_orig[n] = Base::UNASSIGNED;
      });

      auto pg_dup = KATANA_CHECKED(Base::DuplicateGraphWithSameTopo(*pg));
      KATANA_CHECKED(Base::CopyEdgeProperty(
          pg, pg_dup.get(), edge_weight_property_name,
          temp_edge_property_names[0], txn_ctx));
      KATANA_CHECKED(ConstructNodeProperties<NodeData>(pg_dup.get(), txn_ctx));

      pg_mutable = std::move(pg_dup);
    }

    KATANA_LOG_ASSERT(pg_mutable);

    double prev_mod = -1;  // Previous modularity
    double curr_mod = -1;  // Current modularity
    uint32_t phase = 0;

    std::unique_ptr<katana::PropertyGraph> pg_curr = std::move(pg_mutable);
    uint32_t iter = 0;
    uint64_t num_nodes_orig = clusters_orig.size();
    while (true) {
      iter++;
      phase++;

      Graph graph_curr = KATANA_CHECKED(Graph::Make(pg_curr.get()));
      if (graph_curr.num_nodes() > plan.min_graph_size()) {
        switch (plan.algorithm()) {
        case LouvainClusteringPlan::kDoAll: {
          curr_mod = KATANA_CHECKED(LouvainWithoutLockingDoAll(
              pg_curr.get(), curr_mod, plan.modularity_threshold_per_round(),
              iter));
          break;
        }
        case LouvainClusteringPlan::kDeterministic: {
          curr_mod = KATANA_CHECKED(LouvainDeterministic(
              pg_curr.get(), curr_mod, plan.modularity_threshold_per_round(),
              iter));
          break;
        }
        default:
          return KATANA_ERROR(
              katana::ErrorCode::InvalidArgument, "Unknown algorithm");
        }
      } else {
        break;
      }

      uint64_t num_unique_clusters =
          Base::template RenumberClustersContiguously<CurrentCommunityID>(
              &graph_curr);

      if (iter < plan.max_iterations() &&
          (curr_mod - prev_mod) > plan.modularity_threshold_total()) {
        if (!plan.enable_vf() && phase == 1) {
          KATANA_LOG_DEBUG_ASSERT(num_nodes_orig == graph_curr.num_nodes());
          katana::do_all(katana::iterate(graph_curr), [&](GNode n) {
            clusters_orig[n] =
                graph_curr.template GetData<CurrentCommunityID>(n);
          });
        } else {
          katana::do_all(
              katana::iterate((uint64_t)0, num_nodes_orig), [&](GNode n) {
                if (clusters_orig[n] != Base::UNASSIGNED) {
                  KATANA_LOG_DEBUG_ASSERT(
                      clusters_orig[n] < graph_curr.num_nodes());
                  clusters_orig[n] =
                      graph_curr.template GetData<CurrentCommunityID>(
                          clusters_orig[n]);
                }
              });
        }

        auto coarsened_graph_result = Base::template GraphCoarsening<
            NodeData, EdgeData, EdgeWeightType, CurrentCommunityID>(
            graph_curr, pg_curr.get(), num_unique_clusters,
            temp_node_property_names, temp_edge_property_names, txn_ctx);
        if (!coarsened_graph_result) {
          return coarsened_graph_result.error();
        }

        pg_curr = std::move(coarsened_graph_result.value());

        prev_mod = curr_mod;
      } else {
        break;
      }
    }
    return katana::ResultSuccess();
  }
};

template <typename EdgeWeightType>
static katana::Result<void>
AddDefaultEdgeWeight(
    katana::PropertyGraph* pg, const std::string& edge_weight_property_name,
    tsuba::TxnContext* txn_ctx) {
  using EdgeData = std::tuple<EdgeWeightType>;

  if (auto res = katana::analytics::ConstructEdgeProperties<EdgeData>(
          pg, txn_ctx, {edge_weight_property_name});
      !res) {
    return res.error();
  }

  auto typed_graph =
      KATANA_CHECKED((katana::TypedPropertyGraph<std::tuple<>, EdgeData>::Make(
          pg, {}, {edge_weight_property_name})));
  katana::do_all(
      katana::iterate(typed_graph.all_edges()),
      [&](auto e) { typed_graph.template GetEdgeData<EdgeWeightType>(e) = 1; },
      katana::steal(), katana::loopname("InitEdgeWeight"));
  return katana::ResultSuccess();
}

template <typename EdgeWeightType>
static katana::Result<void>
LouvainClusteringWithWrap(
    katana::PropertyGraph* pg, const std::string& edge_weight_property_name,
    const std::string& output_property_name, LouvainClusteringPlan plan,
    tsuba::TxnContext* txn_ctx) {
  static_assert(
      std::is_integral_v<EdgeWeightType> ||
      std::is_floating_point_v<EdgeWeightType>);

  std::vector<TemporaryPropertyGuard> temp_node_properties(3);
  std::generate_n(
      temp_node_properties.begin(), temp_node_properties.size(),
      [&]() { return TemporaryPropertyGuard{pg->NodeMutablePropertyView()}; });
  std::vector<std::string> temp_node_property_names(
      temp_node_properties.size());
  std::transform(
      temp_node_properties.begin(), temp_node_properties.end(),
      temp_node_property_names.begin(),
      [](const TemporaryPropertyGuard& p) { return p.name(); });

  using Impl = LouvainClusteringImplementation<EdgeWeightType>;
  KATANA_CHECKED(ConstructNodeProperties<typename Impl::NodeData>(
      pg, txn_ctx, temp_node_property_names));

  /*
   * To keep track of communities for nodes in the original graph.
   * Community will be set to UNASSINED for isolated nodes
   */
  katana::NUMAArray<uint64_t> clusters_orig;
  clusters_orig.allocateBlocked(pg->num_nodes());

  LouvainClusteringImplementation<EdgeWeightType> impl{};
  KATANA_CHECKED(impl.LouvainClustering(
      pg, edge_weight_property_name, temp_node_property_names, clusters_orig,
      plan, txn_ctx));

  KATANA_CHECKED(ConstructNodeProperties<std::tuple<CurrentCommunityID>>(
      pg, txn_ctx, {output_property_name}));

  auto graph = KATANA_CHECKED((
      katana::TypedPropertyGraph<std::tuple<CurrentCommunityID>, std::tuple<>>::
          Make(pg, {output_property_name}, {})));

  katana::do_all(
      katana::iterate(graph),
      [&](uint32_t i) {
        graph.GetData<CurrentCommunityID>(i) = clusters_orig[i];
      },
      katana::loopname("Add clusterIDs"), katana::no_stats());

  return katana::ResultSuccess();
}

}  // anonymous namespace

katana::Result<void>
katana::analytics::LouvainClustering(
    katana::PropertyGraph* pg, const std::string& edge_weight_property_name,
    const std::string& output_property_name, tsuba::TxnContext* txn_ctx,
    LouvainClusteringPlan plan) {
  if (!edge_weight_property_name.empty() &&
      !pg->HasEdgeProperty(edge_weight_property_name)) {
    return KATANA_ERROR(
        katana::ErrorCode::NotFound, "Edge Property: {} Not found",
        edge_weight_property_name);
  }
  // If edge property name empty, add int64_t property
  // add initialize it to 1.
  if (edge_weight_property_name.empty()) {
    TemporaryPropertyGuard temporary_edge_property{
        pg->EdgeMutablePropertyView()};
    struct EdgeWt : public katana::PODProperty<int64_t> {};
    KATANA_CHECKED(AddDefaultEdgeWeight<EdgeWt>(
        pg, temporary_edge_property.name(), txn_ctx));
    return LouvainClusteringWithWrap<int64_t>(
        pg, temporary_edge_property.name(), output_property_name, plan,
        txn_ctx);
  }

  switch (KATANA_CHECKED(pg->GetEdgeProperty(edge_weight_property_name))
              ->type()
              ->id()) {
  case arrow::UInt32Type::type_id:
    return LouvainClusteringWithWrap<uint32_t>(
        pg, edge_weight_property_name, output_property_name, plan, txn_ctx);
  case arrow::Int32Type::type_id:
    return LouvainClusteringWithWrap<int32_t>(
        pg, edge_weight_property_name, output_property_name, plan, txn_ctx);
  case arrow::UInt64Type::type_id:
    return LouvainClusteringWithWrap<uint64_t>(
        pg, edge_weight_property_name, output_property_name, plan, txn_ctx);
  case arrow::Int64Type::type_id:
    return LouvainClusteringWithWrap<int64_t>(
        pg, edge_weight_property_name, output_property_name, plan, txn_ctx);
  case arrow::FloatType::type_id:
    return LouvainClusteringWithWrap<float>(
        pg, edge_weight_property_name, output_property_name, plan, txn_ctx);
  case arrow::DoubleType::type_id:
    return LouvainClusteringWithWrap<double>(
        pg, edge_weight_property_name, output_property_name, plan, txn_ctx);
  default:
    return KATANA_ERROR(
        katana::ErrorCode::TypeError, "Unsupported type: {}",
        KATANA_CHECKED(pg->GetEdgeProperty(edge_weight_property_name))
            ->type()
            ->ToString());
  }
}

/// \cond DO_NOT_DOCUMENT
katana::Result<void>
katana::analytics::LouvainClusteringAssertValid(
    [[maybe_unused]] katana::PropertyGraph* pg,
    [[maybe_unused]] const std::string& edge_weight_property_name,
    [[maybe_unused]] const std::string& property_name) {
  // TODO(gill): This should have real checks.
  return katana::ResultSuccess();
}
/// \endcond

void
katana::analytics::LouvainClusteringStatistics::Print(std::ostream& os) const {
  os << "Total number of clusters = " << n_clusters << std::endl;
  os << "Total number of non trivial clusters = " << n_non_trivial_clusters
     << std::endl;
  os << "Number of nodes in the largest cluster = " << largest_cluster_size
     << std::endl;
  os << "Ratio of nodes in the largest cluster = " << largest_cluster_proportion
     << std::endl;
  os << "Louvain modularity = " << modularity << std::endl;
}

template <typename EdgeWeightType>
katana::Result<double>
CalModularityWrap(
    katana::PropertyGraph* pg, const std::string& edge_weight_property_name,
    const std::string& property_name) {
  using CommTy = CommunityType<EdgeWeightType>;
  using NodeData = std::tuple<PreviousCommunityID>;
  using EdgeData = std::tuple<EdgeWeight<EdgeWeightType>>;
  using Graph = katana::TypedPropertyGraphView<
      katana::PropertyGraphViews::Undirected, NodeData, EdgeData>;
  using ClusterBase = katana::analytics::ClusteringImplementationBase<
      Graph, EdgeWeightType, CommTy>;
  auto graph_result =
      Graph::Make(pg, {property_name}, {edge_weight_property_name});
  if (!graph_result) {
    return graph_result.error();
  }
  auto graph = graph_result.value();
  return ClusterBase::template CalModularityFinal<
      EdgeWeightType, PreviousCommunityID>(graph);
}

katana::Result<katana::analytics::LouvainClusteringStatistics>
katana::analytics::LouvainClusteringStatistics::Compute(
    katana::PropertyGraph* pg, const std::string& edge_weight_property_name,
    const std::string& property_name, tsuba::TxnContext* txn_ctx) {
  auto graph_result = katana::
      TypedPropertyGraph<std::tuple<PreviousCommunityID>, std::tuple<>>::Make(
          pg, {property_name}, {});
  if (!graph_result) {
    return graph_result.error();
  }
  auto graph = graph_result.value();

  using Map = katana::gstl::Map<uint64_t, uint64_t>;

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

  katana::GAccumulator<size_t> accumReps;

  katana::do_all(
      katana::iterate(graph),
      [&](const uint32_t& x) {
        auto& n = graph.template GetData<PreviousCommunityID>(x);
        accumMap.update(Map{std::make_pair(n, uint64_t{1})});
      },
      katana::loopname("CountLargest"));

  Map& map = accumMap.reduce();
  size_t reps = map.size();

  using ClusterSizePair = std::pair<uint32_t, uint32_t>;

  auto sizeMax = [](const ClusterSizePair& a, const ClusterSizePair& b) {
    if (a.second > b.second) {
      return a;
    }
    return b;
  };

  auto identity = []() { return ClusterSizePair{}; };

  auto maxComp = katana::make_reducible(sizeMax, identity);

  katana::GAccumulator<uint64_t> non_trivial_clusters;
  katana::do_all(katana::iterate(map), [&](const ClusterSizePair& x) {
    maxComp.update(x);
    if (x.second > 1) {
      non_trivial_clusters += 1;
    }
  });

  ClusterSizePair largest = maxComp.reduce();

  // Compensate for dropping representative node of components
  size_t largest_cluster_size = largest.second + 1;
  double largest_cluster_proportion = 0;
  if (!graph.empty()) {
    largest_cluster_proportion = double(largest_cluster_size) / graph.size();
  }

  double modularity = 0.0;

  if (!edge_weight_property_name.empty() &&
      !pg->HasEdgeProperty(edge_weight_property_name)) {
    return KATANA_ERROR(
        katana::ErrorCode::NotFound, "Edge Property: {} Not found",
        edge_weight_property_name);
  }
  // If edge property name is empty, add int64_t edge property and
  // initialize it to 1.
  if (edge_weight_property_name.empty()) {
    TemporaryPropertyGuard temporary_edge_property{
        pg->EdgeMutablePropertyView()};
    struct EdgeWt : public katana::PODProperty<int64_t> {};
    KATANA_CHECKED(AddDefaultEdgeWeight<EdgeWt>(
        pg, temporary_edge_property.name(), txn_ctx));

    auto modularity_result = CalModularityWrap<int64_t>(
        pg, temporary_edge_property.name(), property_name);
    if (!modularity_result) {
      return modularity_result.error();
    }
    modularity = modularity_result.value();
  } else {
    switch (KATANA_CHECKED(pg->GetEdgeProperty(edge_weight_property_name))
                ->type()
                ->id()) {
    case arrow::UInt32Type::type_id: {
      modularity = KATANA_CHECKED(CalModularityWrap<uint32_t>(
          pg, edge_weight_property_name, property_name));
      break;
    }
    case arrow::Int32Type::type_id: {
      modularity = KATANA_CHECKED(CalModularityWrap<int32_t>(
          pg, edge_weight_property_name, property_name));
      break;
    }
    case arrow::UInt64Type::type_id: {
      modularity = KATANA_CHECKED(CalModularityWrap<uint64_t>(
          pg, edge_weight_property_name, property_name));
      break;
    }
    case arrow::Int64Type::type_id: {
      modularity = KATANA_CHECKED(CalModularityWrap<int64_t>(
          pg, edge_weight_property_name, property_name));
      break;
    }
    case arrow::FloatType::type_id: {
      modularity = KATANA_CHECKED(CalModularityWrap<float>(
          pg, edge_weight_property_name, property_name));
      break;
    }
    case arrow::DoubleType::type_id: {
      modularity = KATANA_CHECKED(CalModularityWrap<double>(
          pg, edge_weight_property_name, property_name));
      break;
    }
    default:
      return KATANA_ERROR(
          katana::ErrorCode::TypeError, "Unsupported type: {}",
          KATANA_CHECKED(pg->GetEdgeProperty(edge_weight_property_name))
              ->type()
              ->ToString());
    }
  }
  return LouvainClusteringStatistics{
      reps, non_trivial_clusters.reduce(), largest_cluster_size,
      largest_cluster_proportion, modularity};
}
