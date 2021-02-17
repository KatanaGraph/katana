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

#include <deque>
#include <type_traits>

#include "katana/TypedPropertyGraph.h"
#include "katana/analytics/ClusteringImplementationBase.h"

using namespace katana::analytics;
namespace {

template <typename EdgeWeightType>
struct LouvainClusteringImplementation
    : public katana::analytics::ClusteringImplementationBase<
          katana::TypedPropertyGraph<
              std::tuple<
                  PreviousCommunityId, CurrentCommunityId,
                  DegreeWeight<EdgeWeightType>>,
              std::tuple<EdgeWeight<EdgeWeightType>>>,
          EdgeWeightType, CommunityType<EdgeWeightType>> {
  using NodeData = std::tuple<
      PreviousCommunityId, CurrentCommunityId, DegreeWeight<EdgeWeightType>>;
  using EdgeData = std::tuple<EdgeWeight<EdgeWeightType>>;
  using CommTy = CommunityType<EdgeWeightType>;
  using CommunityArray = katana::LargeArray<CommTy>;

  using Graph = katana::TypedPropertyGraph<NodeData, EdgeData>;
  using GNode = typename Graph::Node;

  using Base = katana::analytics::ClusteringImplementationBase<
      Graph, EdgeWeightType, CommTy>;

  katana::Result<double> LouvainWithoutLockingDoAll(
      katana::PropertyGraph* pfg, double lower,
      double modularity_threshold_per_round, uint32_t& iter) {
    katana::StatTimer TimerClusteringTotal("Timer_Clustering_Total");
    TimerClusteringTotal.start();

    auto graph_result = Graph::Make(pfg);
    if (!graph_result) {
      return graph_result.error();
    }
    Graph graph = graph_result.value();

    CommunityArray c_info;    // Community info
    CommunityArray c_update;  // Used for updating community

    /* Variables needed for Modularity calculation */
    double constant_for_second_term;
    double prev_mod = lower;
    double curr_mod = -1;
    uint32_t num_iter = iter;

    /*** Initialization ***/
    c_info.allocateBlocked(graph.num_nodes());
    c_update.allocateBlocked(graph.num_nodes());

    /* Initialization each node to its own cluster */
    katana::do_all(katana::iterate(graph), [&](GNode n) {
      graph.template GetData<CurrentCommunityId>(n) = n;
      graph.template GetData<PreviousCommunityId>(n) = n;
    });

    /* Calculate the weighted degree sum for each vertex */
    Base::template SumVertexDegreeWeight<EdgeWeightType>(&graph, c_info);

    /* Compute the total weight (2m) and 1/2m terms */
    constant_for_second_term =
        Base::template CalConstantForSecondTerm<EdgeWeightType>(graph);

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
                graph.template GetData<CurrentCommunityId>(n);
            auto& n_data_degree_wt =
                graph.template GetData<DegreeWeight<EdgeWeightType>>(n);

            uint64_t degree =
                std::distance(graph.edge_begin(n), graph.edge_end(n));
            uint64_t local_target = Base::UNASSIGNED;
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
              katana::atomicAdd(c_info[local_target].size, (uint64_t)1);
              katana::atomicSub(
                  c_info[n_data_curr_comm_id].degree_wt, n_data_degree_wt);
              katana::atomicSub(c_info[n_data_curr_comm_id].size, (uint64_t)1);

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

public:
  katana::Result<void> LouvainClustering(
      katana::PropertyGraph* pfg, const std::string& edge_weight_property_name,
      const std::vector<std::string>& temp_node_property_names,
      katana::LargeArray<uint64_t>& clusters_orig, LouvainClusteringPlan plan) {
    /*
     * Construct temp property graph. This graph gets coarsened as the
     * computation proceeds.
     */
    auto pfg_mutable = std::make_unique<katana::PropertyGraph>();
    katana::LargeArray<uint64_t> out_indices_next;
    katana::LargeArray<uint32_t> out_dests_next;

    out_indices_next.allocateInterleaved(pfg->topology().num_nodes());
    out_dests_next.allocateInterleaved(pfg->topology().num_edges());

    auto numeric_array_out_indices =
        std::make_shared<arrow::NumericArray<arrow::UInt64Type>>(
            static_cast<int64_t>(pfg->topology().num_nodes()),
            arrow::MutableBuffer::Wrap(
                out_indices_next.data(), pfg->topology().num_nodes()));
    auto numeric_array_out_dests =
        std::make_shared<arrow::NumericArray<arrow::UInt32Type>>(
            static_cast<int64_t>(pfg->topology().num_edges()),
            arrow::MutableBuffer::Wrap(
                out_dests_next.data(), pfg->topology().num_edges()));

    if (auto r = pfg_mutable->SetTopology(katana::GraphTopology{
            .out_indices = std::move(numeric_array_out_indices),
            .out_dests = std::move(numeric_array_out_dests),
        });
        !r) {
      return r.error();
    }
    if (auto result = ConstructNodeProperties<NodeData>(
            pfg_mutable.get(), temp_node_property_names);
        !result) {
      return result.error();
    }
    std::vector<std::string> temp_edge_property_names = {
        "_katana_temporary_property_" + edge_weight_property_name};
    if (auto result = ConstructEdgeProperties<EdgeData>(
            pfg_mutable.get(), temp_edge_property_names);
        !result) {
      return result.error();
    }

    auto graph_result = Graph::Make(pfg);
    if (!graph_result) {
      return graph_result.error();
    }
    Graph graph_curr = graph_result.value();

    /*
    * Vertex following optimization
    */
    if (plan.is_enable_vf()) {
      Base::VertexFollowing(&graph_curr);  // Find nodes that follow other nodes

      uint64_t num_unique_clusters =
          Base::RenumberClustersContiguously(&graph_curr);

      /*
     * Initialize node cluster id.
     */
      katana::do_all(katana::iterate(graph_curr), [&](GNode n) {
        clusters_orig[n] = graph_curr.template GetData<CurrentCommunityId>(n);
      });

      // Build new graph to remove the isolated nodes
      auto coarsened_graph_result =
          Base::template GraphCoarsening<NodeData, EdgeData, EdgeWeightType>(
              graph_curr, pfg_mutable.get(), num_unique_clusters,
              temp_node_property_names, temp_edge_property_names);
      if (!coarsened_graph_result) {
        return coarsened_graph_result.error();
      }

      auto pfg_next = std::move(coarsened_graph_result.value());
      pfg_mutable = std::move(pfg_next);

    } else {
      /*
       * Initialize node cluster id.
       */
      katana::do_all(
          katana::iterate(graph_curr), [&](GNode n) { clusters_orig[n] = -1; });

      if (auto r = Base::CreateDuplicateGraph(
              pfg, pfg_mutable.get(), edge_weight_property_name,
              temp_edge_property_names[0]);
          !r) {
        return r.error();
      }

      if (auto result = ConstructNodeProperties<NodeData>(pfg_mutable.get());
          !result) {
        return result.error();
      }
    }

    double prev_mod = -1;  // Previous modularity
    double curr_mod = -1;  // Current modularity
    uint32_t phase = 0;

    std::unique_ptr<katana::PropertyGraph> pfg_curr =
        std::make_unique<katana::PropertyGraph>();
    pfg_curr = std::move(pfg_mutable);
    uint32_t iter = 0;
    uint64_t num_nodes_orig = clusters_orig.size();
    while (true) {
      iter++;
      phase++;

      auto graph_result = Graph::Make(pfg_curr.get());
      if (!graph_result) {
        return graph_result.error();
      }
      Graph graph_curr = graph_result.value();
      if (graph_curr.num_nodes() > plan.min_graph_size()) {
        switch (plan.algorithm()) {
        case LouvainClusteringPlan::kDoAll: {
          auto curr_mod_result = LouvainWithoutLockingDoAll(
              pfg_curr.get(), curr_mod, plan.modularity_threshold_per_round(),
              iter);
          if (!curr_mod_result) {
            return curr_mod_result.error();
          }
          curr_mod = curr_mod_result.value();
          break;
        }
        default:
          return katana::ErrorCode::InvalidArgument;
        }
      }

      uint64_t num_unique_clusters =
          Base::RenumberClustersContiguously(&graph_curr);

      if (iter < plan.max_iterations() &&
          (curr_mod - prev_mod) > plan.modularity_threshold_total()) {
        if (!plan.is_enable_vf() && phase == 1) {
          KATANA_LOG_DEBUG_ASSERT(num_nodes_orig == graph_curr.num_nodes());
          katana::do_all(katana::iterate(graph_curr), [&](GNode n) {
            clusters_orig[n] =
                graph_curr.template GetData<CurrentCommunityId>(n);
          });
        } else {
          katana::do_all(
              katana::iterate((uint64_t)0, num_nodes_orig), [&](GNode n) {
                if (clusters_orig[n] != Base::UNASSIGNED) {
                  KATANA_LOG_DEBUG_ASSERT(
                      clusters_orig[n] < graph_curr.num_nodes());
                  clusters_orig[n] =
                      graph_curr.template GetData<CurrentCommunityId>(
                          clusters_orig[n]);
                }
              });
        }

        auto coarsened_graph_result =
            Base::template GraphCoarsening<NodeData, EdgeData, EdgeWeightType>(
                graph_curr, pfg_curr.get(), num_unique_clusters,
                temp_node_property_names, temp_edge_property_names);
        if (!coarsened_graph_result) {
          return coarsened_graph_result.error();
        }

        pfg_curr = std::move(coarsened_graph_result.value());

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
LouvainClusteringWithWrap(
    katana::PropertyGraph* pfg, const std::string& edge_weight_property_name,
    const std::string& output_property_name, LouvainClusteringPlan plan) {
  static_assert(
      std::is_integral_v<EdgeWeightType> ||
      std::is_floating_point_v<EdgeWeightType>);

  //! The property name with prefixed with "_katana_temporary_property"
  //! are reserved for internal use only.
  std::vector<std::string> temp_node_property_names = {
      "_katana_temporary_property_CurrentId",
      "_katana_temporary_property_PreviousId",
      "_katana_temporary_property_DegreeWt"};
  using Impl = LouvainClusteringImplementation<EdgeWeightType>;
  if (auto result = ConstructNodeProperties<typename Impl::NodeData>(
          pfg, temp_node_property_names);
      !result) {
    return result.error();
  }

  /*
   * To keep track of communities for nodes in the original graph.
   * Community will be set to -1 for isolated nodes
   */
  katana::LargeArray<uint64_t> clusters_orig;
  clusters_orig.allocateBlocked(pfg->num_nodes());

  LouvainClusteringImplementation<EdgeWeightType> impl{};
  if (auto r = impl.LouvainClustering(
          pfg, edge_weight_property_name, temp_node_property_names,
          clusters_orig, plan);
      !r) {
    return r.error();
  }

  // // Remove all the existing node/edge properties
  // if (auto r = pfg->RemoveAllNodeProperties(); !r) {
  //   return r.error();
  // }
  for (auto property : temp_node_property_names) {
    if (auto r = pfg->RemoveNodeProperty(property); !r) {
      return r.error();
    }
  }

  if (auto r = ConstructNodeProperties<std::tuple<CurrentCommunityId>>(
          pfg, {output_property_name});
      !r) {
    return r.error();
  }

  auto graph_result =
      katana::TypedPropertyGraph<std::tuple<CurrentCommunityId>, std::tuple<>>::
          Make(pfg, {output_property_name}, {});
  if (!graph_result) {
    return graph_result.error();
  }
  auto graph = graph_result.value();

  katana::do_all(
      katana::iterate(graph),
      [&](uint32_t i) {
        graph.GetData<CurrentCommunityId>(i) = clusters_orig[i];
      },
      katana::loopname("Add clusterIds"), katana::no_stats());

  return katana::ResultSuccess();
}

}  // anonymous namespace

katana::Result<void>
katana::analytics::LouvainClustering(
    katana::PropertyGraph* pg, const std::string& edge_weight_property_name,
    const std::string& output_property_name, LouvainClusteringPlan plan) {
  switch (pg->GetEdgeProperty(edge_weight_property_name)->type()->id()) {
  case arrow::UInt32Type::type_id:
    return LouvainClusteringWithWrap<uint32_t>(
        pg, edge_weight_property_name, output_property_name, plan);
  case arrow::Int32Type::type_id:
    return LouvainClusteringWithWrap<int32_t>(
        pg, edge_weight_property_name, output_property_name, plan);
  case arrow::UInt64Type::type_id:
    return LouvainClusteringWithWrap<uint64_t>(
        pg, edge_weight_property_name, output_property_name, plan);
  case arrow::Int64Type::type_id:
    return LouvainClusteringWithWrap<int64_t>(
        pg, edge_weight_property_name, output_property_name, plan);
  case arrow::FloatType::type_id:
    return LouvainClusteringWithWrap<float>(
        pg, edge_weight_property_name, output_property_name, plan);
  case arrow::DoubleType::type_id:
    return LouvainClusteringWithWrap<double>(
        pg, edge_weight_property_name, output_property_name, plan);
  default:
    return katana::ErrorCode::TypeError;
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
  using NodeData = std::tuple<PreviousCommunityId>;
  using EdgeData = std::tuple<EdgeWeight<EdgeWeightType>>;
  using Graph = katana::TypedPropertyGraph<NodeData, EdgeData>;
  using ClusterBase = katana::analytics::ClusteringImplementationBase<
      Graph, EdgeWeightType, CommTy>;
  auto graph_result =
      Graph::Make(pg, {property_name}, {edge_weight_property_name});
  if (!graph_result) {
    return graph_result.error();
  }
  auto graph = graph_result.value();
  return ClusterBase::template CalModularityFinal<Graph, EdgeWeightType>(graph);
}

katana::Result<katana::analytics::LouvainClusteringStatistics>
katana::analytics::LouvainClusteringStatistics::Compute(
    katana::PropertyGraph* pg, const std::string& edge_weight_property_name,
    const std::string& property_name) {
  auto graph_result = katana::
      TypedPropertyGraph<std::tuple<PreviousCommunityId>, std::tuple<>>::Make(
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
        auto& n = graph.template GetData<PreviousCommunityId>(x);
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

  switch (pg->GetEdgeProperty(edge_weight_property_name)->type()->id()) {
  case arrow::UInt32Type::type_id: {
    auto modularity_result = CalModularityWrap<uint32_t>(
        pg, edge_weight_property_name, property_name);
    if (!modularity_result) {
      return modularity_result.error();
    }
    modularity = modularity_result.value();
    break;
  }
  case arrow::Int32Type::type_id: {
    auto modularity_result = CalModularityWrap<int32_t>(
        pg, edge_weight_property_name, property_name);
    if (!modularity_result) {
      return modularity_result.error();
    }
    modularity = modularity_result.value();
    break;
  }
  case arrow::UInt64Type::type_id: {
    auto modularity_result = CalModularityWrap<uint64_t>(
        pg, edge_weight_property_name, property_name);
    if (!modularity_result) {
      return modularity_result.error();
    }
    modularity = modularity_result.value();
    break;
  }
  case arrow::Int64Type::type_id: {
    auto modularity_result = CalModularityWrap<int64_t>(
        pg, edge_weight_property_name, property_name);
    if (!modularity_result) {
      return modularity_result.error();
    }
    modularity = modularity_result.value();
    break;
  }
  case arrow::FloatType::type_id: {
    auto modularity_result =
        CalModularityWrap<float>(pg, edge_weight_property_name, property_name);
    if (!modularity_result) {
      return modularity_result.error();
    }
    modularity = modularity_result.value();
    break;
  }
  case arrow::DoubleType::type_id: {
    auto modularity_result =
        CalModularityWrap<double>(pg, edge_weight_property_name, property_name);
    if (!modularity_result) {
      return modularity_result.error();
    }
    modularity = modularity_result.value();
    break;
  }
  default:
    return katana::ErrorCode::TypeError;
  }
  return LouvainClusteringStatistics{
      reps, non_trivial_clusters.reduce(), largest_cluster_size,
      largest_cluster_proportion, modularity};
}
