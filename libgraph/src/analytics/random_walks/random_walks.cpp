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

#include "katana/analytics/random_walks/random_walks.h"

#include "katana/TypedPropertyGraph.h"

using namespace katana::analytics;

const int RandomWalksPlan::kChunkSize = 1;

namespace {

using SortedPropertyGraphView = katana::PropertyGraphViews::EdgesSortedByDestID;

struct Node2VecAlgo {
  using NodeData = std::tuple<>;
  using EdgeData = std::tuple<>;

  using SortedGraphView = katana::TypedPropertyGraphView<
      SortedPropertyGraphView, NodeData, EdgeData>;
  using GNode = typename SortedGraphView::Node;

  const RandomWalksPlan& plan_;
  Node2VecAlgo(const RandomWalksPlan& plan) : plan_(plan) {}

  GNode FindSampleNeighbor(
      const SortedGraphView& graph, const GNode& n,
      const size_t n_deg, const double prob) {
    KATANA_LOG_ASSERT(n_deg > 0);
    double total_wt = n_deg;

    uint32_t edge_index = std::floor(prob * total_wt);
    auto ei = graph.OutEdges(n).begin() + edge_index;
    return graph.OutEdgeDst(*ei);
  }

  void GraphRandomWalk(
      const SortedGraphView& graph,
      katana::InsertBag<std::vector<uint32_t>>* walks) {
    katana::PerThreadStorage<std::mt19937> generator;
    katana::PerThreadStorage<std::uniform_real_distribution<double>>
      distribution;

    for (uint32_t i = 0; i < distribution.size(); i++) {
      *distribution.getRemote(i) =
          std::uniform_real_distribution<double>(0.0, 1.0);
    }

    double prob_forward = 1.0 / plan_.forward_probability();
    double prob_backward = 1.0 / plan_.backward_probability();

    double upper_bound = 1.0;

    upper_bound = (upper_bound > prob_forward) ? upper_bound : prob_forward;
    upper_bound = (upper_bound > prob_backward) ? upper_bound : prob_backward;

    double lower_bound = 1.0;

    lower_bound = (lower_bound < prob_forward) ? lower_bound : prob_forward;
    lower_bound = (lower_bound < prob_backward) ? lower_bound : prob_backward;

    uint64_t total_walks = graph.size() * plan_.number_of_walks();

    katana::do_all(
        katana::iterate(uint64_t(0), total_walks),
        [&](uint64_t idx) {
          GNode::underlying_type n_val = idx % graph.size();
          GNode n{n_val};
          auto n_deg = graph.OutDegree(n);

          //check if n has no neighbor
          if (n_deg == 0) {
            return;
          }

          std::uniform_real_distribution<double>& dist =
              *distribution.getLocal();

          std::vector<uint32_t> walk;
          walk.push_back(n.value());

          //random value between 0 and 1
          double prob = dist(*generator.getLocal());

          //Assumption: All edges have weight 1
          auto nbr = FindSampleNeighbor(graph, n, n_deg, prob);

          walk.push_back(nbr.value());

          for (uint32_t current_walk = 2; current_walk <= plan_.walk_length();
               current_walk++) {
            GNode curr {walk[current_walk - 1]};
            GNode prev {walk[current_walk - 2]};

            auto curr_deg = graph.OutDegree(curr);

            //check if n has no neighbor
            if (curr_deg == 0) {
              break;
            }
            //acceptance-rejection sampling
            while (true) {
              //sample x
              double prob = dist(*generator.getLocal());

              auto nbr = FindSampleNeighbor(graph, curr, curr_deg, prob);

              //sample y
              double y = dist(*generator.getLocal());
              y = y * upper_bound;

              if (y <= lower_bound) {
                //accept this sample
                walk.push_back(nbr.value());
                break;
              } else {
                //compute transition probability
                double alpha;

                //check if nbr is same as the previous node on this walk
                if (nbr == prev) {
                  alpha = prob_backward;
                }  //check if nbr is also a neighbor of the previous node on this walk
                else if (graph.HasEdge(prev, nbr)) {
                  alpha = 1.0;
                } else {
                  alpha = prob_forward;
                }

                if (y <= alpha) {
                  //accept y
                  walk.push_back(nbr.value());
                  break;
                }
              }
            }
          }

          walks->push(std::move(walk));
        },
        katana::steal(), katana::chunk_size<RandomWalksPlan::kChunkSize>(),
        katana::loopname("Node2vec walks"), katana::no_stats());

  }

  void operator()(
      const SortedGraphView& graph,
      katana::InsertBag<std::vector<uint32_t>>* walks) {
    GraphRandomWalk(graph, walks);
  }
};

struct Edge2VecAlgo {
  using EdgeType = katana::UInt32Property;

  using NodeData = std::tuple<>;
  using EdgeData = std::tuple<EdgeType>;

  using SortedGraphView = katana::TypedPropertyGraphView<
      SortedPropertyGraphView, NodeData, EdgeData>;
  using GNode = typename SortedGraphView::Node;

  const RandomWalksPlan& plan_;
  Edge2VecAlgo(const RandomWalksPlan& plan) : plan_(plan) {}

  //transition matrix
  std::vector<std::vector<double>> transition_matrix_;

  void Initialize() {
    transition_matrix_.resize(plan_.number_of_edge_types() + 1);
    //initialize transition matrix
    for (uint32_t i = 0; i <= plan_.number_of_edge_types(); i++) {
      for (uint32_t j = 0; j <= plan_.number_of_edge_types(); j++) {
        transition_matrix_[i].push_back(1.0);
      }
    }
  }

  std::pair<GNode, EdgeType::ViewType::value_type> FindSampleNeighbor(
      const SortedGraphView& graph, const GNode& n,
      const size_t n_deg, const double prob) {
    KATANA_LOG_ASSERT(n_deg > 0);
    double total_wt = n_deg;

    uint32_t edge_index = std::floor(prob * total_wt);
    auto ei = graph.OutEdges(n).begin() + edge_index;
    return std::make_pair(
        graph.OutEdgeDst(*ei), graph.GetEdgeData<EdgeType>(*ei));
  }

  void GraphRandomWalk(
      const SortedGraphView& graph,
      katana::InsertBag<std::vector<uint32_t>>* walks,
      katana::InsertBag<std::vector<uint32_t>>* types_walks) {

    katana::PerThreadStorage<std::mt19937> generator;
    katana::PerThreadStorage<std::uniform_real_distribution<double>>
        distribution;

    for (uint32_t i = 0; i < distribution.size(); i++) {
      *distribution.getRemote(i) =
          std::uniform_real_distribution<double>(0.0, 1.0);
    }

    double prob_forward = 1.0 / plan_.forward_probability();
    double prob_backward = 1.0 / plan_.backward_probability();

    double upper_bound = 1.0;

    upper_bound = (upper_bound > prob_forward) ? upper_bound : prob_forward;
    upper_bound = (upper_bound > prob_backward) ? upper_bound : prob_backward;

    uint64_t total_walks = graph.size() * plan_.number_of_walks();

    katana::do_all(
        katana::iterate(uint64_t(0), total_walks),
        [&](uint64_t idx) {

        GNode::underlying_type  n_val = idx % graph.size();
        GNode n{n_val};
        auto n_deg = graph.OutDegree(n);

          //check if n has no neighbor
          if (n_deg == 0) {
            return;
          }

          std::uniform_real_distribution<double>& dist =
              *distribution.getLocal();

          std::vector<uint32_t> walk;
          std::vector<uint32_t> types_vec;

          walk.push_back(n.value());

          //random value between 0 and 1
          double prob = dist(*generator.getLocal());

          //Assumption: All edges have weight 1
          auto nbr_pair = FindSampleNeighbor(graph, n, n_deg, prob);
          KATANA_LOG_ASSERT(nbr_pair.first < graph.NumNodes());

          walk.push_back(nbr_pair.first.value());
          types_vec.push_back(nbr_pair.second);

          for (uint32_t current_walk = 2; current_walk <= plan_.walk_length();
               current_walk++) {

            GNode curr{walk[walk.size() - 1]};
            auto curr_deg = graph.OutDegree(curr);

            //check if n has no neighbor
            if (curr_deg == 0) {
              return;
            }
            GNode prev{walk[walk.size() - 2]};

            uint32_t p1 = types_vec.back();  //last element of types_vec

            //acceptance-rejection sampling
            while (true) {
              //sample x
              double prob = dist(*generator.getLocal());

              auto nbr_type_pair =
                  FindSampleNeighbor(graph, curr, curr_deg, prob);

              GNode nbr = nbr_type_pair.first;
              EdgeType::ViewType::value_type p2 = nbr_type_pair.second;

              //sample y
              double y = dist(*generator.getLocal());
              y = y * upper_bound;

              //compute transition probability
              double alpha;

              //check if nbr is same as the previous node on this walk
              if (nbr == prev) {
                alpha = prob_backward;
              }  //check if nbr is also a neighbor of the previous node on this walk
              else if (graph.HasEdge(prev, nbr)) {
                alpha = 1.0;
              } else {
                alpha = prob_forward;
              }

              alpha = alpha * transition_matrix_[p1][p2];
              if (alpha >= y) {
                //accept y
                walk.push_back(nbr.value());
                types_vec.push_back(p2);
                break;
              }
            }  //end while

          }  //end for

          (*walks).push(std::move(walk));
          (*types_walks).push(std::move(types_vec));
        },
        katana::steal(), katana::chunk_size<RandomWalksPlan::kChunkSize>(),
        katana::loopname("Edge2vec walks"), katana::no_stats());
  }

  //compute the histogram of edge types for each walk
  std::vector<std::vector<uint32_t>> ComputeNumEdgeTypeVectors(
      const katana::InsertBag<std::vector<uint32_t>>& types_walks) {
    std::vector<std::vector<uint32_t>> num_edge_types_walks;

    katana::PerThreadStorage<std::vector<std::vector<uint32_t>>>
        per_thread_num_edge_types_walks;
    katana::do_all(
        katana::iterate(types_walks),
        [&](const std::vector<uint32_t>& types_walk) {
          std::vector<uint32_t> num_edge_types(
              plan_.number_of_edge_types() + 1, 0);

          for (auto type : types_walk) {
            num_edge_types[type]++;
          }

          per_thread_num_edge_types_walks.getLocal()->emplace_back(
              std::move(num_edge_types));
        });

    for (unsigned j = 0; j < katana::getActiveThreads(); ++j) {
      for (auto num_edge_types :
           *per_thread_num_edge_types_walks.getRemote(j)) {
        num_edge_types_walks.push_back(std::move(num_edge_types));
      }
    }

    return num_edge_types_walks;
  }

  std::vector<std::vector<uint32_t>> TransformVectors(
      const std::vector<std::vector<uint32_t>>& num_edge_types_walks) {
    uint32_t rows = num_edge_types_walks.size();
    std::vector<std::vector<uint32_t>> transformed_num_edge_types_walks;
    transformed_num_edge_types_walks.resize(plan_.number_of_edge_types() + 1);

    katana::do_all(
        katana::iterate(uint32_t(0), plan_.number_of_edge_types() + 1),
        [&](uint32_t j) {
          for (uint32_t i = 0; i < rows; i++) {
            transformed_num_edge_types_walks[j].push_back(
                num_edge_types_walks[i][j]);
          }
        });

    return transformed_num_edge_types_walks;
  }

  std::vector<double> ComputeMeans(const std::vector<std::vector<uint32_t>>&
                                       transformed_num_edge_types_walks) {
    std::vector<double> means(plan_.number_of_edge_types() + 1);

    for (uint32_t i = 1; i <= plan_.number_of_edge_types(); i++) {
      uint32_t sum = 0;
      for (auto n : transformed_num_edge_types_walks[i]) {
        sum += n;
      }

      means[i] = ((double)sum) / (transformed_num_edge_types_walks[i].size());
    }

    return means;
  }

  double sigmoidCal(const double pears) {
    return 1 / (1 + exp(-pears));  //exact sig
  }

  double pearsonCorr(
      const uint32_t i, const uint32_t j,
      const std::vector<std::vector<uint32_t>>&
          transformed_num_edge_types_walks,
      const std::vector<double>& means) {
    const std::vector<uint32_t>& x = transformed_num_edge_types_walks[i];
    const std::vector<uint32_t>& y = transformed_num_edge_types_walks[j];

    double sum = 0.0;
    double sig1 = 0.0;
    double sig2 = 0.0;

    for (uint32_t m = 0; m < x.size(); m++) {
      sum += ((double)x[m] - means[i]) * ((double)y[m] - means[j]);
      sig1 += ((double)x[m] - means[i]) * ((double)x[m] - means[i]);
      sig2 += ((double)y[m] - means[j]) * ((double)y[m] - means[j]);
    }

    sum = sum / x.size();

    sig1 = sig1 / x.size();
    sig1 = sqrt(sig1);

    sig2 = sig2 / x.size();
    sig2 = sqrt(sig2);

    double corr = sum / (sig1 * sig2);
    return corr;
  }

  void ComputeTransitionMatrix(
      const std::vector<std::vector<uint32_t>>&
          transformed_num_edge_types_walks,
      const std::vector<double>& means) {
    katana::do_all(
        katana::iterate(uint32_t(1), plan_.number_of_edge_types() + 1),
        [&](uint32_t i) {
          for (uint32_t j = 1; j <= plan_.number_of_edge_types(); j++) {
            double pearson_corr =
                pearsonCorr(i, j, transformed_num_edge_types_walks, means);
            double sigmoid = sigmoidCal(pearson_corr);

            transition_matrix_[i][j] = sigmoid;
          }
        });
  }

  void operator()(
      const SortedGraphView& graph,
      katana::InsertBag<std::vector<uint32_t>>* walks) {

    uint32_t iterations = plan_.max_iterations();

    Initialize();

    for (uint32_t iter = 0; iter < iterations; iter++) {
      //E step; generate walks
      katana::InsertBag<std::vector<uint32_t>> types_walks;

      GraphRandomWalk(graph, walks, &types_walks);

      //Update transition matrix
      std::vector<std::vector<uint32_t>> num_edge_types_walks =
          ComputeNumEdgeTypeVectors(types_walks);

      std::vector<std::vector<uint32_t>> transformed_num_edge_types_walks =
          TransformVectors(num_edge_types_walks);

      std::vector<double> means =
          ComputeMeans(transformed_num_edge_types_walks);

      ComputeTransitionMatrix(transformed_num_edge_types_walks, means);
    }
  }
};


}  //namespace

template <typename Algorithm>
static katana::Result<std::vector<std::vector<uint32_t>>>
RandomWalksWithWrap(
    const typename Algorithm::SortedGraphView& graph, RandomWalksPlan plan) {
  katana::ReportPageAllocGuard page_alloc;

  Algorithm algo(plan);

  katana::StatTimer execTime("RandomWalks");
  execTime.start();
  katana::InsertBag<std::vector<uint32_t>> walks;
  algo(graph, &walks);
  execTime.stop();

  std::vector<std::vector<uint32_t>> walks_in_vector;
  walks_in_vector.reserve(plan.number_of_walks());
  std::move(walks.begin(), walks.end(), std::back_inserter(walks_in_vector));
  return walks_in_vector;
}

// TODO(amber): Change return type to return a vector of Opaque GNode type insead
// of uint32_t
katana::Result<std::vector<std::vector<uint32_t>>>
katana::analytics::RandomWalks(PropertyGraph* pg, RandomWalksPlan plan) {
  switch (plan.algorithm()) {
  case RandomWalksPlan::kNode2Vec: {
    auto graph =
        KATANA_CHECKED(Node2VecAlgo::SortedGraphView::Make(pg, {}, {}));
    return RandomWalksWithWrap<Node2VecAlgo>(graph, plan);
  }
  case RandomWalksPlan::kEdge2Vec: {
    TemporaryPropertyGuard tmp_edge_prop{pg->NodeMutablePropertyView()};
    auto graph = KATANA_CHECKED(
        Edge2VecAlgo::SortedGraphView::Make(pg, {}, {tmp_edge_prop.name()}));
    return RandomWalksWithWrap<Edge2VecAlgo>(graph, plan);
  }
  default:
    return ErrorCode::InvalidArgument;
  }
}

/// \cond DO_NOT_DOCUMENT
katana::Result<void>
katana::analytics::RandomWalksAssertValid(
    [[maybe_unused]] katana::PropertyGraph* pg) {
  // TODO(gill): This should have real checks.
  return katana::ResultSuccess();
}
/// \endcond
