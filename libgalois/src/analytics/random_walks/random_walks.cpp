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

struct Node2VecAlgo {
  using NodeData = std::tuple<>;
  using EdgeData = std::tuple<>;

  typedef katana::TypedPropertyGraph<NodeData, EdgeData> Graph;
  typedef typename Graph::Node GNode;

  const RandomWalksPlan& plan_;
  Node2VecAlgo(const RandomWalksPlan& plan) : plan_(plan) {}

  GNode FindSampleNeighbor(
      const Graph& graph, const GNode& n,
      const katana::NUMAArray<uint64_t>& degree, double prob) {
    if (degree[n] == 0) {
      return graph.num_nodes();
    }
    double total_wt = degree[n];

    uint32_t edge_index = std::floor(prob * total_wt);
    auto edge = graph.edge_begin(n) + edge_index;
    return *graph.GetEdgeDest(edge);
  }

  void GraphRandomWalk(
      const Graph& graph, katana::InsertBag<std::vector<uint32_t>>* walks,
      const katana::NUMAArray<uint64_t>& degree) {
    katana::PerThreadStorage<std::mt19937> generator;
    katana::PerThreadStorage<std::uniform_real_distribution<double>*>
        distribution;

    for (uint32_t i = 0; i < distribution.size(); i++) {
      *distribution.getRemote(i) =
          new std::uniform_real_distribution<double>(0.0, 1.0);
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
          GNode n = idx % graph.size();

          //check if n has no neighbor
          if (degree[n] == 0) {
            return;
          }

          std::uniform_real_distribution<double>* dist =
              *distribution.getLocal();

          std::vector<uint32_t> walk;
          walk.push_back(n);

          //random value between 0 and 1
          double prob = (*dist)(*generator.getLocal());

          //Assumption: All edges have weight 1
          Graph::Node nbr = FindSampleNeighbor(graph, n, degree, prob);
          KATANA_LOG_ASSERT(nbr < graph.num_nodes());

          walk.push_back(std::move(nbr));

          for (uint32_t current_walk = 2; current_walk <= plan_.walk_length();
               current_walk++) {
            uint32_t curr = walk[current_walk - 1];
            uint32_t prev = walk[current_walk - 2];

            //check if n has no neighbor
            if (degree[curr] == 0) {
              break;
            }
            //acceptance-rejection sampling
            while (true) {
              //sample x
              double prob = (*dist)(*generator.getLocal());

              Graph::Node nbr = FindSampleNeighbor(graph, curr, degree, prob);
              KATANA_LOG_ASSERT(nbr < graph.num_nodes());

              //sample y
              double y = (*dist)(*generator.getLocal());
              y = y * upper_bound;

              if (y <= lower_bound) {
                //accept this sample
                walk.push_back(std::move(nbr));
                break;
              } else {
                //compute transition probability
                double alpha;

                //check if nbr is same as the previous node on this walk
                if (nbr == prev) {
                  alpha = prob_backward;
                }  //check if nbr is also a neighbor of the previous node on this walk
                else if (
                    katana::FindEdgeSortedByDest(graph, prev, nbr) !=
                    graph.edge_end(prev)) {
                  alpha = 1.0;
                } else {
                  alpha = prob_forward;
                }

                if (y <= alpha) {
                  //accept y
                  walk.push_back(std::move(nbr));
                  break;
                }
              }
            }
          }

          walks->push(std::move(walk));
        },
        katana::steal(), katana::chunk_size<RandomWalksPlan::kChunkSize>(),
        katana::loopname("Node2vec walks"), katana::no_stats());

    for (uint32_t i = 0; i < distribution.size(); i++) {
      delete (*distribution.getRemote(i));
    }
  }

  void operator()(
      const Graph& graph, katana::InsertBag<std::vector<uint32_t>>* walks,
      const katana::NUMAArray<uint64_t>& degree) {
    GraphRandomWalk(graph, walks, degree);
  }
};

struct Edge2VecAlgo {
  using EdgeType = katana::UInt32Property;

  using NodeData = std::tuple<>;
  using EdgeData = std::tuple<EdgeType>;

  typedef katana::TypedPropertyGraph<NodeData, EdgeData> Graph;
  typedef typename Graph::Node GNode;

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
      const Graph& graph, const GNode& n,
      const katana::NUMAArray<uint64_t>& degree, double prob) {
    if (degree[n] == 0) {
      return std::make_pair(graph.num_nodes(), 1);
    }
    double total_wt = degree[n];
    prob = prob * total_wt;

    uint32_t edge_index = std::floor(prob);
    auto edge = graph.edge_begin(n) + edge_index;
    return std::make_pair(
        *graph.GetEdgeDest(edge), graph.GetEdgeData<EdgeType>(edge));
  }

  void GraphRandomWalk(
      const Graph& graph, katana::InsertBag<std::vector<uint32_t>>* walks,
      katana::InsertBag<std::vector<uint32_t>>* types_walks,
      const katana::NUMAArray<uint64_t>& degree) {
    katana::PerThreadStorage<std::mt19937> generator;
    katana::PerThreadStorage<std::uniform_real_distribution<double>*>
        distribution;

    for (uint32_t i = 0; i < distribution.size(); i++) {
      *distribution.getRemote(i) =
          new std::uniform_real_distribution<double>(0.0, 1.0);
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
          GNode n = idx % graph.size();

          //check if n has no neighbor
          if (degree[n] == 0) {
            return;
          }

          std::uniform_real_distribution<double>* dist =
              *distribution.getLocal();

          std::vector<uint32_t> walk;
          std::vector<uint32_t> types_vec;

          walk.push_back(n);

          //random value between 0 and 1
          double prob = (*dist)(*generator.getLocal());

          //Assumption: All edges have weight 1
          auto nbr_pair = FindSampleNeighbor(graph, n, degree, prob);
          KATANA_LOG_ASSERT(nbr_pair.first < graph.num_nodes());

          walk.push_back(std::move(nbr_pair.first));
          types_vec.push_back(nbr_pair.second);

          for (uint32_t current_walk = 2; current_walk <= plan_.walk_length();
               current_walk++) {
            uint32_t curr = walk[walk.size() - 1];
            //check if n has no neighbor
            if (degree[curr] == 0) {
              return;
            }
            uint32_t prev = walk[walk.size() - 2];

            uint32_t p1 = types_vec.back();  //last element of types_vec

            //acceptance-rejection sampling
            while (true) {
              //sample x
              double prob = (*dist)(*generator.getLocal());

              auto nbr_type_pair =
                  FindSampleNeighbor(graph, curr, degree, prob);
              KATANA_LOG_ASSERT(nbr_pair.first < graph.num_nodes());

              Graph::Node nbr = nbr_type_pair.first;
              EdgeType::ViewType::value_type p2 = nbr_type_pair.second;

              //sample y
              double y = (*dist)(*generator.getLocal());
              y = y * upper_bound;

              //compute transition probability
              double alpha;

              //check if nbr is same as the previous node on this walk
              if (nbr == prev) {
                alpha = prob_backward;
              }  //check if nbr is also a neighbor of the previous node on this walk
              else if (
                  katana::FindEdgeSortedByDest(graph, prev, nbr) !=
                  graph.edge_end(prev)) {
                alpha = 1.0;
              } else {
                alpha = prob_forward;
              }

              alpha = alpha * transition_matrix_[p1][p2];
              if (alpha >= y) {
                //accept y
                walk.push_back(std::move(nbr));
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
      const Graph& graph, katana::InsertBag<std::vector<uint32_t>>* walks,
      const katana::NUMAArray<uint64_t>& degree) {
    uint32_t iterations = plan_.max_iterations();

    Initialize();

    for (uint32_t iter = 0; iter < iterations; iter++) {
      //E step; generate walks
      katana::InsertBag<std::vector<uint32_t>> types_walks;

      GraphRandomWalk(graph, walks, &types_walks, degree);

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

template <typename Graph>
void
InitializeDegrees(const Graph& graph, katana::NUMAArray<uint64_t>* degree) {
  katana::do_all(katana::iterate(graph), [&](typename Graph::Node n) {
    // Treat this as O(1) time because subtracting iterators is just pointer
    // or number subtraction. So don't use steal().
    (*degree)[n] = graph.edges(n).size();
  });
}

}  //namespace

template <typename Algorithm>
static katana::Result<std::vector<std::vector<uint32_t>>>
RandomWalksWithWrap(katana::PropertyGraph* pg, RandomWalksPlan plan) {
  katana::ReportPageAllocGuard page_alloc;

  if (auto res = katana::SortAllEdgesByDest(pg); !res) {
    return res.error();
  }

  // TODO(amp): This is incorrect. For Node2vec this needs to be:
  //    Algorithm::Graph::Make(pg, {}, {}) // Ignoring all properties.
  //  For Edge2vec this needs to be:
  //    Algorithm::Graph::Make(pg, {}, {edge_type_property_name})
  //  The current version requires the input to have exactly the properties
  //  expected by the algorithm implementation.
  auto pg_result = Algorithm::Graph::Make(pg);
  if (!pg_result) {
    return pg_result.error();
  }

  auto graph = pg_result.value();

  Algorithm algo(plan);

  katana::NUMAArray<uint64_t> degree;
  degree.allocateBlocked(graph.size());
  InitializeDegrees<typename Algorithm::Graph>(graph, &degree);

  katana::StatTimer execTime("RandomWalks");
  execTime.start();
  katana::InsertBag<std::vector<uint32_t>> walks;
  algo(graph, &walks, degree);
  execTime.stop();

  degree.destroy();
  degree.deallocate();

  std::vector<std::vector<uint32_t>> walks_in_vector;
  walks_in_vector.reserve(plan.number_of_walks());
  std::move(walks.begin(), walks.end(), std::back_inserter(walks_in_vector));
  return walks_in_vector;
}

katana::Result<std::vector<std::vector<uint32_t>>>
katana::analytics::RandomWalks(PropertyGraph* pg, RandomWalksPlan plan) {
  switch (plan.algorithm()) {
  case RandomWalksPlan::kNode2Vec:
    return RandomWalksWithWrap<Node2VecAlgo>(pg, plan);
  case RandomWalksPlan::kEdge2Vec:
    return RandomWalksWithWrap<Edge2VecAlgo>(pg, plan);
  default:
    return ErrorCode::InvalidArgument;
  }
}

/// \cond DO_NOT_DOCUMENT
katana::Result<void>
katana::analytics::RandomWalksAssertValid([
    [maybe_unused]] katana::PropertyGraph* pg) {
  // TODO(gill): This should have real checks.
  return katana::ResultSuccess();
}
/// \endcond
