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

#include <algorithm>
#include <fstream>
#include <iostream>
#include <random>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/generator_iterator.hpp>
#include <boost/iterator/transform_iterator.hpp>
#include <boost/random/uniform_real.hpp>
#include <boost/random/variate_generator.hpp>

#include "Lonestar/BoilerPlate.h"
#include "katana/LargeArray.h"
#include "katana/PerThreadStorage.h"

const char* name = "RandomWalks";
const char* desc = "Find paths by random walks on the graph";

enum Algo { node2vec, edge2vec };

namespace cll = llvm::cl;

static cll::opt<std::string> inputFile(
    cll::Positional, cll::desc("<input file>"), cll::Required);

static cll::opt<std::string> outputFile(
    "outputFile", cll::desc("File name to output walks (Default: walks.txt)"),
    cll::init("walks.txt"));

static cll::opt<Algo> algo(
    "algo", cll::desc("Choose an algorithm:"),
    cll::values(
        clEnumValN(Algo::node2vec, "Node2vec", "Node2vec random walks"),
        clEnumValN(Algo::edge2vec, "Edge2vec", "Heterogeneous Edge2vec ")),
    cll::init(Algo::node2vec));

static cll::opt<uint32_t> maxIterations(
    "maxIterations", cll::desc("Number of iterations for Edge2vec algorithm"),
    cll::init(10));

static cll::opt<uint32_t> walkLength(
    "walkLength", cll::desc("Length of random walks (Default: 10)"),
    cll::init(10));

static cll::opt<double> probBack(
    "probBack", cll::desc("Probability of moving back to parent"),
    cll::init(1.0));

static cll::opt<double> probForward(
    "probForward", cll::desc("Probability of moving forward (2-hops)"),
    cll::init(1.0));

static cll::opt<double> numWalks(
    "numWalks", cll::desc("Number of walks"), cll::init(1));

static cll::opt<uint32_t> numEdgeTypes(
    "numEdgeTypes", cll::desc("Number of edge types (only for Edge2Vec)"),
    cll::init(1));

using EdgeWeight = katana::UInt32Property;
using EdgeType = katana::UInt32Property;

using NodeData = std::tuple<>;
using EdgeData = std::tuple<EdgeWeight, EdgeType>;

using EdgeDataToAdd = std::tuple<EdgeType>;

typedef katana::PropertyGraph<NodeData, EdgeData> Graph;
typedef typename Graph::Node GNode;

constexpr static unsigned kChunkSize = 1U;

struct Node2VecAlgo {
  using NodeData = std::tuple<>;
  using EdgeData = std::tuple<>;

  typedef katana::PropertyGraph<NodeData, EdgeData> Graph;
  typedef typename Graph::Node GNode;

  void FindSampleNeighbor(
      const Graph& graph, const GNode& n,
      const katana::LargeArray<uint64_t>& degree, double prob, GNode* nbr) {
    double total_wt = degree[n];
    prob = prob * total_wt;

    uint32_t edge_index = std::floor(prob);
    auto edge = graph.edge_begin(n) + edge_index;
    *nbr = *(graph.GetEdgeDest(edge));
  }

  void GraphRandomWalk(
      const Graph& graph,
      katana::InsertBag<katana::gstl::Vector<uint32_t>>* walks,
      const katana::LargeArray<uint64_t>& degree) {
    katana::PerThreadStorage<std::mt19937> generator;
    katana::PerThreadStorage<std::uniform_real_distribution<double>*>
        distribution;

    for (uint32_t i = 0; i < distribution.size(); i++) {
      *distribution.getRemote(i) =
          new std::uniform_real_distribution<double>(0.0, 1.0);
    }

    double prob_forward = 1.0 / probForward;
    double prob_backward = 1.0 / probBack;

    double upper_bound = 1.0;

    upper_bound = (upper_bound > prob_forward) ? upper_bound : prob_forward;
    upper_bound = (upper_bound > prob_backward) ? upper_bound : prob_backward;

    double lower_bound = 1.0;

    lower_bound = (lower_bound < prob_forward) ? lower_bound : prob_forward;
    lower_bound = (lower_bound < prob_backward) ? lower_bound : prob_backward;

    uint32_t total_walks = graph.size() * numWalks;

    katana::do_all(
        katana::iterate((uint32_t)0, total_walks),
        [&](uint32_t idx) {
          GNode n = idx % graph.size();

          std::uniform_real_distribution<double>* dist =
              *distribution.getLocal();

          katana::gstl::Vector<uint32_t> walk;
          walk.push_back(n);

          //random value between 0 and 1
          double prob = (*dist)(*generator.getLocal());

          //Assumption: All edges have weight 1
          Graph::Node nbr;
          FindSampleNeighbor(graph, n, degree, prob, &nbr);

          walk.push_back(std::move(nbr));

          for (uint32_t walk_iter = 2; walk_iter <= walkLength; walk_iter++) {
            uint32_t curr = walk[walk_iter - 1];
            uint32_t prev = walk[walk_iter - 2];

            //acceptance-rejection sampling
            while (true) {
              //sample x
              double prob = (*dist)(*generator.getLocal());

              Graph::Node nbr;
              FindSampleNeighbor(graph, curr, degree, prob, &nbr);

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

                if (alpha >= y) {
                  //accept y
                  walk.push_back(std::move(nbr));
                  break;
                }
              }
            }
          }

          (*walks).push(std::move(walk));
        },
        katana::steal(), katana::chunk_size<kChunkSize>(),
        katana::loopname("node2vec-walks"));

    for (uint32_t i = 0; i < distribution.size(); i++) {
      delete (*distribution.getRemote(i));
    }
  }

  void RandomWalks(
      const Graph& graph,
      katana::InsertBag<katana::gstl::Vector<uint32_t>>* walks,
      const katana::LargeArray<uint64_t>& degree) {
    GraphRandomWalk(graph, walks, degree);
  }
};

struct Edge2VecAlgo {
  using EdgeType = katana::UInt32Property;

  using NodeData = std::tuple<>;
  using EdgeData = std::tuple<EdgeType>;

  typedef katana::PropertyGraph<NodeData, EdgeData> Graph;
  typedef typename Graph::Node GNode;

  //transition matrix
  katana::gstl::Vector<katana::gstl::Vector<double>> transition_matrix_;

  void Initialize() {
    transition_matrix_.resize(numEdgeTypes + 1);
    //initialize transition matrix
    for (uint32_t i = 0; i <= numEdgeTypes; i++) {
      for (uint32_t j = 0; j <= numEdgeTypes; j++) {
        transition_matrix_[i].push_back(1.0);
      }
    }
  }

  void FindSampleNeighbor(
      const Graph& graph, const GNode& n,
      const katana::LargeArray<uint64_t>& degree, double prob, GNode* nbr,
      uint32_t* type) {
    double total_wt = degree[n];
    prob = prob * total_wt;

    uint32_t edge_index = std::floor(prob);
    auto edge = graph.edge_begin(n) + edge_index;
    *nbr = *(graph.GetEdgeDest(edge));
    *type = graph.GetEdgeData<EdgeType>(edge);
  }

  void GraphRandomWalk(
      const Graph& graph,
      katana::InsertBag<katana::gstl::Vector<uint32_t>>* walks,
      katana::InsertBag<katana::gstl::Vector<uint32_t>>* types_walks,
      const katana::LargeArray<uint64_t>& degree) {
    katana::PerThreadStorage<std::mt19937> generator;
    katana::PerThreadStorage<std::uniform_real_distribution<double>*>
        distribution;

    for (uint32_t i = 0; i < distribution.size(); i++) {
      *distribution.getRemote(i) =
          new std::uniform_real_distribution<double>(0.0, 1.0);
    }

    double prob_forward = 1.0 / probForward;
    double prob_backward = 1.0 / probBack;

    double upper_bound = 1.0;

    upper_bound = (upper_bound > prob_forward) ? upper_bound : prob_forward;
    upper_bound = (upper_bound > prob_backward) ? upper_bound : prob_backward;

    uint32_t total_walks = graph.size() * numWalks;

    katana::do_all(
        katana::iterate((uint32_t)0, total_walks),
        [&](uint32_t idx) {
          GNode n = idx % graph.size();

          std::uniform_real_distribution<double>* dist =
              *distribution.getLocal();

          katana::gstl::Vector<uint32_t> walk;
          katana::gstl::Vector<uint32_t> types_vec;

          walk.push_back(n);

          //random value between 0 and 1
          double prob = (*dist)(*generator.getLocal());

          //Assumption: All edges have weight 1
          Graph::Node nbr;
          uint32_t type_id;
          FindSampleNeighbor(graph, n, degree, prob, &nbr, &type_id);

          walk.push_back(std::move(nbr));
          types_vec.push_back(type_id);

          for (uint32_t walk_iter = 2; walk_iter <= walkLength; walk_iter++) {
            uint32_t curr = walk[walk.size() - 1];
            uint32_t prev = walk[walk.size() - 2];

            uint32_t p1 = types_vec.back();  //last element of types_vec

            //acceptance-rejection sampling
            while (true) {
              //sample x

              double prob = (*dist)(*generator.getLocal());

              Graph::Node nbr;
              uint32_t p2;
              FindSampleNeighbor(graph, curr, degree, prob, &nbr, &p2);

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
        katana::steal(), katana::chunk_size<kChunkSize>(),
        katana::loopname("edge2vec-loops"));
  }

  //compute the histogram of edge types for each walk
  katana::gstl::Vector<katana::gstl::Vector<uint32_t>>
  ComputeNumEdgeTypeVectors(
      const katana::InsertBag<katana::gstl::Vector<uint32_t>>& types_walks) {
    katana::gstl::Vector<katana::gstl::Vector<uint32_t>> num_edge_types_walks;

    katana::PerThreadStorage<
        katana::gstl::Vector<katana::gstl::Vector<uint32_t>>>
        per_thread_num_edge_types_walks;
    katana::do_all(
        katana::iterate(types_walks),
        [&](const katana::gstl::Vector<uint32_t>& types_walk) {
          katana::gstl::Vector<uint32_t> num_edge_types(numEdgeTypes + 1, 0);

          for (auto type : types_walk) {
            num_edge_types[type]++;
          }

          (*per_thread_num_edge_types_walks.getLocal())
              .emplace_back(std::move(num_edge_types));
        });

    for (unsigned j = 0; j < katana::getActiveThreads(); ++j) {
      for (auto num_edge_types :
           *per_thread_num_edge_types_walks.getRemote(j)) {
        num_edge_types_walks.push_back(std::move(num_edge_types));
      }
    }

    return num_edge_types_walks;
  }

  katana::gstl::Vector<katana::gstl::Vector<uint32_t>> TransformVectors(
      const katana::gstl::Vector<katana::gstl::Vector<uint32_t>>&
          num_edge_types_walks) {
    uint32_t rows = num_edge_types_walks.size();
    katana::gstl::Vector<katana::gstl::Vector<uint32_t>>
        transformed_num_edge_types_walks;
    transformed_num_edge_types_walks.resize(numEdgeTypes + 1);

    katana::do_all(
        katana::iterate((uint32_t)0, numEdgeTypes + 1), [&](uint32_t j) {
          for (uint32_t i = 0; i < rows; i++) {
            transformed_num_edge_types_walks[j].push_back(
                num_edge_types_walks[i][j]);
          }
        });

    return transformed_num_edge_types_walks;
  }

  katana::gstl::Vector<double> ComputeMeans(
      const katana::gstl::Vector<katana::gstl::Vector<uint32_t>>&
          transformed_num_edge_types_walks) {
    katana::gstl::Vector<double> means(numEdgeTypes + 1);

    for (uint32_t i = 1; i <= numEdgeTypes; i++) {
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
      const katana::gstl::Vector<katana::gstl::Vector<uint32_t>>&
          transformed_num_edge_types_walks,
      const katana::gstl::Vector<double>& means) {
    katana::gstl::Vector<uint32_t> x = transformed_num_edge_types_walks[i];
    katana::gstl::Vector<uint32_t> y = transformed_num_edge_types_walks[j];

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
      const katana::gstl::Vector<katana::gstl::Vector<uint32_t>>&
          transformed_num_edge_types_walks,
      const katana::gstl::Vector<double>& means) {
    katana::do_all(
        katana::iterate((uint32_t)1, numEdgeTypes + 1), [&](uint32_t i) {
          for (uint32_t j = 1; j <= numEdgeTypes; j++) {
            double pearson_corr =
                pearsonCorr(i, j, transformed_num_edge_types_walks, means);
            double sigmoid = sigmoidCal(pearson_corr);

            transition_matrix_[i][j] = sigmoid;
          }
        });
  }

  void RandomWalks(
      const Graph& graph,
      katana::InsertBag<katana::gstl::Vector<uint32_t>>* walks,
      const katana::LargeArray<uint64_t>& degree) {
    uint32_t iterations = maxIterations;

    Initialize();

    for (uint32_t iter = 0; iter < iterations; iter++) {
      //E step; generate walks
      katana::InsertBag<katana::gstl::Vector<uint32_t>> types_walks;

      GraphRandomWalk(graph, walks, &types_walks, degree);

      //Update transition matrix
      katana::gstl::Vector<katana::gstl::Vector<uint32_t>>
          num_edge_types_walks = ComputeNumEdgeTypeVectors(types_walks);

      katana::gstl::Vector<katana::gstl::Vector<uint32_t>>
          transformed_num_edge_types_walks =
              TransformVectors(num_edge_types_walks);

      katana::gstl::Vector<double> means =
          ComputeMeans(transformed_num_edge_types_walks);

      ComputeTransitionMatrix(transformed_num_edge_types_walks, means);
    }
  }
};

template <typename Graph>
void
InitializeDegrees(const Graph& graph, katana::LargeArray<uint64_t>* degree) {
  katana::do_all(
      katana::iterate(graph),
      [&](GNode n) {
        (*degree)[n] = std::distance(graph.edge_begin(n), graph.edge_end(n));
      },
      katana::steal());
}

void
PrintWalks(
    const katana::InsertBag<katana::gstl::Vector<uint32_t>>& walks,
    const std::string& output_file) {
  std::ofstream f(output_file);

  for (auto walk : walks) {
    for (auto node : walk) {
      f << node << " ";
    }
    f << std::endl;
  }
}

template <typename Algo>
void
run() {
  using Graph = typename Algo::Graph;

  Algo algo;

  katana::gInfo("Reading from file: ", inputFile, "\n");
  std::unique_ptr<katana::PropertyFileGraph> pfg =
      MakeFileGraph(inputFile, edge_property_name);

  auto pg_result = katana::PropertyGraph<
      typename Algo::NodeData, typename Algo::EdgeData>::Make(pfg.get());

  if (!pg_result) {
    KATANA_LOG_FATAL("could not make property graph: {}", pg_result.error());
  }

  auto res = katana::SortAllEdgesByDest(pfg.get());
  if (!res) {
    KATANA_LOG_FATAL("Sorting property file graph failed: {}", res.error());
  }

  Graph graph = pg_result.value();

  katana::gInfo(
      "Read ", graph.num_nodes(), " nodes, ", graph.num_edges(), " edges\n");

  katana::gPrint("size: ", graph.size(), "\n");

  katana::InsertBag<katana::gstl::Vector<uint32_t>> walks;

  katana::gInfo("Starting random walks...");
  katana::StatTimer execTime("Timer_0");
  execTime.start();

  katana::LargeArray<uint64_t> degree;
  degree.allocateBlocked(graph.size());
  InitializeDegrees<Graph>(graph, &degree);

  algo.RandomWalks(graph, &walks, degree);

  degree.destroy();
  degree.deallocate();

  execTime.stop();

  if (output) {
    std::string output_file = outputLocation + "/" + outputFile;
    katana::gInfo("Writing random walks to a file: ", output_file);
    PrintWalks(walks, output_file);
  }
}

int
main(int argc, char** argv) {
  std::unique_ptr<katana::SharedMemSys> G =
      LonestarStart(argc, argv, name, desc, nullptr, &inputFile);

  katana::StatTimer totalTime("TimerTotal");
  totalTime.start();

  if (!symmetricGraph) {
    KATANA_DIE(
        "This application requires a symmetric graph input;"
        " please use the -symmetricGraph flag "
        " to indicate the input is a symmetric graph.");
  }

  katana::gInfo("Only considering unweighted graph currently");

  switch (algo) {
  case Algo::node2vec:
    run<Node2VecAlgo>();
    break;
  case Algo::edge2vec:
    run<Edge2VecAlgo>();
    break;
  default:
    std::cerr << "Unknown algorithm\n";
    abort();
  }

  return 0;
}
