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
#include "katana/PerThreadStorage.h"
#include "katana/LargeArray.h"

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
    "maxIterations", cll::desc("Number of iterations"), cll::init(10));

static cll::opt<uint32_t> walkLength(
    "walkLength", cll::desc("Length of random walks (Default: 10)"),
    cll::init(10));

static cll::opt<double> probBack(
    "probBack", cll::desc("Probability of moving back to parent"),
    cll::init(1.0f));

static cll::opt<double> probForward(
    "probForward", cll::desc("Probability of moving forward (2-hops)"),
    cll::init(1.0f));

static cll::opt<double> numWalks(
    "numWalks", cll::desc("number of walk"), cll::init(1));

static cll::opt<uint32_t> numEdgeTypes(
    "numEdgeTypes", cll::desc("Number of edge types"), cll::init(1));

using EdgeWeight = katana::UInt32Property;
using EdgeType = katana::UInt32Property;

using NodeData = std::tuple<>;
using EdgeData = std::tuple<EdgeWeight, EdgeType>;

using EdgeDataToAdd = std::tuple<EdgeType>;

typedef katana::PropertyGraph<NodeData, EdgeData> Graph;
typedef typename Graph::Node GNode;

std::default_random_engine generator;

std::uniform_real_distribution<double> distribution(0.0, 1.0);

double
sigmoidCal(double pears) {
  return 1 / (1 + exp(-pears));  //exact sig
}

double
pearsonCorr(
    uint32_t i, uint32_t j, katana::gstl::Vector<katana::gstl::Vector<uint32_t>>& v,
    katana::gstl::Vector<double>& means) {
  
	katana::gstl::Vector<uint32_t> x = v[i];
  katana::gstl::Vector<uint32_t> y = v[j];

  double sum = 0.0f;
  double sig1 = 0.0f;
  double sig2 = 0.0f;

  for (uint32_t m = 0; m < x.size(); m++) {
    sum += ((double)x[m] - means[i]) * ((double)y[m] - means[j]);
    sig1 += ((double)x[m] - means[i]) * ((double)x[m] - means[i]);
    sig2 += ((double)y[m] - means[j]) * ((double)y[m] - means[j]);
  }

  sum = sum / ((double)x.size());

  sig1 = sig1 / ((double)x.size());
  sig1 = sqrt(sig1);

  sig2 = sig2 / ((double)x.size());
  sig2 = sqrt(sig2);

  double corr = sum / (sig1 * sig2);
  return corr;
}

struct Node2VecAlgo {
  using NodeData = std::tuple<>;
  using EdgeData = std::tuple<>;

  typedef katana::PropertyGraph<NodeData, EdgeData> Graph;
  typedef typename Graph::Node GNode;

  void Initialize(Graph* graph) { katana::gPrint(graph->size()); }

  void HomoGraphWalkAcceptRejectSampling(
      const Graph&
          graph, katana::InsertBag<katana::gstl::Vector<uint32_t>>& walks, katana::LargeArray<uint64_t>& degree) {

    katana::GAccumulator<uint64_t> num;

    katana::PerThreadStorage<std::mt19937> generator;
    katana::PerThreadStorage<std::uniform_real_distribution<double>*>
        distribution;

    for (uint32_t i = 0; i < distribution.size(); i++) {
      *distribution.getRemote(i) =
          new std::uniform_real_distribution<double>(0.0f, 1.0f);
    }

    double upper_bound = 1.0f;

    upper_bound = (upper_bound > (1.0f / probForward)) ? upper_bound
                                                       : (1.0f / probForward);
    upper_bound =
        (upper_bound > (1.0f / probBack)) ? upper_bound : (1.0f / probBack);

    double lower_bound = 1.0f;

    lower_bound = (lower_bound < (1.0f / probForward)) ? lower_bound
                                                       : (1.0f / probForward);
    lower_bound =
        (lower_bound < (1.0f / probBack)) ? lower_bound : (1.0f / probBack);

    katana::do_all(
        katana::iterate(graph),
        [&](GNode n) {
          std::uniform_real_distribution<double>* dist =
              *distribution.getLocal();

          katana::gstl::Vector<uint32_t> walk;
          walk.push_back(n);

          for (uint32_t walk_iter = 1; walk_iter <= walkLength; walk_iter++) {
            if (walk_iter == 1) {
              //random value between 0 and 1
              double prob = (*dist)(*generator.getLocal());

              //Assumption: All edges have weight 1
              uint32_t total_wt = degree[n];

              prob = prob * ((double)total_wt);
              total_wt = 0;
              uint32_t edge_index = std::floor(prob);

              auto edge = graph.edge_begin(n) + edge_index;
              //sample a neighbor of curr
              Graph::Node nbr = *(graph.GetEdgeDest(edge));

              walk.push_back(nbr);

            } else {
              uint32_t curr = walk[walk_iter - 1];
              uint32_t prev = walk[walk_iter - 2];

              uint32_t total_wt = degree[curr];

              //acceptance-rejection sampling
              while (true) {
                //sample x
                double prob = (*dist)(*generator.getLocal());
                prob = prob * ((double)total_wt);

                uint32_t edge_index = std::floor(prob);

                auto edge = graph.edge_begin(curr) + edge_index;
                Graph::Node nbr = *(graph.GetEdgeDest(edge));

                //sample y
                double y = (*dist)(*generator.getLocal());
                y = y * upper_bound;

                if (y <= lower_bound) {
                  //accept this sample
                  walk.push_back(nbr);
                  break;
                } else {
                  //compute transition probability
                  double alpha;
                  if (nbr == prev)
                    alpha = 1.0f / probBack;
                  else if (
                      katana::FindEdgeSortedByDest(graph, prev, nbr) !=
                      graph.edge_end(prev))
                    alpha = 1.0f;
                  else
                    alpha = 1.0f / probForward;

                  if (alpha >= y) {
                    //accept y
                    walk.push_back(nbr);
                    break;
                  }
                }
              }

            }  //end if-else loop
          }

          num += 1;
          walks.push(walk);
        },
        katana::steal(), katana::loopname("loop"));

    katana::gPrint("num: ", num.reduce(), "\n");
  }
  
  void RandomWalks(
      const Graph& graph, katana::InsertBag<katana::gstl::Vector<uint32_t>>& walks, katana::LargeArray<uint64_t>& degree) {
    walks.clear();

    uint32_t num_walks = 0;

    while (num_walks < numWalks) {
      katana::gInfo("Walk No. : ", num_walks);
      num_walks++;
      HomoGraphWalkAcceptRejectSampling(graph, walks, degree);
    }
  }
};

struct Edge2VecAlgo {
  using EdgeType = katana::UInt32Property;

  using NodeData = std::tuple<>;
  using EdgeData = std::tuple<EdgeType>;

  typedef katana::PropertyGraph<NodeData, EdgeData> Graph;
  typedef typename Graph::Node GNode;

  //transition matrix
  katana::gstl::Vector<katana::gstl::Vector<double>> transition_matrix;

  void Initialize() {
    katana::gInfo("Initializing transistion matrix");
    transition_matrix.resize(numEdgeTypes + 1);
    //initialize transition matrix
    for (uint32_t i = 0; i <= numEdgeTypes; i++) {
      for (uint32_t j = 0; j <= numEdgeTypes; j++) {
        transition_matrix[i].push_back(1.0f);
      }
    }
  }

  void HeteroGraphWalkAcceptRejectSampling(const Graph& graph, katana::InsertBag<katana::gstl::Vector<uint32_t>>& walks,  katana::InsertBag<katana::gstl::Vector<uint32_t>>& types_walks,
		  katana::LargeArray<uint64_t>& degree) {

	  katana::PerThreadStorage<std::mt19937> generator;
    katana::PerThreadStorage<std::uniform_real_distribution<double>*>
        distribution;

    for (uint32_t i = 0; i < distribution.size(); i++) {
      *distribution.getRemote(i) =
          new std::uniform_real_distribution<double>(0.0f, 1.0f);
    }

    double upper_bound = 1.0f;

    upper_bound = (upper_bound > (1.0f / probForward)) ? upper_bound
                                                       : (1.0f / probForward);
    upper_bound =
        (upper_bound > (1.0f / probBack)) ? upper_bound : (1.0f / probBack);

    katana::do_all(katana::iterate(graph), [&](GNode n) {
      std::uniform_real_distribution<double>* dist =
              *distribution.getLocal();
		  
     katana::gstl::Vector<uint32_t> walk;
      katana::gstl::Vector<uint32_t> types_vec;

      walk.push_back(n);
      
      for (uint32_t walk_iter = 1; walk_iter <= walkLength; walk_iter++) {
        if (walk_iter == 1) {
          //random value between 0 and 1
          double prob = (*dist)(*generator.getLocal());

          //Assumption: All edges have weight 1
          uint32_t total_wt = degree[n];

          prob = prob * total_wt;
          total_wt = 0;
          uint32_t edge_index = std::floor(prob);

          auto edge = graph.edge_begin(n) + edge_index;
          //sample a neighbor of curr
          Graph::Node nbr = *(graph.GetEdgeDest(edge));
          uint32_t type = graph.GetEdgeData<EdgeType>(edge);

          walk.push_back(nbr);
          types_vec.push_back(type);

        } else {
          uint32_t curr = walk[walk.size() - 1];
          uint32_t prev = walk[walk.size() - 2];

          uint32_t p1 = types_vec.back();  //last element of types_vec

          double total_wt = degree[curr];

   	  //acceptance-rejection sampling
              while (true) {
                //sample x
		
		      double prob = (*dist)(*generator.getLocal());
                prob = prob * ((double)total_wt);

		uint32_t edge_index = std::floor(prob);

		auto edge = graph.edge_begin(curr) + edge_index;
                Graph::Node nbr = *(graph.GetEdgeDest(edge));
		uint32_t p2 = graph.GetEdgeData<EdgeType>(edge);

		//sample y
		double y = (*dist)(*generator.getLocal());
                y = y * upper_bound;

	        //compute transition probability
		double alpha;
                  if (nbr == prev)
                    alpha = 1.0f / probBack;
                  else if (
                      katana::FindEdgeSortedByDest(graph, prev, nbr) !=
                      graph.edge_end(prev))
                    alpha = 1.0f;
                  else
                    alpha = 1.0f / probForward;
  
		  alpha = alpha*transition_matrix[p1][p2];
		  if (alpha >= y) {
			  //accept y
			 walk.push_back(nbr);
          		types_vec.push_back(p2);
	  		break;
		  }
	      }//end while

	}//end if-else loop
      }    //end for

      walks.push(walk);
      types_walks.push(types_vec);
    }, katana::steal(), katana::loopname("loop"));
  }

  void ComputeVectors(
      katana::gstl::Vector<katana::gstl::Vector<uint32_t>>& v,
      katana::InsertBag<katana::gstl::Vector<uint32_t>>& types_walks) {
    katana::InsertBag<katana::gstl::Vector<uint32_t>> bag;

    katana::do_all(katana::iterate(types_walks), [&](katana::gstl::Vector<uint32_t>& walk) {
      katana::gstl::Vector<uint32_t> vec(numEdgeTypes + 1, 0);

      for (auto type : walk)
        vec[type]++;

      bag.push(vec);
    });

    for (auto vec : bag)
      v.push_back(vec);
  }

  void TransformVectors(
      katana::gstl::Vector<katana::gstl::Vector<uint32_t>>& v,
      katana::gstl::Vector<katana::gstl::Vector<uint32_t>>& transformedV) {
    uint32_t rows = v.size();

    for (uint32_t j = 0; j <= numEdgeTypes; j++)
      for (uint32_t i = 0; i < rows; i++) {
        transformedV[j].push_back(v[i][j]);
      }
  }

  void ComputeMeans(
      katana::gstl::Vector<katana::gstl::Vector<uint32_t>>& v, katana::gstl::Vector<double>& means) {
    katana::do_all(
        katana::iterate((uint32_t)1, numEdgeTypes + 1), [&](uint32_t i) {
          uint32_t sum = 0;
          for (auto n : v[i])
            sum += n;

          means[i] = ((double)sum) / (v[i].size());
        });
  }

  void ComputeTransitionMatrix(
      katana::gstl::Vector<katana::gstl::Vector<uint32_t>>& v, katana::gstl::Vector<double>& means) {
    katana::do_all(
        katana::iterate((uint32_t)1, numEdgeTypes + 1), [&](uint32_t i) {
          for (uint32_t j = 1; j <= numEdgeTypes; j++) {
            double pearson_corr = pearsonCorr(i, j, v, means);
            double sigmoid = sigmoidCal(pearson_corr);

            transition_matrix[i][j] = sigmoid;
          }
        });
  }

  void RandomWalks(
      const Graph& graph, katana::InsertBag<katana::gstl::Vector<uint32_t>>& walks, katana::LargeArray<uint64_t>& degree) {
    uint32_t iterations = maxIterations;

    Initialize();

    while (iterations > 0) {
      iterations--;

      katana::gInfo("Iteration : ", iterations);
      //E step; generate walks
      walks.clear();
      katana::InsertBag<katana::gstl::Vector<uint32_t>> types_walks;

      uint32_t num_walks = 0;

      while (num_walks < numWalks) {
        katana::gInfo("Walk No. : ", num_walks);
        HeteroGraphWalkAcceptRejectSampling(graph, walks, types_walks, degree);
	num_walks++;
      }

      //Update transition matrix
      katana::gstl::Vector<katana::gstl::Vector<uint32_t>> v;
      ComputeVectors(v, types_walks);

      katana::gstl::Vector<katana::gstl::Vector<uint32_t>> transformedV(numEdgeTypes + 1);
      TransformVectors(v, transformedV);

      katana::gstl::Vector<double> means(numEdgeTypes + 1);
      ComputeMeans(transformedV, means);

      ComputeTransitionMatrix(transformedV, means);
    }
  }
};

template <typename Graph>
void InitializeDegrees(Graph* graph, katana::LargeArray<uint64_t>* degree) {

	katana::do_all(katana::iterate(*graph),
			[&](GNode n){

				(*degree)[n] = std::distance(graph->edge_begin(n), graph->edge_end(n));
			}, katana::steal());
}

void
PrintWalks(
    const katana::InsertBag<katana::gstl::Vector<uint32_t>>& walks,
    const std::string& output_file) {
  std::ofstream f(output_file);

  for (auto walk : walks) {
    for (auto node : walk)
      f << node << " ";
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

  katana::gInfo("Read ", graph.num_nodes(), " nodes, ", graph.num_edges(), " edges\n");

  katana::InsertBag<katana::gstl::Vector<uint32_t>> walks;

  katana::gInfo("Starting random walks...");
  katana::StatTimer execTime("Timer_0");
  execTime.start();
  katana::LargeArray<uint64_t> degree;

  degree.allocateBlocked(graph.size());
  InitializeDegrees<Graph>(&graph, &degree);
  algo.RandomWalks(graph, walks, degree);

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
