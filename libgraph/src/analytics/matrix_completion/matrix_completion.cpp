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

#include "katana/analytics/matrix_completion/matrix_completion.h"

#include <cmath>
#include <type_traits>
#include <utility>
#include <vector>

#include "katana/AtomicHelpers.h"
#include "katana/AtomicWrapper.h"
#include "katana/Bag.h"
#include "katana/Galois.h"
#include "katana/ParallelSTL.h"
#include "katana/Properties.h"
#include "katana/Reduction.h"
#include "katana/Timer.h"
#include "katana/TypedPropertyGraph.h"
#include "katana/analytics/MatrixCompletionImplementationBase.h"
#include "katana/analytics/Utils.h"

namespace {

using namespace katana::analytics;

#define LATENT_VECTOR_SIZE 20

struct NodeLatentVector
    : public katana::ArrayProperty<
          katana::CopyableAtomic<double>, LATENT_VECTOR_SIZE> {};

struct EdgeWeight : public katana::PODProperty<double> {};

using NodeData = std::tuple<NodeLatentVector>;
using EdgeData = std::tuple<EdgeWeight>;

typedef katana::TypedPropertyGraph<NodeData, EdgeData> Graph;
typedef typename Graph::Node GNode;

size_t kNumItemNodes = 0;
typedef double LatentValue;

struct MatrixCompletionImplementation
    : public katana::analytics::MatrixCompletionImplementationBase<Graph> {
  double SumSquaredError(Graph& graph) {
    // computing Root Mean Square Error
    // Assuming only item nodes have edges
    katana::GAccumulator<double> error;

    katana::do_all(
        katana::iterate(graph.begin(), graph.begin() + kNumItemNodes),
        [&](GNode n) {
          for (auto ii : graph.OutEdges(n)) {
            auto dst = graph.OutEdgeDst(ii);
            double e = PredictionError<NodeLatentVector>(
                graph.GetData<NodeLatentVector>(n),
                graph.GetData<NodeLatentVector>(dst),
                graph.GetEdgeData<EdgeWeight>(ii));
            error += (e * e);
          }
        });
    return error.reduce();
  }

  // Objective: squared loss with weighted-square-norm regularization
  // Updates latent vectors to reduce the error from the edge value.
  template <typename NodeIndex>
  double DoGradientUpdate(
      katana::PropertyReferenceType<NodeIndex> item_latent_vector,
      katana::PropertyReferenceType<NodeIndex> user_latent_vector,
      double lambda, double edge_rating, double step_size) {
    double error = edge_rating - InnerProduct<NodeIndex>(
                                     item_latent_vector, user_latent_vector);
    // Take gradient step to reduce error
    for (int i = 0; i < LATENT_VECTOR_SIZE; i++) {
      double prev_item = item_latent_vector[i];
      double prev_user = user_latent_vector[i];
      katana::atomicAdd(
          item_latent_vector[i],
          step_size * (error * prev_user - lambda * prev_item));
      katana::atomicAdd(
          user_latent_vector[i],
          step_size * (error * prev_item - lambda * prev_user));
    }
    return error;
  }

  struct StepFunction {
    virtual LatentValue StepSize(
        int round, MatrixCompletionPlan plan) const = 0;
    virtual std::string Name() const = 0;
    virtual bool IsBold() const { return false; }
    virtual ~StepFunction() {}
  };

  struct PurdueStepFunction : public StepFunction {
    virtual std::string Name() const { return "Purdue"; }
    virtual LatentValue StepSize(int round, MatrixCompletionPlan plan) const {
      return plan.learningRate() * 1.5 /
             (1.0 + plan.decayRate() * pow(round + 1, 1.5));
    }
  };

  struct IntelStepFunction : public StepFunction {
    virtual std::string Name() const { return "Intel"; }
    virtual LatentValue StepSize(int round, MatrixCompletionPlan plan) const {
      return plan.learningRate() * pow(plan.decayRate(), round);
    }
  };

  struct BottouStepFunction : public StepFunction {
    virtual std::string Name() const { return "Bottou"; }
    virtual LatentValue StepSize(int round, MatrixCompletionPlan plan) const {
      return plan.learningRate() /
             (1.0 + plan.learningRate() * plan.lambda() * round);
    }
  };

  struct InverseStepFunction : public StepFunction {
    virtual std::string Name() const { return "Inverse"; }
    virtual LatentValue StepSize(int round, MatrixCompletionPlan) const {
      return 1.0 / (round + 1);
    }
  };

  struct BoldStepFunction : public StepFunction {
    virtual std::string Name() const { return "Bold"; }
    virtual bool IsBold() const { return true; }
    virtual LatentValue StepSize(int, MatrixCompletionPlan) const {
      return 0.0;
    }
  };

  katana::Result<StepFunction*> NewStepFunction(MatrixCompletionPlan plan) {
    switch (plan.learningRateFunction()) {
    case MatrixCompletionPlan::kIntel:
      return new IntelStepFunction;
    case MatrixCompletionPlan::kPurdue:
      return new PurdueStepFunction;
    case MatrixCompletionPlan::kBottou:
      return new BottouStepFunction;
    case MatrixCompletionPlan::kInverse:
      return new InverseStepFunction;
    case MatrixCompletionPlan::kBold:
      return new BoldStepFunction;
    default:
      return KATANA_ERROR(
          katana::ErrorCode::InvalidArgument, "Unknown step function");
    }
  }

  double CountFlops(size_t nnz, int rounds, int k, MatrixCompletionPlan plan) {
    double flop = 0;
    if (plan.useExactError()) {
      // dotProduct = 2K, square = 1, sum = 1
      flop += nnz * (2.0 * k + 1 + 1);
    } else {
      // Computed during gradient update: square = 1, sum = 1
      flop += nnz * (1 + 1);
    }
    // dotProduct = 2K, gradient = 10K,
    flop += rounds * (nnz * (12.0 * k));
    return flop;
  }

  size_t InitializeGraphData(Graph& graph, MatrixCompletionPlan plan) {
    katana::StatTimer initTimer("InitializeGraph");
    initTimer.start();
    double top = 1.0 / std::sqrt(LATENT_VECTOR_SIZE);
    katana::PerThreadStorage<std::mt19937> gen;

#if __cplusplus >= 201103L
    std::uniform_real_distribution<LatentValue> dist(0, top);
#else
    std::uniform_real<LatentValue> dist(0, top);
#endif
    bool use_det_init = plan.useDetInit();
    bool use_same_latent_vector = plan.useSameLatentVector();

    if (use_det_init) {
      katana::do_all(katana::iterate(graph), [&](GNode n) {
        auto node_latent_vector = graph.GetData<NodeLatentVector>(n);
        auto val = GenVal(n);
        for (int i = 0; i < LATENT_VECTOR_SIZE; i++) {
          node_latent_vector[i] = val;
        }
      });
    } else {
      katana::do_all(katana::iterate(graph), [&](GNode n) {
        auto node_latent_vector = graph.GetData<NodeLatentVector>(n);
        // all threads initialize their assignment with same generator or
        // a thread local one
        if (use_same_latent_vector) {
          std::mt19937 same_gen;
          for (int i = 0; i < LATENT_VECTOR_SIZE; i++) {
            node_latent_vector[i] = dist(same_gen);
          }
        } else {
          for (int i = 0; i < LATENT_VECTOR_SIZE; i++) {
            node_latent_vector[i] = dist(*gen.getLocal());
          }
        }
      });
    }

    auto active_threads = katana::getActiveThreads();
    std::vector<GNode> largest_node_id_per_thread(active_threads);

    katana::on_each([&](unsigned tid, unsigned nthreads) {
      unsigned int block_size = graph.size() / nthreads;
      if ((graph.size() % nthreads) > 0)
        ++block_size;

      GNode start{tid * block_size};
      GNode end{(tid + 1) * block_size};
      if (end.value() > graph.size()) {
        end = GNode{graph.size()};
      }

      largest_node_id_per_thread[tid] = GNode{0ul};
      for (GNode i = start; i < end; ++i) {
        if (graph.OutDegree(i)) {
          if (largest_node_id_per_thread[tid] < i)
            largest_node_id_per_thread[tid] = i;
        }
      }
    });

    GNode largest_node_id{0ul};
    for (uint32_t t = 0; t < active_threads; ++t) {
      if (largest_node_id < largest_node_id_per_thread[t])
        largest_node_id = largest_node_id_per_thread[t];
    }
    size_t num_item_nodes = largest_node_id.value() + 1;

    initTimer.stop();
    return num_item_nodes;
  }
};

// Common function to execute different algorithms till convergence
template <typename Fn>
void
ExecuteUntilConverged(
    const MatrixCompletionImplementation::StepFunction& sf, Graph& graph, Fn fn,
    MatrixCompletionPlan plan, MatrixCompletionImplementation impl) {
  katana::GAccumulator<double> error_accum;
  std::vector<LatentValue> steps(plan.updatesPerEdge());
  LatentValue last = -1.0;
  unsigned delta_round = plan.updatesPerEdge();
  LatentValue rate = plan.learningRate();

  katana::StatTimer executeAlgoTimer("Algorithm Execution Time");
  katana::TimeAccumulator elapsed;
  elapsed.start();

  for (unsigned int round = 0;; round += delta_round) {
    if (plan.fixedRounds() > 0 && round >= plan.fixedRounds())
      break;
    if (plan.fixedRounds() > 0)
      delta_round = std::min(delta_round, plan.fixedRounds() - round);

    for (unsigned i = 0; i < plan.updatesPerEdge(); ++i) {
      // Assume that loss decreases
      if (sf.IsBold())
        steps[i] = i == 0 ? rate : steps[i - 1] * 1.05;
      else
        steps[i] = sf.StepSize(round + i, plan);
    }

    executeAlgoTimer.start();
    fn(&steps[0], round + delta_round,
       plan.useExactError() ? &error_accum : NULL, plan, impl);
    executeAlgoTimer.stop();
    double error = plan.useExactError() ? error_accum.reduce()
                                        : impl.SumSquaredError(graph);

    elapsed.stop();

    elapsed.start();

    if (!impl.IsFinite(error))
      break;
    if (plan.fixedRounds() <= 0 &&
        (round >= plan.maxUpdates() ||
         std::abs((last - error) / last) < plan.tolerance()))
      break;
    if (sf.IsBold()) {
      // Assume that loss decreases first round
      if (last >= 0.0 && last < error)
        rate = steps[delta_round - 1] * 0.5;
      else
        rate = steps[delta_round - 1] * 1.05;
    }
    last = error;
  }
}

class SGDItemsAlgo {
public:
  bool IsSgd() const { return true; }

  std::string Name() const { return "sgdItemsAlgo"; }

  size_t NumItems() const { return kNumItemNodes; }

private:
  struct Execute {
    Graph& graph;
    katana::GAccumulator<unsigned>& edges_visited;

    void operator()(
        LatentValue* steps, int, katana::GAccumulator<double>* error_accum,
        MatrixCompletionPlan plan, MatrixCompletionImplementation impl) {
      const LatentValue step_size = steps[0];
      katana::do_all(
          katana::iterate(graph.begin(), graph.begin() + kNumItemNodes),
          [&](GNode src) {
            for (auto ii : graph.OutEdges(src)) {
              auto dst = graph.OutEdgeDst(ii);
              auto item_latent_vector = graph.GetData<NodeLatentVector>(src);
              auto user_latent_vector = graph.GetData<NodeLatentVector>(dst);
              LatentValue error = impl.DoGradientUpdate<NodeLatentVector>(
                  item_latent_vector, user_latent_vector, plan.lambda(),
                  graph.GetEdgeData<EdgeWeight>(ii), step_size);

              edges_visited += 1;
              if (plan.useExactError())
                *error_accum += error;
            }
          },
          katana::loopname("sgdItemsAlgo"));
    }
  };

public:
  void operator()(
      Graph& graph, const MatrixCompletionImplementation::StepFunction& sf,
      MatrixCompletionPlan plan, MatrixCompletionImplementation impl) {
    katana::GAccumulator<unsigned> edges_visited;

    katana::StatTimer executeTimer("Time");
    executeTimer.start();

    Execute fn{graph, edges_visited};
    ExecuteUntilConverged(sf, graph, fn, plan, impl);

    executeTimer.stop();

    katana::ReportStatSingle(
        "sgdItemsAlgo", "EdgesVisited", edges_visited.reduce());
  }
};

template <typename Algo>
katana::Result<void>
Run(katana::PropertyGraph* pg, MatrixCompletionPlan plan,
    katana::TxnContext* txn_ctx) {
  KATANA_CHECKED(pg->ConstructNodeProperties<NodeData>(txn_ctx));
  Graph graph = KATANA_CHECKED(Graph::Make(pg));

  Algo algo;

  MatrixCompletionImplementation impl{};

  // initialize latent vectors and get number of item nodes
  kNumItemNodes = impl.InitializeGraphData(graph, plan);

  std::unique_ptr<MatrixCompletionImplementation::StepFunction> sf{
      KATANA_CHECKED(impl.NewStepFunction(plan))};

  katana::StatTimer execTime("MatrixCompletion");

  execTime.start();
  algo(graph, *sf, plan, impl);
  execTime.stop();

  return katana::ResultSuccess();
}

}  // namespace

katana::Result<void>
katana::analytics::MatrixCompletion(
    katana::PropertyGraph* pg, katana::TxnContext* txn_ctx,
    MatrixCompletionPlan plan) {
  switch (plan.algorithm()) {
  case MatrixCompletionPlan::kSGDByItems:
    return Run<SGDItemsAlgo>(pg, plan, txn_ctx);
  default:
    return katana::ErrorCode::InvalidArgument;
  }
}
