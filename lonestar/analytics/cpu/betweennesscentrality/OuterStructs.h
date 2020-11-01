#ifndef GALOIS_BC_OUTER
#define GALOIS_BC_OUTER

#include <fstream>
#include <iomanip>

#include <boost/iterator/filter_iterator.hpp>

#include "Lonestar/BoilerPlate.h"
#include "galois/Galois.h"

using NodeDataOuter = std::tuple<>;
using EdgeDataOuter = std::tuple<>;

typedef galois::graphs::PropertyGraph<NodeDataOuter, EdgeDataOuter> OuterGraph;
typedef typename OuterGraph::Node OuterGNode;

////////////////////////////////////////////////////////////////////////////////

class BCOuter {
  const OuterGraph& graph_;
  int num_nodes_;

  galois::substrate::PerThreadStorage<double*>
      centrality_measure_;  // betweeness measure
  galois::substrate::PerThreadStorage<double*> per_thread_sigma_;
  galois::substrate::PerThreadStorage<int*> per_thread_distance_;
  galois::substrate::PerThreadStorage<double*> per_thread_delta_;
  galois::substrate::PerThreadStorage<galois::gdeque<OuterGNode>*>
      per_thread_successor_;

public:
  /**
   * Constructor initializes thread local storage.
   */
  BCOuter(const OuterGraph& g) : graph_(g), num_nodes_(g.num_nodes()) {
    InitializeLocal();
  }

  /**
   * Constructor destroys thread local storage.
   */
  ~BCOuter(void) { DeleteLocal(); }

  //! Function that does BC for a single source; called by a thread
  void ComputeBC(const OuterGNode current_source) {
    galois::gdeque<OuterGNode> source_queue;

    double* sigma = *per_thread_sigma_.getLocal();
    int* distance = *per_thread_distance_.getLocal();
    double* delta = *per_thread_delta_.getLocal();
    galois::gdeque<OuterGNode>* successor = *per_thread_successor_.getLocal();

    sigma[current_source] = 1;
    distance[current_source] = 1;

    source_queue.push_back(current_source);

    // Do bfs while computing number of shortest paths (saved into sigma)
    // and successors of nodes;
    // Note this bfs makes it so source has distance of 1 instead of 0
    for (auto qq = source_queue.begin(), eq = source_queue.end(); qq != eq;
         ++qq) {
      int src = *qq;

      for (auto edge : graph_.edges(src)) {
        auto dest = graph_.GetEdgeDest(edge);

        if (!distance[*dest]) {
          source_queue.push_back(*dest);
          distance[*dest] = distance[src] + 1;
        }

        if (distance[*dest] == distance[src] + 1) {
          sigma[*dest] = sigma[*dest] + sigma[src];
          successor[src].push_back(*dest);
        }
      }
    }

    // Back-propogate the dependency values (delta) along the BFS DAG
    // ignore the source (hence source_queue.size > 1 and not source_queue.empty)
    while (source_queue.size() > 1) {
      int leaf = source_queue.back();
      source_queue.pop_back();

      double sigma_leaf = sigma[leaf];  // has finalized short path value
      double delta_leaf = delta[leaf];
      auto& succ_list = successor[leaf];

      for (auto successor = succ_list.begin(), succ_end = succ_list.end();
           successor != succ_end; ++successor) {
        delta_leaf +=
            (sigma_leaf / sigma[*successor]) * (1.0 + delta[*successor]);
      }
      delta[leaf] = delta_leaf;
    }

    // save result of this source's BC, reset all local values for next
    // source
    double* Vec = *centrality_measure_.getLocal();
    for (int i = 0; i < num_nodes_; ++i) {
      Vec[i] += delta[i];
      delta[i] = 0;
      sigma[i] = 0;
      distance[i] = 0;
      successor[i].clear();
    }
  }

  /**
   * Runs betweeness-centrality proper. Instead of a vector of sources,
   * it will operate on the first num_sources sources.
   *
   * @param num_sources Num sources to get BC contribution for
   */
  void RunAll(unsigned num_sources) {
    // Each thread works on an individual source node
    galois::do_all(
        galois::iterate(0u, num_sources),
        [&](const OuterGNode& current_source) { ComputeBC(current_source); },
        galois::steal(), galois::loopname("Main"));
  }

  /**
   * Runs betweeness-centrality proper.
   *
   * @tparam Cont type of the data structure that holds the nodes to treat
   * as a source during betweeness-centrality.
   *
   * @param source_vector Data structure that holds nodes to treat as a source during
   * betweeness-centrality
   */
  template <typename Cont>
  void Run(const Cont& source_vector) {
    // Each thread works on an individual source node
    galois::do_all(
        galois::iterate(source_vector),
        [&](const OuterGNode& current_source) { ComputeBC(current_source); },
        galois::steal(), galois::loopname("Main"));
  }

  /**
   * Verification for reference torus graph inputs.
   * All nodes should have the same betweenness value up to
   * some tolerance.
   */
  void verify() {
    double sample_bc = 0.0;
    bool first_time = true;
    for (int i = 0; i < num_nodes_; ++i) {
      double bc = (*centrality_measure_.getRemote(0))[i];

      for (unsigned j = 1; j < galois::getActiveThreads(); ++j)
        bc += (*centrality_measure_.getRemote(j))[i];

      if (first_time) {
        sample_bc = bc;
        galois::gInfo("BC: ", sample_bc);
        first_time = false;
      } else {
        // check if over some tolerance value
        if ((bc - sample_bc) > 0.0001) {
          galois::gInfo(
              "If torus graph, verification failed ", (bc - sample_bc));
          return;
        }
      }
    }
  }

  /**
   * Print betweeness-centrality measures.
   *
   * @param begin first node to print BC measure of
   * @param end iterator after last node to print
   * @param out stream to output to
   * @param precision precision of the floating points outputted by the function
   */
  void PrintBCValues(
      size_t begin, size_t end, std::ostream& out, int precision = 6) {
    for (; begin != end; ++begin) {
      double bc = (*centrality_measure_.getRemote(0))[begin];

      for (unsigned j = 1; j < galois::getActiveThreads(); ++j)
        bc += (*centrality_measure_.getRemote(j))[begin];

      out << begin << " " << std::setiosflags(std::ios::fixed)
          << std::setprecision(precision) << bc << "\n";
    }
  }

  /**
   * Print all betweeness centrality values in the graph.
   */
  void PrintBCcertificate() {
    std::stringstream foutname;
    foutname << "outer_certificate_" << galois::getActiveThreads();

    std::ofstream outf(foutname.str().c_str());
    galois::gInfo("Writing certificate...");

    PrintBCValues(0, num_nodes_, outf, 9);

    outf.close();
  }

  //! sanity check of BC values
  void OuterSanity(const OuterGraph& graph) {
    galois::GReduceMax<float> accum_max;
    galois::GReduceMin<float> accum_min;
    galois::GAccumulator<float> accum_sum;
    accum_max.reset();
    accum_min.reset();
    accum_sum.reset();

    // get max, min, sum of BC values using accumulators and reducers
    galois::do_all(
        galois::iterate(graph),
        [&](LevelGNode n) {
          double bc = (*centrality_measure_.getRemote(0))[n];

          for (unsigned j = 1; j < galois::getActiveThreads(); ++j)
            bc += (*centrality_measure_.getRemote(j))[n];

          accum_max.update(bc);
          accum_min.update(bc);
          accum_sum += bc;
        },
        galois::no_stats(), galois::loopname("OuterSanity"));

    galois::gPrint("Max BC is ", accum_max.reduce(), "\n");
    galois::gPrint("Min BC is ", accum_min.reduce(), "\n");
    galois::gPrint("BC sum is ", accum_sum.reduce(), "\n");
  }

private:
  /**
   * Initialize an array at some provided address.
   *
   * @param addr Address to initialize array at
   */
  template <typename T>
  void InitArray(T** addr) {
    *addr = new T[num_nodes_]();
  }

  /**
   * Destroy an array at some provided address.
   *
   * @param addr Address to destroy array at
   */
  template <typename T>
  void DeleteArray(T** addr) {
    delete[] * addr;
  }

  /**
   * Initialize local thread storage.
   */
  void InitializeLocal(void) {
    galois::on_each([this](unsigned, unsigned) {
      this->InitArray(centrality_measure_.getLocal());
      this->InitArray(per_thread_sigma_.getLocal());
      this->InitArray(per_thread_distance_.getLocal());
      this->InitArray(per_thread_delta_.getLocal());
      this->InitArray(per_thread_successor_.getLocal());
    });
  }

  /**
   * Destroy local thread storage.
   */
  void DeleteLocal(void) {
    galois::on_each([this](unsigned, unsigned) {
      this->DeleteArray(centrality_measure_.getLocal());
      this->DeleteArray(per_thread_sigma_.getLocal());
      this->DeleteArray(per_thread_distance_.getLocal());
      this->DeleteArray(per_thread_delta_.getLocal());
      this->DeleteArray(per_thread_successor_.getLocal());
    });
  }
};

/**
 * Functor that indicates if a node contains outgoing edges
 */
struct HasOut {
  const OuterGraph& graph;
  HasOut(const OuterGraph& g) : graph(g) {}

  bool operator()(const OuterGNode& n) const {
    return *graph.edge_begin(n) != *graph.edge_end(n);
  }
};

////////////////////////////////////////////////////////////////////////////////

void
DoOuterBC() {
  std::cout << "Reading from file: " << inputFile << "\n";
  std::unique_ptr<galois::graphs::PropertyFileGraph> pfg =
      MakeFileGraph(inputFile, edge_property_name);

  auto pg_result =
      galois::graphs::PropertyGraph<NodeDataOuter, EdgeDataOuter>::Make(
          pfg.get());
  if (!pg_result) {
    GALOIS_LOG_FATAL("could not make property graph: {}", pg_result.error());
  }
  OuterGraph graph = pg_result.value();

  std::cout << "Read " << graph.num_nodes() << " nodes, " << graph.num_edges()
            << " edges\n";

  BCOuter bc_outer(graph);

  size_t num_nodes_ = graph.num_nodes();

  // preallocate pages for use in algorithm
  galois::reportPageAlloc("MeminfoPre");
  galois::Prealloc(galois::getActiveThreads() * num_nodes_ / 1650);
  galois::reportPageAlloc("MeminfoMid");

  // vector of sources to process; initialized if doing outSources
  std::vector<OuterGNode> source_vector;
  // preprocessing: find the nodes with out edges we will process and skip
  // over nodes with no out edges; only done if numOfSources isn't specified
  if (numOfSources == 0) {
    // find first node with out edges
    boost::filter_iterator<HasOut, OuterGraph::iterator> begin =
        boost::make_filter_iterator(HasOut(graph), graph.begin(), graph.end());
    boost::filter_iterator<HasOut, OuterGraph::iterator> end =
        boost::make_filter_iterator(HasOut(graph), graph.end(), graph.end());
    // adjustedEnd = last node we will process based on how many iterations
    // (i.e. sources) we want to do
    boost::filter_iterator<HasOut, OuterGraph::iterator> adjustedEnd =
        iterLimit ? galois::safe_advance(begin, end, (int)iterLimit) : end;

    size_t iterations = std::distance(begin, adjustedEnd);
    galois::gPrint(
        "Num Nodes: ", num_nodes_, " Start Node: ", startSource,
        " Iterations: ", iterations, "\n");
    // vector of nodes we want to process
    for (auto node = begin; node != adjustedEnd; ++node) {
      source_vector.push_back(*node);
    }
  }

  // execute algorithm
  galois::StatTimer execTime("Timer_0");
  execTime.start();
  // either Run a contiguous chunk of sources from beginning or Run using
  // sources with outgoing edges only
  if (numOfSources > 0) {
    bc_outer.RunAll(numOfSources);
  } else {
    bc_outer.Run(source_vector);
  }
  execTime.stop();

  bc_outer.PrintBCValues(0, std::min(10UL, num_nodes_), std::cout, 6);
  bc_outer.OuterSanity(graph);
  if (output)
    bc_outer.PrintBCcertificate();

  if (!skipVerify)
    bc_outer.verify();

  galois::reportPageAlloc("MeminfoPost");
}
#endif
