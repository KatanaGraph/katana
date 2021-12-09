#include <boost/iterator/filter_iterator.hpp>

#include "betweenness_centrality_impl.h"
#include "katana/TypedPropertyGraph.h"

using namespace katana::analytics;

namespace {

using NodeDataOuter = std::tuple<>;
using EdgeDataOuter = std::tuple<>;

using OuterGraph = katana::TypedPropertyGraphView<
    katana::PropertyGraphViews::Default, NodeDataOuter, EdgeDataOuter>;
using OuterGNode = typename OuterGraph::Node;

////////////////////////////////////////////////////////////////////////////////

class BCOuter {
  const OuterGraph& graph_;
  int num_nodes_;

  // TODO(amp): centrality_measure_ is basically a manual implementation of
  //  vector GAccumulator. This should use the Reducible framework to hide the
  //  details.
  katana::PerThreadStorage<float*> centrality_measure_;  // Output value
  katana::PerThreadStorage<float*> per_thread_sigma_;
  katana::PerThreadStorage<int*> per_thread_distance_;
  katana::PerThreadStorage<float*> per_thread_delta_;
  katana::PerThreadStorage<katana::gdeque<OuterGNode>*> per_thread_successor_;

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
    katana::gdeque<OuterGNode> source_queue;

    float* sigma = *per_thread_sigma_.getLocal();
    int* distance = *per_thread_distance_.getLocal();
    float* delta = *per_thread_delta_.getLocal();
    katana::gdeque<OuterGNode>* successor = *per_thread_successor_.getLocal();

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
        auto dest = graph_.edge_dest(edge);

        if (!distance[dest]) {
          source_queue.push_back(dest);
          distance[dest] = distance[src] + 1;
        }

        if (distance[dest] == distance[src] + 1) {
          sigma[dest] = sigma[dest] + sigma[src];
          successor[src].push_back(dest);
        }
      }
    }

    // Back-propogate the dependency values (delta) along the BFS DAG
    // ignore the source (hence source_queue.size > 1 and not source_queue.empty)
    while (source_queue.size() > 1) {
      int leaf = source_queue.back();
      source_queue.pop_back();

      float sigma_leaf = sigma[leaf];  // has finalized short path value
      float delta_leaf = delta[leaf];
      auto& succ_list = successor[leaf];

      for (auto current_succ = succ_list.begin(), succ_end = succ_list.end();
           current_succ != succ_end; ++current_succ) {
        delta_leaf +=
            (sigma_leaf / sigma[*current_succ]) * (1.0 + delta[*current_succ]);
      }
      delta[leaf] = delta_leaf;
    }

    // save result of this source's BC, reset all local values for next
    // source
    float* Vec = *centrality_measure_.getLocal();
    for (int i = 0; i < num_nodes_; ++i) {
      Vec[i] += delta[i];
      delta[i] = 0;
      sigma[i] = 0;
      distance[i] = 0;
      successor[i].clear();
    }
  }

  /**
   * Runs betweenness-centrality proper.
   *
   * @tparam Cont type of the data structure that holds the nodes to treat
   * as a source during betweeness-centrality.
   *
   * @param source_vector Data structure that holds nodes to treat as a source during
   * betweenness-centrality
   */
  template <typename Cont>
  void Run(const Cont& source_vector) {
    // Each thread works on an individual source node
    katana::do_all(
        katana::iterate(source_vector),
        [&](const OuterGNode& current_source) { ComputeBC(current_source); },
        katana::steal(), katana::loopname("Main"));
  }

  /**
   * Verification for reference torus graph inputs.
   * All nodes should have the same betweenness value up to
   * some tolerance.
   */
  void Verify() {
    float sample_bc = 0.0;
    bool first_time = true;
    for (int i = 0; i < num_nodes_; ++i) {
      float bc = (*centrality_measure_.getRemote(0))[i];

      for (unsigned j = 1; j < katana::getActiveThreads(); ++j)
        bc += (*centrality_measure_.getRemote(j))[i];

      if (first_time) {
        sample_bc = bc;
        katana::gInfo("BC: ", sample_bc);
        first_time = false;
      } else {
        // check if over some tolerance value
        if ((bc - sample_bc) > 0.0001) {
          katana::gInfo(
              "If torus graph, verification failed ", (bc - sample_bc));
          return;
        }
      }
    }
  }

  katana::Result<std::shared_ptr<arrow::FloatArray>> ExtractBCValues(
      size_t begin, size_t end) {
    arrow::FloatBuilder builder;
    if (auto r = builder.Resize(end - begin); !r.ok()) {
      return katana::ErrorCode::ArrowError;
    }
    for (; begin != end; ++begin) {
      float bc = (*centrality_measure_.getRemote(0))[begin];

      for (unsigned j = 1; j < katana::getActiveThreads(); ++j) {
        bc += (*centrality_measure_.getRemote(j))[begin];
      }

      if (auto r = builder.Append(bc); !r.ok()) {
        return katana::ErrorCode::ArrowError;
      }
    }
    std::shared_ptr<arrow::FloatArray> ret;
    if (auto r = builder.Finish(&ret); !r.ok()) {
      return katana::ErrorCode::ArrowError;
    }
    return ret;
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
    katana::on_each([this](unsigned, unsigned) {
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
    katana::on_each([this](unsigned, unsigned) {
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
    // return *graph.edge_begin(n) != *graph.edge_end(n);
    auto edge_range = graph.edges(n);
    return !edge_range.empty();
  }
};
}  // namespace

////////////////////////////////////////////////////////////////////////////////

katana::Result<void>
BetweennessCentralityOuter(
    katana::PropertyGraph* pg, BetweennessCentralitySources sources,
    const std::string& output_property_name,
    BetweennessCentralityPlan plan [[maybe_unused]]) {
  OuterGraph graph = KATANA_CHECKED(OuterGraph::Make(pg, {}, {}));

  BCOuter bc_outer(graph);

  // preallocate pages for use in algorithm
  katana::EnsurePreallocated(
      katana::getActiveThreads() * graph.num_nodes() / 1650);
  katana::ReportPageAllocGuard page_alloc;

  // vector of sources to process; initialized if doing outSources
  std::vector<uint32_t> source_vector;
  // preprocessing: find the nodes with out edges we will process and skip
  // over nodes with no out edges; only done if numOfSources isn't specified
  if (std::holds_alternative<uint32_t>(sources) &&
      sources != kBetweennessCentralityAllNodes) {
    // find first node with out edges
    boost::filter_iterator<HasOut, OuterGraph::iterator> begin =
        boost::make_filter_iterator(HasOut(graph), graph.begin(), graph.end());
    boost::filter_iterator<HasOut, OuterGraph::iterator> end =
        boost::make_filter_iterator(HasOut(graph), graph.end(), graph.end());
    // adjustedEnd = last node we will process based on how many iterations
    // (i.e. sources) we want to do
    boost::filter_iterator<HasOut, OuterGraph::iterator> adjustedEnd =
        katana::safe_advance(begin, end, (int)std::get<uint32_t>(sources));

    // vector of nodes we want to process
    for (auto node = begin; node != adjustedEnd; ++node) {
      source_vector.push_back(*node);
    }
  } else if (std::holds_alternative<std::vector<uint32_t>>(sources)) {
    source_vector = std::get<std::vector<uint32_t>>(sources);
  }

  // execute algorithm
  katana::StatTimer exec_time("Betweenness Centrality Outer");
  exec_time.start();
  if (sources == kBetweennessCentralityAllNodes) {
    bc_outer.Run(katana::iterate(*pg));
  } else {
    bc_outer.Run(source_vector);
  }
  exec_time.stop();

  auto data_result = bc_outer.ExtractBCValues(0, graph.num_nodes());
  if (!data_result) {
    return data_result.error();
  }

  auto table = arrow::Table::Make(
      arrow::schema({arrow::field(output_property_name, arrow::float32())}),
      {data_result.value()});
  if (auto r = pg->AddNodeProperties(table); !r) {
    return r.error();
  }

  return katana::ResultSuccess();
}
