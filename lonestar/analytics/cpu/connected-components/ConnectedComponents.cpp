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
#include <ostream>
#include <utility>
#include <vector>

#include "Lonestar/BoilerPlate.h"
#include "galois/AtomicHelpers.h"
#include "galois/Bag.h"
#include "galois/Galois.h"
#include "galois/ParallelSTL.h"
#include "galois/Reduction.h"
#include "galois/Timer.h"
#include "galois/UnionFind.h"
#include "galois/graphs/LCGraph.h"
#include "galois/graphs/OCGraph.h"
#include "galois/graphs/TypeTraits.h"
#include "galois/runtime/Profile.h"
#include "llvm/Support/CommandLine.h"

const char* name = "Connected Components";
const char* desc = "Computes the connected components of a graph";

namespace cll = llvm::cl;

enum Algo {
  serial,
  labelProp,
  synchronous,
  async,
  edgeasync,
  blockedasync,
  edgetiledasync,
  afforest,
  edgeafforest,
  edgetiledafforest,
};

static cll::opt<std::string> inputFile(
    cll::Positional, cll::desc("<input file>"), cll::Required);
static cll::opt<Algo> algo(
    "algo", cll::desc("Choose an algorithm:"),
    cll::values(
        clEnumValN(Algo::async, "Async", "Asynchronous"),
        clEnumValN(Algo::edgeasync, "EdgeAsync", "Edge-Asynchronous"),
        clEnumValN(
            Algo::edgetiledasync, "EdgetiledAsync",
            "EdgeTiled-Asynchronous (default)"),
        clEnumValN(Algo::blockedasync, "BlockedAsync", "Blocked asynchronous"),
        clEnumValN(
            Algo::labelProp, "LabelProp", "Using label propagation algorithm"),
        clEnumValN(Algo::serial, "Serial", "Serial"),
        clEnumValN(Algo::synchronous, "Sync", "Synchronous"),
        clEnumValN(Algo::afforest, "Afforest", "Using Afforest sampling"),
        clEnumValN(
            Algo::edgeafforest, "EdgeAfforest",
            "Using Afforest sampling, Edge-wise"),
        clEnumValN(
            Algo::edgetiledafforest, "EdgetiledAfforest",
            "Using Afforest sampling, EdgeTiled")

            ),
    cll::init(Algo::edgetiledasync));

static cll::opt<std::string> largestComponentFilename(
    "outputLargestComponent", cll::desc("[output graph file]"), cll::init(""));
static cll::opt<std::string> permutationFilename(
    "outputNodePermutation", cll::desc("[output node permutation file]"),
    cll::init(""));
#ifndef NDEBUG
enum OutputEdgeType { void_, int32_, int64_ };
static cll::opt<unsigned int> memoryLimit(
    "memoryLimit", cll::desc("Memory limit for out-of-core algorithms (in MB)"),
    cll::init(~0U));
static cll::opt<OutputEdgeType> writeEdgeType(
    "edgeType", cll::desc("Input/Output edge type:"),
    cll::values(
        clEnumValN(OutputEdgeType::void_, "void", "no edge values"),
        clEnumValN(OutputEdgeType::int32_, "int32", "32 bit edge values"),
        clEnumValN(OutputEdgeType::int64_, "int64", "64 bit edge values")),
    cll::init(OutputEdgeType::void_));
#endif

// TODO (bozhi) LLVM commandline library now supports option categorization.
// Categorize params when libllvm is updated to make -help beautiful!
// static cll::OptionCategory ParamCat("Algorithm-Specific Parameters",
//                                       "Only used for specific algorithms.");
static cll::opt<uint32_t> EDGE_TILE_SIZE(
    "edgeTileSize",
    cll::desc("(For Edgetiled algos) Size of edge tiles "
              "(default 512)"),
    // cll::cat(ParamCat),
    cll::init(512));  // 512 -> 64
static const int CHUNK_SIZE = 1;
//! parameter for the Vertex Neighbor Sampling step of Afforest algorithm
static cll::opt<uint32_t> NEIGHBOR_SAMPLES(
    "vns",
    cll::desc("(For Afforest and its variants) number of edges "
              "per vertice to process initially for exposing "
              "partial connectivity (default 2)"),
    // cll::cat(ParamCat),
    cll::init(2));
//! parameter for the Large Component Skipping step of Afforest algorithm
static cll::opt<uint32_t> COMPONENT_SAMPLES(
    "lcs",
    cll::desc("(For Afforest and its variants) number of times "
              "randomly sampling over vertices to approximately "
              "capture the largest intermediate component "
              "(default 1024)"),
    // cll::cat(ParamCat),
    cll::init(1024));

struct Node : public galois::UnionFindNode<Node> {
  using ComponentType = Node*;

  Node() : galois::UnionFindNode<Node>(const_cast<Node*>(this)) {}
  Node(const Node& o) : galois::UnionFindNode<Node>(o.m_component) {}

  Node& operator=(const Node& o) {
    Node c(o);
    std::swap(c, *this);
    return *this;
  }

  ComponentType component() { return this->get(); }
  bool isRepComp(unsigned int) { return false; }
};

const unsigned int LABEL_INF = std::numeric_limits<unsigned int>::max();

/**
 * Serial connected components algorithm. Just use union-find.
 */
struct SerialAlgo {
  using ComponentType = Node*;
  struct NodeComponent {
    using ArrowType = arrow::CTypeTraits<uint64_t>::ArrowType;
    using ViewType = galois::PODPropertyView<ComponentType>;
  };

  using NodeData = std::tuple<NodeComponent>;
  using EdgeData = std::tuple<>;
  typedef galois::graphs::PropertyGraph<NodeData, EdgeData> Graph;
  typedef typename Graph::Node GNode;

  void Initialize(Graph* graph) {
    galois::do_all(galois::iterate(*graph), [&](const GNode& node) {
      graph->GetData<NodeComponent>(node) = new Node();
    });
  }

  void Deallocate(Graph* graph) {
    galois::do_all(galois::iterate(*graph), [&](const GNode& node) {
      delete graph->GetData<NodeComponent>(node);
    });
  }

  void operator()(Graph* graph) {
    for (const GNode& src : *graph) {
      auto& sdata = graph->GetData<NodeComponent>(src);
      for (const auto& ii : graph->edges(src)) {
        const auto& dest = graph->GetEdgeDest(ii);
        auto& ddata = graph->GetData<NodeComponent>(dest);
        sdata->merge(ddata);
      }
    }

    for (const GNode& src : *graph) {
      auto& sdata = graph->GetData<NodeComponent>(src);
      sdata->compress();
    }
  }
};

struct LabelPropAlgo {
  using ComponentType = unsigned int;
  struct NodeComponent {
    using ArrowType = arrow::CTypeTraits<ComponentType>::ArrowType;
    using ViewType = galois::PODPropertyView<std::atomic<ComponentType>>;
  };
  struct NodeOldComponent : public galois::PODProperty<ComponentType> {};

  using NodeData = std::tuple<NodeComponent, NodeOldComponent>;
  using EdgeData = std::tuple<>;
  typedef galois::graphs::PropertyGraph<NodeData, EdgeData> Graph;
  typedef typename Graph::Node GNode;

  void Initialize(Graph* graph) {
    galois::do_all(galois::iterate(*graph), [&](const GNode& node) {
      graph->GetData<NodeComponent>(node).store(node);
      graph->GetData<NodeOldComponent>(node) = LABEL_INF;
    });
  }

  void Deallocate(Graph*) {}

  void operator()(Graph* graph) {
    galois::GReduceLogicalOr changed;
    do {
      changed.reset();
      galois::do_all(
          galois::iterate(*graph),
          [&](const GNode& src) {
            auto& sdata_current_comp = graph->GetData<NodeComponent>(src);
            auto& sdata_old_comp = graph->GetData<NodeOldComponent>(src);
            if (sdata_old_comp > sdata_current_comp) {
              sdata_old_comp = sdata_current_comp;

              changed.update(true);

              for (auto e : graph->edges(src)) {
                auto dest = graph->GetEdgeDest(e);
                auto& ddata_current_comp = graph->GetData<NodeComponent>(*dest);
                ComponentType label_new = sdata_current_comp;
                galois::atomicMin(ddata_current_comp, label_new);
              }
            }
          },
          galois::disable_conflict_detection(), galois::steal(),
          galois::loopname("LabelPropAlgo"));
    } while (changed.reduce());
  }
};

/**
 * Synchronous connected components algorithm.  Initially all nodes are in
 * their own component. Then, we merge endpoints of edges to form the spanning
 * tree. Merging is done in two phases to simplify concurrent updates: (1)
 * find components and (2) union components.  Since the merge phase does not
 * do any finds, we only process a fraction of edges at a time; otherwise,
 * the union phase may unnecessarily merge two endpoints in the same
 * component.
 */
struct SynchronousAlgo {
  using ComponentType = Node*;
  struct NodeComponent {
    using ArrowType = arrow::CTypeTraits<uint64_t>::ArrowType;
    using ViewType = galois::PODPropertyView<ComponentType>;
  };

  using NodeData = std::tuple<NodeComponent>;
  using EdgeData = std::tuple<>;
  typedef galois::graphs::PropertyGraph<NodeData, EdgeData> Graph;
  typedef typename Graph::Node GNode;

  struct Edge {
    GNode src;
    Node* ddata;
    int count;
    Edge(GNode src, Node* ddata, int count)
        : src(src), ddata(ddata), count(count) {}
  };

  void Initialize(Graph* graph) {
    galois::do_all(galois::iterate(*graph), [&](const GNode& node) {
      graph->GetData<NodeComponent>(node) = new Node();
    });
  }

  void Deallocate(Graph* graph) {
    galois::do_all(galois::iterate(*graph), [&](const GNode& node) {
      delete graph->GetData<NodeComponent>(node);
    });
  }

  void operator()(Graph* graph) {
    size_t rounds = 0;
    galois::GAccumulator<size_t> empty_merges;

    galois::InsertBag<Edge> wls[2];
    galois::InsertBag<Edge>* next_bag;
    galois::InsertBag<Edge>* current_bag;

    current_bag = &wls[0];
    next_bag = &wls[1];

    galois::do_all(galois::iterate(*graph), [&](const GNode& src) {
      for (auto ii : graph->edges(src)) {
        const auto& dest = graph->GetEdgeDest(ii);
        if (src >= *dest)
          continue;
        auto& ddata = graph->GetData<NodeComponent>(dest);
        current_bag->push(Edge(src, ddata, 0));
        break;
      }
    });

    while (!current_bag->empty()) {
      galois::do_all(
          galois::iterate(*current_bag),
          [&](const Edge& edge) {
            auto& sdata = graph->GetData<NodeComponent>(edge.src);
            if (!sdata->merge(edge.ddata))
              empty_merges += 1;
          },
          galois::loopname("Merge"));

      galois::do_all(
          galois::iterate(*current_bag),
          [&](const Edge& edge) {
            GNode src = edge.src;
            auto& sdata = graph->GetData<NodeComponent>(src);
            Node* src_component = sdata->findAndCompress();
            Graph::edge_iterator ii = graph->edge_begin(src);
            Graph::edge_iterator ei = graph->edge_end(src);
            int count = edge.count + 1;
            std::advance(ii, count);
            for (; ii != ei; ++ii, ++count) {
              const auto& dest = graph->GetEdgeDest(ii);
              if (src >= *dest)
                continue;
              auto& ddata = graph->GetData<NodeComponent>(dest);
              Node* dest_component = ddata->findAndCompress();
              if (src_component != dest_component) {
                next_bag->push(Edge(src, dest_component, count));
                break;
              }
            }
          },
          galois::loopname("Find"));

      current_bag->clear();
      std::swap(current_bag, next_bag);
      rounds += 1;
    }

    galois::do_all(
        galois::iterate(*graph),
        [&](const GNode& src) {
          auto& sdata = graph->GetData<NodeComponent>(src);
          sdata->compress();
        },
        galois::steal(), galois::loopname("Compress"));

    galois::runtime::reportStat_Single("CC-Sync", "rounds", rounds);
    galois::runtime::reportStat_Single(
        "CC-Sync", "empty_merges", empty_merges.reduce());
  }
};

/**
 * Like synchronous algorithm, but if we restrict path compression (as done is
 * @link{UnionFindNode}), we can perform unions and finds concurrently.
 */
struct AsyncAlgo {
  using ComponentType = Node*;
  struct NodeComponent {
    using ArrowType = arrow::CTypeTraits<uint64_t>::ArrowType;
    using ViewType = galois::PODPropertyView<ComponentType>;
  };

  using NodeData = std::tuple<NodeComponent>;
  using EdgeData = std::tuple<>;
  typedef galois::graphs::PropertyGraph<NodeData, EdgeData> Graph;
  typedef typename Graph::Node GNode;

  void Initialize(Graph* graph) {
    galois::do_all(galois::iterate(*graph), [&](const GNode& node) {
      graph->GetData<NodeComponent>(node) = new Node();
    });
  }

  void Deallocate(Graph* graph) {
    galois::do_all(galois::iterate(*graph), [&](const GNode& node) {
      delete graph->GetData<NodeComponent>(node);
    });
  }

  void operator()(Graph* graph) {
    galois::GAccumulator<size_t> empty_merges;

    galois::do_all(
        galois::iterate(*graph),
        [&](const GNode& src) {
          auto& sdata = graph->GetData<NodeComponent>(src);

          for (const auto& ii : graph->edges(src)) {
            const auto& dest = graph->GetEdgeDest(ii);
            auto& ddata = graph->GetData<NodeComponent>(dest);

            if (src >= *dest)
              continue;

            if (!sdata->merge(ddata))
              empty_merges += 1;
          }
        },
        galois::loopname("CC-Async"));

    galois::do_all(
        galois::iterate(*graph),
        [&](const GNode& src) {
          auto& sdata = graph->GetData<NodeComponent>(src);
          sdata->compress();
        },
        galois::steal(), galois::loopname("CC-Async-Compress"));

    galois::runtime::reportStat_Single(
        "CC-Async", "empty_merges", empty_merges.reduce());
  }
};

struct EdgeAsyncAlgo {
  using ComponentType = Node*;
  struct NodeComponent {
    using ArrowType = arrow::CTypeTraits<uint64_t>::ArrowType;
    using ViewType = galois::PODPropertyView<ComponentType>;
  };

  using NodeData = std::tuple<NodeComponent>;
  using EdgeData = std::tuple<>;
  typedef galois::graphs::PropertyGraph<NodeData, EdgeData> Graph;
  typedef typename Graph::Node GNode;
  using Edge = std::pair<GNode, typename Graph::edge_iterator>;

  void Initialize(Graph* graph) {
    galois::do_all(galois::iterate(*graph), [&](const GNode& node) {
      graph->GetData<NodeComponent>(node) = new Node();
    });
  }

  void Deallocate(Graph* graph) {
    galois::do_all(galois::iterate(*graph), [&](const GNode& node) {
      delete graph->GetData<NodeComponent>(node);
    });
  }

  void operator()(Graph* graph) {
    galois::GAccumulator<size_t> empty_merges;

    galois::InsertBag<Edge> works;

    galois::do_all(
        galois::iterate(*graph),
        [&](const GNode& src) {
          for (const auto& ii : graph->edges(src)) {
            if (src < *(graph->GetEdgeDest(ii))) {
              works.push_back(std::make_pair(src, ii));
            }
          }
        },
        galois::loopname("CC-EdgeAsyncInit"), galois::steal());

    galois::do_all(
        galois::iterate(works),
        [&](Edge& e) {
          auto& sdata = graph->GetData<NodeComponent>(e.first);
          const auto& dest = graph->GetEdgeDest(e.second);
          auto& ddata = graph->GetData<NodeComponent>(dest);

          if (e.first > *dest)
            // continue;
            ;
          else if (!sdata->merge(ddata)) {
            empty_merges += 1;
          }
        },
        galois::loopname("CC-EdgeAsync"), galois::steal());

    galois::do_all(
        galois::iterate(*graph),
        [&](const GNode& src) {
          auto& sdata = graph->GetData<NodeComponent>(src);
          sdata->compress();
        },
        galois::steal(), galois::loopname("CC-Async-Compress"));

    galois::runtime::reportStat_Single(
        "CC-Async", "empty_merges", empty_merges.reduce());
  }
};

/**
 * Improve performance of async algorithm by following machine topology.
 */
struct BlockedAsyncAlgo {
  using ComponentType = Node*;
  struct NodeComponent {
    using ArrowType = arrow::CTypeTraits<uint64_t>::ArrowType;
    using ViewType = galois::PODPropertyView<ComponentType>;
  };

  using NodeData = std::tuple<NodeComponent>;
  using EdgeData = std::tuple<>;
  typedef galois::graphs::PropertyGraph<NodeData, EdgeData> Graph;
  typedef typename Graph::Node GNode;
  using Edge = std::pair<GNode, typename Graph::edge_iterator>;

  void Initialize(Graph* graph) {
    galois::do_all(galois::iterate(*graph), [&](const GNode& node) {
      graph->GetData<NodeComponent>(node) = new Node();
    });
  }

  void Deallocate(Graph* graph) {
    galois::do_all(galois::iterate(*graph), [&](const GNode& node) {
      delete graph->GetData<NodeComponent>(node);
    });
  }

  struct WorkItem {
    GNode src;
    Graph::edge_iterator start;
  };

  //! Add the next edge between components to the worklist
  template <bool MakeContinuation, int Limit, typename Pusher>
  static void process(
      Graph* graph, const GNode& src, const Graph::edge_iterator& start,
      Pusher& pusher) {
    auto& sdata = graph->GetData<NodeComponent>(src);
    int count = 1;
    for (Graph::edge_iterator ii = start, ei = graph->edge_end(src); ii != ei;
         ++ii, ++count) {
      const auto& dest = graph->GetEdgeDest(ii);
      auto& ddata = graph->GetData<NodeComponent>(*dest);

      if (src >= *dest)
        continue;

      if (sdata->merge(ddata)) {
        if (Limit == 0 || count != Limit)
          continue;
      }

      if (MakeContinuation || (Limit != 0 && count == Limit)) {
        WorkItem item = {src, ii + 1};
        pusher.push(item);
        break;
      }
    }
  }

  void operator()(Graph* graph) {
    galois::InsertBag<WorkItem> items;

    galois::do_all(
        galois::iterate(*graph),
        [&](const GNode& src) {
          auto start = graph->edge_begin(src);
          if (galois::substrate::ThreadPool::getSocket() == 0) {
            process<true, 0>(graph, src, start, items);
          } else {
            process<true, 1>(graph, src, start, items);
          }
        },
        galois::loopname("Initialize"));

    galois::for_each(
        galois::iterate(items),
        [&](const WorkItem& item, auto& ctx) {
          process<true, 0>(graph, item.src, item.start, ctx);
        },
        galois::loopname("Merge"),
        galois::wl<galois::worklists::PerSocketChunkFIFO<128>>());

    galois::do_all(
        galois::iterate(*graph),
        [&](const GNode& src) {
          auto& sdata = graph->GetData<NodeComponent>(src);
          sdata->compress();
        },
        galois::steal(), galois::loopname("CC-Async-Compress"));
  }
};

struct EdgeTiledAsyncAlgo {
  using ComponentType = Node*;
  struct NodeComponent {
    using ArrowType = arrow::CTypeTraits<uint64_t>::ArrowType;
    using ViewType = galois::PODPropertyView<ComponentType>;
  };

  using NodeData = std::tuple<NodeComponent>;
  using EdgeData = std::tuple<>;
  typedef galois::graphs::PropertyGraph<NodeData, EdgeData> Graph;
  typedef typename Graph::Node GNode;
  using Edge = std::pair<GNode, typename Graph::edge_iterator>;

  void Initialize(Graph* graph) {
    galois::do_all(galois::iterate(*graph), [&](const GNode& node) {
      graph->GetData<NodeComponent>(node) = new Node();
    });
  }

  void Deallocate(Graph* graph) {
    galois::do_all(galois::iterate(*graph), [&](const GNode& node) {
      delete graph->GetData<NodeComponent>(node);
    });
  }

  struct EdgeTile {
    // Node* sData;
    GNode src;
    Graph::edge_iterator beg;
    Graph::edge_iterator end;
  };

  /*struct EdgeTileMaker {
      EdgeTile operator() (Node* sdata, Graph::edge_iterator beg,
  Graph::edge_iterator end) const{ return EdgeTile{sdata, beg, end};
      }
  };*/

  void operator()(Graph* graph) {
    galois::GAccumulator<size_t> empty_merges;

    galois::InsertBag<EdgeTile> works;

    std::cout << "INFO: Using edge tile size of " << EDGE_TILE_SIZE
              << " and chunk size of " << CHUNK_SIZE << "\n";
    std::cout << "WARNING: Performance varies considerably due to parameter.\n";
    std::cout
        << "WARNING: Do not expect the default to be good for your graph.\n";

    galois::do_all(
        galois::iterate(*graph),
        [&](const GNode& src) {
          auto beg = graph->edge_begin(src);
          const auto& end = graph->edge_end(src);

          assert(beg <= end);
          if ((end - beg) > EDGE_TILE_SIZE) {
            for (; beg + EDGE_TILE_SIZE < end;) {
              const auto& ne = beg + EDGE_TILE_SIZE;
              assert(ne < end);
              works.push_back(EdgeTile{src, beg, ne});
              beg = ne;
            }
          }

          if ((end - beg) > 0) {
            works.push_back(EdgeTile{src, beg, end});
          }
        },
        galois::loopname("CC-EdgeTiledAsyncInit"), galois::steal());

    galois::do_all(
        galois::iterate(works),
        [&](const EdgeTile& tile) {
          const auto& src = tile.src;
          auto& sdata = graph->GetData<NodeComponent>(src);

          for (auto ii = tile.beg; ii != tile.end; ++ii) {
            const auto& dest = graph->GetEdgeDest(ii);
            if (src >= *dest)
              continue;

            auto& ddata = graph->GetData<NodeComponent>(dest);
            if (!sdata->merge(ddata))
              empty_merges += 1;
          }
        },
        galois::loopname("CC-edgetiledAsync"), galois::steal(),
        galois::chunk_size<CHUNK_SIZE>()  // 16 -> 1
    );

    galois::do_all(
        galois::iterate(*graph),
        [&](const GNode& src) {
          auto& sdata = graph->GetData<NodeComponent>(src);
          sdata->compress();
        },
        galois::steal(), galois::loopname("CC-Async-Compress"));

    galois::runtime::reportStat_Single(
        "CC-edgeTiledAsync", "empty_merges", empty_merges.reduce());
  }
};

template <typename ComponentType, typename Graph, typename NodeIndex>
ComponentType
approxLargestComponent(Graph* graph) {
  using map_type = std::unordered_map<
      ComponentType, int, std::hash<ComponentType>,
      std::equal_to<ComponentType>,
      galois::gstl::Pow2Alloc<std::pair<const ComponentType, int>>>;
  using pair_type = std::pair<ComponentType, int>;

  map_type comp_freq(COMPONENT_SAMPLES);
  std::random_device rd;
  std::mt19937 rng(rd());
  std::uniform_int_distribution<uint32_t> dist(0, graph->size() - 1);
  for (uint32_t i = 0; i < COMPONENT_SAMPLES; i++) {
    auto& ndata = graph->template GetData<NodeIndex>(dist(rng));
    comp_freq[ndata->component()]++;
  }

  assert(!comp_freq.empty());
  auto most_frequent = std::max_element(
      comp_freq.begin(), comp_freq.end(),
      [](const pair_type& a, const pair_type& b) {
        return a.second < b.second;
      });

  galois::gDebug(
      "Approximate largest intermediate component: ", most_frequent->first,
      " (hit rate ", 100.0 * (most_frequent->second) / COMPONENT_SAMPLES, "%)");

  return most_frequent->first;
}

/**
 * CC w/ Afforest sampling.
 *
 * [1] M. Sutton, T. Ben-Nun and A. Barak, "Optimizing Parallel Graph
 * Connectivity Computation via Subgraph Sampling," 2018 IEEE International
 * Parallel and Distributed Processing Symposium (IPDPS), Vancouver, BC, 2018,
 * pp. 12-21.
 */
struct AfforestAlgo {
  struct NodeAfforest : public galois::UnionFindNode<NodeAfforest> {
    using ComponentType = NodeAfforest*;

    NodeAfforest()
        : galois::UnionFindNode<NodeAfforest>(const_cast<NodeAfforest*>(this)) {
    }
    NodeAfforest(const NodeAfforest& o)
        : galois::UnionFindNode<NodeAfforest>(o.m_component) {}

    ComponentType component() { return this->get(); }
    bool isRepComp(unsigned int) { return false; }  // verify

  public:
    void link(NodeAfforest* b) {
      NodeAfforest* a = m_component.load(std::memory_order_relaxed);
      b = b->m_component.load(std::memory_order_relaxed);
      while (a != b) {
        if (a < b)
          std::swap(a, b);
        // Now a > b
        NodeAfforest* ac = a->m_component.load(std::memory_order_relaxed);
        if ((ac == a && a->m_component.compare_exchange_strong(a, b)) ||
            (b == ac))
          break;
        a = (a->m_component.load(std::memory_order_relaxed))
                ->m_component.load(std::memory_order_relaxed);
        b = b->m_component.load(std::memory_order_relaxed);
      }
    }
  };

  struct NodeComponent {
    using ArrowType = arrow::CTypeTraits<uint64_t>::ArrowType;
    using ViewType = galois::PODPropertyView<NodeAfforest::ComponentType>;
  };

  using NodeData = std::tuple<NodeComponent>;
  using EdgeData = std::tuple<>;
  typedef galois::graphs::PropertyGraph<NodeData, EdgeData> Graph;
  typedef typename Graph::Node GNode;

  void Initialize(Graph* graph) {
    galois::do_all(galois::iterate(*graph), [&](const GNode& node) {
      graph->GetData<NodeComponent>(node) = new NodeAfforest();
    });
  }

  void Deallocate(Graph* graph) {
    galois::do_all(galois::iterate(*graph), [&](const GNode& node) {
      delete graph->GetData<NodeComponent>(node);
    });
  }
  using ComponentType = NodeAfforest::ComponentType;

  void operator()(Graph* graph) {
    // (bozhi) should NOT go through single direction in sampling step: nodes
    // with edges less than NEIGHBOR_SAMPLES will fail
    for (uint32_t r = 0; r < NEIGHBOR_SAMPLES; ++r) {
      galois::do_all(
          galois::iterate(*graph),
          [&](const GNode& src) {
            Graph::edge_iterator ii = graph->edge_begin(src);
            Graph::edge_iterator ei = graph->edge_end(src);
            for (std::advance(ii, r); ii < ei; ii++) {
              const auto& dest = *graph->GetEdgeDest(ii);
              auto& sdata = graph->GetData<NodeComponent>(src);
              auto& ddata = graph->GetData<NodeComponent>(dest);
              sdata->link(ddata);
              break;
            }
          },
          galois::steal(), galois::loopname("Afforest-VNS-Link"));

      galois::do_all(
          galois::iterate(*graph),
          [&](const GNode& src) {
            auto& sdata = graph->GetData<NodeComponent>(src);
            sdata->compress();
          },
          galois::steal(), galois::loopname("Afforest-VNS-Compress"));
    }

    galois::StatTimer StatTimer_Sampling("Afforest-LCS-Sampling");
    StatTimer_Sampling.start();
    const ComponentType c =
        approxLargestComponent<ComponentType, Graph, NodeComponent>(graph);
    StatTimer_Sampling.stop();

    galois::do_all(
        galois::iterate(*graph),
        [&](const GNode& src) {
          auto& sdata = graph->GetData<NodeComponent>(src);
          if (sdata->component() == c)
            return;
          Graph::edge_iterator ii = graph->edge_begin(src);
          Graph::edge_iterator ei = graph->edge_end(src);
          for (std::advance(ii, NEIGHBOR_SAMPLES.getValue()); ii < ei; ++ii) {
            const GNode& dest = *graph->GetEdgeDest(ii);
            auto& ddata = graph->GetData<NodeComponent>(dest);
            sdata->link(ddata);
          }
        },
        galois::steal(), galois::loopname("Afforest-LCS-Link"));

    galois::do_all(
        galois::iterate(*graph),
        [&](const GNode& src) {
          auto& sdata = graph->GetData<NodeComponent>(src);
          sdata->compress();
        },
        galois::steal(), galois::loopname("Afforest-LCS-Compress"));
  }
};

/**
 * Edge CC w/ Afforest sampling
 */
struct EdgeAfforestAlgo {
  struct NodeAfforestEdge : public galois::UnionFindNode<NodeAfforestEdge> {
    using ComponentType = NodeAfforestEdge*;

    NodeAfforestEdge()
        : galois::UnionFindNode<NodeAfforestEdge>(
              const_cast<NodeAfforestEdge*>(this)) {}
    NodeAfforestEdge(const NodeAfforestEdge& o)
        : galois::UnionFindNode<NodeAfforestEdge>(o.m_component) {}

    ComponentType component() { return this->get(); }
    bool isRepComp(unsigned int) { return false; }  // verify

  public:
    NodeAfforestEdge* hook_min(NodeAfforestEdge* b, NodeAfforestEdge* c = 0) {
      NodeAfforestEdge* a = m_component.load(std::memory_order_relaxed);
      b = b->m_component.load(std::memory_order_relaxed);
      while (a != b) {
        if (a < b)
          std::swap(a, b);
        // Now a > b
        NodeAfforestEdge* ac = a->m_component.load(std::memory_order_relaxed);
        if (ac == a && a->m_component.compare_exchange_strong(a, b)) {
          if (b == c)
            return a;  //! return victim
          return 0;
        }
        if (b == ac) {
          return 0;
        }
        a = (a->m_component.load(std::memory_order_relaxed))
                ->m_component.load(std::memory_order_relaxed);
        b = b->m_component.load(std::memory_order_relaxed);
      }
      return 0;
    }
  };

  using ComponentType = NodeAfforestEdge::ComponentType;
  struct NodeComponent {
    using ArrowType = arrow::CTypeTraits<uint64_t>::ArrowType;
    using ViewType = galois::PODPropertyView<NodeAfforestEdge::ComponentType>;
  };

  using NodeData = std::tuple<NodeComponent>;
  using EdgeData = std::tuple<>;
  typedef galois::graphs::PropertyGraph<NodeData, EdgeData> Graph;
  typedef typename Graph::Node GNode;

  using Edge = std::pair<GNode, GNode>;

  void Initialize(Graph* graph) {
    galois::do_all(galois::iterate(*graph), [&](const GNode& node) {
      graph->GetData<NodeComponent>(node) = new NodeAfforestEdge();
    });
  }

  void Deallocate(Graph* graph) {
    galois::do_all(galois::iterate(*graph), [&](const GNode& node) {
      delete graph->GetData<NodeComponent>(node);
    });
  }
  void operator()(Graph* graph) {
    // (bozhi) should NOT go through single direction in sampling step: nodes
    // with edges less than NEIGHBOR_SAMPLES will fail
    for (uint32_t r = 0; r < NEIGHBOR_SAMPLES; ++r) {
      galois::do_all(
          galois::iterate(*graph),
          [&](const GNode& src) {
            Graph::edge_iterator ii = graph->edge_begin(src);
            Graph::edge_iterator ei = graph->edge_end(src);
            std::advance(ii, r);
            if (ii < ei) {
              const auto& dst = graph->GetEdgeDest(ii);
              auto& sdata = graph->GetData<NodeComponent>(src);
              auto& ddata = graph->GetData<NodeComponent>(dst);
              sdata->hook_min(ddata);
            }
          },
          galois::steal(), galois::loopname("EdgeAfforest-VNS-Link"));
    }
    galois::do_all(
        galois::iterate(*graph),
        [&](const GNode& src) {
          auto& sdata = graph->GetData<NodeComponent>(src);
          sdata->compress();
        },
        galois::steal(), galois::loopname("EdgeAfforest-VNS-Compress"));

    galois::StatTimer StatTimer_Sampling("EdgeAfforest-LCS-Sampling");
    StatTimer_Sampling.start();
    const ComponentType c =
        approxLargestComponent<ComponentType, Graph, NodeComponent>(graph);
    StatTimer_Sampling.stop();
    const ComponentType c0 = (graph->GetData<NodeComponent>(0));

    galois::InsertBag<Edge> works;

    galois::do_all(
        galois::iterate(*graph),
        [&](const GNode& src) {
          auto& sdata = graph->GetData<NodeComponent>(src);
          if (sdata->component() == c)
            return;
          auto beg = graph->edge_begin(src);
          const auto end = graph->edge_end(src);

          for (std::advance(beg, NEIGHBOR_SAMPLES.getValue()); beg < end;
               beg++) {
            const auto& dest = *(graph->GetEdgeDest(beg));
            auto& ddata = graph->GetData<NodeComponent>(dest);
            if (src < dest || c == ddata->component()) {
              works.push_back(std::make_pair(src, dest));
            }
          }
        },
        galois::loopname("EdgeAfforest-LCS-Assembling"), galois::steal());

    galois::for_each(
        galois::iterate(works),
        [&](const Edge& e, auto& ctx) {
          auto& sdata = graph->GetData<NodeComponent>(e.first);
          if (sdata->component() == c)
            return;
          auto& ddata = graph->GetData<NodeComponent>(e.second);
          ComponentType victim = sdata->hook_min(ddata, c);
          if (victim) {
            auto src = victim - c0;  // TODO (bozhi) tricky!
            for (auto ii : graph->edges(src)) {
              const auto& dest = *graph->GetEdgeDest(ii);
              ctx.push_back(std::make_pair(dest, src));
            }
          }
        },
        galois::disable_conflict_detection(),
        galois::loopname("EdgeAfforest-LCS-Link"));

    galois::do_all(
        galois::iterate(*graph),
        [&](const GNode& src) {
          auto& sdata = graph->GetData<NodeComponent>(src);
          sdata->compress();
        },
        galois::steal(), galois::loopname("EdgeAfforest-LCS-Compress"));
  }
};

/**
 * Edgetiled CC w/ Afforest sampling
 */
struct EdgeTiledAfforestAlgo {
  struct NodeAfforest : public galois::UnionFindNode<NodeAfforest> {
    using ComponentType = NodeAfforest*;

    NodeAfforest()
        : galois::UnionFindNode<NodeAfforest>(const_cast<NodeAfforest*>(this)) {
    }
    NodeAfforest(const NodeAfforest& o)
        : galois::UnionFindNode<NodeAfforest>(o.m_component) {}

    ComponentType component() { return this->get(); }
    bool isRepComp(unsigned int) { return false; }  // verify

  public:
    void link(NodeAfforest* b) {
      NodeAfforest* a = m_component.load(std::memory_order_relaxed);
      b = b->m_component.load(std::memory_order_relaxed);
      while (a != b) {
        if (a < b)
          std::swap(a, b);
        // Now a > b
        NodeAfforest* ac = a->m_component.load(std::memory_order_relaxed);
        if ((ac == a && a->m_component.compare_exchange_strong(a, b)) ||
            (b == ac))
          break;
        a = (a->m_component.load(std::memory_order_relaxed))
                ->m_component.load(std::memory_order_relaxed);
        b = b->m_component.load(std::memory_order_relaxed);
      }
    }
  };

  struct NodeComponent {
    using ArrowType = arrow::CTypeTraits<uint64_t>::ArrowType;
    using ViewType = galois::PODPropertyView<NodeAfforest::ComponentType>;
  };

  using NodeData = std::tuple<NodeComponent>;
  using EdgeData = std::tuple<>;
  typedef galois::graphs::PropertyGraph<NodeData, EdgeData> Graph;
  typedef typename Graph::Node GNode;

  void Initialize(Graph* graph) {
    galois::do_all(galois::iterate(*graph), [&](const GNode& node) {
      graph->GetData<NodeComponent>(node) = new NodeAfforest();
    });
  }

  void Deallocate(Graph* graph) {
    galois::do_all(galois::iterate(*graph), [&](const GNode& node) {
      delete graph->GetData<NodeComponent>(node);
    });
  }

  using ComponentType = NodeAfforest::ComponentType;

  struct EdgeTile {
    GNode src;
    Graph::edge_iterator beg;
    Graph::edge_iterator end;
  };

  void operator()(Graph* graph) {
    // (bozhi) should NOT go through single direction in sampling step: nodes
    // with edges less than NEIGHBOR_SAMPLES will fail
    galois::do_all(
        galois::iterate(*graph),
        [&](const GNode& src) {
          auto ii = graph->edge_begin(src);
          const auto end = graph->edge_end(src);
          for (uint32_t r = 0; r < NEIGHBOR_SAMPLES && ii < end; ++r, ++ii) {
            const auto& dest = *graph->GetEdgeDest(ii);
            auto& sdata = graph->GetData<NodeComponent>(src);
            auto& ddata = graph->GetData<NodeComponent>(dest);
            sdata->link(ddata);
          }
        },
        galois::steal(), galois::loopname("EdgetiledAfforest-VNS-Link"));

    galois::do_all(
        galois::iterate(*graph),
        [&](const GNode& src) {
          auto& sdata = graph->GetData<NodeComponent>(src);
          sdata->compress();
        },
        galois::steal(), galois::loopname("EdgetiledAfforest-VNS-Compress"));

    galois::StatTimer StatTimer_Sampling("EdgetiledAfforest-LCS-Sampling");
    StatTimer_Sampling.start();
    const ComponentType c =
        approxLargestComponent<ComponentType, Graph, NodeComponent>(graph);
    StatTimer_Sampling.stop();

    galois::InsertBag<EdgeTile> works;
    std::cout << "INFO: Using edge tile size of " << EDGE_TILE_SIZE
              << " and chunk size of " << CHUNK_SIZE << "\n";
    galois::do_all(
        galois::iterate(*graph),
        [&](const GNode& src) {
          auto& sdata = graph->GetData<NodeComponent>(src);
          if (sdata->component() == c)
            return;
          auto beg = graph->edge_begin(src);
          const auto end = graph->edge_end(src);

          for (std::advance(beg, NEIGHBOR_SAMPLES.getValue());
               beg + EDGE_TILE_SIZE < end;) {
            auto ne = beg + EDGE_TILE_SIZE;
            assert(ne < end);
            works.push_back(EdgeTile{src, beg, ne});
            beg = ne;
          }

          if ((end - beg) > 0) {
            works.push_back(EdgeTile{src, beg, end});
          }
        },
        galois::loopname("EdgetiledAfforest-LCS-Tiling"), galois::steal());

    galois::do_all(
        galois::iterate(works),
        [&](const EdgeTile& tile) {
          auto& sdata = graph->GetData<NodeComponent>(tile.src);
          if (sdata->component() == c)
            return;
          for (auto ii = tile.beg; ii < tile.end; ++ii) {
            const GNode& dest = *graph->GetEdgeDest(ii);
            auto& ddata = graph->GetData<NodeComponent>(dest);
            sdata->link(ddata);
          }
        },
        galois::steal(), galois::chunk_size<CHUNK_SIZE>(),
        galois::loopname("EdgetiledAfforest-LCS-Link"));

    galois::do_all(
        galois::iterate(*graph),
        [&](const GNode& src) {
          auto& sdata = graph->GetData<NodeComponent>(src);
          sdata->compress();
        },
        galois::steal(), galois::loopname("EdgetiledAfforest-LCS-Compress"));
  }
};

template <typename Graph, typename NodeIndex>
bool
verify(
    Graph*,
    typename std::enable_if<galois::graphs::is_segmented<Graph>::value>::type* =
        0) {
  return true;
}

template <typename Graph, typename NodeIndex>
bool
verify(
    Graph* graph, typename std::enable_if<
                      !galois::graphs::is_segmented<Graph>::value>::type* = 0) {
  using GNode = typename Graph::Node;

  auto is_bad = [&graph](const GNode& n) {
    auto& me = graph->template GetData<NodeIndex>(n);
    for (auto ii : graph->edges(n)) {
      const GNode& dest = *graph->GetEdgeDest(ii);
      auto& data = graph->template GetData<NodeIndex>(dest);
      if (data->component() != me->component()) {
        std::cerr << std::dec << "not in same component: " << (unsigned int)n
                  << " (" << me->component() << ")"
                  << " and " << (unsigned int)dest << " (" << data->component()
                  << ")"
                  << "\n";
        return true;
      }
    }
    return false;
  };

  return galois::ParallelSTL::find_if(graph->begin(), graph->end(), is_bad) ==
         graph->end();
}

template <>
bool
verify<LabelPropAlgo::Graph, LabelPropAlgo::NodeComponent>(
    LabelPropAlgo::Graph* graph,
    typename std::enable_if<
        !galois::graphs::is_segmented<LabelPropAlgo::Graph>::value>::type*) {
  using GNode = typename LabelPropAlgo::Graph::Node;
  auto is_bad = [&graph](const GNode& n) {
    auto& me = graph->template GetData<LabelPropAlgo::NodeComponent>(n);
    for (auto ii : graph->edges(n)) {
      const GNode& dest = *graph->GetEdgeDest(ii);
      auto& data = graph->template GetData<LabelPropAlgo::NodeComponent>(dest);
      if (data != me) {
        std::cerr << std::dec << "not in same component: " << (unsigned int)n
                  << " (" << me << ")"
                  << " and " << (unsigned int)dest << " (" << data << ")"
                  << "\n";
        return true;
      }
    }
    return false;
  };

  return galois::ParallelSTL::find_if(graph->begin(), graph->end(), is_bad) ==
         graph->end();
}

template <typename Algo, typename Graph>
typename Algo::ComponentType
findLargest(Graph* graph) {
  using GNode = typename Graph::Node;
  using ComponentType = typename Algo::ComponentType;

  using Map = galois::gstl::Map<ComponentType, int>;

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

  auto accumMap = galois::make_reducible(reduce, mapIdentity);

  galois::GAccumulator<size_t> accumReps;

  galois::do_all(
      galois::iterate(*graph),
      [&](const GNode& x) {
        auto& n = graph->template GetData<typename Algo::NodeComponent>(x);
        if (n->isRep()) {
          accumReps += 1;
          return;
        }

        // Don't add reps to table to avoid adding components of size
        // 1
        accumMap.update(Map{std::make_pair(n->component(), 1)});
      },
      galois::loopname("CountLargest"));

  Map& map = accumMap.reduce();
  size_t reps = accumReps.reduce();

  using ComponentSizePair = std::pair<ComponentType, int>;

  auto sizeMax = [](const ComponentSizePair& a, const ComponentSizePair& b) {
    if (a.second > b.second) {
      return a;
    }
    return b;
  };

  auto identity = []() { return ComponentSizePair{}; };

  auto maxComp = galois::make_reducible(sizeMax, identity);

  galois::do_all(galois::iterate(map), [&](const ComponentSizePair& x) {
    maxComp.update(x);
  });

  ComponentSizePair largest = maxComp.reduce();

  // Compensate for dropping representative node of components
  double ratio = graph->size() - reps + map.size();
  size_t largestSize = largest.second + 1;
  if (ratio) {
    ratio = largestSize / ratio;
  }

  std::cout << "Total components: " << reps << "\n";
  std::cout << "Number of non-trivial components: " << map.size()
            << " (largest size: " << largestSize << " [" << ratio << "])\n";

  return largest.first;
}

template <>
typename LabelPropAlgo::ComponentType
findLargest<LabelPropAlgo, LabelPropAlgo::Graph>(LabelPropAlgo::Graph* graph) {
  using GNode = typename LabelPropAlgo::Graph::Node;
  using ComponentType = typename LabelPropAlgo::ComponentType;

  using Map = galois::gstl::Map<ComponentType, int>;

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

  auto accumMap = galois::make_reducible(reduce, mapIdentity);

  galois::GAccumulator<size_t> accumReps;

  galois::do_all(
      galois::iterate(*graph),
      [&](const GNode& x) {
        auto& n =
            graph->template GetData<typename LabelPropAlgo::NodeComponent>(x);
        if (n.load() == x) {
          accumReps += 1;
          return;
        }

        // Don't add reps to table to avoid adding components of size
        // 1
        accumMap.update(Map{std::make_pair(n.load(), 1)});
      },
      galois::loopname("CountLargest"));

  Map& map = accumMap.reduce();
  size_t reps = accumReps.reduce();

  using ComponentSizePair = std::pair<ComponentType, int>;

  auto sizeMax = [](const ComponentSizePair& a, const ComponentSizePair& b) {
    if (a.second > b.second) {
      return a;
    }
    return b;
  };

  auto identity = []() { return ComponentSizePair{}; };

  auto maxComp = galois::make_reducible(sizeMax, identity);

  galois::do_all(galois::iterate(map), [&](const ComponentSizePair& x) {
    maxComp.update(x);
  });

  ComponentSizePair largest = maxComp.reduce();

  // Compensate for dropping representative node of components
  double ratio = graph->size() - reps + map.size();
  size_t largestSize = largest.second + 1;
  if (ratio) {
    ratio = largestSize / ratio;
  }

  std::cout << "Total components: " << reps << "\n";
  std::cout << "Number of non-trivial components: " << map.size()
            << " (largest size: " << largestSize << " [" << ratio << "])\n";

  return largest.first;
}

template <typename Algo>
void
run() {
  using Graph = typename Algo::Graph;

  Algo algo;

  std::cout << "Reading from file: " << inputFile << "\n";
  std::unique_ptr<galois::graphs::PropertyFileGraph> pfg =
      MakeFileGraph(inputFile, edge_property_name);

  auto result = ConstructNodeProperties<typename Algo::NodeData>(pfg.get());
  if (!result) {
    GALOIS_LOG_FATAL("cannot make graph: {}", result.error());
  }

  auto pg_result = galois::graphs::PropertyGraph<
      typename Algo::NodeData, typename Algo::EdgeData>::Make(pfg.get());
  if (!pg_result) {
    GALOIS_LOG_FATAL("could not make property graph: {}", pg_result.error());
  }
  Graph graph = pg_result.value();

  std::cout << "Read " << graph.num_nodes() << " nodes, " << graph.num_edges()
            << " edges\n";

  algo.Initialize(&graph);

  galois::Prealloc(1, 3 * graph.size() * sizeof(typename Algo::NodeData));
  galois::reportPageAlloc("MeminfoPre");

  galois::StatTimer execTime("Timer_0");
  execTime.start();
  algo(&graph);
  execTime.stop();

  galois::reportPageAlloc("MeminfoPost");

  if (!skipVerify || largestComponentFilename != "" ||
      permutationFilename != "") {
    findLargest<Algo, Graph>(&graph);
    if (!verify<Graph, typename Algo::NodeComponent>(&graph)) {
      algo.Initialize(&graph);
      GALOIS_DIE("verification failed");
    }
  }
  algo.Deallocate(&graph);
}

int
main(int argc, char** argv) {
  std::unique_ptr<galois::SharedMemSys> G =
      LonestarStart(argc, argv, name, desc, nullptr, &inputFile);

  galois::StatTimer totalTime("TimerTotal");
  totalTime.start();

  if (!symmetricGraph) {
    GALOIS_DIE(
        "This application requires a symmetric graph input;"
        " please use the -symmetricGraph flag "
        " to indicate the input is a symmetric graph.");
  }

  switch (algo) {
  case Algo::async:
    run<AsyncAlgo>();
    break;
  case Algo::edgeasync:
    run<EdgeAsyncAlgo>();
    break;
  case Algo::edgetiledasync:
    run<EdgeTiledAsyncAlgo>();
    break;
  case Algo::blockedasync:
    run<BlockedAsyncAlgo>();
    break;
  case Algo::labelProp:
    run<LabelPropAlgo>();
    break;
  case Algo::serial:
    run<SerialAlgo>();
    break;
  case Algo::synchronous:
    run<SynchronousAlgo>();
    break;
  case Algo::afforest:
    run<AfforestAlgo>();
    break;
  case Algo::edgeafforest:
    run<EdgeAfforestAlgo>();
    break;
  case Algo::edgetiledafforest:
    run<EdgeTiledAfforestAlgo>();
    break;

  default:
    std::cerr << "Unknown algorithm\n";
    abort();
  }

  totalTime.stop();

  return 0;
}
