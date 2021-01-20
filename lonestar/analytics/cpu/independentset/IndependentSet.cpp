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

#include <math.h>

#include <algorithm>
#include <iostream>
#include <random>
#include <type_traits>
#include <utility>
#include <vector>

#include "Lonestar/BoilerPlate.h"
#include "katana/Bag.h"
#include "katana/Galois.h"
#include "katana/ParallelSTL.h"
#include "katana/Profile.h"
#include "katana/Reduction.h"
#include "katana/Timer.h"
#include "llvm/Support/CommandLine.h"

const char* name = "Maximal Independent Set";
const char* desc =
    "Computes a maximal independent set (not maximum) of nodes in a graph";
const char* url = "independent_set";

enum Algo { serial, pull, nondet, detBase, prio, edgetiledprio };

namespace cll = llvm::cl;
static cll::opt<std::string> inputFile(
    cll::Positional, cll::desc("<input file>"), cll::Required);

static cll::opt<Algo> algo(
    "algo", cll::desc("Choose an algorithm:"),
    cll::values(
        clEnumVal(serial, "Serial"),
        clEnumVal(
            pull, "Pull-based (node 0 is initially in the independent set)"),
        clEnumVal(nondet, "Non-deterministic, use bulk synchronous worklist"),
        clEnumVal(detBase, "use deterministic worklist"),
        clEnumVal(
            prio,
            "prio algo based on Martin's GPU ECL-MIS algorithm (default)"),
        clEnumVal(
            edgetiledprio,
            "edge-tiled prio algo based on Martin's GPU ECL-MIS algorithm")),
    cll::init(prio));

enum MatchFlag : char { KUnMatched, KOtherMatched, Matched };

struct SerialAlgo {
  struct NodeFlag {
    using ArrowType = arrow::CTypeTraits<uint8_t>::ArrowType;
    using ViewType = katana::PODPropertyView<MatchFlag>;
  };
  using NodeData = std::tuple<NodeFlag>;
  using EdgeData = std::tuple<>;

  typedef katana::PropertyGraph<NodeData, EdgeData> Graph;
  typedef typename Graph::Node GNode;

  void Initialize(Graph* graph) {
    for (auto n : *graph)
      graph->GetData<NodeFlag>(n) = MatchFlag::KUnMatched;
  }
  void operator()(Graph* graph) {
    // for (Graph::iterator ii = graph->begin(), ei = graph->end(); ii != ei; ++ii) {
    for (auto n : *graph) {
      if (findUnmatched(*graph, n))
        match(graph, n);
    }
  }

  bool findUnmatched(const Graph& graph, GNode src) {
    auto& src_flag = graph.GetData<NodeFlag>(src);
    if (src_flag != MatchFlag::KUnMatched)
      return false;

    for (auto ii : graph.edges(src)) {
      auto dest = graph.GetEdgeDest(ii);
      auto& dest_flag = graph.GetData<NodeFlag>(dest);
      if (dest_flag == MatchFlag::Matched)
        return false;
    }
    return true;
  }

  void match(Graph* graph, GNode src) {
    auto& src_flag = graph->GetData<NodeFlag>(src);
    for (auto ii : graph->edges(src)) {
      auto dest = graph->GetEdgeDest(ii);
      auto& dest_flag = graph->GetData<NodeFlag>(dest);
      dest_flag = MatchFlag::KOtherMatched;
    }
    src_flag = MatchFlag::Matched;
  }
};

template <Algo algo>
struct DefaultAlgo {
  struct NodeFlag {
    using ArrowType = arrow::CTypeTraits<uint8_t>::ArrowType;
    using ViewType = katana::PODPropertyView<MatchFlag>;
  };
  using NodeData = std::tuple<NodeFlag>;
  using EdgeData = std::tuple<>;

  typedef katana::PropertyGraph<NodeData, EdgeData> Graph;
  typedef typename Graph::Node GNode;

  void Initialize(Graph* graph) {
    for (auto n : *graph)
      graph->template GetData<NodeFlag>(n) = MatchFlag::KUnMatched;
  }

  struct LocalState {
    bool mod;
    explicit LocalState() : mod(false) {}
  };

  bool build(const Graph& graph, GNode src) {
    auto& src_flag = graph.template GetData<NodeFlag>(src);
    if (src_flag != MatchFlag::KUnMatched)
      return false;

    for (auto ii : graph.edges(src)) {
      auto dest = graph.GetEdgeDest(ii);
      auto& dest_flag = graph.template GetData<NodeFlag>(dest);
      if (dest_flag == MatchFlag::Matched)
        return false;
    }
    return true;
  }

  void modify(Graph* graph, GNode src) {
    auto& src_flag = graph->template GetData<NodeFlag>(src);
    for (auto ii : graph->edges(src)) {
      auto dest = graph->GetEdgeDest(ii);
      auto& dest_flag = graph->template GetData<NodeFlag>(dest);
      dest_flag = MatchFlag::KOtherMatched;
    }
    src_flag = MatchFlag::Matched;
  }

  template <typename C>
  void processNode(Graph* graph, const GNode& src, C& ctx) {
    bool mod;
    mod = build(*graph, src);
    [[maybe_unused]] auto& src_flag = graph->template GetData<NodeFlag>(src);
    ctx.cautiousPoint();  // Failsafe point

    if (mod) {
      modify(graph, src);
    }
  }

  template <typename WL, typename... Args>
  void run(Graph* graph, Args&&... args) {
    auto detID = [](const GNode& x) { return x; };

    katana::for_each(
        katana::iterate(*graph),
        [&, this](const GNode& src, auto& ctx) {
          this->processNode(graph, src, ctx);
        },
        katana::no_pushes(), katana::wl<WL>(), katana::loopname("DefaultAlgo"),
        katana::det_id<decltype(detID)>(detID),
        katana::local_state<LocalState>(), std::forward<Args>(args)...);
  }

  void operator()(Graph* graph) {
    using DWL = katana::Deterministic<>;

    using BSWL =
        katana::BulkSynchronous<typename katana::PerSocketChunkFIFO<64>>;

    switch (algo) {
    case nondet:
      run<BSWL>(graph);
      break;
    case detBase:
      run<DWL>(graph);
      break;
    default:
      std::cerr << "Unknown algorithm" << algo << "\n";
      abort();
    }
  }
};

struct PullAlgo {
  struct NodeFlag {
    using ArrowType = arrow::CTypeTraits<uint8_t>::ArrowType;
    using ViewType = katana::PODPropertyView<MatchFlag>;
  };
  using NodeData = std::tuple<NodeFlag>;
  using EdgeData = std::tuple<>;

  typedef katana::PropertyGraph<NodeData, EdgeData> Graph;
  typedef typename Graph::Node GNode;

  using Bag = katana::InsertBag<GNode>;

  void Initialize(Graph* graph) {
    for (auto n : *graph)
      graph->GetData<NodeFlag>(n) = MatchFlag::KUnMatched;
  }

  using Counter = katana::GAccumulator<size_t>;

  template <typename R>
  void pull(
      const R& range, const Graph& graph, Bag& matched, Bag& otherMatched,
      Bag& next, Counter& numProcessed) {
    katana::do_all(
        range,
        [&](const GNode& src) {
          numProcessed += 1;
          auto& n_flag = graph.GetData<NodeFlag>(src);
          if (n_flag == MatchFlag::KOtherMatched)
            return;

          MatchFlag flag = MatchFlag::Matched;
          for (auto edge : graph.edges(src)) {
            auto dest = graph.GetEdgeDest(edge);
            if (*dest >= src) {
              continue;
            }

            auto& dest_flag = graph.GetData<NodeFlag>(dest);
            if (dest_flag == MatchFlag::Matched) {
              flag = MatchFlag::KOtherMatched;
              break;
            } else if (dest_flag == MatchFlag::KUnMatched) {
              flag = MatchFlag::KUnMatched;
            }
          }

          if (flag == MatchFlag::KUnMatched) {
            next.push_back(src);
          } else if (flag == MatchFlag::Matched) {
            matched.push_back(src);
          } else {
            otherMatched.push_back(src);
          }
        },
        katana::loopname("pull"));
  }

  template <MatchFlag Flag>
  void take(Bag& bag, Graph* graph, Counter& numTaken) {
    katana::do_all(
        katana::iterate(bag),
        [&](const GNode& src) {
          auto& n_flag = graph->GetData<NodeFlag>(src);
          numTaken += 1;
          n_flag = Flag;
        },
        katana::loopname("take"));
  }

  void operator()(Graph* graph) {
    size_t rounds = 0;
    Counter numProcessed;
    Counter numTaken;

    Bag bags[2];
    Bag* cur = &bags[0];
    Bag* next = &bags[1];
    Bag matched;
    Bag otherMatched;
    uint64_t size = graph->size();
    uint64_t delta = graph->size() / 25;

    Graph::iterator ii = graph->begin();
    Graph::iterator ei = graph->begin();

    while (size > 0) {
      numProcessed.reset();

      if (!cur->empty()) {
        pull(
            katana::iterate(*cur), *graph, matched, otherMatched, *next,
            numProcessed);
      }

      size_t numCur = numProcessed.reduce();
      std::advance(ei, std::min(size, delta) - numCur);

      if (ii != ei) {
        pull(
            katana::iterate(ii, ei), *graph, matched, otherMatched, *next,
            numProcessed);
      }

      ii = ei;

      numTaken.reset();

      take<MatchFlag::Matched>(matched, graph, numTaken);
      take<MatchFlag::KOtherMatched>(otherMatched, graph, numTaken);

      cur->clear();
      matched.clear();
      otherMatched.clear();
      std::swap(cur, next);
      rounds += 1;
      KATANA_LOG_DEBUG_ASSERT(size >= numTaken.reduce());
      size -= numTaken.reduce();
    }

    katana::ReportStatSingle("IndependentSet-PullAlgo", "rounds", rounds);
  }
};

struct PrioAlgo {
  struct NodeFlag : public katana::PODProperty<uint8_t> {};

  using NodeData = std::tuple<NodeFlag>;
  using EdgeData = std::tuple<>;

  typedef katana::PropertyGraph<NodeData, EdgeData> Graph;
  typedef typename Graph::Node GNode;

  void Initialize(Graph* graph) {
    for (auto n : *graph)
      graph->GetData<NodeFlag>(n) = uint8_t{0x01};
  }

  unsigned int hash(unsigned int val) const {
    val = ((val >> 16) ^ val) * 0x45d9f3b;
    val = ((val >> 16) ^ val) * 0x45d9f3b;
    return (val >> 16) ^ val;
  }

  void operator()(Graph* graph) {
    katana::GAccumulator<size_t> rounds;
    katana::GAccumulator<float> nedges;
    katana::GReduceLogicalOr unmatched;
    katana::PerThreadStorage<std::mt19937*> generator;

    katana::do_all(
        katana::iterate(*graph),
        [&](const GNode& src) {
          nedges += std::distance(graph->edge_begin(src), graph->edge_end(src));
        },
        katana::loopname("cal_degree"), katana::steal());

    float nedges_tmp = nedges.reduce();
    float avg_degree = nedges_tmp / (float)graph->size();
    uint8_t in = ~1;
    float scale_avg = ((in / 2) - 1) * avg_degree;

    katana::do_all(
        katana::iterate(*graph),
        [&](const GNode& src) {
          auto& src_flag = graph->GetData<NodeFlag>(src);
          float degree = (float)std::distance(
              graph->edge_begin(src), graph->edge_end(src));
          float x = degree - hash(src) * 0.00000000023283064365386962890625f;
          int res = round(scale_avg / (avg_degree + x));
          uint8_t val = (res + res) | 1;
          src_flag = val;
        },
        katana::loopname("init-prio"), katana::steal());

    do {
      unmatched.reset();
      katana::do_all(
          katana::iterate(*graph),
          [&](const GNode& src) {
            auto& src_flag = graph->GetData<NodeFlag>(src);

            if (!(src_flag & uint8_t{1}))
              return;

            for (auto edge : graph->edges(src)) {
              auto dest = graph->GetEdgeDest(edge);

              auto& dest_flag = graph->GetData<NodeFlag>(dest);

              if (dest_flag == uint8_t{0xfe}) {  // matched, highest prio
                src_flag = uint8_t{0x00};
                unmatched.update(true);
                return;
              }

              if (src_flag > dest_flag)
                continue;
              else if (src_flag == dest_flag) {
                if (src > *dest)
                  continue;
                else if (src == *dest) {
                  src_flag = uint8_t{0x00};  // other_matched
                  return;
                } else {
                  unmatched.update(true);
                  return;
                }
              } else {
                unmatched.update(true);
                return;
              }
            }
            src_flag = uint8_t{0xfe};  // matched, highest prio
          },
          katana::loopname("execute"), katana::steal());

      rounds += 1;
    } while (unmatched.reduce());

    katana::ReportStatSingle(
        "IndependentSet-prioAlgo", "rounds", rounds.reduce());
  }
};

struct EdgeTiledPrioAlgo {
  struct NodeFlag : public katana::PODProperty<uint8_t> {};

  using NodeData = std::tuple<NodeFlag>;
  using EdgeData = std::tuple<>;

  typedef katana::PropertyGraph<NodeData, EdgeData> Graph;
  typedef typename Graph::Node GNode;

  void Initialize(Graph* graph) {
    for (auto n : *graph)
      graph->GetData<NodeFlag>(n) = uint8_t{0x01};
  }

  struct EdgeTile {
    GNode src;
    Graph::edge_iterator beg;
    Graph::edge_iterator end;
    bool flag;
  };

  unsigned int hash(unsigned int val) const {
    val = ((val >> 16) ^ val) * 0x45d9f3b;
    val = ((val >> 16) ^ val) * 0x45d9f3b;
    return (val >> 16) ^ val;
  }

  void operator()(Graph* graph) {
    katana::GAccumulator<size_t> rounds;
    katana::GAccumulator<float> nedges;
    katana::GReduceLogicalOr unmatched;
    katana::PerThreadStorage<std::mt19937*> generator;
    katana::InsertBag<EdgeTile> works;
    const int EDGE_TILE_SIZE = 64;
    katana::do_all(
        katana::iterate(*graph),
        [&](const GNode& src) {
          nedges += std::distance(graph->edge_begin(src), graph->edge_end(src));
        },
        katana::loopname("cal_degree"), katana::steal());

    float nedges_tmp = nedges.reduce();
    float avg_degree = nedges_tmp / (float)graph->size();
    uint8_t in = ~1;
    float scale_avg = ((in / 2) - 1) * avg_degree;

    katana::do_all(
        katana::iterate(*graph),
        [&](const GNode& src) {
          auto& src_flag = graph->GetData<NodeFlag>(src);
          auto beg = graph->edge_begin(src);
          const auto end = graph->edge_end(src);

          float degree = (float)std::distance(beg, end);
          float x = degree - hash(src) * 0.00000000023283064365386962890625f;
          int res = round(scale_avg / (avg_degree + x));
          uint8_t val = (res + res) | 0x03;

          src_flag = val;
          KATANA_LOG_DEBUG_ASSERT(beg <= end);
          if ((end - beg) > EDGE_TILE_SIZE) {
            for (; beg + EDGE_TILE_SIZE < end;) {
              auto ne = beg + EDGE_TILE_SIZE;
              KATANA_LOG_DEBUG_ASSERT(ne < end);
              works.push_back(EdgeTile{src, beg, ne, false});
              beg = ne;
            }
          }
          if ((end - beg) > 0) {
            works.push_back(EdgeTile{src, beg, end, false});
          }
        },
        katana::loopname("init-prio"), katana::steal());

    do {
      unmatched.reset();
      katana::do_all(
          katana::iterate(works),
          [&](EdgeTile& tile) {
            GNode src = tile.src;

            auto& src_flag = graph->GetData<NodeFlag>(src);

            if ((src_flag & uint8_t{1})) {  // is undecided

              for (auto edge = tile.beg; edge != tile.end; ++edge) {
                auto dest = graph->GetEdgeDest(edge);

                auto& dest_flag = graph->GetData<NodeFlag>(dest);

                if (dest_flag ==
                    uint8_t{0xfe}) {  // permanent matched, highest prio
                  src_flag = uint8_t{0x00};
                  return;
                }

                if (src_flag > dest_flag)
                  continue;
                else if (src_flag == dest_flag) {
                  if (src > *dest)
                    continue;
                  else if (src == *dest) {
                    src_flag = uint8_t{0x00};  // other_matched
                    tile.flag = false;
                    return;
                  } else {
                    tile.flag = false;
                    unmatched.update(true);
                    return;
                  }
                } else {
                  tile.flag = false;
                  unmatched.update(true);
                  return;
                }
              }
              tile.flag = true;  // temporary-matched
            }
          },
          katana::loopname("execute"), katana::steal());

      katana::do_all(
          katana::iterate(works),
          [&](EdgeTile& tile) {
            auto src = tile.src;
            auto& src_flag = graph->GetData<NodeFlag>(src);

            if ((src_flag & uint8_t{1}) &&
                tile.flag == false) {     // undecided and temporary no
              src_flag &= uint8_t{0xfd};  // 0x1111 1101, not temporary yes
            }
          },
          katana::loopname("match_reduce"), katana::steal());

      katana::do_all(
          katana::iterate(*graph),
          [&](const GNode& src) {
            auto& src_flag = graph->GetData<NodeFlag>(src);
            if ((src_flag & uint8_t{0x01}) != 0) {    // undecided
              if ((src_flag & uint8_t{0x02}) != 0) {  // temporary yes
                src_flag = uint8_t{0xfe};  // 0x1111 1110, permanent yes
                for (auto edge : graph->edges(src)) {
                  auto dest = graph->GetEdgeDest(edge);

                  auto& dest_flag = graph->GetData<NodeFlag>(dest);
                  dest_flag =
                      uint8_t{0x00};  // MatchFlag::KOtherMatched, permanent no
                }
              } else
                src_flag |= uint8_t{0x03};  // 0x0000 0011, temp yes, undecided
            }
          },
          katana::loopname("match_update"), katana::steal());

      rounds += 1;
    } while (unmatched.reduce());

    katana::ReportStatSingle(
        "IndependentSet-prioAlgo", "rounds", rounds.reduce());
  }
};

template <typename Graph, typename Algo>
struct is_bad {
  using GNode = typename Graph::Node;

  const Graph& graph_;

  is_bad(const Graph& g) : graph_(g) {}

  bool operator()(const GNode& n) const {
    auto& src_flag = graph_.template GetData<typename Algo::NodeFlag>(n);
    if (src_flag == MatchFlag::Matched) {
      for (auto ii : graph_.edges(n)) {
        auto dest = graph_.GetEdgeDest(ii);
        auto& dest_flag =
            graph_.template GetData<typename Algo::NodeFlag>(dest);
        if (*dest != n && dest_flag == MatchFlag::Matched) {
          std::cerr << "double match\n";
          return true;
        }
      }
    } else if (src_flag == MatchFlag::KUnMatched) {
      bool ok = false;
      for (auto ii : graph_.edges(n)) {
        auto dest = graph_.GetEdgeDest(ii);
        auto& dest_flag =
            graph_.template GetData<typename Algo::NodeFlag>(dest);
        if (dest_flag != MatchFlag::KUnMatched) {
          ok = true;
        }
      }
      if (!ok) {
        std::cerr << "not maximal\n";
        return true;
      }
    }
    return false;
  }
};

template <typename Graph, typename Algo>
struct is_matched {
  Graph& graph_;
  using GNode = typename Graph::Node;

  is_matched(Graph& g) : graph_(g) {}

  bool operator()(const GNode& n) const {
    return graph_.template GetData<typename Algo::NodeFlag>(n) ==
           MatchFlag::Matched;
  }
};

template <typename Graph, typename Algo>
bool
verify(Graph* graph, Algo&) {
  using GNode = typename Graph::Node;

  if (std::is_same<Algo, PrioAlgo>::value ||
      std::is_same<Algo, EdgeTiledPrioAlgo>::value) {
    katana::do_all(
        katana::iterate(*graph),
        [&](const GNode& src) {
          auto& src_flag =
              graph->template GetData<typename Algo::NodeFlag>(src);
          if (src_flag == uint8_t{0xfe}) {
            src_flag = MatchFlag::Matched;
          } else if (src_flag == uint8_t{0x00}) {
            src_flag = MatchFlag::KOtherMatched;
          } else
            std::cout << "error in verify_change! Some nodes are not decided."
                      << "\n";
        },
        katana::loopname("verify_change"));
  }

  return katana::ParallelSTL::find_if(
             graph->begin(), graph->end(), is_bad<Graph, Algo>(*graph)) ==
         graph->end();
}

template <typename Algo>
void
run() {
  using Graph = typename Algo::Graph;
  using GNode = typename Graph::Node;

  std::cout << "Reading from file: " << inputFile << "\n";
  std::unique_ptr<katana::PropertyFileGraph> pfg =
      MakeFileGraph(inputFile, edge_property_name);

  auto result = ConstructNodeProperties<typename Algo::NodeData>(pfg.get());
  if (!result) {
    KATANA_LOG_FATAL("failed to construct node properties: {}", result.error());
  }

  auto pg_result = katana::PropertyGraph<
      typename Algo::NodeData, typename Algo::EdgeData>::Make(pfg.get());
  if (!pg_result) {
    KATANA_LOG_FATAL("could not make property graph: {}", pg_result.error());
  }
  Graph graph = pg_result.value();

  std::cout << "Read " << graph.num_nodes() << " nodes, " << graph.num_edges()
            << " edges\n";

  Algo algo;

  katana::Prealloc(
      1, 64 * (sizeof(GNode) + sizeof(typename Algo::NodeFlag)) * graph.size());

  katana::reportPageAlloc("MeminfoPre");
  katana::StatTimer execTime("Timer_0");

  execTime.start();
  algo(&graph);
  execTime.stop();

  katana::reportPageAlloc("MeminfoPost");

  if (!skipVerify && !verify(&graph, algo)) {
    std::cerr << "verification failed\n";
    KATANA_LOG_DEBUG_VASSERT(0, "verification failed");
    abort();
  }

  std::cout << "Cardinality of maximal independent set: "
            << katana::ParallelSTL::count_if(
                   graph.begin(), graph.end(), is_matched<Graph, Algo>(graph))
            << "\n";
}

int
main(int argc, char** argv) {
  std::unique_ptr<katana::SharedMemSys> G =
      LonestarStart(argc, argv, name, desc, url, &inputFile);

  katana::StatTimer totalTime("TimerTotal");
  totalTime.start();

  if (!symmetricGraph) {
    KATANA_DIE(
        "independent set requires a symmetric graph input;"
        " please use the -symmetricGraph flag "
        " to indicate the input is a symmetric graph");
  }

  if (!symmetricGraph) {
    KATANA_DIE(
        "This application requires a symmetric graph input;"
        " please use the -symmetricGraph flag "
        " to indicate the input is a symmetric graph.");
  }

  switch (algo) {
  case serial:
    run<SerialAlgo>();
    break;
  //TODO (gill) This algorithm needs locks and cautious operator.
  case nondet:
    KATANA_LOG_FATAL(
        "This algorithm requires cautious operator which is not supported at "
        "the moment. Please try a different algorithm.");
    run<DefaultAlgo<nondet>>();
    break;
  case detBase:
    KATANA_LOG_FATAL(
        "This algorithm requires cautious operator which is not supported at "
        "the moment. Please try a different algorithm.");
    run<DefaultAlgo<detBase>>();
    break;
  case pull:
    run<PullAlgo>();
    break;
  case prio:
    run<PrioAlgo>();
    break;
  case edgetiledprio:
    run<EdgeTiledPrioAlgo>();
    break;
  default:
    std::cerr << "Unknown algorithm" << algo << "\n";
    abort();
  }

  totalTime.stop();

  return 0;
}
