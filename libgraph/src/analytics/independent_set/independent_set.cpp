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

#include "katana/analytics/independent_set/independent_set.h"

#include <cmath>
#include <type_traits>
#include <utility>
#include <vector>

#include "katana/Bag.h"
#include "katana/Galois.h"
#include "katana/ParallelSTL.h"
#include "katana/Properties.h"
#include "katana/Reduction.h"
#include "katana/Timer.h"
#include "katana/TypedPropertyGraph.h"
#include "katana/analytics/Utils.h"

namespace {

using namespace katana::analytics;

constexpr int kChunkSize = 64;
constexpr float kHashScale = 1.0 / std::numeric_limits<unsigned int>::max();

unsigned int
hash(unsigned int val) {
  val = ((val >> 16) ^ val) * 0x45d9f3b;
  val = ((val >> 16) ^ val) * 0x45d9f3b;
  return (val >> 16) ^ val;
}

enum MatchFlag : char {
  KOtherMatched = false,
  kMatched = true,
  KUnMatched = -1,
};

static_assert(sizeof(MatchFlag) == sizeof(uint8_t));

struct SerialAlgo {
  struct NodeFlag : public katana::PODProperty<uint8_t, MatchFlag> {};
  using NodeData = std::tuple<NodeFlag>;
  using EdgeData = std::tuple<>;

  typedef katana::TypedPropertyGraph<NodeData, EdgeData> Graph;
  typedef typename Graph::Node GNode;

  void Initialize(Graph* graph) {
    for (auto n : *graph) {
      graph->GetData<NodeFlag>(n) = MatchFlag::KUnMatched;
    }
  }

  void operator()(Graph* graph) {
    for (auto n : *graph) {
      if (FindUnmatched(*graph, n)) {
        Match(graph, n);
      }
    }
  }

  bool FindUnmatched(const Graph& graph, GNode src) {
    const auto& src_flag = graph.GetData<NodeFlag>(src);
    if (src_flag != MatchFlag::KUnMatched) {
      return false;
    }
    return std::all_of(
        graph.OutEdges(src).begin(), graph.OutEdges(src).end(), [&](auto ii) {
          auto dest = graph.OutEdgeDst(ii);
          auto& dest_flag = graph.GetData<NodeFlag>(dest);
          return dest_flag != MatchFlag::kMatched;
        });
  }

  void Match(Graph* graph, GNode src) {
    auto& src_flag = graph->GetData<NodeFlag>(src);
    for (auto ii : graph->OutEdges(src)) {
      auto dest = graph->OutEdgeDst(ii);
      auto& dest_flag = graph->GetData<NodeFlag>(dest);
      dest_flag = MatchFlag::KOtherMatched;
    }
    src_flag = MatchFlag::kMatched;
  }
};

template <IndependentSetPlan::Algorithm algo>
struct TransactionalAlgo {
  struct NodeFlag : public katana::PODProperty<uint8_t, MatchFlag> {};
  using NodeData = std::tuple<NodeFlag>;
  using EdgeData = std::tuple<>;

  typedef katana::TypedPropertyGraph<NodeData, EdgeData> Graph;
  typedef typename Graph::Node GNode;

  void Initialize(Graph* graph) {
    for (auto n : *graph) {
      graph->template GetData<NodeFlag>(n) = MatchFlag::KUnMatched;
    }
  }

  struct LocalState {
    bool mod{false};
    explicit LocalState() = default;
  };

  bool build(const Graph& graph, GNode src) {
    auto& src_flag = graph.template GetData<NodeFlag>(src);
    if (src_flag != MatchFlag::KUnMatched) {
      return false;
    }

    for (auto ii : graph.OutEdges(src)) {
      auto dest = graph.OutEdgeDst(ii);
      auto& dest_flag = graph.template GetData<NodeFlag>(dest);
      if (dest_flag == MatchFlag::kMatched) {
        return false;
      }
    }
    return true;
  }

  void modify(Graph* graph, GNode src) {
    auto& src_flag = graph->template GetData<NodeFlag>(src);
    for (auto ii : graph->OutEdges(src)) {
      auto dest = graph->OutEdgeDst(ii);
      auto& dest_flag = graph->template GetData<NodeFlag>(dest);
      dest_flag = MatchFlag::KOtherMatched;
    }
    src_flag = MatchFlag::kMatched;
  }

  template <typename C>
  void processNode(Graph* graph, const GNode& src, C& ctx) {
    bool mod = build(*graph, src);
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

  void operator()(Graph* graph [[maybe_unused]]) {
    //    using DWL = katana::Deterministic<>;
    //
    //    using BSWL = katana::BulkSynchronous<
    //        typename katana::PerSocketChunkFIFO<kChunkSize>>;

    switch (algo) {
      //    case kNondeterministic:
      //      run<BSWL>(graph);
      //      break;
      //    case kDeterministicBase:
      //      run<DWL>(graph);
      //      break;
    default:
      static_assert(algo == -1, "Unknown algorithm");
    }
  }
};

struct PullAlgo {
  struct NodeFlag : public katana::PODProperty<uint8_t, MatchFlag> {};
  using NodeData = std::tuple<NodeFlag>;
  using EdgeData = std::tuple<>;

  typedef katana::TypedPropertyGraph<NodeData, EdgeData> Graph;
  typedef typename Graph::Node GNode;

  using Bag = katana::InsertBag<GNode>;

  void Initialize(Graph* graph) {
    for (auto n : *graph) {
      graph->GetData<NodeFlag>(n) = MatchFlag::KUnMatched;
    }
  }

  using Counter = katana::GAccumulator<size_t>;

  template <typename R>
  void Pull(
      const R& range, const Graph& graph, Bag& matched, Bag& otherMatched,
      Bag& next, Counter& numProcessed) {
    katana::do_all(
        range,
        [&](const GNode& src) {
          numProcessed += 1;
          const auto& n_flag = graph.GetData<NodeFlag>(src);
          if (n_flag == MatchFlag::KOtherMatched) {
            return;
          }

          MatchFlag flag = MatchFlag::kMatched;
          for (auto edge : graph.OutEdges(src)) {
            auto dest = graph.OutEdgeDst(edge);
            if (dest >= src) {
              continue;
            }

            const auto& dest_flag = graph.GetData<NodeFlag>(dest);
            if (dest_flag == MatchFlag::kMatched) {
              flag = MatchFlag::KOtherMatched;
              break;
            } else if (dest_flag == MatchFlag::KUnMatched) {
              flag = MatchFlag::KUnMatched;
            }
          }

          if (flag == MatchFlag::KUnMatched) {
            next.push_back(src);
          } else if (flag == MatchFlag::kMatched) {
            matched.push_back(src);
          } else {
            otherMatched.push_back(src);
          }
        },
        katana::loopname("IndependentSet-pull"), katana::steal());
  }

  inline void Take(MatchFlag flag, Bag& bag, Graph* graph, Counter& numTaken) {
    katana::do_all(
        katana::iterate(bag),
        [&](const GNode& src) {
          auto& n_flag = graph->GetData<NodeFlag>(src);
          numTaken += 1;
          n_flag = flag;
        },
        katana::loopname("IndependentSet-take"));
  }

  void operator()(Graph* graph) {
    size_t rounds = 0;
    Counter numProcessed;
    Counter numTaken;

    auto cur = std::make_unique<Bag>();
    auto next = std::make_unique<Bag>();
    Bag matched;
    Bag otherMatched;
    uint64_t size = graph->size();
    const uint64_t delta = graph->size() / 25;

    Graph::iterator ii = graph->begin();
    Graph::iterator ei = graph->begin();

    while (size > 0) {
      numProcessed.reset();

      if (!cur->empty()) {
        Pull(
            katana::iterate(*cur), *graph, matched, otherMatched, *next,
            numProcessed);
      }

      size_t numCur = numProcessed.reduce();
      ei = katana::safe_advance(
          ei, graph->end(),
          Graph::iterator::difference_type(std::min(size, delta) - numCur));

      if (ii != ei) {
        Pull(
            katana::iterate(ii, ei), *graph, matched, otherMatched, *next,
            numProcessed);
      }

      ii = ei;

      numTaken.reset();

      Take(MatchFlag::kMatched, matched, graph, numTaken);
      Take(MatchFlag::KOtherMatched, otherMatched, graph, numTaken);

      KATANA_LOG_ASSERT(numTaken.reduce() > 0);

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

const auto kPermanentYes = uint8_t{0xfe};
const auto kUndecided = uint8_t{0x01};
const auto kTemporaryYes = uint8_t{0x02};
const auto kPermanentNo = uint8_t{0x00};

struct PrioAlgo {
  struct NodeFlag : public katana::PODProperty<uint8_t> {};

  using NodeData = std::tuple<NodeFlag>;
  using EdgeData = std::tuple<>;

  typedef katana::TypedPropertyGraph<NodeData, EdgeData> Graph;
  typedef typename Graph::Node GNode;

  void Initialize(Graph* graph) {
    for (auto n : *graph) {
      graph->GetData<NodeFlag>(n) = kUndecided;
    }
  }

  void operator()(Graph* graph) {
    katana::GAccumulator<size_t> rounds;
    katana::GReduceLogicalOr unmatched;
    katana::PerThreadStorage<std::mt19937*> generator;

    float avg_degree = graph->NumEdges() / graph->size();
    uint8_t in = ~1;
    float scale_avg = ((in / 2) - 1) * avg_degree;

    katana::do_all(
        katana::iterate(*graph),
        [&](const GNode& src) {
          auto& src_flag = graph->GetData<NodeFlag>(src);
          float degree = graph->OutEdges(src).size();
          float x = degree - hash(src) * kHashScale;
          int res = round(scale_avg / (avg_degree + x));
          uint8_t val = (res + res) | 1;
          src_flag = val;
        },
        katana::loopname("IndependentSet-init-prio"));

    do {
      unmatched.reset();
      katana::do_all(
          katana::iterate(*graph),
          [&](const GNode& src) {
            auto& src_flag = graph->GetData<NodeFlag>(src);

            if (!(src_flag & kUndecided)) {
              return;
            }

            for (auto edge : graph->OutEdges(src)) {
              auto dest = graph->OutEdgeDst(edge);

              auto dest_flag = graph->GetData<NodeFlag>(dest);

              if (dest_flag == kPermanentYes) {  // matched, highest prio
                src_flag = kPermanentNo;
                unmatched.update(true);
                return;
              }

              if (src_flag > dest_flag) {
                continue;
              }
              if (src_flag == dest_flag) {
                if (src > dest) {
                  continue;
                } else if (src == dest) {
                  src_flag = kPermanentNo;  // other_matched
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
            src_flag = kPermanentYes;  // matched, highest prio
          },
          katana::loopname("IndependentSet-execute"), katana::steal());

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

  typedef katana::TypedPropertyGraph<NodeData, EdgeData> Graph;
  typedef typename Graph::Node GNode;

  void Initialize(Graph* graph) {
    for (auto n : *graph) {
      graph->GetData<NodeFlag>(n) = kUndecided;
    }
  }

  struct EdgeTile {
    GNode src;
    Graph::edge_iterator beg;
    Graph::edge_iterator end;
    bool flag;
  };

  void operator()(Graph* graph) {
    katana::GAccumulator<size_t> rounds;
    katana::GReduceLogicalOr unmatched;
    katana::PerThreadStorage<std::mt19937*> generator;
    katana::InsertBag<EdgeTile> works;
    constexpr int kEdgeTileSize = 64;

    float avg_degree = graph->NumEdges() / float(graph->size());
    uint8_t in = ~1;
    float scale_avg = ((in / 2) - 1) * avg_degree;

    katana::do_all(
        katana::iterate(*graph),
        [&](const GNode& src) {
          auto& src_flag = graph->GetData<NodeFlag>(src);
          auto rng = graph->OutEdges(src);
          auto beg = rng.begin();
          const auto end = rng.end();

          float degree = float(graph->OutDegree(src));
          float x = degree - hash(src) * kHashScale;
          int res = round(scale_avg / (avg_degree + x));
          uint8_t val = (res + res) | 0x03;

          src_flag = val;
          KATANA_LOG_DEBUG_ASSERT(beg <= end);
          if ((end - beg) > kEdgeTileSize) {
            for (; beg + kEdgeTileSize < end;) {
              auto ne = beg + kEdgeTileSize;
              KATANA_LOG_DEBUG_ASSERT(ne < end);
              works.push_back(EdgeTile{src, beg, ne, false});
              beg = ne;
            }
          }
          if ((end - beg) > 0) {
            works.push_back(EdgeTile{src, beg, end, false});
          }
        },
        katana::loopname("IndependentSet-init-prio"), katana::steal());

    do {
      unmatched.reset();
      katana::do_all(
          katana::iterate(works),
          [&](EdgeTile& tile) {
            GNode src = tile.src;

            auto& src_flag = graph->GetData<NodeFlag>(src);

            if ((src_flag & kUndecided)) {  // is undecided

              for (auto edge = tile.beg; edge != tile.end; ++edge) {
                auto dest = graph->OutEdgeDst(*edge);

                auto& dest_flag = graph->GetData<NodeFlag>(dest);

                if (dest_flag ==
                    kPermanentYes) {  // permanent matched, highest prio
                  src_flag = kPermanentNo;
                  return;
                }

                if (src_flag > dest_flag) {
                  continue;
                } else if (src_flag == dest_flag) {
                  if (src > dest) {
                    continue;
                  } else if (src == dest) {
                    src_flag = kPermanentNo;  // other_matched
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
          katana::loopname("IndependentSet-execute"), katana::steal());

      katana::do_all(
          katana::iterate(works),
          [&](EdgeTile& tile) {
            auto src = tile.src;
            auto& src_flag = graph->GetData<NodeFlag>(src);

            if ((src_flag & kUndecided) &&
                tile.flag == false) {      // undecided and temporary no
              src_flag &= ~kTemporaryYes;  // 0x1111 1101, not temporary yes
            }
          },
          katana::loopname("IndependentSet-match_reduce"));

      katana::do_all(
          katana::iterate(*graph),
          [&](const GNode& src) {
            auto& src_flag = graph->GetData<NodeFlag>(src);
            if ((src_flag & kUndecided) != 0) {       // undecided
              if ((src_flag & kTemporaryYes) != 0) {  // temporary yes
                src_flag = kPermanentYes;  // 0x1111 1110, permanent yes
                for (auto edge : graph->OutEdges(src)) {
                  auto dest = graph->OutEdgeDst(edge);

                  auto& dest_flag = graph->GetData<NodeFlag>(dest);
                  dest_flag =
                      kPermanentNo;  // MatchFlag::KOtherMatched, permanent no
                }
              } else {
                src_flag |= kUndecided |
                            kTemporaryYes;  // 0x0000 0011, temp yes, undecided
              }
            }
          },
          katana::loopname("IndependentSet-match_update"), katana::steal());

      rounds += 1;
    } while (unmatched.reduce());

    katana::ReportStatSingle(
        "IndependentSet-prioAlgo", "rounds", rounds.reduce());
  }
};

struct IsBad {
  struct NodeFlag : public katana::PODProperty<uint8_t> {};
  using NodeData = std::tuple<NodeFlag>;
  using EdgeData = std::tuple<>;

  typedef katana::TypedPropertyGraph<NodeData, EdgeData> Graph;

  using GNode = typename Graph::Node;

  const Graph& graph_;

  IsBad(const Graph& g) : graph_(g) {}

  bool operator()(const GNode& n) const {
    const auto& src_flag = graph_.template GetData<NodeFlag>(n);
    if (src_flag != true && src_flag != false) {
      // Fail if we see something that isn't strictly a boolean.
      // This could happen if an algorithm leaves an unmatched node behind.
      return true;
    }
    if (src_flag) {
      for (auto ii : graph_.OutEdges(n)) {
        auto dest = graph_.OutEdgeDst(ii);
        const auto& dest_flag = graph_.template GetData<NodeFlag>(dest);

        if (dest != n && dest_flag) {
          // Fail if two set members are connected by an egde.
          return true;
        }
      }
    }
    return false;
  }
};

template <typename Algo>
katana::Result<void>
Run(const std::shared_ptr<katana::PropertyGraph>& pg,
    const std::string& output_property_name, katana::TxnContext* txn_ctx) {
  using Graph = typename Algo::Graph;
  using GNode = typename Graph::Node;
  auto result = pg->ConstructNodeProperties<typename Algo::NodeData>(
      txn_ctx, {output_property_name});
  if (!result) {
    return result.error();
  }

  auto pg_result = katana::TypedPropertyGraph<
      typename Algo::NodeData,
      typename Algo::EdgeData>::Make(pg, {output_property_name}, {});
  if (!pg_result) {
    return pg_result.error();
  }
  Graph graph = pg_result.value();

  Algo impl;

  impl.Initialize(&graph);

  katana::EnsurePreallocated(
      1, kChunkSize * (sizeof(GNode) + sizeof(typename Algo::NodeFlag)) *
             graph.size());

  katana::ReportPageAllocGuard page_alloc;
  katana::StatTimer exec_time("IndependentSet");

  exec_time.start();
  impl(&graph);
  exec_time.stop();
  page_alloc.Report();

  if (std::is_same<Algo, PrioAlgo>::value ||
      std::is_same<Algo, EdgeTiledPrioAlgo>::value) {
    // For these algorithms we need to translate the flags into MatchFlag/bool.
    // Check for errors as we go since it costs almost nothing.
    katana::GReduceLogicalOr has_error;
    katana::do_all(
        katana::iterate(graph),
        [&](const GNode& src) {
          auto& src_flag = graph.template GetData<typename Algo::NodeFlag>(src);
          if (src_flag == kPermanentYes) {
            src_flag = MatchFlag::kMatched;
          } else if (src_flag == kPermanentNo) {
            src_flag = MatchFlag::KOtherMatched;
          } else {
            has_error.update(true);
          }
        },
        katana::loopname("verify_change"), katana::no_stats());
    if (has_error.reduce()) {
      return katana::ErrorCode::AssertionFailed;
    }
  }

  return katana::ResultSuccess();
}

}  // namespace

katana::Result<void>
katana::analytics::IndependentSet(
    const std::shared_ptr<PropertyGraph>& pg,
    const std::string& output_property_name, katana::TxnContext* txn_ctx,
    IndependentSetPlan plan) {
  switch (plan.algorithm()) {
  case IndependentSetPlan::kSerial:
    return Run<SerialAlgo>(pg, output_property_name, txn_ctx);
  case IndependentSetPlan::kPull:
    return Run<PullAlgo>(pg, output_property_name, txn_ctx);
  case IndependentSetPlan::kPriority:
    return Run<PrioAlgo>(pg, output_property_name, txn_ctx);
  case IndependentSetPlan::kEdgeTiledPriority:
    return Run<EdgeTiledPrioAlgo>(pg, output_property_name, txn_ctx);
  default:
    return katana::ErrorCode::InvalidArgument;
  }
}

katana::Result<void>
katana::analytics::IndependentSetAssertValid(
    const std::shared_ptr<PropertyGraph>& pg,
    const std::string& property_name) {
  auto pg_result =
      katana::TypedPropertyGraph<IsBad::NodeData, IsBad::EdgeData>::Make(
          pg, {property_name}, {});
  if (!pg_result) {
    return pg_result.error();
  }
  IsBad::Graph graph = pg_result.value();

  bool ok = katana::ParallelSTL::find_if(
                graph.begin(), graph.end(), IsBad(graph)) == graph.end();
  if (!ok) {
    return katana::ErrorCode::AssertionFailed;
  }

  return katana::ResultSuccess();
}

void
katana::analytics::IndependentSetStatistics::Print(std::ostream& os) const {
  os << "Cardinality = " << cardinality << std::endl;
}

katana::Result<IndependentSetStatistics>
katana::analytics::IndependentSetStatistics::Compute(
    const std::shared_ptr<PropertyGraph>& pg,
    const std::string& property_name) {
  auto property_result = pg->GetNodePropertyTyped<uint8_t>(property_name);
  if (!property_result) {
    return property_result.error();
  }
  auto property = property_result.value();
  auto count = katana::ParallelSTL::count_if(
      uint32_t(0), uint32_t(property->length()),
      [&property](size_t i) { return property->Value(i); });
  return IndependentSetStatistics{uint32_t(count)};
}
