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

#include "galois/analytics/connected_components/connected_components.h"

#include "galois/ArrowRandomAccessBuilder.h"

using namespace galois::analytics;

const int ConnectedComponentsPlan::kChunkSize = 1;

namespace {

const unsigned int kInfinity = std::numeric_limits<unsigned int>::max();
struct ConnectedComponentsNode
    : public galois::UnionFindNode<ConnectedComponentsNode> {
  using ComponentType = ConnectedComponentsNode*;

  ConnectedComponentsNode()
      : galois::UnionFindNode<ConnectedComponentsNode>(
            const_cast<ConnectedComponentsNode*>(this)) {}
  ConnectedComponentsNode(const ConnectedComponentsNode& o)
      : galois::UnionFindNode<ConnectedComponentsNode>(o.m_component) {}

  ConnectedComponentsNode& operator=(const ConnectedComponentsNode& o) {
    ConnectedComponentsNode c(o);
    std::swap(c, *this);
    return *this;
  }

  ComponentType component() { return this->get(); }
  bool isRepComp(unsigned int) { return false; }
};

struct ConnectedComponentsSerialAlgo {
  using ComponentType = ConnectedComponentsNode*;
  struct NodeComponent {
    using ArrowType = arrow::CTypeTraits<uint64_t>::ArrowType;
    using ViewType = galois::PODPropertyView<ComponentType>;
  };

  using NodeData = std::tuple<NodeComponent>;
  using EdgeData = std::tuple<>;
  typedef galois::graphs::PropertyGraph<NodeData, EdgeData> Graph;
  typedef typename Graph::Node GNode;

  ConnectedComponentsPlan& plan_;
  ConnectedComponentsSerialAlgo(ConnectedComponentsPlan& plan) : plan_(plan) {}

  void Initialize(Graph* graph) {
    galois::do_all(galois::iterate(*graph), [&](const GNode& node) {
      graph->GetData<NodeComponent>(node) = new ConnectedComponentsNode();
    });
  }

  void Deallocate(Graph* graph) {
    galois::do_all(galois::iterate(*graph), [&](const GNode& node) {
      auto& sdata = graph->GetData<NodeComponent>(node);
      auto component_ptr = sdata->component();
      delete sdata;
      sdata = component_ptr;
    });
  }

  void operator()(Graph* graph) {
    for (const GNode& src : *graph) {
      auto& sdata = graph->GetData<NodeComponent>(src);
      for (const auto& ii : graph->edges(src)) {
        auto dest = graph->GetEdgeDest(ii);
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

struct ConnectedComponentsLabelPropAlgo {
  using ComponentType = uint64_t;
  struct NodeComponent {
    using ArrowType = arrow::CTypeTraits<ComponentType>::ArrowType;
    using ViewType = galois::PODPropertyView<std::atomic<ComponentType>>;
  };

  using NodeData = std::tuple<NodeComponent>;
  using EdgeData = std::tuple<>;
  typedef galois::graphs::PropertyGraph<NodeData, EdgeData> Graph;
  typedef typename Graph::Node GNode;

  galois::LargeArray<ComponentType> old_component_;
  ConnectedComponentsPlan& plan_;
  ConnectedComponentsLabelPropAlgo(ConnectedComponentsPlan& plan)
      : plan_(plan) {}

  void Initialize(Graph* graph) {
    old_component_.allocateBlocked(graph->size());
    galois::do_all(galois::iterate(*graph), [&](const GNode& node) {
      graph->GetData<NodeComponent>(node).store(node);
      old_component_[node] = kInfinity;
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
            auto& sdata_old_comp = old_component_[src];
            if (sdata_old_comp > sdata_current_comp) {
              sdata_old_comp = sdata_current_comp;

              changed.update(true);

              for (auto e : graph->edges(src)) {
                auto dest = graph->GetEdgeDest(e);
                auto& ddata_current_comp = graph->GetData<NodeComponent>(dest);
                ComponentType label_new = sdata_current_comp;
                galois::atomicMin(ddata_current_comp, label_new);
              }
            }
          },
          galois::disable_conflict_detection(), galois::steal(),
          galois::loopname("ConnectedComponentsLabelPropAlgo"));
    } while (changed.reduce());
  }
};

struct ConnectedComponentsSynchronousAlgo {
  using ComponentType = ConnectedComponentsNode*;
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
    ConnectedComponentsNode* ddata;
    int count;
    Edge(GNode src, ConnectedComponentsNode* ddata, int count)
        : src(src), ddata(ddata), count(count) {}
  };

  ConnectedComponentsPlan& plan_;
  ConnectedComponentsSynchronousAlgo(ConnectedComponentsPlan& plan)
      : plan_(plan) {}

  void Initialize(Graph* graph) {
    galois::do_all(galois::iterate(*graph), [&](const GNode& node) {
      graph->GetData<NodeComponent>(node) = new ConnectedComponentsNode();
    });
  }

  void Deallocate(Graph* graph) {
    galois::do_all(galois::iterate(*graph), [&](const GNode& node) {
      auto& sdata = graph->GetData<NodeComponent>(node);
      auto component_ptr = sdata->component();
      delete sdata;
      sdata = component_ptr;
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
        auto dest = graph->GetEdgeDest(ii);
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
            ConnectedComponentsNode* src_component = sdata->findAndCompress();
            Graph::edge_iterator ii = graph->edge_begin(src);
            Graph::edge_iterator ei = graph->edge_end(src);
            int count = edge.count + 1;
            std::advance(ii, count);
            for (; ii != ei; ++ii, ++count) {
              auto dest = graph->GetEdgeDest(ii);
              if (src >= *dest)
                continue;
              auto& ddata = graph->GetData<NodeComponent>(dest);
              ConnectedComponentsNode* dest_component =
                  ddata->findAndCompress();
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

    galois::ReportStatSingle("CC-Sync", "rounds", rounds);
    galois::ReportStatSingle("CC-Sync", "empty_merges", empty_merges.reduce());
  }
};

struct ConnectedComponentsAsyncAlgo {
  using ComponentType = ConnectedComponentsNode*;
  struct NodeComponent {
    using ArrowType = arrow::CTypeTraits<uint64_t>::ArrowType;
    using ViewType = galois::PODPropertyView<ComponentType>;
  };

  using NodeData = std::tuple<NodeComponent>;
  using EdgeData = std::tuple<>;
  typedef galois::graphs::PropertyGraph<NodeData, EdgeData> Graph;
  typedef typename Graph::Node GNode;

  ConnectedComponentsPlan& plan_;
  ConnectedComponentsAsyncAlgo(ConnectedComponentsPlan& plan) : plan_(plan) {}

  void Initialize(Graph* graph) {
    galois::do_all(galois::iterate(*graph), [&](const GNode& node) {
      graph->GetData<NodeComponent>(node) = new ConnectedComponentsNode();
    });
  }

  void Deallocate(Graph* graph) {
    galois::do_all(galois::iterate(*graph), [&](const GNode& node) {
      auto& sdata = graph->GetData<NodeComponent>(node);
      auto component_ptr = sdata->component();
      delete sdata;
      sdata = component_ptr;
    });
  }

  void operator()(Graph* graph) {
    galois::GAccumulator<size_t> empty_merges;

    galois::do_all(
        galois::iterate(*graph),
        [&](const GNode& src) {
          auto& sdata = graph->GetData<NodeComponent>(src);

          for (const auto& ii : graph->edges(src)) {
            auto dest = graph->GetEdgeDest(ii);
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

    galois::ReportStatSingle("CC-Async", "empty_merges", empty_merges.reduce());
  }
};

struct ConnectedComponentsEdgeAsyncAlgo {
  using ComponentType = ConnectedComponentsNode*;
  struct NodeComponent {
    using ArrowType = arrow::CTypeTraits<uint64_t>::ArrowType;
    using ViewType = galois::PODPropertyView<ComponentType>;
  };

  using NodeData = std::tuple<NodeComponent>;
  using EdgeData = std::tuple<>;
  typedef galois::graphs::PropertyGraph<NodeData, EdgeData> Graph;
  typedef typename Graph::Node GNode;
  using Edge = std::pair<GNode, typename Graph::edge_iterator>;

  ConnectedComponentsPlan& plan_;
  ConnectedComponentsEdgeAsyncAlgo(ConnectedComponentsPlan& plan)
      : plan_(plan) {}

  void Initialize(Graph* graph) {
    galois::do_all(galois::iterate(*graph), [&](const GNode& node) {
      graph->GetData<NodeComponent>(node) = new ConnectedComponentsNode();
    });
  }

  void Deallocate(Graph* graph) {
    galois::do_all(galois::iterate(*graph), [&](const GNode& node) {
      auto& sdata = graph->GetData<NodeComponent>(node);
      auto component_ptr = sdata->component();
      delete sdata;
      sdata = component_ptr;
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
          auto dest = graph->GetEdgeDest(e.second);
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

    galois::ReportStatSingle("CC-Async", "empty_merges", empty_merges.reduce());
  }
};

struct ConnectedComponentsBlockedAsyncAlgo {
  using ComponentType = ConnectedComponentsNode*;
  struct NodeComponent {
    using ArrowType = arrow::CTypeTraits<uint64_t>::ArrowType;
    using ViewType = galois::PODPropertyView<ComponentType>;
  };

  using NodeData = std::tuple<NodeComponent>;
  using EdgeData = std::tuple<>;
  typedef galois::graphs::PropertyGraph<NodeData, EdgeData> Graph;
  typedef typename Graph::Node GNode;
  using Edge = std::pair<GNode, typename Graph::edge_iterator>;

  ConnectedComponentsPlan& plan_;
  ConnectedComponentsBlockedAsyncAlgo(ConnectedComponentsPlan& plan)
      : plan_(plan) {}

  void Initialize(Graph* graph) {
    galois::do_all(galois::iterate(*graph), [&](const GNode& node) {
      graph->GetData<NodeComponent>(node) = new ConnectedComponentsNode();
    });
  }

  void Deallocate(Graph* graph) {
    galois::do_all(galois::iterate(*graph), [&](const GNode& node) {
      auto& sdata = graph->GetData<NodeComponent>(node);
      auto component_ptr = sdata->component();
      delete sdata;
      sdata = component_ptr;
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
      auto dest = graph->GetEdgeDest(ii);
      auto& ddata = graph->GetData<NodeComponent>(dest);

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

struct ConnectedComponentsEdgeTiledAsyncAlgo {
  using ComponentType = ConnectedComponentsNode*;
  struct NodeComponent {
    using ArrowType = arrow::CTypeTraits<uint64_t>::ArrowType;
    using ViewType = galois::PODPropertyView<ComponentType>;
  };

  using NodeData = std::tuple<NodeComponent>;
  using EdgeData = std::tuple<>;
  typedef galois::graphs::PropertyGraph<NodeData, EdgeData> Graph;
  typedef typename Graph::Node GNode;
  using Edge = std::pair<GNode, typename Graph::edge_iterator>;

  ConnectedComponentsPlan& plan_;
  ConnectedComponentsEdgeTiledAsyncAlgo(ConnectedComponentsPlan& plan)
      : plan_(plan) {}

  void Initialize(Graph* graph) {
    galois::do_all(galois::iterate(*graph), [&](const GNode& node) {
      graph->GetData<NodeComponent>(node) = new ConnectedComponentsNode();
    });
  }

  void Deallocate(Graph* graph) {
    galois::do_all(galois::iterate(*graph), [&](const GNode& node) {
      auto& sdata = graph->GetData<NodeComponent>(node);
      auto component_ptr = sdata->component();
      delete sdata;
      sdata = component_ptr;
    });
  }

  struct EdgeTile {
    // ConnectedComponentsNode* sData;
    GNode src;
    Graph::edge_iterator beg;
    Graph::edge_iterator end;
  };

  /*struct EdgeTileMaker {
      EdgeTile operator() (ConnectedComponentsNode* sdata, Graph::edge_iterator beg,
  Graph::edge_iterator end) const{ return EdgeTile{sdata, beg, end};
      }
  };*/

  void operator()(Graph* graph) {
    galois::GAccumulator<size_t> empty_merges;

    galois::InsertBag<EdgeTile> works;
    galois::do_all(
        galois::iterate(*graph),
        [&](const GNode& src) {
          auto beg = graph->edge_begin(src);
          const auto& end = graph->edge_end(src);

          assert(beg <= end);
          if ((end - beg) > plan_.edge_tile_size()) {
            for (; beg + plan_.edge_tile_size() < end;) {
              const auto& ne = beg + plan_.edge_tile_size();
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
            auto dest = graph->GetEdgeDest(ii);
            if (src >= *dest)
              continue;

            auto& ddata = graph->GetData<NodeComponent>(dest);
            if (!sdata->merge(ddata))
              empty_merges += 1;
          }
        },
        galois::loopname("CC-edgetiledAsync"), galois::steal(),
        galois::chunk_size<ConnectedComponentsPlan::kChunkSize>()  // 16 -> 1
    );

    galois::do_all(
        galois::iterate(*graph),
        [&](const GNode& src) {
          auto& sdata = graph->GetData<NodeComponent>(src);
          sdata->compress();
        },
        galois::steal(), galois::loopname("CC-Async-Compress"));

    galois::ReportStatSingle(
        "CC-edgeTiledAsync", "empty_merges", empty_merges.reduce());
  }
};

template <typename ComponentType, typename Graph, typename NodeIndex>
ComponentType
approxLargestComponent(Graph* graph, uint32_t component_sample_frequency) {
  using map_type = std::unordered_map<
      ComponentType, int, std::hash<ComponentType>,
      std::equal_to<ComponentType>,
      galois::gstl::Pow2Alloc<std::pair<const ComponentType, int>>>;
  using pair_type = std::pair<ComponentType, int>;

  map_type comp_freq(component_sample_frequency);
  std::random_device rd;
  std::mt19937 rng(rd());
  std::uniform_int_distribution<uint32_t> dist(0, graph->size() - 1);
  for (uint32_t i = 0; i < component_sample_frequency; i++) {
    ComponentType ndata = graph->template GetData<NodeIndex>(dist(rng));
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
      " (hit rate ",
      100.0 * (most_frequent->second) / component_sample_frequency, "%)");

  return most_frequent->first;
}

struct ConnectedComponentsAfforestAlgo {
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

  ConnectedComponentsPlan& plan_;
  ConnectedComponentsAfforestAlgo(ConnectedComponentsPlan& plan)
      : plan_(plan) {}

  void Initialize(Graph* graph) {
    galois::do_all(galois::iterate(*graph), [&](const GNode& node) {
      graph->GetData<NodeComponent>(node) = new NodeAfforest();
    });
  }

  void Deallocate(Graph* graph) {
    galois::do_all(galois::iterate(*graph), [&](const GNode& node) {
      auto& sdata = graph->GetData<NodeComponent>(node);
      auto component_ptr = sdata->component();
      delete sdata;
      sdata = component_ptr;
    });
  }
  using ComponentType = NodeAfforest::ComponentType;

  void operator()(Graph* graph) {
    // (bozhi) should NOT go through single direction in sampling step: nodes
    // with edges less than NEIGHBOR_SAMPLES will fail
    for (uint32_t r = 0; r < plan_.neighbor_sample_size(); ++r) {
      galois::do_all(
          galois::iterate(*graph),
          [&](const GNode& src) {
            Graph::edge_iterator ii = graph->edge_begin(src);
            Graph::edge_iterator ei = graph->edge_end(src);
            for (std::advance(ii, r); ii < ei; ii++) {
              auto dest = graph->GetEdgeDest(ii);
              auto& sdata = graph->GetData<NodeComponent>(src);
              ComponentType ddata = graph->GetData<NodeComponent>(dest);
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
        approxLargestComponent<ComponentType, Graph, NodeComponent>(
            graph, plan_.component_sample_frequency());
    StatTimer_Sampling.stop();

    galois::do_all(
        galois::iterate(*graph),
        [&](const GNode& src) {
          auto& sdata = graph->GetData<NodeComponent>(src);
          if (sdata->component() == c)
            return;
          Graph::edge_iterator ii = graph->edge_begin(src);
          Graph::edge_iterator ei = graph->edge_end(src);
          for (std::advance(ii, plan_.neighbor_sample_size()); ii < ei; ++ii) {
            auto dest = graph->GetEdgeDest(ii);
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

struct ConnectedComponentsEdgeAfforestAlgo {
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

  ConnectedComponentsPlan& plan_;
  ConnectedComponentsEdgeAfforestAlgo(ConnectedComponentsPlan& plan)
      : plan_(plan) {}

  void Initialize(Graph* graph) {
    galois::do_all(galois::iterate(*graph), [&](const GNode& node) {
      graph->GetData<NodeComponent>(node) = new NodeAfforestEdge();
    });
  }

  void Deallocate(Graph* graph) {
    galois::do_all(galois::iterate(*graph), [&](const GNode& node) {
      auto& sdata = graph->GetData<NodeComponent>(node);
      auto component_ptr = sdata->component();
      delete sdata;
      sdata = component_ptr;
    });
  }
  void operator()(Graph* graph) {
    // (bozhi) should NOT go through single direction in sampling step: nodes
    // with edges less than NEIGHBOR_SAMPLES will fail
    for (uint32_t r = 0; r < plan_.neighbor_sample_size(); ++r) {
      galois::do_all(
          galois::iterate(*graph),
          [&](const GNode& src) {
            Graph::edge_iterator ii = graph->edge_begin(src);
            Graph::edge_iterator ei = graph->edge_end(src);
            std::advance(ii, r);
            if (ii < ei) {
              auto dest = graph->GetEdgeDest(ii);
              auto& sdata = graph->GetData<NodeComponent>(src);
              auto& ddata = graph->GetData<NodeComponent>(dest);
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
        approxLargestComponent<ComponentType, Graph, NodeComponent>(
            graph, plan_.component_sample_frequency());
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

          for (std::advance(beg, plan_.neighbor_sample_size()); beg < end;
               beg++) {
            auto dest = graph->GetEdgeDest(beg);
            auto& ddata = graph->GetData<NodeComponent>(dest);
            if (src < *dest || c == ddata->component()) {
              works.push_back(std::make_pair(src, *dest));
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
              auto dest = graph->GetEdgeDest(ii);
              ctx.push_back(std::make_pair(*dest, src));
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

struct ConnectedComponentsEdgeTiledAfforestAlgo {
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

  ConnectedComponentsPlan& plan_;
  ConnectedComponentsEdgeTiledAfforestAlgo(ConnectedComponentsPlan& plan)
      : plan_(plan) {}

  void Initialize(Graph* graph) {
    galois::do_all(galois::iterate(*graph), [&](const GNode& node) {
      graph->GetData<NodeComponent>(node) = new NodeAfforest();
    });
  }

  void Deallocate(Graph* graph) {
    galois::do_all(galois::iterate(*graph), [&](const GNode& node) {
      auto& sdata = graph->GetData<NodeComponent>(node);
      auto component_ptr = sdata->component();
      delete sdata;
      sdata = component_ptr;
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
          for (uint32_t r = 0; r < plan_.neighbor_sample_size() && ii < end;
               ++r, ++ii) {
            auto dest = graph->GetEdgeDest(ii);
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
        approxLargestComponent<ComponentType, Graph, NodeComponent>(
            graph, plan_.component_sample_frequency());
    StatTimer_Sampling.stop();

    galois::InsertBag<EdgeTile> works;
    galois::do_all(
        galois::iterate(*graph),
        [&](const GNode& src) {
          auto& sdata = graph->GetData<NodeComponent>(src);
          if (sdata->component() == c)
            return;
          auto beg = graph->edge_begin(src);
          const auto end = graph->edge_end(src);

          for (std::advance(beg, plan_.neighbor_sample_size());
               beg + plan_.edge_tile_size() < end;) {
            auto ne = beg + plan_.edge_tile_size();
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
            auto dest = graph->GetEdgeDest(ii);
            auto& ddata = graph->GetData<NodeComponent>(dest);
            sdata->link(ddata);
          }
        },
        galois::steal(),
        galois::chunk_size<ConnectedComponentsPlan::kChunkSize>(),
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

}  //namespace

template <typename Algorithm>
static galois::Result<void>
ConnectedComponentsWithWrap(
    galois::graphs::PropertyFileGraph* pfg, std::string output_property_name,
    ConnectedComponentsPlan plan) {
  if (auto r = ConstructNodeProperties<
          std::tuple<typename Algorithm::NodeComponent>>(
          pfg, {output_property_name});
      !r) {
    return r.error();
  }
  auto pg_result = Algorithm::Graph::Make(pfg, {output_property_name}, {});
  if (!pg_result) {
    return pg_result.error();
  }

  auto graph = pg_result.value();

  Algorithm algo(plan);

  algo.Initialize(&graph);

  galois::StatTimer execTime("ConnectedComponent");
  execTime.start();

  algo(&graph);
  execTime.stop();

  algo.Deallocate(&graph);

  execTime.stop();

  return galois::ResultSuccess();
}

galois::Result<void>
galois::analytics::ConnectedComponents(
    graphs::PropertyFileGraph* pfg, const std::string& output_property_name,
    ConnectedComponentsPlan plan) {
  switch (plan.algorithm()) {
  case ConnectedComponentsPlan::kSerial:
    return ConnectedComponentsWithWrap<ConnectedComponentsSerialAlgo>(
        pfg, output_property_name, plan);
    break;
  case ConnectedComponentsPlan::kLabelProp:
    return ConnectedComponentsWithWrap<ConnectedComponentsLabelPropAlgo>(
        pfg, output_property_name, plan);
    break;
  case ConnectedComponentsPlan::kSynchronous:
    return ConnectedComponentsWithWrap<ConnectedComponentsSynchronousAlgo>(
        pfg, output_property_name, plan);
    break;
  case ConnectedComponentsPlan::kAsynchronous:
    return ConnectedComponentsWithWrap<ConnectedComponentsAsyncAlgo>(
        pfg, output_property_name, plan);
    break;
  case ConnectedComponentsPlan::kEdgeAsynchronous:
    return ConnectedComponentsWithWrap<ConnectedComponentsEdgeAsyncAlgo>(
        pfg, output_property_name, plan);
    break;
  case ConnectedComponentsPlan::kEdgeTiledAsynchronous:
    return ConnectedComponentsWithWrap<ConnectedComponentsEdgeTiledAsyncAlgo>(
        pfg, output_property_name, plan);
    break;
  case ConnectedComponentsPlan::kBlockedAsynchronous:
    return ConnectedComponentsWithWrap<ConnectedComponentsBlockedAsyncAlgo>(
        pfg, output_property_name, plan);
    break;
  case ConnectedComponentsPlan::kAfforest:
    return ConnectedComponentsWithWrap<ConnectedComponentsAfforestAlgo>(
        pfg, output_property_name, plan);
    break;
  case ConnectedComponentsPlan::kEdgeAfforest:
    return ConnectedComponentsWithWrap<ConnectedComponentsEdgeAfforestAlgo>(
        pfg, output_property_name, plan);
    break;
  case ConnectedComponentsPlan::kEdgeTiledAfforest:
    return ConnectedComponentsWithWrap<
        ConnectedComponentsEdgeTiledAfforestAlgo>(
        pfg, output_property_name, plan);
    break;
  default:
    return ErrorCode::InvalidArgument;
  }
}

galois::Result<void>
galois::analytics::ConnectedComponentsAssertValid(
    graphs::PropertyFileGraph* pfg, const std::string& property_name) {
  using ComponentType = uint64_t;
  struct NodeComponent : public galois::PODProperty<ComponentType> {};

  using NodeData = std::tuple<NodeComponent>;
  using EdgeData = std::tuple<>;
  typedef galois::graphs::PropertyGraph<NodeData, EdgeData> Graph;
  typedef typename Graph::Node GNode;

  auto pg_result = Graph::Make(pfg, {property_name}, {});
  if (!pg_result) {
    return pg_result.error();
  }

  auto graph = pg_result.value();

  auto is_bad = [&graph](const GNode& n) {
    auto& me = graph.template GetData<NodeComponent>(n);
    for (auto ii : graph.edges(n)) {
      auto dest = graph.GetEdgeDest(ii);
      auto& data = graph.template GetData<NodeComponent>(dest);
      if (data != me) {
        GALOIS_LOG_DEBUG(
            "{} (component: {}) must be in same component as {} (component: "
            "{})",
            uint32_t{*dest}, data, n, me);
        return true;
      }
    }
    return false;
  };

  if (galois::ParallelSTL::find_if(graph.begin(), graph.end(), is_bad) !=
      graph.end()) {
    return galois::ErrorCode::AssertionFailed;
  }

  return galois::ResultSuccess();
}

galois::Result<ConnectedComponentsStatistics>
galois::analytics::ConnectedComponentsStatistics::Compute(
    galois::graphs::PropertyFileGraph* pfg, const std::string& property_name) {
  using ComponentType = uint64_t;
  struct NodeComponent : public galois::PODProperty<ComponentType> {};

  using NodeData = std::tuple<NodeComponent>;
  using EdgeData = std::tuple<>;
  typedef galois::graphs::PropertyGraph<NodeData, EdgeData> Graph;
  typedef typename Graph::Node GNode;

  auto pg_result = Graph::Make(pfg, {property_name}, {});
  if (!pg_result) {
    return pg_result.error();
  }

  auto graph = pg_result.value();

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
      galois::iterate(graph),
      [&](const GNode& x) {
        auto& n = graph.template GetData<NodeComponent>(x);
        accumMap.update(Map{std::make_pair(n, 1)});
      },
      galois::loopname("CountLargest"));

  Map& map = accumMap.reduce();
  size_t reps = map.size();

  using ComponentSizePair = std::pair<ComponentType, int>;

  auto sizeMax = [](const ComponentSizePair& a, const ComponentSizePair& b) {
    if (a.second > b.second) {
      return a;
    }
    return b;
  };

  auto identity = []() { return ComponentSizePair{}; };

  auto maxComp = galois::make_reducible(sizeMax, identity);

  galois::GAccumulator<uint64_t> non_trivial_components;
  galois::do_all(galois::iterate(map), [&](const ComponentSizePair& x) {
    maxComp.update(x);
    if (x.second > 1) {
      non_trivial_components += 1;
    }
  });

  ComponentSizePair largest = maxComp.reduce();

  // Compensate for dropping representative node of components
  double ratio_largest_component = graph.size() - reps + map.size();
  size_t largest_component_size = largest.second + 1;
  if (ratio_largest_component) {
    ratio_largest_component = largest_component_size / ratio_largest_component;
  }

  return ConnectedComponentsStatistics{
      reps, non_trivial_components.reduce(), largest_component_size,
      ratio_largest_component};
}

void
galois::analytics::ConnectedComponentsStatistics::Print(std::ostream& os) {
  os << "Total number of components = " << total_components << std::endl;
  os << "Total number of non trivial components = "
     << total_non_trivial_components << std::endl;
  os << "Number of nodes in the largest component = " << largest_component_size
     << std::endl;
  os << "Ratio of nodes in the largest component = " << ratio_largest_component
     << std::endl;
}
