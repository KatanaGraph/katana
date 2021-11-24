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

#include "katana/analytics/connected_components/connected_components.h"

#include "katana/ArrowRandomAccessBuilder.h"
#include "katana/TypedPropertyGraph.h"

using namespace katana::analytics;

const int ConnectedComponentsPlan::kChunkSize = 1;

namespace {

// TODO(amber): Switch to Undirected View after comparing performance changes
using PropGraphView = katana::PropertyGraphViews::Default;

const unsigned int kInfinity = std::numeric_limits<unsigned int>::max();
struct ConnectedComponentsNode
    : public katana::UnionFindNode<ConnectedComponentsNode> {
  using ComponentType = ConnectedComponentsNode*;

  ConnectedComponentsNode()
      : katana::UnionFindNode<ConnectedComponentsNode>(this) {}
  ConnectedComponentsNode(const ConnectedComponentsNode& o)
      : katana::UnionFindNode<ConnectedComponentsNode>(o.m_component) {}

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
  struct NodeComponent : katana::PODProperty<uint64_t, ComponentType> {};

  using NodeData = std::tuple<NodeComponent>;
  using EdgeData = std::tuple<>;
  typedef katana::TypedPropertyGraphView<PropGraphView, NodeData, EdgeData>
      Graph;
  typedef typename Graph::Node GNode;

  const ConnectedComponentsPlan& plan_;
  ConnectedComponentsSerialAlgo(const ConnectedComponentsPlan& plan)
      : plan_(plan) {}

  void Initialize(Graph* graph) {
    katana::do_all(katana::iterate(*graph), [&](const GNode& node) {
      graph->GetData<NodeComponent>(node) = new ConnectedComponentsNode();
    });
  }

  void Deallocate(Graph* graph) {
    katana::do_all(katana::iterate(*graph), [&](const GNode& node) {
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
        auto dest = graph->edge_dest(ii);
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
  struct NodeComponent : public katana::AtomicPODProperty<ComponentType> {};

  using NodeData = std::tuple<NodeComponent>;
  using EdgeData = std::tuple<>;
  typedef katana::TypedPropertyGraphView<PropGraphView, NodeData, EdgeData>
      Graph;
  typedef typename Graph::Node GNode;

  katana::NUMAArray<ComponentType> old_component_;
  ConnectedComponentsPlan& plan_;
  ConnectedComponentsLabelPropAlgo(ConnectedComponentsPlan& plan)
      : plan_(plan) {}

  void Initialize(Graph* graph) {
    old_component_.allocateBlocked(graph->size());
    katana::do_all(katana::iterate(*graph), [&](const GNode& node) {
      graph->GetData<NodeComponent>(node).store(node);
      old_component_[node] = kInfinity;
    });
  }

  void Deallocate(Graph*) {}

  void operator()(Graph* graph) {
    katana::GReduceLogicalOr changed;
    do {
      changed.reset();
      katana::do_all(
          katana::iterate(*graph),
          [&](const GNode& src) {
            auto& sdata_current_comp = graph->GetData<NodeComponent>(src);
            auto& sdata_old_comp = old_component_[src];
            if (sdata_old_comp > sdata_current_comp) {
              sdata_old_comp = sdata_current_comp;

              changed.update(true);

              for (auto e : graph->edges(src)) {
                auto dest = graph->edge_dest(e);
                auto& ddata_current_comp = graph->GetData<NodeComponent>(dest);
                ComponentType label_new = sdata_current_comp;
                katana::atomicMin(ddata_current_comp, label_new);
              }
            }
          },
          katana::disable_conflict_detection(), katana::steal(),
          katana::loopname("ConnectedComponentsLabelPropAlgo"));
    } while (changed.reduce());
  }
};

struct ConnectedComponentsSynchronousAlgo {
  using ComponentType = ConnectedComponentsNode*;
  struct NodeComponent : public katana::PODProperty<uint64_t, ComponentType> {};

  using NodeData = std::tuple<NodeComponent>;
  using EdgeData = std::tuple<>;
  typedef katana::TypedPropertyGraphView<PropGraphView, NodeData, EdgeData>
      Graph;
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
    katana::do_all(katana::iterate(*graph), [&](const GNode& node) {
      graph->GetData<NodeComponent>(node) = new ConnectedComponentsNode();
    });
  }

  void Deallocate(Graph* graph) {
    katana::do_all(katana::iterate(*graph), [&](const GNode& node) {
      auto& sdata = graph->GetData<NodeComponent>(node);
      auto component_ptr = sdata->component();
      delete sdata;
      sdata = component_ptr;
    });
  }

  void operator()(Graph* graph) {
    size_t rounds = 0;
    katana::GAccumulator<size_t> empty_merges;

    katana::InsertBag<Edge> wls[2];
    katana::InsertBag<Edge>* next_bag;
    katana::InsertBag<Edge>* current_bag;

    current_bag = &wls[0];
    next_bag = &wls[1];

    katana::do_all(katana::iterate(*graph), [&](const GNode& src) {
      for (auto e : graph->edges(src)) {
        auto dest = graph->edge_dest(e);
        if (src >= dest)
          continue;
        auto& ddata = graph->GetData<NodeComponent>(dest);
        current_bag->push(Edge(src, ddata, 0));
        break;
      }
    });

    while (!current_bag->empty()) {
      katana::do_all(
          katana::iterate(*current_bag),
          [&](const Edge& edge) {
            auto& sdata = graph->GetData<NodeComponent>(edge.src);
            if (!sdata->merge(edge.ddata))
              empty_merges += 1;
          },
          katana::loopname("Merge"));

      katana::do_all(
          katana::iterate(*current_bag),
          [&](const Edge& edge) {
            GNode src = edge.src;
            auto& sdata = graph->GetData<NodeComponent>(src);
            ConnectedComponentsNode* src_component = sdata->findAndCompress();
            Graph::edge_iterator ii = graph->edges(src).begin();
            Graph::edge_iterator ei = graph->edges(src).end();
            int count = edge.count + 1;
            std::advance(ii, count);
            for (; ii != ei; ++ii, ++count) {
              auto dest = graph->edge_dest(*ii);
              if (src >= dest)
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
          katana::loopname("Find"));

      current_bag->clear();
      std::swap(current_bag, next_bag);
      rounds += 1;
    }

    katana::do_all(
        katana::iterate(*graph),
        [&](const GNode& src) {
          auto& sdata = graph->GetData<NodeComponent>(src);
          sdata->compress();
        },
        katana::steal(), katana::loopname("Compress"));

    katana::ReportStatSingle("CC-Synchronous", "rounds", rounds);
    katana::ReportStatSingle(
        "CC-Synchronous", "empty_merges", empty_merges.reduce());
  }
};

struct ConnectedComponentsAsynchronousAlgo {
  using ComponentType = ConnectedComponentsNode*;
  struct NodeComponent : public katana::PODProperty<uint64_t, ComponentType> {};

  using NodeData = std::tuple<NodeComponent>;
  using EdgeData = std::tuple<>;
  typedef katana::TypedPropertyGraphView<PropGraphView, NodeData, EdgeData>
      Graph;
  typedef typename Graph::Node GNode;

  ConnectedComponentsPlan& plan_;
  ConnectedComponentsAsynchronousAlgo(ConnectedComponentsPlan& plan)
      : plan_(plan) {}

  void Initialize(Graph* graph) {
    katana::do_all(katana::iterate(*graph), [&](const GNode& node) {
      graph->GetData<NodeComponent>(node) = new ConnectedComponentsNode();
    });
  }

  void Deallocate(Graph* graph) {
    katana::do_all(katana::iterate(*graph), [&](const GNode& node) {
      auto& sdata = graph->GetData<NodeComponent>(node);
      auto component_ptr = sdata->component();
      delete sdata;
      sdata = component_ptr;
    });
  }

  void operator()(Graph* graph) {
    katana::GAccumulator<size_t> empty_merges;

    katana::do_all(
        katana::iterate(*graph),
        [&](const GNode& src) {
          auto& sdata = graph->GetData<NodeComponent>(src);

          for (const auto& ii : graph->edges(src)) {
            auto dest = graph->edge_dest(ii);
            auto& ddata = graph->GetData<NodeComponent>(dest);

            if (src >= dest)
              continue;

            if (!sdata->merge(ddata))
              empty_merges += 1;
          }
        },
        katana::loopname("CC-Asynchronous"));

    katana::do_all(
        katana::iterate(*graph),
        [&](const GNode& src) {
          auto& sdata = graph->GetData<NodeComponent>(src);
          sdata->compress();
        },
        katana::steal(), katana::loopname("CC-Asynchronous-Compress"));

    katana::ReportStatSingle(
        "CC-Asynchronous", "empty_merges", empty_merges.reduce());
  }
};

struct ConnectedComponentsEdgeAsynchronousAlgo {
  using ComponentType = ConnectedComponentsNode*;
  struct NodeComponent : public katana::PODProperty<uint64_t, ComponentType> {};

  using NodeData = std::tuple<NodeComponent>;
  using EdgeData = std::tuple<>;
  typedef katana::TypedPropertyGraphView<PropGraphView, NodeData, EdgeData>
      Graph;
  typedef typename Graph::Node GNode;
  // TODO(amber): 2nd element was Graph::edge_iterator
  using Edge = std::pair<GNode, typename Graph::Edge>;

  ConnectedComponentsPlan& plan_;
  ConnectedComponentsEdgeAsynchronousAlgo(ConnectedComponentsPlan& plan)
      : plan_(plan) {}

  void Initialize(Graph* graph) {
    katana::do_all(katana::iterate(*graph), [&](const GNode& node) {
      graph->GetData<NodeComponent>(node) = new ConnectedComponentsNode();
    });
  }

  void Deallocate(Graph* graph) {
    katana::do_all(katana::iterate(*graph), [&](const GNode& node) {
      auto& sdata = graph->GetData<NodeComponent>(node);
      auto component_ptr = sdata->component();
      delete sdata;
      sdata = component_ptr;
    });
  }

  void operator()(Graph* graph) {
    katana::GAccumulator<size_t> empty_merges;

    katana::InsertBag<Edge> works;

    katana::do_all(
        katana::iterate(*graph),
        [&](const GNode& src) {
          for (const auto& ii : graph->edges(src)) {
            if (src < (graph->edge_dest(ii))) {
              works.push_back(std::make_pair(src, ii));
            }
          }
        },
        katana::loopname("CC-EdgeAsynchronousInit"), katana::steal());

    katana::do_all(
        katana::iterate(works),
        [&](Edge& e) {
          auto& sdata = graph->GetData<NodeComponent>(e.first);
          auto dest = graph->edge_dest(e.second);
          auto& ddata = graph->GetData<NodeComponent>(dest);

          if (e.first > dest)
            // continue;
            ;
          else if (!sdata->merge(ddata)) {
            empty_merges += 1;
          }
        },
        katana::loopname("CC-EdgeAsynchronous"), katana::steal());

    katana::do_all(
        katana::iterate(*graph),
        [&](const GNode& src) {
          auto& sdata = graph->GetData<NodeComponent>(src);
          sdata->compress();
        },
        katana::steal(), katana::loopname("CC-Asynchronous-Compress"));

    katana::ReportStatSingle(
        "CC-Asynchronous", "empty_merges", empty_merges.reduce());
  }
};

struct ConnectedComponentsBlockedAsynchronousAlgo {
  using ComponentType = ConnectedComponentsNode*;
  struct NodeComponent : public katana::PODProperty<uint64_t, ComponentType> {};

  using NodeData = std::tuple<NodeComponent>;
  using EdgeData = std::tuple<>;
  typedef katana::TypedPropertyGraphView<PropGraphView, NodeData, EdgeData>
      Graph;
  typedef typename Graph::Node GNode;
  using Edge = std::pair<GNode, typename Graph::edge_iterator>;

  ConnectedComponentsPlan& plan_;
  ConnectedComponentsBlockedAsynchronousAlgo(ConnectedComponentsPlan& plan)
      : plan_(plan) {}

  void Initialize(Graph* graph) {
    katana::do_all(katana::iterate(*graph), [&](const GNode& node) {
      graph->GetData<NodeComponent>(node) = new ConnectedComponentsNode();
    });
  }

  void Deallocate(Graph* graph) {
    katana::do_all(katana::iterate(*graph), [&](const GNode& node) {
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

    for (Graph::edge_iterator ii = start, ei = graph->edges(src).end();
         ii != ei; ++ii, ++count) {
      auto dest = graph->edge_dest(*ii);
      auto& ddata = graph->GetData<NodeComponent>(dest);

      if (src >= dest)
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
    katana::InsertBag<WorkItem> items;

    katana::do_all(
        katana::iterate(*graph),
        [&](const GNode& src) {
          auto start = graph->edges(src).begin();
          if (katana::ThreadPool::getSocket() == 0) {
            process<true, 0>(graph, src, start, items);
          } else {
            process<true, 1>(graph, src, start, items);
          }
        },
        katana::loopname("Initialize"));

    katana::for_each(
        katana::iterate(items),
        [&](const WorkItem& item, auto& ctx) {
          process<true, 0>(graph, item.src, item.start, ctx);
        },
        katana::loopname("Merge"),
        katana::wl<katana::PerSocketChunkFIFO<128>>());

    katana::do_all(
        katana::iterate(*graph),
        [&](const GNode& src) {
          auto& sdata = graph->GetData<NodeComponent>(src);
          sdata->compress();
        },
        katana::steal(), katana::loopname("CC-Asynchronous-Compress"));
  }
};

struct ConnectedComponentsEdgeTiledAsynchronousAlgo {
  using ComponentType = ConnectedComponentsNode*;
  struct NodeComponent : public katana::PODProperty<uint64_t, ComponentType> {};

  using NodeData = std::tuple<NodeComponent>;
  using EdgeData = std::tuple<>;
  typedef katana::TypedPropertyGraphView<PropGraphView, NodeData, EdgeData>
      Graph;

  typedef typename Graph::Node GNode;
  using Edge = std::pair<GNode, typename Graph::edge_iterator>;

  ConnectedComponentsPlan& plan_;
  ConnectedComponentsEdgeTiledAsynchronousAlgo(ConnectedComponentsPlan& plan)
      : plan_(plan) {}

  void Initialize(Graph* graph) {
    katana::do_all(katana::iterate(*graph), [&](const GNode& node) {
      graph->GetData<NodeComponent>(node) = new ConnectedComponentsNode();
    });
  }

  void Deallocate(Graph* graph) {
    katana::do_all(katana::iterate(*graph), [&](const GNode& node) {
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
    katana::GAccumulator<size_t> empty_merges;

    katana::InsertBag<EdgeTile> works;
    katana::do_all(
        katana::iterate(*graph),
        [&](const GNode& src) {
          auto beg = graph->edges(src).begin();
          const auto& end = graph->edges(src).end();

          KATANA_LOG_DEBUG_ASSERT(beg <= end);
          if ((end - beg) > plan_.edge_tile_size()) {
            for (; beg + plan_.edge_tile_size() < end;) {
              const auto& ne = beg + plan_.edge_tile_size();
              KATANA_LOG_DEBUG_ASSERT(ne < end);
              works.push_back(EdgeTile{src, beg, ne});
              beg = ne;
            }
          }

          if ((end - beg) > 0) {
            works.push_back(EdgeTile{src, beg, end});
          }
        },
        katana::loopname("CC-EdgeTiledAsynchronousInit"), katana::steal());

    katana::do_all(
        katana::iterate(works),
        [&](const EdgeTile& tile) {
          const auto& src = tile.src;
          auto& sdata = graph->GetData<NodeComponent>(src);

          for (auto ii = tile.beg; ii != tile.end; ++ii) {
            auto dest = graph->edge_dest(*ii);
            if (src >= dest)
              continue;

            auto& ddata = graph->GetData<NodeComponent>(dest);
            if (!sdata->merge(ddata))
              empty_merges += 1;
          }
        },
        katana::loopname("CC-edgetiledAsynchronous"), katana::steal(),
        katana::chunk_size<ConnectedComponentsPlan::kChunkSize>()  // 16 -> 1
    );

    katana::do_all(
        katana::iterate(*graph),
        [&](const GNode& src) {
          auto& sdata = graph->GetData<NodeComponent>(src);
          sdata->compress();
        },
        katana::steal(), katana::loopname("CC-Asynchronous-Compress"));

    katana::ReportStatSingle(
        "CC-edgeTiledAsynchronous", "empty_merges", empty_merges.reduce());
  }
};

template <typename ComponentType, typename Graph, typename NodeIndex>
ComponentType
approxLargestComponent(Graph* graph, uint32_t component_sample_frequency) {
  using map_type = katana::gstl::UnorderedMap<ComponentType, int>;
  using pair_type = std::pair<ComponentType, int>;

  map_type comp_freq(component_sample_frequency);
  std::random_device rd;
  std::mt19937 rng(rd());
  std::uniform_int_distribution<uint32_t> dist(0, graph->size() - 1);
  for (uint32_t i = 0; i < component_sample_frequency; i++) {
    ComponentType ndata = graph->template GetData<NodeIndex>(dist(rng));
    comp_freq[ndata->component()]++;
  }

  KATANA_LOG_DEBUG_ASSERT(!comp_freq.empty());
  auto most_frequent = std::max_element(
      comp_freq.begin(), comp_freq.end(),
      [](const pair_type& a, const pair_type& b) {
        return a.second < b.second;
      });

  //katana::gDebug(
  //    "Approximate largest intermediate component: ", most_frequent->first,
  //    " (hit rate ",
  //    100.0 * (most_frequent->second) / component_sample_frequency, "%)");

  return most_frequent->first;
}

template <
    typename ComponentType, typename Graph, typename NodeIndex,
    typename ParentArray>
ComponentType
approxLargestComponent(
    Graph* graph, ParentArray& parent_array_,
    uint32_t component_sample_frequency) {
  using map_type = katana::gstl::UnorderedMap<ComponentType, int>;
  using pair_type = std::pair<ComponentType, int>;

  map_type comp_freq(component_sample_frequency);
  std::random_device rd;
  std::mt19937 rng(rd());
  std::uniform_int_distribution<uint32_t> dist(0, graph->size() - 1);
  for (uint32_t i = 0; i < component_sample_frequency; i++) {
    const auto& ndata = parent_array_[dist(rng)];
    comp_freq[ndata.component()]++;
  }

  KATANA_LOG_DEBUG_ASSERT(!comp_freq.empty());
  auto most_frequent = std::max_element(
      comp_freq.cbegin(), comp_freq.cend(),
      [](const pair_type& a, const pair_type& b) {
        return a.second < b.second;
      });

  //katana::gDebug(
  //    "Approximate largest intermediate component: ", most_frequent->first,
  //    " (hit rate ",
  //    100.0 * (most_frequent->second) / component_sample_frequency, "%)");

  return most_frequent->first;
}

struct ConnectedComponentsAfforestAlgo {
  struct NodeAfforest : public katana::UnionFindNode<NodeAfforest> {
    using ComponentType = NodeAfforest*;

    NodeAfforest() : katana::UnionFindNode<NodeAfforest>(this) {}
    NodeAfforest(const NodeAfforest& o)
        : katana::UnionFindNode<NodeAfforest>(o.m_component) {}

    ComponentType component() { return this->get(); }
    ComponentType component() const { return this->get(); }
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

  struct NodeComponent
      : public katana::PODProperty<uint64_t, NodeAfforest::ComponentType> {};

  using NodeData = std::tuple<NodeComponent>;
  using EdgeData = std::tuple<>;
  typedef katana::TypedPropertyGraphView<PropGraphView, NodeData, EdgeData>
      Graph;
  typedef typename Graph::Node GNode;

  ConnectedComponentsPlan& plan_;
  katana::NUMAArray<NodeAfforest> parent_array_;

  ConnectedComponentsAfforestAlgo(ConnectedComponentsPlan& plan)
      : plan_(plan), parent_array_() {}

  void Initialize(Graph* graph) {
    parent_array_.allocateBlocked(graph->size());
    // parent_array_.allocateInterleaved(graph->size());

    katana::do_all(katana::iterate(*graph), [&](const GNode& node) {
      auto& snode = graph->GetData<NodeComponent>(node);
      new (&snode) NodeAfforest();
      new (&parent_array_[node]) NodeAfforest();
      //snode = reinterpret_cast<ComponentType>(&snode);
    });
  }

  void Deallocate(Graph* graph) {
    katana::do_all(katana::iterate(*graph), [&](const GNode& node) {
      auto& sdata = graph->GetData<NodeComponent>(node);
      auto& dataFromArr = parent_array_[node];
      // auto component_ptr = sdata->component();
      auto component_ptr = dataFromArr.component();
      sdata = component_ptr;
    });
  }
  using ComponentType = NodeAfforest::ComponentType;

  void operator()(Graph* graph) {
    // (bozhi) should NOT go through single direction in sampling step: nodes
    // with edges less than NEIGHBOR_SAMPLES will fail
    for (uint32_t r = 0; r < plan_.neighbor_sample_size(); ++r) {
      katana::do_all(
          katana::iterate(*graph),
          [&](const GNode& src) {
            Graph::edge_iterator ii = graph->edges(src).begin();
            Graph::edge_iterator ei = graph->edges(src).end();

            for (std::advance(ii, r); ii < ei; ii++) {
              auto dest = graph->edge_dest(*ii);
              // auto& sdata = graph->GetData<NodeComponent>(src);
              // ComponentType ddata = graph->GetData<NodeComponent>(dest);
              // sdata->link(ddata);
              auto& sdata = parent_array_[src];
              auto& ddata = parent_array_[dest];
              sdata.link(&ddata);
              break;
            }
          },
          katana::steal(), katana::loopname("Afforest-VNS-Link"));

      katana::do_all(
          katana::iterate(*graph),
          [&](const GNode& src) {
            // auto& sdata = graph->GetData<NodeComponent>(src);
            auto& sdata = parent_array_[src];
            sdata.compress();
          },
          katana::steal(), katana::loopname("Afforest-VNS-Compress"));
    }

    katana::StatTimer StatTimer_Sampling("Afforest-LCS-Sampling");
    StatTimer_Sampling.start();
    const ComponentType c =
        approxLargestComponent<ComponentType, Graph, NodeComponent>(
            graph, parent_array_, plan_.component_sample_frequency());
    StatTimer_Sampling.stop();

    katana::do_all(
        katana::iterate(*graph),
        [&](const GNode& src) {
          // auto& sdata = graph->GetData<NodeComponent>(src);
          auto& sdata = parent_array_[src];
          if (sdata.component() == c)
            return;
          Graph::edge_iterator ii = graph->edges(src).begin();
          Graph::edge_iterator ei = graph->edges(src).end();
          for (std::advance(ii, plan_.neighbor_sample_size()); ii < ei; ++ii) {
            auto dest = graph->edge_dest(*ii);
            // auto& ddata = graph->GetData<NodeComponent>(dest);
            auto& ddata = parent_array_[dest];
            // sdata->link(ddata);
            sdata.link(&ddata);
          }
        },
        katana::steal(), katana::loopname("Afforest-LCS-Link"));

    katana::do_all(
        katana::iterate(*graph),
        [&](const GNode& src) {
          // auto& sdata = graph->GetData<NodeComponent>(src);
          // sdata->compress();
          auto& sdata = parent_array_[src];
          sdata.compress();
        },
        katana::steal(), katana::loopname("Afforest-LCS-Compress"));
  }
};

struct ConnectedComponentsEdgeAfforestAlgo {
  struct NodeAfforestEdge : public katana::UnionFindNode<NodeAfforestEdge> {
    using ComponentType = NodeAfforestEdge*;

    NodeAfforestEdge()
        : katana::UnionFindNode<NodeAfforestEdge>(
              const_cast<NodeAfforestEdge*>(this)) {}
    NodeAfforestEdge(const NodeAfforestEdge& o)
        : katana::UnionFindNode<NodeAfforestEdge>(o.m_component) {}

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
  struct NodeComponent
      : public katana::PODProperty<uint64_t, NodeAfforestEdge::ComponentType> {
  };

  using NodeData = std::tuple<NodeComponent>;
  using EdgeData = std::tuple<>;
  typedef katana::TypedPropertyGraphView<PropGraphView, NodeData, EdgeData>
      Graph;
  typedef typename Graph::Node GNode;

  using Edge = std::pair<GNode, GNode>;

  ConnectedComponentsPlan& plan_;
  ConnectedComponentsEdgeAfforestAlgo(ConnectedComponentsPlan& plan)
      : plan_(plan) {}

  void Initialize(Graph* graph) {
    katana::do_all(katana::iterate(*graph), [&](const GNode& node) {
      graph->GetData<NodeComponent>(node) = new NodeAfforestEdge();
    });
  }

  void Deallocate(Graph* graph) {
    katana::do_all(katana::iterate(*graph), [&](const GNode& node) {
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
      katana::do_all(
          katana::iterate(*graph),
          [&](const GNode& src) {
            Graph::edge_iterator ii = graph->edges(src).begin();
            Graph::edge_iterator ei = graph->edges(src).end();
            std::advance(ii, r);
            if (ii < ei) {
              auto dest = graph->edge_dest(*ii);
              auto& sdata = graph->GetData<NodeComponent>(src);
              auto& ddata = graph->GetData<NodeComponent>(dest);
              sdata->hook_min(ddata);
            }
          },
          katana::steal(), katana::loopname("EdgeAfforest-VNS-Link"));
    }
    katana::do_all(
        katana::iterate(*graph),
        [&](const GNode& src) {
          auto& sdata = graph->GetData<NodeComponent>(src);
          sdata->compress();
        },
        katana::steal(), katana::loopname("EdgeAfforest-VNS-Compress"));

    katana::StatTimer StatTimer_Sampling("EdgeAfforest-LCS-Sampling");
    StatTimer_Sampling.start();
    const ComponentType c =
        approxLargestComponent<ComponentType, Graph, NodeComponent>(
            graph, plan_.component_sample_frequency());
    StatTimer_Sampling.stop();
    const ComponentType c0 = (graph->GetData<NodeComponent>(0));

    katana::InsertBag<Edge> works;

    katana::do_all(
        katana::iterate(*graph),
        [&](const GNode& src) {
          auto& sdata = graph->GetData<NodeComponent>(src);
          if (sdata->component() == c)
            return;
          auto beg = graph->edges(src).begin();
          const auto end = graph->edges(src).end();

          for (std::advance(beg, plan_.neighbor_sample_size()); beg < end;
               beg++) {
            auto dest = graph->edge_dest(*beg);
            auto& ddata = graph->GetData<NodeComponent>(dest);
            if (src < dest || c == ddata->component()) {
              works.push_back(std::make_pair(src, dest));
            }
          }
        },
        katana::loopname("EdgeAfforest-LCS-Assembling"), katana::steal());

    katana::for_each(
        katana::iterate(works),
        [&](const Edge& e, auto& ctx) {
          auto& sdata = graph->GetData<NodeComponent>(e.first);
          if (sdata->component() == c)
            return;
          auto& ddata = graph->GetData<NodeComponent>(e.second);
          ComponentType victim = sdata->hook_min(ddata, c);
          if (victim) {
            auto src = victim - c0;  // TODO (bozhi) tricky!
            for (auto ii : graph->edges(src)) {
              auto dest = graph->edge_dest(ii);
              ctx.push_back(std::make_pair(dest, src));
            }
          }
        },
        katana::disable_conflict_detection(),
        katana::loopname("EdgeAfforest-LCS-Link"));

    katana::do_all(
        katana::iterate(*graph),
        [&](const GNode& src) {
          auto& sdata = graph->GetData<NodeComponent>(src);
          sdata->compress();
        },
        katana::steal(), katana::loopname("EdgeAfforest-LCS-Compress"));
  }
};

struct ConnectedComponentsEdgeTiledAfforestAlgo {
  struct NodeAfforest : public katana::UnionFindNode<NodeAfforest> {
    using ComponentType = NodeAfforest*;

    NodeAfforest()
        : katana::UnionFindNode<NodeAfforest>(const_cast<NodeAfforest*>(this)) {
    }
    NodeAfforest(const NodeAfforest& o)
        : katana::UnionFindNode<NodeAfforest>(o.m_component) {}

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

  struct NodeComponent
      : public katana::PODProperty<uint64_t, NodeAfforest::ComponentType> {};

  using NodeData = std::tuple<NodeComponent>;
  using EdgeData = std::tuple<>;
  typedef katana::TypedPropertyGraphView<PropGraphView, NodeData, EdgeData>
      Graph;
  typedef typename Graph::Node GNode;

  ConnectedComponentsPlan& plan_;
  ConnectedComponentsEdgeTiledAfforestAlgo(ConnectedComponentsPlan& plan)
      : plan_(plan) {}

  void Initialize(Graph* graph) {
    katana::do_all(katana::iterate(*graph), [&](const GNode& node) {
      graph->GetData<NodeComponent>(node) = new NodeAfforest();
    });
  }

  void Deallocate(Graph* graph) {
    katana::do_all(katana::iterate(*graph), [&](const GNode& node) {
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
    katana::do_all(
        katana::iterate(*graph),
        [&](const GNode& src) {
          auto ii = graph->edges(src).begin();
          const auto end = graph->edges(src).end();
          for (uint32_t r = 0; r < plan_.neighbor_sample_size() && ii < end;
               ++r, ++ii) {
            auto dest = graph->edge_dest(*ii);
            auto& sdata = graph->GetData<NodeComponent>(src);
            auto& ddata = graph->GetData<NodeComponent>(dest);
            sdata->link(ddata);
          }
        },
        katana::steal(), katana::loopname("EdgetiledAfforest-VNS-Link"));

    katana::do_all(
        katana::iterate(*graph),
        [&](const GNode& src) {
          auto& sdata = graph->GetData<NodeComponent>(src);
          sdata->compress();
        },
        katana::steal(), katana::loopname("EdgetiledAfforest-VNS-Compress"));

    katana::StatTimer StatTimer_Sampling("EdgetiledAfforest-LCS-Sampling");
    StatTimer_Sampling.start();
    const ComponentType c =
        approxLargestComponent<ComponentType, Graph, NodeComponent>(
            graph, plan_.component_sample_frequency());
    StatTimer_Sampling.stop();

    katana::InsertBag<EdgeTile> works;
    katana::do_all(
        katana::iterate(*graph),
        [&](const GNode& src) {
          auto& sdata = graph->GetData<NodeComponent>(src);
          if (sdata->component() == c)
            return;
          auto beg = graph->edges(src).begin();
          const auto end = graph->edges(src).end();

          for (std::advance(beg, plan_.neighbor_sample_size());
               beg + plan_.edge_tile_size() < end;) {
            auto ne = beg + plan_.edge_tile_size();
            KATANA_LOG_DEBUG_ASSERT(ne < end);
            works.push_back(EdgeTile{src, beg, ne});
            beg = ne;
          }

          if ((end - beg) > 0) {
            works.push_back(EdgeTile{src, beg, end});
          }
        },
        katana::loopname("EdgetiledAfforest-LCS-Tiling"), katana::steal());

    katana::do_all(
        katana::iterate(works),
        [&](const EdgeTile& tile) {
          auto& sdata = graph->GetData<NodeComponent>(tile.src);
          if (sdata->component() == c)
            return;
          for (auto ii = tile.beg; ii < tile.end; ++ii) {
            auto dest = graph->edge_dest(*ii);
            auto& ddata = graph->GetData<NodeComponent>(dest);
            sdata->link(ddata);
          }
        },
        katana::steal(),
        katana::chunk_size<ConnectedComponentsPlan::kChunkSize>(),
        katana::loopname("EdgetiledAfforest-LCS-Link"));

    katana::do_all(
        katana::iterate(*graph),
        [&](const GNode& src) {
          auto& sdata = graph->GetData<NodeComponent>(src);
          sdata->compress();
        },
        katana::steal(), katana::loopname("EdgetiledAfforest-LCS-Compress"));
  }
};

}  //namespace

template <typename Algorithm>
static katana::Result<void>
ConnectedComponentsWithWrap(
    katana::PropertyGraph* pg, std::string output_property_name,
    ConnectedComponentsPlan plan) {
  katana::EnsurePreallocated(
      2,
      pg->topology().num_nodes() * sizeof(typename Algorithm::NodeComponent));
  katana::ReportPageAllocGuard page_alloc;

  if (auto r = ConstructNodeProperties<
          std::tuple<typename Algorithm::NodeComponent>>(
          pg, {output_property_name});
      !r) {
    return r.error();
  }
  auto pg_result = Algorithm::Graph::Make(pg, {output_property_name}, {});
  if (!pg_result) {
    return pg_result.error();
  }

  auto graph = pg_result.value();

  Algorithm algo(plan);

  algo.Initialize(&graph);

  katana::StatTimer execTime("ConnectedComponent");
  execTime.start();

  algo(&graph);
  execTime.stop();

  algo.Deallocate(&graph);
  return katana::ResultSuccess();
}

katana::Result<void>
katana::analytics::ConnectedComponents(
    PropertyGraph* pg, const std::string& output_property_name,
    ConnectedComponentsPlan plan) {
  switch (plan.algorithm()) {
  case ConnectedComponentsPlan::kSerial:
    return ConnectedComponentsWithWrap<ConnectedComponentsSerialAlgo>(
        pg, output_property_name, plan);
  case ConnectedComponentsPlan::kLabelProp:
    return ConnectedComponentsWithWrap<ConnectedComponentsLabelPropAlgo>(
        pg, output_property_name, plan);
  case ConnectedComponentsPlan::kSynchronous:
    return ConnectedComponentsWithWrap<ConnectedComponentsSynchronousAlgo>(
        pg, output_property_name, plan);
  case ConnectedComponentsPlan::kAsynchronous:
    return ConnectedComponentsWithWrap<ConnectedComponentsAsynchronousAlgo>(
        pg, output_property_name, plan);
  case ConnectedComponentsPlan::kEdgeAsynchronous:
    return ConnectedComponentsWithWrap<ConnectedComponentsEdgeAsynchronousAlgo>(
        pg, output_property_name, plan);
  case ConnectedComponentsPlan::kEdgeTiledAsynchronous:
    return ConnectedComponentsWithWrap<
        ConnectedComponentsEdgeTiledAsynchronousAlgo>(
        pg, output_property_name, plan);
  case ConnectedComponentsPlan::kBlockedAsynchronous:
    return ConnectedComponentsWithWrap<
        ConnectedComponentsBlockedAsynchronousAlgo>(
        pg, output_property_name, plan);
  case ConnectedComponentsPlan::kAfforest:
    return ConnectedComponentsWithWrap<ConnectedComponentsAfforestAlgo>(
        pg, output_property_name, plan);
  case ConnectedComponentsPlan::kEdgeAfforest:
    return ConnectedComponentsWithWrap<ConnectedComponentsEdgeAfforestAlgo>(
        pg, output_property_name, plan);
  case ConnectedComponentsPlan::kEdgeTiledAfforest:
    return ConnectedComponentsWithWrap<
        ConnectedComponentsEdgeTiledAfforestAlgo>(
        pg, output_property_name, plan);
  default:
    return ErrorCode::InvalidArgument;
  }
}

katana::Result<void>
katana::analytics::ConnectedComponentsAssertValid(
    PropertyGraph* pg, const std::string& property_name) {
  using ComponentType = uint64_t;
  struct NodeComponent : public katana::PODProperty<ComponentType> {};

  using NodeData = std::tuple<NodeComponent>;
  using EdgeData = std::tuple<>;
  typedef katana::TypedPropertyGraphView<PropGraphView, NodeData, EdgeData>
      Graph;
  typedef typename Graph::Node GNode;

  auto pg_result = Graph::Make(pg, {property_name}, {});
  if (!pg_result) {
    return pg_result.error();
  }

  auto graph = pg_result.value();

  auto is_bad = [&graph](const GNode& n) {
    auto& me = graph.template GetData<NodeComponent>(n);
    for (auto ii : graph.edges(n)) {
      auto dest = graph.edge_dest(ii);
      auto& data = graph.template GetData<NodeComponent>(dest);
      if (data != me) {
        KATANA_LOG_DEBUG(
            "{} (component: {}) must be in same component as {} (component: "
            "{})",
            dest, data, n, me);
        return true;
      }
    }
    return false;
  };

  if (katana::ParallelSTL::find_if(graph.begin(), graph.end(), is_bad) !=
      graph.end()) {
    return katana::ErrorCode::AssertionFailed;
  }

  return katana::ResultSuccess();
}

katana::Result<ConnectedComponentsStatistics>
katana::analytics::ConnectedComponentsStatistics::Compute(
    katana::PropertyGraph* pg, const std::string& property_name) {
  using ComponentType = uint64_t;
  struct NodeComponent : public katana::PODProperty<ComponentType> {};

  using NodeData = std::tuple<NodeComponent>;
  using EdgeData = std::tuple<>;
  typedef katana::TypedPropertyGraph<NodeData, EdgeData> Graph;
  typedef typename Graph::Node GNode;

  auto pg_result = Graph::Make(pg, {property_name}, {});
  if (!pg_result) {
    return pg_result.error();
  }

  auto graph = pg_result.value();

  using Map = katana::gstl::Map<ComponentType, int>;

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

  auto accumMap = katana::make_reducible(reduce, mapIdentity);

  katana::GAccumulator<size_t> accumReps;

  katana::do_all(
      katana::iterate(graph),
      [&](const GNode& x) {
        auto& n = graph.template GetData<NodeComponent>(x);
        accumMap.update(Map{std::make_pair(n, 1)});
      },
      katana::loopname("CountLargest"));

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

  auto maxComp = katana::make_reducible(sizeMax, identity);

  katana::GAccumulator<uint64_t> non_trivial_components;
  katana::do_all(katana::iterate(map), [&](const ComponentSizePair& x) {
    maxComp.update(x);
    if (x.second > 1) {
      non_trivial_components += 1;
    }
  });

  ComponentSizePair largest = maxComp.reduce();

  size_t largest_component_size = largest.second;
  double largest_component_ratio = 0;
  if (!graph.empty()) {
    largest_component_ratio = double(largest_component_size) / graph.size();
  }

  return ConnectedComponentsStatistics{
      reps, non_trivial_components.reduce(), largest_component_size,
      largest_component_ratio};
}

void
katana::analytics::ConnectedComponentsStatistics::Print(
    std::ostream& os) const {
  os << "Total number of components = " << total_components << std::endl;
  os << "Total number of non trivial components = "
     << total_non_trivial_components << std::endl;
  os << "Number of nodes in the largest component = " << largest_component_size
     << std::endl;
  os << "Ratio of nodes in the largest component = " << largest_component_ratio
     << std::endl;
}
