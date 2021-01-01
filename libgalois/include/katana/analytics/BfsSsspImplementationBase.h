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

#ifndef GALOIS_LIBGALOIS_GALOIS_ANALYTICS_BFSSSSPIMPLEMENTATIONBASE_H_
#define GALOIS_LIBGALOIS_GALOIS_ANALYTICS_BFSSSSPIMPLEMENTATIONBASE_H_

#include <atomic>
#include <cstdlib>
#include <iostream>

#include "galois/analytics/Utils.h"

namespace galois::analytics {

template <typename _Graph, typename _DistLabel, bool USE_EDGE_WT>
struct BfsSsspImplementationBase {
  const ptrdiff_t edge_tile_size;

  using Graph = _Graph;
  using Dist = _DistLabel;

  constexpr static const bool kTrackWork = false;

  constexpr static const Dist kDistanceInfinity =
      std::numeric_limits<Dist>::max() / 4;

  using GNode = typename Graph::Node;
  using EI = typename Graph::edge_iterator;

  struct UpdateRequest {
    GNode src;
    Dist dist;
    UpdateRequest(const GNode& N, Dist W) : src(N), dist(W) {}
    UpdateRequest() : src(), dist(0) {}

    friend bool operator<(
        const UpdateRequest& left, const UpdateRequest& right) {
      return left.dist == right.dist ? left.src < right.src
                                     : left.dist < right.dist;
    }
  };

  struct UpdateRequestIndexer {
    unsigned shift;
    unsigned long divisor;

    UpdateRequestIndexer(const unsigned _shift)
        : shift(_shift), divisor(std::pow(2, shift)) {}

    template <typename R>
    unsigned int operator()(const R& req) const {
      unsigned int t = req.dist / divisor;
      return t;
    }
  };

  struct SrcEdgeTile {
    GNode src;
    Dist dist;
    EI beg;
    EI end;

    friend bool operator<(const SrcEdgeTile& left, const SrcEdgeTile& right) {
      return left.dist == right.dist ? left.src < right.src
                                     : left.dist < right.dist;
    }
  };

  struct SrcEdgeTileMaker {
    GNode src;
    Dist dist;

    SrcEdgeTile operator()(const EI& beg, const EI& end) const {
      return SrcEdgeTile{src, dist, beg, end};
    }
  };

  template <typename WL, typename TileMaker>
  void PushEdgeTiles(WL& wl, EI beg, const EI end, const TileMaker& f) {
    assert(beg <= end);

    if ((end - beg) > edge_tile_size) {
      for (; beg + edge_tile_size < end;) {
        auto ne = beg + edge_tile_size;
        assert(ne < end);
        wl.push(f(beg, ne));
        beg = ne;
      }
    }

    if ((end - beg) > 0) {
      wl.push(f(beg, end));
    }
  }

  template <typename WL, typename TileMaker>
  void PushEdgeTiles(WL& wl, Graph* graph, GNode src, const TileMaker& f) {
    auto beg = graph->edge_begin(src);
    const auto end = graph->edge_end(src);

    PushEdgeTiles(wl, beg, end, f);
  }

  template <typename WL, typename TileMaker>
  void PushEdgeTilesParallel(
      WL& wl, Graph* graph, GNode src, const TileMaker& f) {
    auto beg = graph->edge_begin(src);
    const auto end = graph->edge_end(src);

    if ((end - beg) > edge_tile_size) {
      galois::on_each(
          [&](const unsigned tid, const unsigned numT) {
            auto p = galois::block_range(beg, end, tid, numT);

            auto b = p.first;
            const auto e = p.second;

            PushEdgeTiles(wl, b, e, f);
          },
          galois::loopname("Init-Tiling"));

    } else if ((end - beg) > 0) {
      wl.push(f(beg, end));
    }
  }

  struct ReqPushWrap {
    template <typename C>
    void operator()(
        C& cont, const GNode& n, const Dist& dist, const char* const) const {
      (*this)(cont, n, dist);
    }

    template <typename C>
    void operator()(C& cont, const GNode& n, const Dist& dist) const {
      cont.push(UpdateRequest(n, dist));
    }
  };

  struct SrcEdgeTilePushWrap {
    Graph* graph;
    BfsSsspImplementationBase& impl;

    template <typename C>
    void operator()(
        C& cont, const GNode& n, const Dist& dist, const char* const) const {
      impl.PushEdgeTilesParallel(cont, graph, n, SrcEdgeTileMaker{n, dist});
    }

    template <typename C>
    void operator()(C& cont, const GNode& n, const Dist& dist) const {
      impl.PushEdgeTiles(cont, graph, n, SrcEdgeTileMaker{n, dist});
    }
  };

  struct OutEdgeRangeFn {
    Graph* graph;
    auto operator()(const GNode& n) const { return graph->edges(n); }

    auto operator()(const UpdateRequest& req) const {
      return graph->edges(req.src);
    }
  };

  struct TileRangeFn {
    template <typename T>
    auto operator()(const T& tile) const {
      return galois::makeIterRange(tile.beg, tile.end);
    }
  };

  template <typename NodeProp, typename EdgeProp>
  struct NotConsistent {
    Graph* g;
    std::atomic<bool>& refb;
    NotConsistent(Graph* g, std::atomic<bool>& refb) : g(g), refb(refb) {}

    // TODO: Why can't this use USE_EDGE_WT directly. It's in scope, but fails.
    template <bool useWt, typename iiTy>
    Dist getEdgeWeight(
        iiTy ii, typename std::enable_if<useWt>::type* = nullptr) const {
      return g->template GetEdgeData<EdgeProp>(ii);
    }

    template <bool useWt, typename iiTy>
    Dist getEdgeWeight(
        iiTy, typename std::enable_if<!useWt>::type* = nullptr) const {
      return 1;
    }

    void operator()(typename Graph::Node node) const {
      Dist sd = g->template GetData<NodeProp>(node);
      if (sd == kDistanceInfinity)
        return;

      for (auto ii : g->edges(node)) {
        auto dest = g->GetEdgeDest(ii);
        Dist dd = g->template GetData<NodeProp>(*dest);
        Dist ew = getEdgeWeight<USE_EDGE_WT>(ii);
        if (dd > sd + ew) {
          GALOIS_LOG_DEBUG(
              "Wrong label: {}, on node: {}, correct label from src node {} is "
              "{}",
              dd, *dest, node, sd + ew);
          refb = true;
          // return;
        }
      }
    }
  };

  template <typename NodeProp>
  struct MaxDist {
    Graph* g;
    galois::GReduceMax<Dist>& m;

    MaxDist(Graph* g, galois::GReduceMax<Dist>& m) : g(g), m(m) {}

    void operator()(typename Graph::Node node) const {
      Dist d = g->template GetData<NodeProp>(node);
      if (d == kDistanceInfinity)
        return;
      m.update(d);
    }
  };

  template <typename NodeProp, typename EdgeProp>
  static bool Verify(Graph* graph, GNode source) {
    if (graph->template GetData<NodeProp>(source) != 0) {
      std::cerr << "ERROR: source has non-zero dist value == "
                << graph->template GetData<NodeProp>(source) << std::endl;
      return false;
    }

    std::atomic<size_t> not_visited(0);
    galois::do_all(galois::iterate(*graph), [&not_visited, &graph](GNode node) {
      if (graph->template GetData<NodeProp>(node) >= kDistanceInfinity)
        ++not_visited;
    });

    if (not_visited)
      std::cerr << not_visited
                << " unvisited nodes; this is an error if the graph is "
                   "strongly connected\n";

    std::atomic<bool> not_c(false);
    galois::do_all(
        galois::iterate(*graph),
        NotConsistent<NodeProp, EdgeProp>(graph, not_c));

    if (not_c) {
      std::cerr << "node found with incorrect distance\n";
      return false;
    }

    galois::GReduceMax<Dist> m;
    galois::do_all(galois::iterate(*graph), MaxDist<NodeProp>(graph, m));

    std::cout << "max dist: " << m.reduce() << "\n";

    return true;
  }
};

template <typename T, typename BucketFunc, size_t MAX_BUCKETS = 543210ul>
class SerialBucketWL {
  using Bucket = std::deque<T>;
  using BucketsCont = std::vector<Bucket>;

  size_t m_minBucket;
  BucketFunc m_func;
  BucketsCont m_buckets;
  Bucket m_lastBucket;

  static_assert(MAX_BUCKETS > 0, "MAX_BUCKETS must be > 0");

public:
  explicit SerialBucketWL(const BucketFunc& f) : m_minBucket(0ul), m_func(f) {
    // reserve enough so that resize never reallocates memory
    // otherwise, minBucket may return an invalid reference
    m_buckets.reserve(MAX_BUCKETS);
  }

  void push(const T& item) {
    size_t b = m_func(item);
    assert(b >= m_minBucket && "can't push below m_minBucket");

    if (b < m_buckets.size()) {
      m_buckets[b].push_back(item);
      return;
    } else {
      if (b >= MAX_BUCKETS) {
        std::cerr << "Increase MAX_BUCKETS limit" << std::endl;
        m_lastBucket.push_back(item);
      } else {
        m_buckets.resize(b + 1);
        m_buckets[b].push_back(item);
      }
    }
  }

  void goToNextBucket(void) {
    while (m_minBucket < m_buckets.size() && m_buckets[m_minBucket].empty()) {
      ++m_minBucket;
    }
  }

  Bucket& minBucket(void) {
    if (m_minBucket < m_buckets.size()) {
      return m_buckets[m_minBucket];
    } else {
      return m_lastBucket;
    }
  }

  bool empty(void) const { return emptyImpl(m_minBucket); }

  bool allEmpty(void) const { return emptyImpl(0ul); }

private:
  bool emptyImpl(size_t start) const {
    for (size_t i = start; i < m_buckets.size(); ++i) {
      if (!m_buckets[i].empty()) {
        return false;
      }
    }

    return m_lastBucket.empty();
  }
};

}  // namespace galois::analytics

#endif  //  LONESTAR_BFS_SSSP_H
