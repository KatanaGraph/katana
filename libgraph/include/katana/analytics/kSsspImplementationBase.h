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

#ifndef KATANA_LIBGRAPH_KATANA_ANALYTICS_KSSSPIMPLEMENTATIONBASE_H_
#define KATANA_LIBGRAPH_KATANA_ANALYTICS_KSSSPIMPLEMENTATIONBASE_H_

#include <atomic>
#include <cstdlib>
#include <iostream>

#include "katana/analytics/Utils.h"

namespace katana::analytics {

template <
    typename _Graph, typename _DistLabel, typename Path, bool UseEdgeWt,
    ptrdiff_t EdgeTileSize = 256>
struct KSsspImplementationBase {
  const ptrdiff_t edge_tile_size;

  using Graph = _Graph;
  using Distance = _DistLabel;

  constexpr static const Distance kDistInfinity =
      std::numeric_limits<Distance>::max() / 4;

  using GNode = typename Graph::Node;
  using EI = typename Graph::edge_iterator;

  struct UpdateRequest {
    GNode src;
    Distance distance;
    Path* path;
    UpdateRequest(const GNode& N, Distance W, Path* P)
        : src(N), distance(W), path(P) {}
    UpdateRequest() : src(), distance(0), path(NULL) {}

    friend bool operator<(
        const UpdateRequest& left, const UpdateRequest& right) {
      return left.distance == right.distance ? left.src < right.src
                                             : left.distance < right.distance;
    }
  };

  struct UpdateRequestIndexer {
    unsigned shift;
    unsigned long divisor;

    UpdateRequestIndexer(const unsigned _shift)
        : shift(_shift), divisor(std::pow(2, shift)) {}

    template <typename R>
    unsigned int operator()(const R& req) const {
      unsigned int t = req.distance / divisor;
      return t;
    }
  };

  struct SrcEdgeTile {
    GNode src;
    Distance distance;
    Path* path;
    EI beg;
    EI end;

    friend bool operator<(const SrcEdgeTile& left, const SrcEdgeTile& right) {
      return left.distance == right.distance ? left.src < right.src
                                             : left.distance < right.distance;
    }
  };

  struct SrcEdgeTileMaker {
    GNode src;
    Distance distance;
    Path* path;

    SrcEdgeTile operator()(const EI& beg, const EI& end) const {
      return SrcEdgeTile{src, distance, path, beg, end};
    }
  };

  template <typename WL, typename TileMaker>
  static void PushEdgeTiles(WL& wl, EI beg, const EI end, const TileMaker& f) {
    KATANA_LOG_DEBUG_ASSERT(beg <= end);

    if ((end - beg) > EdgeTileSize) {
      for (; beg + EdgeTileSize < end;) {
        auto ne = beg + EdgeTileSize;
        KATANA_LOG_DEBUG_ASSERT(ne < end);
        wl.push(f(beg, ne));
        beg = ne;
      }
    }

    if ((end - beg) > 0) {
      wl.push(f(beg, end));
    }
  }

  template <typename WL, typename TileMaker>
  static void PushEdgeTiles(
      WL& wl, Graph* graph, GNode src, const TileMaker& f) {
    auto beg = graph->OutEdges(src).begin();
    const auto end = graph->OutEdges(src).end();

    PushEdgeTiles(wl, beg, end, f);
  }

  template <typename WL, typename TileMaker>
  static void PushEdgeTilesParallel(
      WL& wl, Graph* graph, GNode src, const TileMaker& f) {
    auto beg = graph->OutEdges(src).begin();
    const auto end = graph->OutEdges(src).end();

    if ((end - beg) > EdgeTileSize) {
      katana::on_each(
          [&](const unsigned tid, const unsigned numT) {
            auto p = katana::block_range(beg, end, tid, numT);

            auto b = p.first;
            const auto e = p.second;

            PushEdgeTiles(wl, b, e, f);
          },
          katana::loopname("Init-Tiling"));

    } else if ((end - beg) > 0) {
      wl.push(f(beg, end));
    }
  }

  struct ReqPushWrap {
    template <typename C>
    void operator()(
        C& cont, const GNode& n, const Distance& distance, const Path* path,
        const char* const) const {
      (*this)(cont, n, distance, path);
    }

    template <typename C>
    void operator()(
        C& cont, const GNode& n, const Distance& distance,
        const Path* path) const {
      cont.push(UpdateRequest(n, distance, path));
    }
  };

  struct SrcEdgeTilePushWrap {
    Graph* graph;

    template <typename C>
    void operator()(
        C& cont, const GNode& n, const Distance& distance, const Path* path,
        const char* const) const {
      PushEdgeTilesParallel(
          cont, graph, n, SrcEdgeTileMaker{n, distance, path});
    }

    template <typename C>
    void operator()(
        C& cont, const GNode& n, const Distance& distance,
        const Path* path) const {
      PushEdgeTiles(cont, graph, n, SrcEdgeTileMaker{n, distance, path});
    }
  };

  struct OutEdgeRangeFn {
    Graph* graph;
    auto operator()(const GNode& n) const { return graph->OutEdges(n); }

    auto operator()(const UpdateRequest& req) const {
      return graph->OutEdges(req.src);
    }
  };

  struct TileRangeFn {
    template <typename T>
    auto operator()(const T& tile) const {
      return katana::MakeStandardRange(tile.beg, tile.end);
    }
  };

  template <typename NodeProp, typename EdgeProp>
  struct SanityCheck {
    Graph* g;
    std::atomic<bool>& refb;
    SanityCheck(Graph* g, std::atomic<bool>& refb) : g(g), refb(refb) {}

    template <bool useWt, typename iiTy>
    Distance GetEdgeWeight(
        iiTy, typename std::enable_if<!useWt>::type* = nullptr) const {
      return 1;
    }

    template <bool useWt, typename iiTy>
    Distance GetEdgeWeight(
        iiTy ii, typename std::enable_if<useWt>::type* = nullptr) const {
      return g->template GetEdgeData<EdgeProp>(ii);
    }

    void operator()(typename Graph::Node node) const {
      Distance sd = g->template GetData<NodeProp>(node);
      if (sd == kDistInfinity) {
        return;
      }

      for (auto ii : g->OutEdges(node)) {
        auto dest = g->GetEdgeDst(ii);
        Distance dd = g->template GetData<NodeProp>(*dest);
        Distance ew = GetEdgeWeight<UseEdgeWt>(ii);
        if (dd > sd + ew) {
          katana::gPrint(
              "Wrong label: ", dd, ", on node: ", *dest,
              ", correct label from src node ", node, " is ", sd + ew, "\n");
          refb = true;
          // return;
        }
      }
    }
  };

  template <typename NodeProp>
  struct MaxDist {
    Graph* g;
    katana::GReduceMax<Distance>& m;

    MaxDist(Graph* g, katana::GReduceMax<Distance>& m) : g(g), m(m) {}

    void operator()(typename Graph::Node node) const {
      Distance d = g->template GetData<NodeProp>(node);
      if (d != kDistInfinity) {
        m.update(d);
      }
    }
  };

  template <typename NodeProp, typename EdgeProp = katana::PODProperty<int64_t>>
  static bool Verify(Graph* graph, GNode source) {
    if (graph->template GetData<NodeProp>(source) != 0) {
      KATANA_LOG_ERROR(
          "ERROR: source has non-zero dist value == ",
          graph->template GetData<NodeProp>(source), "\n");
      return false;
    }

    std::atomic<size_t> not_visited(0);
    katana::do_all(katana::iterate(*graph), [&not_visited, &graph](GNode node) {
      if (graph->template GetData<NodeProp>(node) >= kDistInfinity) {
        ++not_visited;
      }
    });

    if (not_visited) {
      KATANA_LOG_WARN(
          "{} unvisited nodes; this is an error if the graph is strongly "
          "connected\n",
          not_visited);
    }

    std::atomic<bool> not_c(false);
    katana::do_all(
        katana::iterate(*graph), SanityCheck<NodeProp, EdgeProp>(graph, not_c));

    if (not_c) {
      KATANA_LOG_ERROR("node found with incorrect distance\n");
      return false;
    }

    katana::GReduceMax<Distance> m;
    katana::do_all(katana::iterate(*graph), MaxDist<NodeProp>(graph, m));

    katana::gPrint("max dist: ", m.reduce(), "\n");

    return true;
  }
};

}  // namespace katana::analytics

#endif
