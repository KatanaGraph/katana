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

#ifndef KATANA_LIBGALOIS_KATANA_LCCSRGRAPH_H_
#define KATANA_LIBGALOIS_KATANA_LCCSRGRAPH_H_

#include <fstream>
#include <type_traits>

#include "katana/Details.h"
#include "katana/FileGraph.h"
#include "katana/Galois.h"
#include "katana/GraphHelpers.h"
#include "katana/PODResizeableArray.h"
#include "katana/config.h"

namespace katana {
/**
 * Local computation graph (i.e., graph structure does not change). The data
 * representation is the traditional compressed-sparse-row (CSR) format.
 *
 * The position of template parameters may change between Galois releases; the
 * most robust way to specify them is through the with_XXX nested templates.
 *
 * An example of use:
 *
 * \snippet test/graph.cpp Using a graph
 *
 * And in C++11:
 *
 * \snippet test/graph.cpp Using a graph cxx11
 *
 * @tparam NodeTy data on nodes
 * @tparam EdgeTy data on out edges
 */
//! [doxygennuma]
template <
    typename NodeTy, typename EdgeTy, bool HasNoLockable = false,
    bool UseNumaAlloc = false, bool HasOutOfLineLockable = false,
    typename FileEdgeTy = EdgeTy>
class LC_CSR_Graph :
    //! [doxygennuma]
    private internal::LocalIteratorFeature<UseNumaAlloc>,
    private internal::OutOfLineLockableFeature<
        HasOutOfLineLockable && !HasNoLockable> {
  template <typename Graph>
  friend class LC_InOut_Graph;

public:
  template <bool _has_id>
  struct with_id {
    typedef LC_CSR_Graph type;
  };

  template <typename _node_data>
  struct with_node_data {
    typedef LC_CSR_Graph<
        _node_data, EdgeTy, HasNoLockable, UseNumaAlloc, HasOutOfLineLockable,
        FileEdgeTy>
        type;
  };

  template <typename _edge_data>
  struct with_edge_data {
    typedef LC_CSR_Graph<
        NodeTy, _edge_data, HasNoLockable, UseNumaAlloc, HasOutOfLineLockable,
        FileEdgeTy>
        type;
  };

  template <typename _file_edge_data>
  struct with_file_edge_data {
    typedef LC_CSR_Graph<
        NodeTy, EdgeTy, HasNoLockable, UseNumaAlloc, HasOutOfLineLockable,
        _file_edge_data>
        type;
  };

  //! If true, do not use abstract locks in graph
  template <bool _has_no_lockable>
  struct with_no_lockable {
    typedef LC_CSR_Graph<
        NodeTy, EdgeTy, _has_no_lockable, UseNumaAlloc, HasOutOfLineLockable,
        FileEdgeTy>
        type;
  };
  template <bool _has_no_lockable>
  using _with_no_lockable = LC_CSR_Graph<
      NodeTy, EdgeTy, _has_no_lockable, UseNumaAlloc, HasOutOfLineLockable,
      FileEdgeTy>;

  //! If true, use NUMA-aware graph allocation; otherwise, use NUMA interleaved
  //! allocation.
  template <bool _use_numa_alloc>
  struct with_numa_alloc {
    typedef LC_CSR_Graph<
        NodeTy, EdgeTy, HasNoLockable, _use_numa_alloc, HasOutOfLineLockable,
        FileEdgeTy>
        type;
  };
  template <bool _use_numa_alloc>
  using _with_numa_alloc = LC_CSR_Graph<
      NodeTy, EdgeTy, HasNoLockable, _use_numa_alloc, HasOutOfLineLockable,
      FileEdgeTy>;

  //! If true, store abstract locks separate from nodes
  template <bool _has_out_of_line_lockable>
  struct with_out_of_line_lockable {
    typedef LC_CSR_Graph<
        NodeTy, EdgeTy, HasNoLockable, UseNumaAlloc, _has_out_of_line_lockable,
        FileEdgeTy>
        type;
  };

  typedef read_default_graph_tag read_tag;

protected:
  typedef LargeArray<EdgeTy> EdgeData;
  typedef LargeArray<uint32_t> EdgeDst;
  typedef internal::NodeInfoBaseTypes<
      NodeTy, !HasNoLockable && !HasOutOfLineLockable>
      NodeInfoTypes;
  typedef internal::NodeInfoBase<
      NodeTy, !HasNoLockable && !HasOutOfLineLockable>
      NodeInfo;
  typedef LargeArray<uint64_t> EdgeIndData;
  typedef LargeArray<NodeInfo> NodeData;

public:
  typedef uint32_t GraphNode;
  typedef EdgeTy edge_data_type;
  typedef FileEdgeTy file_edge_data_type;
  typedef NodeTy node_data_type;
  typedef typename EdgeData::reference edge_data_reference;
  typedef typename NodeInfoTypes::reference node_data_reference;
  using edge_iterator =
      boost::counting_iterator<typename EdgeIndData::value_type>;
  using edges_iterator = StandardRange<NoDerefIterator<edge_iterator>>;
  using iterator = boost::counting_iterator<typename EdgeDst::value_type>;
  typedef iterator const_iterator;
  typedef iterator local_iterator;
  typedef iterator const_local_iterator;

protected:
  NodeData nodeData;
  EdgeIndData edgeIndData;
  EdgeDst edgeDst;
  EdgeData edgeData;

  uint64_t numNodes;
  uint64_t numEdges;

  typedef internal::EdgeSortIterator<
      GraphNode, typename EdgeIndData::value_type, EdgeDst, EdgeData>
      edge_sort_iterator;

  edge_iterator raw_begin(GraphNode N) const {
    return edge_iterator((N == 0) ? 0 : edgeIndData[N - 1]);
  }

  edge_iterator raw_end(GraphNode N) const {
    return edge_iterator(edgeIndData[N]);
  }

  edge_sort_iterator edge_sort_begin(GraphNode N) {
    return edge_sort_iterator(*raw_begin(N), &edgeDst, &edgeData);
  }

  edge_sort_iterator edge_sort_end(GraphNode N) {
    return edge_sort_iterator(*raw_end(N), &edgeDst, &edgeData);
  }

  template <bool _A1 = HasNoLockable, bool _A2 = HasOutOfLineLockable>
  void acquireNode(
      GraphNode N, MethodFlag mflag,
      typename std::enable_if<!_A1 && !_A2>::type* = 0) {
    katana::acquire(&nodeData[N], mflag);
  }

  template <bool _A1 = HasOutOfLineLockable, bool _A2 = HasNoLockable>
  void acquireNode(
      GraphNode N, MethodFlag mflag,
      typename std::enable_if<_A1 && !_A2>::type* = 0) {
    this->outOfLineAcquire(getId(N), mflag);
  }

  template <bool _A1 = HasOutOfLineLockable, bool _A2 = HasNoLockable>
  void acquireNode(
      GraphNode, MethodFlag, typename std::enable_if<_A2>::type* = 0) {}

  template <
      bool _A1 = EdgeData::has_value,
      bool _A2 = LargeArray<FileEdgeTy>::has_value>
  void constructEdgeValue(
      FileGraph& graph, typename FileGraph::edge_iterator nn,
      typename std::enable_if<!_A1 || _A2>::type* = 0) {
    typedef LargeArray<FileEdgeTy> FED;
    if (EdgeData::has_value)
      edgeData.set(*nn, graph.getEdgeData<typename FED::value_type>(nn));
  }

  template <
      bool _A1 = EdgeData::has_value,
      bool _A2 = LargeArray<FileEdgeTy>::has_value>
  void constructEdgeValue(
      FileGraph&, typename FileGraph::edge_iterator nn,
      typename std::enable_if<_A1 && !_A2>::type* = 0) {
    edgeData.set(*nn, {});
  }

  size_t getId(GraphNode N) { return N; }

  GraphNode getNode(size_t n) { return n; }

public:
  LC_CSR_Graph() = default;

  /**
   * Accesses the "prefix sum" of this graph; takes advantage of the fact
   * that edge_end(n) is basically prefix_sum[n] (if a prefix sum existed +
   * if prefix_sum[0] = number of edges in node 0).
   *
   * ONLY USE IF GRAPH HAS BEEN LOADED
   *
   * @param n Index into edge prefix sum
   * @returns The value that would be located at index n in an edge prefix sum
   * array
   */
  uint64_t operator[](uint64_t n) { return *(edge_end(n)); }

  template <typename EdgeNumFnTy, typename EdgeDstFnTy, typename EdgeDataFnTy>
  LC_CSR_Graph(
      uint32_t _numNodes, uint64_t _numEdges, EdgeNumFnTy edgeNum,
      EdgeDstFnTy _edgeDst, EdgeDataFnTy _edgeData)
      : numNodes(_numNodes), numEdges(_numEdges) {
    if (UseNumaAlloc) {
      //! [numaallocex]
      nodeData.allocateBlocked(numNodes);
      edgeIndData.allocateBlocked(numNodes);
      edgeDst.allocateBlocked(numEdges);
      edgeData.allocateBlocked(numEdges);
      //! [numaallocex]
      this->outOfLineAllocateBlocked(numNodes, false);
    } else {
      nodeData.allocateInterleaved(numNodes);
      edgeIndData.allocateInterleaved(numNodes);
      edgeDst.allocateInterleaved(numEdges);
      edgeData.allocateInterleaved(numEdges);
      this->outOfLineAllocateInterleaved(numNodes);
    }
    for (size_t n = 0; n < numNodes; ++n) {
      nodeData.constructAt(n);
    }
    uint64_t cur = 0;
    for (size_t n = 0; n < numNodes; ++n) {
      cur += edgeNum(n);
      edgeIndData[n] = cur;
    }
    cur = 0;
    for (size_t n = 0; n < numNodes; ++n) {
      for (uint64_t e = 0, ee = edgeNum(n); e < ee; ++e) {
        if (EdgeData::has_value)
          edgeData.set(cur, _edgeData(n, e));
        edgeDst[cur] = _edgeDst(n, e);
        ++cur;
      }
    }
  }

  node_data_reference getData(
      GraphNode N, MethodFlag mflag = MethodFlag::WRITE) {
    // katana::checkWrite(mflag, false);
    NodeInfo& NI = nodeData[N];
    acquireNode(N, mflag);
    return NI.getData();
  }

  edge_data_reference getEdgeData(
      edge_iterator ni,
      [[maybe_unused]] MethodFlag mflag = MethodFlag::UNPROTECTED) {
    // katana::checkWrite(mflag, false);
    return edgeData[*ni];
  }

  GraphNode getEdgeDst(edge_iterator ni) const { return edgeDst[*ni]; }

  size_t size() const { return numNodes; }
  size_t sizeEdges() const { return numEdges; }

  uint64_t num_nodes() const { return numNodes; }
  uint64_t num_edges() const { return numEdges; }

  iterator begin() const { return iterator(0); }
  iterator end() const { return iterator(numNodes); }

  const_local_iterator local_begin() const {
    return const_local_iterator(this->localBegin(numNodes));
  }

  const_local_iterator local_end() const {
    return const_local_iterator(this->localEnd(numNodes));
  }

  local_iterator local_begin() {
    return local_iterator(this->localBegin(numNodes));
  }

  local_iterator local_end() {
    return local_iterator(this->localEnd(numNodes));
  }

  edge_iterator edge_begin(GraphNode N, MethodFlag mflag = MethodFlag::WRITE) {
    acquireNode(N, mflag);
    if (!HasNoLockable && katana::shouldLock(mflag)) {
      for (edge_iterator ii = raw_begin(N), ee = raw_end(N); ii != ee; ++ii) {
        acquireNode(edgeDst[*ii], mflag);
      }
    }
    return raw_begin(N);
  }

  edge_iterator edge_end(GraphNode N, MethodFlag mflag = MethodFlag::WRITE) {
    acquireNode(N, mflag);
    return raw_end(N);
  }

  edge_iterator edge_begin(GraphNode N) const { return raw_begin(N); }

  edge_iterator edge_end(GraphNode N) const { return raw_end(N); }

  auto getDegree(GraphNode N) const {
    return std::distance(raw_begin(N), raw_end(N));
  }

  edge_iterator findEdge(GraphNode N1, GraphNode N2) {
    return std::find_if(edge_begin(N1), edge_end(N1), [=](edge_iterator e) {
      return getEdgeDst(e) == N2;
    });
  }

  edge_iterator findEdgeSortedByDst(GraphNode N1, GraphNode N2) {
    auto e = std::lower_bound(
        edge_begin(N1), edge_end(N1), N2,
        [=](edge_iterator e, GraphNode N) { return getEdgeDst(e) < N; });
    return (getEdgeDst(e) == N2) ? e : edge_end(N1);
  }

  edges_iterator edges(GraphNode N, MethodFlag mflag = MethodFlag::WRITE) {
    return internal::make_no_deref_range(
        edge_begin(N, mflag), edge_end(N, mflag));
  }

  edges_iterator out_edges(GraphNode N, MethodFlag mflag = MethodFlag::WRITE) {
    return edges(N, mflag);
  }

  /**
   * Sorts outgoing edges of a node. Comparison function is over EdgeTy.
   */
  template <typename CompTy>
  void sortEdgesByEdgeData(
      GraphNode N, const CompTy& comp = std::less<EdgeTy>(),
      MethodFlag mflag = MethodFlag::WRITE) {
    acquireNode(N, mflag);
    std::sort(
        edge_sort_begin(N), edge_sort_end(N),
        internal::EdgeSortCompWrapper<EdgeSortValue<GraphNode, EdgeTy>, CompTy>(
            comp));
  }

  /**
   * Sorts outgoing edges of a node.
   * Comparison function is over <code>EdgeSortValue<EdgeTy></code>.
   */
  template <typename CompTy>
  void sortEdges(
      GraphNode N, const CompTy& comp, MethodFlag mflag = MethodFlag::WRITE) {
    acquireNode(N, mflag);
    std::sort(edge_sort_begin(N), edge_sort_end(N), comp);
  }

  /**
   * Sorts outgoing edges of a node. Comparison is over getEdgeDst(e).
   */
  void sortEdgesByDst(GraphNode N, MethodFlag mflag = MethodFlag::WRITE) {
    acquireNode(N, mflag);
    typedef EdgeSortValue<GraphNode, EdgeTy> EdgeSortVal;
    std::sort(
        edge_sort_begin(N), edge_sort_end(N),
        [=](const EdgeSortVal& e1, const EdgeSortVal& e2) {
          return e1.dst < e2.dst;
        });
  }

  /**
   * Sorts all outgoing edges of all nodes in parallel. Comparison is over
   * getEdgeDst(e).
   */
  void sortAllEdgesByDst(MethodFlag mflag = MethodFlag::WRITE) {
    katana::do_all(
        katana::iterate(size_t{0}, this->size()),
        [=](GraphNode N) { this->sortEdgesByDst(N, mflag); },
        katana::no_stats(), katana::steal());
  }

  void allocateFrom(const FileGraph& graph) {
    numNodes = graph.size();
    numEdges = graph.sizeEdges();
    if (UseNumaAlloc) {
      nodeData.allocateBlocked(numNodes);
      edgeIndData.allocateBlocked(numNodes);
      edgeDst.allocateBlocked(numEdges);
      edgeData.allocateBlocked(numEdges);
      this->outOfLineAllocateBlocked(numNodes);
    } else {
      nodeData.allocateInterleaved(numNodes);
      edgeIndData.allocateInterleaved(numNodes);
      edgeDst.allocateInterleaved(numEdges);
      edgeData.allocateInterleaved(numEdges);
      this->outOfLineAllocateInterleaved(numNodes);
    }
  }

  void allocateFrom(uint32_t nNodes, uint64_t nEdges) {
    numNodes = nNodes;
    numEdges = nEdges;

    if (UseNumaAlloc) {
      nodeData.allocateBlocked(numNodes);
      edgeIndData.allocateBlocked(numNodes);
      edgeDst.allocateBlocked(numEdges);
      edgeData.allocateBlocked(numEdges);
      this->outOfLineAllocateBlocked(numNodes);
    } else {
      nodeData.allocateInterleaved(numNodes);
      edgeIndData.allocateInterleaved(numNodes);
      edgeDst.allocateInterleaved(numEdges);
      edgeData.allocateInterleaved(numEdges);
      this->outOfLineAllocateInterleaved(numNodes);
    }
  }

  void destroyAndAllocateFrom(uint32_t nNodes, uint64_t nEdges) {
    numNodes = nNodes;
    numEdges = nEdges;

    deallocate();
    if (UseNumaAlloc) {
      nodeData.allocateBlocked(numNodes);
      edgeIndData.allocateBlocked(numNodes);
      edgeDst.allocateBlocked(numEdges);
      edgeData.allocateBlocked(numEdges);
      this->outOfLineAllocateBlocked(numNodes);
    } else {
      nodeData.allocateInterleaved(numNodes);
      edgeIndData.allocateInterleaved(numNodes);
      edgeDst.allocateInterleaved(numEdges);
      edgeData.allocateInterleaved(numEdges);
      this->outOfLineAllocateInterleaved(numNodes);
    }
  }

  void constructNodes() {
#ifndef KATANA_GRAPH_CONSTRUCT_SERIAL
    for (uint32_t x = 0; x < numNodes; ++x) {
      nodeData.constructAt(x);
      this->outOfLineConstructAt(x);
    }
#else
    katana::do_all(
        katana::iterate(UINT64_C(0), numNodes),
        [&](uint64_t x) {
          nodeData.constructAt(x);
          this->outOfLineConstructAt(x);
        },
        katana::no_stats(), katana::loopname("CONSTRUCT_NODES"));
#endif
  }

  void deallocate() {
    nodeData.destroy();
    nodeData.deallocate();

    edgeIndData.deallocate();
    edgeIndData.destroy();

    edgeDst.deallocate();
    edgeDst.destroy();

    edgeData.deallocate();
    edgeData.destroy();
  }

  void constructEdge(
      uint64_t e, uint32_t dst, const typename EdgeData::value_type& val) {
    edgeData.set(e, val);
    edgeDst[e] = dst;
  }

  void constructEdge(uint64_t e, uint32_t dst) { edgeDst[e] = dst; }

  void fixEndEdge(uint32_t n, uint64_t e) { edgeIndData[n] = e; }

  /**
   * Perform an in-memory transpose of the graph, replacing the original
   * CSR to CSC
   */
  void transpose(const char* regionName = NULL) {
    katana::StatTimer timer("TIMER_GRAPH_TRANSPOSE", regionName);
    timer.start();

    EdgeDst edgeDst_old;
    EdgeData edgeData_new;
    EdgeIndData edgeIndData_old;
    EdgeIndData edgeIndData_temp;

    if (UseNumaAlloc) {
      edgeIndData_old.allocateBlocked(numNodes);
      edgeIndData_temp.allocateBlocked(numNodes);
      edgeDst_old.allocateBlocked(numEdges);
      edgeData_new.allocateBlocked(numEdges);
    } else {
      edgeIndData_old.allocateInterleaved(numNodes);
      edgeIndData_temp.allocateInterleaved(numNodes);
      edgeDst_old.allocateInterleaved(numEdges);
      edgeData_new.allocateInterleaved(numEdges);
    }

    // Copy old node->index location + initialize the temp array
    katana::do_all(
        katana::iterate(UINT64_C(0), numNodes),
        [&](uint64_t n) {
          edgeIndData_old[n] = edgeIndData[n];
          edgeIndData_temp[n] = 0;
        },
        katana::no_stats(), katana::loopname("TRANSPOSE_EDGEINTDATA_COPY"));

    // get destination of edge, copy to array, and
    katana::do_all(
        katana::iterate(UINT64_C(0), numEdges),
        [&](uint64_t e) {
          auto dst = edgeDst[e];
          edgeDst_old[e] = dst;
          // counting outgoing edges in the tranpose graph by
          // counting incoming edges in the original graph
          __sync_add_and_fetch(&edgeIndData_temp[dst], 1);
        },
        katana::no_stats(), katana::loopname("TRANSPOSE_EDGEINTDATA_INC"));

    // TODO is it worth doing parallel prefix sum?
    // prefix sum calculation of the edge index array
    for (uint32_t n = 1; n < numNodes; ++n) {
      edgeIndData_temp[n] += edgeIndData_temp[n - 1];
    }

    // copy over the new tranposed edge index data
    katana::do_all(
        katana::iterate(UINT64_C(0), numNodes),
        [&](uint64_t n) { edgeIndData[n] = edgeIndData_temp[n]; },
        katana::no_stats(), katana::loopname("TRANSPOSE_EDGEINTDATA_SET"));

    // edgeIndData_temp[i] will now hold number of edges that all nodes
    // before the ith node have
    if (numNodes >= 1) {
      edgeIndData_temp[0] = 0;
      katana::do_all(
          katana::iterate(UINT64_C(1), numNodes),
          [&](uint64_t n) { edgeIndData_temp[n] = edgeIndData[n - 1]; },
          katana::no_stats(), katana::loopname("TRANSPOSE_EDGEINTDATA_TEMP"));
    }

    katana::do_all(
        katana::iterate(UINT64_C(0), numNodes),
        [&](uint64_t src) {
          // e = start index into edge array for a particular node
          uint64_t e = (src == 0) ? 0 : edgeIndData_old[src - 1];

          // get all outgoing edges of a particular node in the
          // non-transpose and convert to incoming
          while (e < edgeIndData_old[src]) {
            // destination nodde
            auto dst = edgeDst_old[e];
            // location to save edge
            auto e_new = __sync_fetch_and_add(&(edgeIndData_temp[dst]), 1);
            // save src as destination
            edgeDst[e_new] = src;
            // copy edge data to "new" array
            edgeDataCopy(edgeData_new, edgeData, e_new, e);
            e++;
          }
        },
        katana::no_stats(), katana::loopname("TRANSPOSE_EDGEDST"));

    // if edge weights, then overwrite edgeData with new edge data
    if (EdgeData::has_value) {
      katana::do_all(
          katana::iterate(UINT64_C(0), numEdges),
          [&](uint64_t e) { edgeDataCopy(edgeData, edgeData_new, e, e); },
          katana::no_stats(), katana::loopname("TRANSPOSE_EDGEDATA_SET"));
    }

    timer.stop();
  }

  template <bool is_non_void = EdgeData::has_value>
  void edgeDataCopy(
      EdgeData& edgeData_new, EdgeData& edgeData, uint64_t e_new, uint64_t e,
      typename std::enable_if<is_non_void>::type* = 0) {
    edgeData_new[e_new] = edgeData[e];
  }

  template <bool is_non_void = EdgeData::has_value>
  void edgeDataCopy(
      EdgeData&, EdgeData&, uint64_t, uint64_t,
      typename std::enable_if<!is_non_void>::type* = 0) {
    // does nothing
  }

  template <
      typename E = EdgeTy,
      std::enable_if_t<!std::is_same<E, void>::value, int>* = nullptr>
  void constructFrom(
      FileGraph& graph, unsigned tid, unsigned total,
      const bool readUnweighted = false) {
    // at this point memory should already be allocated
    auto r =
        graph
            .divideByNode(
                NodeData::size_of::value + EdgeIndData::size_of::value +
                    LC_CSR_Graph::size_of_out_of_line::value,
                EdgeDst::size_of::value + EdgeData::size_of::value, tid, total)
            .first;

    this->setLocalRange(*r.first, *r.second);

    for (FileGraph::iterator ii = r.first, ei = r.second; ii != ei; ++ii) {
      nodeData.constructAt(*ii);
      edgeIndData[*ii] = *graph.edge_end(*ii);

      this->outOfLineConstructAt(*ii);

      for (FileGraph::edge_iterator nn = graph.edge_begin(*ii),
                                    en = graph.edge_end(*ii);
           nn != en; ++nn) {
        if (readUnweighted) {
          edgeData.set(*nn, {});
        } else {
          constructEdgeValue(graph, nn);
        }
        edgeDst[*nn] = graph.getEdgeDst(nn);
      }
    }
  }

  template <
      typename E = EdgeTy,
      std::enable_if_t<std::is_same<E, void>::value, int>* = nullptr>
  void constructFrom(
      FileGraph& graph, unsigned tid, unsigned total,
      [[maybe_unused]] const bool readUnweighted = false) {
    // at this point memory should already be allocated
    auto r =
        graph
            .divideByNode(
                NodeData::size_of::value + EdgeIndData::size_of::value +
                    LC_CSR_Graph::size_of_out_of_line::value,
                EdgeDst::size_of::value + EdgeData::size_of::value, tid, total)
            .first;

    this->setLocalRange(*r.first, *r.second);

    for (FileGraph::iterator ii = r.first, ei = r.second; ii != ei; ++ii) {
      nodeData.constructAt(*ii);
      edgeIndData[*ii] = *graph.edge_end(*ii);

      this->outOfLineConstructAt(*ii);

      for (FileGraph::edge_iterator nn = graph.edge_begin(*ii),
                                    en = graph.edge_end(*ii);
           nn != en; ++nn) {
        constructEdgeValue(graph, nn);
        edgeDst[*nn] = graph.getEdgeDst(nn);
      }
    }
  }

  /**
   * Returns the reference to the edgeIndData LargeArray
   * (a prefix sum of edges)
   *
   * @returns reference to LargeArray edgeIndData
   */
  const EdgeIndData& getEdgePrefixSum() const { return edgeIndData; }

  //! Set the edge data for a specified edge; assumes memory already allocated
  void setEdgeData(uint64_t e, const typename EdgeData::value_type& val) {
    edgeData.set(e, val);
  }

  auto divideByNode(size_t nodeSize, size_t edgeSize, size_t id, size_t total) {
    return katana::divideNodesBinarySearch(
        numNodes, numEdges, nodeSize, edgeSize, id, total, edgeIndData);
  }

  /**
   *
   * custom allocator for vector<vector<>>
   * Adding for Louvain clustering
   * TODO: Find better way to do this
   */
  void constructFrom(
      uint32_t numNodes, uint64_t numEdges, std::vector<uint64_t>& prefix_sum,
      std::vector<std::vector<uint32_t>>& edges_id,
      std::vector<std::vector<EdgeTy>>& edges_data) {
    // allocateFrom(numNodes, numEdges);
    /*
     * Deallocate if reusing the graph
     */
    destroyAndAllocateFrom(numNodes, numEdges);
    constructNodes();

    katana::do_all(katana::iterate((uint32_t)0, numNodes), [&](uint32_t n) {
      edgeIndData[n] = prefix_sum[n];
    });

    katana::do_all(katana::iterate((uint32_t)0, numNodes), [&](uint32_t n) {
      if (n == 0) {
        if (edgeIndData[n] > 0) {
          std::copy(edges_id[n].begin(), edges_id[n].end(), edgeDst.begin());
          std::copy(
              edges_data[n].begin(), edges_data[n].end(), edgeData.begin());
        }
      } else {
        if (edgeIndData[n] - edgeIndData[n - 1] > 0) {
          std::copy(
              edges_id[n].begin(), edges_id[n].end(),
              edgeDst.begin() + edgeIndData[n - 1]);
          std::copy(
              edges_data[n].begin(), edges_data[n].end(),
              edgeData.begin() + edgeIndData[n - 1]);
        }
      }
    });

    initializeLocalRanges();
  }
  void constructFrom(
      uint32_t numNodes, uint64_t numEdges, std::vector<uint64_t>& prefix_sum,
      katana::gstl::Vector<katana::PODResizeableArray<uint32_t>>& edges_id,
      std::vector<std::vector<EdgeTy>>& edges_data) {
    allocateFrom(numNodes, numEdges);
    constructNodes();

    katana::do_all(katana::iterate((uint32_t)0, numNodes), [&](uint32_t n) {
      edgeIndData[n] = prefix_sum[n];
    });

    katana::do_all(katana::iterate((uint32_t)0, numNodes), [&](uint32_t n) {
      if (n == 0) {
        if (edgeIndData[n] > 0) {
          std::copy(edges_id[n].begin(), edges_id[n].end(), edgeDst.begin());
          std::copy(
              edges_data[n].begin(), edges_data[n].end(), edgeData.begin());
        }
      } else {
        if (edgeIndData[n] - edgeIndData[n - 1] > 0) {
          std::copy(
              edges_id[n].begin(), edges_id[n].end(),
              edgeDst.begin() + edgeIndData[n - 1]);
          std::copy(
              edges_data[n].begin(), edges_data[n].end(),
              edgeData.begin() + edgeIndData[n - 1]);
        }
      }
    });

    initializeLocalRanges();
  }

  template <
      typename E = EdgeTy,
      std::enable_if_t<!std::is_same<E, void>::value, int>* = nullptr>
  void constructFrom(
      uint32_t numNodes, uint64_t numEdges,
      katana::LargeArray<uint64_t>&& prefix_sum,
      katana::gstl::Vector<katana::PODResizeableArray<uint32_t>>& edges_id,
      std::vector<std::vector<EdgeTy>>& edges_data) {
    allocateFrom(numNodes, numEdges);
    constructNodes();

    edgeIndData = std::move(prefix_sum);

    katana::do_all(katana::iterate((uint32_t)0, numNodes), [&](uint32_t n) {
      if (n == 0) {
        if (edgeIndData[n] > 0) {
          std::copy(edges_id[n].begin(), edges_id[n].end(), edgeDst.begin());
          std::copy(
              edges_data[n].begin(), edges_data[n].end(), edgeData.begin());
        }
      } else {
        if (edgeIndData[n] - edgeIndData[n - 1] > 0) {
          std::copy(
              edges_id[n].begin(), edges_id[n].end(),
              edgeDst.begin() + edgeIndData[n - 1]);
          std::copy(
              edges_data[n].begin(), edges_data[n].end(),
              edgeData.begin() + edgeIndData[n - 1]);
        }
      }
    });

    initializeLocalRanges();
  }

  template <
      typename E = EdgeTy,
      std::enable_if_t<std::is_same<E, void>::value, int>* = nullptr>
  void constructFrom(
      uint32_t numNodes, uint64_t numEdges,
      katana::LargeArray<uint64_t>&& prefix_sum,
      katana::gstl::Vector<katana::PODResizeableArray<uint32_t>>& edges_id) {
    allocateFrom(numNodes, numEdges);
    constructNodes();

    edgeIndData = std::move(prefix_sum);

    katana::do_all(katana::iterate((uint32_t)0, numNodes), [&](uint32_t n) {
      if (n == 0) {
        if (edgeIndData[n] > 0) {
          std::copy(edges_id[n].begin(), edges_id[n].end(), edgeDst.begin());
        }
      } else {
        if (edgeIndData[n] - edgeIndData[n - 1] > 0) {
          std::copy(
              edges_id[n].begin(), edges_id[n].end(),
              edgeDst.begin() + edgeIndData[n - 1]);
        }
      }
    });

    initializeLocalRanges();
  }

  /**
   * Reads the GR files directly into in-memory
   * data-structures of LC_CSR graphs using freads.
   *
   * Edge is not void.
   *
   */
  template <
      typename U = void,
      typename std::enable_if<!std::is_void<EdgeTy>::value, U>::type* = nullptr>
  void readGraphFromGRFile(const std::string& filename) {
    std::ifstream graphFile(filename.c_str());
    uint64_t header[4];
    graphFile.read(reinterpret_cast<char*>(header), sizeof(uint64_t) * 4);
    if (!graphFile) {
      KATANA_DIE("failed to read file");
    }
    uint64_t version = header[0];
    numNodes = header[2];
    numEdges = header[3];
    katana::gPrint(
        "Number of Nodes: ", numNodes, ", Number of Edges: ", numEdges, "\n");
    allocateFrom(numNodes, numEdges);
    constructNodes();
    /**
     * Load outIndex array
     **/
    assert(edgeIndData.data());
    if (!edgeIndData.data()) {
      KATANA_DIE("out of memory");
    }

    // start position to read index data
    uint64_t readPosition = (4 * sizeof(uint64_t));
    graphFile.seekg(readPosition);
    graphFile.read(
        reinterpret_cast<char*>(edgeIndData.data()),
        sizeof(uint64_t) * numNodes);
    if (!graphFile) {
      KATANA_DIE("failed to read file");
    }

    /**
     * Load edgeDst array
     **/
    assert(edgeDst.data());
    if (!edgeDst.data()) {
      KATANA_DIE("out of memory");
    }

    readPosition = ((4 + numNodes) * sizeof(uint64_t));
    graphFile.seekg(readPosition);
    if (version == 1) {
      graphFile.read(
          reinterpret_cast<char*>(edgeDst.data()), sizeof(uint32_t) * numEdges);
      if (!graphFile) {
        KATANA_DIE("failed to read file");
      }
      readPosition =
          ((4 + numNodes) * sizeof(uint64_t) + numEdges * sizeof(uint32_t));
      // version 1 padding TODO make version agnostic
      if (numEdges % 2) {
        readPosition += sizeof(uint32_t);
      }
    } else if (version == 2) {
      graphFile.read(
          reinterpret_cast<char*>(edgeDst.data()), sizeof(uint64_t) * numEdges);
      if (!graphFile) {
        KATANA_DIE("failed to read file");
      }
      readPosition =
          ((4 + numNodes) * sizeof(uint64_t) + numEdges * sizeof(uint64_t));
      if (numEdges % 2) {
        readPosition += sizeof(uint64_t);
      }
    } else {
      KATANA_DIE("unknown file version: ", version);
    }
    /**
     * Load edge data array
     **/
    assert(edgeData.data());
    if (!edgeData.data()) {
      KATANA_DIE("out of memory");
    }
    graphFile.seekg(readPosition);
    graphFile.read(
        reinterpret_cast<char*>(edgeData.data()), sizeof(EdgeTy) * numEdges);
    if (!graphFile) {
      KATANA_DIE("failed to read file");
    }

    initializeLocalRanges();
  }

  /**
   * Reads the GR files directly into in-memory
   * data-structures of LC_CSR graphs using freads.
   *
   * Edge is void.
   *
   */
  template <
      typename U = void,
      typename std::enable_if<std::is_void<EdgeTy>::value, U>::type* = nullptr>
  void readGraphFromGRFile(const std::string& filename) {
    std::ifstream graphFile(filename.c_str());
    uint64_t header[4];
    graphFile.read(reinterpret_cast<char*>(header), sizeof(uint64_t) * 4);
    if (!graphFile) {
      KATANA_DIE("failed to read file");
    }

    uint64_t version = header[0];
    numNodes = header[2];
    numEdges = header[3];
    katana::gPrint(
        "Number of Nodes: ", numNodes, ", Number of Edges: ", numEdges, "\n");
    allocateFrom(numNodes, numEdges);
    constructNodes();
    /**
     * Load outIndex array
     **/
    assert(edgeIndData.data());
    if (!edgeIndData.data()) {
      KATANA_DIE("out of memory");
    }
    // start position to read index data
    uint64_t readPosition = (4 * sizeof(uint64_t));
    graphFile.seekg(readPosition);
    graphFile.read(
        reinterpret_cast<char*>(edgeIndData.data()),
        sizeof(uint64_t) * numNodes);
    if (!graphFile) {
      KATANA_DIE("failed to read file");
    }

    /**
     * Load edgeDst array
     **/
    assert(edgeDst.data());
    if (!edgeDst.data()) {
      KATANA_DIE("out of memory");
    }
    readPosition = ((4 + numNodes) * sizeof(uint64_t));
    graphFile.seekg(readPosition);
    if (version == 1) {
      graphFile.read(
          reinterpret_cast<char*>(edgeDst.data()), sizeof(uint32_t) * numEdges);
      if (!graphFile) {
        KATANA_DIE("failed to read file");
      }
    } else if (version == 2) {
      graphFile.read(
          reinterpret_cast<char*>(edgeDst.data()), sizeof(uint64_t) * numEdges);
      if (!graphFile) {
        KATANA_DIE("failed to read file");
      }
    } else {
      KATANA_DIE("unknown file version: ", version);
    }

    initializeLocalRanges();
  }

  /**
   * Given a manually created graph, initialize the local ranges on this graph
   * so that threads can iterate over a balanced number of vertices.
   */
  void initializeLocalRanges() {
    katana::on_each([&](unsigned tid, unsigned total) {
      auto r = divideByNode(0, 1, tid, total).first;
      this->setLocalRange(*r.first, *r.second);
    });
  }

  /**
   * Return degrees in a vector; useful if degrees need to be accessed quickly
   * (1 memory access instead of 2 from subtracting begin and end)
   */
  gstl::Vector<uint32_t> countDegrees() const {
    gstl::Vector<uint32_t> savedDegrees(numNodes);
    katana::do_all(
        katana::iterate(this->begin(), this->end()),
        [&](unsigned v) { savedDegrees[v] = this->getDegree(v); },
        katana::loopname("DegreeCounting"));
    return savedDegrees;
  }
};

}  // namespace katana

#endif
