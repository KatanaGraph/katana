#pragma once

#include "galois/graphs/LC_CSR_CSC_Graph.h"

namespace galois {
namespace graphs {

/**
 * A LC_CSR_CSC_Graph specialized for edge labels.
 *
 * @tparam NodeTy type of the node data
 * @tparam EdgeTy type of the edge data
 * @tparam EdgeDataByValue If set to true, the in-edges will have their own
 * copy of the edge data. Otherwise, the in-edge edge data will be shared with
 * its corresponding out-edge.
 * @tparam HasNoLockable If set to true, then node accesses will cannot acquire
 * an abstract lock. Otherwise, accessing nodes can get a lock.
 * @tparam UseNumaAlloc If set to true, allocate data in a possibly more NUMA
 * friendly way.
 * @tparam HasOutOfLineLockable
 * @tparam FileEdgeTy
 */
template <typename NodeTy, typename EdgeTy, bool EdgeDataByValue = false,
          bool HasNoLockable = false, bool UseNumaAlloc = false,
          bool HasOutOfLineLockable = false, typename FileEdgeTy = EdgeTy>
class LC_CSR_Labeled_Graph
    : public LC_CSR_CSC_Graph<NodeTy, EdgeTy, EdgeDataByValue, HasNoLockable,
                              UseNumaAlloc, HasOutOfLineLockable, FileEdgeTy> {
  // typedef to make it easier to read
  //! Typedef referring to base LC_CSR_Graph
  using BaseGraph = LC_CSR_Graph<NodeTy, EdgeTy, HasNoLockable, UseNumaAlloc,
                                 HasOutOfLineLockable, FileEdgeTy>;
  //! Typedef referring to the derived LC_CSR_CSC_Graph class
  using DerivedGraph =
      LC_CSR_CSC_Graph<NodeTy, EdgeTy, EdgeDataByValue, HasNoLockable,
                       UseNumaAlloc, HasOutOfLineLockable, FileEdgeTy>;
  //! Typedef referring to this class itself
  using ThisGraph =
      LC_CSR_Labeled_Graph<NodeTy, EdgeTy, EdgeDataByValue, HasNoLockable,
                           UseNumaAlloc, HasOutOfLineLockable, FileEdgeTy>;

public:
  //! Graph node typedef
  using GraphNode = uint32_t;

protected:
  // retypedefs of base class
  //! large array for edge data
  using EdgeData = LargeArray<EdgeTy>;
  //! large array for edge destinations
  using EdgeDst = LargeArray<uint32_t>;
  //! large array for edge index data
  using EdgeIndData = LargeArray<uint64_t>;

public:
  //! iterator for edges
  using edge_iterator =
      boost::counting_iterator<typename EdgeIndData::value_type>;
  //! iterator for data
  using data_iterator = typename std::vector<EdgeTy>::const_iterator;
  //! reference to edge data
  using edge_data_reference = typename EdgeData::reference;

protected:
  //! edge index data for the labeled edges
  EdgeIndData edgeIndDataLabeled;
  //! edge index data for the reverse labeled edges
  EdgeIndData inEdgeIndDataLabeled;

  //! distinct edge data labels
  uint32_t numEdgeLabels;
  //! map from edge index to edge label
  std::vector<EdgeTy> edgeIndexToLabelMap;
  //! map from edge label to edge index
  std::unordered_map<EdgeTy, uint32_t> edgeLabelToIndexMap;

  void constructEdgeLabelIndex() {
    galois::substrate::PerThreadStorage<std::set<EdgeTy>> edgeLabels;
    galois::do_all(
        galois::iterate((size_t)0, this->size()),
        [&](GraphNode N) {
          for (auto e : edges(N)) {
            auto& data = this->getEdgeData(e);
            edgeLabels.getLocal()->insert(data);
          }
          for (auto e : in_edges(N)) {
            auto& data = this->getInEdgeData(e);
            edgeLabels.getLocal()->insert(data);
          }
        },
        galois::no_stats(), galois::steal());

    // ordered map
    std::map<EdgeTy, uint32_t> sortedMap;
    for (uint32_t i = 0; i < galois::runtime::activeThreads; ++i) {
      auto& edgeLabelsSet = *edgeLabels.getRemote(i);
      for (auto edgeLabel : edgeLabelsSet) {
        sortedMap[edgeLabel] = 1;
      }
    }

    // unordered map
    numEdgeLabels = 0;
    for (auto edgeLabelPair : sortedMap) {
      edgeLabelToIndexMap[edgeLabelPair.first] = numEdgeLabels++;
      edgeIndexToLabelMap.push_back(edgeLabelPair.first);
    }
  }

  void constructEdgeIndDataLabeled() {
    size_t size = BaseGraph::size() * numEdgeLabels;
    if (UseNumaAlloc) {
      edgeIndDataLabeled.allocateBlocked(size);
    } else {
      edgeIndDataLabeled.allocateInterleaved(size);
    }

    galois::do_all(
        galois::iterate((size_t)0, this->size()),
        [&](GraphNode N) {
          auto offset    = N * this->numEdgeLabels;
          uint32_t index = 0;
          for (auto e : edges(N)) {
            auto& data = this->getEdgeData(e);
            while (data != edgeIndexToLabelMap[index]) {
              this->edgeIndDataLabeled[offset + index] = *e;
              index++;
              assert(index < this->numEdgeLabels);
            }
          }
          auto e = edge_end(N);
          while (index < this->numEdgeLabels) {
            this->edgeIndDataLabeled[offset + index] = *e;
            index++;
          }
        },
        galois::no_stats(), galois::steal());

    // for (size_t i = 0; i < BaseGraph::size(); ++i) {
    //   for (size_t j = 0; j < numEdgeLabels; ++j) {
    //     galois::gDebug("Vertex index ", i, " Label ",
    //     edgeIndexToLabelMap.at(j), " Out edge offset ", edgeIndDataLabeled[i
    //     * numEdgeLabels + j], "\n");
    //   }
    // }
  }

  void constructInEdgeIndDataLabeled() {
    size_t size = BaseGraph::size() * numEdgeLabels;
    if (UseNumaAlloc) {
      inEdgeIndDataLabeled.allocateBlocked(size);
    } else {
      inEdgeIndDataLabeled.allocateInterleaved(size);
    }

    galois::do_all(
        galois::iterate((size_t)0, this->size()),
        [&](GraphNode N) {
          auto offset    = N * this->numEdgeLabels;
          uint32_t index = 0;
          for (auto e : in_edges(N)) {
            auto& data = this->getInEdgeData(e);
            while (data != edgeIndexToLabelMap[index]) {
              this->inEdgeIndDataLabeled[offset + index] = *e;
              index++;
              assert(index < this->numEdgeLabels);
            }
          }
          auto e = in_edge_end(N);
          while (index < this->numEdgeLabels) {
            this->inEdgeIndDataLabeled[offset + index] = *e;
            index++;
          }
        },
        galois::no_stats(), galois::steal());

    // for (size_t i = 0; i < BaseGraph::size(); ++i) {
    //   for (size_t j = 0; j < numEdgeLabels; ++j) {
    //     galois::gDebug("Vertex index ", i, " Label ",
    //     edgeIndexToLabelMap.at(j)," In edge offset ", inEdgeIndDataLabeled[i
    //     * numEdgeLabels + j], "\n");
    //   }
    // }
  }

public:
  //! default constructor
  LC_CSR_Labeled_Graph() = default;
  //! default move constructor
  LC_CSR_Labeled_Graph(LC_CSR_Labeled_Graph&& rhs) = default;
  //! default = operator
  LC_CSR_Labeled_Graph& operator=(LC_CSR_Labeled_Graph&&) = default;

  /////////////////////////////////////////////////////////////////////////////
  // Access functions
  /////////////////////////////////////////////////////////////////////////////

  using BaseGraph::edge_begin;
  using BaseGraph::edge_end;
  using BaseGraph::edges;
  using BaseGraph::raw_begin;
  using BaseGraph::raw_end;
  using DerivedGraph::in_edge_begin;
  using DerivedGraph::in_edge_end;
  using DerivedGraph::in_edges;
  using DerivedGraph::in_raw_begin;
  using DerivedGraph::in_raw_end;

  /**
   * Grabs edge beginning without lock/safety.
   *
   * @param N node to get edge beginning of
   * @param data label to get edge beginning of
   * @returns Iterator to first edge of node N
   */
  edge_iterator raw_begin(GraphNode N, const EdgeTy& data) const {
    auto index = (N * numEdgeLabels) + edgeLabelToIndexMap.at(data);
    return edge_iterator((index == 0) ? 0 : edgeIndDataLabeled[index - 1]);
  }

  /**
   * Grabs edge end without lock/safety.
   *
   * @param N node to get edge end of
   * @param data label to get edge end of
   * @returns Iterator to end of edges of node N (i.e. first edge of
   * node N+1)
   */
  edge_iterator raw_end(GraphNode N, const EdgeTy& data) const {
    auto index = (N * numEdgeLabels) + edgeLabelToIndexMap.at(data);
    return edge_iterator(edgeIndDataLabeled[index]);
  }

  /**
   * Wrapper to get the edge end of a node; lock if necessary.
   *
   * @param N node to get edge beginning of
   * @param data label to get edge beginning of
   * @param mflag how safe the acquire should be
   * @returns Iterator to first edge of node N
   */
  edge_iterator edge_begin(GraphNode N, const EdgeTy& data,
                           MethodFlag mflag = MethodFlag::WRITE) {
    BaseGraph::acquireNode(N, mflag);
    if (!HasNoLockable && galois::runtime::shouldLock(mflag)) {
      for (edge_iterator ii = raw_begin(N, data), ee = raw_end(N, data);
           ii != ee; ++ii) {
        BaseGraph::acquireNode(BaseGraph::edgeDst[*ii], mflag);
      }
    }
    return raw_begin(N, data);
  }

  /**
   * Wrapper to get the edge end of a node; lock if necessary.
   *
   * @param N node to get edge end of
   * @param data label to get edge end of
   * @param mflag how safe the acquire should be
   * @returns Iterator to end of edges of node N (i.e. first edge of N+1)
   */
  edge_iterator edge_end(GraphNode N, const EdgeTy& data,
                         MethodFlag mflag = MethodFlag::WRITE) {
    BaseGraph::acquireNode(N, mflag);
    return raw_end(N, data);
  }

  /**
   * @param N node to get edges for
   * @param data label to get edges of
   * @param mflag how safe the acquire should be
   * @returns Range to edges of node N
   */
  runtime::iterable<NoDerefIterator<edge_iterator>>
  edges(GraphNode N, const EdgeTy& data, MethodFlag mflag = MethodFlag::WRITE) {
    return internal::make_no_deref_range(edge_begin(N, data, mflag),
                                         edge_end(N, data, mflag));
  }

  /**
   * @param N node to get degree for
   * @param data label to get degree of
   * @param mflag how safe the acquire should be
   * @returns Degree of node N
   */
  auto degree(GraphNode N, const EdgeTy& data) const {
    return std::distance(raw_begin(N, data), raw_end(N, data));
  }

  /**
   * @param N node to get degree for
   * @param mflag how safe the acquire should be
   * @returns Degree of node N
   */
  auto degree(GraphNode N) const {
    return std::distance(raw_begin(N), raw_end(N));
  }

  /**
   * Grabs in edge beginning without lock/safety.
   *
   * @param N node to get edge beginning of
   * @param data label to get edge beginning of
   * @returns Iterator to first in edge of node N
   */
  edge_iterator in_raw_begin(GraphNode N, const EdgeTy& data) const {
    auto index = (N * numEdgeLabels) + edgeLabelToIndexMap.at(data);
    return edge_iterator((index == 0) ? 0 : inEdgeIndDataLabeled[index - 1]);
  }

  /**
   * Grabs in edge end without lock/safety.
   *
   * @param N node to get edge end of
   * @param data label to get edge end of
   * @returns Iterator to end of in edges of node N (i.e. first edge of
   * node N+1)
   */
  edge_iterator in_raw_end(GraphNode N, const EdgeTy& data) const {
    auto index = (N * numEdgeLabels) + edgeLabelToIndexMap.at(data);
    return edge_iterator(inEdgeIndDataLabeled[index]);
  }

  /**
   * Wrapper to get the in edge end of a node; lock if necessary.
   *
   * @param N node to get edge beginning of
   * @param data label to get edge beginning of
   * @param mflag how safe the acquire should be
   * @returns Iterator to first in edge of node N
   */
  edge_iterator in_edge_begin(GraphNode N, const EdgeTy& data,
                              MethodFlag mflag = MethodFlag::WRITE) {
    BaseGraph::acquireNode(N, mflag);
    if (!HasNoLockable && galois::runtime::shouldLock(mflag)) {
      for (edge_iterator ii = in_raw_begin(N, data), ee = in_raw_end(N, data);
           ii != ee; ++ii) {
        BaseGraph::acquireNode(DerivedGraph::inEdgeDst[*ii], mflag);
      }
    }
    return in_raw_begin(N, data);
  }

  /**
   * Wrapper to get the in edge end of a node; lock if necessary.
   *
   * @param N node to get in edge end of
   * @param data label to get edge end of
   * @param mflag how safe the acquire should be
   * @returns Iterator to end of in edges of node N (i.e. first in edge of N+1)
   */
  edge_iterator in_edge_end(GraphNode N, const EdgeTy& data,
                            MethodFlag mflag = MethodFlag::WRITE) {
    BaseGraph::acquireNode(N, mflag);
    return in_raw_end(N, data);
  }

  /**
   * @param N node to get in edges for
   * @param data label to get edges of
   * @param mflag how safe the acquire should be
   * @returns Range to in edges of node N
   */
  runtime::iterable<NoDerefIterator<edge_iterator>>
  in_edges(GraphNode N, const EdgeTy& data,
           MethodFlag mflag = MethodFlag::WRITE) {
    return internal::make_no_deref_range(in_edge_begin(N, data, mflag),
                                         in_edge_end(N, data, mflag));
  }

  /**
   * @param N node to get in-degree for
   * @param data label to get in-degree of
   * @param mflag how safe the acquire should be
   * @returns In-degree of node N
   */
  auto in_degree(GraphNode N, const EdgeTy& data) const {
    return std::distance(in_raw_begin(N, data), in_raw_end(N, data));
  }

  /**
   * @param N node to get in-degree for
   * @param mflag how safe the acquire should be
   * @returns In-degree of node N
   */
  auto in_degree(GraphNode N) const {
    return std::distance(in_raw_begin(N), in_raw_end(N));
  }

  data_iterator data_begin() const { return edgeIndexToLabelMap.cbegin(); }

  data_iterator data_end() const { return edgeIndexToLabelMap.cend(); }

  runtime::iterable<NoDerefIterator<data_iterator>> data_range() const {
    return internal::make_no_deref_range(data_begin(), data_end());
  }

  /////////////////////////////////////////////////////////////////////////////
  // Utility
  /////////////////////////////////////////////////////////////////////////////

  /**
   * Sorts outgoing edges of a node. Comparison is over
   * getEdgeData(e) and then getEdgeDst(e).
   */
  void sortEdgesByDataThenDst(GraphNode N,
                              MethodFlag mflag = MethodFlag::WRITE) {
    BaseGraph::acquireNode(N, mflag);
    typedef EdgeSortValue<GraphNode, EdgeTy> EdgeSortVal;
    std::sort(BaseGraph::edge_sort_begin(N), BaseGraph::edge_sort_end(N),
              [=](const EdgeSortVal& e1, const EdgeSortVal& e2) {
                auto& data1 = e1.get();
                auto& data2 = e2.get();
                if (data1 < data2)
                  return true;
                else if (data1 > data2)
                  return false;
                else
                  return e1.dst < e2.dst;
              });
  }

  /**
   * Sorts all outgoing edges of all nodes in parallel. Comparison is over
   * getEdgeData(e) and then getEdgeDst(e).
   */
  void sortAllEdgesByDataThenDst(MethodFlag mflag = MethodFlag::WRITE) {
    galois::do_all(
        galois::iterate((size_t)0, this->size()),
        [=](GraphNode N) { this->sortEdgesByDataThenDst(N, mflag); },
        galois::no_stats(), galois::steal());
  }

  /**
   * Sorts outgoing edges of a node. Comparison is over
   * getEdgeData(e) and then getEdgeDst(e).
   */
  void sortInEdgesByDataThenDst(GraphNode N,
                                MethodFlag mflag = MethodFlag::WRITE) {
    BaseGraph::acquireNode(N, mflag);
    // depending on value/ref the type of EdgeSortValue changes
    using EdgeSortVal = EdgeSortValue<
        GraphNode,
        typename std::conditional<EdgeDataByValue, EdgeTy, uint64_t>::type>;

    std::sort(DerivedGraph::in_edge_sort_begin(N),
              DerivedGraph::in_edge_sort_end(N),
              [=](const EdgeSortVal& e1, const EdgeSortVal& e2) {
                auto data1 =
                    EdgeDataByValue ? e1.get() : BaseGraph::edgeData[e1.get()];
                auto data2 =
                    EdgeDataByValue ? e2.get() : BaseGraph::edgeData[e2.get()];
                if (data1 < data2)
                  return true;
                else if (data1 > data2)
                  return false;
                else
                  return e1.dst < e2.dst;
              });
  }

  /**
   * Sorts all incoming edges of all nodes in parallel. Comparison is over
   * getEdgeData(e) and then getEdgeDst(e).
   */
  void sortAllInEdgesByDataThenDst(MethodFlag mflag = MethodFlag::WRITE) {
    galois::do_all(
        galois::iterate((size_t)0, this->size()),
        [=](GraphNode N) { this->sortInEdgesByDataThenDst(N, mflag); },
        galois::no_stats(), galois::steal());
  }

  void constructAndSortIndex() {
    // sort outgoing edges
    sortAllEdgesByDataThenDst();

    // construct incoming edges
    // must occur after sorting outgoing edges if !EdgeDataByValue
    DerivedGraph::constructIncomingEdges();
    // sort incoming edges
    sortAllInEdgesByDataThenDst();

    // galois::gDebug("outgoing edges");
    // for (unsigned i = 0; i < BaseGraph::size(); i++) {
    //  for (auto j = BaseGraph::edge_begin(i);
    //       j != BaseGraph::edge_end(i);
    //       j++) {
    //    galois::gDebug(i, " ", BaseGraph::getEdgeDst(j), " ",
    //                  BaseGraph::getEdgeData(j));
    //  }
    // }

    // galois::gDebug("incoming edges");
    // for (unsigned i = 0; i < BaseGraph::size(); i++) {
    //  for (auto j = DerivedGraph::in_edge_begin(i);
    //       j != DerivedGraph::in_edge_end(i);
    //       j++) {
    //    galois::gDebug(i, " ", DerivedGraph::getInEdgeDst(j), " ",
    //                  DerivedGraph::getInEdgeData(j));
    //  }
    // }

    constructEdgeLabelIndex();
    constructEdgeIndDataLabeled();
    constructInEdgeIndDataLabeled();
  }
};

} // namespace graphs
} // namespace galois
