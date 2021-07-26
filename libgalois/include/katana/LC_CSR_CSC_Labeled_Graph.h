#pragma once

#include "katana/LC_CSR_CSC_Graph.h"

namespace katana {

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
template <
    typename NodeTy, typename EdgeTy, bool EdgeDataByValue = false,
    bool HasNoLockable = false, bool UseNumaAlloc = false,
    bool HasOutOfLineLockable = false, typename FileEdgeTy = EdgeTy>
class LC_CSR_CSC_Labeled_Graph
    : public LC_CSR_CSC_Graph<
          NodeTy, EdgeTy, EdgeDataByValue, HasNoLockable, UseNumaAlloc,
          HasOutOfLineLockable, FileEdgeTy> {
  //! Typedef referring to the base LC_CSR_CSC_Graph class
  using BaseGraph = LC_CSR_CSC_Graph<
      NodeTy, EdgeTy, EdgeDataByValue, HasNoLockable, UseNumaAlloc,
      HasOutOfLineLockable, FileEdgeTy>;

public:
  //! Graph node typedef
  using GraphNode = uint32_t;

protected:
  // retypedefs of base class
  //! large array for edge data
  using EdgeData = NUMAArray<EdgeTy>;
  //! large array for edge destinations
  using EdgeDst = NUMAArray<uint32_t>;
  //! large array for edge index data
  using EdgeIndData = NUMAArray<uint64_t>;

public:
  //! iterator for edges
  using edge_iterator =
      boost::counting_iterator<typename EdgeIndData::value_type>;
  //! Edges as a range
  using edges_iterator = StandardRange<NoDerefIterator<edge_iterator>>;
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

  //! out degrees of the data graph
  NUMAArray<uint32_t> degrees_;
  //! in degrees of the data graph
  NUMAArray<uint32_t> in_degrees_;

public:
  using node_data_const_reference =
      typename BaseGraph::NodeInfoTypes::const_reference;

  /////////////////////////////////////////////////////////////////////////////
  // Access functions
  /////////////////////////////////////////////////////////////////////////////

  node_data_const_reference GetData(GraphNode N) const {
    return this->nodeData[N].getData();
  }

  typename BaseGraph::node_data_reference GetData(GraphNode N) {
    return this->nodeData[N].getData();
  }

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
  edge_iterator edge_begin(
      GraphNode N, const EdgeTy& data, MethodFlag mflag = MethodFlag::WRITE) {
    this->acquireNode(N, mflag);
    if (!HasNoLockable && katana::shouldLock(mflag)) {
      for (edge_iterator ii = raw_begin(N, data), ee = raw_end(N, data);
           ii != ee; ++ii) {
        this->acquireNode(this->edgeDst[*ii], mflag);
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
  edge_iterator edge_end(
      GraphNode N, const EdgeTy& data, MethodFlag mflag = MethodFlag::WRITE) {
    this->acquireNode(N, mflag);
    return raw_end(N, data);
  }

  /**
   * @param N node to get edges for
   * @param data label to get edges of
   * @returns Range to edges of node N
   */
  edges_iterator edges(GraphNode N, const EdgeTy& data) const {
    return internal::make_no_deref_range(raw_begin(N, data), raw_end(N, data));
  }

  /**
   * @param N node to get degree for
   * @returns Degree of node N
   */
  size_t GetDegree(GraphNode N) const {
    return std::distance(BaseGraph::raw_begin(N), BaseGraph::raw_end(N));
  }

  /**
   * @param N node to get degree for
   * @param data label to get degree of
   * @returns Degree of node N
   */
  size_t GetDegree(GraphNode N, const EdgeTy& data) const {
    return std::distance(raw_begin(N, data), raw_end(N, data));
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
  edge_iterator in_edge_begin(
      GraphNode N, const EdgeTy& data, MethodFlag mflag = MethodFlag::WRITE) {
    this->acquireNode(N, mflag);
    if (!HasNoLockable && katana::shouldLock(mflag)) {
      for (edge_iterator ii = in_raw_begin(N, data), ee = in_raw_end(N, data);
           ii != ee; ++ii) {
        this->acquireNode(this->inEdgeDst[*ii], mflag);
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
  edge_iterator in_edge_end(
      GraphNode N, const EdgeTy& data, MethodFlag mflag = MethodFlag::WRITE) {
    this->acquireNode(N, mflag);
    return in_raw_end(N, data);
  }

  /**
   * @param N node to get in edges for
   * @param data label to get edges of
   * @returns Range to in edges of node N
   */
  edges_iterator in_edges(GraphNode N, const EdgeTy& data) const {
    return internal::make_no_deref_range(
        in_raw_begin(N, data), in_raw_end(N, data));
  }

  /**
   * @param N node to get in-degree for
   * @returns In-degree of node N
   */
  size_t GetInDegree(GraphNode N) const {
    return std::distance(BaseGraph::in_raw_begin(N), BaseGraph::in_raw_end(N));
  }

  /**
   * @param N node to get in-degree for
   * @param data label to get in-degree of
   * @returns In-degree of node N
   */
  size_t GetInDegree(GraphNode N, const EdgeTy& data) const {
    return std::distance(in_raw_begin(N, data), in_raw_end(N, data));
  }

  /**
   * Wrapper to get the begin iterator to distinct edge labels in the graph.
   *
   * @returns Iterator to first distinct edge label
   */
  data_iterator DistinctEdgeLabelsBegin() const {
    return edgeIndexToLabelMap.cbegin();
  }

  /**
   * Wrapper to get the end iterator to distinct edge labels in the graph.
   *
   * @returns Iterator to end distinct edge label
   */
  data_iterator DistinctEdgeLabelsEnd() const {
    return edgeIndexToLabelMap.cend();
  }

  /**
   * Wrapper to get the distinct edge labels in the graph.
   *
   * @returns Range of the distinct edge labels
   */
  auto DistinctEdgeLabels() const {
    return MakeStandardRange(
        DistinctEdgeLabelsBegin(), DistinctEdgeLabelsEnd());
  }

  /**
   * @param data label to check
   * @returns true iff there exists some edge in the graph with that label
   */
  bool DoesEdgeLabelExist(const EdgeTy& data) const {
    return (edgeLabelToIndexMap.find(data) != edgeLabelToIndexMap.end());
  }

  template <bool in_edges>
  class EdgeIteratorComparator {
  public:
    using Graph = LC_CSR_CSC_Graph<
        NodeTy, EdgeTy, EdgeDataByValue, HasNoLockable, UseNumaAlloc,
        HasOutOfLineLockable, FileEdgeTy>;
    const Graph& graph_;

    EdgeIteratorComparator(const Graph& graph) : graph_(graph) {}

    bool operator()(edge_iterator a, GraphNode key) {
      auto value = in_edges ? graph_.getInEdgeDst(a) : graph_.getEdgeDst(a);
      return value < key;
    }

    bool operator()(GraphNode key, edge_iterator a) {
      auto value = in_edges ? graph_.getInEdgeDst(a) : graph_.getEdgeDst(a);
      return key < value;
    }
  };

  /**
   * Returns all edges from src to dst with some label.  If not found, returns
   * nothing.
   */
  std::optional<edges_iterator> FindAllEdgesWithLabel(
      GraphNode node, GraphNode key, const EdgeTy& data) const {
    // trivial check; can't be connected if degree is 0
    if (degrees_[node] == 0 || in_degrees_[key] == 0) {
      return std::nullopt;
    }

    edge_iterator begin = raw_begin(node, data);
    edge_iterator end = raw_end(node, data);
    auto range = internal::make_no_deref_range(begin, end);
    EdgeIteratorComparator<false> comp(*this);
    auto [first_it, last_it] =
        std::equal_range(range.begin(), range.end(), key, comp);
    if (*first_it == end || this->getEdgeDst(*first_it) != key) {
      return std::nullopt;
    }
    KATANA_LOG_DEBUG_ASSERT(this->getEdgeDst(*last_it - 1) == key);
    return internal::make_no_deref_range(*first_it, *last_it);
  }

  template <bool in_edges>
  bool IsConnectedWithEdgeLabelDirected(
      GraphNode node, GraphNode key, const EdgeTy& data) const {
    // trivial check; can't be connected if degree is 0
    if (in_edges) {
      if (in_degrees_[node] == 0 || degrees_[key] == 0) {
        return false;
      }
    } else if (degrees_[node] == 0 || in_degrees_[key] == 0) {
      return false;
    }

    edge_iterator begin =
        in_edges ? in_raw_begin(node, data) : raw_begin(node, data);
    edge_iterator end = in_edges ? in_raw_end(node, data) : raw_end(node, data);
    auto range = internal::make_no_deref_range(begin, end);

    EdgeIteratorComparator<in_edges> comp(*this);
    return std::binary_search(range.begin(), range.end(), key, comp);
  }

  /**
   * Check if vertex src is connected to vertex dst with the given edge data.
   * Note that this method assumes edge mirrors are present and will check both
   * in and out edges.
   *
   * @param src source node of the edge
   * @param dst destination node of the edge
   * @param data label of the edge
   * @returns true iff the edge exists
   */
  bool IsConnectedWithEdgeLabel(
      GraphNode src, GraphNode dst, const EdgeTy& data) const {
    if (degrees_[src] < in_degrees_[dst]) {
      return IsConnectedWithEdgeLabelDirected<false>(src, dst, data);
    }
    return IsConnectedWithEdgeLabelDirected<true>(dst, src, data);
  }

  /**
   * Check if vertex src is connected to vertex dst with any edge data. Note
   * that this method assumes edge mirrors are present and will check both in
   * and out edges.
   *
   * @param src source node of the edge
   * @param dst destination node of the edge
   * @returns true iff the edge exists
   */
  bool IsConnected(GraphNode src, GraphNode dst) const {
    // trivial check; can't be connected if degree is 0
    if (degrees_[src] == 0 || in_degrees_[dst] == 0) {
      return false;
    }
    for (auto data : DistinctEdgeLabels()) {
      if (IsConnectedWithEdgeLabel(src, dst, data)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Given some vector, sort the indices of that vector as if they were
   * the edge destinations that would get sorted if one sorted by the
   * edge data then destinations. Used mainly to rearrange other vectors
   * that also need to be sorted besides destinations/data  (since the
   * current infrastructure only supports sorting those 2 arrays at
   * the moment).
   */
  void SortVectorByDataThenDst(katana::NUMAArray<uint64_t>& vector_to_sort) {
    katana::do_all(
        katana::iterate(uint64_t{0}, this->size()),
        [&](size_t node_id) {
          // get this node's first and last edge
          uint32_t first_edge = *(BaseGraph::edge_begin(node_id));
          uint32_t last_edge = *(BaseGraph::edge_end(node_id));
          // get iterators to locations to sort in the vector
          auto begin_sort_iterator = vector_to_sort.begin() + first_edge;
          auto end_sort_iterator = vector_to_sort.begin() + last_edge;

          // rearrange vector indices based on how the destinations of this
          // graph will eventually be sorted sort function not based on vector
          // being passed, but rather the data and destination of the graph
          std::sort(
              begin_sort_iterator, end_sort_iterator,
              [&](const uint64_t e1, const uint64_t e2) {
                // get edge data and destinations
                EdgeTy data1 = this->getEdgeData(e1);
                EdgeTy data2 = this->getEdgeData(e2);
                if (data1 != data2) {
                  return data1 < data2;
                }

                uint32_t dst1 = this->getEdgeDst(e1);
                uint32_t dst2 = this->getEdgeDst(e2);
                return dst1 < dst2;
              });
        },
        katana::steal(), katana::no_stats(),
        katana::loopname("SortVectorByDataThenDst"));
  }

  void ConstructAndSortIndex() {
    // sort outgoing edges
    SortAllEdgesByDataThenDst();

    // construct incoming edges
    // must occur after sorting outgoing edges if !EdgeDataByValue
    this->constructIncomingEdges();

    // sort incoming edges
    SortAllInEdgesByDataThenDst();

    ConstructEdgeLabelIndex();
    ConstructEdgeIndDataLabeled();
    ConstructInEdgeIndDataLabeled();

    degrees_ = this->countDegrees();
    in_degrees_ = this->countInDegrees();
  }

private:
  void ConstructEdgeLabelIndex() {
    katana::PerThreadStorage<std::set<EdgeTy>> edgeLabels;
    katana::do_all(
        katana::iterate(uint64_t{0}, this->size()),
        [&](GraphNode N) {
          for (auto e : BaseGraph::edges(N)) {
            const uint64_t& data = this->getEdgeData(e);
            edgeLabels.getLocal()->insert(data);
          }
          for (auto e : BaseGraph::in_edges(N)) {
            const uint64_t& data = this->getInEdgeData(e);
            edgeLabels.getLocal()->insert(data);
          }
        },
        katana::no_stats(), katana::steal());

    // ordered map
    std::map<EdgeTy, uint32_t> sortedMap;
    for (uint32_t i = 0; i < katana::activeThreads; ++i) {
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

  void ConstructEdgeIndDataLabeled() {
    size_t size = this->size() * numEdgeLabels;
    if (UseNumaAlloc) {
      edgeIndDataLabeled.allocateBlocked(size);
    } else {
      edgeIndDataLabeled.allocateInterleaved(size);
    }

    katana::do_all(
        katana::iterate(uint64_t{0}, this->size()),
        [&](GraphNode N) {
          auto offset = N * this->numEdgeLabels;
          uint32_t index = 0;
          for (auto e : BaseGraph::edges(N)) {
            const uint64_t& data = this->getEdgeData(e);
            while (data != edgeIndexToLabelMap[index]) {
              this->edgeIndDataLabeled[offset + index] = *e;
              index++;
              KATANA_LOG_DEBUG_ASSERT(index < this->numEdgeLabels);
            }
          }
          auto e = BaseGraph::edge_end(N);
          while (index < this->numEdgeLabels) {
            this->edgeIndDataLabeled[offset + index] = *e;
            index++;
          }
        },
        katana::no_stats(), katana::steal());
  }

  void ConstructInEdgeIndDataLabeled() {
    size_t size = this->size() * numEdgeLabels;
    if (UseNumaAlloc) {
      inEdgeIndDataLabeled.allocateBlocked(size);
    } else {
      inEdgeIndDataLabeled.allocateInterleaved(size);
    }

    katana::do_all(
        katana::iterate(uint64_t{0}, this->size()),
        [&](GraphNode N) {
          auto offset = N * this->numEdgeLabels;
          uint32_t index = 0;
          for (auto e : BaseGraph::in_edges(N)) {
            const uint64_t& data = this->getInEdgeData(e);
            while (data != edgeIndexToLabelMap[index]) {
              this->inEdgeIndDataLabeled[offset + index] = *e;
              index++;
              KATANA_LOG_DEBUG_ASSERT(index < this->numEdgeLabels);
            }
          }
          auto e = BaseGraph::in_edge_end(N);
          while (index < this->numEdgeLabels) {
            this->inEdgeIndDataLabeled[offset + index] = *e;
            index++;
          }
        },
        katana::no_stats(), katana::steal());
  }

  /**
   * Sorts outgoing edges of a node. Comparison is over
   * getEdgeData(e) and then getEdgeDst(e).
   */
  void SortEdgesByDataThenDst(
      GraphNode N, MethodFlag mflag = MethodFlag::WRITE) {
    this->acquireNode(N, mflag);
    typedef EdgeSortValue<GraphNode, EdgeTy> EdgeSortVal;
    std::sort(
        this->edge_sort_begin(N), this->edge_sort_end(N),
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
  void SortAllEdgesByDataThenDst(MethodFlag mflag = MethodFlag::WRITE) {
    katana::do_all(
        katana::iterate(uint64_t{0}, this->size()),
        [=](GraphNode N) { this->SortEdgesByDataThenDst(N, mflag); },
        katana::no_stats(), katana::steal());
  }

  /**
   * Sorts outgoing edges of a node. Comparison is over
   * getEdgeData(e) and then getEdgeDst(e).
   */
  void SortInEdgesByDataThenDst(
      GraphNode N, MethodFlag mflag = MethodFlag::WRITE) {
    this->acquireNode(N, mflag);
    // depending on value/ref the type of EdgeSortValue changes
    using EdgeSortVal = EdgeSortValue<
        GraphNode,
        typename std::conditional<EdgeDataByValue, EdgeTy, uint64_t>::type>;

    std::sort(
        this->in_edge_sort_begin(N), this->in_edge_sort_end(N),
        [=](const EdgeSortVal& e1, const EdgeSortVal& e2) {
          auto data1 = EdgeDataByValue ? e1.get() : this->edgeData[e1.get()];
          auto data2 = EdgeDataByValue ? e2.get() : this->edgeData[e2.get()];
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
  void SortAllInEdgesByDataThenDst(MethodFlag mflag = MethodFlag::WRITE) {
    katana::do_all(
        katana::iterate(uint64_t{0}, this->size()),
        [=](GraphNode N) { this->SortInEdgesByDataThenDst(N, mflag); },
        katana::no_stats(), katana::steal());
  }
};

}  // namespace katana
