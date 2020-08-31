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
    : public LC_CSR_Graph<NodeTy, EdgeTy, HasNoLockable, UseNumaAlloc,
                          HasOutOfLineLockable, FileEdgeTy> {
  // typedef to make it easier to read
  //! Typedef referring to base LC_CSR_Graph
  using BaseGraph = LC_CSR_Graph<NodeTy, EdgeTy, HasNoLockable, UseNumaAlloc,
                                 HasOutOfLineLockable, FileEdgeTy>;
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
  galois::gstl::Vector<uint32_t> degrees; // TODO: change these to LargeArray

  void constructEdgeLabelIndex() {
    galois::substrate::PerThreadStorage<std::set<EdgeTy>> edgeLabels;
    galois::do_all(
        galois::iterate(size_t{0}, this->size()),
        [&](GraphNode N) {
          for (auto e : BaseGraph::edges(N)) {
            auto& data = this->getEdgeData(e);
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
        galois::iterate(size_t{0}, this->size()),
        [&](GraphNode N) {
          auto offset    = N * this->numEdgeLabels;
          uint32_t index = 0;
          for (auto e : BaseGraph::edges(N)) {
            auto& data = this->getEdgeData(e);
            while (data != edgeIndexToLabelMap[index]) {
              this->edgeIndDataLabeled[offset + index] = *e;
              index++;
              assert(index < this->numEdgeLabels);
            }
          }
          auto e = BaseGraph::edge_end(N);
          while (index < this->numEdgeLabels) {
            this->edgeIndDataLabeled[offset + index] = *e;
            index++;
          }
        },
        galois::no_stats(), galois::steal());
  }

public:
  using node_data_const_reference =
      typename BaseGraph::NodeInfoTypes::const_reference;

  /////////////////////////////////////////////////////////////////////////////
  // Access functions
  /////////////////////////////////////////////////////////////////////////////

  node_data_const_reference getData(GraphNode N) const {
    return this->nodeData[N].getData();
  }

  typename BaseGraph::node_data_reference getData(GraphNode N) {
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
   * @returns Range to edges of node N
   */
  edges_iterator edges(GraphNode N, const EdgeTy& data) const {
    return internal::make_no_deref_range(raw_begin(N, data), raw_end(N, data));
  }

  /**
   * @param N node to get degree for
   * @returns Degree of node N
   */
  size_t getDegree(GraphNode N) const {
    return std::distance(BaseGraph::raw_begin(N), BaseGraph::raw_end(N));
  }

  /**
   * @param N node to get degree for
   * @param data label to get degree of
   * @returns Degree of node N
   */
  size_t getDegree(GraphNode N, const EdgeTy& data) const {
    return std::distance(raw_begin(N, data), raw_end(N, data));
  }

  /**
   * Wrapper to get the begin iterator to distinct edge labels in the graph.
   *
   * @returns Iterator to first distinct edge label
   */
  data_iterator distinctEdgeLabelsBegin() const {
    return edgeIndexToLabelMap.cbegin();
  }

  /**
   * Wrapper to get the end iterator to distinct edge labels in the graph.
   *
   * @returns Iterator to end distinct edge label
   */
  data_iterator distinctEdgeLabelsEnd() const {
    return edgeIndexToLabelMap.cend();
  }

  /**
   * Wrapper to get the distinct edge labels in the graph.
   *
   * @returns Range of the distinct edge labels
   */
  auto distinctEdgeLabels() const {
    return internal::make_no_deref_range(distinctEdgeLabelsBegin(),
                                         distinctEdgeLabelsEnd());
  }

  /**
   * @param data label to check
   * @returns true iff there exists some edge in the graph with that label
   */
  bool doesEdgeLabelExist(const EdgeTy& data) const {
    return (edgeLabelToIndexMap.find(data) != edgeLabelToIndexMap.end());
  }

  /////////////////////////////////////////////////////////////////////////////
  // Utility
  /////////////////////////////////////////////////////////////////////////////

protected:
  /**
   * Check if a vertex is present between in the edge destinations list
   *
   * @param key vertex to search for
   * @param begin start of edge list iterator
   * @param end end of edge list iterator
   * @returns true iff the key exists
   */
  std::optional<edge_iterator> binarySearch(GraphNode key, edge_iterator begin,
                                            edge_iterator end) const {
    edge_iterator l = begin;
    edge_iterator r = end - 1;
    while (r >= l) {
      edge_iterator mid = l + (r - l) / 2;
      GraphNode value   = BaseGraph::getEdgeDst(mid);
      if (value == key) {
        return mid;
      }
      if (value < key)
        l = mid + 1;
      else
        r = mid - 1;
    }
    return std::nullopt;
  }

public:
  /**
   * Check if vertex src is connected to vertex dst with the given edge data
   *
   * @param src source node of the edge
   * @param dst destination node of the edge
   * @param data label of the edge
   * @returns true iff the edge exists
   */
  bool isConnectedWithEdgeLabel(GraphNode src, GraphNode dst,
                                const EdgeTy& data) const {
    // trivial check; can't be connected if degree is 0
    if (degrees[src] == 0) {
      return false;
    }
    unsigned key    = dst;
    unsigned search = src;
    return binarySearch(key, raw_begin(search, data), raw_end(search, data))
        .has_value();
  }

  /**
   * Check if vertex src is connected to vertex dst with any edge data
   *
   * @param src source node of the edge
   * @param dst destination node of the edge
   * @returns true iff the edge exists
   */
  bool isConnected(GraphNode src, GraphNode dst) const {
    // trivial check; can't be connected if degree is 0
    if (degrees[src] == 0) {
      return false;
    }
    for (auto data : distinctEdgeLabels()) {
      if (isConnectedWithEdgeLabel(src, dst, data)) {
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
  void sortVectorByDataThenDst(std::vector<uint64_t>& vector_to_sort) {
    galois::do_all(
        galois::iterate((size_t)0, this->size()),
        [&](size_t node_id) {
          // get this node's first and last edge
          uint32_t first_edge = *(BaseGraph::edge_begin(node_id));
          uint32_t last_edge  = *(BaseGraph::edge_end(node_id));
          // get iterators to locations to sort in the vector
          auto begin_sort_iterator = vector_to_sort.begin() + first_edge;
          auto end_sort_iterator   = vector_to_sort.begin() + last_edge;

          // rearrange vector indices based on how the destinations of this
          // graph will eventually be sorted sort function not based on vector
          // being passed, but rather the data and destination of the graph
          std::sort(begin_sort_iterator, end_sort_iterator,
                    [&](const uint64_t e1, const uint64_t e2) {
                      // get edge data and destinations
                      EdgeTy data1 = this->getEdgeData(e1);
                      EdgeTy data2 = this->getEdgeData(e2);
                      if (data1 < data2) {
                        return true;
                      } else if (data1 > data2) {
                        return false;
                      } else {
                        uint32_t dst1 = this->getEdgeDst(e1);
                        uint32_t dst2 = this->getEdgeDst(e2);
                        return dst1 < dst2;
                      }
                    });
        },
        galois::steal(), galois::no_stats(),
        galois::loopname("SortVectorByDataThenDst"));
  }

  /**
   * Returns an edge iterator to an edge with some source and destination by
   * searching for the destination via the source vertex's edges.
   * If not found, returns nothing.
   */
  std::optional<edge_iterator> findEdge(GraphNode src, GraphNode dst) const {
    // trivial check; can't be connected if degree is 0
    if (degrees[src] == 0) {
      return std::nullopt;
    }

    // loop through all data labels
    for (const data_iterator data : distinctEdgeLabels()) {
      // always use out edges (we want an id to the out edge returned)
      std::optional<edge_iterator> r =
          binarySearch(dst, raw_begin(src, *data), raw_end(src, *data));

      // return if something was found
      if (r) {
        return r;
      }
    }

    // not found, return empty optional
    return std::nullopt;
  }

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

  void constructAndSortIndex() {
    // sort outgoing edges
    sortAllEdgesByDataThenDst();

    constructEdgeLabelIndex();
    constructEdgeIndDataLabeled();

    degrees = BaseGraph::countDegrees();
    // TODO(roshan) calculate inDegrees using Gluon
  }
};

} // namespace graphs
} // namespace galois
