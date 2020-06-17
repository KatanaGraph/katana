#pragma once

#include "pangolin/types.h"
#include "pangolin/base_embedding.h"
#include "pangolin/embedding_queue.h"
#include "galois/graphs/QueryGraph.h"
#include "querying/GraphSimulation.h"
#include <algorithm>

#define QUERY_CHUNK_SIZE 256

//! Ordering class: true if left vertex has higher degree than right vertex
//! what this ends up doing is putting high degree vertices first
class OrderVertices {
  QueryGraph& graph;

  uint32_t totalDegree(VertexId v) {
    uint32_t num_edges =
        std::distance(graph.in_edge_begin(v), graph.in_edge_end(v));
    num_edges += std::distance(graph.edge_begin(v), graph.edge_end(v));
    return num_edges;
  }

public:
  OrderVertices(QueryGraph& g) : graph(g) {}

  bool operator()(VertexId left, const VertexId right) {
    if (totalDegree(left) >= totalDegree(right))
      return true;
    return false;
  }
};

template <bool afterGraphSimulation>
class SubgraphQueryMiner {
  //! graph to do matching on
  QueryGraph& dataGraph;
  //! graph that specifies the pattern we want to match
  QueryGraph& queryGraph;
  //! vector storing order in which to query the query graph
  std::vector<VertexId> matchingOrderToVertexMap;
  //! reverse map of matchingOrderToVertexMap
  std::vector<VertexId> vertexToMatchingOrderMap;
  //! out degrees of the data graph
  galois::gstl::Vector<uint32_t> degrees; // TODO: change these to LargeArray
  //! in degrees of the data graph
  galois::gstl::Vector<uint32_t> inDegrees;
  //! total number of matches found to the query graph
  size_t total_count;

  typedef BaseEmbedding EmbeddingType;
  typedef EmbeddingQueue<EmbeddingType> EmbeddingQueueType;
  //! neighbors are (1) node index that is the neighbor
  //! (2) label of edge connecting it to a node in question
  using NeighborsTy = galois::gstl::Vector<std::pair<unsigned, QueryEdgeData>>;

  //! returns the id'th query vertex
  VertexId get_query_vertex(unsigned id) {
    return matchingOrderToVertexMap[id];
  }

  //! returns true if node is to be dropped from consideration for the
  //! given query node
  inline bool pruneNode(const QueryGNode& queryNodeID, QueryNode& dataNode) {
    if (afterGraphSimulation) {
      return !(dataNode.matched & (1 << queryNodeID));
    } else {
      return !matchNodeLabel(queryGraph.getData(queryNodeID), dataNode);
    }
  }

  template <bool inEdges>
  inline bool directed_binary_search(unsigned key,
                                     QueryGraph::edge_iterator begin,
                                     QueryGraph::edge_iterator end) {
    QueryGraph::edge_iterator l = begin;
    QueryGraph::edge_iterator r = end - 1;
    // TODO does anything bad happen if you subtract 1 from begin? that is
    // what this loop relies on; is begin - 1 < begin?
    while (r >= l) {
      QueryGraph::edge_iterator mid = l + (r - l) / 2;
      unsigned value =
          inEdges ? dataGraph.getInEdgeDst(mid) : dataGraph.getEdgeDst(mid);
      if (value == key) {
        return true;
      }
      if (value < key)
        l = mid + 1;
      else
        r = mid - 1;
    }
    return false;
  }

  //! check if vertex a is connected to vertex b in a directed, labeled graph
  inline bool is_connected_with_label(unsigned a, unsigned b,
                                      const QueryEdgeData& label) {
    // trivial check; can't be connected if degree is 0
    if (degrees[a] == 0 || inDegrees[b] == 0)
      return false;
    unsigned key    = b;
    unsigned search = a;
    if (degrees[a] > inDegrees[b]) {
      key        = a;
      search     = b;
      auto begin = dataGraph.in_edge_begin(search, label);
      auto end   = dataGraph.in_edge_end(search, label);
      return directed_binary_search<true>(key, begin, end);
    }
    auto begin = dataGraph.edge_begin(search, label);
    auto end   = dataGraph.edge_end(search, label);
    return directed_binary_search<false>(key, begin, end);
  }

  /**
   * Determines if a data node is suitable to be added to an existing
   * embedding.
   *
   * \param n Number of nodes already in embedding
   * \param emb Embedding to add to
   * \param index Index into neighbors vector of the query node that is
   * being extended from by this embedding
   * \param dst Candidate for extension (index into the data graph)
   * \param neighbors Neighbors of the new query node that needs to be added
   * to the embedding
   * \param numInNeighbors Number of in-neighbors used to determine which
   * neighbors are in (because they're a prefix of the vector)
   */
  bool toAdd(unsigned n, const EmbeddingType& emb, unsigned index,
             const VertexId dst, const NeighborsTy& neighbors,
             unsigned numInNeighbors) {
    VertexId next_qnode =
        get_query_vertex(n); // using matching order to get query vertex id

    // galois::gDebug(", deg(d) = ", dataGraph.degree(dst), ", deg(q) = ",
    // queryGraph.degree(pos+1));

    // make sure data label matches query label
    if (pruneNode(next_qnode, dataGraph.getData(dst)))
      return false;

    // make sure degree is at least as much as corresponding query vertex
    if (!matchNodeDegree(queryGraph, next_qnode, dataGraph, dst))
      return false;

    // if this vertex already exists in the embedding, not qualified to add
    for (unsigned i = 0; i < n; ++i)
      if (dst == emb.get_vertex(i))
        return false;

    galois::gDebug("Checking connectivity of data vertex: ", dst, "...");

    for (unsigned i = 0; i < neighbors.size(); ++i) {
      // ignore self
      if (i == index)
        continue;

      auto q_order      = neighbors[i].first;
      auto qeData       = neighbors[i].second;
      VertexId d_vertex = emb.get_vertex(q_order);

      // i.e. if this is an in-neighbor
      if (numInNeighbors > i) {
        // check the backward connectivity with previous vertices in the
        // embedding
        galois::gDebug("Checking if ", dst, " is an outgoing neighbor of ",
                       d_vertex, "...");
#ifdef USE_QUERY_GRAPH_WITH_MULTIPLEXING_EDGE_LABELS
        bool connected = false;
        for (auto deData : dataGraph.data_range()) {
          if (matchEdgeLabel(qeData, *deData) &&
              is_connected_with_label(d_vertex, dst, *deData)) {
            connected = true;
            break;
          }
        }
        if (!connected)
          return false;
#else
        if (!is_connected_with_label(d_vertex, dst, qeData)) {
          return false;
        }
#endif
      } else {
        // check the forward connectivity with previous vertices in the
        // embedding
        galois::gDebug("Checking if ", dst, " is an incoming neighbor of ",
                       d_vertex, "...");
#ifdef USE_QUERY_GRAPH_WITH_MULTIPLEXING_EDGE_LABELS
        bool connected = false;
        for (auto deData : dataGraph.data_range()) {
          if (matchEdgeLabel(qeData, *deData) &&
              is_connected_with_label(dst, d_vertex, *deData)) {
            connected = true;
            break;
          }
        }
        if (!connected)
          return false;
#else
        if (!is_connected_with_label(dst, d_vertex, qeData)) {
          return false;
        }
#endif
      }
    }

    galois::gDebug("Extending with vertex ", dst);
    return true;
  }

  /**
   * Adds dst to an existing embedding or determines if no more extension
   * is required (if number of nodes matches query graph size).
   *
   * \param n Number of nodes that already exist in the embedding
   * \param emb Old embedding being extended
   * \param dst New node being added to the embedding
   * \param neighbors Only used if DFS is enabled; replaced  by new neighbors
   * of new query in next DFS step
   * \param numInNeighbors Only used in DFS
   * \param out_queue Next embedding list for next round of search
   */
  template <bool DFS, bool printEmbeddings = false>
  void addEmbedding(unsigned n, const EmbeddingType& emb, const VertexId dst,
                    NeighborsTy& neighbors, unsigned& numInNeighbors,
                    EmbeddingQueueType& out_queue) {
    // generate a new embedding and add it to the next queue
    if (n < queryGraph.size() - 1) {
      EmbeddingType new_emb(emb);
      new_emb.push_back(dst);
      if (DFS) {
        // dfs would keep digging in this embedding
        process_embedding<DFS, printEmbeddings>(new_emb, neighbors,
                                                numInNeighbors, out_queue);
      } else {
        // bfs pushes it to handle later
        out_queue.push_back(new_emb);
      }
    } else {
      // this embedding has matched the entire query graph; increment
      // count
      if (printEmbeddings) {
        EmbeddingType new_emb(emb);
        // note: this queue holds all matching embeddings; use if
        // post-processing or the like is required
        new_emb.push_back(dst);
        galois::gPrint("Found embedding: ", new_emb, "\n");
      }
      total_count += 1; // size = queryGraph.size()
    }
  }

  /**
   * Given the number of vertices already matched from the query graph,
   * get the neighbors of the next query vertex to match that
   * have already been matched and add them to a vector.
   *
   * \param n Number of vertices already matched from the query graph;
   * can be used to derive the next query vertex
   * \param neighbors Vector to add neighbors of the next query vertex
   * to with in-neighbors coming first; these vertices have already been
   * matched
   * \param numInNeighbors Number of in-neighbors of the next
   * query vertex; can be used to determine which neighbors in the
   * vector are in-neighbors since they serve as the prefix of the vector
   */
  void constructNeighbors(unsigned n, NeighborsTy& neighbors,
                          unsigned& numInNeighbors) {
    // get next query vertex
    VertexId next_qnode =
        get_query_vertex(n); // using matching order to get query vertex id
    galois::gDebug("Incoming neighbors of query vertex ", next_qnode, "(level ",
                   n, "):");

    // for each incoming neighbor of the next query vertex in the query graph
    for (auto q_edge : queryGraph.in_edges(next_qnode)) {
      VertexId q_dst = queryGraph.getInEdgeDst(q_edge);
      unsigned q_order =
          vertexToMatchingOrderMap[q_dst]; // using query vertex id to get its
                                           // matching order

      // add the neighbor that is already visited (less than n implies matched
      // before)
      if (q_order < n) {
        auto qeData = queryGraph.getInEdgeData(q_edge);
        neighbors.push_back(std::make_pair(q_order, qeData));
        galois::gDebug(q_order, ",", qeData);
      }
    }
    // track number of inneighbors so you know how large the prefix in
    // the neighbors vector is
    numInNeighbors = neighbors.size();

    // now do the same to outgoing neighbors
    galois::gDebug("Outgoing neighbors of query vertex ", next_qnode, "(level ",
                   n, "):");
    // for each outgoing neighbor of the next query vertex in the query graph
    for (auto q_edge : queryGraph.edges(next_qnode)) {
      VertexId q_dst = queryGraph.getEdgeDst(q_edge);
      unsigned q_order =
          vertexToMatchingOrderMap[q_dst]; // using query vertex id to get its
                                           // matching order

      // add the neighbor that is already visited
      if (q_order < n) {
        auto qeData = queryGraph.getEdgeData(q_edge);
        neighbors.push_back(std::make_pair(q_order, qeData));
        galois::gDebug(q_order, ",", qeData);
      }
    }
    // TODO assertion may not hold if the query node is a disconnected node
    assert(neighbors.size() > 0);
  }

  //! Pick the neighbor with the least number of candidates/edges
  unsigned pickNeighbor(const EmbeddingType& emb, const NeighborsTy& neighbors,
                        const unsigned numInNeighbors) {
    unsigned index;
    if (neighbors.size() < 3) { // TODO: make this configurable
      // if # numbers is low, just pick the first neighbor to save time
      index = 0;
    } else {
      // loop through neighbors, pick one with lowest edges
      index                = neighbors.size();
      size_t numCandidates = dataGraph.size(); // conservative upper limit
      for (unsigned i = 0; i < neighbors.size(); ++i) {
        auto q_order = neighbors[i].first;
        auto qeData  = neighbors[i].second;
        // guaranteed to work because neighbors consists only of already
        // matched edges
        VertexId d_vertex = emb.get_vertex(q_order);
        size_t numEdges;
        if (numInNeighbors > i) {
          numEdges = dataGraph.degree(d_vertex, qeData);
        } else {
          numEdges = dataGraph.in_degree(d_vertex, qeData);
        }
        if (numEdges < numCandidates) {
          numCandidates = numEdges;
          index         = i;
        }
      }
    }
    return index;
  }

  /**
   * Given an embedding, attempt to extend it with another data graph node
   * that matches a query node chosen from its qualifying neighbors.
   *
   * \param emb Embeeding to extend
   * \param neighbors Neighbors of the next query node that needs to be
   * added to the embedding
   * \param numInNeighbors Number of in neighbors in the neighbors
   * vector: used to identify which are the in neighbors as they are a
   * prefix in the vector
   * \param out_queue Embeddings to handle in the next round of search
   */
  template <bool DFS, bool printEmbeddings = false>
  void process_embedding(const EmbeddingType& emb, NeighborsTy& neighbors,
                         unsigned numInNeighbors,
                         EmbeddingQueueType& out_queue) {
    galois::gDebug("Current embedding: ", emb);
    unsigned n = emb.size();

    // why reconstruct it if it's DFS? because each embedding size
    // is different unlike BFS where all embeddings grow equally
    if (DFS) {
      neighbors.clear();
      constructNeighbors(n, neighbors, numInNeighbors);
    }

    // pick the neighbor with the least number of candidates/edges
    // to extend this embedding from
    unsigned index    = pickNeighbor(emb, neighbors, numInNeighbors);
    auto q_order      = neighbors.at(index).first;
    auto qeData       = neighbors.at(index).second;
    VertexId d_vertex = emb.get_vertex(q_order);
    galois::gDebug("Picked data vertex to extend: ", d_vertex);

    if (numInNeighbors > index) { // d_vertex is incoming neighbor
#ifdef USE_QUERY_GRAPH_WITH_MULTIPLEXING_EDGE_LABELS
      for (auto deData : dataGraph.data_range()) {
        if (!matchEdgeLabel(qeData, *deData))
          continue;
#else
      {
        auto deData = &qeData;
#endif
        // each outgoing neighbor of d_vertex is a candidate
        // only loop over edges with the label that we want (deData)
        for (auto d_edge : dataGraph.edges(d_vertex, *deData)) {
          QueryGNode d_dst = dataGraph.getEdgeDst(d_edge);
          galois::gDebug("Checking outgoing neighbor of ", d_vertex, ": ",
                         d_dst, "...");
          // determine if this is to be added to embedding
          if (toAdd(n, emb, index, d_dst, neighbors, numInNeighbors)) {
            addEmbedding<DFS, printEmbeddings>(n, emb, d_dst, neighbors,
                                               numInNeighbors, out_queue);
          }
        }
      }
    } else { // d_vertex is outgoing neighbor
#ifdef USE_QUERY_GRAPH_WITH_MULTIPLEXING_EDGE_LABELS
      for (auto deData : dataGraph.data_range()) {
        if (!matchEdgeLabel(qeData, *deData))
          continue;
#else
      {
        auto deData = &qeData;
#endif
        // each incoming neighbor of d_vertex is a candidate
        for (auto d_edge : dataGraph.in_edges(d_vertex, *deData)) {
          QueryGNode d_dst = dataGraph.getInEdgeDst(d_edge);
          galois::gDebug("Checking incoming neighbor ", d_vertex, ": ", d_dst,
                         "...");
          if (toAdd(n, emb, index, d_dst, neighbors, numInNeighbors)) {
            addEmbedding<DFS, printEmbeddings>(n, emb, d_dst, neighbors,
                                               numInNeighbors, out_queue);
          }
        }
      }
    }
  }

  /**
   * Attempts to extend each existing embedding in the in_queue by a single
   * vertex; new embeddings are added to out_queue.
   */
  template <bool DFS, bool printEmbeddings = false>
  inline void extend_vertex(EmbeddingQueueType& in_queue,
                            EmbeddingQueueType& out_queue) {

    unsigned numInNeighbors;
    NeighborsTy neighbors;
    // if not DFS, then all embeddings share the same neighbors list as they
    // are all the same size
    if (!DFS) {
      // number of vertices already in an embedding; number of already matched
      // vertices
      unsigned numMatched = in_queue.begin()->size();
      // get neighbors of next query node
      constructNeighbors(numMatched, neighbors, numInNeighbors);
    }

    // process each embedding
    galois::do_all(
        galois::iterate(in_queue),
        [&](const EmbeddingType& emb) {
          process_embedding<DFS, printEmbeddings>(emb, neighbors,
                                                  numInNeighbors, out_queue);
        },
        galois::chunk_size<QUERY_CHUNK_SIZE>(), galois::steal(),
        galois::loopname("Extending"));
  }

public:
  SubgraphQueryMiner(QueryGraph& dgraph, QueryGraph& qgraph)
      : dataGraph(dgraph), queryGraph(qgraph), total_count(0) {
    degrees   = dataGraph.countDegrees();
    inDegrees = dataGraph.countInDegrees();
  }

  //! gets matching order of vertices in the query graph
  void init() {
    // assert(queryGraph.size() > 2);
    // set_queryGraph.size()(queryGraph.size());
    // set_num_patterns(1);
    matchingOrderToVertexMap.resize(queryGraph.size());
    vertexToMatchingOrderMap.resize(queryGraph.size());

    for (VertexId i = 0; i < queryGraph.size(); ++i) {
      matchingOrderToVertexMap[i] = i;
    }
    OrderVertices orderQueryVertices(queryGraph);
    // FIXTHIS as it may lead to unconnected subgraphs
    std::sort(matchingOrderToVertexMap.begin(), matchingOrderToVertexMap.end(),
              orderQueryVertices);
    // at this point, matching order to vertex map has query vertices in
    // high-degree-first order
    galois::gDebug("Matching Order (query vertices):");
    // reverse map
    for (VertexId i = 0; i < queryGraph.size(); ++i) {
      vertexToMatchingOrderMap[matchingOrderToVertexMap[i]] = i;
      galois::gDebug(matchingOrderToVertexMap[i], "\n");
    }
  }

  //! Do the querying
  template <bool DFS, bool printEmbeddings = false>
  void exec() {
    VertexId curr_qnode = get_query_vertex(0);
    EmbeddingQueueType queue, queue2;

    // initial match of first query node
    galois::do_all(
        galois::iterate(dataGraph.begin(), dataGraph.end()),
        [&](QueryGNode n) {
          // check if the data node matches with the current query node
          if (!pruneNode(curr_qnode, dataGraph.getData(n)) &&
              matchNodeDegree(queryGraph, curr_qnode, dataGraph, n)) {
            // create an embedding for each matched node
            EmbeddingType emb;
            emb.push_back(n);
            queue.push_back(emb);
          }
        },
        galois::loopname("EmbeddingInit"));

    // TODO handle 1 node query graph case here; everything in emb is a match

    if (DFS) {
      extend_vertex<true, printEmbeddings>(queue, queue2);
    } else {
      unsigned level = 1;
      while (queue.begin() != queue.end()) {
        if (printEmbeddings)
          queue.printout_embeddings(level, true);

        extend_vertex<false, printEmbeddings>(queue, queue2);

        if (level == queryGraph.size() - 1)
          break; // if embedding size = k, done

        // old/new worklist swapping
        queue.swap(queue2);
        queue2.clear();
        level++;
      }
    }
  }

  size_t get_total_count() { return total_count; }

  void print_output() {
    galois::gDebug("Number of matched subgraphs: ", get_total_count(), "\n");
  }
};

/**
 * Get subgraph matches from data graph given some query graph.
 */
template <bool afterGraphSimulation>
size_t subgraphQuery(QueryGraph& query_graph, QueryGraph& data_graph) {
  galois::StatTimer initTime("MiningInitTime");
  initTime.start();
  SubgraphQueryMiner<afterGraphSimulation> miner(data_graph, query_graph);
  // get matching order in the miner class
  miner.init();
  initTime.stop();

  bool showEmbed = false;
  galois::StatTimer miningTime("PatternMiningTime");
  miningTime.start();
  if (showEmbed) {
    miner.template exec<false, true>();
  } else {
    miner.template exec<false, false>();
  }
  miningTime.stop();
  return miner.get_total_count();
}
