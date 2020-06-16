#pragma once

#include "pangolin/types.h"
#include "pangolin/base_embedding.h"
#include "pangolin/embedding_queue.h"
#include "galois/graphs/QueryGraph.h"
#include "querying/GraphSimulation.h"
#include <algorithm>

#define QUERY_CHUNK_SIZE 256

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
  QueryGraph& dataGraph;
  QueryGraph& queryGraph;
  std::vector<VertexId> matchingOrderToVertexMap;
  std::vector<VertexId> vertexToMatchingOrderMap;
  size_t total_count;
  galois::gstl::Vector<uint32_t> degrees; // TODO: change these to LargeArray
  galois::gstl::Vector<uint32_t> inDegrees;

  typedef BaseEmbedding EmbeddingType;
  typedef EmbeddingQueue<EmbeddingType> EmbeddingQueueType;
  using NeighborsTy = galois::gstl::Vector<std::pair<unsigned, QueryEdgeData>>;

  VertexId get_query_vertex(unsigned id) {
    return matchingOrderToVertexMap[id];
  }

  inline bool pruneNode(const QueryGNode& queryNodeID,
                               QueryNode& dataNode) {
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

  // check if vertex a is connected to vertex b in a directed, labeled graph
  inline bool is_connected_with_label(unsigned a, unsigned b,
                                      const QueryEdgeData& label) {
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

  bool toExtend(unsigned, const EmbeddingType&, unsigned) {
    return true;
  }

  bool toAdd(unsigned n, const EmbeddingType& emb,
                    unsigned index, const VertexId dst,
                    const NeighborsTy& neighbors, unsigned numInNeighbors) {
    VertexId next_qnode =
        get_query_vertex(n); // using matching order to get query vertex id

    // galois::gDebug(", deg(d) = ", dataGraph.degree(dst), ", deg(q) = ",
    // queryGraph.degree(pos+1));

    if (pruneNode(next_qnode, dataGraph.getData(dst)))
      return false;

    // if the degree is smaller than that of its corresponding query vertex
    if (!matchNodeDegree(queryGraph, next_qnode, dataGraph, dst))
      return false;

    // if this vertex already exists in the embedding
    for (unsigned i = 0; i < n; ++i)
      if (dst == emb.get_vertex(i))
        return false;

    galois::gDebug("Checking connectivity of data vertex: ", dst, "...\n");

    for (unsigned i = 0; i < neighbors.size(); ++i) {
      if (i == index)
        continue;

      auto q_order      = neighbors[i].first;
      auto qeData       = neighbors[i].second;
      VertexId d_vertex = emb.get_vertex(q_order);

      if (numInNeighbors > index) {
        // check the backward connectivity with previous vertices in the
        // embedding
        galois::gDebug("Checking if ", dst, " is an outgoing neighbor of ", d_vertex, "...\n");
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
        galois::gDebug("Checking if ", dst, " is an incoming neighbor of ", d_vertex, "...\n");
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

    galois::gDebug("Extending with vertex ", dst, "\n");
    return true;
  }

  template <bool DFS, bool printEmbeddings = false>
  void addEmbedding(unsigned n, const EmbeddingType& emb, const VertexId dst,
                    NeighborsTy& neighbors, unsigned& numInNeighbors,
                    EmbeddingQueueType& out_queue) {
    if (n < queryGraph.size() - 1) { // generate a new embedding and add it to the next queue
      EmbeddingType new_emb(emb);
      new_emb.push_back(dst);
      if (DFS) {
        process_embedding<DFS, printEmbeddings>(new_emb, neighbors,
                                                numInNeighbors, out_queue);
      } else {
        out_queue.push_back(new_emb);
      }
    } else {
      if (printEmbeddings) {
        EmbeddingType new_emb(emb);
        new_emb.push_back(dst);
        galois::gPrint("Found embedding: ", new_emb, "\n");
      }
      total_count += 1; // if size = queryGraph.size(), no need to add to the queue, just accumulate
    }
  }

  void constructNeighbors(unsigned n, NeighborsTy& neighbors,
                          unsigned& numInNeighbors) {
    // get next query vertex
    VertexId next_qnode =
        get_query_vertex(n); // using matching order to get query vertex id
    galois::gDebug("Incoming neighbors of query vertex ", next_qnode, "(level ", n, "):\n");

    // for each incoming neighbor of the next query vertex in the query graph
    for (auto q_edge : queryGraph.in_edges(next_qnode)) {
      VertexId q_dst = queryGraph.getInEdgeDst(q_edge);
      unsigned q_order =
          vertexToMatchingOrderMap[q_dst]; // using query vertex id to get its
                                           // matching order

      // add the neighbor that is already visited
      if (q_order < n) {
        auto qeData = queryGraph.getInEdgeData(q_edge);
        neighbors.push_back(std::make_pair(q_order, qeData));
        galois::gDebug(q_order, ",", qeData, "\n");
      }
    }
    numInNeighbors = neighbors.size();
    galois::gDebug("Outgoing neighbors of query vertex ", next_qnode, "(level ", n, "):\n");
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
        galois::gDebug(q_order, ",", qeData, "\n");
      }
    }
    assert(neighbors.size() > 0);
  }

  unsigned pickNeighbor(const EmbeddingType& emb, const NeighborsTy& neighbors,
                        const unsigned numInNeighbors) {
    // pick the neighbor with the least number of candidates/edges
    unsigned index;
    if (neighbors.size() < 3) { // TODO: make this configurable
      index = 0;
    } else {
      index                = neighbors.size();
      size_t numCandidates = dataGraph.size(); // conservative upper limit
      for (unsigned i = 0; i < neighbors.size(); ++i) {
        auto q_order      = neighbors[i].first;
        auto qeData       = neighbors[i].second;
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

  template <bool DFS, bool printEmbeddings = false>
  void process_embedding(const EmbeddingType& emb, NeighborsTy& neighbors,
                         unsigned numInNeighbors,
                         EmbeddingQueueType& out_queue) {
    galois::gDebug("Current embedding: ", emb, "\n");
    unsigned n = emb.size();

    if (DFS) {
      neighbors.clear();
      constructNeighbors(n, neighbors, numInNeighbors);
    }

    // pick the neighbor with the least number of candidates/edges
    unsigned index    = pickNeighbor(emb, neighbors, numInNeighbors);
    auto q_order      = neighbors.at(index).first;
    auto qeData       = neighbors.at(index).second;
    VertexId d_vertex = emb.get_vertex(q_order);
    galois::gDebug("Picked data vertex to extend: ", d_vertex, "\n");

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
        for (auto d_edge : dataGraph.edges(d_vertex, *deData)) {
          QueryGNode d_dst = dataGraph.getEdgeDst(d_edge);
          galois::gDebug("Checking outgoing neighbor of ", d_vertex, ": ", d_dst, "...\n");
          if (toAdd(n, emb, d_dst, index, neighbors, numInNeighbors)) {
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
          galois::gDebug("Checking incoming neighbor ", d_vertex, ": ", d_dst, "...\n");
          if (toAdd(n, emb, d_dst, index, neighbors, numInNeighbors)) {
            addEmbedding<DFS, printEmbeddings>(n, emb, d_dst, neighbors,
                                               numInNeighbors, out_queue);
          }
        }
      }
    }
  }

  template <bool DFS, bool printEmbeddings = false>
  inline void extend_vertex(EmbeddingQueueType& in_queue,
                            EmbeddingQueueType& out_queue) {

    unsigned numInNeighbors;
    NeighborsTy neighbors;
    if (!DFS) {
      unsigned n = in_queue.begin()->size();
      constructNeighbors(n, neighbors, numInNeighbors);
    }

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
        degrees = dataGraph.countDegrees();
        inDegrees = dataGraph.countInDegrees();
  }

  ~SubgraphQueryMiner() {}

  void init() {
    assert(queryGraph.size() > 2);
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
    galois::gDebug("Matching Order (query vertices):\n");
    for (VertexId i = 0; i < queryGraph.size(); ++i) {
      vertexToMatchingOrderMap[matchingOrderToVertexMap[i]] = i;
      galois::gDebug(matchingOrderToVertexMap[i], "\n");
    }
  }

  template <bool DFS, bool printEmbeddings = false>
  void exec() {
    VertexId curr_qnode = get_query_vertex(0);
    EmbeddingQueueType queue, queue2;

    galois::do_all(
        galois::iterate(dataGraph.begin(), dataGraph.end()),
        [&](QueryGNode n) {
          if (!pruneNode(curr_qnode, dataGraph.getData(n)) &&
              matchNodeDegree(queryGraph, curr_qnode, dataGraph, n)) {
            EmbeddingType emb;
            emb.push_back(n);
            queue.push_back(emb);
          }
        },
        galois::loopname("EmbeddingInit"));

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
        queue.swap(queue2);
        queue2.clear();
        level++;
      }
    }
  }

  size_t get_total_count() {
    return total_count;
  }

  void print_output() {
    galois::gDebug("Number of matched subgraphs: ", get_total_count(), "\n");
  }
};

template <bool afterGraphSimulation>
size_t subgraphQuery(QueryGraph& query_graph, QueryGraph& data_graph) {
  galois::StatTimer initTime("MiningInitTime");
  initTime.start();
  SubgraphQueryMiner<afterGraphSimulation> miner(data_graph, query_graph);
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
