#pragma once

//#define USE_DAG
#define USE_SIMPLE
#define ENABLE_STEAL
//#define USE_EMB_LIST
#define CHUNK_SIZE 256
#define USE_BASE_TYPES
#define USE_QUERY_GRAPH
#define USE_QUERY_GRAPH_TYPE
#include "pangolin.h"
#include "GraphSimulation.h"
#include <algorithm>

class OrderVertices {
	Graph& graph;

	uint32_t totalDegree(VertexId v) {
		uint32_t num_edges = std::distance(graph.in_edge_begin(v), graph.in_edge_end(v));
		num_edges += std::distance(graph.edge_begin(v), graph.edge_end(v));
		return num_edges;
	}

public:
	OrderVertices(Graph& g) : graph(g) {}

	bool operator() (VertexId left, const VertexId right) {
		if (totalDegree(left) >= totalDegree(right))
			return true;
		return false;
	}
};

template <bool afterGraphSimulation>
class AppMiner : public VertexMiner {
protected:
	Graph *query_graph;
	std::vector<VertexId> matchingOrderToVertexMap;
	std::vector<VertexId> vertexToMatchingOrderMap;

	unsigned get_in_degree(Graph *g, const VertexId vid) {
		return std::distance(g->in_edge_begin(vid), g->in_edge_end(vid));
	}

	unsigned get_in_degree(Graph *g, const VertexId vid, const EdgeData& label) {
		return std::distance(g->in_edge_begin(vid, label), g->in_edge_end(vid, label));
	}

	unsigned get_degree(Graph *g, const VertexId vid, const EdgeData& label) {
		return std::distance(g->edge_begin(vid, label), g->edge_end(vid, label));
	}

	bool pruneNode(Graph* queryGraph, const GNode& queryNodeID, Node& dataNode) {
		if (afterGraphSimulation) {
			return !(dataNode.matched & (1 << queryNodeID));
		} else {
			return !matchNodeLabel(queryGraph->getData(queryNodeID), dataNode);
		}
	}

	bool pruneNodeUsingDegree(Graph* graph, const GNode& nodeID, Graph* queryGraph, const GNode& queryNodeID) {
		// if the degree is smaller than that of its corresponding query vertex
#ifdef USE_QUERY_GRAPH_WITH_MULTIPLEXING_EDGE_LABELS
		if (VertexMiner::get_degree(graph, nodeID) < VertexMiner::get_degree(query_graph, queryNodeID)) return true;
		if (get_in_degree(graph, nodeID) < get_in_degree(query_graph, queryNodeID)) return true;
#else
		for (auto qeData : query_graph->data_range()) {
			if (get_degree(graph, nodeID, *qeData) < get_degree(query_graph, queryNodeID, *qeData)) return true;
			if (get_in_degree(graph, nodeID, *qeData) < get_in_degree(query_graph, queryNodeID, *qeData)) return true;
		}
#endif
		return false;
	}

	template <bool inEdges>
	inline bool directed_binary_search(unsigned key, 
		Graph::edge_iterator begin, Graph::edge_iterator end) {
		Graph::edge_iterator l = begin;
		Graph::edge_iterator r = end-1;
		while (r >= l) {
			Graph::edge_iterator mid = l + (r - l) / 2;
			unsigned value = inEdges ? graph->getInEdgeDst(mid) : graph->getEdgeDst(mid);
			if (value == key) {
				return true;
			}
			if (value < key) l = mid + 1;
			else r = mid - 1;
		}
		return false;
	}

	// check if vertex a is connected to vertex b in a directed, labeled graph
	inline bool is_connected_with_label(unsigned a, unsigned b, const EdgeData& label) {
		if (degrees[a] == 0 || indegrees[b] == 0) return false;
		unsigned key = b;
		unsigned search = a;
		if (degrees[a] > indegrees[b]) {
			key = a;
			search = b;
			auto begin = graph->in_edge_begin(search, label);
			auto end = graph->in_edge_end(search, label);
			return directed_binary_search<true>(key, begin, end);
		} 
		auto begin = graph->edge_begin(search, label);
		auto end = graph->edge_end(search, label);
		return directed_binary_search<false>(key, begin, end);
	}

public:
	AppMiner(Graph *g) : VertexMiner(g) {}

	AppMiner(Graph* dgraph, Graph* qgraph)
		: VertexMiner(dgraph), query_graph(qgraph) {}

	~AppMiner() {}

	void init() {
		assert(query_graph->size() > 2);
		set_max_size(query_graph->size());
		set_num_patterns(1);
		matchingOrderToVertexMap.resize(max_size);
		vertexToMatchingOrderMap.resize(max_size);

		for (VertexId i = 0; i < query_graph->size(); ++i) {
			matchingOrderToVertexMap[i] = i;
		}
		OrderVertices orderQueryVertices(*query_graph);
		std::sort(matchingOrderToVertexMap.begin(), matchingOrderToVertexMap.end(), orderQueryVertices);
		for (VertexId i = 0; i < query_graph->size(); ++i) {
			vertexToMatchingOrderMap[matchingOrderToVertexMap[i]] = i;
		}
	}

	bool toExtend(unsigned n, const BaseEmbedding &emb, unsigned pos) {
		return true;
	}

	bool toAdd(unsigned n, const BaseEmbedding &emb, const VertexId dst, unsigned pos) {
		// hack pos to find if dst is destination or source
		// TODO: find a better way to pass this info
		bool source = false;
		if (pos >= n) {
			pos -= n;
			source = true;
		}
		assert(pos < n);

		VertexId next_qnode = get_query_vertex(n); // using matching order to get query vertex id

		galois::gDebug("n = ", n, ", pos = ", pos, ", src = ", emb.get_vertex(pos), ", dst = ", dst, "\n");
		//galois::gDebug(", deg(d) = ", get_degree(graph, dst), ", deg(q) = ", get_degree(query_graph, pos+1));

		if (pruneNode(query_graph, next_qnode, graph->getData(dst))) return false;

		// if the degree is smaller than that of its corresponding query vertex
		if (pruneNodeUsingDegree(graph, dst, query_graph, next_qnode)) return false;

		// if this vertex already exists in the embedding
		for (unsigned i = 0; i < n; ++i) if (dst == emb.get_vertex(i)) return false;

		if (source) pos += n;
		// check the backward connectivity with previous vertices in the embedding
		for (auto e : query_graph->in_edges(next_qnode)) {
			VertexId q_dst = query_graph->getInEdgeDst(e);
			unsigned q_order = vertexToMatchingOrderMap[q_dst];
			if (q_order < n && q_order != pos) {
				VertexId d_vertex = emb.get_vertex(q_order);
				galois::gDebug("in d_vertex = ", d_vertex, "\n");
				auto qeData = query_graph->getInEdgeData(e);
#ifdef USE_QUERY_GRAPH_WITH_MULTIPLEXING_EDGE_LABELS
				bool connected = false;
				for (auto deData : graph->data_range())
				{
					if (matchEdgeLabel(qeData, *deData) &&
						is_connected_with_label(d_vertex, dst, *deData)) {
						connected = true;
						break;
					}
				}
				if (!connected) return false;
#else
				if (!is_connected_with_label(d_vertex, dst, qeData)) {
					return false;
				}
#endif
			}
		}
		if (source) pos -= n;
		// check the forward connectivity with previous vertices in the embedding
		for (auto e : query_graph->edges(next_qnode)) {
			VertexId q_dst = query_graph->getEdgeDst(e);
			unsigned q_order = vertexToMatchingOrderMap[q_dst];
			if (q_order < n && q_order != pos) {
				VertexId d_vertex = emb.get_vertex(q_order);
				galois::gDebug("out d_vertex = ", d_vertex, "\n");
				auto qeData = query_graph->getEdgeData(e);
#ifdef USE_QUERY_GRAPH_WITH_MULTIPLEXING_EDGE_LABELS
				bool connected = false;
				for (auto deData : graph->data_range())
				{
					if (matchEdgeLabel(qeData, *deData) &&
						is_connected_with_label(dst, d_vertex, *deData)) {
						connected = true;
						break;
					}
				}
				if (!connected) return false;
#else
				if (!is_connected_with_label(dst, d_vertex, qeData)) {
					return false;
				}
#endif
			}
		}

		galois::gDebug("\t extending with vertex ", dst, "\n");
		return true;
	}

	template <bool printEmbeddings = false>
	inline void extend_vertex(BaseEmbeddingQueue &in_queue, BaseEmbeddingQueue &out_queue) {

		galois::do_all(galois::iterate(in_queue),
			[&](const BaseEmbedding& emb) {
				unsigned n = emb.size();
				galois::gDebug("current embedding: ", emb, "\n");

				// get next query vertex
				VertexId next_qnode = get_query_vertex(n); // using matching order to get query vertex id

				bool found_neighbor = false;

				// for each incoming neighbor of the next query vertex in the query graph
				for (auto q_edge : query_graph->in_edges(next_qnode)) {
					VertexId q_dst = query_graph->getInEdgeDst(q_edge);
					unsigned q_order = vertexToMatchingOrderMap[q_dst]; // using query vertex id to get its matching order

					// pick a neighbor that is already visited
					if (q_order < n) {
						// get the matched data vertex
						VertexId d_vertex = emb.get_vertex(q_order);

						auto qeData = query_graph->getInEdgeData(q_edge);

#ifdef USE_QUERY_GRAPH_WITH_MULTIPLEXING_EDGE_LABELS
						for (auto deData : graph->data_range())
						{
        				if (!matchEdgeLabel(qeData, *deData)) continue;
#else
						{
						auto deData = &qeData;
#endif
						// each outgoing neighbor of d_vertex is a candidate
						for (auto d_edge : graph->edges(d_vertex, *deData)) {

							GNode d_dst = graph->getEdgeDst(d_edge);
							if (toAdd(n, emb, d_dst, q_order)) {
								if (n < max_size-1) { // generate a new embedding and add it to the next queue
									BaseEmbedding new_emb(emb);
									new_emb.push_back(d_dst);
									out_queue.push_back(new_emb);
								} else {
									if (printEmbeddings) {
										BaseEmbedding new_emb(emb);
										new_emb.push_back(d_dst);
										galois::gPrint("Found embedding: ", new_emb, "\n");
									}
									total_num += 1; // if size = max_size, no need to add to the queue, just accumulate
								}
							}
						}
						}

						found_neighbor = true;
						break;
					}
				}

				if (!found_neighbor) {
				// for each outgoing neighbor of the next query vertex in the query graph
				for (auto q_edge : query_graph->edges(next_qnode)) {
					VertexId q_dst = query_graph->getEdgeDst(q_edge);
					unsigned q_order = vertexToMatchingOrderMap[q_dst]; // using query vertex id to get its matching order

					// pick a neighbor that is already visited
					if (q_order < n) {
						// get the matched data vertex
						VertexId d_vertex = emb.get_vertex(q_order);

						auto qeData = query_graph->getEdgeData(q_edge);

#ifdef USE_QUERY_GRAPH_WITH_MULTIPLEXING_EDGE_LABELS
						for (auto deData : graph->data_range())
						{
        				if (!matchEdgeLabel(qeData, *deData)) continue;
#else
						{
						auto deData = &qeData;
#endif

						// each incoming neighbor of d_vertex is a candidate
						for (auto d_edge : graph->in_edges(d_vertex, *deData)) {

							GNode d_dst = graph->getInEdgeDst(d_edge);
							if (toAdd(n, emb, d_dst, q_order+n)) {
								if (n < max_size-1) { // generate a new embedding and add it to the next queue
									BaseEmbedding new_emb(emb);
									new_emb.push_back(d_dst);
									out_queue.push_back(new_emb);
								} else {
									if (printEmbeddings) {
										BaseEmbedding new_emb(emb);
										new_emb.push_back(d_dst);
										galois::gPrint("Found embedding: ", new_emb, "\n");
									}
									total_num += 1; // if size = max_size, no need to add to the queue, just accumulate
								}
							}
						}
						}

						break;
					}
				}
				}
			},
			galois::chunk_size<CHUNK_SIZE>(),
			galois::steal(),
			galois::loopname("Extending")
		);
	}

	VertexId get_query_vertex(unsigned id) {
		return matchingOrderToVertexMap[id];
	}

	template <bool printEmbeddings = false>
	void exec() {
		VertexId curr_qnode = get_query_vertex(0);
		EmbeddingQueueType queue, queue2;

		galois::do_all(galois::iterate(graph->begin(), graph->end()),
			[&](GNode n) { 
				if (!pruneNode(query_graph, curr_qnode, graph->getData(n)) && 
					!pruneNodeUsingDegree(graph, n, query_graph, curr_qnode)) { 
					EmbeddingType emb;
					emb.push_back(n);
					queue.push_back(emb);
				}
			},
			galois::loopname("EmbeddingInit")
			);

		unsigned level = 1;
		while(queue.begin() != queue.end()) {
			if (printEmbeddings) queue.printout_embeddings(level, debug);
			extend_vertex<printEmbeddings>(queue, queue2);
			if (level == query_graph->size()-1) break; // if embedding size = k, done
			queue.swap(queue2);
			queue2.clear();
			level++;
		}
	}

	void print_output() {
		galois::gDebug("\ntotal_num_subgraphs = ", get_total_count(), "\n");
	}
};

template <bool afterGraphSimulation>
size_t subgraphQuery(Graph& query_graph, Graph& data_graph) {
	galois::StatTimer initTime("MiningInitTime");
	initTime.start();
	ResourceManager rm;
	AppMiner<afterGraphSimulation> miner(&data_graph, &query_graph);
	miner.init();
	initTime.stop();

	galois::StatTimer miningTime("PatternMiningTime");
	miningTime.start();
	if (show) {
		miner.template exec<true>();
	} else {
		miner.template exec<false>();
	}
	miningTime.stop();
	return miner.get_total_count();
}
