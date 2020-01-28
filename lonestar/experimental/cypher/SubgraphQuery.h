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

	bool operator() (VertexId left, VertexId right) {
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

	unsigned get_in_degree(Graph *g, VertexId vid) {
		return std::distance(g->in_edge_begin(vid), g->in_edge_end(vid));
	}

	bool pruneNode(Graph* queryGraph, GNode& queryNodeID, Node& dataNode) {
		if (afterGraphSimulation) {
			return !(dataNode.matched & (1 << queryNodeID));
		} else {
			return !matchNodeLabel(queryGraph->getData(queryNodeID), dataNode);
		}
	}

	inline bool near_search(unsigned key, EdgeData& label, Graph::edge_iterator mid, Graph::edge_iterator begin, Graph::edge_iterator end) {
		auto temp = mid + 1;
		unsigned value;
		while (temp != end) {
			value = graph->getEdgeDst(temp);
			if (value == key) {
				auto& edgeData = graph->getEdgeData(temp);
				if (matchEdgeLabel(label, edgeData)) {
					return true;
				}
			} else {
				return false;
			}
			temp++;
		}
		temp = mid - 1;
		while (temp != begin) {
			value = graph->getEdgeDst(temp);
			if (value == key) {
				auto& edgeData = graph->getEdgeData(temp);
				if (matchEdgeLabel(label, edgeData)) {
					return true;
				}
			} else {
				return false;
			}
			temp--;
		}
		if (temp == begin) {
			value = graph->getEdgeDst(temp);
			if (value == key) {
				auto& edgeData = graph->getEdgeData(temp);
				if (matchEdgeLabel(label, edgeData)) {
					return true;
				}
			}
		}
		return false;
	}

	inline bool binary_search_with_label(unsigned key, EdgeData& label, Graph::edge_iterator begin, Graph::edge_iterator end) {
		Graph::edge_iterator l = begin;
		Graph::edge_iterator r = end-1;
		while (r >= l) {
			Graph::edge_iterator mid = l + (r - l) / 2;
			unsigned value = graph->getEdgeDst(mid);
			if (value == key) {
				auto& edgeData = graph->getEdgeData(mid);
				if (matchEdgeLabel(label, edgeData)) {
					return true;
				} else {
#ifdef GRAPH_HAS_MULTI_EDGES
					return near_search(key, label, mid, begin, end);
#else
					return false;
#endif
				}
			}
			if (value < key) l = mid + 1;
			else r = mid - 1;
		}
		return false;
	}

	// check if vertex a is connected to vertex b in a directed, labeled graph
	inline bool is_connected_with_label(unsigned a, unsigned b, EdgeData& label) {
		if (degrees[a] == 0 || degrees[b] == 0) return false;
		unsigned key = b;
		unsigned search = a;
		// TODO: optimize for direction
		auto begin = graph->edge_begin(search, galois::MethodFlag::UNPROTECTED);
		auto end = graph->edge_end(search, galois::MethodFlag::UNPROTECTED);
		// return serial_search(key, begin, end);
		return binary_search_with_label(key, label, begin, end);
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

	bool toAdd(unsigned n, const BaseEmbedding &emb, VertexId dst, unsigned pos) {
		// hack pos to find if dst is destination or source
		// TODO: find a better way to pass this info
		bool source = false;
		if (pos >= n) {
			pos -= n;
			source = true;
		}
		assert(pos < n);

		VertexId next_qnode = get_query_vertex(n); // using matching order to get query vertex id
		if (debug) {
			VertexId src = emb.get_vertex(pos);
			std::cout << "\t n = " << n << ", pos = " << pos << ", src = " << src << ", dst = " << dst << "\n";
			//std::cout << ", deg(d) = " << get_degree(graph, dst) << ", deg(q) = " << get_degree(query_graph, pos+1);
		}

		if (pruneNode(query_graph, next_qnode, graph->getData(dst))) return false;

		// if the degree is smaller than that of its corresponding query vertex
		if (get_degree(graph, dst) < get_degree(query_graph, next_qnode)) return false;
		if (get_in_degree(graph, dst) < get_in_degree(query_graph, next_qnode)) return false;

		// if this vertex already exists in the embedding
		for (unsigned i = 0; i < n; ++i) if (dst == emb.get_vertex(i)) return false;

		if (source) pos += n;
		// check the backward connectivity with previous vertices in the embedding
		for (auto e : query_graph->in_edges(next_qnode)) {
			VertexId q_dst = query_graph->getInEdgeDst(e);
			unsigned q_order = vertexToMatchingOrderMap[q_dst];
			if (q_order < n && q_order != pos) {
				VertexId d_vertex = emb.get_vertex(q_order);
				if (debug) std:: cout << "\t\t in d_vertex = " << d_vertex << "\n";
				auto qeData = query_graph->getInEdgeData(e);
				if (!is_connected_with_label(d_vertex, dst, qeData)) return false;
			}
		}
		if (source) pos -= n;
		// check the forward connectivity with previous vertices in the embedding
		for (auto e : query_graph->edges(next_qnode)) {
			VertexId q_dst = query_graph->getEdgeDst(e);
			unsigned q_order = vertexToMatchingOrderMap[q_dst];
			if (q_order < n && q_order != pos) {
				VertexId d_vertex = emb.get_vertex(q_order);
				if (debug) std:: cout << "\t\t out d_vertex = " << d_vertex << "\n";
				auto qeData = query_graph->getEdgeData(e);
				if (!is_connected_with_label(dst, d_vertex, qeData)) return false;
			}
		}

		if (debug) std::cout << "\t extending with vertex " << dst << "\n";
		return true;
	}

	inline void extend_vertex(BaseEmbeddingQueue &in_queue, BaseEmbeddingQueue &out_queue) {
		galois::do_all(galois::iterate(in_queue),
			[&](const BaseEmbedding& emb) {
				unsigned n = emb.size();
				if (debug) std::cout << "current embedding: " << emb << "\n";

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

						// each outgoing neighbor of d_vertex is a candidate
						for (auto d_edge : graph->edges(d_vertex)) {
							auto deData = graph->getEdgeData(d_edge);
        					if (!matchEdgeLabel(qeData, deData)) continue;

							GNode d_dst = graph->getEdgeDst(d_edge);
							if (toAdd(n, emb, d_dst, q_order)) {
								if (n < max_size-1) { // generate a new embedding and add it to the next queue
									BaseEmbedding new_emb(emb);
									new_emb.push_back(d_dst);
									out_queue.push_back(new_emb);
								} else {
									if (show) {
										BaseEmbedding new_emb(emb);
										new_emb.push_back(d_dst);
										std::cout << "Found embedding: " << new_emb << "\n";
									}
									total_num += 1; // if size = max_size, no need to add to the queue, just accumulate
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

						// each incoming neighbor of d_vertex is a candidate
						for (auto d_edge : graph->in_edges(d_vertex)) {
							GNode d_dst = graph->getInEdgeDst(d_edge);
							if (toAdd(n, emb, d_dst, q_order+n)) {
								if (n < max_size-1) { // generate a new embedding and add it to the next queue
									BaseEmbedding new_emb(emb);
									new_emb.push_back(d_dst);
									out_queue.push_back(new_emb);
								} else {
									if (show) {
										BaseEmbedding new_emb(emb);
										new_emb.push_back(d_dst);
										std::cout << "Found embedding: " << new_emb << "\n";
									}
									total_num += 1; // if size = max_size, no need to add to the queue, just accumulate
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

	void exec() {
		VertexId curr_qnode = get_query_vertex(0);
		EmbeddingQueueType queue, queue2;
		for (size_t i = 0; i < graph->size(); ++i) {
			if (pruneNode(query_graph, curr_qnode, graph->getData(i))) continue;
			if(get_degree(graph, i) < get_degree(query_graph, curr_qnode)) continue;
			EmbeddingType emb;
			emb.push_back(i);
			queue.push_back(emb);
		}
		unsigned level = 1;
		while(true) {
			if (show) queue.printout_embeddings(level, debug);
			extend_vertex(queue, queue2);
			if (level == query_graph->size()-1) break; // if embedding size = k, done
			queue.swap(queue2);
			queue2.clear();
			level++;
		}
	}

	void print_output() {
		std::cout << "\n\ttotal_num_subgraphs = " << get_total_count() << "\n";
	}
};

template <bool afterGraphSimulation>
size_t subgraphQuery(Graph& query_graph, Graph& data_graph) {
	ResourceManager rm;
	AppMiner<afterGraphSimulation> miner(&data_graph, &query_graph);
	miner.init();

	galois::StatTimer miningTime("PatternMiningTime");
	miningTime.start();
	miner.exec();
	miningTime.stop();
	return miner.get_total_count();
}
