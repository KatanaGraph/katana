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


	bool pruneNode(Graph* queryGraph, const GNode& queryNodeID, Node& dataNode) {
		if (afterGraphSimulation) {
			return !(dataNode.matched & (1 << queryNodeID));
		} else {
			return !matchNodeLabel(queryGraph->getData(queryNodeID), dataNode);
		}
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
	using NeighborsTy = galois::gstl::Vector<std::pair<unsigned, EdgeData>>;

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
		// FIXTHIS as it may lead to unconnected subgraphs
		std::sort(matchingOrderToVertexMap.begin(), matchingOrderToVertexMap.end(), orderQueryVertices);
		for (VertexId i = 0; i < query_graph->size(); ++i) {
			vertexToMatchingOrderMap[matchingOrderToVertexMap[i]] = i;
		}
	}

	bool toExtend(unsigned n, const BaseEmbedding &emb, unsigned pos) {
		return true;
	}

	bool toAdd(unsigned n, const BaseEmbedding &emb, const VertexId dst, unsigned index, const NeighborsTy& neighbors, unsigned numInNeighbors) {

 		VertexId next_qnode = get_query_vertex(n); // using matching order to get query vertex id

		galois::gDebug("n = ", n, ", pos = ", neighbors[index].first, ", src = ", emb.get_vertex(neighbors[index].first), ", dst = ", dst, "\n");
		//galois::gDebug(", deg(d) = ", graph->degree(dst), ", deg(q) = ", query_graph->degree(pos+1));

		if (pruneNode(query_graph, next_qnode, graph->getData(dst))) return false;

		// if the degree is smaller than that of its corresponding query vertex
		if (!matchNodeDegree(*query_graph, next_qnode, *graph, dst)) return false;

		// if this vertex already exists in the embedding
		for (unsigned i = 0; i < n; ++i) if (dst == emb.get_vertex(i)) return false;

		for (unsigned i = 0; i < neighbors.size(); ++i) {
			if (i == index) continue;

			auto q_order = neighbors[i].first;
			auto qeData = neighbors[i].second;
			VertexId d_vertex = emb.get_vertex(q_order);

	 		if (numInNeighbors > index) {
				// check the backward connectivity with previous vertices in the embedding
				galois::gDebug("in d_vertex = ", d_vertex, "\n");
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
			} else {
				// check the forward connectivity with previous vertices in the embedding
				galois::gDebug("out d_vertex = ", d_vertex, "\n");
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

	template <bool DFS, bool printEmbeddings = false>
	void addEmbedding(unsigned n, const BaseEmbedding &emb, const VertexId dst, NeighborsTy& neighbors, unsigned& numInNeighbors, BaseEmbeddingQueue &out_queue) {
		if (n < max_size-1) { // generate a new embedding and add it to the next queue
			BaseEmbedding new_emb(emb);
			new_emb.push_back(dst);
			if (DFS) {
				process_embedding<DFS, printEmbeddings>(new_emb, neighbors, numInNeighbors, out_queue);
			} else {
				out_queue.push_back(new_emb);
			}
		} else {
			if (printEmbeddings) {
				BaseEmbedding new_emb(emb);
				new_emb.push_back(dst);
				galois::gPrint("Found embedding: ", new_emb, "\n");
			}
			total_num += 1; // if size = max_size, no need to add to the queue, just accumulate
		}
	}

	void constructNeighbors(unsigned n, NeighborsTy& neighbors, unsigned& numInNeighbors) {
		// get next query vertex
		VertexId next_qnode = get_query_vertex(n); // using matching order to get query vertex id

		// for each incoming neighbor of the next query vertex in the query graph
		for (auto q_edge : query_graph->in_edges(next_qnode)) {
			VertexId q_dst = query_graph->getInEdgeDst(q_edge);
			unsigned q_order = vertexToMatchingOrderMap[q_dst]; // using query vertex id to get its matching order

			// pick a neighbor that is already visited
			if (q_order < n) {
				auto qeData = query_graph->getInEdgeData(q_edge);
				neighbors.push_back(std::make_pair(q_order, qeData));
			}
		}
		numInNeighbors = neighbors.size();
		// for each outgoing neighbor of the next query vertex in the query graph
		for (auto q_edge : query_graph->edges(next_qnode)) {
			VertexId q_dst = query_graph->getEdgeDst(q_edge);
			unsigned q_order = vertexToMatchingOrderMap[q_dst]; // using query vertex id to get its matching order

			// pick a neighbor that is already visited
			if (q_order < n) {
				auto qeData = query_graph->getEdgeData(q_edge);
				neighbors.push_back(std::make_pair(q_order, qeData));
			}
		}
		assert(neighbors.size() > 0);
	}

	unsigned pickNeighbor(const BaseEmbedding& emb, const NeighborsTy& neighbors, const unsigned numInNeighbors) {
		// pick the neighbor with the least number of candidates/edges
		unsigned index;
		if (neighbors.size() < 3) { // TODO: make this configurable
			index = 0;
		} else {
			index = neighbors.size();
			size_t numCandidates = graph->size(); // conservative upper limit
			for (unsigned i = 0; i < neighbors.size(); ++i) {
				auto q_order = neighbors[i].first;
				auto qeData = neighbors[i].second;
				VertexId d_vertex = emb.get_vertex(q_order);
				size_t numEdges;
				if (numInNeighbors > i) {
					numEdges = graph->degree(d_vertex, qeData);
				} else {
					numEdges = graph->in_degree(d_vertex, qeData);
				}
				if (numEdges < numCandidates) {
					numCandidates = numEdges;
					index = i;
				}
			}
		}
		return index;
	}

	template <bool DFS, bool printEmbeddings = false>
	void process_embedding(const BaseEmbedding& emb, NeighborsTy& neighbors, unsigned numInNeighbors, BaseEmbeddingQueue &out_queue) {
		galois::gDebug("current embedding: ", emb, "\n");
		unsigned n = emb.size();

		if (DFS) {
			neighbors.clear();
			constructNeighbors(n, neighbors, numInNeighbors);
		}

		// pick the neighbor with the least number of candidates/edges
		unsigned index = pickNeighbor(emb, neighbors, numInNeighbors);
		auto q_order = neighbors.at(index).first;
		auto qeData = neighbors.at(index).second;
		VertexId d_vertex = emb.get_vertex(q_order);

		if (numInNeighbors > index) {
#ifdef USE_QUERY_GRAPH_WITH_MULTIPLEXING_EDGE_LABELS
			for (auto deData : graph->data_range()) {
				if (!matchEdgeLabel(qeData, *deData)) continue;
#else
			{
				auto deData = &qeData;
#endif
				// each outgoing neighbor of d_vertex is a candidate
				for (auto d_edge : graph->edges(d_vertex, *deData)) {
					GNode d_dst = graph->getEdgeDst(d_edge);
					if (toAdd(n, emb, d_dst, index, neighbors, numInNeighbors)) {
						addEmbedding<DFS, printEmbeddings>(n, emb, d_dst, neighbors, numInNeighbors, out_queue);
					}
				}
			}
		} else {
#ifdef USE_QUERY_GRAPH_WITH_MULTIPLEXING_EDGE_LABELS
			for (auto deData : graph->data_range()) {
				if (!matchEdgeLabel(qeData, *deData)) continue;
#else
			{
				auto deData = &qeData;
#endif
				// each incoming neighbor of d_vertex is a candidate
				for (auto d_edge : graph->in_edges(d_vertex, *deData)) {
					GNode d_dst = graph->getInEdgeDst(d_edge);
					if (toAdd(n, emb, d_dst, index, neighbors, numInNeighbors)) {
						addEmbedding<DFS, printEmbeddings>(n, emb, d_dst, neighbors, numInNeighbors, out_queue);
					}
				}
			}
		}
	}

	template <bool DFS, bool printEmbeddings = false>
	inline void extend_vertex(BaseEmbeddingQueue &in_queue, BaseEmbeddingQueue &out_queue) {

		unsigned numInNeighbors;
		NeighborsTy neighbors;
		if (!DFS) {
			unsigned n = in_queue.begin()->size();
			constructNeighbors(n, neighbors, numInNeighbors);
		}

		galois::do_all(galois::iterate(in_queue),
			[&](const BaseEmbedding& emb) {
				process_embedding<DFS, printEmbeddings>(emb, neighbors, numInNeighbors, out_queue);
			},
			galois::chunk_size<CHUNK_SIZE>(),
			galois::steal(),
			galois::loopname("Extending")
		);
	}

	VertexId get_query_vertex(unsigned id) {
		return matchingOrderToVertexMap[id];
	}

	template <bool DFS, bool printEmbeddings = false>
	void exec() {
		VertexId curr_qnode = get_query_vertex(0);
		EmbeddingQueueType queue, queue2;

		galois::do_all(galois::iterate(graph->begin(), graph->end()),
			[&](GNode n) { 
				if (!pruneNode(query_graph, curr_qnode, graph->getData(n)) && 
					matchNodeDegree(*query_graph, curr_qnode, *graph, n)) { 
					EmbeddingType emb;
					emb.push_back(n);
					queue.push_back(emb);
				}
			},
			galois::loopname("EmbeddingInit")
			);
		
		if (DFS) {
			extend_vertex<true, printEmbeddings>(queue, queue2);
		} else {
			unsigned level = 1;
			while(queue.begin() != queue.end()) {
				if (printEmbeddings) queue.printout_embeddings(level, debug);
				extend_vertex<false, printEmbeddings>(queue, queue2);
				if (level == query_graph->size()-1) break; // if embedding size = k, done
				queue.swap(queue2);
				queue2.clear();
				level++;
			}
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
		miner.template exec<false, true>();
	} else {
		miner.template exec<false, false>();
	}
	miningTime.stop();
	return miner.get_total_count();
}
