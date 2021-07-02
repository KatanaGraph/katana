#include "Helper.h"

/**
 * Initialize the nodes in the graph
 *
 * @param graph Graph
 * @param num_hedges Number of hyperedges in the specified param graph
 */
void
InitNodes(HyperGraph* graph, const uint32_t num_hedges) {
  katana::do_all(
      katana::iterate(*graph),
      [&](GNode n) {
        MetisNode& node = graph->getData(n);
        NetnumTy max_netnum = std::numeric_limits<NetnumTy>::max();
        NetvalTy max_netval = std::numeric_limits<NetvalTy>::max();
        // only hyper edge has its own indices.
        node.netnum = (n < num_hedges) ? (n + 1) : max_netnum;
        node.netrand = max_netval;
        node.netval = max_netval;
        node.node_id = n + 1;  ///< all nodes/hedges have indices.
        node.graph_index = 0;
        node.InitRefine();  ///< partition <- 0, bound <- true.
      },
      katana::loopname("Init-Nodes"));
}

/**
 * Constructs LC_CSR graph from the input file
 *
 * @param graph Graph to be constructed
 * @param filename Input graph file name
 */
void
ConstructGraph(
    HyperGraph* graph, const std::string filename,
    const bool skip_isolated_hedges) {
  std::ifstream f(filename.c_str());
  std::string line;
  std::getline(f, line);
  std::stringstream ss(line);
  uint32_t num_hedges{0}, num_hnodes{0}, total_num_nodes{0};
  uint64_t num_fedges{0};

  ss >> num_hedges >> num_hnodes;

  katana::gPrint(" Number of hedges: ", num_hedges, "\n");
  katana::gPrint(" Number of nodes: ", num_hnodes, "\n");

  katana::StatTimer timer_graph_construt("MetisGraphConstruct");
  timer_graph_construt.start();
  // Inspection phase: count the number of hyper edges.
  uint32_t num_read_hedges{0};
  while (std::getline(f, line)) {
    if (num_read_hedges >= num_hedges) {
      KATANA_LOG_FATAL("ERROR: too many lines in input file");
    }
    std::stringstream ss(line);
    uint32_t node_id;
    uint32_t num_nodes_in_hedge{0};
    while (ss >> node_id) {
      if ((node_id < 1) || (node_id > num_hnodes)) {
        KATANA_LOG_FATAL("ERROR: node value {} out of bounds", node_id);
      }
      num_nodes_in_hedge++;
    }

    if (num_nodes_in_hedge > 1) {
      num_read_hedges++;
    }
  }
  num_hedges = num_read_hedges;
  total_num_nodes = num_hedges + num_hnodes;

  // Reset the file descriptor for execution phase.
  f.clear();
  f.seekg(0);
  std::getline(f, line);

  // Execution phase: construct hyper graph.
  EdgeDstVecTy edges_id(total_num_nodes);
  NUMAArrayUint64Ty prefix_edges;
  prefix_edges.allocateInterleaved(total_num_nodes);
  num_read_hedges = 0;
  while (std::getline(f, line)) {
    if (num_read_hedges > num_hedges) {
      KATANA_LOG_FATAL("ERROR: too many lines in input file");
    }
    std::stringstream ss(line);
    GNode node_id;
    GNode first_new_node_id{0};
    uint32_t num_nodes_in_hedge{0};
    while (ss >> node_id) {
      if (node_id < 1 || node_id > num_hnodes) {
        KATANA_LOG_FATAL("ERROR: node id {} out of bounds", node_id);
      }
      // Node is relocated to the next slots of the hyper edge.
      GNode new_node_id = num_hedges + (node_id - 1);
      if (!skip_isolated_hedges) {
        edges_id[num_read_hedges].push_back(new_node_id);
      } else {
        if (num_nodes_in_hedge > 0) {
          if (num_nodes_in_hedge == 1) {
            edges_id[num_read_hedges].push_back(first_new_node_id);
          }
          edges_id[num_read_hedges].push_back(new_node_id);
        } else {
          // If user passes the option to ignore hyperedges with only one node,
          // then do not push the first node now.
          // Just postpone pushing this and push it if the current hyperedge
          // has at least 2 nodes.
          first_new_node_id = new_node_id;
        }
        num_nodes_in_hedge++;
      }
    }

    if (skip_isolated_hedges && num_nodes_in_hedge < 2) {
      continue;
    }
    prefix_edges[num_read_hedges] = num_nodes_in_hedge;
    num_fedges += prefix_edges[num_read_hedges++];
  }
  f.close();
  graph->SetHedges(num_hedges);
  graph->SetHnodes(num_hnodes);

  katana::ParallelSTL::partial_sum(
      prefix_edges.begin(), prefix_edges.end(), prefix_edges.begin());

  // # nodes = (# of hyper edges + # of nodes), which means each hyper edge
  // is considered as a node.
  // # edges = (# of normal edges).
  graph->constructFrom(
      total_num_nodes, num_fedges, std::move(prefix_edges), edges_id);
  InitNodes(graph, num_hedges);

  timer_graph_construt.stop();
  katana::gPrint(
      " Time to construct Metis Graph ", timer_graph_construt.get(), "\n");
}

/**
 * Priority assinging functions.
 */
void
PrioritizeHigherDegree(GNode node, HyperGraph* fine_graph) {
  NetvalTy num_edges =
      std::distance(fine_graph->edge_begin(node), fine_graph->edge_end(node));
  fine_graph->getData(node).netval = -num_edges;
}
void
PrioritizeRandom(GNode node, HyperGraph* fine_graph) {
  MetisNode& node_data = fine_graph->getData(node);

  node_data.netval = -node_data.netrand;
  node_data.netrand = -node_data.netnum;
}
void
PrioritizeLowerDegree(GNode node, HyperGraph* fine_graph) {
  NetvalTy num_edges =
      std::distance(fine_graph->edge_begin(node), fine_graph->edge_end(node));
  fine_graph->getData(node).netval = num_edges;
}
void
PrioritizeHigherWeight(GNode node, HyperGraph* fine_graph) {
  WeightTy w = 0;
  for (auto& e : fine_graph->edges(node)) {
    GNode dst = fine_graph->getEdgeDst(e);
    w += fine_graph->getData(dst).weight;
  }
  fine_graph->getData(node).netval = -w;
}
void
PrioritizeDegree(GNode node, HyperGraph* fine_graph) {
  WeightTy w = 0;
  for (auto& e : fine_graph->edges(node)) {
    GNode dst = fine_graph->getEdgeDst(e);
    w += fine_graph->getData(dst).weight;
  }
  fine_graph->getData(node).netval = w;
}

void
SortNodesByGainAndWeight(
    HyperGraph* graph, std::vector<GNode>* nodes, uint32_t end_offset = 0) {
  auto end_iter =
      (end_offset == 0) ? nodes->end() : nodes->begin() + end_offset;
  std::sort(nodes->begin(), end_iter, [&graph](GNode& l_opr, GNode& r_opr) {
    MetisNode& l_data = graph->getData(l_opr);
    MetisNode& r_data = graph->getData(r_opr);
    float l_gain = l_data.GetGain();
    float r_gain = r_data.GetGain();
    float l_weight = l_data.weight;
    float r_weight = r_data.weight;
    float l_cost = l_gain / l_weight;
    float r_cost = r_gain / r_weight;

    if (fabs(l_cost - r_cost) < 0.00001f) {
      uint32_t l_nid = l_data.node_id;
      uint32_t r_nid = r_data.node_id;

      return l_nid < r_nid;
    }

    return l_cost > r_cost;
  });
}

void
InitGain(HyperGraph* g) {
  uint32_t num_hedges = g->GetHedges();
  uint32_t size_graph = static_cast<uint32_t>(g->size());

  katana::do_all(
      katana::iterate(num_hedges, size_graph),
      [&](uint32_t n) {
        MetisNode& node = g->getData(n);
        node.positive_gain = 0;
        node.negative_gain = 0;
      },
      katana::loopname("Init-Gains"));

  typedef std::vector<GainTy> LocalGainVector;
  katana::PerThreadStorage<LocalGainVector> thread_local_gain_vector;

  uint32_t num_threads = katana::getActiveThreads();
  uint32_t subvec_size = size_graph - num_hedges;

  katana::do_all(katana::iterate(uint32_t{0}, num_threads), [&](uint32_t i) {
    thread_local_gain_vector.getRemote(i)->resize(subvec_size, 0);
  });

  katana::do_all(
      katana::iterate(uint32_t{0}, num_hedges),
      [&](uint32_t n) {
        uint32_t num_p0_nodes{0}, num_p1_nodes{0};
        for (auto& fedge : g->edges(n)) {
          GNode node = g->getEdgeDst(fedge);
          if (g->getData(node).partition == 0) {
            ++num_p0_nodes;
          } else {
            ++num_p1_nodes;
          }

          if (num_p0_nodes > 1 && num_p1_nodes > 1) {
            break;
          }
        }

        LocalGainVector& gain_vector = *(thread_local_gain_vector.getLocal());

        // --> p1 = 1 or 0/ p2 = 1 or 0
        if (!(num_p0_nodes > 1 && num_p1_nodes > 1) &&
            (num_p0_nodes + num_p1_nodes > 1)) {
          for (auto& fedge : g->edges(n)) {
            GNode node = g->getEdgeDst(fedge);
            MetisNode& node_data = g->getData(node);
            uint32_t part = node_data.partition;
            uint32_t nodep = (part == 0) ? num_p0_nodes : num_p1_nodes;
            if (nodep == 1) {
              gain_vector[node - num_hedges] += 1;
            }
            // It means that one of p1 or p2 is zero.
            if (nodep == (num_p0_nodes + num_p1_nodes)) {
              gain_vector[node - num_hedges] -= 1;
            }
          }
        }
      },
      katana::steal(), katana::loopname("Calculate-Gains"));

  katana::do_all(
      katana::iterate(num_hedges, size_graph),
      [&](GNode n) {
        GainTy gain{0};
        uint32_t index_n = n - num_hedges;
        for (uint32_t i = 0; i < num_threads; i++) {
          gain += (*(thread_local_gain_vector.getRemote(i)))[index_n];
        }

        g->getData(n).positive_gain = gain;
      },
      katana::loopname("Reduce-Gains"));
}

void
InitGain(
    const std::vector<std::pair<uint32_t, uint32_t>>& combined_edgelist,
    const std::vector<std::pair<uint32_t, uint32_t>>& combined_nodelist,
    const std::vector<HyperGraph*>& g) {
  uint32_t total_nodes = combined_nodelist.size();
  uint32_t total_hedges = combined_edgelist.size();

  katana::do_all(
      katana::iterate(uint32_t{0}, total_nodes),
      [&](uint32_t n) {
        auto node_index_pair = combined_nodelist[n];
        GNode node_id = node_index_pair.first;
        uint32_t index = node_index_pair.second;
        MetisNode& node_data = g[index]->getData(node_id);
        node_data.positive_gain = 0;
        node_data.negative_gain = 0;
        node_data.list_index = n;
      },
      katana::loopname("Init-Gains"));

  typedef std::vector<GainTy> LocalGainVector;
  katana::PerThreadStorage<LocalGainVector> thread_local_positive_gain_vector;
  katana::PerThreadStorage<LocalGainVector> thread_local_negative_gain_vector;

  uint32_t num_threads = katana::getActiveThreads();

  katana::do_all(katana::iterate(uint32_t{0}, num_threads), [&](uint32_t i) {
    thread_local_positive_gain_vector.getRemote(i)->resize(total_nodes, 0);
    thread_local_negative_gain_vector.getRemote(i)->resize(total_nodes, 0);
  });

  katana::do_all(
      katana::iterate(uint32_t{0}, total_hedges),
      [&](uint32_t n) {
        auto hedge_index_pair = combined_edgelist[n];
        GNode node_id = hedge_index_pair.first;
        uint32_t index = hedge_index_pair.second;
        HyperGraph& graph = *g[index];
        uint32_t num_p0_nodes{0}, num_p1_nodes{0};

        for (auto& fedge : graph.edges(node_id)) {
          GNode node = graph.getEdgeDst(fedge);
          if (graph.getData(node).partition == 0) {
            ++num_p0_nodes;
          } else {
            ++num_p1_nodes;
          }

          if (num_p0_nodes > 1 && num_p1_nodes > 1) {
            break;
          }
        }

        LocalGainVector& positive_gain_vector =
            *(thread_local_positive_gain_vector.getLocal());
        LocalGainVector& negative_gain_vector =
            *(thread_local_negative_gain_vector.getLocal());

        // --> p1 = 1 or 0/ p2 = 1 or 0
        if (!(num_p0_nodes > 1 && num_p1_nodes > 1) &&
            (num_p0_nodes + num_p1_nodes > 1)) {
          for (auto& fedge : graph.edges(node_id)) {
            GNode dst_id = graph.getEdgeDst(fedge);
            MetisNode& node_data = graph.getData(dst_id);
            uint32_t part = node_data.partition;
            uint32_t nodep = (part == 0) ? num_p0_nodes : num_p1_nodes;
            uint32_t list_index = node_data.list_index;

            if (nodep == 1) {
              positive_gain_vector[list_index] += 1;
            }
            if (nodep == (num_p0_nodes + num_p1_nodes)) {
              // it means that one of p1 or p2 is zero.
              negative_gain_vector[list_index] += 1;
            }
          }
        }
      },
      katana::steal(), katana::loopname("Calculate-Gains"));

  katana::do_all(
      katana::iterate(uint32_t{0}, total_nodes),
      [&](uint32_t n) {
        GainTy positive_gain{0};
        GainTy negative_gain{0};

        for (uint32_t i = 0; i < num_threads; i++) {
          positive_gain +=
              (*(thread_local_positive_gain_vector.getRemote(i)))[n];
          negative_gain +=
              (*(thread_local_negative_gain_vector.getRemote(i)))[n];
        }

        auto node_index_pair = combined_nodelist[n];
        GNode node_id = node_index_pair.first;
        uint32_t index = node_index_pair.second;
        MetisNode& node_data = g[index]->getData(node_id);
        node_data.positive_gain = positive_gain;
        node_data.negative_gain = negative_gain;
      },
      katana::loopname("Reduce-Gains"));
}
