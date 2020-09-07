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

#include "Lonestar/BoilerPlate.h"

#include "Bipart.h"
#include "Helper.h"

namespace cll = llvm::cl;

static const char* name = "BIPART";
static const char* desc =
    "Partitions a hypergraph into K parts while minimizing the graph cut";
static const char* url = "BiPart";

static cll::opt<std::string>
    input_file(cll::Positional, cll::desc("<input file>"), cll::Required);

static cll::opt<MatchingPolicy> matching_policy(
    cll::desc("Choose the matching policy:"),
    cll::values(
        clEnumVal(
            HigherDegree,
            "HigherDegree: Higher Priority assigned to high degree hyperedges"),
        clEnumVal(
            LowerDegree,
            "LowerDegree: Higher Priority assigned to low degree hyperedges"),
        clEnumVal(
            HigherWeight,
            "HigherWeight: Higher Priority assigned to high weight hyperedges"),
        clEnumVal(
            LowerWeight,
            "LowerWeight: Higher Priority assigned to low weight hyperedges"),
        clEnumVal(Random, "Random: Priority assigned using deterministic hash "
                          "of hyperedge ids")),
    cll::init(HigherDegree));

static cll::opt<std::string> output_file_name(
    "output_file_name",
    cll::desc("File name to store partition ids for the nodes"));

static cll::opt<uint32_t>
    max_coarse_graph_size("max_coarse_graph_size",
                          cll::desc("Size of coarsest graph allowed"),
                          cll::init(25));

static cll::opt<uint32_t>
    num_partitions("num_partitions", cll::desc("Number of partitions required"),
                   cll::init(2));

// Flag that forces user to be aware that they should be passing in a
// hMetis graph.
static cll::opt<bool> hyper_metis_graph(
    "hyperMetisGraph",
    cll::desc(
        "Specify that the input graph is in a valid HypgerGraph Metis format "
        "(http://glaros.dtc.umn.edu/gkhome/fetch/sw/hmetis/manual.pdf)"),
    cll::init(false));

static cll::opt<bool>
    output("output", cll::desc("Specify if partitions need to be written"),
           cll::init(false));

static cll::opt<bool> skip_lone_hedges(
    "skip_lone_hedges",
    cll::desc("Specify if degree 1 hyperedges should not be included"),
    cll::init(false));

/**
 * Main Partitioning function for creating bi-partitions for all
 * graphs at a given level of the k-way recursion tree
 *
 * @param metis_graphs Vector containing metis graphs
 * @param max_coarsen_level Maximum number of coarsening levels allowed
 * @param target_partitions Vector containing target number of partitions for
 * each of the graphs in the specified param metis_graphs
 */
void Partition(std::vector<MetisGraph*> metis_graphs,
               uint32_t max_coarsen_level,
               std::vector<uint32_t>& target_partitions) {
  assert(metis_graphs.size() == target_partitions.size());
  galois::StatTimer exec_timer("Total-Partition");
  exec_timer.start();

  galois::StatTimer timer_coarsing("Total-Coarsening");
  timer_coarsing.start();
  Coarsen(metis_graphs, max_coarsen_level, matching_policy);
  timer_coarsing.stop();

  galois::StatTimer timer_partitioning("Total-Partitioning-CoarsestGraph");
  timer_partitioning.start();
  PartitionCoarsestGraphs(metis_graphs, target_partitions);
  timer_partitioning.stop();

  galois::StatTimer timer_refining("Total-Refining");
  timer_refining.start();
  Refine(metis_graphs);
  timer_refining.stop();

  exec_timer.stop();
}

/**
 * Computes the edge cut value
 *
 * @param g Graph
 *
 * @returns The value of edge cut for graph g with the current partitioning
 * assignment
 */
uint32_t ComputingCut(GGraph& g) {
  galois::GAccumulator<uint32_t> edgecut;
  galois::do_all(
      galois::iterate(uint32_t{0}, g.hedges),
      [&](GNode n) {
        uint32_t first_edge_partition_id =
            g.getData(g.getEdgeDst(g.edge_begin(n))).GetPartition();
        bool edges_are_cut = false;
        for (auto e = g.edge_begin(n) + 1; e < g.edge_end(n); e++) {
          GNode dst             = g.getEdgeDst(e);
          uint32_t partition_id = g.getData(dst).GetPartition();
          if (partition_id != first_edge_partition_id) {
            edges_are_cut = true;
            break;
          }
        }
        if (edges_are_cut) {
          edgecut += 1;
        }
      },
      galois::loopname("Compute-CutSize"));
  return edgecut.reduce();
}

/**
 * Constructs a concatenated list of the hyperedges and nodes
 *
 * @param metis_graphs  Vector containing metis graphs
 * @param combined_edge_list Vector of concatenated list of the hyperedges
 * that needs to be constructed
 * @param combined_node_list Vector of concatenated list of the nodes
 * that needs to be constructed
 */
void ConstructCombinedLists(
    std::vector<MetisGraph*>& metis_graphs,
    std::vector<std::pair<uint32_t, uint32_t>>& combined_edge_list,
    std::vector<std::pair<uint32_t, uint32_t>>& combined_node_list) {
  uint32_t edge_index{0};
  uint32_t node_index{0};
  uint32_t num_partitions = metis_graphs.size();

  for (uint32_t i = 0; i < num_partitions; i++) {
    if (metis_graphs[i] != nullptr) {
      GGraph& g = *(metis_graphs[i]->GetGraph());

      for (GNode n = 0; n < g.hedges; n++) {
        combined_edge_list[edge_index] = std::make_pair(n, i);
        edge_index++;
      }
      for (GNode n = g.hedges; n < static_cast<uint32_t>(g.size()); n++) {
        combined_node_list[node_index] = std::make_pair(n, i);
        node_index++;
      }
    }
  }
}

/**
 * Create k partitions
 *
 * @param metis_graph Metis graph representing the original input graph
 */
void CreateKPartitions(MetisGraph& metis_graph) {
  galois::StatTimer initial_partition_timer("Initial-Partition");
  galois::StatTimer intermediate_partition_timer("Intermediate-Partition");
  galois::StatTimer update_graphtree_timer("Update-GraphTree");
  GGraph& graph            = *metis_graph.GetGraph();
  uint32_t total_num_nodes = graph.size();
  uint32_t num_hedges      = graph.hedges;
  std::vector<MetisGraph*> metis_graphs;
  metis_graphs.push_back(&metis_graph);

  // Number of partitions to create.
  std::vector<uint32_t> partitions_list;
  partitions_list.push_back(num_partitions);

  initial_partition_timer.start();
  // Initial partitioning into two cgraphs.
  Partition(metis_graphs, max_coarse_graph_size, partitions_list);
  initial_partition_timer.stop();

  // Calculate number of iterations/levels required.
  uint32_t num_levels = log2(static_cast<float>(num_partitions));
  uint32_t to_process_partitions[static_cast<uint32_t>(num_partitions)];
  std::memset(to_process_partitions, 0, num_partitions * sizeof(uint32_t));

  uint32_t second_partition               = (num_partitions + 1) / 2;
  to_process_partitions[0]                = second_partition;
  to_process_partitions[second_partition] = num_partitions / 2;

  galois::do_all(
      galois::iterate(num_hedges, total_num_nodes),
      [&](uint32_t n) {
        MetisNode& node            = graph.getData(n);
        uint32_t partition_of_node = node.GetPartition();
        // Change the second partition as the middle.
        if (partition_of_node == 1) {
          node.SetPartition(second_partition);
        }
      },
      galois::loopname("Initial-Assign-Partition"));

  std::set<uint32_t> current_level_indices;
  std::set<uint32_t> next_level_indices;
  current_level_indices.insert(0);
  current_level_indices.insert(second_partition);

  // FIXME(HCLee) we may replace this with a vector.
  std::vector<galois::InsertBag<GNode>> mem_nodes_of_parts;
  std::vector<galois::InsertBag<GNode>> mem_hedges_of_parts;
  mem_nodes_of_parts.resize(num_partitions);
  mem_hedges_of_parts.resize(num_partitions);

  std::vector<uint32_t> pgraph_index;
  pgraph_index.resize(num_partitions);

  for (uint32_t level = 1; level < num_levels; level++) {
    for (uint32_t i = 0; i < num_partitions; i++) {
      mem_nodes_of_parts[i].clear();
      mem_hedges_of_parts[i].clear();
    }

    // Assign index to each subgraph of the partitions.
    // Note that pgraph_index does not need to be reset.
    // It is always overwritten by the new index values.
    uint32_t index{0};
    for (uint32_t i : current_level_indices) {
      pgraph_index[i] = index++;
    }

    for (uint32_t n = num_hedges; n < total_num_nodes; n++) {
      MetisNode& node    = graph.getData(n);
      uint32_t partition = node.GetPartition();
      mem_nodes_of_parts[partition].emplace(n);
      // Assign graph index.
      node.SetGraphIndex(pgraph_index[partition]);
    }

    // 1): Graph index of the nodes is assigned.

    galois::do_all(
        galois::iterate(uint32_t{0}, num_hedges),
        [&](uint32_t hedge) {
          auto f_edge          = *(graph.edges(hedge).begin());
          GNode f_dst          = graph.getEdgeDst(f_edge);
          uint32_t f_partition = graph.getData(f_dst).GetPartition();
          bool flag{true};

          for (auto& fedge : graph.edges(hedge)) {
            GNode dst          = graph.getEdgeDst(fedge);
            uint32_t partition = graph.getData(dst).GetPartition();

            if (partition != f_partition) {
              flag = false;
              break;
            }
          }
          // The `flag` would be false if any member node
          // of the hyperedge are in the different partitions.
          // If the `flag` is true, then the current hedge is still
          // valid and partitionable.

          uint32_t h_partition{0};
          if (flag) {
            h_partition = f_partition;
          } else {
            h_partition = kInfPartition;
          }

          graph.getData(hedge).SetPartition(h_partition);
        },
        galois::steal(), galois::loopname("Set-CompleteHEdge-Partition"));

    // 2): Candidate partitions of the hedges is assigned.

    for (uint32_t h = 0; h < num_hedges; h++) {
      uint32_t partition = graph.getData(h).GetPartition();
      if (partition != kInfPartition) {
        mem_hedges_of_parts[partition].emplace(h);
        graph.getData(h).SetGraphIndex(pgraph_index[partition]);
      }
    }

    // 3): Graph indices of the hedges are assigned.

    // The currently processed number of partitions.
    uint32_t num_partitions = current_level_indices.size();
    std::vector<MetisGraph*> metis_graph_vec(num_partitions);
    std::vector<GGraph*> gr(num_partitions);
    std::vector<uint32_t> target_partitions(num_partitions);

    std::vector<uint32_t> num_hedges_per_partition(num_partitions);
    std::vector<uint32_t> num_hnodes_per_partition(num_partitions);

    galois::InsertBag<std::pair<uint32_t, uint32_t>> hedges_bag;
    galois::InsertBag<std::pair<uint32_t, uint32_t>> hnodes_bag;

    for (uint32_t i : current_level_indices) {
      if (to_process_partitions[i] > 1) {
        uint32_t index         = pgraph_index[i];
        metis_graph_vec[index] = new MetisGraph();
        gr[index]              = metis_graph_vec[index]->GetGraph();
      }
    }

    galois::do_all(
        galois::iterate(current_level_indices),
        [&](uint32_t i) {
          uint32_t ed = 0;
          for (GNode h : mem_hedges_of_parts[i]) {
            graph.getData(h).SetChildId(ed++);
          }

          uint32_t id = ed;
          // <partition no, # of member hedges>.
          hedges_bag.emplace(std::make_pair(i, ed));

          for (GNode n : mem_nodes_of_parts[i]) {
            graph.getData(n).SetChildId(id++);
          }
          // <partition no, # of member nodes>.
          hnodes_bag.emplace(std::make_pair(i, id - ed));
        },
        galois::steal(), galois::loopname("Set-Child-IDs"));

    // 4): Assign slot id for hyper edge and its member nodes.

    for (auto& pair : hedges_bag) {
      num_hedges_per_partition[pgraph_index[pair.first]] = pair.second;
    }

    for (auto& pair : hnodes_bag) {
      num_hnodes_per_partition[pgraph_index[pair.first]] = pair.second;
    }

    std::vector<galois::gstl::Vector<galois::PODResizeableArray<uint32_t>>>
        edges_ids(num_partitions);
    std::vector<LargeArrayUint64Ty> edges_prefixsum(num_partitions);

    // Construct a new graph.
    for (uint32_t i : current_level_indices) {
      uint32_t index = pgraph_index[i];
      uint32_t total_nodes =
          num_hedges_per_partition[index] + num_hnodes_per_partition[index];
      edges_ids[index].resize(total_nodes);
      edges_prefixsum[index].allocateInterleaved(total_nodes);
    }

    galois::do_all(
        galois::iterate(uint32_t{0}, num_hedges),
        [&](GNode src) {
          MetisNode& src_node = graph.getData(src);
          uint32_t partition  = src_node.GetPartition();
          if (partition == kInfPartition) {
            return;
          }
          uint32_t index = pgraph_index[partition];
          GNode slot_id  = src_node.GetChildId();

          for (auto& e : graph.edges(src)) {
            GNode dst         = graph.getEdgeDst(e);
            GNode dst_slot_id = graph.getData(dst).GetChildId();
            edges_ids[index][slot_id].push_back(dst_slot_id);
          }
        },
        galois::steal(), galois::chunk_size<kChunkSize>(),
        galois::loopname("Build-EdgeIds"));

    std::vector<galois::GAccumulator<uint64_t>> num_edges_acc(num_partitions);

    for (uint32_t i : current_level_indices) {
      uint32_t index = pgraph_index[i];
      for (uint32_t c = 0; c < num_hedges_per_partition[index]; c++) {
        edges_prefixsum[index][c] = edges_ids[index][c].size();
        num_edges_acc[index] += edges_prefixsum[index][c];
      }
    }

    for (uint32_t i : current_level_indices) {
      uint32_t index = pgraph_index[i];
      uint64_t edges = num_edges_acc[index].reduce();
      uint32_t ipart_num_nodes =
          num_hedges_per_partition[index] + num_hnodes_per_partition[index];
      GGraph& cur_graph = *gr[index];
      for (uint32_t c = 1; c < ipart_num_nodes; ++c) {
        edges_prefixsum[index][c] += edges_prefixsum[index][c - 1];
      }

      cur_graph.constructFrom(ipart_num_nodes, edges,
                              std::move(edges_prefixsum[index]),
                              edges_ids[index]);
      cur_graph.hedges = num_hedges_per_partition[index];
      cur_graph.hnodes = num_hnodes_per_partition[index];
    }

    for (uint32_t i : current_level_indices) {
      uint32_t index    = pgraph_index[i];
      GGraph& cur_graph = *gr[index];
      InitNodes(cur_graph, cur_graph.hedges);
    }

    for (uint32_t i : current_level_indices) {
      target_partitions[pgraph_index[i]] = to_process_partitions[i];
    }

    intermediate_partition_timer.start();
    Partition(metis_graph_vec, max_coarse_graph_size, target_partitions);
    intermediate_partition_timer.stop();

    update_graphtree_timer.start();
    for (uint32_t i : current_level_indices) {
      uint32_t index  = pgraph_index[i];
      MetisGraph* mcg = metis_graph_vec[index];

      if (mcg == nullptr) {
        continue;
      }

      while (mcg->GetCoarsenedGraph() != nullptr) {
        mcg = mcg->GetCoarsenedGraph();
      }

      while (mcg->GetParentGraph() != nullptr &&
             mcg->GetParentGraph()->GetParentGraph() != nullptr) {
        mcg = mcg->GetParentGraph();
        delete mcg->GetCoarsenedGraph();
      }
    }
    update_graphtree_timer.stop();

    for (uint32_t i : current_level_indices) {
      uint32_t tmp                                = to_process_partitions[i];
      uint32_t second_partition                   = (tmp + 1) / 2;
      to_process_partitions[i]                    = second_partition;
      to_process_partitions[i + second_partition] = (tmp) / 2;
      next_level_indices.insert(i);
      next_level_indices.insert(i + second_partition);

      galois::do_all(
          galois::iterate(mem_nodes_of_parts[i]),
          [&](GNode src) {
            MetisNode& src_data = graph.getData(src);
            GNode n             = src_data.GetChildId();
            uint32_t partition = gr[pgraph_index[i]]->getData(n).GetPartition();
            if (partition == 0) {
              src_data.SetPartition(i);
            } else if (partition == 1) {
              src_data.SetPartition(i + second_partition);
            }
          },
          galois::loopname("Reassign-Partition"));
    }

    for (uint32_t i : current_level_indices) {
      MetisGraph* mcg = metis_graph_vec[pgraph_index[i]];
      delete mcg;
    }

    current_level_indices = next_level_indices;
    next_level_indices.clear();
  }
  galois::runtime::reportStat_Single("BiPart", "Edge-Cut", ComputingCut(graph));
  galois::runtime::reportStat_Single("BiPart", "Partitions",
                                     static_cast<uint32_t>(num_partitions));
}

/**
 * Main Function
 */
int main(int argc, char** argv) {
  galois::SharedMemSys G;
  LonestarStart(argc, argv, name, desc, url, &input_file);

  galois::StatTimer total_time("TimerTotal");
  total_time.start();
  galois::StatTimer create_partition_time("Create-Partitions");

  if (!hyper_metis_graph) {
    GALOIS_LOG_FATAL(
        "This application requires a HyperGraph Metis input;"
        " please use the -hyperMetisGraph flag "
        " to indicate the input is a valid HyperGraph Metis format "
        "(http://glaros.dtc.umn.edu/gkhome/fetch/sw/hmetis/manual.pdf).");
  }

  MetisGraph metis_graph;
  GGraph& graph = *metis_graph.GetGraph();

  ConstructGraph(graph, input_file, skip_lone_hedges);

  uint32_t total_num_nodes = graph.size();
  uint32_t num_hedges      = graph.hedges;
  GraphStat(graph);

  galois::preAlloc(galois::runtime::numPagePoolAllocTotal() * 20);
  galois::reportPageAlloc("MeminfoPre");

  create_partition_time.start();
  CreateKPartitions(metis_graph);
  create_partition_time.stop();

  galois::reportPageAlloc("MeminfoPost");
  total_time.stop();

  if (output) {
    galois::gPrint("Number of hyper-edges: ", num_hedges, "\n");
    galois::gPrint("Total graph size (include hyper-edges): ", total_num_nodes,
                   "\n");
    std::vector<uint32_t> parts(total_num_nodes - num_hedges);
    std::vector<uint32_t> IDs(total_num_nodes - num_hedges);

    for (GNode n = num_hedges; n < total_num_nodes; n++) {
      parts[n - num_hedges] = graph.getData(n).GetPartition();
      IDs[n - num_hedges]   = n - num_hedges + 1;
    }

    std::ofstream output_file(output_file_name.c_str());

    for (uint32_t i = 0; i < static_cast<uint32_t>(parts.size()); i++)
      output_file << IDs[i] << " " << parts[i] << "\n";

    output_file.close();
  }
  return 0;
}
