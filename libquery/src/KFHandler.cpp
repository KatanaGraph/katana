/**
 * Copyright (C) 2020, KatanaGraph
 */

/**
 * @file KFHandler.cpp
 *
 * Definition of functions to load Katana Form on disk into the in memory
 * graph used by the Cypher querying engine.
 */

#include "querying/KFHandler.h"
#include "galois/Logging.h"

namespace querying {

// helper functions for graph loading contained in this private namespace
namespace {

//! Gets the node label columns + saves node attributes to the attributed graph
//! object
galois::gstl::Vector<int>
processNodeAttributes(const PFGPointer& pfg,
                      galois::graphs::AttributedGraph& att_graph) {
  // designates which columns are labels
  galois::gstl::Vector<int> node_label_columns;

  // TODO(lhoang) for now any boolean property is considered a label and not a
  // property; need metadata to distinguish between the two
  const ArrowSchemaPointer& pfg_node_schema = pfg->node_schema();
  // count the number of labels in the schema
  for (int i = 0; i < pfg_node_schema->num_fields(); i++) {
    const ArrowFieldPointer& current_field = pfg_node_schema->field(i);

    // TODO(lhoang) for now a bool is considered a label; some metadata will be
    // used in the future
    if (current_field->type()->Equals(arrow::boolean())) {
      node_label_columns.push_back(i);
    } else {
      // attribute; add it to the atrribute map
      att_graph.insertNodeArrowAttribute(current_field->name(),
                                         pfg->NodeProperty(i));
    }
  }

  // TODO currently a restriction on number of node/edge labels due to using
  // bits to represent them; best would be to use a dynamic bitset or something
  // similar in the future
  if (node_label_columns.size() > 32) {
    GALOIS_LOG_FATAL("Number of node label types cannot be more than 32: {}",
                     node_label_columns.size());
  }
  galois::gInfo("Num node labels is ", node_label_columns.size());

  return node_label_columns;
}

//! Gets the edge label columns + saves edge attributes to the attributed graph
//! object
galois::gstl::Vector<int>
processEdgeAttributes(const PFGPointer& pfg,
                      galois::graphs::AttributedGraph& att_graph) {
  galois::gstl::Vector<int> edge_label_columns;

  // count number of edge labels
  const ArrowSchemaPointer& pfg_edge_schema = pfg->edge_schema();

  // count the number of labels in the schema
  for (int i = 0; i < pfg_edge_schema->num_fields(); i++) {
    const ArrowFieldPointer& current_field = pfg_edge_schema->field(i);
    // TODO(lhoang) for now a bool is considered a label; some metadata will be
    // used in the future
    if (current_field->type()->Equals(arrow::boolean())) {
      edge_label_columns.push_back(i);
    } else {
      // attribute; add it to the atrribute map
      att_graph.insertEdgeArrowAttribute(current_field->name(),
                                         pfg->EdgeProperty(i));
    }
  }

  // TODO currently a restriction on number of node/edge labels due to using
  // bits to represent them; best would be to use a dynamic bitset or something
  // similar in the future
  if (edge_label_columns.size() > 32) {
    GALOIS_LOG_FATAL("Number of edge label types cannot be more than 32: {}",
                     edge_label_columns.size());
  }

  return edge_label_columns;
}

//! Loop through arrow columns corresponding to node labels and set/construct
//! them in the attributed graph
void constructNodeLabels(const PFGPointer& pfg,
                         const galois::gstl::Vector<int>& node_label_columns,
                         galois::graphs::AttributedGraph& att_graph) {
  uint32_t current_node_label_count         = 0;
  const ArrowSchemaPointer& pfg_node_schema = pfg->node_schema();
  // loop through columns we know correspond to labels
  for (int i : node_label_columns) {
    const ArrowFieldPointer& current_field = pfg_node_schema->field(i);
    att_graph.setNodeLabelMetadata(current_node_label_count,
                                   current_field->name().c_str());
    const ArrowChunkedArrayPointer& current_label_column = pfg->NodeProperty(i);

    size_t num_with_label = 0;
    // offset required to deal with multiple chunks in a chunked array
    int64_t current_array_offset = 0;
    // loop through the chunked boolean array (we know for sure it's boolean)
    for (const ArrowArrayPointer& chunk : current_label_column->chunks()) {
      auto boolean_array = std::static_pointer_cast<arrow::BooleanArray>(chunk);
      int64_t boolean_array_size = boolean_array->length();

      for (int64_t cur_node = 0; cur_node < boolean_array_size; cur_node++) {
        // if boolean is true, it means the node has the label
        if (boolean_array->Value(cur_node)) {
          // account for chunk offset
          int64_t node_to_set = cur_node + current_array_offset;
          // current node label count is the bit assigned to the label
          att_graph.addToNodeLabel(node_to_set, current_node_label_count);
          num_with_label++;
        }
      }
      // increment offset for the next chunk
      current_array_offset += boolean_array_size;
    }

    galois::gInfo("Number of nodes with the label ", current_field->name(),
                  " with bit ", current_node_label_count, " is ",
                  num_with_label);

    // increment for the next label
    current_node_label_count++;
  }
}

//! Loop through arrow columns corresponding to edge labels and set/construct
//! them in the attributed graph
void constructEdgeLabels(const PFGPointer& pfg,
                         const galois::gstl::Vector<int>& edge_label_columns,
                         galois::graphs::AttributedGraph& att_graph) {
  // edges next
  uint32_t current_edge_label_count         = 0;
  const ArrowSchemaPointer& pfg_edge_schema = pfg->edge_schema();
  // loop through columns we know correspond to labels
  for (int i : edge_label_columns) {
    const ArrowFieldPointer& current_field = pfg_edge_schema->field(i);
    att_graph.setEdgeLabelMetadata(current_edge_label_count,
                                   current_field->name().c_str());
    const ArrowChunkedArrayPointer& current_label_column = pfg->EdgeProperty(i);

    size_t num_with_label = 0;
    // offset required to deal with multiple chunks in a chunked array
    int64_t current_array_offset = 0;
    // loop through the chunked boolean array (we know for sure it's boolean)
    for (const std::shared_ptr<arrow::Array>& chunk :
         current_label_column->chunks()) {
      auto boolean_array = std::static_pointer_cast<arrow::BooleanArray>(chunk);
      int64_t boolean_array_size = boolean_array->length();

      for (int64_t cur_edge = 0; cur_edge < boolean_array_size; cur_edge++) {
        // if boolean is true, it means the edge has the label
        if (boolean_array->Value(cur_edge)) {
          // account for chunk offset
          int64_t edge_to_set = cur_edge + current_array_offset;
          // current edge label count is the bit assigned to the label
          att_graph.addToEdgeLabel(edge_to_set, current_edge_label_count);
          num_with_label++;
        }
      }
      // increment offset for the next chunk
      current_array_offset += boolean_array_size;
    }

    galois::gInfo("Number of edges with the label ", current_field->name(),
                  " with bit ", current_edge_label_count, " is ",
                  num_with_label);

    // increment for the next label
    current_edge_label_count++;
  }
}

//! Construct the attributed graph's topology using the GraphTopology object
//! from the PFG
void constructGraphTopology(const galois::graphs::GraphTopology& pfg_topo,
                            galois::graphs::AttributedGraph& att_graph) {
  uint64_t pfg_num_nodes = pfg_topo.num_nodes();
  uint64_t pfg_num_edges = pfg_topo.num_edges();

  // set the out indices of the underlying CSR graph
  galois::do_all(
      galois::iterate((uint64_t)0, pfg_num_nodes),
      [&](uint64_t n) {
        att_graph.fixEndEdge(n, pfg_topo.out_indices->Value(n));
      },
      galois::loopname("Katana2CypherFixEndEdge"), galois::no_stats());

  // set edge destinations
  galois::do_all(
      galois::iterate((uint64_t)0, pfg_num_edges),
      [&](uint64_t n) {
        att_graph.fixEndEdge(n, pfg_topo.out_indices->Value(n));
      },
      galois::loopname("Katana2CypherFixEndEdge"), galois::no_stats());
}

} // namespace

void loadKatanaFormToCypherGraph(galois::graphs::AttributedGraph& att_graph,
                                 const std::string metadata_path) {
  // load property file graph from metadata
  auto pfg_result = galois::graphs::PropertyFileGraph::Make(metadata_path);
  if (!pfg_result) {
    GALOIS_LOG_FATAL("failed to load pfg: {}", pfg_result.error());
  }

  std::shared_ptr<galois::graphs::PropertyFileGraph> pfg = pfg_result.value();
  const galois::graphs::GraphTopology& pfg_topo          = pfg->topology();
  // first, allocate memory for the att_graph: needs num nodes/edges and
  // different kinds of labels for nodes/edges
  uint64_t pfg_num_nodes = pfg_topo.num_nodes();
  uint64_t pfg_num_edges = pfg_topo.num_edges();
  galois::gInfo("Num nodes is ", pfg_num_nodes, " edges is ", pfg_num_edges);

  // process node/edge attributes + get indices which contain the labels
  // and relationship types (labels differ from properties/attributes
  galois::gstl::Vector<int> node_label_columns =
      processNodeAttributes(pfg, att_graph);
  galois::gstl::Vector<int> edge_label_columns =
      processEdgeAttributes(pfg, att_graph);

  // allocate memory for the cypher graph given nodes, edges, and label count
  att_graph.allocateGraphLDBC(pfg_num_nodes, pfg_num_edges,
                              node_label_columns.size(),
                              edge_label_columns.size());

  // Attributes already handled during first pass above: now need to handle
  // node/edge labels by setting bits on CSR graph + setting up the metadata in
  // the graph end
  constructNodeLabels(pfg, node_label_columns, att_graph);
  constructEdgeLabels(pfg, edge_label_columns, att_graph);

  // handle the topology (fix end edge, destinations)
  // TODO possible to not use LC_CSR backend at all; for now, use it to get
  // something working/proof of concept
  constructGraphTopology(pfg_topo, att_graph);

  // final setup; construct backward edges
  att_graph.graph.constructAndSortIndex();
}

} // namespace querying
