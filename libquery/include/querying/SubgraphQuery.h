#pragma once

#include "galois/graphs/QueryGraph.h"

/**
 * Get subgraph matches from data graph given some query graph.
 */
size_t subgraphQuery(QueryGraph& query_graph, QueryGraph& data_graph);