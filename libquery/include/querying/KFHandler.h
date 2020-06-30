#ifndef KATANA_KF_HANDLER
#define KATANA_KF_HANDLER
/**
 * Copyright (C) 2020, KatanaGraph
 */

/**
 * @file KFHandler.h
 *
 * Declaration of functions to load Katana Form on disk into the in memory
 * graph used by the Cypher querying engine.
 */

#include <memory>
#include "galois/graphs/PropertyFileGraph.h"
#include "galois/graphs/AttributedGraph.h"

// TODO better namespace name later
namespace querying {

// defining some typedefs for use to make code less verbose
using PFGPointer         = std::shared_ptr<galois::graphs::PropertyFileGraph>;
using ArrowSchemaPointer = std::shared_ptr<arrow::Schema>;
using ArrowFieldPointer  = std::shared_ptr<arrow::Field>;
using ArrowArrayPointer  = std::shared_ptr<arrow::Array>;
using ArrowChunkedArrayPointer = std::shared_ptr<arrow::ChunkedArray>;

/**
 * Loads KatanaForm on disk into a graph object compatible with the Cypher
 * engine.
 * @param att_graph Graph object to load into
 * @param Path to the meta file for KatanaForm that designates how to interpret
 * the different files in Katana form
 */
void loadKatanaFormToCypherGraph(galois::graphs::AttributedGraph& att_graph,
                                 const std::string metadata_path);

} // namespace querying

#endif // ifdef guard ending
