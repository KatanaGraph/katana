#ifndef GALOIS_TOOLS_GRAPH_CONVERT_GRAPH_PROPERTIES_CONVERT_MONGODB_H_
#define GALOIS_TOOLS_GRAPH_CONVERT_GRAPH_PROPERTIES_CONVERT_MONGODB_H_

#include <mongoc/mongoc.h>
#include <bson/bson.h>

#include "graph-properties-convert.h"

namespace galois {

void HandleNodeDocumentMongoDB(PropertyGraphBuilder* builder, const bson_t* doc,
                               const std::string& collection_name);
void HandleEdgeDocumentMongoDB(PropertyGraphBuilder*, const bson_t* doc,
                               const std::string& collection_name);

GraphComponents ConvertMongoDB(const std::string& db_name,
                               const std::string& mapping,
                               const size_t chunk_size);
void GenerateMappingMongoDB(const std::string& db_name,
                            const std::string& outfile);

} // end namespace galois

#endif
