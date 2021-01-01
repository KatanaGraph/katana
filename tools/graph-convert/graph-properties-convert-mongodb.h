#ifndef KATANA_TOOLS_GRAPH_CONVERT_GRAPH_PROPERTIES_CONVERT_MONGODB_H_
#define KATANA_TOOLS_GRAPH_CONVERT_GRAPH_PROPERTIES_CONVERT_MONGODB_H_

#include <bson/bson.h>
#include <mongoc/mongoc.h>

#include "katana/BuildGraph.h"

namespace katana {

void HandleNodeDocumentMongoDB(
    PropertyGraphBuilder* builder, const bson_t* doc,
    const std::string& collection_name);
void HandleEdgeDocumentMongoDB(
    PropertyGraphBuilder*, const bson_t* doc,
    const std::string& collection_name);

GraphComponents ConvertMongoDB(
    const std::string& db_name, const std::string& mapping,
    const size_t chunk_size);
void GenerateMappingMongoDB(
    const std::string& db_name, const std::string& outfile);

}  // end namespace katana

#endif
