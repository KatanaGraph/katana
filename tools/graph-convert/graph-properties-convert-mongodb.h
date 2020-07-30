#ifndef GALOIS_TOOLS_GRAPH_CONVERT_GRAPH_PROPERTIES_CONVERT_MONGODB_H_
#define GALOIS_TOOLS_GRAPH_CONVERT_GRAPH_PROPERTIES_CONVERT_MONGODB_H_

#include "graph-properties-convert.h"

namespace galois {

GraphComponents ConvertMongoDB(const std::string& db_name,
                               const std::string& mapping,
                               const size_t chunk_size);
void GenerateMappingMongoDB(const std::string& db_name,
                            const std::string& outfile);

} // end namespace galois

#endif
