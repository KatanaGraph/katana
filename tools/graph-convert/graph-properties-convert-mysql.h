#ifndef KATANA_TOOLS_GRAPH_CONVERT_GRAPH_PROPERTIES_CONVERT_MYSQL_H_
#define KATANA_TOOLS_GRAPH_CONVERT_GRAPH_PROPERTIES_CONVERT_MYSQL_H_

#include "katana/BuildGraph.h"

namespace katana {

GraphComponents ConvertMysql(
    const std::string& db_name, const std::string& mapping,
    const size_t chunk_size, const std::string& host, const std::string& user);
void GenerateMappingMysql(
    const std::string& db_name, const std::string& outfile,
    const std::string& host, const std::string& user);

}  // end namespace katana

#endif
