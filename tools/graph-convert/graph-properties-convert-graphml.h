#ifndef KATANA_TOOLS_GRAPH_CONVERT_GRAPH_PROPERTIES_CONVERT_GRAPHML_H_
#define KATANA_TOOLS_GRAPH_CONVERT_GRAPH_PROPERTIES_CONVERT_GRAPHML_H_

#include "katana/BuildGraph.h"

namespace katana {

GraphComponents ConvertGraphML(
    const std::string& input_filename, size_t chunk_size);

}  // end namespace katana

#endif
