#ifndef GALOIS_TOOLS_GRAPH_CONVERT_GRAPH_PROPERTIES_CONVERT_GRAPHML_H_
#define GALOIS_TOOLS_GRAPH_CONVERT_GRAPH_PROPERTIES_CONVERT_GRAPHML_H_

#include "galois/BuildGraph.h"

namespace galois {

GraphComponents ConvertGraphML(
    const std::string& input_filename, size_t chunk_size);

}  // end namespace galois

#endif
