#ifndef KATANA_LIBGRAPH_KATANA_GRAPHML_H_
#define KATANA_LIBGRAPH_KATANA_GRAPHML_H_

#include <libxml/xmlreader.h>

#include "katana/BuildGraph.h"

namespace katana {

/// ConvertGraphML converts a GraphML file into katana form
///
/// \param infilename Path to source graphml file
/// \param chunk_size Chunk size for in memory representations during conversion.
///     Generally this term can be ignored, but it can be decreased to reduce
///     memory usage when converting large inputs
/// \param verbose If true, print graph data to the standard out while
///     converting.
/// \returns A collection of Arrow tables of node properties/labels, edge
///     properties/types, and CSR topology
KATANA_EXPORT katana::Result<katana::GraphComponents> ConvertGraphML(
    const std::string& infilename, size_t chunk_size = 25000,
    bool verbose = false);

/// ConvertGraphML converts a GraphML file into katana form
///
/// \param reader xml text reader object for the document
/// \param chunk_size Chunk size for in memory representations during conversion.
///     Generally this term can be ignored, but it can be decreased to reduce
///     memory usage when converting large inputs
/// \param verbose If true, print graph data to the standard out while
///     converting.
/// \returns A collection of Arrow tables of node properties/labels, edge
///     properties/types, and CSR topology
KATANA_EXPORT katana::Result<katana::GraphComponents> ConvertGraphML(
    xmlTextReaderPtr reader, size_t chunk_size = 25000, bool verbose = false);

}  // end namespace katana

#endif
