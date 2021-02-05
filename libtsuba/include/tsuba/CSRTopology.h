#ifndef KATANA_LIBTSUBA_TSUBA_CSRTOPOLOGY_H_
#define KATANA_LIBTSUBA_TSUBA_CSRTOPOLOGY_H_

#include <cstdint>

#include "katana/BitMath.h"

namespace tsuba {

/// The file format for RDG topologies is CSR (Compressed Sparse Row). These
/// files used to have the file extension .gr (a name tradition continued here)
/// The structs in this file describe how these GR files are laid out

/// The metadata block at the head of every CSR file
struct CSRTopologyHeader {
  uint64_t version{0};
  uint64_t edge_type_size{0};
  uint64_t num_nodes{0};
  uint64_t num_edges{0};
};

/// The header and out index array of every CSR file. The length of out_indexes
/// depends on the number of nodes.
struct CSRTopologyPrefix {
  CSRTopologyHeader header;
  uint64_t out_indexes[];  // NOLINT needed for layout
};

constexpr uint64_t
CSRTopologyFileSize(const CSRTopologyHeader& header) {
  uint64_t edge_size =
      header.version == 1 ? sizeof(uint32_t) : sizeof(uint64_t);
  return sizeof(header) + ((header.num_nodes) * sizeof(uint64_t)) +
         katana::AlignUp<uint64_t>(header.num_edges * edge_size) +
         (header.num_edges * header.edge_type_size);
}

}  // namespace tsuba

#endif
