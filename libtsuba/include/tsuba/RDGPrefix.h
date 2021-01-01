#ifndef KATANA_LIBTSUBA_TSUBA_RDGPREFIX_H_
#define KATANA_LIBTSUBA_TSUBA_RDGPREFIX_H_

#include <cstdint>

#include "tsuba/FileView.h"
#include "tsuba/tsuba.h"

namespace tsuba {

class RDGMeta;

/// An RDGPrefix loads the header information from the topology CSR, this is
/// used by the partitioner to avoid downloading the whole RDG to make
/// partitioning decisions
class KATANA_EXPORT RDGPrefix {
  struct GRHeader {
    uint64_t version{0};
    uint64_t edge_type_size{0};
    uint64_t num_nodes{0};
    uint64_t num_edges{0};
  };

  /// includes the header and the list of indexes
  struct GRPrefix {
    GRHeader header;
    uint64_t
        out_indexes[]; /* NOLINT length is defined by num_nodes_ in header */
  };

public:
  static katana::Result<RDGPrefix> Make(RDGHandle handle);

  uint64_t num_nodes() const { return prefix_->header.num_nodes; }
  uint64_t num_edges() const { return prefix_->header.num_edges; }
  uint64_t version() const { return prefix_->header.version; }
  uint64_t view_offset() const { return view_offset_; }

  const uint64_t* out_indexes() const {
    return static_cast<const uint64_t*>(prefix_->out_indexes);
  }

  const uint64_t& operator[](uint64_t n) const {
    assert(n < num_nodes());
    return prefix_->out_indexes[n];
  }

  std::vector<uint64_t> range(uint64_t first, uint64_t second) const {
    const uint64_t* out_indexes = prefix_->out_indexes;
    return std::vector<uint64_t>(out_indexes + first, out_indexes + second);
  }

private:
  RDGPrefix(FileView&& prefix_storage, uint64_t view_offset)
      : prefix_storage_(std::move(prefix_storage)),
        view_offset_(view_offset),
        prefix_(prefix_storage_.ptr<GRPrefix>()) {}

  RDGPrefix() = default;

  FileView prefix_storage_;
  uint64_t view_offset_;
  const GRPrefix* prefix_{nullptr};

  static katana::Result<RDGPrefix> DoMakePrefix(const RDGMeta& meta);
};

}  // namespace tsuba

#endif
