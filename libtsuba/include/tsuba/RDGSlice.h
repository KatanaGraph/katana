#ifndef GALOIS_LIBTSUBA_TSUBA_RDGSLICE_H_
#define GALOIS_LIBTSUBA_TSUBA_RDGSLICE_H_

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "galois/Result.h"
#include "galois/config.h"
#include "tsuba/RDGCore.h"
#include "tsuba/RDGPartHeader.h"
#include "tsuba/tsuba.h"

namespace tsuba {

/// A contiguous piece of an RDG
class GALOIS_EXPORT RDGSlice {
public:
  struct SliceArg {
    std::pair<uint64_t, uint64_t> node_range;
    std::pair<uint64_t, uint64_t> edge_range;
    uint64_t topo_off;
    uint64_t topo_size;
  };

  static galois::Result<RDGSlice> Load(
      RDGHandle handle, const SliceArg& slice,
      const std::vector<std::string>* node_props = nullptr,
      const std::vector<std::string>* edge_props = nullptr);

  static galois::Result<RDGSlice> Load(
      const std::string& rdg_meta_path, const SliceArg& slice,
      const std::vector<std::string>* node_props = nullptr,
      const std::vector<std::string>* edge_props = nullptr);

  const std::shared_ptr<arrow::Table>& node_table() const {
    return core_.node_table();
  }
  const std::shared_ptr<arrow::Table>& edge_table() const {
    return core_.edge_table();
  }
  const FileView& topology_file_storage() const {
    return core_.topology_file_storage();
  }

private:
  static galois::Result<RDGSlice> Make(
      const RDGMeta& meta, const std::vector<std::string>* node_props,
      const std::vector<std::string>* edge_props, const SliceArg& slice);

  RDGSlice(RDGPartHeader&& part_header) : core_(std::move(part_header)) {}

  galois::Result<void> DoLoad(
      const galois::Uri& metadata_dir, const SliceArg& slice);

  //
  // Data
  //

  RDGCore core_;
};

}  // namespace tsuba

#endif
