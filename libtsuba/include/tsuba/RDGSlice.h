#ifndef KATANA_LIBTSUBA_TSUBA_RDGSLICE_H_
#define KATANA_LIBTSUBA_TSUBA_RDGSLICE_H_

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "katana/Result.h"
#include "katana/URI.h"
#include "katana/config.h"
#include "tsuba/FileView.h"
#include "tsuba/tsuba.h"

namespace tsuba {

class RDGManifest;
class RDGCore;

/// A contiguous piece of an RDG
class KATANA_EXPORT RDGSlice {
public:
  RDGSlice(const RDGSlice& no_copy) = delete;
  RDGSlice& operator=(const RDGSlice& no_copy) = delete;

  ~RDGSlice();
  RDGSlice(RDGSlice&& other) noexcept;
  RDGSlice& operator=(RDGSlice&& other) noexcept;

  struct SliceArg {
    std::pair<uint64_t, uint64_t> node_range;
    std::pair<uint64_t, uint64_t> edge_range;
    uint64_t topo_off;
    uint64_t topo_size;
  };

  static katana::Result<RDGSlice> Make(
      RDGHandle handle, const SliceArg& slice,
      const std::optional<std::vector<std::string>>& node_props = std::nullopt,
      const std::optional<std::vector<std::string>>& edge_props = std::nullopt);

  static katana::Result<RDGSlice> Make(
      const std::string& rdg_manifest_path, const SliceArg& slice,
      const std::optional<std::vector<std::string>>& node_props = std::nullopt,
      const std::optional<std::vector<std::string>>& edge_props = std::nullopt);

  const std::shared_ptr<arrow::Table>& node_properties() const;
  const std::shared_ptr<arrow::Table>& edge_properties() const;
  const FileView& topology_file_storage() const;

private:
  static katana::Result<RDGSlice> Make(
      const RDGManifest& manifest, const std::vector<std::string>* node_props,
      const std::vector<std::string>* edge_props, const SliceArg& slice);

  RDGSlice(std::unique_ptr<RDGCore>&& core);

  katana::Result<void> DoMake(
      const std::optional<std::vector<std::string>>& node_props,
      const std::optional<std::vector<std::string>>& edge_props,
      const katana::Uri& metadata_dir, const SliceArg& slice);

  //
  // Data
  //

  std::unique_ptr<RDGCore> core_;
};

}  // namespace tsuba

#endif
