#ifndef KATANA_LIBTSUBA_TSUBA_RDGSLICE_H_
#define KATANA_LIBTSUBA_TSUBA_RDGSLICE_H_

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "katana/Result.h"
#include "katana/URI.h"
#include "katana/config.h"
#include "tsuba/FileView.h"
#include "tsuba/RDGLineage.h"
#include "tsuba/RDGTopology.h"
#include "tsuba/tsuba.h"

namespace tsuba {

class RDGManifest;
class RDGCore;

/// A read-only contiguous piece of a partition.
///
/// This class owns a contiguous slice of nodes from the associated default CSR
/// topology. It also owns the outgoing edges associated with those nodes.
/// Because of the structure of CSR, those edges form a contiguous slice.
/// As a result, it also owns a contiguous slice from each of
/// the node and edge property arrays and the node and edge type id arrays.
///
/// A typical use would be loading an unpartitioned graph on multiple hosts.
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

  // metadata sorts of things
  const katana::Uri& rdg_dir() const;
  uint32_t partition_id() const;

  // properties and friends
  std::shared_ptr<arrow::Schema> full_node_schema() const;
  std::shared_ptr<arrow::Schema> full_edge_schema() const;
  const std::shared_ptr<arrow::Table>& node_properties() const;
  const std::shared_ptr<arrow::Table>& edge_properties() const;

  // topology and friends
  const FileView& topology_file_storage() const;

  // optional partition metadata
  const std::vector<std::shared_ptr<arrow::ChunkedArray>>& master_nodes()
      const {
    return master_nodes_;
  }
  const std::vector<std::shared_ptr<arrow::ChunkedArray>>& mirror_nodes()
      const {
    return mirror_nodes_;
  }
  const std::shared_ptr<arrow::ChunkedArray>& host_to_owned_global_node_ids()
      const {
    return host_to_owned_global_node_ids_;
  }
  const std::shared_ptr<arrow::ChunkedArray>& host_to_owned_global_edge_ids()
      const {
    return host_to_owned_global_edge_ids_;
  }
  const std::shared_ptr<arrow::ChunkedArray>& local_to_user_id() const {
    return local_to_user_id_;
  }
  const std::shared_ptr<arrow::ChunkedArray>& local_to_global_id() const {
    return local_to_global_id_;
  }

  // type info
  /// Determine if the EntityTypeIDs are stored in properties, or outside
  /// in their own dedicated structures
  bool IsEntityTypeIDsOutsideProperties() const;
  const FileView& node_entity_type_id_array_file_storage() const;
  const FileView& edge_entity_type_id_array_file_storage() const;
  katana::Result<katana::EntityTypeManager> node_entity_type_manager() const;
  katana::Result<katana::EntityTypeManager> edge_entity_type_manager() const;

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
  // NB: we intentionally do not include a property cache here because that will
  // complicate the property cache logic; one could be added in the future if it
  // seems necessary

  void InitArrowVectors();
  std::vector<std::shared_ptr<arrow::ChunkedArray>> mirror_nodes_;
  std::vector<std::shared_ptr<arrow::ChunkedArray>> master_nodes_;
  std::shared_ptr<arrow::ChunkedArray> host_to_owned_global_node_ids_;
  std::shared_ptr<arrow::ChunkedArray> host_to_owned_global_edge_ids_;
  std::shared_ptr<arrow::ChunkedArray> local_to_user_id_;
  std::shared_ptr<arrow::ChunkedArray> local_to_global_id_;

  // How this graph was derived from the previous version
  RDGLineage lineage_;
};

}  // namespace tsuba

#endif
