#ifndef KATANA_LIBTSUBA_TSUBA_RDG_H_
#define KATANA_LIBTSUBA_TSUBA_RDG_H_

#include <cstdint>
#include <memory>
#include <string>

#include <arrow/api.h>
#include <arrow/chunked_array.h>
#include <nlohmann/json.hpp>

#include "katana/Result.h"
#include "katana/URI.h"
#include "katana/config.h"
#include "tsuba/Errors.h"
#include "tsuba/FileFrame.h"
#include "tsuba/FileView.h"
#include "tsuba/PartitionMetadata.h"
#include "tsuba/RDGLineage.h"
#include "tsuba/ReadGroup.h"
#include "tsuba/WriteGroup.h"
#include "tsuba/tsuba.h"

namespace tsuba {

class RDGManifest;
class RDGCore;
struct PropStorageInfo;

struct KATANA_EXPORT RDGLoadOptions {
  /// Which partition of the RDG on storage should be loaded
  /// nullopt means the partition associated with the current host's ID will be
  /// loaded
  std::optional<uint32_t> partition_id_to_load;
  /// List of node properties that should be loaded
  /// nullptr means all node properties will be loaded
  const std::vector<std::string>* node_properties{nullptr};
  /// List of edge properties that should be loaded
  /// nullptr means all edge properties will be loaded
  const std::vector<std::string>* edge_properties{nullptr};
};

class KATANA_EXPORT RDG {
public:
  enum RDGVersioningPolicy { RetainVersion = 0, IncrementVersion };
  RDG(const RDG& no_copy) = delete;
  RDG& operator=(const RDG& no_dopy) = delete;

  RDG();
  ~RDG();
  RDG(RDG&& other) noexcept;
  RDG& operator=(RDG&& other) noexcept;

  /// Perform some checks on assumed invariants
  katana::Result<void> Validate() const;

  /// Determine if two RDGs are Equal //TODO (yasser): verify that this now works with
  //views
  bool Equals(const RDG& other) const;

  /// @brief Store RDG with lineage based on command line and update version based on the versioning policy.
  /// @param handle :: handle indicating where to store RDG
  /// @param command_line :: added to metadata to track lineage of RDG
  /// @param versioning_action :: can be set to 'RDG::RDGVersioningPolicy::IncrementVersion' or
  /// @param ff :: if ff is not nullptr, it is persisted as the topology for this RDG.
  /// 'RDG::RDGVersioningPolicy::RetainVersion' to indicate whether RDG version is
  katana::Result<void> Store(
      RDGHandle handle, const std::string& command_line,
      RDGVersioningPolicy versioning_action, std::unique_ptr<FileFrame> ff);

  /// @brief Store new version of the RDG with lineage based on command line.
  /// @param handle :: handle indicating where to store RDG
  /// @param command_line :: added to metadata to track lineage of RDG
  /// @param ff :: FileFrame for topology information, can be nullptr. If set, topology
  /// information will be persisted to ff.
  katana::Result<void> Store(
      RDGHandle handle, const std::string& command_line,
      std::unique_ptr<FileFrame> ff) {
    return Store(handle, command_line, IncrementVersion, std::move(ff));
  }

  /// @brief Store new version of RDG with lineage based on command line.
  /// @param handle :: handle indicating where to store RDG
  /// @param command_line :: added to metadata to track lineage of RDG
  katana::Result<void> Store(
      RDGHandle handle, const std::string& command_line) {
    return Store(handle, command_line, IncrementVersion, nullptr);
  }

  /// @brief Store RDG with lineage based on command line and update version based on the versioning policy.
  /// @param handle :: handle indicating where to store RDG
  /// @param command_line :: added to metadata to track lineage of RDG
  /// @param versioning_action :: can be set to 'RDG::RDGVersioningPolicy::IncrementVersion' or
  /// 'RDG::RDGVersioningPolicy::RetainVersion' to indicate whether RDG version is
  /// changing with this store.
  katana::Result<void> Store(
      RDGHandle handle, const std::string& command_line,
      RDGVersioningPolicy versioning_action) {
    return Store(handle, command_line, versioning_action, nullptr);
  }

  katana::Result<void> AddNodeProperties(
      const std::shared_ptr<arrow::Table>& props);

  katana::Result<void> AddEdgeProperties(
      const std::shared_ptr<arrow::Table>& props);

  katana::Result<void> UpsertNodeProperties(
      const std::shared_ptr<arrow::Table>& props);

  katana::Result<void> UpsertEdgeProperties(
      const std::shared_ptr<arrow::Table>& props);

  katana::Result<void> RemoveNodeProperty(uint32_t i);
  katana::Result<void> RemoveEdgeProperty(uint32_t i);

  katana::Result<void> UnloadNodeProperty(uint32_t i);
  katana::Result<void> UnloadEdgeProperty(uint32_t i);

  void MarkAllPropertiesPersistent();

  katana::Result<void> MarkNodePropertiesPersistent(
      const std::vector<std::string>& persist_node_props);
  katana::Result<void> MarkEdgePropertiesPersistent(
      const std::vector<std::string>& persist_edge_props);

  /// Explain to graph how it is derived from previous version
  void AddLineage(const std::string& command_line);

  /// Load the RDG described by the metadata in handle into memory.
  static katana::Result<RDG> Make(RDGHandle handle, const RDGLoadOptions& opts);

  katana::Result<void> UnbindTopologyFileStorage();

  /// Inform this RDG that it's topology is in storage at this location
  /// without loading it into memory. \param new_top must exist and be in
  /// the correct directory for this RDG
  katana::Result<void> SetTopologyFile(const katana::Uri& new_top);

  void AddMirrorNodes(std::shared_ptr<arrow::ChunkedArray>&& a) {
    mirror_nodes_.emplace_back(std::move(a));
  }

  void AddMasterNodes(std::shared_ptr<arrow::ChunkedArray>&& a) {
    master_nodes_.emplace_back(std::move(a));
  }

  //
  // accessors and mutators
  //

  const katana::Uri& rdg_dir() const { return rdg_dir_; }
  void set_rdg_dir(const katana::Uri& rdg_dir) { rdg_dir_ = rdg_dir; }

  uint32_t partition_id() const { return partition_id_; }
  void set_partition_id(uint32_t partition_id) { partition_id_ = partition_id; }

  /// The node properties
  const std::shared_ptr<arrow::Table>& node_properties() const;

  /// The edge properties
  const std::shared_ptr<arrow::Table>& edge_properties() const;

  /// Remove all node properties
  void DropNodeProperties();

  /// Remove all edge properties
  void DropEdgeProperties();

  const std::vector<std::shared_ptr<arrow::ChunkedArray>>& master_nodes()
      const {
    return master_nodes_;
  }
  void set_master_nodes(std::vector<std::shared_ptr<arrow::ChunkedArray>>&& a) {
    master_nodes_ = std::move(a);
  }

  const std::vector<std::shared_ptr<arrow::ChunkedArray>>& mirror_nodes()
      const {
    return mirror_nodes_;
  }
  void set_mirror_nodes(std::vector<std::shared_ptr<arrow::ChunkedArray>>&& a) {
    mirror_nodes_ = std::move(a);
  }

  const std::shared_ptr<arrow::ChunkedArray>& host_to_owned_global_node_ids()
      const {
    return host_to_owned_global_node_ids_;
  }
  void set_host_to_owned_global_node_ids(
      std::shared_ptr<arrow::ChunkedArray>&& a) {
    host_to_owned_global_node_ids_ = std::move(a);
  }

  const std::shared_ptr<arrow::ChunkedArray>& host_to_owned_global_edge_ids()
      const {
    return host_to_owned_global_edge_ids_;
  }
  void set_host_to_owned_global_edge_ids(
      std::shared_ptr<arrow::ChunkedArray>&& a) {
    host_to_owned_global_edge_ids_ = std::move(a);
  }

  const std::shared_ptr<arrow::ChunkedArray>& local_to_user_id() const {
    return local_to_user_id_;
  }
  void set_local_to_user_id(std::shared_ptr<arrow::ChunkedArray>&& a) {
    local_to_user_id_ = std::move(a);
  }

  const std::shared_ptr<arrow::ChunkedArray>& local_to_global_id() const {
    return local_to_global_id_;
  }
  void set_local_to_global_id(std::shared_ptr<arrow::ChunkedArray>&& a) {
    local_to_global_id_ = std::move(a);
  }

  const PartitionMetadata& part_metadata() const;
  void set_part_metadata(const PartitionMetadata& metadata);

  const FileView& topology_file_storage() const;

  void set_view_name(const std::string& v) { view_type_ = v; }

private:
  std::string view_type_;
  RDG(std::unique_ptr<RDGCore>&& core);

  void InitEmptyTables();

  katana::Result<void> DoMake(const katana::Uri& metadata_dir);

  static katana::Result<RDG> Make(
      const RDGManifest& manifest, const RDGLoadOptions& opts);

  katana::Result<void> AddPartitionMetadataArray(
      const std::shared_ptr<arrow::Table>& props);

  katana::Result<std::vector<tsuba::PropStorageInfo>> WritePartArrays(
      const katana::Uri& dir, tsuba::WriteGroup* desc);

  katana::Result<void> DoStore(
      RDGHandle handle, const std::string& command_line,
      RDGVersioningPolicy versioning_action, std::unique_ptr<WriteGroup> desc);

  //
  // Data
  //

  std::unique_ptr<RDGCore> core_;

  std::vector<std::shared_ptr<arrow::ChunkedArray>> mirror_nodes_;
  std::vector<std::shared_ptr<arrow::ChunkedArray>> master_nodes_;
  // Called while constructing to put these arrays into a usable state for Distribution
  void InitArrowVectors();
  std::shared_ptr<arrow::ChunkedArray> host_to_owned_global_node_ids_;
  std::shared_ptr<arrow::ChunkedArray> host_to_owned_global_edge_ids_;
  std::shared_ptr<arrow::ChunkedArray> local_to_user_id_;
  std::shared_ptr<arrow::ChunkedArray> local_to_global_id_;

  /// name of the graph that was used to load this RDG
  katana::Uri rdg_dir_;
  /// which partition of the graph was loaded
  uint32_t partition_id_{std::numeric_limits<uint32_t>::max()};
  // How this graph was derived from the previous version
  RDGLineage lineage_;
};

}  // namespace tsuba

#endif
