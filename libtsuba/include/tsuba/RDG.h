#ifndef KATANA_LIBTSUBA_TSUBA_RDG_H_
#define KATANA_LIBTSUBA_TSUBA_RDG_H_

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

#include <arrow/api.h>
#include <arrow/chunked_array.h>
#include <nlohmann/json.hpp>

#include "katana/EntityTypeManager.h"
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
class PropStorageInfo;

struct KATANA_EXPORT RDGLoadOptions {
  /// Which partition of the RDG on storage should be loaded
  /// nullopt means the partition associated with the current host's ID will be
  /// loaded
  std::optional<uint32_t> partition_id_to_load;
  /// List of node properties that should be loaded
  /// nullptr means all node properties will be loaded
  std::optional<std::vector<std::string>> node_properties{std::nullopt};
  /// List of edge properties that should be loaded
  /// nullptr means all edge properties will be loaded
  std::optional<std::vector<std::string>> edge_properties{std::nullopt};
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

  /// Determine if the EntityTypeIDs are stored in properties, or outside
  /// in their own dedicated structures
  bool IsEntityTypeIDsOutsideProperties() const;

  /// Perform some checks on assumed invariants
  katana::Result<void> Validate() const;

  /// Determine if two RDGs are Equal //TODO (yasser): verify that this now works with
  //views
  bool Equals(const RDG& other) const;

  /// @brief Store RDG with lineage based on command line and update version based on the versioning policy.
  /// @param handle :: handle indicating where to store RDG
  /// @param command_line :: added to metadata to track lineage of RDG
  /// @param versioning_action :: can be set to 'RDG::RDGVersioningPolicy::IncrementVersion' or
  /// 'RDG::RDGVersioningPolicy::RetainVersion' to indicate whether RDG version is changing with this store.
  /// @param topology_ff :: if not nullptr, it is persisted as the topology for this RDG.
  /// @param node_entity_type_id_array_ff :: if not nullptr, it is persisted as the node_entity_type_id_array for this RDG.
  /// @param edge_entity_type_id_array_ff :: if not nullptr, it is persisted as the edge_entity_type_id_array for this RDG.
  /// @param node_entity_type_manager :: persisted as the node EntityTypeID -> Atomic node EntityType id mapping and Atomic node EntityType ID -> Atomic EntityType Name
  /// @param edge_entity_type_manager :: persisted as the edge EntityTypeID -> Atomic node EntityType id mapping and Atomic edge EntityType ID -> Atomic EntityType Name
  katana::Result<void> Store(
      RDGHandle handle, const std::string& command_line,
      RDGVersioningPolicy versioning_action,
      std::unique_ptr<FileFrame> topology_ff,
      std::unique_ptr<FileFrame> node_entity_type_id_array_ff,
      std::unique_ptr<FileFrame> edge_entity_type_id_array_ff,
      const katana::EntityTypeManager& node_entity_type_manager,
      const katana::EntityTypeManager& edge_entity_type_manager);

  /// @brief Store new version of the RDG with lineage based on command line.
  /// @param handle :: handle indicating where to store RDG
  /// @param command_line :: added to metadata to track lineage of RDG
  /// @param topology_ff :: if not nullptr, it is persisted as the topology for this RDG.
  /// @param node_entity_type_id_array_ff :: if not nullptr, it is persisted as the node_entity_type_id_array for this RDG.
  /// @param edge_entity_type_id_array_ff :: if not nullptr, it is persisted as the edge_entity_type_id_array for this RDG.
  /// @param node_entity_type_manager :: persisted as the node EntityTypeID -> Atomic node EntityType id mapping and Atomic node EntityType ID -> Atomic EntityType Name
  /// @param edge_entity_type_manager :: persisted as the edge EntityTypeID -> Atomic node EntityType id mapping and Atomic edge EntityType ID -> Atomic EntityType Name
  katana::Result<void> Store(
      RDGHandle handle, const std::string& command_line,
      std::unique_ptr<FileFrame> topology_ff,
      std::unique_ptr<FileFrame> node_entity_type_id_array_ff,
      std::unique_ptr<FileFrame> edge_entity_type_id_array_ff,
      const katana::EntityTypeManager& node_entity_type_manager,
      const katana::EntityTypeManager& edge_entity_type_manager) {
    return Store(
        handle, command_line, IncrementVersion, std::move(topology_ff),
        std::move(node_entity_type_id_array_ff),
        std::move(edge_entity_type_id_array_ff), node_entity_type_manager,
        edge_entity_type_manager);
  }

  /// @brief Store new version of RDG with lineage based on command line.
  /// @param handle :: handle indicating where to store RDG
  /// @param command_line :: added to metadata to track lineage of RDG
  katana::Result<void> Store(
      RDGHandle handle, const std::string& command_line) {
    katana::EntityTypeManager node_entity_type_manager;
    katana::EntityTypeManager edge_entity_type_manager;
    return Store(
        handle, command_line, IncrementVersion, nullptr, nullptr, nullptr,
        node_entity_type_manager, edge_entity_type_manager);
  }

  /// @brief Store RDG with lineage based on command line and update version based on the versioning policy.
  /// @param handle :: handle indicating where to store RDG
  /// @param command_line :: added to metadata to track lineage of RDG
  /// @param versioning_action :: can be set to 'RDG::RDGVersioningPolicy::IncrementVersion' or
  /// 'RDG::RDGVersioningPolicy::RetainVersion' to indicate whether RDG version is changing with this store.
  katana::Result<void> Store(
      RDGHandle handle, const std::string& command_line,
      RDGVersioningPolicy versioning_action) {
    katana::EntityTypeManager node_entity_type_manager;
    katana::EntityTypeManager edge_entity_type_manager;
    return Store(
        handle, command_line, versioning_action, nullptr, nullptr, nullptr,
        node_entity_type_manager, edge_entity_type_manager);
  }

  katana::Result<void> AddNodeProperties(
      const std::shared_ptr<arrow::Table>& props);

  katana::Result<void> AddEdgeProperties(
      const std::shared_ptr<arrow::Table>& props);

  katana::Result<void> UpsertNodeProperties(
      const std::shared_ptr<arrow::Table>& props);

  katana::Result<void> UpsertEdgeProperties(
      const std::shared_ptr<arrow::Table>& props);

  katana::Result<void> RemoveNodeProperty(int i);
  katana::Result<void> RemoveEdgeProperty(int i);

  /// Ensure the node property at index `i` was written back to storage
  /// then free its memory
  katana::Result<void> UnloadNodeProperty(int i);

  /// Ensure the edge property at index `i` was written back to storage
  /// then free its memory
  katana::Result<void> UnloadEdgeProperty(int i);

  /// Load node property with a particular name and insert it into the
  /// property table at index. If index is invalid, the property is put
  /// in the last slot. A given property cannot be loaded more than once
  katana::Result<void> LoadNodeProperty(const std::string& name, int i = -1);

  /// Load edge property with a particular name and insert it into the
  /// property table at index. If index is greater than the last column
  /// index in the table, it is put in the last slot. A given property
  /// cannot be loaded more than once
  katana::Result<void> LoadEdgeProperty(const std::string& name, int i = -1);

  std::vector<std::string> ListNodeProperties() const;
  std::vector<std::string> ListEdgeProperties() const;

  /// Explain to graph how it is derived from previous version
  void AddLineage(const std::string& command_line);

  /// Load the RDG described by the metadata in handle into memory.
  static katana::Result<RDG> Make(RDGHandle handle, const RDGLoadOptions& opts);

  katana::Result<void> UnbindTopologyFileStorage();

  /// Inform this RDG that its topology is in storage at this location
  /// without loading it into memory. \param new_top must exist and be in
  /// the correct directory for this RDG
  katana::Result<void> SetTopologyFile(const katana::Uri& new_top);

  katana::Result<void> UnbindNodeEntityTypeIDArrayFileStorage();

  /// Inform this RDG that its Node Entity Type ID Array is in storage at this location
  /// without loading it into memory. \param new_type_id_array must exist and be in
  /// the correct directory for this RDG
  katana::Result<void> SetNodeEntityTypeIDArrayFile(
      const katana::Uri& new_type_id_array);

  katana::Result<void> UnbindEdgeEntityTypeIDArrayFileStorage();

  /// Inform this RDG that its Edge Entity Type ID Array is in storage at this location
  /// without loading it into memory. \param new_type_id_array must exist and be in
  /// the correct directory for this RDG
  katana::Result<void> SetEdgeEntityTypeIDArrayFile(
      const katana::Uri& new_type_id_array);

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

  std::shared_ptr<arrow::Schema> full_node_schema() const;

  std::shared_ptr<arrow::Schema> full_edge_schema() const;

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

  const FileView& node_entity_type_id_array_file_storage() const;

  const FileView& edge_entity_type_id_array_file_storage() const;

  katana::Result<katana::EntityTypeManager> node_entity_type_manager() const;

  katana::Result<katana::EntityTypeManager> edge_entity_type_manager() const;

  void set_view_name(const std::string& v) { view_type_ = v; }

private:
  std::string view_type_;
  RDG(std::unique_ptr<RDGCore>&& core);

  void InitEmptyTables();

  katana::Result<void> DoMake(
      const std::vector<PropStorageInfo*>& node_props_to_be_loaded,
      const std::vector<PropStorageInfo*>& edge_props_to_be_loaded,
      const RDGManifest& manifest);

  static katana::Result<RDG> Make(
      const RDGManifest& manifest, const RDGLoadOptions& opts);

  katana::Result<void> AddPartitionMetadataArray(
      const std::shared_ptr<arrow::Table>& props);

  katana::Result<std::vector<tsuba::PropStorageInfo>> WritePartArrays(
      const katana::Uri& dir, tsuba::WriteGroup* desc);

  katana::Result<void> DoStore(
      RDGHandle handle, const std::string& command_line,
      RDGVersioningPolicy versioning_action,
      std::unique_ptr<WriteGroup> write_group);

  katana::Result<void> DoStoreTopology(
      RDGHandle handle, std::unique_ptr<FileFrame> topology_ff,
      std::unique_ptr<WriteGroup>& write_group);

  katana::Result<void> DoStoreNodeEntityTypeIDArray(
      RDGHandle handle, std::unique_ptr<FileFrame> node_entity_type_id_array_ff,
      std::unique_ptr<WriteGroup>& write_group);

  katana::Result<void> DoStoreEdgeEntityTypeIDArray(
      RDGHandle handle, std::unique_ptr<FileFrame> edge_entity_type_id_array_ff,
      std::unique_ptr<WriteGroup>& write_group);

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
