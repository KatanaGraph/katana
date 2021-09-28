#ifndef KATANA_LIBTSUBA_RDGCORE_H_
#define KATANA_LIBTSUBA_RDGCORE_H_

#include <memory>

#include <arrow/api.h>

#include "RDGPartHeader.h"
#include "katana/config.h"
#include "tsuba/FileView.h"

namespace tsuba {

class KATANA_EXPORT RDGCore {
public:
  RDGCore() {
    InitEmptyProperties();
    InitArrowVectors();
  }

  RDGCore(RDGPartHeader&& part_header) : part_header_(std::move(part_header)) {
    InitEmptyProperties();
    InitArrowVectors();
  }

  bool Equals(const RDGCore& other) const;

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

  // type info will be missing for properties that weren't loaded
  // make sure it's not missing
  katana::Result<void> EnsureNodeTypesLoaded();
  katana::Result<void> EnsureEdgeTypesLoaded();

  //
  // Accessors and Mutators
  //

  // special partition property names
  static const inline std::string kMirrorNodesPropName = "mirror_nodes";
  static const inline std::string kMasterNodesPropName = "master_nodes";
  static const inline std::string kHostToOwnedGlobalNodeIDsPropName =
      "host_to_owned_global_node_ids";
  static const inline std::string kHostToOwnedGlobalEdgeIDsPropName =
      "host_to_owned_global_edge_ids";
  static const inline std::string kLocalToUserIDPropName = "local_to_user_id";
  static const inline std::string kLocalToGlobalIDPropName =
      "local_to_global_id";
  // deprecated; only here to support backward compatibility
  static const inline std::string kDeprecatedLocalToGlobalIDPropName =
      "local_to_global_vector";
  static const inline std::string kDeprecatedHostToOwnedGlobalNodeIDsPropName =
      "host_to_owned_global_ids";

  static std::string MirrorPropName(unsigned i) {
    return std::string(kMirrorNodesPropName) + "_" + std::to_string(i);
  }

  static std::string MasterPropName(unsigned i) {
    return std::string(kMasterNodesPropName) + "_" + std::to_string(i);
  }
  katana::Result<void> AddPartitionMetadataArray(
      const std::shared_ptr<arrow::Table>& props);

  const katana::Uri& rdg_dir() const { return rdg_dir_; }
  void set_rdg_dir(const katana::Uri& rdg_dir) { rdg_dir_ = rdg_dir; }

  uint32_t partition_id() const { return partition_id_; }
  void set_partition_id(uint32_t partition_id) { partition_id_ = partition_id; }

  const std::shared_ptr<arrow::Table>& node_properties() const {
    return node_properties_;
  }
  void set_node_properties(std::shared_ptr<arrow::Table>&& node_properties) {
    node_properties_ = std::move(node_properties);
  }

  const std::shared_ptr<arrow::Table>& edge_properties() const {
    return edge_properties_;
  }
  void set_edge_properties(std::shared_ptr<arrow::Table>&& edge_properties) {
    edge_properties_ = std::move(edge_properties);
  }

  void drop_node_properties() {
    std::vector<std::shared_ptr<arrow::Array>> empty;
    node_properties_ = arrow::Table::Make(arrow::schema({}), empty, 0);
    part_header_.set_node_prop_info_list({});
  }
  void drop_edge_properties() {
    std::vector<std::shared_ptr<arrow::Array>> empty;
    edge_properties_ = arrow::Table::Make(arrow::schema({}), empty, 0);
    part_header_.set_edge_prop_info_list({});
  }

  void AddMirrorNodes(std::shared_ptr<arrow::ChunkedArray>&& a) {
    mirror_nodes_.emplace_back(std::move(a));
  }

  void AddMasterNodes(std::shared_ptr<arrow::ChunkedArray>&& a) {
    master_nodes_.emplace_back(std::move(a));
  }

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

  const RDGLineage& lineage() const { return lineage_; }
  void set_lineage(RDGLineage&& lineage) { lineage_ = lineage; }

  const FileView& topology_file_storage() const {
    return topology_file_storage_;
  }
  FileView& topology_file_storage() { return topology_file_storage_; }
  void set_topology_file_storage(FileView&& topology_file_storage) {
    topology_file_storage_ = std::move(topology_file_storage);
  }

  const FileView& node_entity_type_id_array_file_storage() const {
    return node_entity_type_id_array_file_storage_;
  }
  FileView& node_entity_type_id_array_file_storage() {
    return node_entity_type_id_array_file_storage_;
  }
  void set_node_entity_type_id_array_file_storage(
      FileView&& node_entity_type_id_array_file_storage) {
    node_entity_type_id_array_file_storage_ =
        std::move(node_entity_type_id_array_file_storage);
  }

  const FileView& edge_entity_type_id_array_file_storage() const {
    return edge_entity_type_id_array_file_storage_;
  }
  FileView& edge_entity_type_id_array_file_storage() {
    return edge_entity_type_id_array_file_storage_;
  }
  void set_edge_entity_type_id_array_file_storage(
      FileView&& edge_entity_type_id_array_file_storage) {
    edge_entity_type_id_array_file_storage_ =
        std::move(edge_entity_type_id_array_file_storage);
  }

  const RDGPartHeader& part_header() const { return part_header_; }
  RDGPartHeader& part_header() { return part_header_; }
  void set_part_header(RDGPartHeader&& part_header) {
    part_header_ = std::move(part_header);
  }

  katana::Result<void> RegisterTopologyFile(const std::string& new_top) {
    part_header_.set_topology_path(new_top);
    return topology_file_storage_.Unbind();
  }

  katana::Result<void> UnbindTopologyFile() {
    return topology_file_storage_.Unbind();
  }

  katana::Result<void> RegisterNodeEntityTypeIDArrayFile(
      const std::string& new_type_id_array) {
    part_header_.set_node_entity_type_id_array_path(new_type_id_array);
    return node_entity_type_id_array_file_storage_.Unbind();
  }

  katana::Result<void> RegisterEdgeEntityTypeIDArrayFile(
      const std::string& new_type_id_array) {
    part_header_.set_edge_entity_type_id_array_path(new_type_id_array);
    return edge_entity_type_id_array_file_storage_.Unbind();
  }

  void AddCommandLine(const std::string& command_line) {
    lineage_.AddCommandLine(command_line);
  }

private:
  void InitEmptyProperties();

  //
  // Data
  //

  std::shared_ptr<arrow::Table> node_properties_;
  std::shared_ptr<arrow::Table> edge_properties_;

  FileView topology_file_storage_;

  FileView node_entity_type_id_array_file_storage_;
  FileView edge_entity_type_id_array_file_storage_;

  RDGPartHeader part_header_;

  std::vector<std::shared_ptr<arrow::ChunkedArray>> mirror_nodes_;
  std::vector<std::shared_ptr<arrow::ChunkedArray>> master_nodes_;
  // Called while constructing to put these arrays into a usable state for
  // Distribution
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
