#ifndef GALOIS_LIBTSUBA_TSUBA_RDG_H_
#define GALOIS_LIBTSUBA_TSUBA_RDG_H_

#include <cstdint>
#include <memory>
#include <string>

#include <arrow/api.h>
#include <arrow/chunked_array.h>
#include <nlohmann/json.hpp>

#include "galois/Result.h"
#include "galois/Uri.h"
#include "galois/config.h"
#include "tsuba/Errors.h"
#include "tsuba/FileFrame.h"
#include "tsuba/FileView.h"
#include "tsuba/PartitionMetadata.h"
#include "tsuba/RDGLineage.h"
#include "tsuba/WriteGroup.h"
#include "tsuba/tsuba.h"

namespace tsuba {

class RDGMeta;
class RDGCore;
struct PropStorageInfo;

class GALOIS_EXPORT RDG {
public:
  RDG(const RDG& no_copy) = delete;
  RDG& operator=(const RDG& no_dopy) = delete;

  RDG();
  ~RDG();
  RDG(RDG&& other) noexcept;
  RDG& operator=(RDG&& other) noexcept;

  /// Perform some checks on assumed invariants
  galois::Result<void> Validate() const;

  /// Determine if two RDGs are Equal
  bool Equals(const RDG& other) const;

  /// Store this RDG at `handle`, if `ff` is not null, it is assumed to contain
  /// an updated topology and persisted as such
  galois::Result<void> Store(
      RDGHandle handle, const std::string& command_line,
      std::unique_ptr<FileFrame> ff = nullptr);

  galois::Result<void> AddNodeProperties(
      const std::shared_ptr<arrow::Table>& table);

  galois::Result<void> AddEdgeProperties(
      const std::shared_ptr<arrow::Table>& table);

  galois::Result<void> RemoveNodeProperty(uint32_t i);
  galois::Result<void> RemoveEdgeProperty(uint32_t i);

  void MarkAllPropertiesPersistent();

  galois::Result<void> MarkNodePropertiesPersistent(
      const std::vector<std::string>& persist_node_props);
  galois::Result<void> MarkEdgePropertiesPersistent(
      const std::vector<std::string>& persist_edge_props);

  /// Explain to graph how it is derived from previous version
  void AddLineage(const std::string& command_line);

  /// Load the RDG described by the metadata in handle into memory
  static galois::Result<RDG> Make(
      RDGHandle handle, const std::vector<std::string>* node_props = nullptr,
      const std::vector<std::string>* edge_props = nullptr);

  galois::Result<void> UnbindTopologyFileStorage();

  void AddMirrorNodes(std::shared_ptr<arrow::ChunkedArray>&& a) {
    mirror_nodes_.emplace_back(std::move(a));
  }

  void AddMasterNodes(std::shared_ptr<arrow::ChunkedArray>&& a) {
    master_nodes_.emplace_back(std::move(a));
  }

  //
  // accessors and mutators
  //

  const galois::Uri& rdg_dir() const { return rdg_dir_; }
  void set_rdg_dir(const galois::Uri& rdg_dir) { rdg_dir_ = rdg_dir; }

  /// The table of node properties
  const std::shared_ptr<arrow::Table>& node_table() const;

  /// The table of edge properties
  const std::shared_ptr<arrow::Table>& edge_table() const;

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

  const std::shared_ptr<arrow::ChunkedArray>& local_to_global_vector() const {
    return local_to_global_vector_;
  }
  void set_local_to_global_vector(std::shared_ptr<arrow::ChunkedArray>&& a) {
    local_to_global_vector_ = std::move(a);
  }

  const PartitionMetadata& part_metadata() const;
  void set_part_metadata(const PartitionMetadata& metadata);

  const FileView& topology_file_storage() const;

private:
  RDG(std::unique_ptr<RDGCore>&& core);

  void InitEmptyTables();

  galois::Result<void> DoMake(const galois::Uri& metadata_dir);

  static galois::Result<RDG> Make(
      const RDGMeta& meta, const std::vector<std::string>* node_props,
      const std::vector<std::string>* edge_props);

  galois::Result<void> AddPartitionMetadataArray(
      const std::shared_ptr<arrow::Table>& table);

  galois::Result<std::vector<tsuba::PropStorageInfo>> WritePartArrays(
      const galois::Uri& dir, tsuba::WriteGroup* desc);

  galois::Result<void> DoStore(
      RDGHandle handle, const std::string& command_line,
      std::unique_ptr<WriteGroup> desc);

  //
  // Data
  //

  std::unique_ptr<RDGCore> core_;

  std::vector<std::shared_ptr<arrow::ChunkedArray>> mirror_nodes_;
  std::vector<std::shared_ptr<arrow::ChunkedArray>> master_nodes_;
  std::shared_ptr<arrow::ChunkedArray> local_to_global_vector_;

  /// name of the graph that was used to load this RDG
  galois::Uri rdg_dir_;
  // How this graph was derived from the previous version
  RDGLineage lineage_;
};

}  // namespace tsuba

#endif
