#ifndef KATANA_LIBTSUBA_RDGPARTHEADER_H_
#define KATANA_LIBTSUBA_RDGPARTHEADER_H_

#include <cassert>
#include <vector>

#include <arrow/api.h>

#include "katana/JSON.h"
#include "katana/Result.h"
#include "katana/Uri.h"
#include "tsuba/PartitionMetadata.h"
#include "tsuba/WriteGroup.h"
#include "tsuba/tsuba.h"

namespace tsuba {

struct PropStorageInfo {
  std::string name;
  std::string path;
  bool persist{false};
};

class KATANA_EXPORT RDGPartHeader {
public:
  static katana::Result<RDGPartHeader> Make(const katana::Uri& partition_path);

  katana::Result<void> Validate() const;

  katana::Result<void> PrunePropsTo(
      const std::vector<std::string>* node_props,
      const std::vector<std::string>* edge_props);

  katana::Result<void> Write(RDGHandle handle, WriteGroup* writes) const;

  void UnbindFromStorage();

  //
  // Property manipulation
  //

  void AppendNodePropStorageInfo(PropStorageInfo&& pmd) {
    node_prop_info_list_.emplace_back(std::move(pmd));
  }

  void AppendEdgePropStorageInfo(PropStorageInfo&& pmd) {
    edge_prop_info_list_.emplace_back(std::move(pmd));
  }

  void RemoveNodeProperty(uint32_t i) {
    auto& p = node_prop_info_list_;
    assert(i < p.size());
    p.erase(p.begin() + i);
  }

  void RemoveEdgeProperty(uint32_t i) {
    auto& p = edge_prop_info_list_;
    assert(i < p.size());
    p.erase(p.begin() + i);
  }

  //
  // Property persistence
  //

  void MarkAllPropertiesPersistent();

  katana::Result<void> MarkNodePropertiesPersistent(
      const std::vector<std::string>& persist_node_props);

  katana::Result<void> MarkEdgePropertiesPersistent(
      const std::vector<std::string>& persist_edge_props);

  //
  // Accessors/Mutators
  //

  const std::string& topology_path() const { return topology_path_; }
  void set_topology_path(std::string path) { topology_path_ = std::move(path); }

  const std::vector<PropStorageInfo>& node_prop_info_list() const {
    return node_prop_info_list_;
  }
  void set_node_prop_info_list(
      std::vector<PropStorageInfo>&& node_prop_info_list) {
    node_prop_info_list_ = std::move(node_prop_info_list);
  }

  const std::vector<PropStorageInfo>& edge_prop_info_list() const {
    return edge_prop_info_list_;
  }
  void set_edge_prop_info_list(
      std::vector<PropStorageInfo>&& edge_prop_info_list) {
    edge_prop_info_list_ = std::move(edge_prop_info_list);
  }

  const std::vector<PropStorageInfo>& part_prop_info_list() const {
    return part_prop_info_list_;
  }
  void set_part_properties(std::vector<PropStorageInfo>&& part_prop_info_list) {
    part_prop_info_list_ = std::move(part_prop_info_list);
  }

  const PartitionMetadata& metadata() const { return metadata_; }
  void set_metadata(const PartitionMetadata& metadata) { metadata_ = metadata; }

  friend void to_json(nlohmann::json& j, const RDGPartHeader& header);
  friend void from_json(const nlohmann::json& j, RDGPartHeader& header);

private:
  static katana::Result<RDGPartHeader> MakeJson(
      const katana::Uri& partition_path);
  static katana::Result<RDGPartHeader> MakeParquet(
      const katana::Uri& partition_path);

  std::vector<PropStorageInfo> part_prop_info_list_;
  std::vector<PropStorageInfo> node_prop_info_list_;
  std::vector<PropStorageInfo> edge_prop_info_list_;

  /// Metadata filled in by CuSP, or from storage (meta partition file)
  PartitionMetadata metadata_;

  std::string topology_path_;
};

void to_json(nlohmann::json& j, const RDGPartHeader& header);
void from_json(const nlohmann::json& j, RDGPartHeader& header);

void to_json(nlohmann::json& j, const PropStorageInfo& propmd);
void from_json(const nlohmann::json& j, PropStorageInfo& propmd);

void to_json(nlohmann::json& j, const PartitionMetadata& propmd);
void from_json(const nlohmann::json& j, PartitionMetadata& propmd);

void to_json(
    nlohmann::json& j, const std::vector<tsuba::PropStorageInfo>& vec_pmd);

}  // namespace tsuba

#endif
