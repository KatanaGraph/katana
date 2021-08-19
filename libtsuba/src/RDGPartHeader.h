#ifndef KATANA_LIBTSUBA_RDGPARTHEADER_H_
#define KATANA_LIBTSUBA_RDGPARTHEADER_H_

#include <cassert>
#include <optional>
#include <vector>

#include <arrow/api.h>

#include "katana/JSON.h"
#include "katana/Result.h"
#include "katana/URI.h"
#include "tsuba/Errors.h"
#include "tsuba/PartitionMetadata.h"
#include "tsuba/RDG.h"
#include "tsuba/WriteGroup.h"
#include "tsuba/tsuba.h"

namespace tsuba {

/// PropStorageInfo objects track the state of properties, and sanity check their
/// transitions. N.b., It does not "DO" the transitions, this structure is purely
/// for bookkeeping
///
/// Properties have 3 states:
///  * Absent - exists in storage but is not in memory
///  * Clean  - in memory and matches what is in storage
///  * Dirty  - in memory but does not match what is in storage
///
/// The state machine looks like this:
///
/// EXISTING                                     NEW
/// PROPERTY          modify                   PROPERTY
///   |   +---------------------------------+     |
///   |   |              +-------+  modify  |     |
///   |   |     +------->| Clean +------+   |     |
///   |   | load|        +-+-----+      |   |     |
///   |   |     |          |   ^        v   v     |
///   |  +------+-+        |   |       +-------+  |
///   +->| Absent |<-------+   +-------+ Dirty |<-+
///      +--------+  unload     write  +-------+
///
/// Properties either start out in storage as part of an RDG on disk
/// (EXISTING PROPERTY) or start out in memory as part of an RDG in
/// memory (NEW PROPERTY)
class PropStorageInfo {
  enum class State {
    kAbsent,
    kClean,
    kDirty,
  };

public:
  PropStorageInfo(std::string name, std::shared_ptr<arrow::DataType> type)
      : name_(std::move(name)),
        path_(),
        type_(std::move(type)),
        state_(State::kDirty) {}

  PropStorageInfo(std::string name, std::string path)
      : name_(std::move(name)),
        path_(std::move(path)),
        state_(State::kAbsent) {}

  void WasLoaded(const std::shared_ptr<arrow::DataType>& type) {
    KATANA_LOG_ASSERT(state_ == State::kAbsent);
    state_ = State::kClean;
    type_ = type;
  }

  void WasModified(const std::shared_ptr<arrow::DataType>& type) {
    path_.clear();
    state_ = State::kDirty;
    type_ = type;
  }

  void WasWritten(std::string_view new_path) {
    KATANA_LOG_ASSERT(state_ == State::kDirty);
    path_ = new_path;
    state_ = State::kClean;
  }

  void WasUnloaded() {
    KATANA_LOG_ASSERT(state_ == State::kClean);
    state_ = State::kAbsent;
  }

  bool IsAbsent() const { return state_ == State::kAbsent; }

  bool IsClean() const { return state_ == State::kClean; }

  bool IsDirty() const { return state_ == State::kDirty; }

  const std::string& name() const { return name_; }
  const std::string& path() const { return path_; }
  const std::shared_ptr<arrow::DataType>& type() const { return type_; }

  // since we don't have type info in the header don't know the
  // type when this would have been constructed. Allow others to
  // fix up the type in this case, required until we can get the type
  // from PartHeader files
  void set_type(std::shared_ptr<arrow::DataType> type) {
    KATANA_LOG_ASSERT(state_ == State::kAbsent);
    type_ = std::move(type);
  }

  friend void to_json(nlohmann::json& j, const PropStorageInfo& propmd);
  friend void from_json(const nlohmann::json& j, PropStorageInfo& propmd);

  // required for json
  PropStorageInfo() = default;

private:
  std::string name_;
  std::string path_;
  std::shared_ptr<arrow::DataType> type_;
  State state_;
};

class KATANA_EXPORT RDGPartHeader {
public:
  static katana::Result<RDGPartHeader> Make(const katana::Uri& partition_path);

  katana::Result<void> Validate() const;

  katana::Result<void> Write(
      RDGHandle handle, WriteGroup* writes,
      RDG::RDGVersioningPolicy retain_version) const;

  /// Mark all in-memory properties dirty so that they can be written
  /// out, copy out-of-memory properties
  katana::Result<void> ChangeStorageLocation(
      const katana::Uri& old_location, const katana::Uri& new_location);

  //
  // Property manipulation
  //

  katana::Result<std::vector<PropStorageInfo*>> SelectNodeProperties(
      const std::optional<std::vector<std::string>>& names = std::nullopt) {
    return SelectProperties(&node_prop_info_list_, names);
  }

  katana::Result<std::vector<PropStorageInfo*>> SelectEdgeProperties(
      const std::optional<std::vector<std::string>>& names = std::nullopt) {
    return SelectProperties(&edge_prop_info_list_, names);
  }

  katana::Result<std::vector<PropStorageInfo*>> SelectPartitionProperties() {
    return SelectProperties(&part_prop_info_list_, std::nullopt);
  }

  void UpsertNodePropStorageInfo(PropStorageInfo&& pmd) {
    auto pmd_it = std::find_if(
        node_prop_info_list_.begin(), node_prop_info_list_.end(),
        [&](const PropStorageInfo& my_pmd) {
          return my_pmd.name() == pmd.name();
        });
    if (pmd_it == node_prop_info_list_.end()) {
      node_prop_info_list_.emplace_back(std::move(pmd));
    } else {
      pmd_it->IsDirty();
    }
  }

  void UpsertEdgePropStorageInfo(PropStorageInfo&& pmd) {
    auto pmd_it = std::find_if(
        edge_prop_info_list_.begin(), edge_prop_info_list_.end(),
        [&](const PropStorageInfo& my_pmd) {
          return my_pmd.name() == pmd.name();
        });
    if (pmd_it == edge_prop_info_list_.end()) {
      edge_prop_info_list_.emplace_back(std::move(pmd));
    } else {
      pmd_it->IsDirty();
    }
  }

  void RemoveNodeProperty(uint32_t i) {
    auto& p = node_prop_info_list_;
    KATANA_LOG_DEBUG_ASSERT(i < p.size());
    p.erase(p.begin() + i);
  }

  katana::Result<void> RemoveNodeProperty(const std::string& name) {
    auto& p = node_prop_info_list_;
    auto it = std::find_if(p.begin(), p.end(), [&](const PropStorageInfo& psi) {
      return psi.name() == name;
    });
    if (it == p.end()) {
      return KATANA_ERROR(
          tsuba::ErrorCode::PropertyNotFound, "no such node property");
    }
    p.erase(it);
    return katana::ResultSuccess();
  }

  void RemoveEdgeProperty(uint32_t i) {
    auto& p = edge_prop_info_list_;
    KATANA_LOG_DEBUG_ASSERT(i < p.size());
    p.erase(p.begin() + i);
  }

  katana::Result<void> RemoveEdgeProperty(const std::string& name) {
    auto& p = edge_prop_info_list_;
    auto it = std::find_if(p.begin(), p.end(), [&](const PropStorageInfo& psi) {
      return psi.name() == name;
    });
    if (it == p.end()) {
      return KATANA_ERROR(
          tsuba::ErrorCode::PropertyNotFound, "no such edge property");
    }
    p.erase(it);
    return katana::ResultSuccess();
  }

  //
  // Accessors/Mutators
  //

  const std::string& topology_path() const { return topology_path_; }
  void set_topology_path(std::string path) { topology_path_ = std::move(path); }

  const std::vector<PropStorageInfo>& node_prop_info_list() const {
    return node_prop_info_list_;
  }
  std::vector<PropStorageInfo>& node_prop_info_list() {
    return node_prop_info_list_;
  }
  void set_node_prop_info_list(
      std::vector<PropStorageInfo>&& node_prop_info_list) {
    node_prop_info_list_ = std::move(node_prop_info_list);
  }

  const std::vector<PropStorageInfo>& edge_prop_info_list() const {
    return edge_prop_info_list_;
  }
  std::vector<PropStorageInfo>& edge_prop_info_list() {
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

  const std::vector<std::string>& node_property_indexes_column_name() const {
    return node_property_indexes_column_name_;
  }
  std::vector<std::string>& node_property_indexes_column_name() {
    return node_property_indexes_column_name_;
  }
  void set_node_property_indexes_column_name(
      std::vector<std::string>&& node_property_indexes_column_name) {
    node_property_indexes_column_name_ = std::move(node_property_indexes_column_name);
  }

  const std::vector<std::string>& edge_property_indexes_column_name() const {
    return edge_property_indexes_column_name_;
  }
  std::vector<std::string>& edge_property_indexes_column_name() {
    return edge_property_indexes_column_name_;
  }
  void set_edge_property_indexes_column_name(
      std::vector<std::string>&& edge_property_indexes_column_name) {
    edge_property_indexes_column_name_ = std::move(edge_property_indexes_column_name);
  }

  const PartitionMetadata& metadata() const { return metadata_; }
  void set_metadata(const PartitionMetadata& metadata) { metadata_ = metadata; }

  friend void to_json(nlohmann::json& j, const RDGPartHeader& header);
  friend void from_json(const nlohmann::json& j, RDGPartHeader& header);

private:
  static katana::Result<std::vector<PropStorageInfo*>> DoSelectProperties(
      std::vector<PropStorageInfo>* storage_info,
      const std::vector<std::string>& names) {
    std::unordered_map<std::string, std::pair<PropStorageInfo*, bool>>
        name_to_slot;
    for (auto& prop : *storage_info) {
      name_to_slot.emplace(prop.name(), std::make_pair(&prop, false));
    }

    std::vector<PropStorageInfo*> properties;
    for (const auto& name : names) {
      auto it = name_to_slot.find(name);
      if (it == name_to_slot.end()) {
        return KATANA_ERROR(
            ErrorCode::PropertyNotFound, "no property named {}",
            std::quoted(name));
      }
      if (it->second.second) {
        return KATANA_ERROR(
            ErrorCode::InvalidArgument, "property cannot be loaded twice ({})",
            std::quoted(name));
      }
      it->second.second = true;
      properties.emplace_back(it->second.first);
    }
    return properties;
  }

  static katana::Result<std::vector<PropStorageInfo*>> DoSelectProperties(
      std::vector<PropStorageInfo>* storage_info) {
    // all of the properties
    std::vector<PropStorageInfo*> properties;
    for (auto& prop : *storage_info) {
      properties.emplace_back(&prop);
    }
    return properties;
  }

  static katana::Result<std::vector<PropStorageInfo*>> SelectProperties(
      std::vector<PropStorageInfo>* storage_info,
      const std::optional<std::vector<std::string>>& names) {
    if (names) {
      return DoSelectProperties(storage_info, names.value());
    }
    return DoSelectProperties(storage_info);
  }

  static katana::Result<RDGPartHeader> MakeJson(
      const katana::Uri& partition_path);

  std::vector<PropStorageInfo> part_prop_info_list_;
  std::vector<PropStorageInfo> node_prop_info_list_;
  std::vector<PropStorageInfo> edge_prop_info_list_;

  /// Column Names to create property index from on startup
  std::vector<std::string>
      node_property_indexes_column_name_;  //nhomann serializes this automagically. to/from json required if column name type is (in the future) changed from string to a custom one
  std::vector<std::string>
      edge_property_indexes_column_name_;  //nhomann serializes this automagically. to/from json required if column name type is (in the future) changed from string to a custom one


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
