#ifndef KATANA_LIBTSUBA_RDGPARTHEADER_H_
#define KATANA_LIBTSUBA_RDGPARTHEADER_H_

#include <cassert>
#include <cstddef>
#include <optional>
#include <regex>
#include <string>
#include <utility>
#include <vector>

#include <arrow/api.h>

#include "katana/EntityTypeManager.h"
#include "katana/JSON.h"
#include "katana/Logging.h"
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

  katana::Result<void> ValidateEntityTypeIDStructures() const;
  static bool IsPartitionFileUri(const katana::Uri& uri);
  // TODO(vkarthik): Move this somewhere else because this depends on the Parse function here. Might
  // need to reorganize all the parsing properly.
  static katana::Result<uint64_t> ParseHostFromPartitionFile(
      const std::string& file);

  bool IsEntityTypeIDsOutsideProperties() const;
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

  const std::string& node_entity_type_id_array_path() const {
    return node_entity_type_id_array_path_;
  }
  void set_node_entity_type_id_array_path(std::string path) {
    node_entity_type_id_array_path_ = std::move(path);
  }

  const std::string& edge_entity_type_id_array_path() const {
    return edge_entity_type_id_array_path_;
  }
  void set_edge_entity_type_id_array_path(std::string path) {
    edge_entity_type_id_array_path_ = std::move(path);
  }

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

  const PartitionMetadata& metadata() const { return metadata_; }
  void set_metadata(const PartitionMetadata& metadata) { metadata_ = metadata; }

  uint32_t storage_format_version() const { return storage_format_version_; }
  void update_storage_format_version() {
    storage_format_version_ = latest_storage_format_version_;
  }

  const tsuba::EntityTypeIDToSetOfEntityTypeIDsStorageMap&
  node_entity_type_id_dictionary() const {
    return node_entity_type_id_dictionary_;
  }

  const tsuba::EntityTypeIDToSetOfEntityTypeIDsStorageMap&
  edge_entity_type_id_dictionary() const {
    return edge_entity_type_id_dictionary_;
  }

  void ValidateDictBitset(
      const katana::EntityTypeIDToSetOfEntityTypeIDsMap& manager_map,
      const tsuba::EntityTypeIDToSetOfEntityTypeIDsStorageMap& id_dict) const {
    for (const auto& pair : id_dict) {
      katana::EntityTypeID cur_id = pair.first;
      tsuba::StorageSetOfEntityTypeIDs cur_id_set = pair.second;
      for (auto& id : cur_id_set) {
        KATANA_LOG_ASSERT(manager_map[cur_id][id] == true);
      }
    }
  }

  void StoreNodeEntityTypeManager(const katana::EntityTypeManager& manager) {
    tsuba::EntityTypeIDToSetOfEntityTypeIDsStorageMap id_dict;
    katana::EntityTypeIDToAtomicTypeNameMap id_name;

    ConvertEntityTypeManager(manager, &id_dict, &id_name);

    if (!id_dict.empty()) {
      set_node_entity_type_id_dictionary(id_dict);
    } else {
      KATANA_LOG_WARN("converted node id_dict is empty, not setting!");
    }
    if (!id_name.empty()) {
      set_node_entity_type_id_name(id_name);
    } else {
      KATANA_LOG_WARN("converted node id_name is empty, not setting!");
    }
  }

  void StoreEdgeEntityTypeManager(const katana::EntityTypeManager& manager) {
    tsuba::EntityTypeIDToSetOfEntityTypeIDsStorageMap id_dict;
    katana::EntityTypeIDToAtomicTypeNameMap id_name;

    ConvertEntityTypeManager(manager, &id_dict, &id_name);

    if (!id_dict.empty()) {
      set_edge_entity_type_id_dictionary(id_dict);
    } else {
      KATANA_LOG_WARN("converted edge id_dict is empty, not setting!");
    }
    if (!id_name.empty()) {
      set_edge_entity_type_id_name(id_name);
    } else {
      KATANA_LOG_WARN("converted edge id_name is empty, not setting!");
    }
  }

  katana::Result<katana::EntityTypeManager> GetEntityTypeManager(
      const tsuba::EntityTypeIDToSetOfEntityTypeIDsStorageMap& id_dict,
      const katana::EntityTypeIDToAtomicTypeNameMap& id_name) const {
    katana::EntityTypeIDToAtomicTypeNameMap manager_name_map;
    katana::EntityTypeIDToSetOfEntityTypeIDsMap manager_type_id_map;
    katana::EntityTypeID cur_entity_type_id = 0;

    // convert id_name -> EntityTypeID Name map
    manager_name_map = id_name;

    // convert id_dict -> EntityTypeID map
    manager_type_id_map.resize(id_dict.size());
    for (const auto& pair : id_dict) {
      cur_entity_type_id = pair.first;
      katana::SetOfEntityTypeIDs cur_set;
      for (const auto& id : pair.second) {
        cur_set.set(id);
      }
      manager_type_id_map.at(cur_entity_type_id) = cur_set;
    }

    KATANA_LOG_ASSERT(manager_type_id_map.size() == id_dict.size());
    KATANA_LOG_ASSERT(manager_name_map.size() == id_name.size());

    ValidateDictBitset(manager_type_id_map, id_dict);

    auto manager = katana::EntityTypeManager(
        std::move(manager_name_map), std::move(manager_type_id_map));

    KATANA_LOG_ASSERT(manager.GetNumEntityTypes() == id_dict.size());
    KATANA_LOG_ASSERT(
        manager.GetEntityTypeIDToAtomicTypeNameMap().size() == id_name.size());

    ValidateDictBitset(manager.GetEntityTypeIDToAtomicEntityTypeIDs(), id_dict);

    return katana::Result<katana::EntityTypeManager>(std::move(manager));
  }

  katana::Result<katana::EntityTypeManager> GetNodeEntityTypeManager() {
    return GetEntityTypeManager(
        node_entity_type_id_dictionary_, node_entity_type_id_name_);
  }

  katana::Result<katana::EntityTypeManager> GetEdgeEntityTypeManager() {
    return GetEntityTypeManager(
        edge_entity_type_id_dictionary_, edge_entity_type_id_name_);
  }

  friend void to_json(nlohmann::json& j, const RDGPartHeader& header);
  friend void from_json(const nlohmann::json& j, RDGPartHeader& header);

private:
  /// Extract the EntityType information from an EntityTypeManager and convert it for storage
  void ConvertEntityTypeManager(
      const katana::EntityTypeManager& manager,
      tsuba::EntityTypeIDToSetOfEntityTypeIDsStorageMap* id_dict,
      katana::EntityTypeIDToAtomicTypeNameMap* id_name) const {
    katana::EntityTypeIDToSetOfEntityTypeIDsMap manager_type_id_sets =
        manager.GetEntityTypeIDToAtomicEntityTypeIDs();

    size_t num_entity_types = manager_type_id_sets.size();
    for (size_t i = 0, ni = num_entity_types; i < ni; ++i) {
      auto cur_id = katana::EntityTypeID(i);
      tsuba::StorageSetOfEntityTypeIDs empty_set;
      id_dict->emplace(std::make_pair(cur_id, empty_set));
    }
    for (size_t i = 0, ni = num_entity_types; i < ni; ++i) {
      auto cur_id = katana::EntityTypeID(i);
      for (size_t j = 0, nj = num_entity_types; j < nj; ++j) {
        if (manager_type_id_sets[i].test(j)) {
          // if we have seen this EntityTypeID already, add to its set
          id_dict->at(cur_id).emplace_back(katana::EntityTypeID(j));
        }
      }
    }

    // Convert EntityTypeID name map
    *id_name = manager.GetEntityTypeIDToAtomicTypeNameMap();
    ValidateDictBitset(manager_type_id_sets, *id_dict);
  }

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
        KATANA_LOG_WARN(
            "Non-existant property \"{}\" requested. Skipping.", name);
        continue;
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

  void set_node_entity_type_id_dictionary(
      const tsuba::EntityTypeIDToSetOfEntityTypeIDsStorageMap&
          node_entity_type_id_dictionary) {
    node_entity_type_id_dictionary_ = node_entity_type_id_dictionary;
  }

  void set_edge_entity_type_id_dictionary(
      const tsuba::EntityTypeIDToSetOfEntityTypeIDsStorageMap&
          edge_entity_type_id_dictionary) {
    edge_entity_type_id_dictionary_ = edge_entity_type_id_dictionary;
  }

  void set_node_entity_type_id_name(
      const katana::EntityTypeIDToAtomicTypeNameMap& node_entity_type_id_name) {
    node_entity_type_id_name_ = node_entity_type_id_name;
  }

  void set_edge_entity_type_id_name(
      const katana::EntityTypeIDToAtomicTypeNameMap& edge_entity_type_id_name) {
    edge_entity_type_id_name_ = edge_entity_type_id_name;
  }

  std::vector<PropStorageInfo> part_prop_info_list_;
  std::vector<PropStorageInfo> node_prop_info_list_;
  std::vector<PropStorageInfo> edge_prop_info_list_;

  /// Metadata filled in by CuSP, or from storage (meta partition file)
  PartitionMetadata metadata_;

  /// tracks changes to json on disk structure of the PartitionHeader
  /// current one is defined by latest_storage_format_version_
  /// When a graph is loaded from file, this is overwritten with the loaded value
  /// When a graph is created in memory, this is updated on store
  uint32_t storage_format_version_ = 0;

  static const uint32_t kPartitionStorageFormatVersion1 = 1;
  static const uint32_t kPartitionStorageFormatVersion2 = 2;
  /// current_storage_format_version_ to be bumped any time
  /// the on disk format of RDGPartHeader changes
  uint32_t latest_storage_format_version_ = kPartitionStorageFormatVersion2;

  std::string topology_path_;

  std::string node_entity_type_id_array_path_;
  std::string edge_entity_type_id_array_path_;

  // entity_type_id_dictionary maps from Entity Type ID to set of  Atomic Entity Type IDs
  // if EntityTypeID is an Atomic Type ID, then the set is size 1 containing only itself
  // if EntityTypeID is a Combination Type ID, then the set contains all of the Atomic Entity Type IDs that make it
  tsuba::EntityTypeIDToSetOfEntityTypeIDsStorageMap
      node_entity_type_id_dictionary_;
  tsuba::EntityTypeIDToSetOfEntityTypeIDsStorageMap
      edge_entity_type_id_dictionary_;

  // entity_type_id_name maps from Atomic Entity Type ID to string name for the Entity Type ID
  katana::EntityTypeIDToAtomicTypeNameMap node_entity_type_id_name_;
  katana::EntityTypeIDToAtomicTypeNameMap edge_entity_type_id_name_;
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
