#include "RDGPartHeader.h"

#include <vector>

#include "Constants.h"
#include "GlobalState.h"
#include "PartitionTopologyMetadata.h"
#include "RDGHandleImpl.h"
#include "katana/ErrorCode.h"
#include "katana/Experimental.h"
#include "katana/FaultTest.h"
#include "katana/FileView.h"
#include "katana/Logging.h"
#include "katana/RDGStorageFormatVersion.h"
#include "katana/RDGTopology.h"
#include "katana/Result.h"

using json = nlohmann::json;

namespace {

const char* kTopologyPathKey = "kg.v1.topology.path";
const char* kNodePropertyKey = "kg.v1.node_property";
const char* kEdgePropertyKey = "kg.v1.edge_property";
const char* kPartPropertyFilesKey = "kg.v1.part_property_files";
const char* kPartPropertyMetaKey = "kg.v1.part_property_meta";
const char* kStorageFormatVersionKey = "kg.v1.storage_format_version";
const char* kUnstableStorageFormatFlagKey = "kg.v1.unstable_storage_format";
// Array file at path maps from Node ID to EntityTypeID of that Node
const char* kNodeEntityTypeIDArrayPathKey = "kg.v1.node_entity_type_id_array";
// Array file at path maps from Edge ID to EntityTypeID of that Edge
const char* kEdgeEntityTypeIDArrayPathKey = "kg.v1.edge_entity_type_id_array";
// Dictionary maps from Node Entity Type ID to set of Node Atomic Entity Type IDs
const char* kNodeEntityTypeIDDictionaryKey =
    "kg.v1.node_entity_type_id_dictionary";
// Dictionary maps from Edge Entity Type ID to set of Edge Atomic Entity Type IDs
const char* kEdgeEntityTypeIDDictionaryKey =
    "kg.v1.edge_entity_type_id_dictionary";
// Name maps from Node Entity Type ID to set of string names for the Node Entity Type ID
const char* kNodeEntityTypeIDNameKey = "kg.v1.node_entity_type_id_name";
// Name maps from Atomic Edge Entity Type ID to set of string names for the Edge Entity Type ID
const char* kEdgeEntityTypeIDNameKey = "kg.v1.edge_entity_type_id_name";
// Metadata object for partition topology entries
const char* kPartitionTopologyMetadataKey = "kg.v1.partition_topology_metadata";
// Set of topology entries
const char* kPartitionTopologyMetadataEntriesKey =
    "kg.v1.partition_topology_metadata_entries";
const char* kPartitionTopologyMetadataEntriesSizeKey =
    "kg.v1.partition_topology_metadata_entries_size";

//
//constexpr std::string_view  mirror_nodes_prop_name = "mirror_nodes";
//constexpr std::string_view  master_nodes_prop_name = "master_nodes";
//constexpr std::string_view  local_to_global_prop_name = "local_to_global_id";

// special partition property names

katana::Result<void>
CopyProperty(
    katana::PropStorageInfo* prop, const katana::Uri& old_location,
    const katana::Uri& new_location) {
  katana::Uri old_path = old_location.Join(prop->path());
  katana::Uri new_path = new_location.Join(prop->path());
  katana::FileView fv;

  KATANA_CHECKED(fv.Bind(old_path.string(), true));
  return katana::FileStore(new_path.string(), fv.ptr<uint8_t>(), fv.size());
}

katana::PropStorageInfo*
find_prop_info(
    const std::string& name, std::vector<katana::PropStorageInfo>* prop_infos) {
  auto it = std::find_if(
      prop_infos->begin(), prop_infos->end(),
      [&name](katana::PropStorageInfo& psi) { return psi.name() == name; });
  if (it == prop_infos->end()) {
    return nullptr;
  }

  return &(*it);
}

// TODO(vkarthik): repetitive code from RDGManifest, try to unify
katana::Result<uint64_t>
Parse(const std::string& str) {
  uint64_t val = strtoul(str.c_str(), nullptr, 10);
  if (val == ULONG_MAX && errno == ERANGE) {
    return KATANA_ERROR(
        katana::ResultErrno(), "manifest file found with out of range version");
  }
  return val;
}

// Regex for partition files
const std::regex kPartitionFile(
    "part_vers([0-9]+)_(rdg[0-9A-Za-z-]*)_node([0-9]+)$");
const int kPartitionMatchHostIndex = 3;

}  // namespace

katana::Result<katana::RDGPartHeader>
katana::RDGPartHeader::MakeJson(const katana::Uri& partition_path) {
  katana::FileView fv;
  KATANA_CHECKED(fv.Bind(partition_path.string(), true));

  if (fv.size() == 0) {
    return katana::RDGPartHeader();
  }

  katana::RDGPartHeader header;
  KATANA_CHECKED(katana::JsonParse<katana::RDGPartHeader>(fv, &header));

  return header;
}

katana::Result<katana::RDGPartHeader>
katana::RDGPartHeader::Make(const katana::Uri& partition_path) {
  return KATANA_CHECKED(MakeJson(partition_path));
}

katana::Result<void>
katana::RDGPartHeader::Write(
    katana::RDGHandle handle, katana::WriteGroup* writes,
    katana::RDG::RDGVersioningPolicy retain_version) const {
  std::string serialized = KATANA_CHECKED(katana::JsonDump(*this));

  // POSIX files end with newlines
  serialized = serialized + "\n";

  TSUBA_PTP(internal::FaultSensitivity::Normal);
  auto ff = std::make_unique<FileFrame>();
  KATANA_CHECKED(ff->Init(serialized.size()));
  if (auto res = ff->Write(serialized.data(), serialized.size()); !res.ok()) {
    return KATANA_ERROR(ArrowToKatana(res.code()), "arrow error: {}", res);
  }

  auto next_version =
      (retain_version == katana::RDG::RDGVersioningPolicy::RetainVersion)
          ? handle.impl_->rdg_manifest().version()
          : (handle.impl_->rdg_manifest().version() + 1);
  KATANA_LOG_DEBUG("Next verison: {}", next_version);
  ff->Bind(RDGManifest::PartitionFileName(
               handle.impl_->rdg_manifest().viewtype(),
               handle.impl_->rdg_manifest().dir(), Comm()->Rank, next_version)
               .string());

  writes->StartStore(std::move(ff));
  TSUBA_PTP(internal::FaultSensitivity::Normal);
  return katana::ResultSuccess();
}

katana::Result<uint64_t>
katana::RDGPartHeader::ParseHostFromPartitionFile(const std::string& file) {
  std::smatch sub_match;
  if (!std::regex_match(file, sub_match, kPartitionFile)) {
    return katana::ErrorCode::InvalidArgument;
  }
  return Parse(sub_match[kPartitionMatchHostIndex]);
}

bool
katana::RDGPartHeader::IsPartitionFileUri(const katana::Uri& uri) {
  bool res = std::regex_match(uri.BaseName(), kPartitionFile);
  return res;
}

bool
katana::RDGPartHeader::IsEntityTypeIDsOutsideProperties() const {
  return (storage_format_version_ >= kPartitionStorageFormatVersion2);
}

bool
katana::RDGPartHeader::IsUint16tEntityTypeIDs() const {
  return (storage_format_version_ >= kPartitionStorageFormatVersion3);
}

bool
katana::RDGPartHeader::IsMetadataOutsideTopologyFile() const {
  return (storage_format_version_ >= kPartitionStorageFormatVersion3);
}

katana::Result<void>
katana::RDGPartHeader::ValidateEntityTypeIDStructures() const {
  if (node_entity_type_id_array_path_.empty()) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "node_entity_type_id_array_path is empty");
  }

  if (edge_entity_type_id_array_path_.empty()) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "edge_entity_type_id_array_path is empty");
  }

  if (node_entity_type_id_dictionary_.empty()) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "node_entity_type_id_dictionary_ is empty");
  }

  if (edge_entity_type_id_dictionary_.empty()) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "edge_entity_type_id_dictionary_ is empty");
  }

  if (node_entity_type_id_name_.empty()) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "node_entity_type_id_name_ is empty");
  }

  if (edge_entity_type_id_name_.empty()) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "edge_entity_type_id_name_ is empty");
  }

  return katana::ResultSuccess();
}

katana::Result<void>
katana::RDGPartHeader::Validate() const {
  for (const auto& md : node_prop_info_list_) {
    if (md.path().find('/') != std::string::npos) {
      return KATANA_ERROR(
          ErrorCode::InvalidArgument,
          "node_property path doesn't contain a slash (/): {}", md.path());
    }
  }
  for (const auto& md : edge_prop_info_list_) {
    if (md.path().find('/') != std::string::npos) {
      return KATANA_ERROR(
          ErrorCode::InvalidArgument,
          "edge_property path doesn't contain a slash (/): {}", md.path());
    }
  }

  KATANA_CHECKED(topology_metadata_.Validate());

  if (IsEntityTypeIDsOutsideProperties()) {
    KATANA_CHECKED(ValidateEntityTypeIDStructures());
  }

  return katana::ResultSuccess();
}

katana::Result<void>
katana::RDGPartHeader::ChangeStorageLocation(
    const katana::Uri& old_location, const katana::Uri& new_location) {
  for (PropStorageInfo& prop : node_prop_info_list_) {
    if (prop.IsAbsent()) {
      KATANA_CHECKED(CopyProperty(&prop, old_location, new_location));
    } else {
      prop.WasModified(prop.type());
    }
  }
  for (PropStorageInfo& prop : edge_prop_info_list_) {
    if (prop.IsAbsent()) {
      KATANA_CHECKED(CopyProperty(&prop, old_location, new_location));
    } else {
      prop.WasModified(prop.type());
    }
  }
  for (PropStorageInfo& prop : part_prop_info_list_) {
    if (prop.IsAbsent()) {
      KATANA_CHECKED(CopyProperty(&prop, old_location, new_location));
    } else {
      prop.WasModified(prop.type());
    }
  }
  // clear out specific file paths so that we know to store them later
  node_entity_type_id_array_path_ = "";
  edge_entity_type_id_array_path_ = "";
  topology_metadata_.ChangeStorageLocation();

  return katana::ResultSuccess();
}

katana::PropStorageInfo*
katana::RDGPartHeader::find_node_prop_info(const std::string& name) {
  return find_prop_info(name, &node_prop_info_list());
}
katana::PropStorageInfo*
katana::RDGPartHeader::find_edge_prop_info(const std::string& name) {
  return find_prop_info(name, &edge_prop_info_list());
}
katana::PropStorageInfo*
katana::RDGPartHeader::find_part_prop_info(const std::string& name) {
  return find_prop_info(name, &part_prop_info_list());
}

// specialized PropStorageInfo vec transformation to avoid nulls in the output
void
katana::to_json(json& j, const std::vector<katana::PropStorageInfo>& vec_pmd) {
  j = json::array();
  for (const auto& pmd : vec_pmd) {
    j.push_back(pmd);
  }
}

void
katana::to_json(json& j, const katana::RDGPartHeader& header) {
  // ensure the part header flag and the env var flag are always in sync to prevent misuse
  if (KATANA_EXPERIMENTAL_ENABLED(UnstableRDGStorageFormat)) {
    if (header.unstable_storage_format_) {
      KATANA_LOG_WARN("Storing RDG in unstable format");
    }
    // else
    KATANA_LOG_VASSERT(
        header.unstable_storage_format_,
        "UnstableRDGStorageFormat env var is set, but "
        "RDGPartHeader.unstable_storage_format_ is false."
        "The UnstableRDGStorageFormat env var should only be set when working "
        "with features which require the unstable storage format");
  } else {
    KATANA_LOG_VASSERT(
        !header.unstable_storage_format_,
        "UnstableRDGStorageFormat env var is not set, but "
        "RDGPartHeader.unstable_storage_format_ is true");
  }

  j = json{
      {kNodePropertyKey, header.node_prop_info_list_},
      {kEdgePropertyKey, header.edge_prop_info_list_},
      {kPartPropertyFilesKey, header.part_prop_info_list_},
      {kPartPropertyMetaKey, header.metadata_},
      {kStorageFormatVersionKey, header.storage_format_version_},
      {kUnstableStorageFormatFlagKey, header.unstable_storage_format_},
      {kNodeEntityTypeIDArrayPathKey, header.node_entity_type_id_array_path_},
      {kEdgeEntityTypeIDArrayPathKey, header.edge_entity_type_id_array_path_},
      {kNodeEntityTypeIDDictionaryKey, header.node_entity_type_id_dictionary_},
      {kEdgeEntityTypeIDDictionaryKey, header.edge_entity_type_id_dictionary_},
      {kNodeEntityTypeIDNameKey, header.node_entity_type_id_name_},
      {kEdgeEntityTypeIDNameKey, header.edge_entity_type_id_name_},
      {kPartitionTopologyMetadataKey, header.topology_metadata_}};
}

void
katana::from_json(const json& j, katana::RDGPartHeader& header) {
  j.at(kNodePropertyKey).get_to(header.node_prop_info_list_);
  j.at(kEdgePropertyKey).get_to(header.edge_prop_info_list_);
  j.at(kPartPropertyFilesKey).get_to(header.part_prop_info_list_);
  j.at(kPartPropertyMetaKey).get_to(header.metadata_);

  /// Storage Format Version Handling

  // support loading "storage_format_version=1" RDGs, aka RDGs without
  // an explicit storage_format_version
  if (auto it = j.find(kStorageFormatVersionKey); it != j.end()) {
    it->get_to(header.storage_format_version_);
  } else {
    header.storage_format_version_ = kPartitionStorageFormatVersion1;
  }

  // load the unstable_storage_format flag if it is present in the RDG
  // RDGs created before support for unstable_storage_format was added do not have this flag.
  // If it is not present, the RDG is assumed to *NOT* be stored in an unstable_storage_format since the rdg was created before unstable formats were introduced
  if (auto it = j.find(kUnstableStorageFormatFlagKey); it != j.end()) {
    it->get_to(header.unstable_storage_format_);
  } else {
    header.unstable_storage_format_ = false;
  }

  // Ensure unstable_storage_format RDGs are not loaded when the feature flag is not set
  if (KATANA_EXPERIMENTAL_ENABLED(UnstableRDGStorageFormat)) {
    KATANA_LOG_WARN(
        "UnstableRDGStorageFormat is set. RDGs will be stored in an unstable "
        "storage format. Loading RDGs stored in an unstable storage format "
        "will be permitted.");
    if (header.unstable_storage_format_) {
      KATANA_LOG_WARN(
          "Loading an RDG stored in an unstable storage format. If any issues "
          "are encountered, please regenerate this RDG before filing any bugs, "
          "as the unstable storage format can change without notice.");
    }
  } else {
    if (header.unstable_storage_format_) {
      throw std::runtime_error(
          "Loaded graph is an RDG stored in an unstable storage format, but "
          "env var KATANA_ENABLE_EXPERIMENTAL='UnstableRDGStorageFormat' is "
          "not set"
          "Unstable storage formats can change without notice and are "
          "unsupported so should not be used outside of development\n"
          "If you know what you are doing and would like to load this RDG "
          "anyway, please set "
          "KATANA_ENABLE_EXPERIMENTAL='UnstableRDGStorageFormat' in your "
          "environment");
    }
  }

  // Handle the different storage_format_versions

  if (header.storage_format_version_ == kPartitionStorageFormatVersion2) {
    // Version 2 was found to be buggy,
    throw std::runtime_error(
        "Loaded graph is RDG storage_format_version 2 (aka RDG v2), which is "
        "not supported. Please re-import this graph to get an RDG with the "
        "most recent storage_format_version");
  }

  // Version 2 added entity type id files
  if (header.storage_format_version_ >= kPartitionStorageFormatVersion2) {
    j.at(kNodeEntityTypeIDArrayPathKey)
        .get_to(header.node_entity_type_id_array_path_);
    j.at(kEdgeEntityTypeIDArrayPathKey)
        .get_to(header.edge_entity_type_id_array_path_);
    j.at(kNodeEntityTypeIDDictionaryKey)
        .get_to(header.node_entity_type_id_dictionary_);
    j.at(kEdgeEntityTypeIDDictionaryKey)
        .get_to(header.edge_entity_type_id_dictionary_);
    j.at(kNodeEntityTypeIDNameKey).get_to(header.node_entity_type_id_name_);
    j.at(kEdgeEntityTypeIDNameKey).get_to(header.edge_entity_type_id_name_);
  }

  // Version 3 added topology metadata
  if (header.storage_format_version_ >= kPartitionStorageFormatVersion3) {
    j.at(kPartitionTopologyMetadataKey).get_to(header.topology_metadata_);
  } else {
    //make a new minimal entry from just the path so we can load the metadata later
    PartitionTopologyMetadataEntry entry = PartitionTopologyMetadataEntry();
    entry.topology_state_ = RDGTopology::TopologyKind::kCSR;
    entry.node_sort_state_ = RDGTopology::NodeSortKind::kAny;
    entry.edge_sort_state_ = RDGTopology::EdgeSortKind::kAny;
    if (header.metadata_.transposed_) {
      entry.transpose_state_ = RDGTopology::TransposeKind::kYes;
    } else {
      entry.transpose_state_ = RDGTopology::TransposeKind::kNo;
    }
    j.at(kTopologyPathKey).get_to(entry.path_);
    header.topology_metadata_.Append(entry);
  }
}

void
katana::to_json(json& j, const katana::PartitionMetadata& pmd) {
  j = json{
      {"magic", kPartitionMagicNo},
      {"policy_id", pmd.policy_id_},
      {"transposed", pmd.transposed_},
      {"is_outgoing_edge_cut", pmd.is_outgoing_edge_cut_},
      {"is_incoming_edge_cut", pmd.is_incoming_edge_cut_},
      {"num_global_nodes", pmd.num_global_nodes_},
      {"max_global_node_id", pmd.max_global_node_id_},
      {"num_global_edges", pmd.num_global_edges_},
      {"num_nodes", pmd.num_nodes_},
      {"num_edges", pmd.num_edges_},
      {"num_owned", pmd.num_owned_},
      {"cartesian_grid", pmd.cartesian_grid_}};
}

void
katana::from_json(const json& j, katana::PartitionMetadata& pmd) {
  uint32_t magic;
  j.at("magic").get_to(magic);

  j.at("policy_id").get_to(pmd.policy_id_);

  j.at("transposed").get_to(pmd.transposed_);
  j.at("is_outgoing_edge_cut").get_to(pmd.is_outgoing_edge_cut_);
  j.at("is_incoming_edge_cut").get_to(pmd.is_incoming_edge_cut_);
  j.at("num_global_nodes").get_to(pmd.num_global_nodes_);
  if (auto it = j.find("max_global_node_id"); it != j.end()) {
    it->get_to(pmd.max_global_node_id_);
  } else {
    pmd.max_global_node_id_ = pmd.num_global_nodes_ - 1;
  }
  j.at("num_global_edges").get_to(pmd.num_global_edges_);
  j.at("num_nodes").get_to(pmd.num_nodes_);
  j.at("num_edges").get_to(pmd.num_edges_);
  j.at("num_owned").get_to(pmd.num_owned_);
  j.at("cartesian_grid").get_to(pmd.cartesian_grid_);

  if (magic != kPartitionMagicNo) {
    // nlohmann::json reports errors using exceptions
    throw std::runtime_error("partition magic number mismatch");
  }
}

void
katana::from_json(const nlohmann::json& j, katana::PropStorageInfo& propmd) {
  j.at(0).get_to(propmd.name_);
  j.at(1).get_to(propmd.path_);
  propmd.state_ = PropStorageInfo::State::kAbsent;
}

void
katana::to_json(json& j, const katana::PropStorageInfo& propmd) {
  j = json{propmd.name(), propmd.path()};
}

void
katana::from_json(
    const nlohmann::json& j, katana::PartitionTopologyMetadataEntry& topo) {
  j.at("path").get_to(topo.path_);
  if (topo.path_.empty()) {
    throw std::runtime_error("loaded topology with empty path");
  }

  j.at("num_nodes").get_to(topo.num_nodes_);
  j.at("num_edges").get_to(topo.num_edges_);
  j.at("edge_index_to_property_index_map_present")
      .get_to(topo.edge_index_to_property_index_map_present_);
  j.at("node_index_to_property_index_map_present")
      .get_to(topo.node_index_to_property_index_map_present_);
  j.at("edge_condensed_type_id_map_present")
      .get_to(topo.edge_condensed_type_id_map_present_);
  j.at("edge_condensed_type_id_map_size")
      .get_to(topo.edge_condensed_type_id_map_size_);
  j.at("node_condensed_type_id_map_present")
      .get_to(topo.node_condensed_type_id_map_present_);
  j.at("node_condensed_type_id_map_size")
      .get_to(topo.node_condensed_type_id_map_size_);
  j.at("topology_state").get_to(topo.topology_state_);
  j.at("transpose_state").get_to(topo.transpose_state_);
  j.at("edge_sort_state").get_to(topo.edge_sort_state_);
  j.at("node_sort_state").get_to(topo.node_sort_state_);
  KATANA_LOG_DEBUG(
      "read topology with: topology_state={}, transpose_state={}, "
      "edge_sort_state={}, node_sort_state={}",
      topo.topology_state_, topo.transpose_state_, topo.edge_sort_state_,
      topo.node_sort_state_);
}

void
katana::to_json(json& j, const katana::PartitionTopologyMetadataEntry& topo) {
  KATANA_LOG_VASSERT(
      !topo.path_.empty(), "tried to store topology with empty path");

  KATANA_LOG_ASSERT(
      topo.topology_state_ != katana::RDGTopology::TopologyKind::kInvalid);
  KATANA_LOG_ASSERT(
      topo.transpose_state_ != katana::RDGTopology::TransposeKind::kInvalid);
  KATANA_LOG_VASSERT(
      topo.transpose_state_ != katana::RDGTopology::TransposeKind::kAny,
      "Cannot store a TransposeKind::kAny topology");
  KATANA_LOG_ASSERT(
      topo.edge_sort_state_ != katana::RDGTopology::EdgeSortKind::kInvalid);
  KATANA_LOG_ASSERT(
      topo.node_sort_state_ != katana::RDGTopology::NodeSortKind::kInvalid);

  j = json{
      {"path", topo.path_},
      {"num_edges", topo.num_edges_},
      {"num_nodes", topo.num_nodes_},
      {"edge_index_to_property_index_map_present",
       topo.edge_index_to_property_index_map_present_},
      {"node_index_to_property_index_map_present",
       topo.node_index_to_property_index_map_present_},
      {"edge_condensed_type_id_map_present",
       topo.edge_condensed_type_id_map_present_},
      {"edge_condensed_type_id_map_size",
       topo.edge_condensed_type_id_map_size_},
      {"edge_condensed_type_id_map_size",
       topo.edge_condensed_type_id_map_size_},
      {"node_condensed_type_id_map_size",
       topo.node_condensed_type_id_map_size_},
      {"node_condensed_type_id_map_present",
       topo.node_condensed_type_id_map_present_},
      {"topology_state", topo.topology_state_},
      {"transpose_state", topo.transpose_state_},
      {"edge_sort_state", topo.edge_sort_state_},
      {"node_sort_state", topo.node_sort_state_}};

  KATANA_LOG_DEBUG(
      "stored topology with: topology_state={}, transpose_state={}, "
      "edge_sort_state={}, node_sort_state={}",
      topo.topology_state_, topo.transpose_state_, topo.edge_sort_state_,
      topo.node_sort_state_);
}

void
katana::from_json(
    const nlohmann::json& j, katana::PartitionTopologyMetadata& topomd) {
  uint32_t tmp_num;

  // initally parse json into vector so we can verify the number of elements
  std::vector<PartitionTopologyMetadataEntry> entries_vec;
  j.at(kPartitionTopologyMetadataEntriesSizeKey).get_to(tmp_num);
  topomd.set_num_entries(tmp_num);
  j.at(kPartitionTopologyMetadataEntriesKey).get_to(entries_vec);
  if (size(entries_vec) != topomd.num_entries()) {
    throw std::runtime_error("invalid file");
  }

  // move the vector contents into our entries array
  std::copy_n(
      std::make_move_iterator(entries_vec.begin()), topomd.num_entries_,
      topomd.entries_.begin());
}

void
katana::to_json(json& j, const katana::PartitionTopologyMetadata& topomd) {
  KATANA_LOG_ASSERT(topomd.num_entries_ >= 1);
  KATANA_LOG_VERBOSE(
      "storing {} PartitionTopologyMetadata entries", topomd.num_entries_);

  // if we store the array, we will always store kMaxNumTopologies since the json parser has no way
  // of telling if an entry is present, valid, or actually just empty. Wrap in a vector to avoid this and only store
  // the topologies we actually have present and valid
  std::vector<PartitionTopologyMetadataEntry> entries_vec;
  size_t num_valid_entries = 0;
  for (auto it = topomd.Entries().begin();
       it != topomd.Entries().begin() + topomd.num_entries_; ++it) {
    if (it->invalid_) {
      continue;
    }
    entries_vec.emplace_back(*it);
    num_valid_entries++;
  }

  j = json{
      {kPartitionTopologyMetadataEntriesSizeKey, num_valid_entries},
      {kPartitionTopologyMetadataEntriesKey, entries_vec},
  };
}
