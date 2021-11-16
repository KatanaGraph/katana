#include "RDGPartHeader.h"

#include <vector>

#include "Constants.h"
#include "GlobalState.h"
#include "PartitionTopologyMetadata.h"
#include "RDGHandleImpl.h"
#include "katana/Logging.h"
#include "katana/Result.h"
#include "tsuba/Errors.h"
#include "tsuba/FaultTest.h"
#include "tsuba/FileView.h"
#include "tsuba/RDGTopology.h"

using json = nlohmann::json;

namespace {

const char* kTopologyPathKey = "kg.v1.topology.path";
const char* kNodePropertyKey = "kg.v1.node_property";
const char* kEdgePropertyKey = "kg.v1.edge_property";
const char* kPartPropertyFilesKey = "kg.v1.part_property_files";
const char* kPartPropertyMetaKey = "kg.v1.part_property_meta";
const char* kStorageFormatVersionKey = "kg.v1.storage_format_version";
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
    tsuba::PropStorageInfo* prop, const katana::Uri& old_location,
    const katana::Uri& new_location) {
  katana::Uri old_path = old_location.Join(prop->path());
  katana::Uri new_path = new_location.Join(prop->path());
  tsuba::FileView fv;

  KATANA_CHECKED(fv.Bind(old_path.string(), true));
  return tsuba::FileStore(new_path.string(), fv.ptr<uint8_t>(), fv.size());
}

}  // namespace

// TODO(vkarthik): repetitive code from RDGManifest, try to unify
namespace {

katana::Result<uint64_t>
Parse(const std::string& str) {
  uint64_t val = strtoul(str.c_str(), nullptr, 10);
  if (val == ULONG_MAX && errno == ERANGE) {
    return KATANA_ERROR(
        katana::ResultErrno(), "manifest file found with out of range version");
  }
  return val;
}

}  // namespace

namespace tsuba {

// Regex for partition files
const std::regex kPartitionFile(
    "part_vers([0-9]+)_(rdg[0-9A-Za-z-]*)_node([0-9]+)$");
const int kPartitionMatchHostIndex = 3;

katana::Result<RDGPartHeader>
RDGPartHeader::MakeJson(const katana::Uri& partition_path) {
  tsuba::FileView fv;
  KATANA_CHECKED(fv.Bind(partition_path.string(), true));

  if (fv.size() == 0) {
    return tsuba::RDGPartHeader();
  }

  tsuba::RDGPartHeader header;
  KATANA_CHECKED(katana::JsonParse<tsuba::RDGPartHeader>(fv, &header));

  return header;
}

katana::Result<RDGPartHeader>
RDGPartHeader::Make(const katana::Uri& partition_path) {
  return KATANA_CHECKED(MakeJson(partition_path));
}

katana::Result<void>
RDGPartHeader::Write(
    RDGHandle handle, WriteGroup* writes,
    RDG::RDGVersioningPolicy retain_version) const {
  std::string serialized = KATANA_CHECKED(katana::JsonDump(*this));

  // POSIX files end with newlines
  serialized = serialized + "\n";

  TSUBA_PTP(internal::FaultSensitivity::Normal);
  auto ff = std::make_unique<FileFrame>();
  KATANA_CHECKED(ff->Init(serialized.size()));
  if (auto res = ff->Write(serialized.data(), serialized.size()); !res.ok()) {
    return KATANA_ERROR(ArrowToTsuba(res.code()), "arrow error: {}", res);
  }

  auto next_version =
      (retain_version == tsuba::RDG::RDGVersioningPolicy::RetainVersion)
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
RDGPartHeader::ParseHostFromPartitionFile(const std::string& file) {
  std::smatch sub_match;
  if (!std::regex_match(file, sub_match, kPartitionFile)) {
    return tsuba::ErrorCode::InvalidArgument;
  }
  return Parse(sub_match[kPartitionMatchHostIndex]);
}

bool
RDGPartHeader::IsPartitionFileUri(const katana::Uri& uri) {
  bool res = std::regex_match(uri.BaseName(), kPartitionFile);
  return res;
}

bool
RDGPartHeader::IsEntityTypeIDsOutsideProperties() const {
  return (storage_format_version_ >= kPartitionStorageFormatVersion2);
}

bool
RDGPartHeader::IsUint16tEntityTypeIDs() const {
  return (storage_format_version_ >= kPartitionStorageFormatVersion3);
}

bool
RDGPartHeader::IsMetadataOutsideTopologyFile() const {
  return (storage_format_version_ >= kPartitionStorageFormatVersion3);
}

katana::Result<void>
RDGPartHeader::ValidateEntityTypeIDStructures() const {
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
RDGPartHeader::Validate() const {
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
RDGPartHeader::ChangeStorageLocation(
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

}  // namespace tsuba

// specialized PropStorageInfo vec transformation to avoid nulls in the output
void
tsuba::to_json(json& j, const std::vector<tsuba::PropStorageInfo>& vec_pmd) {
  j = json::array();
  for (const auto& pmd : vec_pmd) {
    j.push_back(pmd);
  }
}

void
tsuba::to_json(json& j, const tsuba::RDGPartHeader& header) {
  j = json{
      {kNodePropertyKey, header.node_prop_info_list_},
      {kEdgePropertyKey, header.edge_prop_info_list_},
      {kPartPropertyFilesKey, header.part_prop_info_list_},
      {kPartPropertyMetaKey, header.metadata_},
      {kStorageFormatVersionKey, header.storage_format_version_},
      {kNodeEntityTypeIDArrayPathKey, header.node_entity_type_id_array_path_},
      {kEdgeEntityTypeIDArrayPathKey, header.edge_entity_type_id_array_path_},
      {kNodeEntityTypeIDDictionaryKey, header.node_entity_type_id_dictionary_},
      {kEdgeEntityTypeIDDictionaryKey, header.edge_entity_type_id_dictionary_},
      {kNodeEntityTypeIDNameKey, header.node_entity_type_id_name_},
      {kEdgeEntityTypeIDNameKey, header.edge_entity_type_id_name_},
      {kPartitionTopologyMetadataKey, header.topology_metadata_}};
}

void
tsuba::from_json(const json& j, tsuba::RDGPartHeader& header) {
  j.at(kNodePropertyKey).get_to(header.node_prop_info_list_);
  j.at(kEdgePropertyKey).get_to(header.edge_prop_info_list_);
  j.at(kPartPropertyFilesKey).get_to(header.part_prop_info_list_);
  j.at(kPartPropertyMetaKey).get_to(header.metadata_);

  if (auto it = j.find(kStorageFormatVersionKey); it != j.end()) {
    it->get_to(header.storage_format_version_);
  } else {
    header.storage_format_version_ =
        RDGPartHeader::kPartitionStorageFormatVersion1;
  }

  // Version 2 was found to be buggy,
  KATANA_LOG_VASSERT(
      header.storage_format_version_ !=
          RDGPartHeader::kPartitionStorageFormatVersion2,
      "Loaded graph is RDG storage_format_version 2 (aka RDG v2), which is not "
      "supported. "
      "Please re-import this graph to get an RDG with the most recent "
      "storage_format_version");

  // Version 2 added entity type id files
  if (header.storage_format_version_ >=
      RDGPartHeader::kPartitionStorageFormatVersion2) {
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
  if (header.storage_format_version_ >=
      RDGPartHeader::kPartitionStorageFormatVersion3) {
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
tsuba::to_json(json& j, const tsuba::PartitionMetadata& pmd) {
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
tsuba::from_json(const json& j, tsuba::PartitionMetadata& pmd) {
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
    throw std::runtime_error("Partition magic number mismatch");
  }
}

void
tsuba::from_json(const nlohmann::json& j, tsuba::PropStorageInfo& propmd) {
  j.at(0).get_to(propmd.name_);
  j.at(1).get_to(propmd.path_);
  propmd.state_ = PropStorageInfo::State::kAbsent;
}

void
tsuba::to_json(json& j, const tsuba::PropStorageInfo& propmd) {
  j = json{propmd.name(), propmd.path()};
}

void
tsuba::from_json(
    const nlohmann::json& j, tsuba::PartitionTopologyMetadataEntry& topo) {
  j.at("path").get_to(topo.path_);
  KATANA_LOG_VASSERT(!topo.path_.empty(), "loaded topology with empty path");
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
tsuba::to_json(json& j, const tsuba::PartitionTopologyMetadataEntry& topo) {
  KATANA_LOG_VASSERT(
      !topo.path_.empty(), "tried to store topology with empty path");

  KATANA_LOG_ASSERT(
      topo.topology_state_ != tsuba::RDGTopology::TopologyKind::kInvalid);
  KATANA_LOG_ASSERT(
      topo.transpose_state_ != tsuba::RDGTopology::TransposeKind::kInvalid);
  KATANA_LOG_VASSERT(
      topo.transpose_state_ != tsuba::RDGTopology::TransposeKind::kAny,
      "Cannot store a TransposeKind::kAny topology");
  KATANA_LOG_ASSERT(
      topo.edge_sort_state_ != tsuba::RDGTopology::EdgeSortKind::kInvalid);
  KATANA_LOG_ASSERT(
      topo.node_sort_state_ != tsuba::RDGTopology::NodeSortKind::kInvalid);

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
tsuba::from_json(
    const nlohmann::json& j, tsuba::PartitionTopologyMetadata& topomd) {
  uint32_t tmp_num;

  // initally parse json into vector so we can verify the number of elements
  std::vector<PartitionTopologyMetadataEntry> entries_vec;
  j.at(kPartitionTopologyMetadataEntriesSizeKey).get_to(tmp_num);
  topomd.set_num_entries(tmp_num);
  j.at(kPartitionTopologyMetadataEntriesKey).get_to(entries_vec);
  KATANA_LOG_ASSERT(size(entries_vec) == topomd.num_entries());

  // move the vector contents into our entries array
  std::copy_n(
      std::make_move_iterator(entries_vec.begin()), topomd.num_entries_,
      topomd.entries_.begin());
}

void
tsuba::to_json(json& j, const tsuba::PartitionTopologyMetadata& topomd) {
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
