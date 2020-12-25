#include "RDGPartHeader.h"

#include "Constants.h"
#include "GlobalState.h"
#include "RDGHandleImpl.h"
#include "galois/Logging.h"
#include "tsuba/Errors.h"
#include "tsuba/FaultTest.h"
#include "tsuba/FileView.h"

template <typename T>
using Result = galois::Result<T>;
using json = nlohmann::json;

namespace {

// TODO (witchel) these key are deprecated as part of parquet
const char* kTopologyPathKey = "kg.v1.topology.path";
const char* kNodePropertyPathKey = "kg.v1.node_property.path";
const char* kNodePropertyNameKey = "kg.v1.node_property.name";
const char* kEdgePropertyPathKey = "kg.v1.edge_property.path";
const char* kEdgePropertyNameKey = "kg.v1.edge_property.name";
const char* kPartPropertyPathKey = "kg.v1.part_property.path";
const char* kPartPropertyNameKey = "kg.v1.part_property.name";
const char* kPartOtherMetadataKey = "kg.v1.other_part_metadata.key";

const char* kNodePropertyKey = "kg.v1.node_property";
const char* kEdgePropertyKey = "kg.v1.edge_property";
const char* kPartPropertyFilesKey = "kg.v1.part_property_files";
const char* kPartProperyMetaKey = "kg.v1.part_property_meta";
//
//constexpr std::string_view  mirror_nodes_prop_name = "mirror_nodes";
//constexpr std::string_view  master_nodes_prop_name = "master_nodes";
//constexpr std::string_view  local_to_global_prop_name = "local_to_global_vector";

// special partition property names

// TODO (witchel) Deprecated.  Remove with ReadMetadataParquet, below
galois::Result<std::vector<tsuba::PropStorageInfo>>
MakeProperties(std::vector<std::string>&& values) {
  std::vector v = std::move(values);

  if ((v.size() % 2) != 0) {
    GALOIS_LOG_DEBUG("failed: number of values {} is not even", v.size());
    return tsuba::ErrorCode::InvalidArgument;
  }

  std::vector<tsuba::PropStorageInfo> prop_info_list;
  std::unordered_set<std::string> names;
  prop_info_list.reserve(v.size() / 2);

  for (size_t i = 0, n = v.size(); i < n; i += 2) {
    const auto& name = v[i];
    const auto& path = v[i + 1];

    names.insert(name);

    prop_info_list.emplace_back(tsuba::PropStorageInfo{
        .name = name,
        .path = path,
    });
  }

  assert(names.size() == prop_info_list.size());

  return prop_info_list;
}

}  // namespace

namespace tsuba {

// TODO (witchel) Deprecated.  Remove when input graphs don't use parquet metadata
/// ReadMetadata reads metadata from a Parquet file and returns the extracted
/// property graph specific fields as well as the unparsed fields.
///
/// The order of metadata fields is significant, and repeated metadata fields
/// are used to encode lists of values.
galois::Result<RDGPartHeader>
RDGPartHeader::MakeParquet(const galois::Uri& partition_path) {
  auto fv = std::make_shared<FileView>();
  if (auto res = fv->Bind(partition_path.string(), false); !res) {
    GALOIS_LOG_DEBUG(
        "cannot open {}: {}", partition_path.string(), res.error());
    return res.error();
  }

  if (fv->size() == 0) {
    return RDGPartHeader{};
  }

  std::shared_ptr<parquet::FileMetaData> md;
  try {
    md = parquet::ReadMetaData(fv);
  } catch (const std::exception& exp) {
    GALOIS_LOG_DEBUG(
        "arrow error reading {}: {}", partition_path.string(), exp.what());
    return ErrorCode::ArrowError;
  }

  const std::shared_ptr<const arrow::KeyValueMetadata>& kv_metadata =
      md->key_value_metadata();

  if (!kv_metadata) {
    return ErrorCode::InvalidArgument;
  }

  std::vector<std::string> node_values;
  std::vector<std::string> edge_values;
  std::vector<std::string> part_values;
  std::vector<std::pair<std::string, std::string>> other_metadata;
  std::string topology_path;
  for (int64_t i = 0, n = kv_metadata->size(); i < n; ++i) {
    const std::string& k = kv_metadata->key(i);
    const std::string& v = kv_metadata->value(i);

    if (k == kNodePropertyPathKey || k == kNodePropertyNameKey) {
      node_values.emplace_back(v);
    } else if (k == kEdgePropertyPathKey || k == kEdgePropertyNameKey) {
      edge_values.emplace_back(v);
    } else if (k == kPartPropertyPathKey || k == kPartPropertyNameKey) {
      part_values.emplace_back(v);
    } else if (k == kTopologyPathKey) {
      if (!topology_path.empty()) {
        return ErrorCode::InvalidArgument;
      }
      topology_path = v;
    } else {
      other_metadata.emplace_back(std::make_pair(k, v));
    }
  }

  auto node_prop_info_list_result = MakeProperties(std::move(node_values));
  if (!node_prop_info_list_result) {
    return node_prop_info_list_result.error();
  }

  auto edge_prop_info_list_result = MakeProperties(std::move(edge_values));
  if (!edge_prop_info_list_result) {
    return edge_prop_info_list_result.error();
  }

  auto part_prop_info_list_result = MakeProperties(std::move(part_values));
  if (!part_prop_info_list_result) {
    return part_prop_info_list_result.error();
  }

  // these are always persisted
  for (auto& prop : part_prop_info_list_result.value()) {
    prop.persist = true;
  }

  PartitionMetadata part_metadata;
  for (const auto& [k, v] : other_metadata) {
    if (k == kPartOtherMetadataKey) {
      if (auto res = galois::JsonParse(v, &part_metadata); !res) {
        return res.error();
      }
    }
  }

  RDGPartHeader header;

  header.node_prop_info_list_ = std::move(node_prop_info_list_result.value());
  header.edge_prop_info_list_ = std::move(edge_prop_info_list_result.value());
  header.part_prop_info_list_ = std::move(part_prop_info_list_result.value());
  header.topology_path_ = std::move(topology_path);
  header.metadata_ = std::move(part_metadata);

  return RDGPartHeader(std::move(header));
}

galois::Result<RDGPartHeader>
RDGPartHeader::MakeJson(const galois::Uri& partition_path) {
  tsuba::FileView fv;
  if (auto res = fv.Bind(partition_path.string(), true); !res) {
    return res.error();
  }
  if (fv.size() == 0) {
    return tsuba::RDGPartHeader();
  }

  tsuba::RDGPartHeader header;
  auto json_res = galois::JsonParse<tsuba::RDGPartHeader>(fv, &header);
  if (!json_res) {
    return json_res.error();
  }
  return header;
}

galois::Result<RDGPartHeader>
RDGPartHeader::Make(const galois::Uri& partition_path) {
  galois::Result<RDGPartHeader> res = MakeJson(partition_path);
  if (res) {
    return res;
  }

  GALOIS_LOG_ERROR("failed to parse JSON RDGPartHeader: {}", res.error());
  GALOIS_LOG_ERROR("falling back on Parquet (deprecated)");

  try {
    return MakeParquet(partition_path);
  } catch (const std::exception& exp) {
    GALOIS_LOG_DEBUG("arrow exception: {}", exp.what());
    return ErrorCode::ArrowError;
  }
}

Result<void>
RDGPartHeader::Write(RDGHandle handle, WriteGroup* writes) const {
  auto serialized_res = galois::JsonDump(*this);
  if (!serialized_res) {
    return serialized_res.error();
  }

  std::string serialized = std::move(serialized_res.value());

  // POSIX files end with newlines
  serialized = serialized + "\n";

  TSUBA_PTP(internal::FaultSensitivity::Normal);
  auto ff = std::make_unique<FileFrame>();
  if (auto res = ff->Init(serialized.size()); !res) {
    return res.error();
  }
  if (auto res = ff->Write(serialized.data(), serialized.size()); !res.ok()) {
    GALOIS_LOG_DEBUG("arrow error: {}", res);
    return ArrowToTsuba(res.code());
  }

  ff->Bind(RDGMeta::PartitionFileName(
               handle.impl_->rdg_meta().dir(), Comm()->ID,
               handle.impl_->rdg_meta().version() + 1)
               .string());

  writes->StartStore(std::move(ff));
  TSUBA_PTP(internal::FaultSensitivity::Normal);
  return galois::ResultSuccess();
}

// const * so that they are nullable
galois::Result<void>
RDGPartHeader::PrunePropsTo(
    const std::vector<std::string>* node_properties,
    const std::vector<std::string>* edge_properties) {
  if (node_properties != nullptr) {
    std::unordered_map<std::string, const PropStorageInfo&> node_paths;
    for (const PropStorageInfo& m : node_prop_info_list_) {
      node_paths.insert({m.name, m});
    }

    std::vector<PropStorageInfo> next_node_prop_info_list;
    for (const std::string& s : *node_properties) {
      auto it = node_paths.find(s);
      if (it == node_paths.end()) {
        GALOIS_LOG_DEBUG("failed: node property `{}` not found", s);
        return ErrorCode::PropertyNotFound;
      }

      next_node_prop_info_list.emplace_back(it->second);
    }
    node_prop_info_list_ = next_node_prop_info_list;
  }

  if (edge_properties != nullptr) {
    std::unordered_map<std::string, const PropStorageInfo&> edge_paths;
    for (const PropStorageInfo& m : edge_prop_info_list_) {
      edge_paths.insert({m.name, m});
    }

    std::vector<PropStorageInfo> next_edge_prop_info_list;
    for (const std::string& s : *edge_properties) {
      auto it = edge_paths.find(s);
      if (it == edge_paths.end()) {
        GALOIS_LOG_DEBUG("failed: edge property `{}` not found", s);
        return ErrorCode::PropertyNotFound;
      }

      next_edge_prop_info_list.emplace_back(it->second);
    }
    edge_prop_info_list_ = next_edge_prop_info_list;
  }
  return galois::ResultSuccess();
}

Result<void>
RDGPartHeader::Validate() const {
  for (const auto& md : node_prop_info_list_) {
    if (md.path.find('/') != std::string::npos) {
      GALOIS_LOG_DEBUG(
          "failed: node_property path doesn't contain a slash: \"{}\"",
          md.path);
      return ErrorCode::InvalidArgument;
    }
  }
  for (const auto& md : edge_prop_info_list_) {
    if (md.path.find('/') != std::string::npos) {
      GALOIS_LOG_DEBUG(
          "failed: edge_property path doesn't contain a slash: \"{}\"",
          md.path);
      return ErrorCode::InvalidArgument;
    }
  }
  if (topology_path_.empty()) {
    GALOIS_LOG_DEBUG("failed: topology_path: \"{}\" is empty", topology_path_);
    return ErrorCode::InvalidArgument;
  }
  if (topology_path_.find('/') != std::string::npos) {
    GALOIS_LOG_DEBUG(
        "failed: topology_path doesn't contain a slash: \"{}\"",
        topology_path_);
    return ErrorCode::InvalidArgument;
  }
  return galois::ResultSuccess();
}

void
RDGPartHeader::MarkAllPropertiesPersistent() {
  std::for_each(
      node_prop_info_list_.begin(), node_prop_info_list_.end(),
      [](auto& p) { return p.persist = true; });

  std::for_each(
      edge_prop_info_list_.begin(), edge_prop_info_list_.end(),
      [](auto& p) { return p.persist = true; });
}

Result<void>
RDGPartHeader::MarkNodePropertiesPersistent(
    const std::vector<std::string>& persist_node_props) {
  if (persist_node_props.size() > node_prop_info_list_.size()) {
    GALOIS_LOG_DEBUG(
        "failed: persist props sz: {} names, rdg.node_prop_info_list_ sz: {}",
        persist_node_props.size(), node_prop_info_list_.size());
    return ErrorCode::InvalidArgument;
  }
  for (uint32_t i = 0; i < persist_node_props.size(); ++i) {
    if (!persist_node_props[i].empty()) {
      node_prop_info_list_[i].name = persist_node_props[i];
      node_prop_info_list_[i].path = "";
      node_prop_info_list_[i].persist = true;
      GALOIS_LOG_DEBUG("node persist {}", node_prop_info_list_[i].name);
    }
  }
  return galois::ResultSuccess();
}

Result<void>
RDGPartHeader::MarkEdgePropertiesPersistent(
    const std::vector<std::string>& persist_edge_props) {
  if (persist_edge_props.size() > edge_prop_info_list_.size()) {
    GALOIS_LOG_DEBUG(
        "failed: persist props sz: {} names, rdg.edge_prop_info_list_ sz: {}",
        persist_edge_props.size(), edge_prop_info_list_.size());
    return ErrorCode::InvalidArgument;
  }
  for (uint32_t i = 0; i < persist_edge_props.size(); ++i) {
    if (!persist_edge_props[i].empty()) {
      edge_prop_info_list_[i].name = persist_edge_props[i];
      edge_prop_info_list_[i].path = "";
      edge_prop_info_list_[i].persist = true;
      GALOIS_LOG_DEBUG("edge persist {}", edge_prop_info_list_[i].name);
    }
  }
  return galois::ResultSuccess();
}

void
RDGPartHeader::UnbindFromStorage() {
  for (PropStorageInfo& prop : node_prop_info_list_) {
    prop.path = "";
  }
  for (PropStorageInfo& prop : edge_prop_info_list_) {
    prop.path = "";
  }
  for (PropStorageInfo& prop : part_prop_info_list_) {
    prop.path = "";
  }
  topology_path_ = "";
}

}  // namespace tsuba

// specialized PropStorageInfo vec transformation to avoid nulls in the output
void
tsuba::to_json(json& j, const std::vector<tsuba::PropStorageInfo>& vec_pmd) {
  j = json::array();
  for (const auto& pmd : vec_pmd) {
    if (pmd.persist) {
      j.push_back(pmd);
    }
  }
}

void
tsuba::to_json(json& j, const tsuba::RDGPartHeader& header) {
  j = json{
      {kTopologyPathKey, header.topology_path_},
      {kNodePropertyKey, header.node_prop_info_list_},
      {kEdgePropertyKey, header.edge_prop_info_list_},
      {kPartPropertyFilesKey, header.part_prop_info_list_},
      {kPartProperyMetaKey, header.metadata_},
  };
}

void
tsuba::from_json(const json& j, tsuba::RDGPartHeader& header) {
  j.at(kTopologyPathKey).get_to(header.topology_path_);
  j.at(kNodePropertyKey).get_to(header.node_prop_info_list_);
  j.at(kEdgePropertyKey).get_to(header.edge_prop_info_list_);
  j.at(kPartPropertyFilesKey).get_to(header.part_prop_info_list_);
  j.at(kPartProperyMetaKey).get_to(header.metadata_);
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
      {"num_global_edges", pmd.num_global_edges_},
      {"num_nodes", pmd.num_nodes_},
      {"num_edges", pmd.num_edges_},
      {"num_owned", pmd.num_owned_},
      {"num_nodes_with_edges", pmd.num_nodes_with_edges_},
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
  j.at("num_global_edges").get_to(pmd.num_global_edges_);
  j.at("num_nodes").get_to(pmd.num_nodes_);
  j.at("num_edges").get_to(pmd.num_edges_);
  j.at("num_owned").get_to(pmd.num_owned_);
  j.at("num_nodes_with_edges").get_to(pmd.num_nodes_with_edges_);
  j.at("cartesian_grid").get_to(pmd.cartesian_grid_);

  if (magic != kPartitionMagicNo) {
    // nlohmann::json reports errors using exceptions
    throw std::runtime_error("Partition magic number mismatch");
  }
}

void
tsuba::from_json(const nlohmann::json& j, tsuba::PropStorageInfo& propmd) {
  j.at(0).get_to(propmd.name);
  j.at(1).get_to(propmd.path);
}

void
tsuba::to_json(json& j, const tsuba::PropStorageInfo& propmd) {
  if (propmd.persist) {
    j = json{propmd.name, propmd.path};
  }
  // creates a null value if property wasn't supposed to be persisted
}
