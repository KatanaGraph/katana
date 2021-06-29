#include "RDGPartHeader.h"

#include "Constants.h"
#include "GlobalState.h"
#include "RDGHandleImpl.h"
#include "katana/Logging.h"
#include "katana/Result.h"
#include "tsuba/Errors.h"
#include "tsuba/FaultTest.h"
#include "tsuba/FileView.h"

using json = nlohmann::json;

namespace {

// TODO (witchel) these key are deprecated as part of parquet
const char* kTopologyPathKey = "kg.v1.topology.path";
const char* kNodePropertyKey = "kg.v1.node_property";
const char* kEdgePropertyKey = "kg.v1.edge_property";
const char* kPartPropertyFilesKey = "kg.v1.part_property_files";
const char* kPartProperyMetaKey = "kg.v1.part_property_meta";
//
//constexpr std::string_view  mirror_nodes_prop_name = "mirror_nodes";
//constexpr std::string_view  master_nodes_prop_name = "master_nodes";
//constexpr std::string_view  local_to_global_prop_name = "local_to_global_id";

// special partition property names

}  // namespace

namespace tsuba {

katana::Result<RDGPartHeader>
RDGPartHeader::MakeJson(const katana::Uri& partition_path) {
  tsuba::FileView fv;
  if (auto res = fv.Bind(partition_path.string(), true); !res) {
    return res.error();
  }
  if (fv.size() == 0) {
    return tsuba::RDGPartHeader();
  }

  tsuba::RDGPartHeader header;
  auto json_res = katana::JsonParse<tsuba::RDGPartHeader>(fv, &header);
  if (!json_res) {
    return json_res.error();
  }
  return header;
}

katana::Result<RDGPartHeader>
RDGPartHeader::Make(const katana::Uri& partition_path) {
  katana::Result<RDGPartHeader> res = MakeJson(partition_path);
  if (!res) {
    return res.error();
  }
  return res;
}

katana::Result<void>
RDGPartHeader::Write(
    RDGHandle handle, WriteGroup* writes,
    RDG::RDGVersioningPolicy retain_version) const {
  auto serialized_res = katana::JsonDump(*this);
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
    return KATANA_ERROR(ArrowToTsuba(res.code()), "arrow error: {}", res);
  }

  auto next_version =
      (retain_version == tsuba::RDG::RDGVersioningPolicy::RetainVersion)
          ? handle.impl_->rdg_manifest().version()
          : (handle.impl_->rdg_manifest().version() + 1);
  KATANA_LOG_DEBUG("Next verison: {}", next_version);
  ff->Bind(RDGManifest::PartitionFileName(
               handle.impl_->rdg_manifest().viewtype(),
               handle.impl_->rdg_manifest().dir(), Comm()->ID, next_version)
               .string());

  writes->StartStore(std::move(ff));
  TSUBA_PTP(internal::FaultSensitivity::Normal);
  return katana::ResultSuccess();
}

// const * so that they are nullable
katana::Result<void>
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
        return KATANA_ERROR(
            ErrorCode::PropertyNotFound, "node property {} not found", s);
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
        return KATANA_ERROR(
            ErrorCode::PropertyNotFound, "failed: edge property {} not found",
            s);
      }

      next_edge_prop_info_list.emplace_back(it->second);
    }
    edge_prop_info_list_ = next_edge_prop_info_list;
  }
  return katana::ResultSuccess();
}

katana::Result<void>
RDGPartHeader::Validate() const {
  for (const auto& md : node_prop_info_list_) {
    if (md.path.find('/') != std::string::npos) {
      return KATANA_ERROR(
          ErrorCode::InvalidArgument,
          "node_property path doesn't contain a slash (/): {}", md.path);
    }
  }
  for (const auto& md : edge_prop_info_list_) {
    if (md.path.find('/') != std::string::npos) {
      return KATANA_ERROR(
          ErrorCode::InvalidArgument,
          "edge_property path doesn't contain a slash (/): {}", md.path);
    }
  }
  if (topology_path_.empty()) {
    return KATANA_ERROR(ErrorCode::InvalidArgument, "topology_path is empty");
  }
  if (topology_path_.find('/') != std::string::npos) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument,
        "topology_path doesn't contain a slash (/): {}", topology_path_);
  }
  return katana::ResultSuccess();
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

katana::Result<void>
RDGPartHeader::MarkNodePropertiesPersistent(
    const std::vector<std::string>& persist_node_props) {
  if (persist_node_props.size() > node_prop_info_list_.size()) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument,
        "failed: persist props sz: {} names, rdg.node_prop_info_list_ sz: {}",
        persist_node_props.size(), node_prop_info_list_.size());
  }
  for (uint32_t i = 0; i < persist_node_props.size(); ++i) {
    if (!persist_node_props[i].empty()) {
      node_prop_info_list_[i].name = persist_node_props[i];
      node_prop_info_list_[i].path = "";
      node_prop_info_list_[i].persist = true;
      KATANA_LOG_DEBUG("node persist {}", node_prop_info_list_[i].name);
    }
  }
  return katana::ResultSuccess();
}

katana::Result<void>
RDGPartHeader::MarkEdgePropertiesPersistent(
    const std::vector<std::string>& persist_edge_props) {
  if (persist_edge_props.size() > edge_prop_info_list_.size()) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument,
        "persist props sz: {} names, rdg.edge_prop_info_list_ sz: {}",
        persist_edge_props.size(), edge_prop_info_list_.size());
  }
  for (uint32_t i = 0; i < persist_edge_props.size(); ++i) {
    if (!persist_edge_props[i].empty()) {
      edge_prop_info_list_[i].name = persist_edge_props[i];
      edge_prop_info_list_[i].path = "";
      edge_prop_info_list_[i].persist = true;
      KATANA_LOG_DEBUG("edge persist {}", edge_prop_info_list_[i].name);
    }
  }
  return katana::ResultSuccess();
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
