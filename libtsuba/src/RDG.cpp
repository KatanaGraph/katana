#include "tsuba/RDG.h"

#include <cassert>
#include <exception>
#include <fstream>
#include <iterator>
#include <memory>
#include <regex>
#include <string>
#include <unordered_set>
#include <vector>

#include <arrow/chunked_array.h>
#include <arrow/filesystem/api.h>
#include <arrow/memory_pool.h>
#include <arrow/type_fwd.h>
#include <arrow/util/string_view.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/schema.h>
#include <parquet/arrow/writer.h>
#include <parquet/file_reader.h>
#include <parquet/platform.h>
#include <parquet/properties.h>

#include "AddProperties.h"
#include "GlobalState.h"
#include "RDGCore.h"
#include "RDGHandleImpl.h"
#include "katana/ArrowInterchange.h"
#include "katana/ErrorCode.h"
#include "katana/JSON.h"
#include "katana/Logging.h"
#include "katana/Result.h"
#include "katana/URI.h"
#include "tsuba/Errors.h"
#include "tsuba/FaultTest.h"
#include "tsuba/ParquetWriter.h"
#include "tsuba/ReadGroup.h"
#include "tsuba/WriteGroup.h"
#include "tsuba/file.h"
#include "tsuba/tsuba.h"

using json = nlohmann::json;

namespace {

// special partition property names
const char* kMirrorNodesPropName = "mirror_nodes";
const char* kMasterNodesPropName = "master_nodes";
const char* kHostToOwnedGlobalNodeIDsPropName = "host_to_owned_global_node_ids";
const char* kHostToOwnedGlobalEdgeIDsPropName = "host_to_owned_global_edge_ids";
const char* kLocalToUserIDPropName = "local_to_user_id";
const char* kLocalToGlobalIDPropName = "local_to_global_id";
// deprecated; only here to support backward compatibility
const char* kDeprecatedLocalToGlobalIDPropName = "local_to_global_vector";
const char* kDeprecatedHostToOwnedGlobalNodeIDsPropName =
    "host_to_owned_global_ids";

std::string
MirrorPropName(unsigned i) {
  return std::string(kMirrorNodesPropName) + "_" + std::to_string(i);
}

std::string
MasterPropName(unsigned i) {
  return std::string(kMasterNodesPropName) + "_" + std::to_string(i);
}

katana::Result<std::string>
StoreArrowArrayAtName(
    const std::shared_ptr<arrow::ChunkedArray>& array, const katana::Uri& dir,
    const std::string& name, tsuba::WriteGroup* desc) {
  auto writer_res = tsuba::ParquetWriter::Make(array, name);
  if (!writer_res) {
    return writer_res.error().WithContext("making property writer");
  }

  katana::Uri new_path = dir.RandFile(name);
  auto res = writer_res.value()->WriteToUri(new_path, desc);
  if (!res) {
    return res.error().WithContext("writing property writer");
  }
  return new_path.BaseName();
}

katana::Result<void>
WriteProperties(
    const arrow::Table& props, std::vector<tsuba::PropStorageInfo*> prop_info,
    const katana::Uri& dir, tsuba::WriteGroup* desc) {
  const auto& schema = props.schema();

  std::vector<std::string> next_paths;
  for (size_t i = 0, n = prop_info.size(); i < n; ++i) {
    if (!prop_info[i]->IsDirty()) {
      continue;
    }
    std::string name = prop_info[i]->name().empty() ? schema->field(i)->name()
                                                    : prop_info[i]->name();
    std::string path =
        KATANA_CHECKED(StoreArrowArrayAtName(props.column(i), dir, name, desc));

    prop_info[i]->WasWritten(path);
  }
  TSUBA_PTP(tsuba::internal::FaultSensitivity::Normal);

  return katana::ResultSuccess();
}

katana::Result<void>
CommitRDG(
    tsuba::RDGHandle handle, uint32_t policy_id, bool transposed,
    tsuba::RDG::RDGVersioningPolicy versioning_action,
    const tsuba::RDGLineage& lineage, std::unique_ptr<tsuba::WriteGroup> desc) {
  katana::CommBackend* comm = tsuba::Comm();
  tsuba::RDGManifest new_manifest =
      (versioning_action == tsuba::RDG::RetainVersion)
          ? handle.impl_->rdg_manifest().SameVersion(
                comm->Num, policy_id, transposed, lineage)
          : handle.impl_->rdg_manifest().NextVersion(
                comm->Num, policy_id, transposed, lineage);

  // wait for all the work we queued to finish
  TSUBA_PTP(tsuba::internal::FaultSensitivity::High);
  if (auto res = desc->Finish(); !res) {
    return res.error().WithContext("at least one async write failed");
  }
  TSUBA_PTP(tsuba::internal::FaultSensitivity::High);
  comm->Barrier();

  TSUBA_PTP(tsuba::internal::FaultSensitivity::High);
  katana::Result<void> ret = tsuba::OneHostOnly([&]() -> katana::Result<void> {
    TSUBA_PTP(tsuba::internal::FaultSensitivity::High);

    std::string curr_s = new_manifest.ToJsonString();
    auto res = tsuba::FileStore(
        tsuba::RDGManifest::FileName(
            handle.impl_->rdg_manifest().dir(),
            handle.impl_->rdg_manifest().viewtype(), new_manifest.version())
            .string(),
        reinterpret_cast<const uint8_t*>(curr_s.data()), curr_s.size());
    if (!res) {
      return res.error().WithContext(
          "CommitRDG future failed {}",
          tsuba::RDGManifest::FileName(
              handle.impl_->rdg_manifest().dir(),
              handle.impl_->rdg_manifest().viewtype(), new_manifest.version()));
    }
    return katana::ResultSuccess();
  });
  if (ret) {
    handle.impl_->set_rdg_manifest(std::move(new_manifest));
  }
  return ret;
}

}  // namespace

katana::Result<void>
tsuba::RDG::AddPartitionMetadataArray(
    const std::shared_ptr<arrow::Table>& props) {
  auto field = props->schema()->field(0);
  const std::string& name = field->name();
  std::shared_ptr<arrow::ChunkedArray> col = props->column(0);

  if (name.find(kMirrorNodesPropName) == 0) {
    AddMirrorNodes(std::move(col));
  } else if (name.find(kMasterNodesPropName) == 0) {
    AddMasterNodes(std::move(col));
  } else if (name == kHostToOwnedGlobalNodeIDsPropName) {
    set_host_to_owned_global_node_ids(std::move(col));
  } else if (name == kHostToOwnedGlobalEdgeIDsPropName) {
    set_host_to_owned_global_edge_ids(std::move(col));
  } else if (name == kLocalToUserIDPropName) {
    set_local_to_user_id(std::move(col));
  } else if (name == kLocalToGlobalIDPropName) {
    set_local_to_global_id(std::move(col));
  } else if (name == kDeprecatedLocalToGlobalIDPropName) {
    KATANA_LOG_WARN(
        "deprecated graph format; replace the existing graph by storing the "
        "current graph");
    set_local_to_global_id(std::move(col));
  } else if (name == kDeprecatedHostToOwnedGlobalNodeIDsPropName) {
    KATANA_LOG_WARN(
        "deprecated graph format; replace the existing graph by storing the "
        "current graph");
    set_host_to_owned_global_node_ids(std::move(col));
  } else {
    return KATANA_ERROR(ErrorCode::InvalidArgument, "checking metadata name");
  }
  return katana::ResultSuccess();
}

void
tsuba::RDG::AddLineage(const std::string& command_line) {
  lineage_.AddCommandLine(command_line);
}

tsuba::RDGFile::~RDGFile() {
  auto result = Close(handle_);
  if (!result) {
    KATANA_LOG_ERROR("closing RDGFile: {}", result.error());
  }
}

katana::Result<std::vector<tsuba::PropStorageInfo>>
tsuba::RDG::WritePartArrays(const katana::Uri& dir, tsuba::WriteGroup* desc) {
  std::vector<tsuba::PropStorageInfo> next_properties;

  KATANA_LOG_DEBUG(
      "WritePartArrays master sz: {} mirrors sz: {} h2owned sz : {} "
      "h2owned_edges sz: {} l2u sz: {} "
      "l2g sz: {}",
      master_nodes_.size(), mirror_nodes_.size(),
      host_to_owned_global_node_ids_ == nullptr
          ? 0
          : host_to_owned_global_node_ids_->length(),
      host_to_owned_global_edge_ids_ == nullptr
          ? 0
          : host_to_owned_global_edge_ids_->length(),
      local_to_user_id_ == nullptr ? 0 : local_to_user_id_->length(),
      local_to_global_id_ == nullptr ? 0 : local_to_global_id_->length());

  for (size_t i = 0; i < mirror_nodes_.size(); ++i) {
    std::string name = MirrorPropName(i);
    std::string path = KATANA_CHECKED_CONTEXT(
        StoreArrowArrayAtName(mirror_nodes_[i], dir, name, desc), "storing {}",
        name);
    next_properties.emplace_back(tsuba::PropStorageInfo(name, path));
  }

  for (size_t i = 0; i < master_nodes_.size(); ++i) {
    std::string name = MasterPropName(i);
    std::string path = KATANA_CHECKED_CONTEXT(
        StoreArrowArrayAtName(master_nodes_[i], dir, name, desc), "storing {}",
        name);
    next_properties.emplace_back(tsuba::PropStorageInfo(name, path));
  }

  if (host_to_owned_global_node_ids_ != nullptr) {
    std::string name = kHostToOwnedGlobalNodeIDsPropName;
    std::string path = KATANA_CHECKED_CONTEXT(
        StoreArrowArrayAtName(host_to_owned_global_node_ids_, dir, name, desc),
        "storing {}", name);
    next_properties.emplace_back(tsuba::PropStorageInfo(name, path));
  }

  if (host_to_owned_global_edge_ids_ != nullptr) {
    std::string name = kHostToOwnedGlobalEdgeIDsPropName;
    std::string path = KATANA_CHECKED_CONTEXT(
        StoreArrowArrayAtName(host_to_owned_global_edge_ids_, dir, name, desc),
        "storing {}", name);
    next_properties.emplace_back(tsuba::PropStorageInfo(name, path));
  }

  if (local_to_user_id_ != nullptr) {
    std::string name = kLocalToUserIDPropName;
    std::string path = KATANA_CHECKED_CONTEXT(
        StoreArrowArrayAtName(local_to_user_id_, dir, name, desc), "storing {}",
        name);
    next_properties.emplace_back(tsuba::PropStorageInfo(name, path));
  }

  if (local_to_global_id_ != nullptr) {
    std::string name = kLocalToGlobalIDPropName;
    std::string path = KATANA_CHECKED_CONTEXT(
        StoreArrowArrayAtName(
            local_to_global_id_, dir, kLocalToGlobalIDPropName, desc),
        "storing {}", name);
    next_properties.emplace_back(tsuba::PropStorageInfo(name, path));
  }

  return next_properties;
}

katana::Result<void>
tsuba::RDG::DoStoreTopology(
    RDGHandle handle, std::unique_ptr<FileFrame> topology_ff,
    std::unique_ptr<WriteGroup>& write_group) {
  if (!topology_ff && !topology_file_storage().Valid()) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument,
        "no topology file frame update, but topology_file_storage is invalid");
  }

  if (topology_ff) {
    KATANA_LOG_DEBUG("Persisting new topology");
    // we have an update, store the passed in memory state
    katana::Uri path_uri = MakeTopologyFileName(handle);
    topology_ff->Bind(path_uri.string());
    TSUBA_PTP(internal::FaultSensitivity::Normal);
    write_group->StartStore(std::move(topology_ff));
    TSUBA_PTP(internal::FaultSensitivity::Normal);
    core_->part_header().set_topology_path(path_uri.BaseName());
  } else if (handle.impl_->rdg_manifest().dir() != rdg_dir_) {
    KATANA_LOG_DEBUG("persisting topology in new location");
    // we don't have an update, but we are persisting in a new location
    // store our in memory state
    katana::Uri path_uri = MakeTopologyFileName(handle);

    TSUBA_PTP(internal::FaultSensitivity::Normal);
    // depends on `topology_file_storage_` outliving writes
    write_group->StartStore(
        path_uri.string(), core_->topology_file_storage().ptr<uint8_t>(),
        core_->topology_file_storage().size());
    TSUBA_PTP(internal::FaultSensitivity::Normal);
    core_->part_header().set_topology_path(path_uri.BaseName());
  } else {
    // no update, rdg_dir is unchanged, assert if we don't have a valid path
    KATANA_LOG_ASSERT(core_->part_header().topology_path().empty() == false);
  }
  // else: no update, not persisting in a new location, so nothing for us to do

  return katana::ResultSuccess();
}

katana::Result<void>
tsuba::RDG::DoStoreNodeEntityTypeIDArray(
    RDGHandle handle, std::unique_ptr<FileFrame> node_entity_type_id_array_ff,
    std::unique_ptr<WriteGroup>& write_group) {
  if (!node_entity_type_id_array_ff &&
      !node_entity_type_id_array_file_storage().Valid()) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument,
        "no node_entity_type_id_array file frame update, but "
        "node_entity_type_id_array_file_storage is invalid");
  }

  if (node_entity_type_id_array_ff) {
    // we have an update, store the passed in memory state
    katana::Uri path_uri = MakeNodeEntityTypeIDArrayFileName(handle);
    node_entity_type_id_array_ff->Bind(path_uri.string());
    TSUBA_PTP(internal::FaultSensitivity::Normal);
    write_group->StartStore(std::move(node_entity_type_id_array_ff));
    TSUBA_PTP(internal::FaultSensitivity::Normal);
    core_->part_header().set_node_entity_type_id_array_path(
        path_uri.BaseName());
  } else if (handle.impl_->rdg_manifest().dir() != rdg_dir_) {
    KATANA_LOG_DEBUG("persisting node_entity_type_id_array in new location");
    // we don't have an update, but we are persisting in a new location
    // store our in memory state
    katana::Uri path_uri = MakeNodeEntityTypeIDArrayFileName(handle);

    TSUBA_PTP(internal::FaultSensitivity::Normal);
    // depends on `node_entity_type_id_array_` outliving writes
    write_group->StartStore(
        path_uri.string(),
        core_->node_entity_type_id_array_file_storage().ptr<uint8_t>(),
        core_->node_entity_type_id_array_file_storage().size());
    TSUBA_PTP(internal::FaultSensitivity::Normal);
    core_->part_header().set_node_entity_type_id_array_path(
        path_uri.BaseName());
  } else {
    // no update, rdg_dir is unchanged, assert if we don't have a valid path
    KATANA_LOG_ASSERT(
        core_->part_header().node_entity_type_id_array_path().empty() == false);
  }
  // else: no update, not persisting in a new location, so nothing for us to do

  return katana::ResultSuccess();
}

katana::Result<void>
tsuba::RDG::DoStoreEdgeEntityTypeIDArray(
    RDGHandle handle, std::unique_ptr<FileFrame> edge_entity_type_id_array_ff,
    std::unique_ptr<WriteGroup>& write_group) {
  if (!edge_entity_type_id_array_ff &&
      !edge_entity_type_id_array_file_storage().Valid()) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument,
        "no edge_entity_type_id_array file frame update, but "
        "edge_entity_type_id_array_file_storage is invalid");
  }

  if (edge_entity_type_id_array_ff) {
    // we have an update, store the passed in memory state
    katana::Uri path_uri = MakeEdgeEntityTypeIDArrayFileName(handle);
    edge_entity_type_id_array_ff->Bind(path_uri.string());
    TSUBA_PTP(internal::FaultSensitivity::Normal);
    write_group->StartStore(std::move(edge_entity_type_id_array_ff));
    TSUBA_PTP(internal::FaultSensitivity::Normal);
    core_->part_header().set_edge_entity_type_id_array_path(
        path_uri.BaseName());
  } else if (handle.impl_->rdg_manifest().dir() != rdg_dir_) {
    KATANA_LOG_DEBUG("persisting edge_entity_type_id_array in new location");
    // we don't have an update, but we are persisting in a new location
    // store our in memory state
    katana::Uri path_uri = MakeEdgeEntityTypeIDArrayFileName(handle);

    TSUBA_PTP(internal::FaultSensitivity::Normal);
    // depends on `edge_entity_type_id_array_` outliving writes
    write_group->StartStore(
        path_uri.string(),
        core_->edge_entity_type_id_array_file_storage().ptr<uint8_t>(),
        core_->edge_entity_type_id_array_file_storage().size());
    TSUBA_PTP(internal::FaultSensitivity::Normal);
    core_->part_header().set_edge_entity_type_id_array_path(
        path_uri.BaseName());
  } else {
    // no update, rdg_dir is unchanged, assert if we don't have a valid path
    KATANA_LOG_ASSERT(
        core_->part_header().edge_entity_type_id_array_path().empty() == false);
  }
  // else: no update, not persisting in a new location, so nothing for us to do

  return katana::ResultSuccess();
}

katana::Result<void>
tsuba::RDG::DoStore(
    RDGHandle handle, const std::string& command_line,
    RDGVersioningPolicy versioning_action,
    std::unique_ptr<WriteGroup> write_group) {
  // bump the storage format version to the latest
  core_->part_header().update_storage_format_version();

  std::vector<std::string> node_prop_names;
  for (const auto& field : core_->node_properties()->fields()) {
    node_prop_names.emplace_back(field->name());
  }

  std::vector<PropStorageInfo*> node_props_to_store = KATANA_CHECKED(
      core_->part_header().SelectNodeProperties(node_prop_names));

  KATANA_CHECKED_CONTEXT(
      WriteProperties(
          *core_->node_properties(), node_props_to_store,
          handle.impl_->rdg_manifest().dir(), write_group.get()),
      "writing node properties");

  std::vector<std::string> edge_prop_names;
  for (const auto& field : core_->edge_properties()->fields()) {
    edge_prop_names.emplace_back(field->name());
  }

  std::vector<PropStorageInfo*> edge_props_to_store = KATANA_CHECKED(
      core_->part_header().SelectEdgeProperties(edge_prop_names));

  KATANA_CHECKED_CONTEXT(
      WriteProperties(
          *core_->edge_properties(), edge_props_to_store,
          handle.impl_->rdg_manifest().dir(), write_group.get()),
      "writing edge properties");

  core_->part_header().set_part_properties(KATANA_CHECKED_CONTEXT(
      WritePartArrays(handle.impl_->rdg_manifest().dir(), write_group.get()),
      "writing partition metadata"));

  //If a view type has been set, use it otherwise pass in the default view type
  if (view_type_.empty()) {
    handle.impl_->set_viewtype(tsuba::kDefaultRDGViewType);
  } else {
    handle.impl_->set_viewtype(view_type_);
  }

  if (auto write_result = core_->part_header().Write(
          handle, write_group.get(), versioning_action);
      !write_result) {
    return write_result.error().WithContext("failed to write metadata");
  }

  // Update lineage and commit
  lineage_.AddCommandLine(command_line);
  if (auto res = CommitRDG(
          handle, core_->part_header().metadata().policy_id_,
          core_->part_header().metadata().transposed_, versioning_action,
          lineage_, std::move(write_group));
      !res) {
    return res.error().WithContext("failed to finalize RDG");
  }
  return katana::ResultSuccess();
}

katana::Result<void>
tsuba::RDG::DoMake(
    const std::vector<PropStorageInfo*>& node_props_to_be_loaded,
    const std::vector<PropStorageInfo*>& edge_props_to_be_loaded,
    const katana::Uri& metadata_dir) {
  ReadGroup grp;

  KATANA_CHECKED_CONTEXT(
      AddProperties(
          metadata_dir, node_props_to_be_loaded, &grp,
          [rdg = this](const std::shared_ptr<arrow::Table>& props)
              -> katana::Result<void> {
            std::shared_ptr<arrow::Table> prop_table =
                rdg->core_->node_properties();

            if (prop_table && prop_table->num_columns() > 0) {
              for (int i = 0; i < props->num_columns(); ++i) {
                prop_table = KATANA_CHECKED(prop_table->AddColumn(
                    prop_table->num_columns(), props->field(i),
                    props->column(i)));
              }
            } else {
              prop_table = props;
            }
            rdg->core_->set_node_properties(std::move(prop_table));
            return katana::ResultSuccess();
          }),
      "populating node properties");

  KATANA_CHECKED_CONTEXT(
      AddProperties(
          metadata_dir, edge_props_to_be_loaded, &grp,
          [rdg = this](const std::shared_ptr<arrow::Table>& props)
              -> katana::Result<void> {
            std::shared_ptr<arrow::Table> prop_table =
                rdg->core_->edge_properties();

            if (prop_table && prop_table->num_columns() > 0) {
              for (int i = 0; i < props->num_columns(); ++i) {
                prop_table = KATANA_CHECKED(prop_table->AddColumn(
                    prop_table->num_columns(), props->field(i),
                    props->column(i)));
              }
            } else {
              prop_table = props;
            }
            rdg->core_->set_edge_properties(std::move(prop_table));
            return katana::ResultSuccess();
          }),
      "populating edge properties");

  katana::Uri t_path = metadata_dir.Join(core_->part_header().topology_path());
  if (auto res = core_->topology_file_storage().Bind(t_path.string(), true);
      !res) {
    return res.error();
  }

  if (core_->part_header().IsEntityTypeIDsOutsideProperties()) {
    katana::Uri node_entity_type_id_array_path = metadata_dir.Join(
        core_->part_header().node_entity_type_id_array_path());
    if (auto res = core_->node_entity_type_id_array_file_storage().Bind(
            node_entity_type_id_array_path.string(), true);
        !res) {
      return res.error();
    }

    katana::Uri edge_entity_type_id_array_path = metadata_dir.Join(
        core_->part_header().edge_entity_type_id_array_path());
    if (auto res = core_->edge_entity_type_id_array_file_storage().Bind(
            edge_entity_type_id_array_path.string(), true);
        !res) {
      return res.error();
    }
  }
  rdg_dir_ = metadata_dir;

  std::vector<PropStorageInfo*> part_info =
      KATANA_CHECKED(core_->part_header().SelectPartitionProperties());

  if (part_info.empty()) {
    return grp.Finish();
  }

  KATANA_CHECKED_CONTEXT(
      AddProperties(
          metadata_dir, part_info, &grp,
          [rdg = this](const std::shared_ptr<arrow::Table>& props) {
            return rdg->AddPartitionMetadataArray(props);
          }),
      "populating partition metadata");
  KATANA_CHECKED(grp.Finish());

  if (local_to_user_id_->length() == 0) {
    // for backward compatibility
    if (local_to_global_id_->length() !=
        core_->part_header().metadata().num_nodes_) {
      return KATANA_ERROR(
          tsuba::ErrorCode::InvalidArgument,
          "regenerate partitions: number of Global Node IDs {} does not "
          "match the number of master nodes {}",
          local_to_global_id_->length(),
          core_->part_header().metadata().num_nodes_);
    }
    // NB: this is a zero-copy slice, so the underlying data is shared
    set_local_to_user_id(local_to_global_id_->Slice(0));
  } else if (
      local_to_user_id_->length() !=
      (core_->part_header().metadata().num_owned_ +
       local_to_global_id_->length())) {
    return KATANA_ERROR(
        tsuba::ErrorCode::InvalidArgument,
        "regenerate partitions: number of User Node IDs {} do not match "
        "number of masters nodes {} plus the number of Global Node IDs {}",
        local_to_user_id_->length(), core_->part_header().metadata().num_owned_,
        local_to_global_id_->length());
  }

  KATANA_LOG_DEBUG(
      "ReadPartMetadata master sz: {} mirrors sz: {} h2nod sz: {} h20e sz: {} "
      "l2u sz: "
      "{} l2g sz: {}",
      master_nodes_.size(), mirror_nodes_.size(),
      host_to_owned_global_node_ids_ == nullptr
          ? 0
          : host_to_owned_global_node_ids_->length(),
      host_to_owned_global_edge_ids_ == nullptr
          ? 0
          : host_to_owned_global_edge_ids_->length(),
      local_to_user_id_ == nullptr ? 0 : local_to_user_id_->length(),
      local_to_global_id_ == nullptr ? 0 : local_to_global_id_->length());

  // these are not Node/Edge types but rather property types we are checking
  KATANA_CHECKED(core_->EnsureNodeTypesLoaded(rdg_dir_));
  KATANA_CHECKED(core_->EnsureEdgeTypesLoaded(rdg_dir_));

  return katana::ResultSuccess();
}

katana::Result<tsuba::RDG>
tsuba::RDG::Make(const RDGManifest& manifest, const RDGLoadOptions& opts) {
  uint32_t partition_id_to_load =
      opts.partition_id_to_load.value_or(Comm()->ID);

  katana::Uri partition_path = manifest.PartitionFileName(partition_id_to_load);

  auto part_header_res = RDGPartHeader::Make(partition_path);
  if (!part_header_res) {
    return part_header_res.error().WithContext(
        "failed to read path {}", partition_path);
  }

  RDG rdg(std::make_unique<RDGCore>(std::move(part_header_res.value())));

  std::vector<PropStorageInfo*> node_props = KATANA_CHECKED(
      rdg.core_->part_header().SelectNodeProperties(opts.node_properties));

  std::vector<PropStorageInfo*> edge_props = KATANA_CHECKED(
      rdg.core_->part_header().SelectEdgeProperties(opts.edge_properties));

  KATANA_CHECKED(rdg.DoMake(node_props, edge_props, manifest.dir()));

  rdg.set_partition_id(partition_id_to_load);

  return RDG(std::move(rdg));
}

bool
tsuba::RDG::IsEntityTypeIDsOutsideProperties() const {
  return core_->part_header().IsEntityTypeIDsOutsideProperties();
}

katana::Result<void>
tsuba::RDG::Validate() const {
  if (auto res = core_->part_header().Validate(); !res) {
    return res.error();
  }
  return katana::ResultSuccess();
}

bool
tsuba::RDG::Equals(const RDG& other) const {
  return core_->Equals(*other.core_);
}

katana::Result<tsuba::RDG>
tsuba::RDG::Make(RDGHandle handle, const RDGLoadOptions& opts) {
  if (!handle.impl_->AllowsRead()) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "handle does not allow full read");
  }
  return Make(handle.impl_->rdg_manifest(), opts);
}

katana::Result<void>
tsuba::RDG::Store(
    RDGHandle handle, const std::string& command_line,
    RDGVersioningPolicy versioning_action,
    std::unique_ptr<FileFrame> topology_ff,
    std::unique_ptr<FileFrame> node_entity_type_id_array_ff,
    std::unique_ptr<FileFrame> edge_entity_type_id_array_ff,
    const katana::EntityTypeManager& node_entity_type_manager,
    const katana::EntityTypeManager& edge_entity_type_manager) {
  if (!handle.impl_->AllowsWrite()) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "handle does not allow write");
  }
  // We trust the partitioner to give us a valid graph, but we
  // report our assumptions
  KATANA_LOG_DEBUG(
      "RDG::Store manifest.num_hosts: {} manifest.policy_id: {} num_hosts: {} "
      "policy_id: {} versioning_action{}",
      handle.impl_->rdg_manifest().num_hosts(),
      handle.impl_->rdg_manifest().policy_id(), tsuba::Comm()->Num,
      core_->part_header().metadata().policy_id_, versioning_action);
  if (handle.impl_->rdg_manifest().dir() != rdg_dir_) {
    KATANA_CHECKED(core_->part_header().ChangeStorageLocation(
        rdg_dir_, handle.impl_->rdg_manifest().dir()));
  }

  auto desc_res = WriteGroup::Make();
  if (!desc_res) {
    return desc_res.error();
  }
  // All write buffers must outlive desc
  std::unique_ptr<WriteGroup> desc = std::move(desc_res.value());

  auto res = DoStoreTopology(handle, std::move(topology_ff), desc);
  if (!res) {
    return res.error();
  }

  res = DoStoreNodeEntityTypeIDArray(
      handle, std::move(node_entity_type_id_array_ff), desc);
  if (!res) {
    return res.error();
  }

  res = DoStoreEdgeEntityTypeIDArray(
      handle, std::move(edge_entity_type_id_array_ff), desc);
  if (!res) {
    return res.error();
  }

  core_->part_header().StoreNodeEntityTypeManager(node_entity_type_manager);
  core_->part_header().StoreEdgeEntityTypeManager(edge_entity_type_manager);

  return DoStore(handle, command_line, versioning_action, std::move(desc));
}

katana::Result<void>
tsuba::RDG::AddNodeProperties(const std::shared_ptr<arrow::Table>& props) {
  if (auto res = core_->AddNodeProperties(props); !res) {
    return res.error();
  }

  return katana::ResultSuccess();
}

katana::Result<void>
tsuba::RDG::AddEdgeProperties(const std::shared_ptr<arrow::Table>& props) {
  if (auto res = core_->AddEdgeProperties(props); !res) {
    return res.error();
  }

  return katana::ResultSuccess();
}

katana::Result<void>
tsuba::RDG::UpsertNodeProperties(const std::shared_ptr<arrow::Table>& props) {
  return core_->UpsertNodeProperties(props);
}

katana::Result<void>
tsuba::RDG::UpsertEdgeProperties(const std::shared_ptr<arrow::Table>& props) {
  return core_->UpsertEdgeProperties(props);
}

katana::Result<void>
tsuba::RDG::RemoveNodeProperty(int i) {
  return core_->RemoveNodeProperty(i);
}

katana::Result<void>
tsuba::RDG::RemoveEdgeProperty(int i) {
  return core_->RemoveEdgeProperty(i);
}

namespace {

katana::Result<std::shared_ptr<arrow::Table>>
UnloadProperty(
    const std::shared_ptr<arrow::Table>& props, int i,
    std::vector<tsuba::PropStorageInfo>* prop_info_list,
    const katana::Uri& dir) {
  if (i < 0 || i > props->num_columns()) {
    return KATANA_ERROR(
        katana::ErrorCode::InvalidArgument, "property index out of bounds");
  }
  const std::string& name = props->field(i)->name();

  auto psi_it = std::find_if(
      prop_info_list->begin(), prop_info_list->end(),
      [&](const tsuba::PropStorageInfo& psi) { return psi.name() == name; });

  KATANA_LOG_ASSERT(psi_it != prop_info_list->end());

  tsuba::PropStorageInfo& prop_info = *psi_it;

  KATANA_LOG_ASSERT(!prop_info.IsAbsent());

  if (prop_info.IsDirty()) {
    std::string path = KATANA_CHECKED(
        StoreArrowArrayAtName(props->column(i), dir, name, nullptr));
    prop_info.WasWritten(path);
  }

  prop_info.WasUnloaded();

  return KATANA_CHECKED(props->RemoveColumn(i));
}

katana::Result<std::shared_ptr<arrow::Table>>
LoadProperty(
    const std::shared_ptr<arrow::Table>& props, const std::string name, int i,
    std::vector<tsuba::PropStorageInfo>* prop_info_list,
    const katana::Uri& dir) {
  if (i < 0 || i > props->num_columns()) {
    i = props->num_columns();
  }

  auto psi_it = std::find_if(
      prop_info_list->begin(), prop_info_list->end(),
      [&](const tsuba::PropStorageInfo& psi) { return psi.name() == name; });

  if (psi_it == prop_info_list->end()) {
    return KATANA_ERROR(
        tsuba::ErrorCode::PropertyNotFound, "no property named {}",
        std::quoted(name));
  }

  tsuba::PropStorageInfo& prop_info = *psi_it;

  if (!prop_info.IsAbsent()) {
    return KATANA_ERROR(
        tsuba::ErrorCode::InvalidArgument, "property {} already loaded",
        std::quoted(name));
  }

  std::shared_ptr<arrow::Table> new_table;

  KATANA_CHECKED(tsuba::AddProperties(
      dir, {&prop_info}, nullptr,
      [&](const std::shared_ptr<arrow::Table>& col) -> katana::Result<void> {
        if (props->num_columns() > 0) {
          new_table = KATANA_CHECKED(
              props->AddColumn(i, col->field(0), col->column(0)));
        } else {
          new_table = col;
        }
        return katana::ResultSuccess();
      }));

  KATANA_LOG_ASSERT(prop_info.IsClean());

  return new_table;
}

}  // namespace

katana::Result<void>
tsuba::RDG::UnloadNodeProperty(int i) {
  std::shared_ptr<arrow::Table> new_props = KATANA_CHECKED(UnloadProperty(
      node_properties(), i, &core_->part_header().node_prop_info_list(),
      rdg_dir()));
  core_->set_node_properties(std::move(new_props));
  return katana::ResultSuccess();
}

katana::Result<void>
tsuba::RDG::UnloadEdgeProperty(int i) {
  std::shared_ptr<arrow::Table> new_props = KATANA_CHECKED(UnloadProperty(
      edge_properties(), i, &core_->part_header().edge_prop_info_list(),
      rdg_dir()));
  core_->set_edge_properties(std::move(new_props));
  return katana::ResultSuccess();
}

katana::Result<void>
tsuba::RDG::LoadNodeProperty(const std::string& name, int i) {
  std::shared_ptr<arrow::Table> new_props = KATANA_CHECKED(LoadProperty(
      node_properties(), name, i, &core_->part_header().node_prop_info_list(),
      rdg_dir()));
  core_->set_node_properties(std::move(new_props));
  return katana::ResultSuccess();
}

katana::Result<void>
tsuba::RDG::LoadEdgeProperty(const std::string& name, int i) {
  std::shared_ptr<arrow::Table> new_props = KATANA_CHECKED(LoadProperty(
      edge_properties(), name, i, &core_->part_header().edge_prop_info_list(),
      rdg_dir()));
  core_->set_edge_properties(std::move(new_props));
  return katana::ResultSuccess();
}

std::vector<std::string>
tsuba::RDG::ListNodeProperties() const {
  std::vector<std::string> result;
  for (const auto& prop : core_->part_header().node_prop_info_list()) {
    result.emplace_back(prop.name());
  }
  return result;
}

std::vector<std::string>
tsuba::RDG::ListEdgeProperties() const {
  std::vector<std::string> result;
  for (const auto& prop : core_->part_header().edge_prop_info_list()) {
    result.emplace_back(prop.name());
  }
  return result;
}

const tsuba::PartitionMetadata&
tsuba::RDG::part_metadata() const {
  return core_->part_header().metadata();
}

void
tsuba::RDG::set_part_metadata(const tsuba::PartitionMetadata& metadata) {
  core_->part_header().set_metadata(metadata);
}

const std::shared_ptr<arrow::Table>&
tsuba::RDG::node_properties() const {
  return core_->node_properties();
}

const std::shared_ptr<arrow::Table>&
tsuba::RDG::edge_properties() const {
  return core_->edge_properties();
}

void
tsuba::RDG::DropNodeProperties() {
  core_->drop_node_properties();
}

void
tsuba::RDG::DropEdgeProperties() {
  core_->drop_edge_properties();
}

std::shared_ptr<arrow::Schema>
tsuba::RDG::full_node_schema() const {
  std::vector<std::shared_ptr<arrow::Field>> fields;
  for (const auto& prop : core_->part_header().node_prop_info_list()) {
    KATANA_LOG_VASSERT(
        prop.type(), "should be impossible for type of {} to be null here",
        prop.name());
    fields.emplace_back(
        std::make_shared<arrow::Field>(prop.name(), prop.type()));
  }
  return arrow::schema(fields);
}

std::shared_ptr<arrow::Schema>
tsuba::RDG::full_edge_schema() const {
  std::vector<std::shared_ptr<arrow::Field>> fields;
  for (const auto& prop : core_->part_header().edge_prop_info_list()) {
    KATANA_LOG_VASSERT(
        prop.type(), "should be impossible for type of {} to be null here",
        prop.name());
    fields.emplace_back(
        std::make_shared<arrow::Field>(prop.name(), prop.type()));
  }
  return arrow::schema(fields);
}

const tsuba::FileView&
tsuba::RDG::topology_file_storage() const {
  return core_->topology_file_storage();
}

katana::Result<void>
tsuba::RDG::UnbindTopologyFileStorage() {
  return core_->topology_file_storage().Unbind();
}

katana::Result<void>
tsuba::RDG::SetTopologyFile(const katana::Uri& new_top) {
  katana::Uri dir = new_top.DirName();
  if (dir != rdg_dir_) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument,
        "new topology file must be in this RDG's directory ({})", rdg_dir_);
  }
  return core_->RegisterTopologyFile(new_top.BaseName());
}

const tsuba::FileView&
tsuba::RDG::node_entity_type_id_array_file_storage() const {
  return core_->node_entity_type_id_array_file_storage();
}

katana::Result<katana::EntityTypeManager>
tsuba::RDG::node_entity_type_manager() const {
  return core_->part_header().GetNodeEntityTypeManager();
}

katana::Result<katana::EntityTypeManager>
tsuba::RDG::edge_entity_type_manager() const {
  return core_->part_header().GetEdgeEntityTypeManager();
}

katana::Result<void>
tsuba::RDG::UnbindNodeEntityTypeIDArrayFileStorage() {
  return core_->node_entity_type_id_array_file_storage().Unbind();
}

katana::Result<void>
tsuba::RDG::SetNodeEntityTypeIDArrayFile(const katana::Uri& new_type_id_array) {
  katana::Uri dir = new_type_id_array.DirName();
  if (dir != rdg_dir_) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument,
        "new Node Entity Type ID file must be in this RDG's directory ({})",
        rdg_dir_);
  }
  return core_->RegisterNodeEntityTypeIDArrayFile(new_type_id_array.BaseName());
}

const tsuba::FileView&
tsuba::RDG::edge_entity_type_id_array_file_storage() const {
  return core_->edge_entity_type_id_array_file_storage();
}

katana::Result<void>
tsuba::RDG::UnbindEdgeEntityTypeIDArrayFileStorage() {
  return core_->edge_entity_type_id_array_file_storage().Unbind();
}

katana::Result<void>
tsuba::RDG::SetEdgeEntityTypeIDArrayFile(const katana::Uri& new_type_id_array) {
  katana::Uri dir = new_type_id_array.DirName();
  if (dir != rdg_dir_) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument,
        "new Edge Entity Type ID file must be in this RDG's directory ({})",
        rdg_dir_);
  }
  return core_->RegisterEdgeEntityTypeIDArrayFile(new_type_id_array.BaseName());
}

void
tsuba::RDG::InitArrowVectors() {
  // Create an empty array, accessed by Distribution during loading
  host_to_owned_global_node_ids_ = katana::NullChunkedArray(arrow::uint64(), 0);
  host_to_owned_global_edge_ids_ = katana::NullChunkedArray(arrow::uint64(), 0);
  local_to_user_id_ = katana::NullChunkedArray(arrow::uint64(), 0);
  local_to_global_id_ = katana::NullChunkedArray(arrow::uint64(), 0);
}

tsuba::RDG::RDG(std::unique_ptr<RDGCore>&& core) : core_(std::move(core)) {
  InitArrowVectors();
}

tsuba::RDG::RDG() : core_(std::make_unique<RDGCore>()) { InitArrowVectors(); }

tsuba::RDG::~RDG() = default;
tsuba::RDG::RDG(tsuba::RDG&& other) noexcept = default;
tsuba::RDG& tsuba::RDG::operator=(tsuba::RDG&& other) noexcept = default;
