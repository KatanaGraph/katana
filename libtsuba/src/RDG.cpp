#include "tsuba/RDG.h"

#include <cassert>
#include <exception>
#include <fstream>
#include <memory>
#include <regex>
#include <unordered_set>

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
#include "katana/Backtrace.h"
#include "katana/JSON.h"
#include "katana/Logging.h"
#include "katana/Result.h"
#include "katana/Uri.h"
#include "tsuba/Errors.h"
#include "tsuba/FaultTest.h"
#include "tsuba/ParquetWriter.h"
#include "tsuba/file.h"
#include "tsuba/tsuba.h"

using json = nlohmann::json;

namespace {

// special partition property names
const char* kMirrorNodesPropName = "mirror_nodes";
const char* kMasterNodesPropName = "master_nodes";
const char* kHostToOwnedGlobalIDsPropName = "host_to_owned_global_ids";
const char* kLocalToUserIDPropName = "local_to_user_id";
const char* kLocalToGlobalIDPropName = "local_to_global_id";
// deprecated; only here to support backward compatibility
const char* kDeprecatedLocalToGlobalIDPropName = "local_to_global_vector";

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

katana::Result<std::vector<tsuba::PropStorageInfo>>
WriteProperties(
    const arrow::Table& props,
    const std::vector<tsuba::PropStorageInfo>& prop_info,
    const katana::Uri& dir, tsuba::WriteGroup* desc) {
  const auto& schema = props.schema();

  std::vector<std::string> next_paths;
  for (size_t i = 0, n = prop_info.size(); i < n; ++i) {
    if (!prop_info[i].persist || !prop_info[i].path.empty()) {
      continue;
    }
    auto name = prop_info[i].name.empty() ? schema->field(i)->name()
                                          : prop_info[i].name;
    auto name_res = StoreArrowArrayAtName(props.column(i), dir, name, desc);
    if (!name_res) {
      return name_res.error().WithContext("storing arrow array");
    }
    next_paths.emplace_back(name_res.value());
  }
  TSUBA_PTP(tsuba::internal::FaultSensitivity::Normal);

  if (next_paths.empty()) {
    return prop_info;
  }

  std::vector<tsuba::PropStorageInfo> next_properties = prop_info;
  auto it = next_paths.begin();
  for (auto& v : next_properties) {
    if (v.persist && v.path.empty()) {
      v.path = *it++;
    }
  }

  return next_properties;
}

katana::Result<void>
CommitRDG(
    tsuba::RDGHandle handle, uint32_t policy_id, bool transposed,
    const tsuba::RDGLineage& lineage, std::unique_ptr<tsuba::WriteGroup> desc) {
  katana::CommBackend* comm = tsuba::Comm();
  tsuba::RDGMeta new_meta = handle.impl_->rdg_meta().NextVersion(
      comm->Num, policy_id, transposed, lineage);

  // wait for all the work we queued to finish
  TSUBA_PTP(tsuba::internal::FaultSensitivity::High);
  if (auto res = desc->Finish(); !res) {
    return res.error().WithContext("at least one async write failed");
  }
  TSUBA_PTP(tsuba::internal::FaultSensitivity::High);
  comm->Barrier();

  // NS handles MPI coordination
  if (auto res = tsuba::NS()->Update(
          handle.impl_->rdg_meta().dir(), handle.impl_->rdg_meta().version(),
          new_meta);
      !res) {
    KATANA_LOG_ERROR(
        "unable to update rdg at {}: {}", handle.impl_->rdg_meta().dir(),
        res.error());
  }

  TSUBA_PTP(tsuba::internal::FaultSensitivity::High);
  katana::Result<void> ret = tsuba::OneHostOnly([&]() -> katana::Result<void> {
    TSUBA_PTP(tsuba::internal::FaultSensitivity::High);

    std::string curr_s = new_meta.ToJsonString();
    auto res = tsuba::FileStore(
        tsuba::RDGMeta::FileName(
            handle.impl_->rdg_meta().dir(), new_meta.version())
            .string(),
        reinterpret_cast<const uint8_t*>(curr_s.data()), curr_s.size());
    if (!res) {
      return res.error().WithContext(
          "CommitRDG future failed {}",
          tsuba::RDGMeta::FileName(
              handle.impl_->rdg_meta().dir(), new_meta.version()));
    }
    return katana::ResultSuccess();
  });
  if (ret) {
    handle.impl_->set_rdg_meta(std::move(new_meta));
  }
  return ret;
}

void
AddNodePropStorageInfo(
    tsuba::RDGCore* core, const std::shared_ptr<arrow::Table>& props) {
  const auto& schema = props->schema();
  for (int i = 0, end = props->num_columns(); i < end; ++i) {
    core->part_header().AddNodePropStorageInfo(tsuba::PropStorageInfo{
        .name = schema->field(i)->name(),
        .path = "",
    });
  }
}

void
AddEdgePropStorageInfo(
    tsuba::RDGCore* core, const std::shared_ptr<arrow::Table>& props) {
  const auto& schema = props->schema();
  for (int i = 0, end = props->num_columns(); i < end; ++i) {
    core->part_header().AddEdgePropStorageInfo(tsuba::PropStorageInfo{
        .name = schema->field(i)->name(),
        .path = "",
    });
  }
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
  } else if (name == kHostToOwnedGlobalIDsPropName) {
    set_host_to_owned_global_ids(std::move(col));
  } else if (name == kLocalToUserIDPropName) {
    set_local_to_user_id(std::move(col));
  } else if (name == kLocalToGlobalIDPropName) {
    set_local_to_global_id(std::move(col));
  } else if (name == kDeprecatedLocalToGlobalIDPropName) {
    KATANA_LOG_WARN(
        "deprecated graph format; replace the existing graph by storing the "
        "current graph");
    set_local_to_global_id(std::move(col));
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
      "WritePartArrays master sz: {} mirrors sz: {} h2owned sz : {} l2u sz: {} "
      "l2g sz: {}",
      master_nodes_.size(), mirror_nodes_.size(),
      host_to_owned_global_ids_ == nullptr
          ? 0
          : host_to_owned_global_ids_->length(),
      local_to_user_id_ == nullptr ? 0 : local_to_user_id_->length(),
      local_to_global_id_ == nullptr ? 0 : local_to_global_id_->length());

  for (unsigned i = 0; i < mirror_nodes_.size(); ++i) {
    auto name = MirrorPropName(i);
    auto mirr_res = StoreArrowArrayAtName(mirror_nodes_[i], dir, name, desc);
    if (!mirr_res) {
      return mirr_res.error().WithContext("storing mirrors[{}] arrow array", i);
    }
    next_properties.emplace_back(tsuba::PropStorageInfo{
        .name = name,
        .path = std::move(mirr_res.value()),
        .persist = true,
    });
  }

  for (unsigned i = 0; i < master_nodes_.size(); ++i) {
    auto name = MasterPropName(i);
    auto mast_res = StoreArrowArrayAtName(master_nodes_[i], dir, name, desc);
    if (!mast_res) {
      return mast_res.error().WithContext("storing masters arrow array");
    }
    next_properties.emplace_back(tsuba::PropStorageInfo{
        .name = name,
        .path = std::move(mast_res.value()),
        .persist = true,
    });
  }

  if (host_to_owned_global_ids_ != nullptr) {
    auto h2o_res = StoreArrowArrayAtName(
        host_to_owned_global_ids_, dir, kHostToOwnedGlobalIDsPropName, desc);
    if (!h2o_res) {
      return h2o_res.error();
    }
    next_properties.emplace_back(tsuba::PropStorageInfo{
        .name = kHostToOwnedGlobalIDsPropName,
        .path = std::move(h2o_res.value()),
        .persist = true,
    });
  }

  if (local_to_user_id_ != nullptr) {
    auto l2u_res = StoreArrowArrayAtName(
        local_to_user_id_, dir, kLocalToUserIDPropName, desc);
    if (!l2u_res) {
      return l2u_res.error();
    }
    next_properties.emplace_back(tsuba::PropStorageInfo{
        .name = kLocalToUserIDPropName,
        .path = std::move(l2u_res.value()),
        .persist = true,
    });
  }

  if (local_to_global_id_ != nullptr) {
    auto l2g_res = StoreArrowArrayAtName(
        local_to_global_id_, dir, kLocalToGlobalIDPropName, desc);
    if (!l2g_res) {
      return l2g_res.error().WithContext("storing l2g arrow array");
    }
    next_properties.emplace_back(tsuba::PropStorageInfo{
        .name = kLocalToGlobalIDPropName,
        .path = std::move(l2g_res.value()),
        .persist = true,
    });
  }

  return next_properties;
}

katana::Result<void>
tsuba::RDG::DoStore(
    RDGHandle handle, const std::string& command_line,
    std::unique_ptr<WriteGroup> write_group) {
  if (core_->part_header().topology_path().empty()) {
    // No topology file; create one
    katana::Uri t_path = MakeTopologyFileName(handle);

    TSUBA_PTP(internal::FaultSensitivity::Normal);

    // depends on `topology_file_storage_` outliving writes
    write_group->StartStore(
        t_path.string(), core_->topology_file_storage().ptr<uint8_t>(),
        core_->topology_file_storage().size());
    TSUBA_PTP(internal::FaultSensitivity::Normal);
    core_->part_header().set_topology_path(t_path.BaseName());
  }

  auto node_write_result = WriteProperties(
      *core_->node_properties(), core_->part_header().node_prop_info_list(),
      handle.impl_->rdg_meta().dir(), write_group.get());
  if (!node_write_result) {
    return node_write_result.error().WithContext(
        "failed to write node properties");
  }

  // update node properties with newly written locations
  core_->part_header().set_node_prop_info_list(
      std::move(node_write_result.value()));

  auto edge_write_result = WriteProperties(
      *core_->edge_properties(), core_->part_header().edge_prop_info_list(),
      handle.impl_->rdg_meta().dir(), write_group.get());
  if (!edge_write_result) {
    return edge_write_result.error().WithContext(
        "failed to write edge properties");
  }

  // update edge properties with newly written locations
  core_->part_header().set_edge_prop_info_list(
      std::move(edge_write_result.value()));

  auto part_write_result =
      WritePartArrays(handle.impl_->rdg_meta().dir(), write_group.get());

  if (!part_write_result) {
    return part_write_result.error().WithContext("failed to write part arrays");
  }
  core_->part_header().set_part_properties(
      std::move(part_write_result.value()));

  if (auto write_result = core_->part_header().Write(handle, write_group.get());
      !write_result) {
    return write_result.error().WithContext("failed to write metadata");
  }

  // Update lineage and commit
  lineage_.AddCommandLine(command_line);
  if (auto res = CommitRDG(
          handle, core_->part_header().metadata().policy_id_,
          core_->part_header().metadata().transposed_, lineage_,
          std::move(write_group));
      !res) {
    return res.error().WithContext("failed to finalize RDG");
  }
  return katana::ResultSuccess();
}

katana::Result<void>
tsuba::RDG::DoMake(const katana::Uri& metadata_dir) {
  auto node_result = AddProperties(
      metadata_dir, core_->part_header().node_prop_info_list(),
      [rdg = this](const std::shared_ptr<arrow::Table>& props) {
        return rdg->core_->AddNodeProperties(props);
      });
  if (!node_result) {
    return node_result.error().WithContext("populating node properties");
  }

  auto edge_result = AddProperties(
      metadata_dir, core_->part_header().edge_prop_info_list(),
      [rdg = this](const std::shared_ptr<arrow::Table>& props) {
        return rdg->core_->AddEdgeProperties(props);
      });
  if (!edge_result) {
    return edge_result.error().WithContext("populating edge properties");
  }

  const std::vector<PropStorageInfo>& part_prop_info_list =
      core_->part_header().part_prop_info_list();
  if (!part_prop_info_list.empty()) {
    auto part_result = AddProperties(
        metadata_dir, part_prop_info_list,
        [rdg = this](const std::shared_ptr<arrow::Table>& props) {
          return rdg->AddPartitionMetadataArray(props);
        });
    if (!part_result) {
      return part_result.error();
    }

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
          local_to_user_id_->length(),
          core_->part_header().metadata().num_owned_,
          local_to_global_id_->length());
    }

    KATANA_LOG_DEBUG(
        "ReadPartMetadata master sz: {} mirrors sz: {} h2owned sz: {} l2u sz: "
        "{} l2g sz: {}",
        master_nodes_.size(), mirror_nodes_.size(),
        host_to_owned_global_ids_ == nullptr
            ? 0
            : host_to_owned_global_ids_->length(),
        local_to_user_id_ == nullptr ? 0 : local_to_user_id_->length(),
        local_to_global_id_ == nullptr ? 0 : local_to_global_id_->length());
  }

  katana::Uri t_path = metadata_dir.Join(core_->part_header().topology_path());
  if (auto res = core_->topology_file_storage().Bind(t_path.string(), true);
      !res) {
    return res.error();
  }

  rdg_dir_ = metadata_dir;
  return katana::ResultSuccess();
}

katana::Result<tsuba::RDG>
tsuba::RDG::Make(const RDGMeta& meta, const RDGLoadOptions& opts) {
  uint32_t partition_id_to_load =
      opts.partition_id_to_load.value_or(Comm()->ID);
  katana::Uri partition_path = meta.PartitionFileName(partition_id_to_load);

  auto part_header_res = RDGPartHeader::Make(partition_path);
  if (!part_header_res) {
    return part_header_res.error().WithContext(
        "failed to read path {}", partition_path);
  }

  RDG rdg(std::make_unique<RDGCore>(std::move(part_header_res.value())));

  if (auto res = rdg.core_->part_header().PrunePropsTo(
          opts.node_properties, opts.edge_properties);
      !res) {
    return res.error();
  }

  if (auto res = rdg.DoMake(meta.dir()); !res) {
    return res.error();
  }

  rdg.set_partition_id(partition_id_to_load);

  return RDG(std::move(rdg));
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
  return Make(handle.impl_->rdg_meta(), opts);
}

katana::Result<void>
tsuba::RDG::Store(
    RDGHandle handle, const std::string& command_line,
    std::unique_ptr<FileFrame> ff) {
  if (!handle.impl_->AllowsWrite()) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "handle does not allow write");
  }
  // We trust the partitioner to give us a valid graph, but we
  // report our assumptions
  KATANA_LOG_DEBUG(
      "RDG::Store meta.num_hosts: {} meta.policy_id: {} num_hosts: {} "
      "policy_id: {}",
      handle.impl_->rdg_meta().num_hosts(),
      handle.impl_->rdg_meta().policy_id(), tsuba::Comm()->Num,
      core_->part_header().metadata().policy_id_);
  if (handle.impl_->rdg_meta().dir() != rdg_dir_) {
    core_->part_header().UnbindFromStorage();
  }

  auto desc_res = WriteGroup::Make();
  if (!desc_res) {
    return desc_res.error();
  }
  // All write buffers must outlive desc
  std::unique_ptr<WriteGroup> desc = std::move(desc_res.value());

  if (ff) {
    katana::Uri t_path = handle.impl_->rdg_meta().dir().RandFile("topology");

    ff->Bind(t_path.string());
    TSUBA_PTP(internal::FaultSensitivity::Normal);
    desc->StartStore(std::move(ff));
    TSUBA_PTP(internal::FaultSensitivity::Normal);
    core_->part_header().set_topology_path(t_path.BaseName());
  }

  return DoStore(handle, command_line, std::move(desc));
}

katana::Result<void>
tsuba::RDG::AddNodeProperties(const std::shared_ptr<arrow::Table>& props) {
  if (auto res = core_->AddNodeProperties(props); !res) {
    return res.error();
  }

  AddNodePropStorageInfo(core_.get(), props);

  KATANA_LOG_DEBUG_ASSERT(
      static_cast<size_t>(core_->node_properties()->num_columns()) ==
      core_->part_header().node_prop_info_list().size());

  return katana::ResultSuccess();
}

katana::Result<void>
tsuba::RDG::AddEdgeProperties(const std::shared_ptr<arrow::Table>& props) {
  if (auto res = core_->AddEdgeProperties(props); !res) {
    return res.error();
  }

  AddEdgePropStorageInfo(core_.get(), props);

  KATANA_LOG_DEBUG_ASSERT(
      static_cast<size_t>(core_->edge_properties()->num_columns()) ==
      core_->part_header().edge_prop_info_list().size());

  return katana::ResultSuccess();
}

katana::Result<void>
tsuba::RDG::UpsertNodeProperties(const std::shared_ptr<arrow::Table>& props) {
  if (auto res = core_->UpsertNodeProperties(props); !res) {
    return res.error();
  }

  AddNodePropStorageInfo(core_.get(), props);

  KATANA_LOG_DEBUG_ASSERT(
      static_cast<size_t>(core_->node_properties()->num_columns()) ==
      core_->part_header().node_prop_info_list().size());

  return katana::ResultSuccess();
}

katana::Result<void>
tsuba::RDG::UpsertEdgeProperties(const std::shared_ptr<arrow::Table>& props) {
  if (auto res = core_->UpsertEdgeProperties(props); !res) {
    return res.error();
  }

  AddEdgePropStorageInfo(core_.get(), props);

  KATANA_LOG_DEBUG_ASSERT(
      static_cast<size_t>(core_->edge_properties()->num_columns()) ==
      core_->part_header().edge_prop_info_list().size());

  return katana::ResultSuccess();
}

katana::Result<void>
tsuba::RDG::RemoveNodeProperty(uint32_t i) {
  return core_->RemoveNodeProperty(i);
}

katana::Result<void>
tsuba::RDG::RemoveEdgeProperty(uint32_t i) {
  return core_->RemoveEdgeProperty(i);
}

void
tsuba::RDG::MarkAllPropertiesPersistent() {
  core_->part_header().MarkAllPropertiesPersistent();
}

katana::Result<void>
tsuba::RDG::MarkNodePropertiesPersistent(
    const std::vector<std::string>& persist_node_props) {
  return core_->part_header().MarkNodePropertiesPersistent(persist_node_props);
}

katana::Result<void>
tsuba::RDG::MarkEdgePropertiesPersistent(
    const std::vector<std::string>& persist_edge_props) {
  return core_->part_header().MarkEdgePropertiesPersistent(persist_edge_props);
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

void
tsuba::RDG::InitArrowVectors() {
  // Create an empty array, accessed by Distribution during loading
  host_to_owned_global_ids_ = katana::NullChunkedArray(arrow::uint64(), 0);
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
