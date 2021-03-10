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
#include "tsuba/file.h"
#include "tsuba/tsuba.h"

using json = nlohmann::json;

namespace {

// special partition property names
const char* kMirrorNodesPropName = "mirror_nodes";
const char* kMasterNodesPropName = "master_nodes";
const char* kLocalToGlobalIDPropName = "local_to_global_id";
// deprecated; only here to support backward compatibility
const char* kDeprecatedLocalToGlobalIDPropName = "local_to_global_vector";

std::shared_ptr<parquet::WriterProperties>
StandardWriterProperties() {
  // int64 timestamps with nanosecond resolution requires Parquet version 2.0.
  // In Arrow to Parquet version 1.0, nanosecond timestamps will get truncated
  // to milliseconds.
  return parquet::WriterProperties::Builder()
      .version(parquet::ParquetVersion::PARQUET_2_0)
      ->data_page_version(parquet::ParquetDataPageVersion::V2)
      ->build();
}

std::shared_ptr<parquet::ArrowWriterProperties>
StandardArrowProperties() {
  return parquet::ArrowWriterProperties::Builder().build();
}

/// Store the arrow array as a table in a unique file, return
/// the final name of that file
katana::Result<std::string>
DoStoreArrowArrayAtName(
    const std::shared_ptr<arrow::ChunkedArray>& array, const katana::Uri& dir,
    const std::string& name, tsuba::WriteGroup* desc) {
  katana::Uri next_path = dir.RandFile(name);

  // Metadata paths should relative to dir
  std::shared_ptr<arrow::Table> column = arrow::Table::Make(
      arrow::schema({arrow::field(name, array->type())}), {array});

  auto ff = std::make_shared<tsuba::FileFrame>();
  if (auto res = ff->Init(); !res) {
    return res.error();
  }

  auto write_result = parquet::arrow::WriteTable(
      *column, arrow::default_memory_pool(), ff,
      std::numeric_limits<int64_t>::max(), StandardWriterProperties(),
      StandardArrowProperties());

  if (!write_result.ok()) {
    return KATANA_ERROR(
        tsuba::ErrorCode::ArrowError, "arrow error: {}", write_result);
  }

  ff->Bind(next_path.string());
  TSUBA_PTP(tsuba::internal::FaultSensitivity::Normal);
  desc->StartStore(std::move(ff));
  return next_path.BaseName();
}

katana::Result<std::string>
StoreArrowArrayAtName(
    const std::shared_ptr<arrow::ChunkedArray>& array, const katana::Uri& dir,
    const std::string& name, tsuba::WriteGroup* desc) {
  try {
    return DoStoreArrowArrayAtName(array, dir, name, desc);
  } catch (const std::exception& exp) {
    return KATANA_ERROR(
        tsuba::ErrorCode::ArrowError, "arrow exception: {}", exp.what());
  }
}

std::string
MirrorPropName(unsigned i) {
  return std::string(kMirrorNodesPropName) + "_" + std::to_string(i);
}

std::string
MasterPropName(unsigned i) {
  return std::string(kMasterNodesPropName) + "_" + std::to_string(i);
}

katana::Result<std::shared_ptr<arrow::ChunkedArray>>
LargeStringToChunkedString(
    const std::shared_ptr<arrow::LargeStringArray>& arr) {
  std::vector<std::shared_ptr<arrow::Array>> chunks;

  arrow::StringBuilder builder;

  uint64_t inserted = 0;
  constexpr uint64_t kMaxSize = 2147483646;

  for (uint64_t i = 0, size = arr->length(); i < size; ++i) {
    if (!arr->IsValid(i)) {
      auto status = builder.AppendNull();
      if (!status.ok()) {
        KATANA_LOG_ERROR("could not append null: {}", status);
        return tsuba::ErrorCode::ArrowError;
      }
      continue;
    }
    arrow::util::string_view val = arr->GetView(i);
    uint64_t val_size = val.size();
    KATANA_LOG_ASSERT(val_size < kMaxSize);
    if (inserted + val_size >= kMaxSize) {
      std::shared_ptr<arrow::Array> new_arr;
      auto status = builder.Finish(&new_arr);
      if (!status.ok()) {
        KATANA_LOG_ERROR("could not finish building string array: {}", status);
        return tsuba::ErrorCode::ArrowError;
      }
      chunks.emplace_back(new_arr);
      inserted = 0;
      builder.Reset();
    }
    inserted += val_size;
    auto status = builder.Append(val);
    if (!status.ok()) {
      KATANA_LOG_ERROR("could not add string to array builder: {}", status);
      return tsuba::ErrorCode::ArrowError;
    }
  }
  if (inserted > 0) {
    std::shared_ptr<arrow::Array> new_arr;
    auto status = builder.Finish(&new_arr);
    if (!status.ok()) {
      KATANA_LOG_ERROR("could finish building string array: {}", status);
      return tsuba::ErrorCode::ArrowError;
    }
    chunks.emplace_back(new_arr);
  }

  auto maybe_res = arrow::ChunkedArray::Make(chunks, arrow::utf8());
  if (!maybe_res.ok()) {
    KATANA_LOG_ERROR(
        "could not make arrow chunked array: {}", maybe_res.status());
    return tsuba::ErrorCode::ArrowError;
  }
  return maybe_res.ValueOrDie();
}

// HandleBadParquetTypes here and HandleBadParquetTypes in AddProperties.cpp
// workaround a libarrow2.0 limitation in reading and writing LargeStrings to
// parquet files.
katana::Result<std::shared_ptr<arrow::ChunkedArray>>
HandleBadParquetTypes(std::shared_ptr<arrow::ChunkedArray> old_array) {
  if (old_array->num_chunks() > 1) {
    return old_array;
  }
  switch (old_array->type()->id()) {
  case arrow::Type::type::LARGE_STRING: {
    std::shared_ptr<arrow::LargeStringArray> large_string_array =
        std::static_pointer_cast<arrow::LargeStringArray>(old_array->chunk(0));
    return LargeStringToChunkedString(large_string_array);
  }
  default:
    return old_array;
  }
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
    auto fixed_type_column = HandleBadParquetTypes(props.column(i));
    if (!fixed_type_column) {
      return fixed_type_column.error();
    }
    auto name_res =
        StoreArrowArrayAtName(fixed_type_column.value(), dir, name, desc);
    if (!name_res) {
      return name_res.error();
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
  } else if (name == kLocalToGlobalIDPropName) {
    set_local_to_global_vector(std::move(col));
  } else if (name == kDeprecatedLocalToGlobalIDPropName) {
    KATANA_LOG_WARN(
        "deprecated graph format; replace the existing graph by storing the "
        "current graph");
    set_local_to_global_vector(std::move(col));
  } else {
    return tsuba::ErrorCode::InvalidArgument;
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
      "WritePartArrays master sz: {} mirros sz: {} l2g sz: {}",
      master_nodes_.size(), mirror_nodes_.size(),
      local_to_global_id_ == nullptr ? 0 : local_to_global_id_->length());

  for (unsigned i = 0; i < mirror_nodes_.size(); ++i) {
    auto name = MirrorPropName(i);
    auto mirr_res = StoreArrowArrayAtName(mirror_nodes_[i], dir, name, desc);
    if (!mirr_res) {
      return mirr_res.error();
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
      return mast_res.error();
    }
    next_properties.emplace_back(tsuba::PropStorageInfo{
        .name = name,
        .path = std::move(mast_res.value()),
        .persist = true,
    });
  }

  if (local_to_global_id_ != nullptr) {
    auto l2g_res = StoreArrowArrayAtName(
        local_to_global_id_, dir, kLocalToGlobalIDPropName, desc);
    if (!l2g_res) {
      return l2g_res.error();
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
    return res.error();
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
    return node_result.error();
  }

  auto edge_result = AddProperties(
      metadata_dir, core_->part_header().edge_prop_info_list(),
      [rdg = this](const std::shared_ptr<arrow::Table>& props) {
        return rdg->core_->AddEdgeProperties(props);
      });
  if (!edge_result) {
    return edge_result.error();
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

  const auto& schema = props->schema();
  for (int i = 0, end = props->num_columns(); i < end; ++i) {
    core_->part_header().AppendNodePropStorageInfo(tsuba::PropStorageInfo{
        .name = schema->field(i)->name(),
        .path = "",
    });
  }

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

  const auto& schema = props->schema();
  for (int i = 0, end = props->num_columns(); i < end; ++i) {
    core_->part_header().AppendEdgePropStorageInfo(tsuba::PropStorageInfo{
        .name = schema->field(i)->name(),
        .path = "",
    });
  }

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

tsuba::RDG::RDG(std::unique_ptr<RDGCore>&& core) : core_(std::move(core)) {
  // Create an empty array, accessed by Distribution during loading
  local_to_global_id_ = katana::EmptyChunkedArray(arrow::uint64(), 0);
}

tsuba::RDG::RDG() : core_(std::make_unique<RDGCore>()) {
  // Create an empty array, accessed by Distribution during loading
  local_to_global_id_ = katana::EmptyChunkedArray(arrow::uint64(), 0);
}

tsuba::RDG::~RDG() = default;
tsuba::RDG::RDG(tsuba::RDG&& other) noexcept = default;
tsuba::RDG& tsuba::RDG::operator=(tsuba::RDG&& other) noexcept = default;
