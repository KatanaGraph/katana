#include "tsuba/RDG.h"

#include <cassert>
#include <exception>
#include <fstream>
#include <memory>
#include <regex>
#include <unordered_set>

#include <arrow/filesystem/api.h>
#include <boost/filesystem.hpp>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/schema.h>
#include <parquet/arrow/writer.h>
#include <parquet/file_reader.h>
#include <parquet/platform.h>
#include <parquet/properties.h>

#include "GlobalState.h"
#include "galois/FileSystem.h"
#include "galois/JSON.h"
#include "galois/Logging.h"
#include "galois/Result.h"
#include "tsuba/Errors.h"
#include "tsuba/FaultTest.h"
#include "tsuba/FileFrame.h"
#include "tsuba/RDG_internal.h"
#include "tsuba/file.h"
#include "tsuba/tsuba.h"

// constexpr uint32_t kPropertyMagicNo  = 0x4B808280; // KPRP
// constexpr uint32_t kPartitionMagicNo = 0x4B808284; // KPRT
constexpr uint32_t kRDGMagicNo = 0x4B524447;  // KRDG

static const char* topology_path_key = "kg.v1.topology.path";
static const char* node_property_path_key = "kg.v1.node_property.path";
static const char* node_property_name_key = "kg.v1.node_property.name";
static const char* edge_property_path_key = "kg.v1.edge_property.path";
static const char* edge_property_name_key = "kg.v1.edge_property.name";
static const char* part_property_path_key = "kg.v1.part_property.path";
static const char* part_property_name_key = "kg.v1.part_property.name";

static const char* part_other_metadata_key = "kg.v1.other_part_metadata.key";

// special partition property names
static const char* mirror_nodes_prop_name = "mirror_nodes";
static const char* master_nodes_prop_name = "master_nodes";
static const char* local_to_global_prop_name = "local_to_global_vector";
static const char* global_to_local_keys_prop_name = "global_to_local_keys";
static const char* global_to_local_vals_prop_name = "global_to_local_values";

namespace fs = boost::filesystem;
using json = nlohmann::json;

struct tsuba::RDGHandleImpl {
  // Property paths are relative rdg_meta.dir_
  std::string partition_path;
  uint32_t flags;
  RDGMeta rdg_meta;

  /// Perform some checks on assumed invariants
  galois::Result<void> Validate() const {
    if (rdg_meta.dir_.empty()) {
      GALOIS_LOG_DEBUG("rdg_meta.dir_: \"{}\" is empty", rdg_meta.dir_);
      return ErrorCode::InvalidArgument;
    }
    return galois::ResultSuccess();
  }
  constexpr bool AllowsReadPartial() const { return flags & kReadPartial; }
  constexpr bool AllowsRead() const { return !AllowsReadPartial(); }
  constexpr bool AllowsWrite() const { return flags & kReadWrite; }
};

namespace {

// this regex basically says:
//  some uri (with or without scheme) that ends in meta, and meta may or may not
//  have a version appended
const std::regex kMetaFileRegex(
    "(?:[a-zA-Z0-9]+://)?[-a-zA-Z0-9./]+/meta(?:_[0-9]+)?");

// if it doesn't name a meta file, assume it's meant to be a managed URI
bool
IsManagedURI(const std::string& uri) {
  return !std::regex_match(uri, kMetaFileRegex);
}

const std::regex kMetaVersion("meta(?:_([0-9]+)|)");

galois::Result<uint64_t>
Parse(const std::string& str) {
  uint64_t val = strtoul(str.c_str(), NULL, 10);
  if (errno == ERANGE) {
    GALOIS_LOG_ERROR("meta file found with out of range version");
    return galois::ResultErrno();
  }
  return val;
}

galois::Result<uint64_t>
ParseVersion(std::string file) {
  std::smatch sub_match;
  if (!std::regex_match(file, sub_match, kMetaVersion)) {
    return tsuba::ErrorCode::InvalidArgument;
  }
  return Parse(sub_match[1]);
}

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

galois::Result<std::shared_ptr<arrow::Table>>
DoLoadTable(const std::string& expected_name, const std::string& file_path) {
  auto fv = std::make_shared<tsuba::FileView>(tsuba::FileView());
  if (auto res = fv->Bind(file_path, false); !res) {
    return res.error();
  }

  std::unique_ptr<parquet::arrow::FileReader> reader;

  auto open_file_result =
      parquet::arrow::OpenFile(fv, arrow::default_memory_pool(), &reader);
  if (!open_file_result.ok()) {
    GALOIS_LOG_DEBUG("arrow error: {}", open_file_result);
    return tsuba::ErrorCode::ArrowError;
  }

  std::shared_ptr<arrow::Table> out;
  auto read_result = reader->ReadTable(&out);
  if (!read_result.ok()) {
    GALOIS_LOG_DEBUG("arrow error: {}", read_result);
    return tsuba::ErrorCode::ArrowError;
  }

  // Combine multiple chunks into one. Binary and string columns (c.f. large
  // binary and large string columns) are a special case. They may not be
  // combined into a single chunk due to the fact the offset type for these
  // columns is int32_t and thus the maximum size of an arrow::Array for these
  // types is 2^31.
  auto combine_result = out->CombineChunks(arrow::default_memory_pool());
  if (!combine_result.ok()) {
    GALOIS_LOG_DEBUG("arrow error: {}", combine_result.status());
    return tsuba::ErrorCode::ArrowError;
  }

  out = std::move(combine_result.ValueOrDie());

  std::shared_ptr<arrow::Schema> schema = out->schema();
  if (schema->num_fields() != 1) {
    GALOIS_LOG_DEBUG("expected 1 field found {} instead", schema->num_fields());
    return tsuba::ErrorCode::InvalidArgument;
  }

  if (schema->field(0)->name() != expected_name) {
    GALOIS_LOG_DEBUG(
        "expected {} found {} instead", expected_name,
        schema->field(0)->name());
    return tsuba::ErrorCode::InvalidArgument;
  }

  return out;
}

galois::Result<std::shared_ptr<arrow::Table>>
LoadTable(const std::string& expected_name, const std::string& file_path) {
  try {
    return DoLoadTable(expected_name, file_path);
  } catch (const std::exception& exp) {
    GALOIS_LOG_DEBUG("arrow exception: {}", exp.what());
    return tsuba::ErrorCode::ArrowError;
  }
}

galois::Result<std::shared_ptr<arrow::Table>>
DoLoadPartialTable(
    const std::string& expected_name, const std::string& file_path,
    int64_t offset, int64_t length) {
  if (offset < 0 || length < 0) {
    return tsuba::ErrorCode::InvalidArgument;
  }
  auto fv = std::make_shared<tsuba::FileView>(tsuba::FileView());
  if (auto res = fv->Bind(file_path, 0, 0, false); !res) {
    return res.error();
  }

  std::unique_ptr<parquet::arrow::FileReader> reader;

  auto open_file_result =
      parquet::arrow::OpenFile(fv, arrow::default_memory_pool(), &reader);
  if (!open_file_result.ok()) {
    GALOIS_LOG_DEBUG("arrow error: {}", open_file_result);
    return tsuba::ErrorCode::ArrowError;
  }

  std::vector<int> row_groups;
  int rg_count = reader->num_row_groups();
  int64_t row_offset = 0;
  int64_t cumulative_rows = 0;
  int64_t file_offset = 0;
  int64_t cumulative_bytes = 0;
  for (int i = 0; cumulative_rows < offset + length && i < rg_count; ++i) {
    auto rg_md = reader->parquet_reader()->metadata()->RowGroup(i);
    int64_t new_rows = rg_md->num_rows();
    int64_t new_bytes = rg_md->total_byte_size();
    if (offset < cumulative_rows + new_rows) {
      if (row_groups.empty()) {
        row_offset = offset - cumulative_rows;
        file_offset = cumulative_bytes;
      }
      row_groups.push_back(i);
    }
    cumulative_rows += new_rows;
    cumulative_bytes += new_bytes;
  }

  if (auto res = fv->Fill(file_offset, cumulative_bytes, false); !res) {
    return res.error();
  }

  std::shared_ptr<arrow::Table> out;
  auto read_result = reader->ReadRowGroups(row_groups, &out);
  if (!read_result.ok()) {
    GALOIS_LOG_DEBUG("arrow error: {}", read_result);
    return tsuba::ErrorCode::ArrowError;
  }

  auto combine_result = out->CombineChunks(arrow::default_memory_pool());
  if (!combine_result.ok()) {
    GALOIS_LOG_DEBUG("arrow error: {}", combine_result.status());
    return tsuba::ErrorCode::ArrowError;
  }

  out = std::move(combine_result.ValueOrDie());

  std::shared_ptr<arrow::Schema> schema = out->schema();
  if (schema->num_fields() != 1) {
    GALOIS_LOG_DEBUG("expected 1 field found {} instead", schema->num_fields());
    return tsuba::ErrorCode::InvalidArgument;
  }

  if (schema->field(0)->name() != expected_name) {
    GALOIS_LOG_DEBUG(
        "expected {} found {} instead", expected_name,
        schema->field(0)->name());
    return tsuba::ErrorCode::InvalidArgument;
  }

  return out->Slice(row_offset, length);
}

template <typename AddFn>
galois::Result<void>
AddTables(
    const fs::path& dir, const std::vector<tsuba::PropertyMetadata>& properties,
    AddFn add_fn) {
  for (const tsuba::PropertyMetadata& properties : properties) {
    fs::path p_path{dir};
    p_path.append(properties.path);

    auto load_result = LoadTable(properties.name, p_path.string());
    if (!load_result) {
      return load_result.error();
    }

    std::shared_ptr<arrow::Table> table = load_result.value();

    auto add_result = add_fn(table);
    if (!add_result) {
      return add_result.error();
    }
  }

  return galois::ResultSuccess();
}

template <typename AddFn>
galois::Result<void>
AddPartialTables(
    const fs::path& dir, const std::vector<tsuba::PropertyMetadata>& properties,
    std::pair<uint64_t, uint64_t> range, AddFn add_fn) {
  for (const tsuba::PropertyMetadata& properties : properties) {
    fs::path p_path{dir};
    p_path.append(properties.path);

    auto load_result = tsuba::internal::LoadPartialTable(
        properties.name, p_path.string(), range.first,
        range.second - range.first);
    if (!load_result) {
      return load_result.error();
    }

    std::shared_ptr<arrow::Table> table = load_result.value();

    auto add_result = add_fn(table);
    if (!add_result) {
      return add_result.error();
    }
  }

  return galois::ResultSuccess();
}

/// Store the arrow array as a table in a unique file, return
/// the final name of that file
galois::Result<std::string>
DoStoreArrowArrayAtName(
    const std::shared_ptr<arrow::ChunkedArray>& array, const std::string& dir,
    const std::string& name) {
  auto path_res = galois::NewPath(dir, name);
  if (!path_res) {
    return path_res.error();
  }
  std::string next_path = path_res.value();

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
    GALOIS_LOG_DEBUG("arrow error: {}", write_result);
    return tsuba::ErrorCode::ArrowError;
  }

  ff->Bind(next_path);
  TSUBA_PTP(tsuba::internal::FaultSensitivity::Normal);
  if (auto res = ff->Persist(); !res) {
    return res.error();
  }

  return galois::ExtractFileName(next_path);
}

galois::Result<std::string>
StoreArrowArrayAtName(
    const std::shared_ptr<arrow::ChunkedArray>& array, const std::string& dir,
    const std::string& name) {
  try {
    return DoStoreArrowArrayAtName(array, dir, name);
  } catch (const std::exception& exp) {
    GALOIS_LOG_DEBUG("arrow exception: {}", exp.what());
    return tsuba::ErrorCode::ArrowError;
  }
}

galois::Result<std::vector<tsuba::PropertyMetadata>>
WriteTable(
    const arrow::Table& table,
    const std::vector<tsuba::PropertyMetadata>& properties,
    const std::string& dir) {
  const auto& schema = table.schema();

  std::vector<std::string> next_paths;
  for (size_t i = 0, n = properties.size(); i < n; ++i) {
    if (!properties[i].path.empty()) {
      continue;
    }
    auto name_res =
        StoreArrowArrayAtName(table.column(i), dir, schema->field(i)->name());
    if (!name_res) {
      return name_res.error();
    }
    next_paths.emplace_back(name_res.value());
  }
  TSUBA_PTP(tsuba::internal::FaultSensitivity::Normal);

  if (next_paths.empty()) {
    return properties;
  }

  std::vector<tsuba::PropertyMetadata> next_properties = properties;
  auto it = next_paths.begin();
  for (auto& v : next_properties) {
    if (v.path.empty()) {
      v.path = *it++;
    }
  }

  return next_properties;
}

std::string
MirrorPropName(unsigned i) {
  return std::string(mirror_nodes_prop_name) + "_" + std::to_string(i);
}

std::string
MasterPropName(unsigned i) {
  return std::string(master_nodes_prop_name) + "_" + std::to_string(i);
}

galois::Result<std::vector<tsuba::PropertyMetadata>>
WritePartArrays(
    const tsuba::PartitionMetadata& part_meta, const std::string& dir) {
  std::vector<tsuba::PropertyMetadata> next_properties;

  for (unsigned i = 0; i < part_meta.mirror_nodes_.size(); ++i) {
    auto name = MirrorPropName(i);
    auto mirr_res =
        StoreArrowArrayAtName(part_meta.mirror_nodes_[i], dir, name);
    if (!mirr_res) {
      return mirr_res.error();
    }
    next_properties.emplace_back(tsuba::PropertyMetadata{
        .name = name,
        .path = std::move(mirr_res.value()),
    });
  }

  for (unsigned i = 0; i < part_meta.master_nodes_.size(); ++i) {
    auto name = MasterPropName(i);
    auto mast_res =
        StoreArrowArrayAtName(part_meta.master_nodes_[i], dir, name);
    if (!mast_res) {
      return mast_res.error();
    }
    next_properties.emplace_back(tsuba::PropertyMetadata{
        .name = name,
        .path = std::move(mast_res.value()),
    });
  }

  auto l2g_res = StoreArrowArrayAtName(
      part_meta.local_to_global_vector_, dir, local_to_global_prop_name);
  if (!l2g_res) {
    return l2g_res.error();
  }
  next_properties.emplace_back(tsuba::PropertyMetadata{
      .name = local_to_global_prop_name, .path = std::move(l2g_res.value())});

  auto g2l_keys_res = StoreArrowArrayAtName(
      part_meta.global_to_local_keys_, dir, global_to_local_keys_prop_name);
  if (!g2l_keys_res) {
    return g2l_keys_res.error();
  }
  next_properties.emplace_back(tsuba::PropertyMetadata{
      .name = global_to_local_keys_prop_name,
      .path = std::move(g2l_keys_res.value()),
  });

  auto g2l_vals_res = StoreArrowArrayAtName(
      part_meta.global_to_local_values_, dir, global_to_local_vals_prop_name);
  if (!g2l_vals_res) {
    return g2l_vals_res.error();
  }
  next_properties.emplace_back(tsuba::PropertyMetadata{
      .name = global_to_local_vals_prop_name,
      .path = std::move(g2l_vals_res.value()),
  });

  return next_properties;
}

/// Add the property columns in @table to @to_update. If @new_properties is true
/// assume that @table include all new properties and append their meta data to
/// @properties (this is true when adding new properties to the graph) else
/// assume that the properties are being reloaded from disk
galois::Result<void>
AddProperties(
    const std::shared_ptr<arrow::Table>& table,
    std::shared_ptr<arrow::Table>* to_update,
    std::vector<tsuba::PropertyMetadata>* properties, bool new_properties) {
  std::shared_ptr<arrow::Table> current = *to_update;

  if (current->num_columns() > 0 && current->num_rows() != table->num_rows()) {
    GALOIS_LOG_DEBUG(
        "expected {} rows found {} instead", current->num_rows(),
        table->num_rows());
    return tsuba::ErrorCode::InvalidArgument;
  }

  std::shared_ptr<arrow::Table> next = current;

  if (current->num_columns() == 0 && current->num_rows() == 0) {
    next = table;
  } else {
    const auto& schema = table->schema();
    int last = current->num_columns();

    for (int i = 0, n = schema->num_fields(); i < n; i++) {
      auto result =
          next->AddColumn(last + i, schema->field(i), table->column(i));
      if (!result.ok()) {
        GALOIS_LOG_DEBUG("arrow error: {}", result.status());
        return tsuba::ErrorCode::ArrowError;
      }

      next = result.ValueOrDie();
    }
  }

  if (!next->schema()->HasDistinctFieldNames()) {
    GALOIS_LOG_DEBUG("column names are not distinct");
    return tsuba::ErrorCode::InvalidArgument;
  }

  if (new_properties) {
    const auto& schema = next->schema();
    for (int i = current->num_columns(), end = next->num_columns(); i < end;
         ++i) {
      properties->emplace_back(tsuba::PropertyMetadata{
          .name = schema->field(i)->name(),
          .path = "",
      });
    }
    assert(static_cast<size_t>(next->num_columns()) == properties->size());
  } else {
    // sanity check since we shouldn't be able to add more old properties to
    // next than are noted in properties
    assert(static_cast<size_t>(next->num_columns()) <= properties->size());
  }

  *to_update = next;

  return galois::ResultSuccess();
}

galois::Result<std::vector<tsuba::PropertyMetadata>>
MakeProperties(std::vector<std::string>&& values) {
  std::vector v = std::move(values);

  if ((v.size() % 2) != 0) {
    GALOIS_LOG_DEBUG("number of values {} is not even", v.size());
    return tsuba::ErrorCode::InvalidArgument;
  }

  std::vector<tsuba::PropertyMetadata> properties;
  std::unordered_set<std::string> names;
  properties.reserve(v.size() / 2);

  for (size_t i = 0, n = v.size(); i < n; i += 2) {
    const auto& name = v[i];
    const auto& path = v[i + 1];

    names.insert(name);

    properties.emplace_back(tsuba::PropertyMetadata{
        .name = name,
        .path = path,
    });
  }

  assert(names.size() == properties.size());

  return properties;
}

/// ReadMetadata reads metadata from a Parquet file and returns the extracted
/// property graph specific fields as well as the unparsed fields.
///
/// The order of metadata fields is significant, and repeated metadata fields
/// are used to encode lists of values.
galois::Result<tsuba::RDG>
DoReadMetadata(const std::string& partition_path) {
  auto fv = std::make_shared<tsuba::FileView>();
  if (auto res = fv->Bind(partition_path, false); !res) {
    return res.error();
  }

  if (fv->size() == 0) {
    return tsuba::RDG();
  }

  std::shared_ptr<parquet::FileMetaData> md;
  try {
    md = parquet::ReadMetaData(fv);
  } catch (const std::exception& exp) {
    GALOIS_LOG_DEBUG("arrow error: {}", exp.what());
    return tsuba::ErrorCode::ArrowError;
  }

  const std::shared_ptr<const arrow::KeyValueMetadata>& kv_metadata =
      md->key_value_metadata();

  if (!kv_metadata) {
    return tsuba::ErrorCode::InvalidArgument;
  }

  std::vector<std::string> node_values;
  std::vector<std::string> edge_values;
  std::vector<std::string> part_values;
  std::vector<std::pair<std::string, std::string>> other_metadata;
  std::string topology_path;
  for (int64_t i = 0, n = kv_metadata->size(); i < n; ++i) {
    const std::string& k = kv_metadata->key(i);
    const std::string& v = kv_metadata->value(i);

    if (k == node_property_path_key || k == node_property_name_key) {
      node_values.emplace_back(v);
    } else if (k == edge_property_path_key || k == edge_property_name_key) {
      edge_values.emplace_back(v);
    } else if (k == part_property_path_key || k == part_property_name_key) {
      part_values.emplace_back(v);
    } else if (k == topology_path_key) {
      if (!topology_path.empty()) {
        return tsuba::ErrorCode::InvalidArgument;
      }
      topology_path = v;
    } else {
      other_metadata.emplace_back(std::make_pair(k, v));
    }
  }

  auto node_properties_result = MakeProperties(std::move(node_values));
  if (!node_properties_result) {
    return node_properties_result.error();
  }

  auto edge_properties_result = MakeProperties(std::move(edge_values));
  if (!edge_properties_result) {
    return edge_properties_result.error();
  }

  auto part_properties_result = MakeProperties(std::move(part_values));
  if (!part_properties_result) {
    return part_properties_result.error();
  }

  tsuba::RDG rdg;

  rdg.node_properties_ = std::move(node_properties_result.value());
  rdg.edge_properties_ = std::move(edge_properties_result.value());
  rdg.part_properties_ = std::move(part_properties_result.value());
  rdg.other_metadata_ = std::move(other_metadata);
  rdg.topology_path_ = std::move(topology_path);

  return tsuba::RDG(std::move(rdg));
}

galois::Result<tsuba::RDG>
ReadMetadata(const std::string& partition_path) {
  try {
    return DoReadMetadata(partition_path);
  } catch (const std::exception& exp) {
    GALOIS_LOG_DEBUG("arrow exception: {}", exp.what());
    return tsuba::ErrorCode::ArrowError;
  }
}

galois::Result<void>
AddPartitionMetadataArray(
    tsuba::PartitionMetadata* p_meta,
    const std::shared_ptr<arrow::Table>& table) {
  auto field = table->schema()->field(0);
  const std::string& name = field->name();
  const std::shared_ptr<arrow::ChunkedArray>& col = table->column(0);
  if (name.find(mirror_nodes_prop_name) == 0) {
    p_meta->mirror_nodes_.emplace_back(col);
  } else if (name.find(master_nodes_prop_name) == 0) {
    p_meta->master_nodes_.emplace_back(col);
  } else if (name == local_to_global_prop_name) {
    p_meta->local_to_global_vector_ = col;
  } else if (name == global_to_local_keys_prop_name) {
    p_meta->global_to_local_keys_ = col;
  } else if (name == global_to_local_vals_prop_name) {
    p_meta->global_to_local_values_ = col;
  } else {
    return tsuba::ErrorCode::InvalidArgument;
  }
  return galois::ResultSuccess();
}

galois::Result<tsuba::RDGPrefix>
BindOutIndex(const std::string& topology_path) {
  tsuba::GRHeader header;
  if (auto res = tsuba::FilePeek(topology_path, &header); !res) {
    return res.error();
  }
  tsuba::FileView fv;
  if (auto res = fv.Bind(
          topology_path, sizeof(header) + (header.num_nodes * sizeof(uint64_t)),
          true);
      !res) {
    return res.error();
  }
  tsuba::RDGPrefix pfx;
  pfx.prefix_storage = tsuba::FileView(std::move(fv));
  pfx.view_offset = sizeof(header) + (header.num_nodes * sizeof(uint64_t));
  return tsuba::RDGPrefix(std::move(pfx));
}

galois::Result<void>
CommitRDG(tsuba::RDGHandle handle, uint32_t policy_id, bool transpose) {
  galois::CommBackend* comm = tsuba::Comm();
  tsuba::RDGMeta new_meta(
      handle.impl_->rdg_meta.version_ + 1, handle.impl_->rdg_meta.version_,
      comm->Num, policy_id, transpose, handle.impl_->rdg_meta.dir_);
  galois::Result<void> ret = galois::ResultSuccess();

  TSUBA_PTP(tsuba::internal::FaultSensitivity::High);
  comm->Barrier();

  // NS handles MPI coordination
  if (auto res = tsuba::NS()->Update(
          handle.impl_->rdg_meta.dir_, handle.impl_->rdg_meta.version_,
          new_meta);
      !res) {
    GALOIS_LOG_ERROR(
        "unable to update rdg at {}: {}", handle.impl_->rdg_meta.dir_,
        res.error());
  }

  TSUBA_PTP(tsuba::internal::FaultSensitivity::High);
  if (comm->ID == 0) {
    TSUBA_PTP(tsuba::internal::FaultSensitivity::High);

    std::string curr_s = new_meta.ToJsonString();
    auto curr_res = tsuba::FileStoreAsync(
        tsuba::RDGMeta::FileName(
            handle.impl_->rdg_meta.dir_, new_meta.version_),
        reinterpret_cast<const uint8_t*>(curr_s.data()), curr_s.size());
    TSUBA_PTP(tsuba::internal::FaultSensitivity::High);

    if (!curr_res) {
      GALOIS_LOG_ERROR("failed to store current RDGMeta file");
      ret = curr_res.error();
    } else {
      TSUBA_PTP(tsuba::internal::FaultSensitivity::High);
      auto curr_fut = std::move(curr_res.value());
      while (curr_fut != nullptr && !curr_fut->Done()) {
        if (auto res = (*curr_fut)(); !res) {
          GALOIS_LOG_ERROR(
              "future failed to store previous RDGMeta file {}", res.error());
        }
      }
    }
  }
  TSUBA_PTP(tsuba::internal::FaultSensitivity::High);
  comm->Barrier();
  TSUBA_PTP(tsuba::internal::FaultSensitivity::High);

  handle.impl_->rdg_meta = new_meta;
  return ret;
}

galois::Result<std::string>
PartitionFileName(const tsuba::RDGMeta& meta, bool intend_partial_read) {
  if (intend_partial_read) {
    if (meta.num_hosts_ != 1) {
      GALOIS_LOG_ERROR("cannot partially read partitioned graph");
      return tsuba::ErrorCode::InvalidArgument;
    }
    return tsuba::RDGMeta::PartitionFileName(meta.dir_, 0, meta.version_);
  }
  if (meta.num_hosts_ != 0 && meta.num_hosts_ != tsuba::Comm()->Num) {
    GALOIS_LOG_ERROR(
        "number of hosts for partitioned graph {} does not "
        "match number of current hosts {}",
        meta.num_hosts_, tsuba::Comm()->Num);
    // Query depends on being able to load a graph this way
    if (meta.num_hosts_ == 1) {
      // TODO(thunt) eliminate this special case after query is updated not
      // to depend on this behavior
      return tsuba::RDGMeta::PartitionFileName(meta.dir_, 0, meta.version_);
    }
    return tsuba::ErrorCode::InvalidArgument;
  }
  return tsuba::RDGMeta::PartitionFileName(
      meta.dir_, tsuba::Comm()->ID, meta.version_);
}

galois::Result<std::string>
GetPartPath(const std::string& rdg_meta_uri, bool intended_partial_read) {
  auto rdg_res = tsuba::RDGMeta::Make(rdg_meta_uri);
  if (rdg_res) {
    tsuba::RDGMeta meta = std::move(rdg_res.value());
    return PartitionFileName(meta, intended_partial_read);
  }
  if (rdg_res.error() != galois::ErrorCode::JsonParseFailed) {
    return rdg_res.error();
  }

  // Maintain this?
  GALOIS_WARN_ONCE(
      "Deprecated behavior: treating invalid RDG file like a "
      "partition file");
  return rdg_meta_uri;
}

galois::Result<tsuba::RDGPrefix>
DoExaminePrefix(const std::string& partition_path, const std::string& dir) {
  auto meta_res = ReadMetadata(partition_path);
  if (!meta_res) {
    return meta_res.error();
  }

  tsuba::RDGPrefix pfx;
  tsuba::RDG rdg = std::move(meta_res.value());
  if (rdg.topology_path_.empty()) {
    return tsuba::RDGPrefix(std::move(pfx));
  }

  fs::path t_path{dir};
  t_path.append(rdg.topology_path_);

  auto out_idx_res = BindOutIndex(t_path.string());
  if (!out_idx_res) {
    return out_idx_res.error();
  }
  tsuba::RDGPrefix npfx = std::move(out_idx_res.value());

  // This is safe because BindOutIndex binds the start of the file
  npfx.prefix = npfx.prefix_storage.ptr<tsuba::GRPrefix>();
  return tsuba::RDGPrefix(std::move(npfx));
}

galois::Result<std::pair<tsuba::RDGMeta, std::string>>
GetMetaAndPartitionPath(const std::string& rdg_name, bool intend_partial_read) {
  auto meta_res = tsuba::RDGMeta::Make(rdg_name);
  if (!meta_res) {
    return meta_res.error();
  }

  tsuba::RDGMeta meta(std::move(meta_res.value()));
  auto path_res = PartitionFileName(meta, intend_partial_read);
  if (!path_res) {
    return path_res.error();
  }
  return std::make_pair(meta, path_res.value());
}

}  // namespace

namespace tsuba {

// NOLINTNEXTLINE needed non-const ref for nlohmann compat
void
to_json(json& j, const RDGMeta& meta) {
  j = json{
      {"magic", kRDGMagicNo},
      {"version", meta.version_},
      {"previous_version", meta.previous_version_},
      {"num_hosts", meta.num_hosts_},
      {"policy_id", meta.policy_id_},
      {"transpose", meta.transpose_},
  };
}

// NOLINTNEXTLINE needed non-const ref for nlohmann compat
void
from_json(const json& j, RDGMeta& meta) {
  uint32_t magic;
  j.at("magic").get_to(magic);
  j.at("version").get_to(meta.version_);
  j.at("num_hosts").get_to(meta.num_hosts_);

  // these values are temporarily optional
  if (auto it = j.find("previous_version"); it != j.end()) {
    it->get_to(meta.previous_version_);
  }
  if (auto it = j.find("policy_id"); it != j.end()) {
    it->get_to(meta.policy_id_);
  }
  if (auto it = j.find("transpose"); it != j.end()) {
    it->get_to(meta.transpose_);
  }

  if (magic != kRDGMagicNo) {
    // nlohmann::json reports errors using exceptions
    throw std::runtime_error("RDG Magic number mismatch");
  }
}

galois::Result<RDGMeta>
RDGMeta::MakeFromStorage(const std::string& path) {
  FileView fv;
  if (auto res = fv.Bind(path, true); !res) {
    return res.error();
  }
  auto dirname_res = galois::ExtractDirName(path);
  if (!dirname_res) {
    return dirname_res.error();
  }
  std::string dirname = dirname_res.value();
  RDGMeta meta(dirname_res.value());
  auto meta_res = JsonParse<RDGMeta>(fv, &meta);
  if (!meta_res) {
    return meta_res.error();
  }
  meta.dir_ = dirname;
  return meta;
}

galois::Result<RDGMeta>
RDGMeta::Make(const std::string& meta_dir, uint64_t version) {
  return MakeFromStorage(FileName(meta_dir, version));
}

galois::Result<RDGMeta>
RDGMeta::Make(const std::string& rdg) {
  if (IsManagedURI(rdg)) {
    auto res = NS()->Get(rdg);
    if (res) {
      res.value().dir_ = rdg;
    }
    return res;
  }
  return MakeFromStorage(rdg);
}

std::string
RDGMeta::ToJsonString() const {
  // POSIX specifies that text files end in a newline
  std::string s = json(*this).dump() + '\n';
  return s;
}

// e.g., rdg_dir == s3://witchel-tests-east2/fault/simple/
std::string
RDGMeta::FileName(const std::string& rdg_dir, uint64_t version) {
  assert(rdg_dir.empty() || IsManagedURI(rdg_dir));

  return galois::JoinPath(rdg_dir, fmt::format("meta_{}", version));
}

std::string
RDGMeta::PartitionFileName(
    const std::string& rdg_dir, uint32_t node_id, uint64_t version) {
  assert(IsManagedURI(rdg_dir));
  return galois::JoinPath(rdg_dir, fmt::format("meta_{}_{}", node_id, version));
}

// NOLINTNEXTLINE needed non-const ref for nlohmann compat
void
to_json(nlohmann::json& j, const PartitionMetadata& pmd) {
  j = json{
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

// NOLINTNEXTLINE needed non-const ref for nlohmann compat
void
from_json(const nlohmann::json& j, PartitionMetadata& pmd) {
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
}

RDGFile::~RDGFile() {
  auto result = Close(handle_);
  if (!result) {
    GALOIS_LOG_ERROR("closing RDGFile: {}", result.error());
  }
}

std::pair<std::vector<std::string>, std::vector<std::string>>
RDG::MakeMetadata() const {
  std::vector<std::string> keys;
  std::vector<std::string> values;

  keys.emplace_back(topology_path_key);
  values.emplace_back(topology_path_);

  for (const auto& v : node_properties_) {
    keys.emplace_back(node_property_name_key);
    values.emplace_back(v.name);
    keys.emplace_back(node_property_path_key);
    values.emplace_back(v.path);
  }

  for (const auto& v : edge_properties_) {
    keys.emplace_back(edge_property_name_key);
    values.emplace_back(v.name);
    keys.emplace_back(edge_property_path_key);
    values.emplace_back(v.path);
  }

  for (const auto& v : part_properties_) {
    keys.emplace_back(part_property_name_key);
    values.emplace_back(v.name);
    keys.emplace_back(part_property_path_key);
    values.emplace_back(v.path);
  }

  for (const auto& v : other_metadata_) {
    keys.emplace_back(v.first);
    values.emplace_back(v.second);
  }

  return std::make_pair(keys, values);
}

galois::Result<void>
RDG::DoWriteMetadata(RDGHandle handle, const arrow::Schema& schema) {
  std::shared_ptr<parquet::SchemaDescriptor> schema_descriptor;
  auto to_result = parquet::arrow::ToParquetSchema(
      &schema, *StandardWriterProperties(), &schema_descriptor);
  if (!to_result.ok()) {
    GALOIS_LOG_DEBUG("arrow error: {}", to_result);
    return ErrorCode::ArrowError;
  }

  auto kvs = MakeMetadata();
  auto parquet_kvs =
      std::make_shared<parquet::KeyValueMetadata>(kvs.first, kvs.second);
  auto builder = parquet::FileMetaDataBuilder::Make(
      schema_descriptor.get(), StandardWriterProperties(), parquet_kvs);
  auto md = builder->Finish();

  auto ff = std::make_shared<FileFrame>();
  if (auto res = ff->Init(); !res) {
    return res.error();
  }

  auto write_result = parquet::arrow::WriteMetaDataFile(*md, ff.get());
  if (!write_result.ok()) {
    return ErrorCode::InvalidArgument;
  }

  ff->Bind(RDGMeta::PartitionFileName(
      handle.impl_->rdg_meta.dir_, Comm()->ID,
      handle.impl_->rdg_meta.version_ + 1));
  TSUBA_PTP(internal::FaultSensitivity::Normal);
  if (auto res = ff->Persist(); !res) {
    return res.error();
  }
  TSUBA_PTP(internal::FaultSensitivity::Normal);
  return galois::ResultSuccess();
}

// const * so that they are nullable
galois::Result<void>
RDG::PrunePropsTo(
    const std::vector<std::string>* node_properties,
    const std::vector<std::string>* edge_properties) {
  // this function should be called during load BEFORE properties are populated
  assert(node_table_->num_rows() == 0);
  assert(node_table_->num_columns() == 0);
  assert(edge_table_->num_rows() == 0);
  assert(edge_table_->num_columns() == 0);

  if (node_properties != nullptr) {
    std::unordered_map<std::string, std::string> node_paths;
    for (const PropertyMetadata& m : node_properties_) {
      node_paths.insert({m.name, m.path});
    }

    std::vector<PropertyMetadata> next_node_properties;
    for (const std::string& s : *node_properties) {
      auto it = node_paths.find(s);
      if (it == node_paths.end()) {
        return ErrorCode::PropertyNotFound;
      }

      next_node_properties.emplace_back(PropertyMetadata{
          .name = it->first,
          .path = it->second,
      });
    }
    node_properties_ = next_node_properties;
  }

  if (edge_properties != nullptr) {
    std::unordered_map<std::string, std::string> edge_paths;
    for (const PropertyMetadata& m : edge_properties_) {
      edge_paths.insert({m.name, m.path});
    }

    std::vector<PropertyMetadata> next_edge_properties;
    for (const std::string& s : *edge_properties) {
      auto it = edge_paths.find(s);
      if (it == edge_paths.end()) {
        return ErrorCode::PropertyNotFound;
      }

      next_edge_properties.emplace_back(PropertyMetadata{
          .name = it->first,
          .path = it->second,
      });
    }
    edge_properties_ = next_edge_properties;
  }
  return galois::ResultSuccess();
}

galois::Result<void>
RDG::DoStore(RDGHandle handle) {
  GALOIS_LOG_DEBUG("Writing out to {}", handle.impl_->rdg_meta.dir_);
  if (topology_path_.empty()) {
    // No topology file; create one
    auto path_res = galois::NewPath(handle.impl_->rdg_meta.dir_, "topology");
    if (!path_res) {
      return path_res.error();
    }
    std::string t_path = std::move(path_res.value());

    TSUBA_PTP(internal::FaultSensitivity::Normal);
    if (auto res = FileStore(
            t_path, topology_file_storage_.ptr<uint8_t>(),
            topology_file_storage_.size());
        !res) {
      return res.error();
    }
    TSUBA_PTP(internal::FaultSensitivity::Normal);
    topology_path_ = galois::ExtractFileName(t_path);
  }

  auto node_write_result =
      WriteTable(*node_table_, node_properties_, handle.impl_->rdg_meta.dir_);
  if (!node_write_result) {
    GALOIS_LOG_DEBUG("unable to write node properties");
    return node_write_result.error();
  }
  node_properties_ = std::move(node_write_result.value());

  auto edge_write_result =
      WriteTable(*edge_table_, edge_properties_, handle.impl_->rdg_meta.dir_);
  if (!edge_write_result) {
    GALOIS_LOG_DEBUG("unable to write edge properties");
    return edge_write_result.error();
  }
  edge_properties_ = std::move(edge_write_result.value());

  if (part_metadata_) {
    if (!part_properties_.empty()) {
      GALOIS_LOG_ERROR("We don't support repartitioning (yet)");
      return ErrorCode::NotImplemented;
    }
    auto part_write_result =
        WritePartArrays(*part_metadata_, handle.impl_->rdg_meta.dir_);
    if (!part_write_result) {
      GALOIS_LOG_DEBUG("WritePartMetadata for part_properties failed");
      return part_write_result.error();
    }
    part_properties_ = std::move(part_write_result.value());

    // stash the rest of the struct in other_metadata
    other_metadata_.emplace_back(
        part_other_metadata_key, json(*part_metadata_).dump());
  }

  auto merge_result = arrow::SchemaBuilder::Merge(
      {node_table_->schema(), edge_table_->schema()},
      arrow::SchemaBuilder::ConflictPolicy::CONFLICT_APPEND);
  if (!merge_result.ok()) {
    GALOIS_LOG_DEBUG("arrow error: {}", merge_result.status());
    return ErrorCode::ArrowError;
  }

  if (auto validate_result = handle.impl_->Validate(); !validate_result) {
    GALOIS_LOG_DEBUG("RDGHandle failed to validate");
    return validate_result.error();
  }

  if (auto validate_result = Validate(); !validate_result) {
    GALOIS_LOG_DEBUG("rdg failed to validate");
    return validate_result.error();
  }

  std::shared_ptr<arrow::Schema> merged = merge_result.ValueOrDie();

  if (auto write_result = WriteMetadata(handle, *merged); !write_result) {
    GALOIS_LOG_DEBUG("metadata write error");
    return write_result.error();
  }
  bool transpose = false;
  uint32_t policy_id = 0;
  if (part_metadata_) {
    transpose = part_metadata_->transposed_;
    policy_id = part_metadata_->policy_id_;
  }
  if (auto res = CommitRDG(handle, policy_id, transpose); !res) {
    return res.error();
  }
  return galois::ResultSuccess();
}

galois::Result<void>
RDG::DoLoad(const std::string& metadata_dir) {
  fs::path dir = metadata_dir;
  auto node_result = AddTables(
      dir, node_properties_,
      [rdg = this](const std::shared_ptr<arrow::Table>& table) {
        return AddProperties(
            table, &rdg->node_table_, &rdg->node_properties_,
            /* new_properties = */ false);
      });
  if (!node_result) {
    return node_result.error();
  }

  auto edge_result = AddTables(
      dir, this->edge_properties_,
      [rdg = this](const std::shared_ptr<arrow::Table>& table) {
        return AddProperties(
            table, &rdg->edge_table_, &rdg->edge_properties_,
            /* new_properties = */ false);
      });
  if (!edge_result) {
    return edge_result.error();
  }

  if (!part_properties_.empty()) {
    for (const auto& [k, v] : other_metadata_) {
      if (k == part_other_metadata_key) {
        part_metadata_ = std::make_unique<PartitionMetadata>();
        if (auto res = JsonParse(v, part_metadata_.get()); !res) {
          return res;
        }
      }
    }
    if (!part_metadata_) {
      GALOIS_LOG_DEBUG("found part properties but not the other metadata");
      return ErrorCode::InvalidArgument;
    }
    auto part_result = AddTables(
        dir, part_properties_,
        [rdg = this](const std::shared_ptr<arrow::Table>& table) {
          return AddPartitionMetadataArray(rdg->part_metadata_.get(), table);
        });
    if (!part_result) {
      return edge_result.error();
    }
  }

  fs::path t_path{dir};
  t_path.append(topology_path_);

  if (auto res = topology_file_storage_.Bind(t_path.string(), true); !res) {
    return res.error();
  }
  topology_size_ = topology_file_storage_.size();
  rdg_dir_ = metadata_dir;
  return galois::ResultSuccess();
}

galois::Result<void>
RDG::DoLoadPartial(const std::string& metadata_dir, const SliceArg& slice) {
  fs::path dir = metadata_dir;
  fs::path t_path{dir};
  t_path.append(topology_path_);

  if (auto res = topology_file_storage_.Bind(
          t_path.string(), slice.topo_off, slice.topo_off + slice.topo_size,
          true);
      !res) {
    return res.error();
  }
  topology_size_ = slice.topo_size;

  auto node_result = AddPartialTables(
      dir, node_properties_, slice.node_range,
      [rdg = this](const std::shared_ptr<arrow::Table>& table) {
        return AddProperties(
            table, &rdg->node_table_, &rdg->node_properties_,
            /* new_properties = */ false);
      });
  if (!node_result) {
    return node_result.error();
  }
  auto edge_result = AddPartialTables(
      dir, edge_properties_, slice.edge_range,
      [rdg = this](const std::shared_ptr<arrow::Table>& table) {
        return AddProperties(
            table, &rdg->edge_table_, &rdg->edge_properties_,
            /* new_properties = */ false);
      });
  if (!edge_result) {
    return edge_result.error();
  }

  rdg_dir_ = metadata_dir;
  return galois::ResultSuccess();
}

RDG::RDG() {
  std::vector<std::shared_ptr<arrow::Array>> empty;
  node_table_ = arrow::Table::Make(arrow::schema({}), empty, 0);
  edge_table_ = arrow::Table::Make(arrow::schema({}), empty, 0);
}

galois::Result<RDG>
RDG::Make(
    const std::string& partition_path,
    const std::vector<std::string>* node_props,
    const std::vector<std::string>* edge_props, const SliceArg* slice) {
  auto rdg_res = ReadMetadata(partition_path);
  if (!rdg_res) {
    return rdg_res.error();
  }
  RDG rdg(std::move(rdg_res.value()));

  fs::path m_path{partition_path};
  std::string metadata_dir = m_path.parent_path().string();

  if (auto res = rdg.PrunePropsTo(node_props, edge_props); !res) {
    return res.error();
  }
  if (slice != nullptr) {
    if (auto res = rdg.DoLoadPartial(metadata_dir, *slice); !res) {
      return res.error();
    }
  } else {
    if (auto res = rdg.DoLoad(metadata_dir); !res) {
      return res.error();
    }
  }
  return RDG(std::move(rdg));
}

galois::Result<void>
RDG::Validate() const {
  for (const auto& md : node_properties_) {
    if (md.path.find('/') != std::string::npos) {
      GALOIS_LOG_DEBUG(
          "node_property path doesn't contain a slash: \"{}\"", md.path);
      return ErrorCode::InvalidArgument;
    }
  }
  for (const auto& md : edge_properties_) {
    if (md.path.find('/') != std::string::npos) {
      GALOIS_LOG_DEBUG(
          "edge_property path doesn't contain a slash: \"{}\"", md.path);
      return ErrorCode::InvalidArgument;
    }
  }
  if (topology_path_.empty()) {
    GALOIS_LOG_DEBUG("either topology_path: \"{}\" is empty", topology_path_);
    return ErrorCode::InvalidArgument;
  }
  if (topology_path_.find('/') != std::string::npos) {
    GALOIS_LOG_DEBUG(
        "topology_path doesn't contain a slash: \"{}\"", topology_path_);
    return ErrorCode::InvalidArgument;
  }
  return galois::ResultSuccess();
}

bool
RDG::Equals(const RDG& other) const {
  // Assumption: t_f_s and other.t_f_s are both fully loaded into memory
  return topology_file_storage_.size() == other.topology_file_storage_.size() &&
         !memcmp(
             topology_file_storage_.ptr<uint8_t>(),
             other.topology_file_storage_.ptr<uint8_t>(),
             topology_file_storage_.size()) &&
         node_table_->Equals(*other.node_table_, true) &&
         edge_table_->Equals(*other.edge_table_, true);
}

galois::Result<RDG>
RDG::Load(
    RDGHandle handle, const std::vector<std::string>* node_props,
    const std::vector<std::string>* edge_props) {
  if (!handle.impl_->AllowsRead()) {
    GALOIS_LOG_DEBUG("handle does not allow full read");
    return ErrorCode::InvalidArgument;
  }
  return RDG::Make(
      handle.impl_->partition_path, node_props, edge_props,
      /* slice */ nullptr);
}

galois::Result<RDG>
RDG::Load(
    const std::string& rdg_meta_uri, const std::vector<std::string>* node_props,
    const std::vector<std::string>* edge_props) {
  auto part_path_res = GetPartPath(rdg_meta_uri, false);
  if (!part_path_res) {
    return part_path_res.error();
  }
  return RDG::Make(
      part_path_res.value(), node_props, edge_props,
      /* slice */ nullptr);
}

galois::Result<RDG>
RDG::LoadPartial(
    RDGHandle handle, const SliceArg& slice,
    const std::vector<std::string>* node_props,
    const std::vector<std::string>* edge_props) {
  if (!handle.impl_->AllowsReadPartial()) {
    GALOIS_LOG_DEBUG("handle does not allow partial read");
    return ErrorCode::InvalidArgument;
  }
  return RDG::Make(
      handle.impl_->partition_path, node_props, edge_props, &slice);
}

galois::Result<RDG>
RDG::LoadPartial(
    const std::string& rdg_meta_uri, const RDG::SliceArg& slice,
    const std::vector<std::string>* node_props,
    const std::vector<std::string>* edge_props) {
  auto part_path_res = GetPartPath(rdg_meta_uri, true);
  if (!part_path_res) {
    return part_path_res.error();
  }
  return RDG::Make(part_path_res.value(), node_props, edge_props, &slice);
}

galois::Result<void>
RDG::Store(RDGHandle handle, FileFrame* ff) {
  if (!handle.impl_->AllowsWrite()) {
    GALOIS_LOG_DEBUG("handle does not allow write");
    return ErrorCode::InvalidArgument;
  }
  if (handle.impl_->rdg_meta.dir_ != rdg_dir_) {
    UnbindFromStorage();
  }

  if (ff) {
    auto path_res = galois::NewPath(handle.impl_->rdg_meta.dir_, "topology");
    if (!path_res) {
      return path_res.error();
    }
    std::string t_path = std::move(path_res.value());
    ff->Bind(t_path);
    TSUBA_PTP(internal::FaultSensitivity::Normal);
    if (auto res = ff->Persist(); !res) {
      return res.error();
    }
    TSUBA_PTP(internal::FaultSensitivity::Normal);
    topology_path_ = galois::ExtractFileName(t_path);
  }

  return DoStore(handle);
}

galois::Result<void>
RDG::AddNodeProperties(const std::shared_ptr<arrow::Table>& table) {
  return AddProperties(
      table, &node_table_, &node_properties_,
      /* new_properties = */ true);
}

galois::Result<void>
RDG::AddEdgeProperties(const std::shared_ptr<arrow::Table>& table) {
  return AddProperties(
      table, &edge_table_, &edge_properties_,
      /* new_properties = */ true);
}

galois::Result<void>
RDG::DropNodeProperty(int i) {
  auto result = node_table_->RemoveColumn(i);
  if (!result.ok()) {
    GALOIS_LOG_DEBUG("arrow error: {}", result.status());
    return ErrorCode::ArrowError;
  }

  node_table_ = result.ValueOrDie();

  auto& p = node_properties_;
  assert(static_cast<unsigned>(i) < p.size());
  p.erase(p.begin() + i);

  return galois::ResultSuccess();
}

galois::Result<void>
RDG::DropEdgeProperty(int i) {
  auto result = edge_table_->RemoveColumn(i);
  if (!result.ok()) {
    GALOIS_LOG_DEBUG("arrow error: {}", result.status());
    return ErrorCode::ArrowError;
  }

  edge_table_ = result.ValueOrDie();

  auto& p = edge_properties_;
  assert(static_cast<unsigned>(i) < p.size());
  p.erase(p.begin() + i);

  return galois::ResultSuccess();
}

void
RDG::UnbindFromStorage() {
  for (PropertyMetadata& prop : node_properties_) {
    prop.path = "";
  }
  for (PropertyMetadata& prop : edge_properties_) {
    prop.path = "";
  }
  topology_path_ = "";
}

galois::Result<std::string>
FindLatestMetaFile(const std::string& name) {
  assert(IsManagedURI(name));

  std::unordered_set<std::string> files;
  auto list_res = FileListAsync(name, &files);
  if (!list_res) {
    return list_res.error();
  }

  std::unique_ptr<tsuba::FileAsyncWork> work = std::move(list_res.value());
  if (work != nullptr) {
    while (!work->Done()) {
      if (auto res = (*work)(); !res) {
        return res.error();
      }
    }
  }
  uint64_t version = 0;
  std::string found_meta;
  for (const std::string& file : files) {
    if (auto res = ParseVersion(file); res) {
      uint64_t new_version = res.value();
      if (new_version >= version) {
        version = new_version;
        found_meta = file;
      }
    }
  }
  if (found_meta.empty()) {
    GALOIS_LOG_DEBUG("could not find meta file in {}", name);
    return ErrorCode::InvalidArgument;
  }
  return galois::JoinPath(name, found_meta);
}

}  // namespace tsuba

galois::Result<tsuba::RDGPrefix>
tsuba::ExaminePrefix(const std::string& uri) {
  auto meta_path_res =
      GetMetaAndPartitionPath(uri, /* intend_partial_read */ true);
  if (!meta_path_res) {
    return meta_path_res.error();
  }
  auto& [meta, part_path] = meta_path_res.value();
  return DoExaminePrefix(part_path, meta.dir_);
}

galois::Result<tsuba::RDGPrefix>
tsuba::ExaminePrefix(RDGHandle handle) {
  if (!handle.impl_->AllowsReadPartial()) {
    GALOIS_LOG_DEBUG("handle not intended for partial read");
    return ErrorCode::InvalidArgument;
  }
  return DoExaminePrefix(
      handle.impl_->partition_path, handle.impl_->rdg_meta.dir_);
}

galois::Result<tsuba::RDGHandle>
tsuba::Open(const std::string& rdg_name, uint32_t flags) {
  RDGHandleImpl impl{};
  impl.flags = flags;

  if (!IsManagedURI(rdg_name)) {
    GALOIS_LOG_DEBUG(
        "{} is probably a literal rdg file and not suited for open", rdg_name);
    return ErrorCode::InvalidArgument;
  }

  auto meta_path_res = GetMetaAndPartitionPath(rdg_name, flags & kReadPartial);
  if (!meta_path_res) {
    return meta_path_res.error();
  }

  impl.rdg_meta = std::move(meta_path_res.value().first);
  impl.partition_path = std::move(meta_path_res.value().second);

  return RDGHandle{.impl_ = new RDGHandleImpl(std::move(impl))};
}

galois::Result<tsuba::RDGStat>
tsuba::Stat(const std::string& rdg_name) {
  auto rdg_res = RDGMeta::Make(rdg_name);
  if (!rdg_res) {
    if (rdg_res.error() == galois::ErrorCode::JsonParseFailed) {
      return RDGStat{
          .num_hosts = 1,
          .policy_id = 0,
          .transpose = false,
      };
    }
    return rdg_res.error();
  }

  RDGMeta meta = rdg_res.value();
  return RDGStat{
      .num_hosts = meta.num_hosts_,
      .policy_id = meta.policy_id_,
      .transpose = meta.transpose_,
  };
}

galois::Result<void>
tsuba::Close(RDGHandle handle) {
  delete handle.impl_;
  return galois::ResultSuccess();
}

galois::Result<void>
tsuba::Create(const std::string& name) {
  assert(IsManagedURI(name));
  // the default construction is the empty RDG
  tsuba::RDGMeta meta{};

  galois::CommBackend* comm = Comm();
  if (comm->ID == 0) {
    std::string s = meta.ToJsonString();
    if (auto res = tsuba::FileStore(
            tsuba::RDGMeta::FileName(name, meta.version_),
            reinterpret_cast<const uint8_t*>(s.data()), s.size());
        !res) {
      GALOIS_LOG_ERROR("failed to store RDG file");
      comm->NotifyFailure();
      return res.error();
    }
  }

  // NS handles MPI coordination
  if (auto res = tsuba::NS()->Create(name, meta); !res) {
    GALOIS_LOG_ERROR("failed to create RDG name");
    return res.error();
  }

  comm->Barrier();
  return galois::ResultSuccess();
}

galois::Result<void>
tsuba::Register(const std::string& name) {
  std::string meta_name = name;

  if (IsManagedURI(name)) {
    auto latest_res = FindLatestMetaFile(name);
    if (!latest_res) {
      return latest_res.error();
    }
    meta_name = std::move(latest_res.value());
  }

  auto meta_res = RDGMeta::MakeFromStorage(meta_name);
  if (!meta_res) {
    return meta_res.error();
  }
  RDGMeta meta = std::move(meta_res.value());

  // NS handles MPI coordination
  return tsuba::NS()->Create(meta.dir_, meta);
}

// Return the set of file names that hold this RDG's data
galois::Result<std::unordered_set<std::string>>
tsuba::FileNames(RDGHandle handle) {
  if (!handle.impl_->AllowsRead()) {
    GALOIS_LOG_DEBUG("handle does not allow full read");
    return ErrorCode::InvalidArgument;
  }
  auto rdg_res = ReadMetadata(handle.impl_->partition_path);
  if (!rdg_res) {
    return rdg_res.error();
  }
  std::unordered_set<std::string> fnames{};
  for (auto i = 0U; i < handle.impl_->rdg_meta.num_hosts_; ++i) {
    // All other file names are directory-local, so we pass an empty
    // directory instead of handle.impl_->rdg_meta.path for the partition files
    fnames.emplace(
        RDGMeta::PartitionFileName("", i, handle.impl_->rdg_meta.version_));
  }
  auto rdg = std::move(rdg_res.value());
  std::for_each(
      rdg.node_properties_.begin(), rdg.node_properties_.end(),
      [&fnames](PropertyMetadata pmd) { fnames.emplace(pmd.path); });
  std::for_each(
      rdg.edge_properties_.begin(), rdg.edge_properties_.end(),
      [&fnames](PropertyMetadata pmd) { fnames.emplace(pmd.path); });
  std::for_each(
      rdg.part_properties_.begin(), rdg.part_properties_.end(),
      [&fnames](PropertyMetadata pmd) { fnames.emplace(pmd.path); });

  fnames.emplace(rdg.topology_path_);
  return fnames;
}

// This shouldn't actually be used outside of this file, just exported for
// testing
galois::Result<std::shared_ptr<arrow::Table>>
tsuba::internal::LoadPartialTable(
    const std::string& expected_name, const std::string& file_path,
    int64_t offset, int64_t length) {
  try {
    return DoLoadPartialTable(expected_name, file_path, offset, length);
  } catch (const std::exception& exp) {
    GALOIS_LOG_DEBUG("arrow exception: {}", exp.what());
    return ErrorCode::ArrowError;
  }
}
