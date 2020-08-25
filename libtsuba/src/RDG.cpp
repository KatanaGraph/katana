#include "tsuba/RDG.h"

#include <cassert>
#include <exception>
#include <fstream>
#include <memory>
#include <unordered_set>

#include <arrow/filesystem/api.h>
#include <boost/filesystem.hpp>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/schema.h>
#include <parquet/arrow/writer.h>
#include <parquet/file_reader.h>
#include <parquet/platform.h>
#include <parquet/properties.h>

#include "galois/FileSystem.h"
#include "galois/Logging.h"
#include "galois/Result.h"
#include "tsuba/Errors.h"
#include "tsuba/tsuba.h"
#include "tsuba/file.h"
#include "tsuba/FileFrame.h"
#include "tsuba/RDG_internal.h"
#include "tsuba/FaultTest.h"

#include "GlobalState.h"
#include "json.h"

// constexpr uint32_t kPropertyMagicNo  = 0x4B808280; // KPRP
// constexpr uint32_t kPartitionMagicNo = 0x4B808284; // KPRT
constexpr uint32_t kRDGMagicNo = 0x4B524447; // KRDG

static const char* topology_path_key      = "kg.v1.topology.path";
static const char* node_property_path_key = "kg.v1.node_property.path";
static const char* node_property_name_key = "kg.v1.node_property.name";
static const char* edge_property_path_key = "kg.v1.edge_property.path";
static const char* edge_property_name_key = "kg.v1.edge_property.name";
static const char* part_property_path_key = "kg.v1.part_property.path";
static const char* part_property_name_key = "kg.v1.part_property.name";

static const char* part_other_metadata_key = "kg.v1.other_part_metadata.key";

// special partition property names
static const char* mirror_nodes_prop_name         = "mirror_nodes";
static const char* local_to_global_prop_name      = "local_to_global_vector";
static const char* global_to_local_keys_prop_name = "global_to_local_keys";
static const char* global_to_local_vals_prop_name = "global_to_local_values";

namespace fs = boost::filesystem;
using json   = nlohmann::json;

namespace tsuba {

// NOLINTNEXTLINE needed non-const ref for nlohmann compat
void to_json(json& j, const RDGMeta& meta) {
  j = json{
      {"magic", kRDGMagicNo},
      {"version", meta.version},
      {"previous_version", meta.previous_version},
      {"num_hosts", meta.num_hosts},
      {"policy_id", meta.policy_id},
      {"transpose", meta.transpose},
  };
}

// NOLINTNEXTLINE needed non-const ref for nlohmann compat
void from_json(const json& j, RDGMeta& meta) {
  uint32_t magic;
  j.at("magic").get_to(magic);
  j.at("version").get_to(meta.version);
  j.at("num_hosts").get_to(meta.num_hosts);

  // these values are optional
  if (auto it = j.find("previous_version"); it != j.end()) {
    it->get_to(meta.previous_version);
  }
  if (auto it = j.find("policy_id"); it != j.end()) {
    it->get_to(meta.policy_id);
  }
  if (auto it = j.find("transpose"); it != j.end()) {
    it->get_to(meta.transpose);
  }

  if (magic != kRDGMagicNo) {
    // nlohmann::json reports errors using exceptions
    throw std::runtime_error("RDG Magic number mismatch");
  }
}

galois::Result<RDGMeta> RDGMeta::Make(const std::string& rdg_name) {
  tsuba::FileView fv;
  if (auto res = fv.Bind(rdg_name, true); !res) {
    return res.error();
  }
  auto parse_res = JsonParse<tsuba::RDGMeta>(fv);
  if (!parse_res) {
    return parse_res.error();
  }
  return parse_res.value();
}

std::string RDGMetaToJsonString(const tsuba::RDGMeta& RDGMeta) {
  // POSIX specifies that test files end in a newline
  std::string s = json(RDGMeta).dump() + '\n';
  return s;
}

// e.g., rdg_path == s3://witchel-tests-east2/fault/simple/meta
std::string RDGMeta::FileName(const std::string& rdg_path, uint64_t version) {
  return fmt::format("{}_{}", rdg_path, version);
}

std::string RDGMeta::PartitionFileName(const std::string& rdg_path,
                                       uint32_t node_id, uint64_t version) {
  // witchel: These special casea aren't great, but lineage is coming
  if (rdg_path.size() == 0) {
    return fmt::format("{}_{}_{}", "meta", node_id, version);
  } else if (rdg_path.find("_") != std::string::npos) {
    auto res = galois::ExtractDirName(rdg_path);
    if (!res) {
      GALOIS_LOG_DEBUG("failed to get directory from {}", rdg_path);
      return "";
    }
    return fmt::format("{}/{}_{}_{}", res.value(), "meta", node_id, version);
  }
  return fmt::format("{}_{}_{}", rdg_path, node_id, version);
}

// NOLINTNEXTLINE needed non-const ref for nlohmann compat
void to_json(nlohmann::json& j, const PartitionMetadata& pmd) {
  j = json{{"transposed", pmd.transposed_},
           {"is_vertex_cut", pmd.is_vertex_cut_},
           {"num_global_nodes", pmd.num_global_nodes_},
           {"num_global_edges", pmd.num_global_edges_},
           {"num_nodes", pmd.num_nodes_},
           {"num_edges", pmd.num_edges_},
           {"num_owned", pmd.num_owned_},
           {"num_nodes_with_edges", pmd.num_nodes_with_edges_},
           {"cartesian_grid", pmd.cartesian_grid_}};
}

// NOLINTNEXTLINE needed non-const ref for nlohmann compat
void from_json(const nlohmann::json& j, PartitionMetadata& pmd) {
  j.at("transposed").get_to(pmd.transposed_);
  j.at("is_vertex_cut").get_to(pmd.is_vertex_cut_);
  j.at("num_global_nodes").get_to(pmd.num_global_nodes_);
  j.at("num_global_edges").get_to(pmd.num_global_edges_);
  j.at("num_nodes").get_to(pmd.num_nodes_);
  j.at("num_edges").get_to(pmd.num_edges_);
  j.at("num_owned").get_to(pmd.num_owned_);
  j.at("num_nodes_with_edges").get_to(pmd.num_nodes_with_edges_);
  j.at("cartesian_grid").get_to(pmd.cartesian_grid_);
}

struct RDGHandleImpl {
  // Property paths are relative to metadata path
  std::string metadata_dir;
  std::string rdg_path;
  std::string partition_path;
  uint32_t flags;
  RDGMeta rdg_meta;

  /// Perform some checks on assumed invariants
  galois::Result<void> Validate() const {
    if (metadata_dir.empty()) {
      GALOIS_LOG_DEBUG("metadata_dir: \"{}\" is empty", metadata_dir);
      return ErrorCode::InvalidArgument;
    }
    return galois::ResultSuccess();
  }
  constexpr bool AllowsReadPartial() const {
    return flags & tsuba::kReadPartial;
  }
  constexpr bool AllowsRead() const { return !AllowsReadPartial(); }
  constexpr bool AllowsWrite() const { return flags & tsuba::kReadWrite; }
};

tsuba::RDGFile::~RDGFile() {
  auto result = Close(handle_);
  if (!result) {
    GALOIS_LOG_ERROR("closing RDGFile: {}", result.error());
  }
}

} // namespace tsuba

namespace {

std::shared_ptr<parquet::WriterProperties> StandardWriterProperties() {
  // int64 timestamps with nanosecond resolution requires Parquet version 2.0.
  // In Arrow to Parquet version 1.0, nanosecond timestamps will get truncated
  // to milliseconds.
  return parquet::WriterProperties::Builder()
      .version(parquet::ParquetVersion::PARQUET_2_0)
      ->data_page_version(parquet::ParquetDataPageVersion::V2)
      ->build();
}

std::shared_ptr<parquet::ArrowWriterProperties> StandardArrowProperties() {
  return parquet::ArrowWriterProperties::Builder().build();
}

galois::Result<void>
PrunePropsTo(tsuba::RDG* rdg, const std::vector<std::string>& node_properties,
             const std::vector<std::string>& edge_properties) {

  // this function should be called during load BEFORE properties are populated
  assert(rdg->node_table->num_rows() == 0);
  assert(rdg->node_table->num_columns() == 0);
  assert(rdg->edge_table->num_rows() == 0);
  assert(rdg->node_table->num_columns() == 0);

  std::unordered_map<std::string, std::string> node_paths;
  for (const auto& m : rdg->node_properties) {
    node_paths.insert({m.name, m.path});
  }

  std::vector<tsuba::PropertyMetadata> next_node_properties;
  for (const auto& s : node_properties) {
    auto it = node_paths.find(s);
    if (it == node_paths.end()) {
      return tsuba::ErrorCode::PropertyNotFound;
    }

    next_node_properties.emplace_back(tsuba::PropertyMetadata{
        .name = it->first,
        .path = it->second,
    });
  }

  std::unordered_map<std::string, std::string> edge_paths;
  for (const auto& m : rdg->edge_properties) {
    edge_paths.insert({m.name, m.path});
  }

  std::vector<tsuba::PropertyMetadata> next_edge_properties;
  for (const auto& s : edge_properties) {
    auto it = edge_paths.find(s);
    if (it == edge_paths.end()) {
      return tsuba::ErrorCode::PropertyNotFound;
    }

    next_edge_properties.emplace_back(tsuba::PropertyMetadata{
        .name = it->first,
        .path = it->second,
    });
  }

  rdg->node_properties = next_node_properties;
  rdg->edge_properties = next_edge_properties;

  return galois::ResultSuccess();
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
    GALOIS_LOG_DEBUG("expected {} found {} instead", expected_name,
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
DoLoadPartialTable(const std::string& expected_name,
                   const std::string& file_path, int64_t offset,
                   int64_t length) {

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
  int rg_count             = reader->num_row_groups();
  int64_t row_offset       = 0;
  int64_t cumulative_rows  = 0;
  int64_t file_offset      = 0;
  int64_t cumulative_bytes = 0;
  for (int i = 0; cumulative_rows < offset + length && i < rg_count; ++i) {
    auto rg_md        = reader->parquet_reader()->metadata()->RowGroup(i);
    int64_t new_rows  = rg_md->num_rows();
    int64_t new_bytes = rg_md->total_byte_size();
    if (offset < cumulative_rows + new_rows) {
      if (row_groups.empty()) {
        row_offset  = offset - cumulative_rows;
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
    GALOIS_LOG_DEBUG("expected {} found {} instead", expected_name,
                     schema->field(0)->name());
    return tsuba::ErrorCode::InvalidArgument;
  }

  return out->Slice(row_offset, length);
}

template <typename AddFn>
galois::Result<void>
AddTables(const fs::path& dir,
          const std::vector<tsuba::PropertyMetadata>& properties,
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
AddPartialTables(const fs::path& dir,
                 const std::vector<tsuba::PropertyMetadata>& properties,
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
DoStoreArrowArrayAtName(const std::shared_ptr<arrow::ChunkedArray>& array,
                        const std::string& dir, const std::string& name) {
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
StoreArrowArrayAtName(const std::shared_ptr<arrow::ChunkedArray>& array,
                      const std::string& dir, const std::string& name) {
  try {
    return DoStoreArrowArrayAtName(array, dir, name);
  } catch (const std::exception& exp) {
    GALOIS_LOG_DEBUG("arrow exception: {}", exp.what());
    return tsuba::ErrorCode::ArrowError;
  }
}

galois::Result<std::vector<tsuba::PropertyMetadata>>
WriteTable(const arrow::Table& table,
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
  auto it                                              = next_paths.begin();
  for (auto& v : next_properties) {
    if (v.path.empty()) {
      v.path = *it++;
    }
  }

  return next_properties;
}

std::string MirrorPropName(unsigned i) {
  return std::string(mirror_nodes_prop_name) + "_" + std::to_string(i);
}

galois::Result<std::vector<tsuba::PropertyMetadata>>
WritePartArrays(const tsuba::PartitionMetadata& part_meta,
                const std::string& dir) {

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

  auto l2g_res = StoreArrowArrayAtName(part_meta.local_to_global_vector_, dir,
                                       local_to_global_prop_name);
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
AddProperties(const std::shared_ptr<arrow::Table>& table,
              std::shared_ptr<arrow::Table>* to_update,
              std::vector<tsuba::PropertyMetadata>* properties,
              bool new_properties) {
  std::shared_ptr<arrow::Table> current = *to_update;

  if (current->num_columns() > 0 && current->num_rows() != table->num_rows()) {
    GALOIS_LOG_DEBUG("expected {} rows found {} instead", current->num_rows(),
                     table->num_rows());
    return tsuba::ErrorCode::InvalidArgument;
  }

  std::shared_ptr<arrow::Table> next = current;

  if (current->num_columns() == 0 && current->num_rows() == 0) {
    next = table;
  } else {
    const auto& schema = table->schema();
    int last           = current->num_columns();

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
galois::Result<tsuba::RDG> DoReadMetadata(tsuba::RDGHandle handle) {
  auto fv = std::make_shared<tsuba::FileView>();
  if (auto res = fv->Bind(handle.impl_->partition_path, false); !res) {
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

  rdg.node_properties = std::move(node_properties_result.value());
  rdg.edge_properties = std::move(edge_properties_result.value());
  rdg.part_properties = std::move(part_properties_result.value());
  rdg.other_metadata  = std::move(other_metadata);
  rdg.topology_path   = std::move(topology_path);

  return tsuba::RDG(std::move(rdg));
}

galois::Result<tsuba::RDG> ReadMetadata(tsuba::RDGHandle handle) {
  try {
    return DoReadMetadata(handle);
  } catch (const std::exception& exp) {
    GALOIS_LOG_DEBUG("arrow exception: {}", exp.what());
    return tsuba::ErrorCode::ArrowError;
  }
}

std::pair<std::vector<std::string>, std::vector<std::string>>
MakeMetadata(const tsuba::RDG& rdg) {
  std::vector<std::string> keys;
  std::vector<std::string> values;

  keys.emplace_back(topology_path_key);
  values.emplace_back(rdg.topology_path);

  for (const auto& v : rdg.node_properties) {
    keys.emplace_back(node_property_name_key);
    values.emplace_back(v.name);
    keys.emplace_back(node_property_path_key);
    values.emplace_back(v.path);
  }

  for (const auto& v : rdg.edge_properties) {
    keys.emplace_back(edge_property_name_key);
    values.emplace_back(v.name);
    keys.emplace_back(edge_property_path_key);
    values.emplace_back(v.path);
  }

  for (const auto& v : rdg.part_properties) {
    keys.emplace_back(part_property_name_key);
    values.emplace_back(v.name);
    keys.emplace_back(part_property_path_key);
    values.emplace_back(v.path);
  }

  for (const auto& v : rdg.other_metadata) {
    keys.emplace_back(v.first);
    values.emplace_back(v.second);
  }

  return std::make_pair(keys, values);
}

galois::Result<void> DoWriteMetadata(tsuba::RDGHandle handle,
                                     const tsuba::RDG& rdg,
                                     const arrow::Schema& schema) {

  std::shared_ptr<parquet::SchemaDescriptor> schema_descriptor;
  auto to_result = parquet::arrow::ToParquetSchema(
      &schema, *StandardWriterProperties(), &schema_descriptor);
  if (!to_result.ok()) {
    GALOIS_LOG_DEBUG("arrow error: {}", to_result);
    return tsuba::ErrorCode::ArrowError;
  }

  auto kvs = MakeMetadata(rdg);
  auto parquet_kvs =
      std::make_shared<parquet::KeyValueMetadata>(kvs.first, kvs.second);
  auto builder = parquet::FileMetaDataBuilder::Make(
      schema_descriptor.get(), StandardWriterProperties(), parquet_kvs);
  auto md = builder->Finish();

  auto ff = std::make_shared<tsuba::FileFrame>();
  if (auto res = ff->Init(); !res) {
    return res.error();
  }

  auto write_result = parquet::arrow::WriteMetaDataFile(*md, ff.get());
  if (!write_result.ok()) {
    return tsuba::ErrorCode::InvalidArgument;
  }

  ff->Bind(tsuba::RDGMeta::PartitionFileName(
      handle.impl_->rdg_path, tsuba::Comm()->ID,
      handle.impl_->rdg_meta.version + 1));
  TSUBA_PTP(tsuba::internal::FaultSensitivity::Normal);
  if (auto res = ff->Persist(); !res) {
    return res.error();
  }
  TSUBA_PTP(tsuba::internal::FaultSensitivity::Normal);
  return galois::ResultSuccess();
}

galois::Result<void> WriteMetadata(tsuba::RDGHandle handle,
                                   const tsuba::RDG& rdg,
                                   const arrow::Schema& schema) {
  try {
    return DoWriteMetadata(handle, rdg, schema);
  } catch (const std::exception& exp) {
    GALOIS_LOG_DEBUG("arrow exception: {}", exp.what());
    return tsuba::ErrorCode::ArrowError;
  }
}

// TOCTTOU since it's the local file system but will change when we manage
// our own namespace, create the file here so that at least another call
// will fail
galois::Result<void> CreateNewRDG(const std::string& name,
                                  bool overwrite = false) {
  // need this call in order to respect overwrite
  auto create_res = tsuba::FileCreate(name, overwrite);
  if (!create_res) {
    GALOIS_LOG_ERROR("create_res");
    return create_res;
  }

  std::string s = RDGMetaToJsonString(tsuba::RDGMeta{
      .version          = UINT64_C(0),
      .previous_version = UINT64_C(0),
      .num_hosts        = tsuba::Comm()->Num,
      .policy_id        = UINT64_C(0),
      .transpose        = false,
  });
  if (auto res = tsuba::FileStore(
          name, reinterpret_cast<const uint8_t*>(s.data()), s.size());
      !res) {
    GALOIS_LOG_ERROR("failed to store RDG file");
    return res.error();
  }
  return galois::ResultSuccess();
}

galois::Result<void>
AddPartitionMetadataArray(tsuba::PartitionMetadata* p_meta,
                          const std::shared_ptr<arrow::Table>& table) {
  auto field                                      = table->schema()->field(0);
  const std::string& name                         = field->name();
  const std::shared_ptr<arrow::ChunkedArray>& col = table->column(0);
  if (name.find(mirror_nodes_prop_name) == 0) {
    p_meta->mirror_nodes_.emplace_back(col);
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

galois::Result<void> DoLoad(tsuba::RDGHandle handle, tsuba::RDG* rdg) {
  fs::path dir     = handle.impl_->metadata_dir;
  auto node_result = AddTables(
      dir, rdg->node_properties,
      [&rdg](const std::shared_ptr<arrow::Table>& table) {
        return AddProperties(table, &rdg->node_table, &rdg->node_properties,
                             /* new_properties = */ false);
      });
  if (!node_result) {
    return node_result.error();
  }

  auto edge_result = AddTables(
      dir, rdg->edge_properties,
      [&rdg](const std::shared_ptr<arrow::Table>& table) {
        return AddProperties(table, &rdg->edge_table, &rdg->edge_properties,
                             /* new_properties = */ false);
      });
  if (!edge_result) {
    return edge_result.error();
  }

  if (!rdg->part_properties.empty()) {
    for (const auto& [k, v] : rdg->other_metadata) {
      if (k == part_other_metadata_key) {
        rdg->part_metadata = std::make_unique<tsuba::PartitionMetadata>();
        if (auto res = JsonParse(v, rdg->part_metadata.get()); !res) {
          return res;
        }
      }
    }
    if (!rdg->part_metadata) {
      GALOIS_LOG_DEBUG("found part properties but not the other metadata");
      return tsuba::ErrorCode::InvalidArgument;
    }
    auto part_result = AddTables(
        dir, rdg->part_properties,
        [&rdg](const std::shared_ptr<arrow::Table>& table) {
          return AddPartitionMetadataArray(rdg->part_metadata.get(), table);
        });
    if (!part_result) {
      return edge_result.error();
    }
  }

  fs::path t_path{dir};
  t_path.append(rdg->topology_path);

  if (auto res = rdg->topology_file_storage.Bind(t_path.string(), true); !res) {
    return res.error();
  }
  rdg->topology_size = rdg->topology_file_storage.size();
  rdg->rdg_dir       = handle.impl_->metadata_dir;
  return galois::ResultSuccess();
}

galois::Result<tsuba::RDGPrefix>
BindOutIndex(const std::string& topology_path) {
  tsuba::GRHeader header;
  if (auto res = tsuba::FilePeek(topology_path, &header); !res) {
    return res.error();
  }
  tsuba::FileView fv;
  if (auto res =
          fv.Bind(topology_path,
                  sizeof(header) + (header.num_nodes * sizeof(uint64_t)), true);
      !res) {
    return res.error();
  }
  tsuba::RDGPrefix pfx;
  pfx.prefix_storage = tsuba::FileView(std::move(fv));
  pfx.view_offset    = sizeof(header) + (header.num_nodes * sizeof(uint64_t));
  return tsuba::RDGPrefix(std::move(pfx));
}

galois::Result<void> DoPartialLoad(tsuba::RDGHandle handle,
                                   std::pair<uint64_t, uint64_t> node_range,
                                   std::pair<uint64_t, uint64_t> edge_range,
                                   uint64_t topo_off, uint64_t topo_size,
                                   tsuba::RDG* rdg) {

  fs::path dir = handle.impl_->metadata_dir;
  fs::path t_path{dir};
  t_path.append(rdg->topology_path);

  if (auto res = rdg->topology_file_storage.Bind(t_path.string(), topo_off,
                                                 topo_off + topo_size, true);
      !res) {
    return res.error();
  }
  rdg->topology_size = topo_size;

  auto node_result = AddPartialTables(
      dir, rdg->node_properties, node_range,
      [&rdg](const std::shared_ptr<arrow::Table>& table) {
        return AddProperties(table, &rdg->node_table, &rdg->node_properties,
                             /* new_properties = */ false);
      });
  if (!node_result) {
    return node_result.error();
  }
  auto edge_result = AddPartialTables(
      dir, rdg->edge_properties, edge_range,
      [&rdg](const std::shared_ptr<arrow::Table>& table) {
        return AddProperties(table, &rdg->edge_table, &rdg->edge_properties,
                             /* new_properties = */ false);
      });
  if (!edge_result) {
    return edge_result.error();
  }

  rdg->rdg_dir = handle.impl_->metadata_dir;
  return galois::ResultSuccess();
}

galois::Result<void> CommitRDG(tsuba::RDGHandle handle, uint32_t policy_id,
                               bool transpose) {
  galois::CommBackend* comm = tsuba::Comm();
  tsuba::RDGMeta new_meta{
      .version          = handle.impl_->rdg_meta.version + 1,
      .previous_version = handle.impl_->rdg_meta.version,
      .num_hosts        = comm->Num,
      .policy_id        = policy_id,
      .transpose        = transpose,
  };

  TSUBA_PTP(tsuba::internal::FaultSensitivity::High);
  comm->Barrier();
  TSUBA_PTP(tsuba::internal::FaultSensitivity::High);
  if (comm->ID == 0) {
    TSUBA_PTP(tsuba::internal::FaultSensitivity::High);
    std::string s = RDGMetaToJsonString(handle.impl_->rdg_meta);
    if (auto res = tsuba::FileStore(
            tsuba::RDGMeta::FileName(handle.impl_->rdg_path,
                                     handle.impl_->rdg_meta.version),
            reinterpret_cast<const uint8_t*>(s.data()), s.size());
        !res) {
      TSUBA_PTP(tsuba::internal::FaultSensitivity::High);
      GALOIS_LOG_ERROR("failed to store previous RDGMeta file");
      return res.error();
    }
    s = RDGMetaToJsonString(new_meta);
    TSUBA_PTP(tsuba::internal::FaultSensitivity::High);
    if (auto res = tsuba::FileStore(handle.impl_->rdg_path,
                                    reinterpret_cast<const uint8_t*>(s.data()),
                                    s.size());
        !res) {
      TSUBA_PTP(tsuba::internal::FaultSensitivity::High);
      GALOIS_LOG_ERROR("failed to store RDG file");
      return res.error();
    }
  }
  TSUBA_PTP(tsuba::internal::FaultSensitivity::High);
  comm->Barrier();
  TSUBA_PTP(tsuba::internal::FaultSensitivity::High);

  handle.impl_->rdg_meta = new_meta;
  return galois::ResultSuccess();
}

galois::Result<void> DoStore(tsuba::RDGHandle handle, tsuba::RDG* rdg) {
  if (rdg->topology_path.empty()) {
    // No topology file; create one
    auto path_res = galois::NewPath(handle.impl_->metadata_dir, "topology");
    if (!path_res) {
      return path_res.error();
    }
    std::string t_path = std::move(path_res.value());

    TSUBA_PTP(tsuba::internal::FaultSensitivity::Normal);
    if (auto res =
            tsuba::FileStore(t_path, rdg->topology_file_storage.ptr<uint8_t>(),
                             rdg->topology_file_storage.size());
        !res) {
      return res.error();
    }
    TSUBA_PTP(tsuba::internal::FaultSensitivity::Normal);
    rdg->topology_path = galois::ExtractFileName(t_path);
  }

  auto node_write_result = WriteTable(*rdg->node_table, rdg->node_properties,
                                      handle.impl_->metadata_dir);
  if (!node_write_result) {
    GALOIS_LOG_DEBUG("unable to write node properties");
    return node_write_result.error();
  }
  rdg->node_properties = std::move(node_write_result.value());

  auto edge_write_result = WriteTable(*rdg->edge_table, rdg->edge_properties,
                                      handle.impl_->metadata_dir);
  if (!edge_write_result) {
    GALOIS_LOG_DEBUG("unable to write edge properties");
    return edge_write_result.error();
  }
  rdg->edge_properties = std::move(edge_write_result.value());

  if (rdg->part_metadata) {
    if (!rdg->part_properties.empty()) {
      GALOIS_LOG_ERROR("We don't support repartitioning (yet)");
      return tsuba::ErrorCode::NotImplemented;
    }
    auto part_write_result =
        WritePartArrays(*rdg->part_metadata, handle.impl_->metadata_dir);
    if (!part_write_result) {
      GALOIS_LOG_DEBUG("WritePartMetadata for part_properties failed");
      return part_write_result.error();
    }
    rdg->part_properties = std::move(part_write_result.value());

    // stash the rest of the struct in other_metadata
    rdg->other_metadata.emplace_back(part_other_metadata_key,
                                     json(*rdg->part_metadata).dump());
  }

  auto merge_result = arrow::SchemaBuilder::Merge(
      {rdg->node_table->schema(), rdg->edge_table->schema()},
      arrow::SchemaBuilder::ConflictPolicy::CONFLICT_APPEND);
  if (!merge_result.ok()) {
    GALOIS_LOG_DEBUG("arrow error: {}", merge_result.status());
    return tsuba::ErrorCode::ArrowError;
  }

  if (auto validate_result = handle.impl_->Validate(); !validate_result) {
    GALOIS_LOG_DEBUG("RDGHandle failed to validate");
    return validate_result.error();
  }

  if (auto validate_result = rdg->Validate(); !validate_result) {
    GALOIS_LOG_DEBUG("rdg failed to validate");
    return validate_result.error();
  }

  std::shared_ptr<arrow::Schema> merged = merge_result.ValueOrDie();

  if (auto write_result = WriteMetadata(handle, *rdg, *merged); !write_result) {
    GALOIS_LOG_DEBUG("metadata write error");
    return write_result.error();
  }
  bool transpose     = false;
  uint32_t policy_id = 0;
  if (rdg->part_metadata) {
    transpose = rdg->part_metadata->transposed_;
    policy_id = rdg->part_metadata->policy_id_;
  }
  if (auto res = CommitRDG(handle, policy_id, transpose); !res) {
    return res.error();
  }
  return galois::ResultSuccess();
}

galois::Result<void> UnbindFromStorage(tsuba::RDG* rdg) {
  for (auto& prop : rdg->node_properties) {
    prop.path = "";
  }
  for (auto& prop : rdg->edge_properties) {
    prop.path = "";
  }
  rdg->topology_path = "";
  return galois::ResultSuccess();
}

galois::Result<tsuba::RDGMeta> ParseRDGFile(const std::string& path) {
  tsuba::FileView fv;
  if (auto res = fv.Bind(path, true); !res) {
    return res.error();
  }
  return JsonParse<tsuba::RDGMeta>(fv);
}

} // namespace

tsuba::RDG::RDG() {
  std::vector<std::shared_ptr<arrow::Array>> empty;
  node_table = arrow::Table::Make(arrow::schema({}), empty, 0);
  edge_table = arrow::Table::Make(arrow::schema({}), empty, 0);
}

galois::Result<void> tsuba::RDG::Validate() const {
  for (const auto& md : node_properties) {
    if (md.path.find('/') != std::string::npos) {
      GALOIS_LOG_DEBUG("node_property path doesn't contain a slash: \"{}\"",
                       md.path);
      return ErrorCode::InvalidArgument;
    }
  }
  for (const auto& md : edge_properties) {
    if (md.path.find('/') != std::string::npos) {
      GALOIS_LOG_DEBUG("edge_property path doesn't contain a slash: \"{}\"",
                       md.path);
      return ErrorCode::InvalidArgument;
    }
  }
  if (topology_path.empty()) {
    GALOIS_LOG_DEBUG("either topology_path: \"{}\" is empty", topology_path);
    return ErrorCode::InvalidArgument;
  }
  if (topology_path.find('/') != std::string::npos) {
    GALOIS_LOG_DEBUG("topology_path doesn't contain a slash: \"{}\"",
                     topology_path);
    return ErrorCode::InvalidArgument;
  }
  return galois::ResultSuccess();
}

bool tsuba::RDG::Equals(const RDG& other) const {
  // Assumption: t_f_s and other.t_f_s are both fully loaded into memory
  return topology_file_storage.size() == other.topology_file_storage.size() &&
         !memcmp(topology_file_storage.ptr<uint8_t>(),
                 other.topology_file_storage.ptr<uint8_t>(),
                 topology_file_storage.size()) &&
         node_table->Equals(*other.node_table, true) &&
         edge_table->Equals(*other.edge_table, true);
}

galois::Result<tsuba::RDGPrefix> tsuba::ExaminePrefix(tsuba::RDGHandle handle) {
  if (!handle.impl_->AllowsReadPartial()) {
    GALOIS_LOG_DEBUG("handle not intended for partial read");
    return ErrorCode::InvalidArgument;
  }
  auto meta_res = ReadMetadata(handle);
  if (!meta_res) {
    return meta_res.error();
  }

  tsuba::RDGPrefix pfx;
  RDG rdg = std::move(meta_res.value());
  if (rdg.topology_path.empty()) {
    return RDGPrefix(std::move(pfx));
  }

  fs::path t_path{handle.impl_->metadata_dir};
  t_path.append(rdg.topology_path);

  auto out_idx_res = BindOutIndex(t_path.string());
  if (!out_idx_res) {
    return out_idx_res.error();
  }
  tsuba::RDGPrefix npfx = std::move(out_idx_res.value());

  // This is safe because BindOutIndex binds the start of the file
  npfx.prefix = npfx.prefix_storage.ptr<GRPrefix>();
  return RDGPrefix(std::move(npfx));
}

galois::Result<tsuba::RDGHandle> tsuba::Open(const std::string& rdg_name,
                                             uint32_t flags) {
  tsuba::RDGHandleImpl impl;

  fs::path m_path{rdg_name};
  impl.metadata_dir = m_path.parent_path().string();
  impl.rdg_path     = rdg_name;
  impl.flags        = flags;

  auto rdg_res = ParseRDGFile(rdg_name);
  if (!rdg_res) {
    if (rdg_res.error() == tsuba::ErrorCode::JsonParseFailed) {
      // Maintain this?
      GALOIS_WARN_ONCE("Deprecated behavior: treating invalid RDG file like a "
                       "partition file");
      impl.partition_path = rdg_name;
      impl.rdg_meta       = {.version          = 0UL,
                       .previous_version = 0UL,
                       .num_hosts        = tsuba::Comm()->Num};
      return RDGHandle{.impl_ = new tsuba::RDGHandleImpl(impl)};
    }
    GALOIS_LOG_DEBUG("tsuba::Open 0");
    return rdg_res.error();
  }

  impl.rdg_meta = rdg_res.value();

  auto ret = RDGHandle{.impl_ = new tsuba::RDGHandleImpl(impl)};

  if (ret.impl_->AllowsReadPartial()) {
    if (ret.impl_->rdg_meta.num_hosts != 1) {
      GALOIS_LOG_ERROR("cannot partially read partitioned graph");
      return ErrorCode::InvalidArgument;
    }
    ret.impl_->partition_path = tsuba::RDGMeta::PartitionFileName(
        ret.impl_->rdg_path, 0, ret.impl_->rdg_meta.version);
  } else {
    if (ret.impl_->rdg_meta.num_hosts != tsuba::Comm()->Num) {
      GALOIS_LOG_DEBUG("number of hosts for partitioned graph {} does not "
                       "match number of current hosts {}",
                       ret.impl_->rdg_meta.num_hosts, tsuba::Comm()->Num);
      return ErrorCode::InvalidArgument;
    }
    ret.impl_->partition_path = tsuba::RDGMeta::PartitionFileName(
        ret.impl_->rdg_path, tsuba::Comm()->ID, ret.impl_->rdg_meta.version);
  }
  return ret;
}

galois::Result<tsuba::RDGStat> tsuba::Stat(const std::string& rdg_name) {
  auto rdg_res = ParseRDGFile(rdg_name);
  if (!rdg_res) {
    if (rdg_res.error() == tsuba::ErrorCode::JsonParseFailed) {
      return RDGStat{.num_hosts = 1, .policy_id = 0, .transpose = false};
    }
    return rdg_res.error();
  }

  RDGMeta meta = rdg_res.value();

  return RDGStat{
      .num_hosts = meta.num_hosts,
      .policy_id = meta.policy_id,
      .transpose = meta.transpose,
  };
}

galois::Result<void> tsuba::Close(RDGHandle handle) {
  delete handle.impl_;
  return galois::ResultSuccess();
}

galois::Result<void> tsuba::Create(const std::string& name) {
  galois::CommBackend* comm = tsuba::Comm();
  if (comm->ID == 0) {
    if (auto good = CreateNewRDG(name); !good) {
      GALOIS_LOG_DEBUG("CreateNewRDG failed: {}", good.error());
      comm->NotifyFailure();
      return good.error();
    }
  }
  comm->Barrier();
  return galois::ResultSuccess();
}

galois::Result<void> tsuba::Rename(RDGHandle handle, const std::string& name,
                                   int flags) {
  if (!handle.impl_->AllowsWrite()) {
    GALOIS_LOG_DEBUG("handle does not allow write");
    return ErrorCode::InvalidArgument;
  }
  (void)name;
  (void)flags;
  return tsuba::ErrorCode::NotImplemented;
}

galois::Result<tsuba::RDG> tsuba::Load(RDGHandle handle) {
  if (!handle.impl_->AllowsRead()) {
    GALOIS_LOG_DEBUG("handle does not allow full read");
    return ErrorCode::InvalidArgument;
  }
  auto rdg_res = ReadMetadata(handle);
  if (!rdg_res) {
    return rdg_res.error();
  }
  RDG rdg(std::move(rdg_res.value()));

  if (auto res = DoLoad(handle, &rdg); !res) {
    return res.error();
  }
  return RDG(std::move(rdg));
}

galois::Result<tsuba::RDG>
tsuba::Load(RDGHandle handle, const std::vector<std::string>& node_properties,
            const std::vector<std::string>& edge_properties) {
  if (!handle.impl_->AllowsRead()) {
    GALOIS_LOG_DEBUG("handle does not allow full read");
    return ErrorCode::InvalidArgument;
  }

  auto rdg_res = ReadMetadata(handle);
  if (!rdg_res) {
    return rdg_res.error();
  }
  RDG rdg(std::move(rdg_res.value()));

  if (auto res = PrunePropsTo(&rdg, node_properties, edge_properties); !res) {
    return res.error();
  }
  if (auto res = DoLoad(handle, &rdg); !res) {
    return res.error();
  }
  return RDG(std::move(rdg));
}

galois::Result<tsuba::RDG>
tsuba::LoadPartial(RDGHandle handle, std::pair<uint64_t, uint64_t> node_range,
                   std::pair<uint64_t, uint64_t> edge_range, uint64_t topo_off,
                   uint64_t topo_size) {
  if (!handle.impl_->AllowsReadPartial()) {
    GALOIS_LOG_DEBUG("handle does not allow partial read");
    return ErrorCode::InvalidArgument;
  }
  auto rdg_res = ReadMetadata(handle);
  if (!rdg_res) {
    return rdg_res.error();
  }
  RDG rdg(std::move(rdg_res.value()));
  if (auto res = DoPartialLoad(handle, node_range, edge_range, topo_off,
                               topo_size, &rdg);
      !res) {
    return res.error();
  }
  return RDG(std::move(rdg));
}

galois::Result<tsuba::RDG>
tsuba::LoadPartial(RDGHandle handle, std::pair<uint64_t, uint64_t> node_range,
                   std::pair<uint64_t, uint64_t> edge_range, uint64_t topo_off,
                   uint64_t topo_size,
                   const std::vector<std::string>& node_properties,
                   const std::vector<std::string>& edge_properties) {
  if (!handle.impl_->AllowsReadPartial()) {
    GALOIS_LOG_DEBUG("handle does not allow partial read");
    return ErrorCode::InvalidArgument;
  }
  auto rdg_res = ReadMetadata(handle);
  if (!rdg_res) {
    return rdg_res.error();
  }
  RDG rdg(std::move(rdg_res.value()));

  if (auto res = PrunePropsTo(&rdg, node_properties, edge_properties); !res) {
    return res.error();
  }
  if (auto res = DoPartialLoad(handle, node_range, edge_range, topo_off,
                               topo_size, &rdg);
      !res) {
    return res.error();
  }
  return RDG(std::move(rdg));
}

galois::Result<void> tsuba::Store(RDGHandle handle, RDG* rdg) {
  if (!handle.impl_->AllowsWrite()) {
    GALOIS_LOG_DEBUG("handle does not allow write");
    return ErrorCode::InvalidArgument;
  }
  if (handle.impl_->metadata_dir != rdg->rdg_dir) {
    if (auto res = UnbindFromStorage(rdg); !res) {
      return res.error();
    }
  }
  return DoStore(handle, rdg);
}

galois::Result<void> tsuba::Store(RDGHandle handle, RDG* rdg, FileFrame* ff) {
  if (!handle.impl_->AllowsWrite()) {
    GALOIS_LOG_DEBUG("handle does not allow write");
    return ErrorCode::InvalidArgument;
  }
  // TODO(ddn): property paths will be dangling if metadata directory changes
  // but absolute paths in metadata make moving property files around hard.
  if (handle.impl_->metadata_dir != rdg->rdg_dir) {
    if (auto res = UnbindFromStorage(rdg); !res) {
      return res.error();
    }
  }

  auto path_res = galois::NewPath(handle.impl_->metadata_dir, "topology");
  if (!path_res) {
    return path_res.error();
  }
  std::string t_path = std::move(path_res.value());
  ff->Bind(t_path);
  TSUBA_PTP(tsuba::internal::FaultSensitivity::Normal);
  if (auto res = ff->Persist(); !res) {
    return res.error();
  }
  TSUBA_PTP(tsuba::internal::FaultSensitivity::Normal);
  rdg->topology_path = galois::ExtractFileName(t_path);

  return DoStore(handle, rdg);
}

// Return the set of file names that hold this RDG's data
galois::Result<std::unordered_set<std::string>>
tsuba::FileNames(RDGHandle handle) {
  if (!handle.impl_->AllowsRead()) {
    GALOIS_LOG_DEBUG("handle does not allow full read");
    return ErrorCode::InvalidArgument;
  }
  auto rdg_res = ReadMetadata(handle);
  if (!rdg_res) {
    return rdg_res.error();
  }
  std::unordered_set<std::string> fnames{};
  // Add master meta file
  fnames.emplace(galois::ExtractFileName(handle.impl_->rdg_path));
  for (auto i = 0U; i < handle.impl_->rdg_meta.num_hosts; ++i) {
    // All other file names are directory-local, so we pass an empty
    // directory instead of handle.impl_->rdg_path for the partition files
    fnames.emplace(
        RDGMeta::PartitionFileName("", i, handle.impl_->rdg_meta.version));
  }
  auto rdg = std::move(rdg_res.value());
  std::for_each(
      rdg.node_properties.begin(), rdg.node_properties.end(),
      [&fnames](tsuba::PropertyMetadata pmd) { fnames.emplace(pmd.path); });
  std::for_each(
      rdg.edge_properties.begin(), rdg.edge_properties.end(),
      [&fnames](tsuba::PropertyMetadata pmd) { fnames.emplace(pmd.path); });
  std::for_each(
      rdg.part_properties.begin(), rdg.part_properties.end(),
      [&fnames](tsuba::PropertyMetadata pmd) { fnames.emplace(pmd.path); });

  fnames.emplace(rdg.topology_path);
  return fnames;
}

galois::Result<void>
tsuba::AddNodeProperties(RDG* rdg, const std::shared_ptr<arrow::Table>& table) {
  return AddProperties(table, &rdg->node_table, &rdg->node_properties,
                       /* new_properties = */ true);
}

galois::Result<void>
tsuba::AddEdgeProperties(RDG* rdg, const std::shared_ptr<arrow::Table>& table) {
  return AddProperties(table, &rdg->edge_table, &rdg->edge_properties,
                       /* new_properties = */ true);
}

galois::Result<void> tsuba::DropNodeProperty(RDG* rdg, int i) {
  auto result = rdg->node_table->RemoveColumn(i);
  if (!result.ok()) {
    GALOIS_LOG_DEBUG("arrow error: {}", result.status());
    return ErrorCode::ArrowError;
  }

  rdg->node_table = result.ValueOrDie();

  auto& p = rdg->node_properties;
  assert(static_cast<unsigned>(i) < p.size());
  p.erase(p.begin() + i);

  return galois::ResultSuccess();
}

galois::Result<void> tsuba::DropEdgeProperty(RDG* rdg, int i) {
  auto result = rdg->edge_table->RemoveColumn(i);
  if (!result.ok()) {
    GALOIS_LOG_DEBUG("arrow error: {}", result.status());
    return ErrorCode::ArrowError;
  }

  rdg->edge_table = result.ValueOrDie();

  auto& p = rdg->edge_properties;
  assert(static_cast<unsigned>(i) < p.size());
  p.erase(p.begin() + i);

  return galois::ResultSuccess();
}

// This shouldn't actually be used outside of this file, just exported for
// testing
galois::Result<std::shared_ptr<arrow::Table>>
tsuba::internal::LoadPartialTable(const std::string& expected_name,
                                  const std::string& file_path, int64_t offset,
                                  int64_t length) {
  try {
    return DoLoadPartialTable(expected_name, file_path, offset, length);
  } catch (const std::exception& exp) {
    GALOIS_LOG_DEBUG("arrow exception: {}", exp.what());
    return tsuba::ErrorCode::ArrowError;
  }
}
