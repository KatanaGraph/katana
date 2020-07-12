#include "tsuba/RDG.h"

#include <memory>
#include <fstream>
#include <unordered_set>

#include <boost/filesystem.hpp>
#include <arrow/filesystem/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/schema.h>
#include <parquet/arrow/writer.h>
#include <parquet/file_reader.h>

#include "galois/Logging.h"
#include "galois/Result.h"
#include "galois/FileSystem.h"
#include "tsuba/Errors.h"
#include "tsuba/tsuba.h"
#include "tsuba/file.h"
#include "tsuba/FileFrame.h"

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

namespace fs = boost::filesystem;
using json   = nlohmann::json;

namespace tsuba {

struct RDGMeta {
  uint32_t version;
  uint32_t num_hosts;

  // NOLINTNEXTLINE needed non-const ref for nlohmann compat
  friend void to_json(json& j, const RDGMeta& meta) {
    j = json{{"magic", kRDGMagicNo},
             {"version", meta.version},
             {"num_hosts", meta.num_hosts}};
  }
  // NOLINTNEXTLINE needed non-const ref for nlohmann compat
  friend void from_json(const json& j, RDGMeta& meta) {
    uint32_t magic;
    j.at("magic").get_to(magic);
    j.at("version").get_to(meta.version);
    j.at("num_hosts").get_to(meta.num_hosts);
    if (magic != kRDGMagicNo) {
      // nlohmann::json reports errors using exceptions
      throw std::runtime_error("RDG Magic number mismatch");
    }
  }
};

struct RDGHandle {
  // Property paths are relative to metadata path
  std::string metadata_dir;
  std::string rdg_path;
  std::string partition_path;
  uint32_t flags;
  RDGMeta rdg;

  /// Perform some checks on assumed invariants
  galois::Result<void> Validate() const {
    if (metadata_dir.empty()) {
      GALOIS_LOG_DEBUG("metadata_dir: \"{}\" is empty", metadata_dir);
      return ErrorCode::InvalidArgument;
    }
    return galois::ResultSuccess();
  }
};

} // namespace tsuba

namespace {

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
      return tsuba::ErrorCode::InvalidArgument;
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
      return tsuba::ErrorCode::InvalidArgument;
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
LoadTable(const std::string& expected_name, const fs::path& file_path) {
  // TODO(ddn): parallelize reading
  // TODO(ddn): use custom NUMA allocator

  auto fv = std::make_shared<tsuba::FileView>(tsuba::FileView());
  if (auto res = fv->Bind(file_path.string()); !res) {
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

  std::shared_ptr<arrow::Schema> schema = out->schema();
  if (schema->num_fields() != 1) {
    return tsuba::ErrorCode::InvalidArgument;
  }

  if (schema->field(0)->name() != expected_name) {
    return tsuba::ErrorCode::InvalidArgument;
  }

  return out;
}

template <typename AddFn>
galois::Result<void>
AddTables(const fs::path& dir,
          const std::vector<tsuba::PropertyMetadata>& properties,
          AddFn add_fn) {
  for (const tsuba::PropertyMetadata& properties : properties) {
    fs::path p_path{dir};
    p_path.append(properties.path);

    auto load_result = LoadTable(properties.name, p_path);
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

galois::Result<std::vector<tsuba::PropertyMetadata>>
WriteTable(const arrow::Table& table,
           const std::vector<tsuba::PropertyMetadata>& properties,
           const fs::path& dir) {

  const auto& schema = table.schema();

  std::vector<std::string> next_paths;
  for (size_t i = 0, n = properties.size(); i < n; ++i) {
    if (!properties[i].path.empty()) {
      continue;
    }

    fs::path next_path = galois::NewPath(dir, schema->field(i)->name());

    // Metadata paths should relative to dir
    next_paths.emplace_back(next_path.filename().string());

    std::shared_ptr<arrow::Table> column = arrow::Table::Make(
        arrow::schema({schema->field(i)}), {table.column(i)});

    auto ff = std::make_shared<tsuba::FileFrame>();
    if (auto res = ff->Init(); !res) {
      return res.error();
    }

    auto write_result =
        parquet::arrow::WriteTable(*column, arrow::default_memory_pool(), ff,
                                   std::numeric_limits<int64_t>::max());

    if (!write_result.ok()) {
      GALOIS_LOG_DEBUG("arrow error: {}", write_result);
      return tsuba::ErrorCode::ArrowError;
    }

    ff->Bind(next_path.string());
    if (auto res = ff->Persist(); !res) {
      return res.error();
    }
  }

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

  if (names.size() != properties.size()) {
    return tsuba::ErrorCode::InvalidArgument;
  }

  return properties;
}

/// ReadMetadata reads metadata from a Parquet file and returns the extracted
/// property graph specific fields as well as the unparsed fields.
///
/// The order of metadata fields is significant, and repeated metadata fields
/// are used to encode lists of values.
galois::Result<tsuba::RDG>
ReadMetadata(std::shared_ptr<tsuba::RDGHandle> handle) {
  auto fv = std::make_shared<tsuba::FileView>();
  if (auto res = fv->Bind(handle->partition_path); !res) {
    return res.error();
  }

  if (fv->size() == 0) {
    return tsuba::RDG();
  }

  std::shared_ptr<parquet::FileMetaData> md = parquet::ReadMetaData(fv);
  const std::shared_ptr<const arrow::KeyValueMetadata>& kv_metadata =
      md->key_value_metadata();

  if (!kv_metadata) {
    return tsuba::ErrorCode::InvalidArgument;
  }

  std::vector<std::string> node_values;
  std::vector<std::string> edge_values;
  std::vector<std::pair<std::string, std::string>> other_metadata;
  std::string topology_path;
  for (int64_t i = 0, n = kv_metadata->size(); i < n; ++i) {
    const std::string& k = kv_metadata->key(i);
    const std::string& v = kv_metadata->value(i);

    if (k == node_property_path_key || k == node_property_name_key) {
      node_values.emplace_back(v);
    } else if (k == edge_property_path_key || k == edge_property_name_key) {
      edge_values.emplace_back(v);
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

  tsuba::RDG rdg;

  rdg.node_properties = std::move(node_properties_result.value());
  rdg.edge_properties = std::move(edge_properties_result.value());
  rdg.other_metadata  = std::move(other_metadata);
  rdg.topology_path   = std::move(topology_path);

  return tsuba::RDG(std::move(rdg));
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

  for (const auto& v : rdg.other_metadata) {
    keys.emplace_back(v.first);
    values.emplace_back(v.second);
  }

  return std::make_pair(keys, values);
}

std::string HostPartitionName(const tsuba::RDGHandle& handle,
                              uint32_t version) {
  return fmt::format("{}_{}_{}", handle.rdg_path, tsuba::Comm()->ID, version);
}

galois::Result<void> WriteMetadata(const tsuba::RDGHandle& handle,
                                   const tsuba::RDG& rdg,
                                   const arrow::Schema& schema) {

  std::shared_ptr<parquet::SchemaDescriptor> schema_descriptor;
  auto to_result = parquet::arrow::ToParquetSchema(
      &schema, *parquet::default_writer_properties(), &schema_descriptor);
  if (!to_result.ok()) {
    GALOIS_LOG_DEBUG("arrow error: {}", to_result);
    return tsuba::ErrorCode::ArrowError;
  }

  auto kvs = MakeMetadata(rdg);
  auto parquet_kvs =
      std::make_shared<parquet::KeyValueMetadata>(kvs.first, kvs.second);
  auto builder = parquet::FileMetaDataBuilder::Make(
      schema_descriptor.get(), parquet::default_writer_properties(),
      parquet_kvs);
  auto md = builder->Finish();

  auto ff = std::make_shared<tsuba::FileFrame>();
  if (auto res = ff->Init(); !res) {
    return res.error();
  }

  auto write_result = parquet::arrow::WriteMetaDataFile(*md, ff.get());
  if (!write_result.ok()) {
    return tsuba::ErrorCode::InvalidArgument;
  }

  ff->Bind(HostPartitionName(handle, handle.rdg.version + 1));
  if (auto res = ff->Persist(); !res) {
    return res.error();
  }
  return galois::ResultSuccess();
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

  std::string s = json(tsuba::RDGMeta{
                           .version   = 0,
                           .num_hosts = tsuba::Comm()->Num,
                       })
                      .dump();
  if (auto res = tsuba::FileStore(
          name, reinterpret_cast<const uint8_t*>(s.data()), s.size());
      !res) {
    GALOIS_LOG_ERROR("failed to store RDG file");
    return res.error();
  }
  return galois::ResultSuccess();
}

galois::Result<void> DoLoad(std::shared_ptr<tsuba::RDGHandle> handle,
                            tsuba::RDG* rdg) {
  fs::path dir     = handle->metadata_dir;
  auto node_result = AddTables(
      dir, rdg->node_properties,
      [&rdg, handle](const std::shared_ptr<arrow::Table>& table) {
        return AddProperties(table, &rdg->node_table, &rdg->node_properties,
                             /* new_properties = */ false);
      });
  if (!node_result) {
    return node_result.error();
  }

  auto edge_result = AddTables(
      dir, rdg->edge_properties,
      [&rdg, handle](const std::shared_ptr<arrow::Table>& table) {
        return AddProperties(table, &rdg->edge_table, &rdg->edge_properties,
                             /* new_properties = */ false);
      });
  if (!edge_result) {
    return edge_result.error();
  }

  fs::path t_path{dir};
  t_path.append(rdg->topology_path);

  if (auto res = rdg->topology_file_storage.Bind(t_path.string()); !res) {
    return res.error();
  }
  rdg->rdg_dir = handle->metadata_dir;
  return galois::ResultSuccess();
}

galois::Result<void> CommitRDG(std::shared_ptr<tsuba::RDGHandle> handle) {
  galois::CommBackend* comm = tsuba::Comm();
  tsuba::RDGMeta new_meta{.version   = handle->rdg.version + 1,
                          .num_hosts = comm->Num};
  comm->Barrier();
  if (comm->ID == 0) {
    std::string s = json(new_meta).dump();
    if (auto res = tsuba::FileStore(handle->rdg_path,
                                    reinterpret_cast<const uint8_t*>(s.data()),
                                    s.size());
        !res) {
      GALOIS_LOG_ERROR("failed to store RDG file");
      return res.error();
    }
  }
  comm->Barrier();
  handle->rdg = new_meta;
  return galois::ResultSuccess();
}

galois::Result<void> DoStore(std::shared_ptr<tsuba::RDGHandle> handle,
                             tsuba::RDG* rdg) {
  if (rdg->topology_path.empty()) {
    // No topology file; create one
    fs::path t_path = galois::NewPath(handle->metadata_dir, "topology");

    if (auto res = tsuba::FileStore(t_path.string(),
                                    rdg->topology_file_storage.ptr<uint8_t>(),
                                    rdg->topology_file_storage.size());
        !res) {
      return res.error();
    }
    rdg->topology_path = t_path.filename().string();
  }

  auto node_write_result =
      WriteTable(*rdg->node_table, rdg->node_properties, handle->metadata_dir);
  if (!node_write_result) {
    GALOIS_LOG_DEBUG("WriteTable for node_properties failed");
    return node_write_result.error();
  }
  rdg->node_properties = std::move(node_write_result.value());

  auto edge_write_result =
      WriteTable(*rdg->edge_table, rdg->edge_properties, handle->metadata_dir);
  if (!edge_write_result) {
    GALOIS_LOG_DEBUG("WriteTable for edge_properties failed");
    return edge_write_result.error();
  }
  rdg->edge_properties = std::move(edge_write_result.value());

  auto merge_result = arrow::SchemaBuilder::Merge(
      {rdg->node_table->schema(), rdg->edge_table->schema()},
      arrow::SchemaBuilder::ConflictPolicy::CONFLICT_APPEND);
  if (!merge_result.ok()) {
    GALOIS_LOG_DEBUG("arrow error: {}", merge_result.status());
    return tsuba::ErrorCode::ArrowError;
  }

  if (auto validate_result = handle->Validate(); !validate_result) {
    GALOIS_LOG_DEBUG("RDGHandle failed to validate");
    return validate_result.error();
  }

  if (auto validate_result = rdg->Validate(); !validate_result) {
    GALOIS_LOG_DEBUG("rdg failed to validate");
    return validate_result.error();
  }

  std::shared_ptr<arrow::Schema> merged = merge_result.ValueOrDie();

  if (auto write_result = WriteMetadata(*handle, *rdg, *merged);
      !write_result) {
    GALOIS_LOG_DEBUG("metadata write error");
    return write_result.error();
  }
  if (auto res = CommitRDG(handle); !res) {
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
  if (auto res = fv.Bind(path); !res) {
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
  return topology_file_storage.Equals(other.topology_file_storage) &&
         node_table->Equals(*other.node_table, true) &&
         edge_table->Equals(*other.edge_table, true);
}

galois::Result<std::shared_ptr<tsuba::RDGHandle>>
tsuba::Open(const std::string& rdg_name, uint32_t flags) {
  auto ret = std::make_shared<tsuba::RDGHandle>();

  fs::path m_path{rdg_name};
  ret->metadata_dir = m_path.parent_path().string();
  ret->rdg_path     = rdg_name;
  ret->flags        = flags;

  auto rdg_res = ParseRDGFile(rdg_name);
  if (!rdg_res) {
    if (rdg_res.error() == tsuba::ErrorCode::InvalidArgument) {
      GALOIS_WARN_ONCE("Deprecated behavior: treating invalid RDG file like a "
                       "partition file");
      ret->partition_path = rdg_name;
      ret->rdg            = {.version = 0, .num_hosts = tsuba::Comm()->Num};
      return ret;
    }
    return rdg_res.error();
  }

  ret->rdg = rdg_res.value();
  if (ret->rdg.num_hosts != tsuba::Comm()->Num) {
    return ErrorCode::InvalidArgument;
  }
  ret->partition_path = HostPartitionName(*ret, ret->rdg.version);
  return ret;
}

galois::Result<void> tsuba::Close(std::shared_ptr<RDGHandle> handle) {
  // nothing to do yet
  (void)handle;
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

galois::Result<void> tsuba::Rename(std::shared_ptr<RDGHandle> handle,
                                   const std::string& name, int flags) {
  (void)handle;
  (void)name;
  (void)flags;
  return tsuba::ErrorCode::NotImplemented;
}

galois::Result<tsuba::RDG> tsuba::Load(std::shared_ptr<RDGHandle> handle) {
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
tsuba::Load(std::shared_ptr<RDGHandle> handle,
            const std::vector<std::string>& node_properties,
            const std::vector<std::string>& edge_properties) {

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

galois::Result<void> tsuba::Store(std::shared_ptr<RDGHandle> handle, RDG* rdg) {
  if (handle->metadata_dir != rdg->rdg_dir) {
    if (auto res = UnbindFromStorage(rdg); !res) {
      return res.error();
    }
  }
  return DoStore(handle, rdg);
}

galois::Result<void> tsuba::Store(std::shared_ptr<RDGHandle> handle, RDG* rdg,
                                  std::shared_ptr<FileFrame> ff) {
  // TODO(ddn): property paths will be dangling if metadata directory changes
  // but absolute paths in metadata make moving property files around hard.
  if (handle->metadata_dir != rdg->rdg_dir) {
    if (auto res = UnbindFromStorage(rdg); !res) {
      return res.error();
    }
  }

  fs::path t_path = galois::NewPath(handle->metadata_dir, "topology");
  ff->Bind(t_path.string());
  if (auto res = ff->Persist(); !res) {
    return res.error();
  }
  rdg->topology_path = t_path.filename().string();

  return DoStore(handle, rdg);
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
