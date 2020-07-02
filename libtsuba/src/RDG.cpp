#include "tsuba/RDG.h"

#include <memory>
#include <fstream>
#include <random>
#include <unordered_set>

#include <boost/filesystem.hpp>
#include <arrow/filesystem/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/schema.h>
#include <parquet/arrow/writer.h>
#include <parquet/file_reader.h>

#include "galois/Logging.h"
#include "galois/Result.h"
#include "tsuba/Errors.h"
#include "tsuba/tsuba.h"
#include "tsuba/FileFrame.h"

static const char* topology_path_key      = "kg.v1.topology.path";
static const char* node_property_path_key = "kg.v1.node_property.path";
static const char* node_property_name_key = "kg.v1.node_property.name";
static const char* edge_property_path_key = "kg.v1.edge_property.path";
static const char* edge_property_name_key = "kg.v1.edge_property.name";

namespace fs = boost::filesystem;

namespace tsuba {

struct PropertyMetadata {
  std::string name;
  std::string path;
};

struct RDGHandle {
  std::string topology_path;
  std::vector<tsuba::PropertyMetadata> node_properties;
  std::vector<tsuba::PropertyMetadata> edge_properties;
  std::vector<std::pair<std::string, std::string>> other_metadata;

  // Property paths are relative to metadata path
  std::string metadata_dir;
  std::string path;

  galois::Result<void> Validate() const {
    if (topology_path.empty() || metadata_dir.empty()) {
      return ErrorCode::InvalidArgument;
    }
    if (topology_path.find('/') != std::string::npos) {
      return ErrorCode::InvalidArgument;
    }
    for (const auto& md : node_properties) {
      if (md.path.find('/') != std::string::npos) {
        return ErrorCode::InvalidArgument;
      }
    }
    for (const auto& md : edge_properties) {
      if (md.path.find('/') != std::string::npos) {
        return ErrorCode::InvalidArgument;
      }
    }

    return galois::ResultSuccess();
  }
};

} // namespace tsuba

namespace {

// TODO(ddn): Move this to libsupport

// https://stackoverflow.com/questions/440133
template <typename T = std::mt19937>
auto random_generator() -> T {
  auto constexpr seed_bits = sizeof(typename T::result_type) * T::state_size;
  auto constexpr seed_len =
      seed_bits / std::numeric_limits<std::seed_seq::result_type>::digits;
  auto seed = std::array<std::seed_seq::result_type, seed_len>{};
  auto dev  = std::random_device{};
  std::generate_n(begin(seed), seed_len, std::ref(dev));
  auto seed_seq = std::seed_seq(begin(seed), end(seed));
  return T{seed_seq};
}

std::string generate_random_alphanumeric_string(std::size_t len) {
  static constexpr auto chars = "0123456789"
                                "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                "abcdefghijklmnopqrstuvwxyz";
  thread_local auto rng = random_generator<>();
  auto dist   = std::uniform_int_distribution{{}, std::strlen(chars) - 1};
  auto result = std::string(len, '\0');
  std::generate_n(begin(result), len, [&]() { return chars[dist(rng)]; });
  return result;
}

/// NewPath returns a new path in a directory with the given prefix. It works
/// by appending a random suffix. The generated paths are may not be unique due
/// to the varying atomicity guarantees of future storage backends.
fs::path NewPath(const fs::path& dir, const std::string& prefix) {
  std::string name = prefix;
  name += "-";
  name += generate_random_alphanumeric_string(12);
  fs::path p{dir};
  return p.append(name);
}

galois::Result<void>
PrunePropsTo(std::shared_ptr<tsuba::RDGHandle> handle,
             const std::vector<std::string>& node_properties,
             const std::vector<std::string>& edge_properties) {

  std::unordered_map<std::string, std::string> node_paths;
  for (const auto& m : handle->node_properties) {
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
  for (const auto& m : handle->edge_properties) {
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

  handle->node_properties = next_node_properties;
  handle->edge_properties = next_edge_properties;

  return galois::ResultSuccess();
}

galois::Result<std::shared_ptr<arrow::Table>>
LoadTable(const std::string& expected_name, const fs::path& file_path) {
  // TODO(ddn): parallelize reading
  // TODO(ddn): use custom NUMA allocator

  auto fv = std::make_shared<tsuba::FileView>(tsuba::FileView());
  int err = fv->Bind(file_path.string());
  if (err) {
    return tsuba::ErrorCode::InvalidArgument;
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

    fs::path next_path = NewPath(dir, schema->field(i)->name());

    // Metadata paths should relative to dir
    next_paths.emplace_back(next_path.filename().string());

    std::shared_ptr<arrow::Table> column = arrow::Table::Make(
        arrow::schema({schema->field(i)}), {table.column(i)});

    auto ff = std::make_shared<tsuba::FileFrame>();
    int err = ff->Init();
    if (err) {
      return tsuba::ErrorCode::OutOfMemory;
    }

    auto write_result =
        parquet::arrow::WriteTable(*column, arrow::default_memory_pool(), ff,
                                   std::numeric_limits<int64_t>::max());

    if (!write_result.ok()) {
      GALOIS_LOG_DEBUG("arrow error: {}", write_result);
      return tsuba::ErrorCode::ArrowError;
    }

    ff->Bind(next_path.string());
    err = ff->Persist();
    if (err) {
      return tsuba::ErrorCode::InvalidArgument;
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
galois::Result<std::shared_ptr<tsuba::RDGHandle>>
ReadMetadata(const std::string& metadata_path) {
  auto fv = std::make_shared<tsuba::FileView>(tsuba::FileView());
  int err = fv->Bind(metadata_path);
  if (err) {
    return tsuba::ErrorCode::InvalidArgument;
  }

  fs::path m_path{metadata_path};

  // If we're opening a file for a new graph it will be empty
  if (fv->size() == 0) {
    auto ret          = std::make_shared<tsuba::RDGHandle>();
    ret->metadata_dir = m_path.parent_path().string();
    ret->path         = metadata_path;
    return ret;
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

  auto ret = std::make_shared<tsuba::RDGHandle>(tsuba::RDGHandle{
      .topology_path   = topology_path,
      .node_properties = std::move(node_properties_result.value()),
      .edge_properties = std::move(edge_properties_result.value()),
      .other_metadata  = other_metadata,
      .metadata_dir    = m_path.parent_path().string(),
      .path            = metadata_path,
  });

  auto validate_result = ret->Validate();
  if (!validate_result) {
    return validate_result.error();
  }

  return ret;
}

std::pair<std::vector<std::string>, std::vector<std::string>>
MakeMetadata(const tsuba::RDGHandle& handle) {
  std::vector<std::string> keys;
  std::vector<std::string> values;

  keys.emplace_back(topology_path_key);
  values.emplace_back(handle.topology_path);

  for (const auto& v : handle.node_properties) {
    keys.emplace_back(node_property_name_key);
    values.emplace_back(v.name);
    keys.emplace_back(node_property_path_key);
    values.emplace_back(v.path);
  }

  for (const auto& v : handle.edge_properties) {
    keys.emplace_back(edge_property_name_key);
    values.emplace_back(v.name);
    keys.emplace_back(edge_property_path_key);
    values.emplace_back(v.path);
  }

  for (const auto& v : handle.other_metadata) {
    keys.emplace_back(v.first);
    values.emplace_back(v.second);
  }

  return std::make_pair(keys, values);
}

galois::Result<void> WriteMetadata(const tsuba::RDGHandle& handle,
                                   const arrow::Schema& schema) {

  std::shared_ptr<parquet::SchemaDescriptor> schema_descriptor;
  auto to_result = parquet::arrow::ToParquetSchema(
      &schema, *parquet::default_writer_properties(), &schema_descriptor);
  if (!to_result.ok()) {
    GALOIS_LOG_DEBUG("arrow error: {}", to_result);
    return tsuba::ErrorCode::ArrowError;
  }

  auto kvs = MakeMetadata(handle);
  auto parquet_kvs =
      std::make_shared<parquet::KeyValueMetadata>(kvs.first, kvs.second);
  auto builder = parquet::FileMetaDataBuilder::Make(
      schema_descriptor.get(), parquet::default_writer_properties(),
      parquet_kvs);
  auto md = builder->Finish();

  auto ff = std::make_shared<tsuba::FileFrame>();
  int err = ff->Init();
  if (err) {
    return tsuba::ErrorCode::OutOfMemory;
  }

  auto write_result = parquet::arrow::WriteMetaDataFile(*md, ff.get());
  if (!write_result.ok()) {
    return tsuba::ErrorCode::InvalidArgument;
  }

  ff->Bind(handle.path);
  err = ff->Persist();
  if (err) {
    return tsuba::ErrorCode::InvalidArgument;
  }
  return galois::ResultSuccess();
}

// TOCTTOU since it's the local file system but will change when we manage
// our own namespace, create the file here so that at least another call
// will fail
galois::Result<void> CreateFile(std::string name, bool overwrite = false) {
  fs::path m_path{name};
  if (overwrite && fs::exists(m_path)) {
    return tsuba::ErrorCode::Exists;
  }
  fs::path dir = m_path.parent_path();
  if (boost::system::error_code err; fs::create_directories(dir, err)) {
    return err;
  }
  std::ofstream output(m_path.string());
  return galois::ResultSuccess();
}

} // namespace

namespace tsuba {

RDG::RDG() : handle(std::make_shared<tsuba::RDGHandle>()) {
  std::vector<std::shared_ptr<arrow::Array>> empty;
  node_table = arrow::Table::Make(arrow::schema({}), empty, 0);
  edge_table = arrow::Table::Make(arrow::schema({}), empty, 0);
}

galois::Result<std::shared_ptr<tsuba::RDGHandle>>
Open(const std::string& rdg_name, [[maybe_unused]] int flags) {
  auto metadata_result = ReadMetadata(rdg_name);
  if (!metadata_result) {
    return metadata_result.error();
  }
  return metadata_result;
}

galois::Result<void> Create(const std::string& name) {
  if (auto good = CreateFile(name); !good) {
    return good.error();
  }
  return galois::ResultSuccess();
}

galois::Result<void> Rename(std::shared_ptr<RDGHandle> handle,
                            const std::string& name, int flags) {
  bool overwrite     = flags & kOverwrite;
  auto create_result = CreateFile(name, overwrite);
  if (!create_result) {
    return create_result.error();
  }

  fs::path m_path{name};
  // Property paths are relative to metadata path
  handle->metadata_dir  = m_path.parent_path().string();
  handle->path          = name;
  handle->topology_path = "";
  return galois::ResultSuccess();
}

galois::Result<RDG> Load(std::shared_ptr<tsuba::RDGHandle> handle) {
  RDG g;

  fs::path dir     = handle->metadata_dir;
  auto node_result = AddTables(
      dir, handle->node_properties,
      [&g, handle](const std::shared_ptr<arrow::Table>& table) {
        return AddProperties(table, &g.node_table, &handle->node_properties,
                             /* new_properties = */ false);
      });
  if (!node_result) {
    return node_result.error();
  }

  auto edge_result = AddTables(
      dir, handle->edge_properties,
      [&g, handle](const std::shared_ptr<arrow::Table>& table) {
        return AddProperties(table, &g.edge_table, &handle->edge_properties,
                             /* new_properties = */ false);
      });
  if (!edge_result) {
    return edge_result.error();
  }

  fs::path t_path{dir};
  t_path.append(handle->topology_path);

  if (int err = g.topology_file_storage.Bind(t_path.string()); err) {
    return ErrorCode::InvalidArgument;
  }

  g.handle = handle;

  return RDG(std::move(g));
}

galois::Result<RDG> Load(std::shared_ptr<tsuba::RDGHandle> handle,
                         const std::vector<std::string>& node_properties,
                         const std::vector<std::string>& edge_properties) {

  if (auto res = PrunePropsTo(handle, node_properties, edge_properties); !res) {
    return res.error();
  }
  return Load(handle);
}

galois::Result<void> Store(const RDG& rdg) {
  auto node_write_result = WriteTable(
      *rdg.node_table, rdg.handle->node_properties, rdg.handle->metadata_dir);
  if (!node_write_result) {
    return node_write_result.error();
  }
  rdg.handle->node_properties = std::move(node_write_result.value());

  auto edge_write_result = WriteTable(
      *rdg.edge_table, rdg.handle->edge_properties, rdg.handle->metadata_dir);
  if (!edge_write_result) {
    return edge_write_result.error();
  }
  rdg.handle->edge_properties = std::move(edge_write_result.value());

  auto merge_result = arrow::SchemaBuilder::Merge(
      {rdg.node_table->schema(), rdg.edge_table->schema()},
      arrow::SchemaBuilder::ConflictPolicy::CONFLICT_APPEND);
  if (!merge_result.ok()) {
    GALOIS_LOG_DEBUG("arrow error: {}", merge_result.status());
    return ErrorCode::ArrowError;
  }

  auto validate_result = rdg.handle->Validate();
  if (!validate_result) {
    return validate_result.error();
  }

  std::shared_ptr<arrow::Schema> merged = merge_result.ValueOrDie();

  auto metadata_write_result = WriteMetadata(*rdg.handle, *merged);
  if (!metadata_write_result) {
    return metadata_write_result.error();
  }

  return galois::ResultSuccess();
}

galois::Result<void> Store(const RDG& rdg,
                           std::shared_ptr<tsuba::FileFrame> ff) {
  // TODO(ddn): property paths will be dangling if metadata directory changes
  // but absolute paths in metadata make moving property files around hard.

  fs::path t_path = NewPath(rdg.handle->metadata_dir, "topology");
  ff->Bind(t_path.string());
  int err = ff->Persist();
  if (err) {
    return ErrorCode::InvalidArgument;
  }
  rdg.handle->topology_path = t_path.filename().string();

  return Store(rdg);
}

galois::Result<void>
AddNodeProperties(RDG* rdg, const std::shared_ptr<arrow::Table>& table) {
  return AddProperties(table, &rdg->node_table, &rdg->handle->node_properties,
                       /* new_properties = */ true);
}

galois::Result<void>
AddEdgeProperties(RDG* rdg, const std::shared_ptr<arrow::Table>& table) {
  return AddProperties(table, &rdg->edge_table, &rdg->handle->edge_properties,
                       /* new_properties = */ true);
}

galois::Result<void> DropNodeProperty(RDG* rdg, int i) {
  auto result = rdg->node_table->RemoveColumn(i);
  if (!result.ok()) {
    GALOIS_LOG_DEBUG("arrow error: {}", result.status());
    return ErrorCode::ArrowError;
  }

  rdg->node_table = result.ValueOrDie();

  auto& p = rdg->handle->node_properties;
  assert(static_cast<unsigned>(i) < p.size());
  p.erase(p.begin() + i);

  return galois::ResultSuccess();
}

galois::Result<void> DropEdgeProperty(RDG* rdg, int i) {
  auto result = rdg->edge_table->RemoveColumn(i);
  if (!result.ok()) {
    GALOIS_LOG_DEBUG("arrow error: {}", result.status());
    return ErrorCode::ArrowError;
  }

  rdg->edge_table = result.ValueOrDie();

  auto& p = rdg->handle->edge_properties;
  assert(static_cast<unsigned>(i) < p.size());
  p.erase(p.begin() + i);

  return galois::ResultSuccess();
}

} // namespace tsuba
