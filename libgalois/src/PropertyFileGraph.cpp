#include "galois/graphs/PropertyFileGraph.h"

#include <arrow/buffer.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/filesystem/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/schema.h>
#include <parquet/arrow/writer.h>
#include <parquet/file_reader.h>

#include <filesystem>
#include <parquet/metadata.h>
#include <parquet/properties.h>
#include <sys/mman.h>
#include <system_error>
#include <unordered_set>

#include <algorithm>
#include <array>
#include <cstring>
#include <functional>
#include <limits>
#include <random>
#include <string>

#include "galois/ErrorCode.h"
#include "galois/Logging.h"
#include "tsuba/tsuba.h"

static const char* topology_path_key      = "kg.v1.topology.path";
static const char* node_property_path_key = "kg.v1.node_property.path";
static const char* node_property_name_key = "kg.v1.node_property.name";
static const char* edge_property_path_key = "kg.v1.edge_property.path";
static const char* edge_property_name_key = "kg.v1.edge_property.name";

namespace fs = std::filesystem;

// TODO(ddn): add appropriate error codes, package Arrow errors better

struct PropertyMetadata {
  std::string name;
  std::string path;
};

struct galois::graphs::MetadataImpl {
  std::string topology_path;
  std::vector<PropertyMetadata> node_properties;
  std::vector<PropertyMetadata> edge_properties;
  std::vector<std::pair<std::string, std::string>> other_metadata;

  // Property paths are relative to metadata path
  fs::path metadata_dir;

  outcome::std_result<void> Validate() const {
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

    return outcome::success();
  }
};

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

outcome::std_result<std::shared_ptr<arrow::Table>>
LoadTable(const std::string& expected_name, const fs::path& file_path) {
  // TODO(ddn): parallelize reading
  // TODO(ddn): use custom NUMA allocator

  arrow::fs::LocalFileSystem fs;

  auto open_result = fs.OpenInputFile(file_path);
  if (!open_result.ok()) {
    GALOIS_LOG_DEBUG("arrow error: {}", open_result.status());
    return galois::ErrorCode::ArrowError;
  }

  std::shared_ptr<arrow::io::RandomAccessFile> f = open_result.ValueOrDie();
  std::unique_ptr<parquet::arrow::FileReader> reader;

  auto open_file_result =
      parquet::arrow::OpenFile(f, arrow::default_memory_pool(), &reader);
  if (!open_file_result.ok()) {
    GALOIS_LOG_DEBUG("arrow error: {}", open_file_result);
    return galois::ErrorCode::ArrowError;
  }

  std::shared_ptr<arrow::Table> out;
  auto read_result = reader->ReadTable(&out);
  if (!read_result.ok()) {
    GALOIS_LOG_DEBUG("arrow error: {}", read_result);
    return galois::ErrorCode::ArrowError;
  }

  std::shared_ptr<arrow::Schema> schema = out->schema();
  if (schema->num_fields() != 1) {
    return galois::ErrorCode::InvalidArgument;
  }

  if (schema->field(0)->name() != expected_name) {
    return galois::ErrorCode::InvalidArgument;
  }

  return out;
}

template <typename AddFn>
outcome::std_result<void>
AddTables(const fs::path& dir, const std::vector<PropertyMetadata>& properties,
          AddFn add_fn) {
  for (const PropertyMetadata& properties : properties) {
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

  return outcome::success();
}

outcome::std_result<std::vector<PropertyMetadata>>
WriteTable(const arrow::Table& table,
           const std::vector<PropertyMetadata>& properties,
           const fs::path& dir) {

  const auto& schema = table.schema();

  std::vector<std::string> next_paths;
  for (size_t i = 0, n = properties.size(); i < n; ++i) {
    if (!properties[i].path.empty()) {
      continue;
    }

    fs::path next_path = NewPath(dir, schema->field(i)->name());

    // Metadata paths should relative to dir
    next_paths.emplace_back(next_path.filename());

    std::shared_ptr<arrow::Table> column = arrow::Table::Make(
        arrow::schema({schema->field(i)}), {table.column(i)});

    auto create_result = arrow::io::BufferOutputStream::Create();
    if (!create_result.ok()) {
      GALOIS_LOG_DEBUG("arrow error: {}", create_result.status());
      return galois::ErrorCode::ArrowError;
    }

    std::shared_ptr<arrow::io::BufferOutputStream> out =
        create_result.ValueOrDie();

    auto write_result =
        parquet::arrow::WriteTable(*column, arrow::default_memory_pool(), out,
                                   std::numeric_limits<int64_t>::max());

    if (!write_result.ok()) {
      GALOIS_LOG_DEBUG("arrow error: {}", write_result);
      return galois::ErrorCode::ArrowError;
    }

    auto finish_result = out->Finish();
    if (!finish_result.ok()) {
      GALOIS_LOG_DEBUG("arrow error: {}", finish_result.status());
      return galois::ErrorCode::ArrowError;
    }

    std::shared_ptr<arrow::Buffer> buf = finish_result.ValueOrDie();

    int err = tsuba::Store(next_path, buf->data(), buf->size());
    if (err) {
      return galois::ErrorCode::InvalidArgument;
    }
  }

  if (next_paths.empty()) {
    return properties;
  }

  std::vector<PropertyMetadata> next_properties = properties;
  auto it                                       = next_paths.begin();
  for (auto& v : next_properties) {
    if (v.path.empty()) {
      v.path = *it++;
    }
  }

  return next_properties;
}

outcome::std_result<void>
AddProperties(const std::shared_ptr<arrow::Table>& table,
              std::shared_ptr<arrow::Table>* to_update,
              std::vector<PropertyMetadata>* properties) {
  std::shared_ptr<arrow::Table> current = *to_update;

  if (current->num_columns() > 0 && current->num_rows() != table->num_rows()) {
    return galois::ErrorCode::InvalidArgument;
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
        return galois::ErrorCode::ArrowError;
      }

      next = result.ValueOrDie();
    }
  }

  if (!next->schema()->HasDistinctFieldNames()) {
    return galois::ErrorCode::InvalidArgument;
  }

  const auto& schema = next->schema();
  for (int i = current->num_columns(), end = next->num_columns(); i < end;
       ++i) {
    properties->emplace_back(PropertyMetadata{
        .name = schema->field(i)->name(),
        .path = "",
    });
  }

  *to_update = next;

  return outcome::success();
}

constexpr uint64_t GetGraphSize(uint64_t num_nodes, uint64_t num_edges) {
  /// version, sizeof_edge_data, num_nodes, num_edges
  constexpr int mandatory_fields = 4;

  return (mandatory_fields + num_nodes) * sizeof(uint64_t) +
         (num_edges * sizeof(uint32_t));
}

/// MapTopology takes a file buffer of a topology file and extracts the
/// topology files.
///
/// Format of a topology file (borrowed from the original FileGraph.cpp:
///
///   uint64_t version: 1
///   uint64_t sizeof_edge_data: size of edge data element
///   uint64_t num_nodes: number of nodes
///   uint64_t num_edges: number of edges
///   uint64_t[num_nodes] out_indices: start and end of the edges for a node
///   uint32_t[num_edges] out_dests: destinations (node indexes) of each edge
///   uint32_t padding if num_edges is odd
///   void*[num_edges] edge_data: edge data
///
/// Since property graphs store their edge data separately, we will consider
/// any topology file with non-zero sizeof_edge_data invalid.
outcome::std_result<galois::graphs::GraphTopology>
MapTopology(const galois::FileView& file_view) {
  const auto* data = file_view.ptr<uint64_t>();
  if (file_view.size() < 4) {
    return galois::ErrorCode::InvalidArgument;
  }

  if (data[0] != 1) {
    return galois::ErrorCode::InvalidArgument;
  }

  if (data[1] != 0) {
    return galois::ErrorCode::InvalidArgument;
  }

  uint64_t num_nodes = data[2];
  uint64_t num_edges = data[3];

  uint64_t expected_size = GetGraphSize(num_nodes, num_edges);

  if (file_view.size() < expected_size) {
    return galois::ErrorCode::InvalidArgument;
  }

  const uint64_t* out_indices = &data[4];

  const auto* out_dests =
      reinterpret_cast<const uint32_t*>(out_indices + num_nodes);

  auto indices_buffer = std::make_shared<arrow::Buffer>(
      reinterpret_cast<const uint8_t*>(out_indices), num_nodes);

  auto dests_buffer = std::make_shared<arrow::Buffer>(
      reinterpret_cast<const uint8_t*>(out_dests), num_edges);

  return galois::graphs::GraphTopology{
      .out_indices = std::make_shared<arrow::UInt64Array>(
          indices_buffer->size(), indices_buffer),
      .out_dests = std::make_shared<arrow::UInt32Array>(dests_buffer->size(),
                                                        dests_buffer),
  };
}

outcome::std_result<void>
LoadTopology(const fs::path& topology_path,
             galois::graphs::GraphTopology* topology,
             galois::FileView* topology_file_storage) {

  galois::FileView f;
  int err = f.Bind(topology_path);
  if (err) {
    return galois::ErrorCode::InvalidArgument;
  }

  auto map_result = MapTopology(f);
  if (!map_result) {
    return map_result.error();
  }

  *topology              = std::move(map_result.value());
  *topology_file_storage = std::move(f);

  return outcome::success();
}

class MMapper {
public:
  MMapper(size_t s) {
    void* p = mmap(nullptr, s, PROT_READ | PROT_WRITE,
                   MAP_ANONYMOUS | MAP_POPULATE | MAP_PRIVATE, -1, 0);
    if (p == MAP_FAILED) {
      perror("mmap");
      p = nullptr;
    }

    size = s;
    ptr  = p;
  }

  MMapper(const MMapper&) = delete;
  MMapper&& operator=(const MMapper&) = delete;

  ~MMapper() {
    if (!ptr) {
      return;
    }
    if (munmap(ptr, size)) {
      perror("munmap");
    }
  }

  void* ptr{};
  size_t size{};
};

outcome::std_result<std::string>
WriteTopology(const galois::graphs::GraphTopology& topology,
              const fs::path& dir) {
  // TODO(ddn): avoid buffer copy
  uint64_t num_nodes = topology.num_nodes();
  uint64_t num_edges = topology.num_edges();
  MMapper mmapper(GetGraphSize(num_nodes, num_edges));
  if (!mmapper.ptr) {
    return galois::ErrorCode::InvalidArgument;
  }

  uint64_t* data = reinterpret_cast<uint64_t*>(mmapper.ptr);
  data[0]        = 1;
  data[1]        = 0;
  data[2]        = num_nodes;
  data[3]        = num_edges;

  if (num_nodes) {
    uint64_t* out   = &data[4];
    const auto* raw = topology.out_indices->raw_values();
    static_assert(std::is_same_v<std::decay_t<decltype(*raw)>,
                                 std::decay_t<decltype(*out)>>);
    std::copy_n(raw, num_nodes, out);
  }

  if (num_edges) {
    uint32_t* out   = reinterpret_cast<uint32_t*>(&data[4 + num_nodes]);
    const auto* raw = topology.out_dests->raw_values();
    static_assert(std::is_same_v<std::decay_t<decltype(*raw)>,
                                 std::decay_t<decltype(*out)>>);
    std::copy_n(raw, num_edges, out);
  }

  fs::path t_path = NewPath(dir, "topology");
  int err = tsuba::Store(t_path, reinterpret_cast<uint8_t*>(mmapper.ptr),
                         mmapper.size);
  if (err) {
    return galois::ErrorCode::InvalidArgument;
  }

  return t_path.filename();
}

outcome::std_result<std::vector<PropertyMetadata>>
MakeProperties(std::vector<std::string>&& values) {
  std::vector v = std::move(values);

  if ((v.size() % 2) != 0) {
    return galois::ErrorCode::InvalidArgument;
  }

  std::vector<PropertyMetadata> properties;
  std::unordered_set<std::string> names;
  properties.reserve(v.size() / 2);

  for (size_t i = 0, n = v.size(); i < n; i += 2) {
    const auto& name = v[i];
    const auto& path = v[i + 1];

    names.insert(name);

    properties.emplace_back(PropertyMetadata{
        .name = name,
        .path = path,
    });
  }

  if (names.size() != properties.size()) {
    return galois::ErrorCode::InvalidArgument;
  }

  return properties;
}

/// ReadMetadata reads metadata from a Parquet file and returns the extracted
/// property graph specific fields as well as the unparsed fields.
///
/// The order of metadata fields is significant, and repeated metadata fields
/// are used to encode lists of values.
outcome::std_result<galois::graphs::MetadataImpl>
ReadMetadata(const std::string& metadata_path) {
  arrow::fs::LocalFileSystem fs;

  auto open_result = fs.OpenInputFile(metadata_path);
  if (!open_result.ok()) {
    GALOIS_LOG_DEBUG("arrow error: {}", open_result.status());
    return galois::ErrorCode::ArrowError;
  }

  std::shared_ptr<arrow::io::RandomAccessFile> f = open_result.ValueOrDie();

  std::shared_ptr<parquet::FileMetaData> md = parquet::ReadMetaData(f);
  const std::shared_ptr<const arrow::KeyValueMetadata>& kv_metadata =
      md->key_value_metadata();

  if (!kv_metadata) {
    return galois::ErrorCode::InvalidArgument;
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
        return galois::ErrorCode::InvalidArgument;
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

  fs::path m_path{metadata_path};

  auto ret = galois::graphs::MetadataImpl{
      .topology_path   = topology_path,
      .node_properties = std::move(node_properties_result.value()),
      .edge_properties = std::move(edge_properties_result.value()),
      .other_metadata  = other_metadata,
      .metadata_dir    = m_path.parent_path(),
  };

  auto validate_result = ret.Validate();
  if (!validate_result) {
    return validate_result.error();
  }

  return ret;
}

std::pair<std::vector<std::string>, std::vector<std::string>>
MakeMetadata(const galois::graphs::MetadataImpl& metadata) {
  std::vector<std::string> keys;
  std::vector<std::string> values;

  keys.emplace_back(topology_path_key);
  values.emplace_back(metadata.topology_path);

  for (const auto& v : metadata.node_properties) {
    keys.emplace_back(node_property_name_key);
    values.emplace_back(v.name);
    keys.emplace_back(node_property_path_key);
    values.emplace_back(v.path);
  }

  for (const auto& v : metadata.node_properties) {
    keys.emplace_back(edge_property_name_key);
    values.emplace_back(v.name);
    keys.emplace_back(edge_property_path_key);
    values.emplace_back(v.path);
  }

  for (const auto& v : metadata.other_metadata) {
    keys.emplace_back(v.first);
    values.emplace_back(v.second);
  }

  return std::make_pair(keys, values);
}

outcome::std_result<void>
WriteMetadata(const galois::graphs::MetadataImpl& metadata,
              const arrow::Schema& schema, const fs::path& path) {

  std::shared_ptr<parquet::SchemaDescriptor> schema_descriptor;
  auto to_result = parquet::arrow::ToParquetSchema(
      &schema, *parquet::default_writer_properties(), &schema_descriptor);
  if (!to_result.ok()) {
    GALOIS_LOG_DEBUG("arrow error: {}", to_result);
    return galois::ErrorCode::ArrowError;
  }

  auto kvs = MakeMetadata(metadata);
  auto parquet_kvs =
      std::make_shared<parquet::KeyValueMetadata>(kvs.first, kvs.second);
  auto builder = parquet::FileMetaDataBuilder::Make(
      schema_descriptor.get(), parquet::default_writer_properties(),
      parquet_kvs);
  auto md = builder->Finish();

  auto create_result = arrow::io::BufferOutputStream::Create();
  if (!create_result.ok()) {
    GALOIS_LOG_DEBUG("arrow error: {}", create_result.status());
    return galois::ErrorCode::ArrowError;
  }

  std::shared_ptr<arrow::io::BufferOutputStream> out =
      create_result.ValueOrDie();

  auto write_result = parquet::arrow::WriteMetaDataFile(*md, out.get());
  if (!write_result.ok()) {
    return galois::ErrorCode::InvalidArgument;
  }

  auto finish_result = out->Finish();
  if (!finish_result.ok()) {
    GALOIS_LOG_DEBUG("arrow error: {}", finish_result.status());
    return galois::ErrorCode::ArrowError;
  }

  std::shared_ptr<arrow::Buffer> buf = finish_result.ValueOrDie();

  int err = tsuba::Store(path, buf->data(), buf->size());
  if (err) {
    return galois::ErrorCode::InvalidArgument;
  }
  return outcome::success();
}

} // namespace

galois::graphs::PropertyFileGraph::PropertyFileGraph() {
  std::vector<std::shared_ptr<arrow::Array>> empty;

  node_table_ = arrow::Table::Make(arrow::schema({}), empty, 0);
  edge_table_ = arrow::Table::Make(arrow::schema({}), empty, 0);

  metadata_ = std::make_unique<MetadataImpl>();
}

galois::graphs::PropertyFileGraph::~PropertyFileGraph() = default;

outcome::std_result<std::shared_ptr<galois::graphs::PropertyFileGraph>>
galois::graphs::PropertyFileGraph::Make(MetadataImpl&& metadata) {
  auto g         = std::make_shared<PropertyFileGraph>();
  MetadataImpl m = std::move(metadata);
  fs::path dir   = m.metadata_dir;

  auto node_result = AddTables(
      dir, m.node_properties, [&g](const std::shared_ptr<arrow::Table>& table) {
        return g->AddNodeProperties(table);
      });
  if (!node_result) {
    return node_result.error();
  }

  auto edge_result = AddTables(
      dir, m.edge_properties, [&g](const std::shared_ptr<arrow::Table>& table) {
        return g->AddEdgeProperties(table);
      });
  if (!edge_result) {
    return edge_result.error();
  }

  fs::path t_path{dir};
  t_path.append(m.topology_path);
  auto load_result =
      LoadTopology(t_path, &g->topology_, &g->topology_file_storage_);
  if (!load_result) {
    return load_result.error();
  }

  g->metadata_ = std::make_unique<MetadataImpl>(std::move(m));

  return g;
}

outcome::std_result<std::shared_ptr<galois::graphs::PropertyFileGraph>>
galois::graphs::PropertyFileGraph::Make(const std::string& metadata_path) {
  auto metadata_result = ReadMetadata(metadata_path);
  if (!metadata_result) {
    return metadata_result.error();
  }

  return Make(std::move(metadata_result.value()));
}

outcome::std_result<std::shared_ptr<galois::graphs::PropertyFileGraph>>
galois::graphs::PropertyFileGraph::Make(
    const std::string& metadata_path,
    const std::vector<std::string>& node_properties,
    const std::vector<std::string>& edge_properties) {

  auto metadata_result = ReadMetadata(metadata_path);
  if (!metadata_result) {
    return metadata_result.error();
  }

  MetadataImpl metadata = std::move(metadata_result.value());

  std::unordered_map<std::string, std::string> node_paths;
  for (const auto& m : metadata.node_properties) {
    node_paths.insert({m.name, m.path});
  }

  std::vector<PropertyMetadata> next_node_properties;
  for (const auto& s : node_properties) {
    auto it = node_paths.find(s);
    if (it == node_paths.end()) {
      return ErrorCode::InvalidArgument;
    }

    next_node_properties.emplace_back(PropertyMetadata{
        .name = it->first,
        .path = it->second,
    });
  }

  std::unordered_map<std::string, std::string> edge_paths;
  for (const auto& m : metadata.edge_properties) {
    edge_paths.insert({m.name, m.path});
  }

  std::vector<PropertyMetadata> next_edge_properties;
  for (const auto& s : edge_properties) {
    auto it = edge_paths.find(s);
    if (it == edge_paths.end()) {
      return ErrorCode::InvalidArgument;
    }

    next_edge_properties.emplace_back(PropertyMetadata{
        .name = it->first,
        .path = it->second,
    });
  }

  metadata.node_properties = next_node_properties;
  metadata.edge_properties = next_edge_properties;

  return Make(std::move(metadata));
}

outcome::std_result<void>
galois::graphs::PropertyFileGraph::Write(const std::string& metadata_path) {
  // TODO(ddn): property paths will be dangling if metadata directory changes
  // but absolute paths in metadata make moving property files around hard.

  std::filesystem::path m_path{metadata_path};
  fs::path dir = m_path.parent_path();

  metadata_->metadata_dir = dir;

  if (metadata_->topology_path.empty()) {
    auto result = WriteTopology(topology_, dir);
    if (!result) {
      return result.error();
    }
    metadata_->topology_path = std::move(result.value());
  }

  auto node_write_result = WriteTable(*node_table_, metadata_->node_properties,
                                      metadata_->metadata_dir);
  if (!node_write_result) {
    return node_write_result.error();
  }
  metadata_->node_properties = std::move(node_write_result.value());

  auto edge_write_result = WriteTable(*edge_table_, metadata_->edge_properties,
                                      metadata_->metadata_dir);
  if (!edge_write_result) {
    return edge_write_result.error();
  }
  metadata_->edge_properties = std::move(edge_write_result.value());

  auto merge_result = arrow::SchemaBuilder::Merge(
      {node_table_->schema(), edge_table_->schema()},
      arrow::SchemaBuilder::ConflictPolicy::CONFLICT_APPEND);
  if (!merge_result.ok()) {
    GALOIS_LOG_DEBUG("arrow error: {}", merge_result.status());
    return ErrorCode::ArrowError;
  }

  auto validate_result = metadata_->Validate();
  if (!validate_result) {
    return validate_result.error();
  }

  std::shared_ptr<arrow::Schema> merged = merge_result.ValueOrDie();

  auto metadata_write_result =
      WriteMetadata(*metadata_, *merged, metadata_path);
  if (!metadata_write_result) {
    return metadata_write_result.error();
  }

  return outcome::success();
}

outcome::std_result<void> galois::graphs::PropertyFileGraph::AddNodeProperties(
    const std::shared_ptr<arrow::Table>& table) {
  if (topology_.out_indices &&
      topology_.out_indices->length() != table->num_rows()) {
    return ErrorCode::InvalidArgument;
  }

  return AddProperties(table, &node_table_, &metadata_->node_properties);
}

outcome::std_result<void> galois::graphs::PropertyFileGraph::AddEdgeProperties(
    const std::shared_ptr<arrow::Table>& table) {
  if (topology_.out_dests &&
      topology_.out_dests->length() != table->num_rows()) {
    return ErrorCode::InvalidArgument;
  }

  return AddProperties(table, &edge_table_, &metadata_->edge_properties);
}

outcome::std_result<void>
galois::graphs::PropertyFileGraph::RemoveNodeProperty(int i) {
  auto result = node_table_->RemoveColumn(i);
  if (!result.ok()) {
    GALOIS_LOG_DEBUG("arrow error: {}", result.status());
    return ErrorCode::ArrowError;
  }

  node_table_ = result.ValueOrDie();

  auto& p = metadata_->node_properties;
  assert(static_cast<unsigned>(i) < p.size());
  p.erase(p.begin() + i);

  return outcome::success();
}

outcome::std_result<void>
galois::graphs::PropertyFileGraph::RemoveEdgeProperty(int i) {
  auto result = edge_table_->RemoveColumn(i);
  if (!result.ok()) {
    GALOIS_LOG_DEBUG("arrow error: {}", result.status());
    return ErrorCode::ArrowError;
  }

  edge_table_ = result.ValueOrDie();

  auto& p = metadata_->edge_properties;
  assert(static_cast<unsigned>(i) < p.size());
  p.erase(p.begin() + i);

  return outcome::success();
}

outcome::std_result<void> galois::graphs::PropertyFileGraph::SetTopology(
    const galois::graphs::GraphTopology& topology) {
  topology_                = topology;
  metadata_->topology_path = "";

  return outcome::success();
}
