#include "graph-properties-convert.h"

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <fstream>
#include <iostream>
#include <limits>
#include <map>
#include <optional>
#include <random>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <boost/algorithm/string.hpp>
#include <boost/foreach.hpp>
#include <boost/lexical_cast.hpp>
#include <libxml/xmlreader.h>
#include <arrow/api.h>
#include <arrow/buffer.h>
#include <arrow/io/api.h>
#include <arrow/array/builder_binary.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/json.hpp>

#include "galois/ErrorCode.h"
#include "galois/Galois.h"
#include "galois/Logging.h"
#include "galois/graphs/PropertyFileGraph.h"
#include "galois/ParallelSTL.h"
#include "galois/SharedMemSys.h"
#include "galois/Threads.h"

namespace {

using galois::ImportDataType;

using ArrayBuilders   = std::vector<std::shared_ptr<arrow::ArrayBuilder>>;
using StringBuilders  = std::vector<std::shared_ptr<arrow::StringBuilder>>;
using BooleanBuilders = std::vector<std::shared_ptr<arrow::BooleanBuilder>>;
using ChunkedArrays   = std::vector<std::shared_ptr<arrow::ChunkedArray>>;
using ArrowArrays     = std::vector<std::shared_ptr<arrow::Array>>;
using StringArrays    = std::vector<std::shared_ptr<arrow::StringArray>>;
using BooleanArrays   = std::vector<std::shared_ptr<arrow::BooleanArray>>;
using ArrowFields     = std::vector<std::shared_ptr<arrow::Field>>;
using NullMaps =
    std::pair<std::unordered_map<int, std::shared_ptr<arrow::Array>>,
              std::unordered_map<int, std::shared_ptr<arrow::Array>>>;

struct PropertyKey {
  std::string id;
  bool for_node;
  bool for_edge;
  std::string name;
  ImportDataType type;
  bool is_list;

  PropertyKey(const std::string& id_, bool for_node_, bool for_edge_,
              const std::string& name_, ImportDataType type_, bool is_list_)
      : id(id_), for_node(for_node_), for_edge(for_edge_), name(name_),
        type(type_), is_list(is_list_) {}
  PropertyKey(const std::string& id, ImportDataType type, bool is_list)
      : PropertyKey(id, false, false, id, type, is_list) {}
};

struct PropertiesState {
  std::unordered_map<std::string, size_t> keys;
  ArrowFields schema;
  ArrayBuilders builders;
  std::vector<ArrowArrays> chunks;
};

struct LabelsState {
  std::unordered_map<std::string, size_t> keys;
  ArrowFields schema;
  BooleanBuilders builders;
  std::vector<ArrowArrays> chunks;
};

struct TopologyState {
  // maps node IDs to node indexes
  std::unordered_map<std::string, size_t> node_indexes;
  // node's start of edge lists
  std::vector<uint64_t> out_indices;
  // edge list of destinations
  std::vector<uint32_t> out_dests;
  // list of sources of edges
  std::vector<uint32_t> sources;
  // list of destinations of edges
  std::vector<uint32_t> destinations;

  // for schema mapping
  std::unordered_set<std::string> edge_ids;
  // for data ingestion that does not guarantee nodes are imported first
  std::vector<std::string> sources_intermediate;
  std::vector<std::string> destinations_intermediate;
};

struct GraphState {
  PropertiesState node_properties;
  PropertiesState edge_properties;
  LabelsState node_labels;
  LabelsState edge_types;
  TopologyState topology_builder;
  size_t nodes;
  size_t edges;
};

struct WriterProperties {
  NullMaps null_arrays;
  std::shared_ptr<arrow::Array> false_array;
  const size_t chunk_size;
};

struct CollectionFields {
  std::map<std::string, PropertyKey> property_fields;
  std::set<std::string> embedded_nodes;
  std::set<std::string> embedded_relations;
};

/************************************/
/* Basic Building Utility Functions */
/************************************/

template <typename T>
std::shared_ptr<arrow::Array> BuildArray(std::shared_ptr<T> builder) {
  std::shared_ptr<arrow::Array> array;
  auto st = builder->Finish(&array);
  if (!st.ok()) {
    GALOIS_LOG_FATAL("Error building arrow array: {}", st.ToString());
  }
  return array;
}

ChunkedArrays BuildChunks(std::vector<ArrowArrays>* chunks) {
  ChunkedArrays chunked_arrays;
  chunked_arrays.resize(chunks->size());
  for (size_t n = 0; n < chunks->size(); n++) {
    chunked_arrays[n] = std::make_shared<arrow::ChunkedArray>(chunks->at(n));
  }
  return chunked_arrays;
}

std::shared_ptr<arrow::Table> BuildTable(std::vector<ArrowArrays>* chunks,
                                         ArrowFields* schema_vector) {
  ChunkedArrays columns = BuildChunks(chunks);

  auto schema = std::make_shared<arrow::Schema>(*schema_vector);
  return arrow::Table::Make(schema, columns);
}

/*************************************************************/
/* Utility functions for retrieving null arrays from the map */
/*************************************************************/

template <typename T>
std::shared_ptr<arrow::Array> FindNullArray(std::shared_ptr<T> builder,
                                            WriterProperties* properties) {
  auto type = builder->type()->id();
  std::shared_ptr<arrow::Array> null_array;
  if (type != arrow::Type::LIST) {
    null_array = properties->null_arrays.first.find(type)->second;
  } else {
    auto list_builder = std::static_pointer_cast<arrow::ListBuilder>(builder);
    null_array        = properties->null_arrays.second
                     .find(list_builder->value_builder()->type()->id())
                     ->second;
  }
  return null_array;
}

std::shared_ptr<arrow::Array> FindNullArray(std::shared_ptr<arrow::Array> array,
                                            WriterProperties* properties) {
  auto type = array->type()->id();
  std::shared_ptr<arrow::Array> null_array;
  if (type != arrow::Type::LIST) {
    null_array = properties->null_arrays.first.find(type)->second;
  } else {
    auto list_array = std::static_pointer_cast<arrow::ListArray>(array);
    null_array =
        properties->null_arrays.second.find(list_array->values()->type()->id())
            ->second;
  }
  return null_array;
}

/******************************************************/
/* Functions for finding basic statistics on datasets */
/******************************************************/

void WriteNullStats(const std::vector<ArrowArrays>& table,
                    WriterProperties* properties, size_t total) {
  if (table.size() == 0) {
    std::cout << "This table has no entries" << std::endl;
    return;
  }
  size_t null_constants  = 0;
  size_t non_null_values = 0;

  for (auto col : table) {
    auto null_array = FindNullArray(col[0], properties);
    for (auto chunk : col) {
      if (chunk == null_array) {
        null_constants++;
      } else {
        for (int64_t i = 0; i < chunk->length(); i++) {
          if (!chunk->IsNull(i)) {
            non_null_values++;
          }
        }
      }
    }
  }
  std::cout << "Total non-null Values in Table: " << non_null_values
            << std::endl;
  std::cout << "Total Values in Table: " << total * table.size() << std::endl;
  std::cout << "Value Ratio: "
            << ((double)non_null_values) / (total * table.size()) << std::endl;
  std::cout << "Total Null Chunks in table " << null_constants << std::endl;
  std::cout << "Total Chunks in Table: " << table[0].size() * table.size()
            << std::endl;
  std::cout << "Constant Ratio: "
            << ((double)null_constants) / (table[0].size() * table.size())
            << std::endl;
  std::cout << std::endl;
}

void WriteFalseStats(const std::vector<ArrowArrays>& table,
                     WriterProperties* properties, size_t total) {
  if (table.size() == 0) {
    std::cout << "This table has no entries" << std::endl;
    return;
  }
  size_t false_constants = 0;
  size_t true_values     = 0;

  for (auto col : table) {
    for (auto chunk : col) {
      if (chunk == properties->false_array) {
        false_constants++;
      } else {
        auto array = std::static_pointer_cast<arrow::BooleanArray>(chunk);
        for (int64_t i = 0; i < chunk->length(); i++) {
          if (array->Value(i)) {
            true_values++;
          }
        }
      }
    }
  }
  std::cout << "Total true Values in Table: " << true_values << std::endl;
  std::cout << "Total Values in Table: " << total * table.size() << std::endl;
  std::cout << "True Ratio: " << ((double)true_values) / (total * table.size())
            << std::endl;
  std::cout << "Total False Chunks in table " << false_constants << std::endl;
  std::cout << "Total Chunks in Table: " << table[0].size() * table.size()
            << std::endl;
  std::cout << "Constant Ratio: "
            << ((double)false_constants) / (table[0].size() * table.size())
            << std::endl;
  std::cout << std::endl;
}

/**************************************/
/* Functions for adding arrow columns */
/**************************************/

// Special case for building boolean builders where the empty value is false,
// not null
size_t AddFalseBuilder(const std::pair<std::string, std::string>& label,
                       LabelsState* labels) {
  // add entry to map
  auto index = labels->keys.size();
  labels->keys.insert(std::pair<std::string, size_t>(label.first, index));

  // add column to schema, builders, and chunks
  labels->schema.push_back(arrow::field(label.second, arrow::boolean()));
  labels->builders.push_back(std::make_shared<arrow::BooleanBuilder>());
  labels->chunks.push_back(ArrowArrays{});

  return index;
}

// Special case for adding properties not forward declared as strings since we
// do not know their type
size_t AddStringBuilder(const std::string& column,
                        PropertiesState* properties) {
  // add entry to map
  auto index = properties->keys.size();
  properties->keys.insert(std::pair<std::string, size_t>(column, index));

  // add column to schema, builders, and chunks
  properties->schema.push_back(arrow::field(column, arrow::utf8()));
  properties->builders.push_back(std::make_shared<arrow::StringBuilder>());
  properties->chunks.push_back(ArrowArrays{});

  return index;
}

// Case for adding properties for which we know their type
size_t AddBuilder(PropertiesState* properties, PropertyKey key) {
  auto* pool = arrow::default_memory_pool();
  if (!key.is_list) {
    switch (key.type) {
    case ImportDataType::kString: {
      properties->schema.push_back(arrow::field(key.name, arrow::utf8()));
      properties->builders.push_back(std::make_shared<arrow::StringBuilder>());
      break;
    }
    case ImportDataType::kInt64: {
      properties->schema.push_back(arrow::field(key.name, arrow::int64()));
      properties->builders.push_back(std::make_shared<arrow::Int64Builder>());
      break;
    }
    case ImportDataType::kInt32: {
      properties->schema.push_back(arrow::field(key.name, arrow::int32()));
      properties->builders.push_back(std::make_shared<arrow::Int32Builder>());
      break;
    }
    case ImportDataType::kDouble: {
      properties->schema.push_back(arrow::field(key.name, arrow::float64()));
      properties->builders.push_back(std::make_shared<arrow::DoubleBuilder>());
      break;
    }
    case ImportDataType::kFloat: {
      properties->schema.push_back(arrow::field(key.name, arrow::float32()));
      properties->builders.push_back(std::make_shared<arrow::FloatBuilder>());
      break;
    }
    case ImportDataType::kBoolean: {
      properties->schema.push_back(arrow::field(key.name, arrow::boolean()));
      properties->builders.push_back(std::make_shared<arrow::BooleanBuilder>());
      break;
    }
    case ImportDataType::kTimestampMilli: {
      auto field = arrow::field(
          key.name, arrow::timestamp(arrow::TimeUnit::MILLI, "UTC"));
      properties->schema.push_back(field);
      properties->builders.push_back(
          std::make_shared<arrow::TimestampBuilder>(field->type(), pool));
      break;
    }
    case ImportDataType::kStruct: {
      properties->schema.push_back(arrow::field(key.name, arrow::uint8()));
      properties->builders.push_back(std::make_shared<arrow::UInt8Builder>());
      break;
    }
    default:
      // for now handle uncaught types as strings
      GALOIS_LOG_WARN("treating unknown type {} as string", key.type);
      properties->schema.push_back(arrow::field(key.name, arrow::utf8()));
      properties->builders.push_back(std::make_shared<arrow::StringBuilder>());
      break;
    }
  } else {
    switch (key.type) {
    case ImportDataType::kString: {
      properties->schema.push_back(
          arrow::field(key.name, arrow::list(arrow::utf8())));
      properties->builders.push_back(std::make_shared<arrow::ListBuilder>(
          pool, std::make_shared<arrow::StringBuilder>()));
      break;
    }
    case ImportDataType::kInt64: {
      properties->schema.push_back(
          arrow::field(key.name, arrow::list(arrow::int64())));
      properties->builders.push_back(std::make_shared<arrow::ListBuilder>(
          pool, std::make_shared<arrow::Int64Builder>()));
      break;
    }
    case ImportDataType::kInt32: {
      properties->schema.push_back(
          arrow::field(key.name, arrow::list(arrow::int32())));
      properties->builders.push_back(std::make_shared<arrow::ListBuilder>(
          pool, std::make_shared<arrow::Int32Builder>()));
      break;
    }
    case ImportDataType::kDouble: {
      properties->schema.push_back(
          arrow::field(key.name, arrow::list(arrow::float64())));
      properties->builders.push_back(std::make_shared<arrow::ListBuilder>(
          pool, std::make_shared<arrow::DoubleBuilder>()));
      break;
    }
    case ImportDataType::kFloat: {
      properties->schema.push_back(
          arrow::field(key.name, arrow::list(arrow::float32())));
      properties->builders.push_back(std::make_shared<arrow::ListBuilder>(
          pool, std::make_shared<arrow::FloatBuilder>()));
      break;
    }
    case ImportDataType::kBoolean: {
      properties->schema.push_back(
          arrow::field(key.name, arrow::list(arrow::boolean())));
      properties->builders.push_back(std::make_shared<arrow::ListBuilder>(
          pool, std::make_shared<arrow::BooleanBuilder>()));
      break;
    }
    case ImportDataType::kTimestampMilli: {
      auto field = arrow::field(
          key.name, arrow::timestamp(arrow::TimeUnit::MILLI, "UTC"));

      properties->schema.push_back(arrow::field(key.name, arrow::list(field)));
      properties->builders.push_back(std::make_shared<arrow::ListBuilder>(
          pool,
          std::make_shared<arrow::TimestampBuilder>(field->type(), pool)));
      break;
    }
    default:
      // for now handle uncaught types as strings
      GALOIS_LOG_WARN("treating unknown array type {} as a string array",
                      key.type);
      properties->schema.push_back(
          arrow::field(key.name, arrow::list(arrow::utf8())));
      properties->builders.push_back(std::make_shared<arrow::ListBuilder>(
          pool, std::make_shared<arrow::StringBuilder>()));
      break;
    }
  }
  auto index = properties->keys.size();
  properties->chunks.push_back(ArrowArrays{});
  properties->keys.insert(std::pair<std::string, size_t>(key.id, index));
  return index;
}

/******************************/
/* Functions for parsing data */
/******************************/

std::vector<std::string> ParseStringList(std::string raw_list) {
  std::vector<std::string> list;

  if (raw_list.size() >= 2 && raw_list.front() == '[' &&
      raw_list.back() == ']') {
    raw_list.erase(0, 1);
    raw_list.erase(raw_list.length() - 1, 1);
  } else {
    GALOIS_LOG_ERROR(
        "The provided list was not formatted like neo4j, returning string");
    list.push_back(raw_list);
    return list;
  }

  const char* char_list = raw_list.c_str();
  // parse the list
  for (size_t i = 0; i < raw_list.size();) {
    bool first_quote_found  = false;
    bool found_end_of_elem  = false;
    size_t start_of_elem    = i;
    int consecutive_slashes = 0;

    // parse the field
    for (; !found_end_of_elem && i < raw_list.size(); i++) {
      // if second quote not escaped then end of element reached
      if (char_list[i] == '\"') {
        if (consecutive_slashes % 2 == 0) {
          if (!first_quote_found) {
            first_quote_found = true;
            start_of_elem     = i + 1;
          } else if (first_quote_found) {
            found_end_of_elem = true;
          }
        }
        consecutive_slashes = 0;
      } else if (char_list[i] == '\\') {
        consecutive_slashes++;
      } else {
        consecutive_slashes = 0;
      }
    }
    size_t end_of_elem = i - 1;
    size_t elem_length = end_of_elem - start_of_elem;

    if (end_of_elem <= start_of_elem) {
      list.push_back("");
    } else {

      std::string elem_rough(&char_list[start_of_elem], elem_length);
      std::string elem("");
      elem.reserve(elem_rough.size());
      size_t curr_index = 0;
      size_t next_slash = elem_rough.find_first_of('\\');

      while (next_slash != std::string::npos) {
        elem.append(elem_rough.begin() + curr_index,
                    elem_rough.begin() + next_slash);

        switch (elem_rough[next_slash + 1]) {
        case 'n':
          elem.append("\n");
          break;
        case '\\':
          elem.append("\\");
          break;
        case 'r':
          elem.append("\r");
          break;
        case '0':
          elem.append("\0");
          break;
        case 'b':
          elem.append("\b");
          break;
        case '\'':
          elem.append("\'");
          break;
        case '\"':
          elem.append("\"");
          break;
        case 't':
          elem.append("\t");
          break;
        case 'f':
          elem.append("\f");
          break;
        case 'v':
          elem.append("\v");
          break;
        case '\xFF':
          elem.append("\xFF");
          break;
        default:
          GALOIS_LOG_WARN("Unhandled escape character: {}",
                          elem_rough[next_slash + 1]);
        }

        curr_index = next_slash + 2;
        next_slash = elem_rough.find_first_of('\\', curr_index);
      }
      elem.append(elem_rough.begin() + curr_index, elem_rough.end());

      list.push_back(elem);
    }
  }

  return list;
}

template <typename T>
std::vector<T> ParseNumberList(std::string raw_list) {
  std::vector<T> list;

  if (raw_list.front() == '[' && raw_list.back() == ']') {
    raw_list.erase(0, 1);
    raw_list.erase(raw_list.length() - 1, 1);
  } else {
    GALOIS_LOG_ERROR("The provided list was not formatted like neo4j, "
                     "returning empty vector");
    return list;
  }
  std::vector<std::string> elems;
  boost::split(elems, raw_list, boost::is_any_of(","));

  for (std::string s : elems) {
    list.push_back(boost::lexical_cast<T>(s));
  }
  return list;
}

std::vector<bool> ParseBooleanList(std::string raw_list) {
  std::vector<bool> list;

  if (raw_list.front() == '[' && raw_list.back() == ']') {
    raw_list.erase(0, 1);
    raw_list.erase(raw_list.length() - 1, 1);
  } else {
    GALOIS_LOG_ERROR("The provided list was not formatted like neo4j, "
                     "returning empty vector");
    return list;
  }
  std::vector<std::string> elems;
  boost::split(elems, raw_list, boost::is_any_of(","));

  for (std::string s : elems) {
    bool bool_val = s[0] == 't' || s[0] == 'T';
    list.push_back(bool_val);
  }
  return list;
}

template <typename T, typename W>
std::optional<T> RetrievePrimitive(const W& elt) {
  switch (elt.type()) {
  case bsoncxx::type::k_int64:
    return static_cast<T>(elt.get_int64().value);
  case bsoncxx::type::k_int32:
    return static_cast<T>(elt.get_int32().value);
  case bsoncxx::type::k_double:
    return static_cast<T>(elt.get_double().value);
  case bsoncxx::type::k_bool:
    return static_cast<T>(elt.get_bool().value);
  case bsoncxx::type::k_utf8:
    try {
      return boost::lexical_cast<T>(elt.get_utf8().value.data());
    } catch (const boost::bad_lexical_cast&) {
      // std::cout << "A primitive value was not retrieved: " <<
      // elt.get_utf8().value.data() << std::endl;
      return std::nullopt;
    }
  default:
    // std::cout << "A primitive value was not retrieved\n";
    return std::nullopt;
  }
}

template <typename T>
std::optional<std::string> RetrieveString(const T& elt) {
  switch (elt.type()) {
  case bsoncxx::type::k_int64:
    return boost::lexical_cast<std::string>(elt.get_int64().value);
  case bsoncxx::type::k_int32:
    return boost::lexical_cast<std::string>(elt.get_int32().value);
  case bsoncxx::type::k_double:
    return boost::lexical_cast<std::string>(elt.get_double().value);
  case bsoncxx::type::k_bool:
    return boost::lexical_cast<std::string>(elt.get_bool().value);
  case bsoncxx::type::k_utf8:
    return elt.get_utf8().value.data();
  default:
    // std::cout << "A string value was not retrieved\n";
    return std::nullopt;
  }
}

/************************************************/
/* Functions for adding values to arrow builder */
/************************************************/

// Adds nulls to an array being built until its length == total
template <typename T>
void AddNulls(std::shared_ptr<T> builder, ArrowArrays* chunks,
              std::shared_ptr<arrow::Array> null_array,
              WriterProperties* properties, size_t total) {
  auto chunk_size   = properties->chunk_size;
  auto nulls_needed = total - (chunks->size() * chunk_size) - builder->length();
  arrow::Status st;

  // simplest case, no falses needed
  if (nulls_needed == 0) {
    return;
  }

  // case where nulls needed but mid-array
  if (builder->length() != 0) {
    auto nulls_to_add = std::min(chunk_size - builder->length(), nulls_needed);
    st                = builder->AppendNulls(nulls_to_add);
    nulls_needed -= nulls_to_add;

    // if we filled up a chunk, flush it
    if (((size_t)builder->length()) == chunk_size) {
      chunks->push_back(BuildArray(builder));
    } else {
      // if we did not fill up the array we know it must need no more nulls
      return;
    }
  }

  // case where we are at the start of a new array and have null_arrays to add
  for (auto i = chunk_size; i <= nulls_needed; i += chunk_size) {
    chunks->push_back(null_array);
  }
  nulls_needed %= chunk_size;

  // case where we are at the start of a new array and have less than a
  // null_array to add
  st = builder->AppendNulls(nulls_needed);

  if (!st.ok()) {
    GALOIS_LOG_FATAL("Error appending to an arrow array builder: {}",
                     st.ToString());
  }
}

// Adds nulls to an array being built until its length == total
template <typename T>
void AddNulls(std::shared_ptr<T> builder, ArrowArrays* chunks,
              WriterProperties* properties, size_t total) {
  auto nulls_needed =
      total - (chunks->size() * properties->chunk_size) - builder->length();
  if (nulls_needed == 0) {
    return;
  }
  auto null_array = FindNullArray(builder, properties);
  AddNulls(builder, chunks, null_array, properties, total);
}

// Adds falses to an array being built until its length == total
void AddFalses(std::shared_ptr<arrow::BooleanBuilder> builder,
               ArrowArrays* chunks, WriterProperties* properties,
               size_t total) {
  auto chunk_size = properties->chunk_size;
  auto falses_needed =
      total - (chunks->size() * chunk_size) - builder->length();
  arrow::Status st;

  // simplest case, no falses needed
  if (falses_needed == 0) {
    return;
  }

  // case where falses needed but mid-array
  if (builder->length() != 0) {
    auto falses_to_add =
        std::min(chunk_size - builder->length(), falses_needed);
    for (size_t i = 0; i < falses_to_add; i++) {
      st = builder->Append(false);
    }
    falses_needed -= falses_to_add;

    // if we filled up a chunk, flush it
    if (((size_t)builder->length()) == chunk_size) {
      chunks->push_back(BuildArray(builder));
    } else {
      // if we did not fill up the array we know it must need no more falses
      return;
    }
  }

  // case where we are at the start of a new array and have false_arrays to add
  for (auto i = chunk_size; i <= falses_needed; i += chunk_size) {
    chunks->push_back(properties->false_array);
  }
  falses_needed %= chunk_size;

  // case where we are at the start of a new array and have less than a
  // false_array to add
  for (size_t i = 0; i < falses_needed; i++) {
    st = builder->Append(false);
  }

  if (!st.ok()) {
    GALOIS_LOG_FATAL("Error appending to an arrow array builder: {}",
                     st.ToString());
  }
}

// Append an array to a builder
void AppendArray(std::shared_ptr<arrow::ListBuilder> list_builder,
                 const std::string& val) {
  arrow::Status st = arrow::Status::OK();

  switch (list_builder->value_builder()->type()->id()) {
  case arrow::Type::STRING: {
    auto sb = static_cast<arrow::StringBuilder*>(list_builder->value_builder());
    st      = list_builder->Append();
    auto sarray = ParseStringList(val);
    st          = sb->AppendValues(sarray);
    break;
  }
  case arrow::Type::INT64: {
    auto lb = static_cast<arrow::Int64Builder*>(list_builder->value_builder());
    st      = list_builder->Append();
    auto larray = ParseNumberList<int64_t>(val);
    st          = lb->AppendValues(larray);
    break;
  }
  case arrow::Type::INT32: {
    auto ib = static_cast<arrow::Int32Builder*>(list_builder->value_builder());
    st      = list_builder->Append();
    auto iarray = ParseNumberList<int32_t>(val);
    st          = ib->AppendValues(iarray);
    break;
  }
  case arrow::Type::DOUBLE: {
    auto db = static_cast<arrow::DoubleBuilder*>(list_builder->value_builder());
    st      = list_builder->Append();
    auto darray = ParseNumberList<double>(val);
    st          = db->AppendValues(darray);
    break;
  }
  case arrow::Type::FLOAT: {
    auto fb = static_cast<arrow::FloatBuilder*>(list_builder->value_builder());
    st      = list_builder->Append();
    auto farray = ParseNumberList<float>(val);
    st          = fb->AppendValues(farray);
    break;
  }
  case arrow::Type::BOOL: {
    auto bb =
        static_cast<arrow::BooleanBuilder*>(list_builder->value_builder());
    st          = list_builder->Append();
    auto barray = ParseBooleanList(val);
    st          = bb->AppendValues(barray);
    break;
  }
  default: {
    break;
  }
  }
  if (!st.ok()) {
    GALOIS_LOG_FATAL("Error adding value to arrow list array builder: {}",
                     st.ToString());
  }
}

// Append a non-null value to an array
void AppendValue(std::shared_ptr<arrow::ArrayBuilder> array,
                 const std::string& val) {
  arrow::Status st = arrow::Status::OK();

  switch (array->type()->id()) {
  case arrow::Type::STRING: {
    auto sb = std::static_pointer_cast<arrow::StringBuilder>(array);
    st      = sb->Append(val.c_str(), val.length());
    break;
  }
  case arrow::Type::INT64: {
    auto lb = std::static_pointer_cast<arrow::Int64Builder>(array);
    st      = lb->Append(boost::lexical_cast<int64_t>(val));
    break;
  }
  case arrow::Type::INT32: {
    auto ib = std::static_pointer_cast<arrow::Int32Builder>(array);
    st      = ib->Append(boost::lexical_cast<int32_t>(val));
    break;
  }
  case arrow::Type::DOUBLE: {
    auto db = std::static_pointer_cast<arrow::DoubleBuilder>(array);
    st      = db->Append(boost::lexical_cast<double>(val));
    break;
  }
  case arrow::Type::FLOAT: {
    auto fb = std::static_pointer_cast<arrow::FloatBuilder>(array);
    st      = fb->Append(boost::lexical_cast<float>(val));
    break;
  }
  case arrow::Type::BOOL: {
    auto bb       = std::static_pointer_cast<arrow::BooleanBuilder>(array);
    bool bool_val = val[0] == 't' || val[0] == 'T';
    st            = bb->Append(bool_val);
    break;
  }
  case arrow::Type::LIST: {
    auto lb = std::static_pointer_cast<arrow::ListBuilder>(array);
    AppendArray(lb, val);
    break;
  }
  default: {
    break;
  }
  }
  if (!st.ok()) {
    GALOIS_LOG_FATAL(
        "Error adding value to arrow array builder: {}, parquet error: {}", val,
        st.ToString());
  }
}

// Append an array to a builder
void AppendArray(std::shared_ptr<arrow::ListBuilder> list_builder,
                 const bsoncxx::array::view& val) {
  arrow::Status st = arrow::Status::OK();

  switch (list_builder->value_builder()->type()->id()) {
  case arrow::Type::STRING: {
    auto sb = static_cast<arrow::StringBuilder*>(list_builder->value_builder());
    st      = list_builder->Append();
    for (auto elt : val) {
      auto res = RetrieveString(elt);
      if (res) {
        st = sb->Append(res.value());
      }
    }
    break;
  }
  case arrow::Type::INT64: {
    auto lb = static_cast<arrow::Int64Builder*>(list_builder->value_builder());
    st      = list_builder->Append();
    for (auto elt : val) {
      auto res = RetrievePrimitive<int64_t, bsoncxx::array::element>(elt);
      if (res) {
        st = lb->Append(res.value());
      }
    }
    break;
  }
  case arrow::Type::INT32: {
    auto ib = static_cast<arrow::Int32Builder*>(list_builder->value_builder());
    st      = list_builder->Append();
    for (auto elt : val) {
      auto res = RetrievePrimitive<int32_t, bsoncxx::array::element>(elt);
      if (res) {
        st = ib->Append(res.value());
      }
    }
    break;
  }
  case arrow::Type::DOUBLE: {
    auto db = static_cast<arrow::DoubleBuilder*>(list_builder->value_builder());
    st      = list_builder->Append();
    for (auto elt : val) {
      auto res = RetrievePrimitive<double, bsoncxx::array::element>(elt);
      if (res) {
        st = db->Append(res.value());
      }
    }
    break;
  }
  case arrow::Type::FLOAT: {
    auto fb = static_cast<arrow::FloatBuilder*>(list_builder->value_builder());
    st      = list_builder->Append();
    for (auto elt : val) {
      auto res = RetrievePrimitive<float, bsoncxx::array::element>(elt);
      if (res) {
        st = fb->Append(res.value());
      }
    }
    break;
  }
  case arrow::Type::BOOL: {
    auto bb =
        static_cast<arrow::BooleanBuilder*>(list_builder->value_builder());
    st = list_builder->Append();
    for (auto elt : val) {
      auto res = RetrievePrimitive<bool, bsoncxx::array::element>(elt);
      if (res) {
        st = bb->Append(res.value());
      }
    }
    break;
  }
  case arrow::Type::TIMESTAMP: {
    auto tb =
        static_cast<arrow::TimestampBuilder*>(list_builder->value_builder());
    st = list_builder->Append();
    for (auto elt : val) {
      if (elt.type() == bsoncxx::type::k_date) {
        st = tb->Append(elt.get_date().to_int64());
      }
    }
    break;
  }
  default: {
    break;
  }
  }
  if (!st.ok()) {
    GALOIS_LOG_FATAL("Error adding value to arrow list array builder: {}",
                     st.ToString());
  }
}

// Append a non-null value to an array
void AppendValue(std::shared_ptr<arrow::ArrayBuilder> array,
                 const bsoncxx::document::element& val) {
  arrow::Status st = arrow::Status::OK();

  switch (array->type()->id()) {
  case arrow::Type::STRING: {
    auto sb  = std::static_pointer_cast<arrow::StringBuilder>(array);
    auto res = RetrieveString(val);
    if (res) {
      st = sb->Append(res.value());
    }
    break;
  }
  case arrow::Type::INT64: {
    auto lb  = std::static_pointer_cast<arrow::Int64Builder>(array);
    auto res = RetrievePrimitive<int64_t, bsoncxx::document::element>(val);
    if (res) {
      st = lb->Append(res.value());
    }
    break;
  }
  case arrow::Type::INT32: {
    auto ib  = std::static_pointer_cast<arrow::Int32Builder>(array);
    auto res = RetrievePrimitive<int32_t, bsoncxx::document::element>(val);
    if (res) {
      st = ib->Append(res.value());
    }
    break;
  }
  case arrow::Type::DOUBLE: {
    auto db  = std::static_pointer_cast<arrow::DoubleBuilder>(array);
    auto res = RetrievePrimitive<double, bsoncxx::document::element>(val);
    if (res) {
      st = db->Append(res.value());
    }
    break;
  }
  case arrow::Type::FLOAT: {
    auto fb  = std::static_pointer_cast<arrow::FloatBuilder>(array);
    auto res = RetrievePrimitive<float, bsoncxx::document::element>(val);
    if (res) {
      st = fb->Append(res.value());
    }
    break;
  }
  case arrow::Type::BOOL: {
    auto bb  = std::static_pointer_cast<arrow::BooleanBuilder>(array);
    auto res = RetrievePrimitive<bool, bsoncxx::document::element>(val);
    if (res) {
      st = bb->Append(res.value());
    }
    break;
  }
  case arrow::Type::TIMESTAMP: {
    auto tb = std::static_pointer_cast<arrow::TimestampBuilder>(array);
    if (val.type() == bsoncxx::type::k_date) {
      st = tb->Append(val.get_date().to_int64());
    }
    break;
  }
  // for now uint8_t is an alias for a struct
  case arrow::Type::UINT8: {
    auto bb = std::static_pointer_cast<arrow::UInt8Builder>(array);
    if (val.type() == bsoncxx::type::k_document) {
      st = bb->Append(1);
    }
    break;
  }
  case arrow::Type::LIST: {
    auto lb = std::static_pointer_cast<arrow::ListBuilder>(array);
    if (val.type() == bsoncxx::type::k_array) {
      AppendArray(lb, val.get_array().value);
    }
    break;
  }
  default: {
    break;
  }
  }
  if (!st.ok()) {
    GALOIS_LOG_FATAL(
        "Error adding value to arrow array builder: {}, parquet error: {}",
        val.key().data(), st.ToString());
  }
}

// Add nulls until the array is even and then append val so that length = total
// + 1 at the end
void AddValue(const std::string& val,
              std::shared_ptr<arrow::ArrayBuilder> builder, ArrowArrays* chunks,
              WriterProperties* properties, size_t total) {
  AddNulls(builder, chunks, properties, total);
  AppendValue(builder, val);

  // if we filled up a chunk, flush it
  if (((size_t)builder->length()) == properties->chunk_size) {
    chunks->push_back(BuildArray(builder));
  }
}

void AddValue(const bsoncxx::document::element& val,
              std::shared_ptr<arrow::ArrayBuilder> builder, ArrowArrays* chunks,
              WriterProperties* properties, size_t total) {
  AddNulls(builder, chunks, properties, total);
  AppendValue(builder, val);

  // if we filled up a chunk, flush it
  if (((size_t)builder->length()) == properties->chunk_size) {
    chunks->push_back(BuildArray(builder));
  }
}

// Add nulls until the array is even and then append val so that length = total
// + 1 at the end
template <typename T, typename W>
void AddTypedValue(const W& val, std::shared_ptr<T> builder,
                   ArrowArrays* chunks,
                   std::shared_ptr<arrow::Array> null_array,
                   WriterProperties* properties, size_t total) {
  AddNulls(builder, chunks, null_array, properties, total);
  auto st = builder->Append(val);
  if (!st.ok()) {
    GALOIS_LOG_FATAL("Error appending to an arrow array builder: {}",
                     st.ToString());
  }

  // if we filled up a chunk, flush it
  if (((size_t)builder->length()) == properties->chunk_size) {
    chunks->push_back(BuildArray(builder));
  }
}

// Add nulls until the array is even and then append a list so that length =
// total + 1 at the end
template <typename T, typename W>
void AddArray(const std::shared_ptr<arrow::ListArray>& list_vals,
              const std::shared_ptr<W>& vals, size_t index,
              std::shared_ptr<arrow::ListBuilder> list_builder, T* type_builder,
              ArrowArrays* chunks, std::shared_ptr<arrow::Array> null_array,
              WriterProperties* properties, size_t total) {
  AddNulls(list_builder, chunks, null_array, properties, total);

  int32_t start = list_vals->value_offset(index);
  int32_t end   = list_vals->value_offset(index + 1);

  auto st = list_builder->Append();
  for (int32_t s = start; s < end; s++) {
    st = type_builder->Append(vals->Value(s));
    if (!st.ok()) {
      GALOIS_LOG_FATAL("Error appending value to an arrow array builder: {}",
                       st.ToString());
    }
  }
  // if we filled up a chunk, flush it
  if (((size_t)list_builder->length()) == properties->chunk_size) {
    chunks->push_back(BuildArray(list_builder));
  }
}

// Add nulls until the array is even and then append a list so that length =
// total + 1 at the end
void AddArray(const std::shared_ptr<arrow::ListArray>& list_vals,
              const std::shared_ptr<arrow::StringArray>& vals, size_t index,
              std::shared_ptr<arrow::ListBuilder> list_builder,
              arrow::StringBuilder* type_builder, ArrowArrays* chunks,
              std::shared_ptr<arrow::Array> null_array,
              WriterProperties* properties, size_t total) {
  AddNulls(list_builder, chunks, null_array, properties, total);

  int32_t start = list_vals->value_offset(index);
  int32_t end   = list_vals->value_offset(index + 1);

  auto st = list_builder->Append();
  for (int32_t s = start; s < end; s++) {
    st = type_builder->Append(vals->GetView(s));
    if (!st.ok()) {
      GALOIS_LOG_FATAL("Error appending value to an arrow array builder: {}",
                       st.ToString());
    }
  }
  // if we filled up a chunk, flush it
  if (((size_t)list_builder->length()) == properties->chunk_size) {
    chunks->push_back(BuildArray(list_builder));
  }
}

// Add falses until the array is even and then append true so that length =
// total + 1 at the end
void AddLabel(std::shared_ptr<arrow::BooleanBuilder> builder,
              ArrowArrays* chunks, WriterProperties* properties, size_t total) {
  AddFalses(builder, chunks, properties, total);
  auto st = builder->Append(true);
  if (!st.ok()) {
    GALOIS_LOG_FATAL("Error appending to an arrow array builder: {}",
                     st.ToString());
  }

  // if we filled up a chunk, flush it
  if (((size_t)builder->length()) == properties->chunk_size) {
    chunks->push_back(BuildArray(builder));
  }
}

/******************************************************************************/
/* Functions for ensuring all arrow arrays are of the right length in the end */
/******************************************************************************/

// Used to build the out_dests component of the CSR representation
uint64_t SetEdgeId(TopologyState* topology_builder,
                   std::vector<uint64_t>* offsets, size_t index) {
  uint32_t src  = topology_builder->sources[index];
  uint64_t base = src ? topology_builder->out_indices[src - 1] : 0;
  uint64_t i    = base + offsets->at(src)++;

  topology_builder->out_dests[i] = topology_builder->destinations[index];
  return i;
}

// Resolve string node IDs to node indexes, if a node does not exist create an
// empty node
void ResolveIntermediateIDs(GraphState* builder) {
  TopologyState* topology = &builder->topology_builder;

  for (size_t i = 0; i < topology->destinations.size(); i++) {
    if (topology->destinations[i] == std::numeric_limits<uint32_t>::max()) {
      auto str_id     = topology->destinations_intermediate[i];
      auto dest_index = topology->node_indexes.find(str_id);
      uint32_t dest;
      // if node does not exist, create it
      if (dest_index == topology->node_indexes.end()) {
        dest = builder->nodes;
        topology->node_indexes.insert(
            std::pair<std::string, size_t>(str_id, dest));
        builder->nodes++;
        topology->out_indices.push_back(0);
      } else {
        dest = (uint32_t)dest_index->second;
      }
      topology->destinations[i] = dest;
    }

    if (topology->sources[i] == std::numeric_limits<uint32_t>::max()) {
      auto str_id    = topology->sources_intermediate[i];
      auto src_index = topology->node_indexes.find(str_id);
      uint32_t src;
      if (src_index == topology->node_indexes.end()) {
        src = builder->nodes;
        topology->node_indexes.insert(
            std::pair<std::string, size_t>(str_id, src));
        builder->nodes++;
        topology->out_indices.push_back(0);
      } else {
        src = (uint32_t)src_index->second;
      }
      topology->sources[i] = src;
      topology->out_indices[src]++;
    }
  }
}

// Adds nulls to the array until its length == total
template <typename T>
void EvenOutArray(ArrowArrays* chunks, std::shared_ptr<T> builder,
                  std::shared_ptr<arrow::Array> null_array,
                  WriterProperties* properties, size_t total) {
  AddNulls(builder, chunks, null_array, properties, total);

  if (total % properties->chunk_size != 0) {
    chunks->push_back(BuildArray(builder));
  }
}

// Adds falses to the array until its length == total
void EvenOutArray(ArrowArrays* chunks,
                  std::shared_ptr<arrow::BooleanBuilder> builder,
                  WriterProperties* properties, size_t total) {
  AddFalses(builder, chunks, properties, total);

  if (total % properties->chunk_size != 0) {
    chunks->push_back(BuildArray(builder));
  }
}

// Adds nulls to the arrays until each length == total
void EvenOutChunkBuilders(ArrayBuilders* builders,
                          std::vector<ArrowArrays>* chunks,
                          WriterProperties* properties, size_t total) {
  galois::do_all(galois::iterate((size_t)0, builders->size()),
                 [&](const size_t& i) {
                   AddNulls(builders->at(i), &chunks->at(i), properties, total);

                   if (total % properties->chunk_size != 0) {
                     chunks->at(i).push_back(BuildArray(builders->at(i)));
                   }
                 });
}

// Adds falses to the arrays until each length == total
void EvenOutChunkBuilders(BooleanBuilders* builders,
                          std::vector<ArrowArrays>* chunks,
                          WriterProperties* properties, size_t total) {
  galois::do_all(
      galois::iterate((size_t)0, builders->size()), [&](const size_t& i) {
        AddFalses(builders->at(i), &chunks->at(i), properties, total);

        if (total % properties->chunk_size != 0) {
          chunks->at(i).push_back(BuildArray(builders->at(i)));
        }
      });
}

/**************************************************/
/* Functions for reordering edges into CSR format */
/**************************************************/

// Rearrange an array's entries to match up with those of mapping
template <typename T, typename W>
ArrowArrays
RearrangeArray(std::shared_ptr<T> builder,
               const std::shared_ptr<arrow::ChunkedArray>& chunked_array,
               const std::vector<size_t>& mapping,
               WriterProperties* properties) {
  auto chunk_size = properties->chunk_size;
  ArrowArrays chunks;
  auto st = builder->Reserve(chunk_size);
  if (!st.ok()) {
    GALOIS_LOG_FATAL("Error reserving space for arrow array: {}",
                     st.ToString());
  }
  // cast and store array chunks for use in loop
  std::vector<std::shared_ptr<W>> arrays;
  for (auto chunk : chunked_array->chunks()) {
    arrays.push_back(std::static_pointer_cast<W>(chunk));
  }
  std::shared_ptr<arrow::Array> null_array =
      properties->null_arrays.first.find(builder->type()->id())->second;

  // add non-null values
  for (size_t i = 0; i < mapping.size(); i++) {
    auto array = arrays[mapping[i] / chunk_size];

    if (!array->IsNull(mapping[i] % chunk_size)) {
      auto val = array->Value(mapping[i] % chunk_size);
      AddTypedValue(val, builder, &chunks, null_array, properties, i);
    }
  }
  EvenOutArray(&chunks, builder, null_array, properties, mapping.size());
  return chunks;
}

// Rearrange an array's entries to match up with those of mapping
ArrowArrays
RearrangeArray(std::shared_ptr<arrow::StringBuilder> builder,
               const std::shared_ptr<arrow::ChunkedArray>& chunked_array,
               const std::vector<size_t>& mapping,
               WriterProperties* properties) {
  auto chunk_size = properties->chunk_size;
  ArrowArrays chunks;
  auto st = builder->Reserve(chunk_size);
  if (!st.ok()) {
    GALOIS_LOG_FATAL("Error reserving space for arrow array: {}",
                     st.ToString());
  }
  // cast and store array chunks for use in loop
  std::vector<std::shared_ptr<arrow::StringArray>> arrays;
  for (auto chunk : chunked_array->chunks()) {
    arrays.push_back(std::static_pointer_cast<arrow::StringArray>(chunk));
  }
  std::shared_ptr<arrow::Array> null_array =
      properties->null_arrays.first.find(builder->type()->id())->second;

  // add non-null values
  for (size_t i = 0; i < mapping.size(); i++) {
    auto array = arrays[mapping[i] / chunk_size];

    if (!array->IsNull(mapping[i] % chunk_size)) {
      auto val = array->GetView(mapping[i] % chunk_size);
      AddTypedValue(val, builder, &chunks, null_array, properties, i);
    }
  }
  EvenOutArray(&chunks, builder, null_array, properties, mapping.size());
  return chunks;
}

// Rearrange an array's entries to match up with those of mapping, for labels
ArrowArrays
RearrangeArray(std::shared_ptr<arrow::BooleanBuilder> builder,
               const std::shared_ptr<arrow::ChunkedArray>& chunked_array,
               const std::vector<size_t>& mapping,
               WriterProperties* properties) {
  auto chunk_size = properties->chunk_size;
  ArrowArrays chunks;
  auto st = builder->Reserve(chunk_size);
  if (!st.ok()) {
    GALOIS_LOG_FATAL("Error reserving space for arrow array: {}",
                     st.ToString());
  }
  // cast and store array chunks for use in loop
  std::vector<std::shared_ptr<arrow::BooleanArray>> arrays;
  for (auto chunk : chunked_array->chunks()) {
    arrays.push_back(std::static_pointer_cast<arrow::BooleanArray>(chunk));
  }

  // add non-null values
  for (size_t i = 0; i < mapping.size(); i++) {
    auto val = arrays[mapping[i] / chunk_size]->Value(mapping[i] % chunk_size);
    if (val) {
      AddLabel(builder, &chunks, properties, i);
    }
  }
  EvenOutArray(&chunks, builder, properties, mapping.size());
  return chunks;
}

// Rearrange a list array's entries to match up with those of mapping
template <typename T, typename W>
ArrowArrays RearrangeArray(
    const std::shared_ptr<arrow::ListBuilder>& builder, T* type_builder,
    const std::shared_ptr<arrow::ChunkedArray>& chunked_array,
    const std::vector<size_t>& mapping, WriterProperties* properties) {
  auto chunk_size = properties->chunk_size;
  ArrowArrays chunks;
  auto st = builder->Reserve(chunk_size);
  if (!st.ok()) {
    GALOIS_LOG_FATAL("Error reserving space for arrow array: {}",
                     st.ToString());
  }
  // cast and store array chunks for use in loop
  std::vector<std::shared_ptr<arrow::ListArray>> list_arrays;
  std::vector<std::shared_ptr<W>> sub_arrays;
  for (auto chunk : chunked_array->chunks()) {
    auto array_temp = std::static_pointer_cast<arrow::ListArray>(chunk);
    list_arrays.push_back(array_temp);
    sub_arrays.push_back(std::static_pointer_cast<W>(array_temp->values()));
  }
  std::shared_ptr<arrow::Array> null_array =
      properties->null_arrays.second.find(type_builder->type()->id())->second;

  // add values
  for (size_t i = 0; i < mapping.size(); i++) {
    auto list_array = list_arrays[mapping[i] / chunk_size];
    auto sub_array  = sub_arrays[mapping[i] / chunk_size];
    auto index      = mapping[i] % chunk_size;
    if (!list_array->IsNull(index)) {
      AddArray(list_array, sub_array, index, builder, type_builder, &chunks,
               null_array, properties, i);
    }
  }
  EvenOutArray(&chunks, builder, null_array, properties, mapping.size());
  return chunks;
}

// Rearrange a list array's entries to match up with those of mapping
ArrowArrays RearrangeListArray(
    const std::shared_ptr<arrow::ChunkedArray>& list_chunked_array,
    const std::vector<size_t>& mapping, WriterProperties* properties) {
  auto* pool = arrow::default_memory_pool();
  ArrowArrays chunks;
  auto list_type =
      std::static_pointer_cast<arrow::BaseListType>(list_chunked_array->type())
          ->value_type();

  switch (list_type->id()) {
  case arrow::Type::STRING: {
    auto builder = std::make_shared<arrow::ListBuilder>(
        pool, std::make_shared<arrow::StringBuilder>());
    auto sb = static_cast<arrow::StringBuilder*>(builder->value_builder());
    chunks  = RearrangeArray<arrow::StringBuilder, arrow::StringArray>(
        builder, sb, list_chunked_array, mapping, properties);
    break;
  }
  case arrow::Type::INT64: {
    auto builder = std::make_shared<arrow::ListBuilder>(
        pool, std::make_shared<arrow::Int64Builder>());
    auto lb = static_cast<arrow::Int64Builder*>(builder->value_builder());
    chunks  = RearrangeArray<arrow::Int64Builder, arrow::Int64Array>(
        builder, lb, list_chunked_array, mapping, properties);
    break;
  }
  case arrow::Type::INT32: {
    auto builder = std::make_shared<arrow::ListBuilder>(
        pool, std::make_shared<arrow::Int32Builder>());
    auto ib = static_cast<arrow::Int32Builder*>(builder->value_builder());
    chunks  = RearrangeArray<arrow::Int32Builder, arrow::Int32Array>(
        builder, ib, list_chunked_array, mapping, properties);
    break;
  }
  case arrow::Type::DOUBLE: {
    auto builder = std::make_shared<arrow::ListBuilder>(
        pool, std::make_shared<arrow::DoubleBuilder>());
    auto db = static_cast<arrow::DoubleBuilder*>(builder->value_builder());
    chunks  = RearrangeArray<arrow::DoubleBuilder, arrow::DoubleArray>(
        builder, db, list_chunked_array, mapping, properties);
    break;
  }
  case arrow::Type::FLOAT: {
    auto builder = std::make_shared<arrow::ListBuilder>(
        pool, std::make_shared<arrow::FloatBuilder>());
    auto fb = static_cast<arrow::FloatBuilder*>(builder->value_builder());
    chunks  = RearrangeArray<arrow::FloatBuilder, arrow::FloatArray>(
        builder, fb, list_chunked_array, mapping, properties);
    break;
  }
  case arrow::Type::BOOL: {
    auto builder = std::make_shared<arrow::ListBuilder>(
        pool, std::make_shared<arrow::BooleanBuilder>());
    auto bb = static_cast<arrow::BooleanBuilder*>(builder->value_builder());
    chunks  = RearrangeArray<arrow::BooleanBuilder, arrow::BooleanArray>(
        builder, bb, list_chunked_array, mapping, properties);
    break;
  }
  case arrow::Type::TIMESTAMP: {
    auto builder = std::make_shared<arrow::ListBuilder>(
        pool, std::make_shared<arrow::TimestampBuilder>(list_type, pool));
    auto tb = static_cast<arrow::TimestampBuilder*>(builder->value_builder());
    chunks  = RearrangeArray<arrow::TimestampBuilder, arrow::TimestampArray>(
        builder, tb, list_chunked_array, mapping, properties);
    break;
  }
  case arrow::Type::UINT8: {
    auto builder = std::make_shared<arrow::ListBuilder>(
        pool, std::make_shared<arrow::UInt8Builder>());
    auto bb = static_cast<arrow::UInt8Builder*>(builder->value_builder());
    chunks  = RearrangeArray<arrow::UInt8Builder, arrow::UInt8Array>(
        builder, bb, list_chunked_array, mapping, properties);
    break;
  }
  default: {
    GALOIS_LOG_FATAL(
        "Unsupported arrow array type passed to RearrangeListArray: {}",
        list_type);
  }
  }
  return chunks;
}

// Rearrange each column in a table so that their entries match up with those of
// mapping
std::vector<ArrowArrays> RearrangeTable(const ChunkedArrays& initial,
                                        const std::vector<size_t>& mapping,
                                        WriterProperties* properties) {
  std::vector<ArrowArrays> rearranged;
  rearranged.resize(initial.size());

  galois::do_all(
      galois::iterate((size_t)0, initial.size()), [&](const size_t& n) {
        auto array     = initial[n];
        auto arrayType = array->type()->id();
        ArrowArrays ca;

        switch (arrayType) {
        case arrow::Type::STRING: {
          auto sb = std::make_shared<arrow::StringBuilder>();
          ca      = RearrangeArray(sb, array, mapping, properties);
          break;
        }
        case arrow::Type::INT64: {
          auto lb = std::make_shared<arrow::Int64Builder>();
          ca      = RearrangeArray<arrow::Int64Builder, arrow::Int64Array>(
              lb, array, mapping, properties);
          break;
        }
        case arrow::Type::INT32: {
          auto ib = std::make_shared<arrow::Int32Builder>();
          ca      = RearrangeArray<arrow::Int32Builder, arrow::Int32Array>(
              ib, array, mapping, properties);
          break;
        }
        case arrow::Type::DOUBLE: {
          auto db = std::make_shared<arrow::DoubleBuilder>();
          ca      = RearrangeArray<arrow::DoubleBuilder, arrow::DoubleArray>(
              db, array, mapping, properties);
          break;
        }
        case arrow::Type::FLOAT: {
          auto fb = std::make_shared<arrow::FloatBuilder>();
          ca      = RearrangeArray<arrow::FloatBuilder, arrow::FloatArray>(
              fb, array, mapping, properties);
          break;
        }
        case arrow::Type::BOOL: {
          auto bb = std::make_shared<arrow::BooleanBuilder>();
          ca      = RearrangeArray<arrow::BooleanBuilder, arrow::BooleanArray>(
              bb, array, mapping, properties);
          break;
        }
        case arrow::Type::TIMESTAMP: {
          auto tb = std::make_shared<arrow::TimestampBuilder>(
              array->type(), arrow::default_memory_pool());
          ca = RearrangeArray<arrow::TimestampBuilder, arrow::TimestampArray>(
              tb, array, mapping, properties);
          break;
        }
        case arrow::Type::UINT8: {
          auto bb = std::make_shared<arrow::UInt8Builder>();
          ca      = RearrangeArray<arrow::UInt8Builder, arrow::UInt8Array>(
              bb, array, mapping, properties);
          break;
        }
        case arrow::Type::LIST: {
          ca = RearrangeListArray(array, mapping, properties);
          break;
        }
        default: {
          GALOIS_LOG_FATAL(
              "Unsupported arrow array type passed to RearrangeTable: {}",
              arrayType);
        }
        }
        rearranged[n] = ca;
        array.reset();
      });
  return rearranged;
}

// Rearrange each column in a table so that their entries match up with those of
// mapping, for labels
std::vector<ArrowArrays> RearrangeTypeTable(const ChunkedArrays& initial,
                                            const std::vector<size_t>& mapping,
                                            WriterProperties* properties) {
  std::vector<ArrowArrays> rearranged;
  rearranged.resize(initial.size());

  galois::do_all(galois::iterate((size_t)0, initial.size()),
                 [&](const size_t& n) {
                   auto array = initial[n];

                   auto bb = std::make_shared<arrow::BooleanBuilder>();
                   auto ca = RearrangeArray(bb, array, mapping, properties);
                   rearranged[n] = ca;

                   array.reset();
                 });
  return rearranged;
}

// Build CSR format and rearrange edge tables to correspond to the CSR
std::pair<std::shared_ptr<arrow::Table>, std::shared_ptr<arrow::Table>>
BuildFinalEdges(GraphState* builder, WriterProperties* properties) {
  galois::ParallelSTL::partial_sum(
      builder->topology_builder.out_indices.begin(),
      builder->topology_builder.out_indices.end(),
      builder->topology_builder.out_indices.begin());

  std::vector<size_t> edge_mapping;
  edge_mapping.resize(builder->edges, std::numeric_limits<uint64_t>::max());

  std::vector<uint64_t> offsets;
  offsets.resize(builder->nodes, 0);

  // get edge indices
  for (size_t i = 0; i < builder->topology_builder.sources.size(); i++) {
    uint64_t edgeId = SetEdgeId(&builder->topology_builder, &offsets, i);
    // edge_mapping[i] = edgeId;
    edge_mapping[edgeId] = i;
  }

  auto initial_edges = BuildChunks(&builder->edge_properties.chunks);
  auto initial_types = BuildChunks(&builder->edge_types.chunks);

  auto finalEdgeBuilders =
      RearrangeTable(initial_edges, edge_mapping, properties);
  auto finalTypeBuilders =
      RearrangeTypeTable(initial_types, edge_mapping, properties);

  std::cout << "Edge Properties Post:" << std::endl;
  WriteNullStats(finalEdgeBuilders, properties, builder->edges);
  std::cout << "Edge Types Post:" << std::endl;
  WriteFalseStats(finalTypeBuilders, properties, builder->edges);

  return std::pair<std::shared_ptr<arrow::Table>,
                   std::shared_ptr<arrow::Table>>(
      BuildTable(&finalEdgeBuilders, &builder->edge_properties.schema),
      BuildTable(&finalTypeBuilders, &builder->edge_types.schema));
}

/********************************************************************/
/* Helper functions for building initial null arrow array constants */
/********************************************************************/

template <typename T>
void AddNullArrays(
    std::unordered_map<int, std::shared_ptr<arrow::Array>>* null_map,
    std::unordered_map<int, std::shared_ptr<arrow::Array>>* lists_null_map,
    size_t elts) {
  auto* pool = arrow::default_memory_pool();

  // the builder types are still added for the list types since the list type is
  // extraneous info
  auto builder = std::make_shared<T>();
  auto st      = builder->AppendNulls(elts);
  null_map->insert(std::pair<int, std::shared_ptr<arrow::Array>>(
      builder->type()->id(), BuildArray(builder)));

  auto list_builder =
      std::make_shared<arrow::ListBuilder>(pool, std::make_shared<T>());
  st = list_builder->AppendNulls(elts);
  lists_null_map->insert(std::pair<int, std::shared_ptr<arrow::Array>>(
      builder->type()->id(), BuildArray(list_builder)));

  if (!st.ok()) {
    GALOIS_LOG_FATAL("Error creating null builders: {}", st.ToString());
  }
}

// for Timestamp Types
void AddNullArrays(
    std::unordered_map<int, std::shared_ptr<arrow::Array>>* null_map,
    std::unordered_map<int, std::shared_ptr<arrow::Array>>* lists_null_map,
    size_t elts, std::shared_ptr<arrow::DataType> type) {
  auto* pool = arrow::default_memory_pool();

  // the builder types are still added for the list types since the list type is
  // extraneous info
  auto builder = std::make_shared<arrow::TimestampBuilder>(type, pool);
  auto st      = builder->AppendNulls(elts);
  null_map->insert(std::pair<int, std::shared_ptr<arrow::Array>>(
      builder->type()->id(), BuildArray(builder)));

  auto list_builder = std::make_shared<arrow::ListBuilder>(
      pool, std::make_shared<arrow::TimestampBuilder>(type, pool));
  st = list_builder->AppendNulls(elts);
  lists_null_map->insert(std::pair<int, std::shared_ptr<arrow::Array>>(
      builder->type()->id(), BuildArray(list_builder)));

  if (!st.ok()) {
    GALOIS_LOG_FATAL("Error creating null builders: {}", st.ToString());
  }
}

NullMaps GetNullArrays(size_t elts) {
  std::unordered_map<int, std::shared_ptr<arrow::Array>> null_map;
  std::unordered_map<int, std::shared_ptr<arrow::Array>> lists_null_map;

  AddNullArrays<arrow::StringBuilder>(&null_map, &lists_null_map, elts);
  AddNullArrays<arrow::Int32Builder>(&null_map, &lists_null_map, elts);
  AddNullArrays<arrow::Int64Builder>(&null_map, &lists_null_map, elts);
  AddNullArrays<arrow::FloatBuilder>(&null_map, &lists_null_map, elts);
  AddNullArrays<arrow::DoubleBuilder>(&null_map, &lists_null_map, elts);
  AddNullArrays<arrow::BooleanBuilder>(&null_map, &lists_null_map, elts);
  AddNullArrays<arrow::UInt8Builder>(&null_map, &lists_null_map, elts);
  AddNullArrays(&null_map, &lists_null_map, elts,
                arrow::timestamp(arrow::TimeUnit::MILLI, "UTC"));

  return NullMaps(std::move(null_map), std::move(lists_null_map));
}

std::shared_ptr<arrow::Array> GetFalseArray(size_t elts) {
  auto builder = std::make_shared<arrow::BooleanBuilder>();
  arrow::Status st;
  for (size_t i = 0; i < elts; i++) {
    st = builder->Append(false);
  }
  if (!st.ok()) {
    GALOIS_LOG_FATAL("Error appending to an arrow array builder: {}",
                     st.ToString());
  }
  return BuildArray(builder);
}

/***************************************/
/* Functions for parsing GraphML files */
/***************************************/

// extract the type from an attr.type or attr.list attribute from a key element
ImportDataType ExtractTypeGraphML(xmlChar* value) {
  ImportDataType type = ImportDataType::kString;
  if (xmlStrEqual(value, BAD_CAST "string")) {
    type = ImportDataType::kString;
  } else if (xmlStrEqual(value, BAD_CAST "long")) {
    type = ImportDataType::kInt64;
  } else if (xmlStrEqual(value, BAD_CAST "int")) {
    type = ImportDataType::kInt32;
  } else if (xmlStrEqual(value, BAD_CAST "double")) {
    type = ImportDataType::kDouble;
  } else if (xmlStrEqual(value, BAD_CAST "float")) {
    type = ImportDataType::kFloat;
  } else if (xmlStrEqual(value, BAD_CAST "boolean")) {
    type = ImportDataType::kBoolean;
  } else {
    GALOIS_LOG_ERROR("Came across attr.type: {}, that is not supported",
                     std::string((const char*)value));
    type = ImportDataType::kString;
  }
  return type;
}

/*
 * reader should be pointing to key node elt before calling
 *
 * extracts key attribute information for use later
 */
PropertyKey ProcessKey(xmlTextReaderPtr reader) {
  int ret = xmlTextReaderMoveToNextAttribute(reader);
  xmlChar *name, *value;

  std::string id;
  bool for_node = false;
  bool for_edge = false;
  std::string attr_name;
  ImportDataType type = ImportDataType::kString;
  bool is_list        = false;

  while (ret == 1) {
    name  = xmlTextReaderName(reader);
    value = xmlTextReaderValue(reader);
    if (name != NULL) {
      if (xmlStrEqual(name, BAD_CAST "id")) {
        id = std::string((const char*)value);
      } else if (xmlStrEqual(name, BAD_CAST "for")) {
        for_node = xmlStrEqual(value, BAD_CAST "node") == 1;
        for_edge = xmlStrEqual(value, BAD_CAST "edge") == 1;
      } else if (xmlStrEqual(name, BAD_CAST "attr.name")) {
        attr_name = std::string((const char*)value);
      } else if (xmlStrEqual(name, BAD_CAST "attr.type")) {
        // do this check for neo4j
        if (!is_list) {
          type = ExtractTypeGraphML(value);
        }
      } else if (xmlStrEqual(name, BAD_CAST "attr.list")) {
        is_list = true;
        type    = ExtractTypeGraphML(value);
      } else {
        GALOIS_LOG_ERROR("Attribute on key: {}, was not recognized",
                         std::string((const char*)name));
      }
    }

    xmlFree(name);
    xmlFree(value);
    ret = xmlTextReaderMoveToNextAttribute(reader);
  }
  return PropertyKey{
      id, for_node, for_edge, attr_name, type, is_list,
  };
}

/*
 * reader should be pointing at the data element before calling
 *
 * parses data from a GraphML file into property: pair<string, string>
 */
std::pair<std::string, std::string> ProcessData(xmlTextReaderPtr reader) {
  auto minimum_depth = xmlTextReaderDepth(reader);

  int ret = xmlTextReaderMoveToNextAttribute(reader);
  xmlChar *name, *value;

  std::string key;
  std::string propertyData;

  // parse node attributes for key (required)
  while (ret == 1) {
    name  = xmlTextReaderName(reader);
    value = xmlTextReaderValue(reader);
    if (name != NULL) {
      if (xmlStrEqual(name, BAD_CAST "key")) {
        key = std::string((const char*)value);
      } else {
        GALOIS_LOG_ERROR("Attribute on node: {}, was not recognized",
                         std::string((const char*)name));
      }
    }

    xmlFree(name);
    xmlFree(value);
    ret = xmlTextReaderMoveToNextAttribute(reader);
  }

  // parse xml text nodes for property data
  ret = xmlTextReaderRead(reader);
  // will terminate when </data> reached or an improper read
  while (ret == 1 && minimum_depth < xmlTextReaderDepth(reader)) {
    name = xmlTextReaderName(reader);
    // if elt is an xml text node
    if (xmlTextReaderNodeType(reader) == 3) {
      value        = xmlTextReaderValue(reader);
      propertyData = std::string((const char*)value);
      xmlFree(value);
    }

    xmlFree(name);
    ret = xmlTextReaderRead(reader);
  }
  return make_pair(key, propertyData);
}

/*
 * reader should be pointing at the node element before calling
 *
 * parses the node from a GraphML file into readable form
 */
bool ProcessNode(xmlTextReaderPtr reader, GraphState* builder,
                 WriterProperties* properties) {
  auto minimum_depth = xmlTextReaderDepth(reader);

  int ret = xmlTextReaderMoveToNextAttribute(reader);
  xmlChar *name, *value;

  std::string id;
  std::vector<std::string> labels;

  bool extractedLabels = false; // neo4j includes these twice so only parse 1

  // parse node attributes for id (required) and label(s) (optional)
  while (ret == 1) {
    name  = xmlTextReaderName(reader);
    value = xmlTextReaderValue(reader);

    if (name != NULL) {
      if (xmlStrEqual(name, BAD_CAST "id")) {
        id = std::string((const char*)value);
      } else if (xmlStrEqual(name, BAD_CAST "labels") ||
                 xmlStrEqual(name, BAD_CAST "label")) {
        std::string data((const char*)value);
        // erase prepended ':' if it exists
        if (data.front() == ':') {
          data.erase(0, 1);
        }
        boost::split(labels, data, boost::is_any_of(":"));
        extractedLabels = true;
      } else {
        GALOIS_LOG_ERROR(
            "Attribute on node: {}, with value {} was not recognized",
            std::string((const char*)name), std::string((const char*)value));
      }
    }

    xmlFree(name);
    xmlFree(value);
    ret = xmlTextReaderMoveToNextAttribute(reader);
  }

  bool validNode = !id.empty();
  if (validNode) {
    builder->topology_builder.node_indexes.insert(
        std::pair<std::string, size_t>(
            id, builder->topology_builder.node_indexes.size()));
  }

  // parse "data" xml nodes for properties
  ret = xmlTextReaderRead(reader);
  // will terminate when </node> reached or an improper read
  while (ret == 1 && minimum_depth < xmlTextReaderDepth(reader)) {
    name = xmlTextReaderName(reader);
    if (name == NULL) {
      name = xmlStrdup(BAD_CAST "--");
    }
    // if elt is an xml node (we do not parse text for nodes)
    if (xmlTextReaderNodeType(reader) == 1) {
      // if elt is a "data" xml node read it in
      if (xmlStrEqual(name, BAD_CAST "data")) {
        auto property = ProcessData(reader);
        if (property.first.size() > 0) {
          // we reserve the data fields label and labels for node/edge labels
          if (property.first == std::string("label") ||
              property.first == std::string("labels")) {
            if (!extractedLabels) {
              // erase prepended ':' if it exists
              std::string data = property.second;
              if (data.front() == ':') {
                data.erase(0, 1);
              }
              boost::split(labels, data, boost::is_any_of(":"));
              extractedLabels = true;
            }
          } else if (property.first != std::string("IGNORE")) {
            if (validNode) {
              auto keyIter = builder->node_properties.keys.find(property.first);
              size_t index;
              // if an entry for the key does not already exist, make a default
              // entry for it
              if (keyIter == builder->node_properties.keys.end()) {
                index =
                    AddStringBuilder(property.first, &builder->node_properties);
              } else {
                index = keyIter->second;
              }
              AddValue(property.second,
                       builder->node_properties.builders[index],
                       &builder->node_properties.chunks[index], properties,
                       builder->nodes);
            }
          }
        }
      } else {
        GALOIS_LOG_ERROR("In node found element: {}, which was ignored",
                         std::string((const char*)name));
      }
    }

    xmlFree(name);
    ret = xmlTextReaderRead(reader);
  }

  // add labels if they exists
  if (validNode && labels.size() > 0) {
    for (std::string label : labels) {
      auto entry = builder->node_labels.keys.find(label);
      size_t index;

      // if label does not already exist, add a column
      if (entry == builder->node_labels.keys.end()) {
        index =
            AddFalseBuilder(std::pair<std::string, std::string>(label, label),
                            &builder->node_labels);
      } else {
        index = entry->second;
      }
      AddLabel(builder->node_labels.builders[index],
               &builder->node_labels.chunks[index], properties, builder->nodes);
    }
  }
  return validNode;
}

/*
 * reader should be pointing at the edge element before calling
 *
 * parses the edge from a GraphML file into readable form
 */
bool ProcessEdge(xmlTextReaderPtr reader, GraphState* builder,
                 WriterProperties* properties) {
  auto minimum_depth = xmlTextReaderDepth(reader);

  int ret = xmlTextReaderMoveToNextAttribute(reader);
  xmlChar *name, *value;

  std::string source;
  std::string target;
  std::string type;
  bool extracted_type = false; // neo4j includes these twice so only parse 1

  // parse node attributes for id (required) and label(s) (optional)
  while (ret == 1) {
    name  = xmlTextReaderName(reader);
    value = xmlTextReaderValue(reader);

    if (name != NULL) {
      if (xmlStrEqual(name, BAD_CAST "id")) {
      } else if (xmlStrEqual(name, BAD_CAST "source")) {
        source = std::string((const char*)value);
      } else if (xmlStrEqual(name, BAD_CAST "target")) {
        target = std::string((const char*)value);
      } else if (xmlStrEqual(name, BAD_CAST "labels") ||
                 xmlStrEqual(name, BAD_CAST "label")) {
        type           = std::string((const char*)value);
        extracted_type = true;
      } else {
        GALOIS_LOG_ERROR(
            "Attribute on edge: {}, with value {} was not recognized",
            std::string((const char*)name), std::string((const char*)value));
      }
    }

    xmlFree(name);
    xmlFree(value);
    ret = xmlTextReaderMoveToNextAttribute(reader);
  }

  bool valid_edge = !source.empty() && !target.empty();
  if (valid_edge) {
    auto src_entry  = builder->topology_builder.node_indexes.find(source);
    auto dest_entry = builder->topology_builder.node_indexes.find(target);

    valid_edge = src_entry != builder->topology_builder.node_indexes.end() &&
                 dest_entry != builder->topology_builder.node_indexes.end();
    if (valid_edge) {
      builder->topology_builder.sources.push_back(src_entry->second);
      builder->topology_builder.destinations.push_back(
          (uint32_t)dest_entry->second);
      builder->topology_builder.out_indices[src_entry->second]++;
    }
  }

  // parse "data" xml edges for properties
  ret = xmlTextReaderRead(reader);
  // will terminate when </edge> reached or an improper read
  while (ret == 1 && minimum_depth < xmlTextReaderDepth(reader)) {
    name = xmlTextReaderName(reader);
    if (name == NULL) {
      name = xmlStrdup(BAD_CAST "--");
    }
    // if elt is an xml node (we do not parse text for nodes)
    if (xmlTextReaderNodeType(reader) == 1) {
      // if elt is a "data" xml node read it in
      if (xmlStrEqual(name, BAD_CAST "data")) {
        auto property = ProcessData(reader);
        if (property.first.size() > 0) {
          // we reserve the data fields label and labels for node/edge labels
          if (property.first == std::string("label") ||
              property.first == std::string("labels")) {
            if (!extracted_type) {
              type           = property.second;
              extracted_type = true;
            }
          } else if (property.first != std::string("IGNORE")) {
            if (valid_edge) {
              auto keyIter = builder->edge_properties.keys.find(property.first);
              size_t index;
              // if an entry for the key does not already exist, make a default
              // entry for it
              if (keyIter == builder->edge_properties.keys.end()) {
                index =
                    AddStringBuilder(property.first, &builder->edge_properties);
              } else {
                index = keyIter->second;
              }
              AddValue(property.second,
                       builder->edge_properties.builders[index],
                       &builder->edge_properties.chunks[index], properties,
                       builder->edges);
            }
          }
        }
      } else {
        GALOIS_LOG_ERROR("In edge found element: {}, which was ignored",
                         std::string((const char*)name));
      }
    }

    xmlFree(name);
    ret = xmlTextReaderRead(reader);
  }

  // add type if it exists
  if (valid_edge && type.length() > 0) {
    auto entry = builder->edge_types.keys.find(type);
    size_t index;

    // if type does not already exist, add a column
    if (entry == builder->edge_types.keys.end()) {
      index = AddFalseBuilder(std::pair<std::string, std::string>(type, type),
                              &builder->edge_types);
    } else {
      index = entry->second;
    }
    AddLabel(builder->edge_types.builders[index],
             &builder->edge_types.chunks[index], properties, builder->edges);
  }
  return valid_edge;
}

/*
 * reader should be pointing at the graph element before calling
 *
 * parses the graph structure from a GraphML file into Galois format
 */
void ProcessGraph(xmlTextReaderPtr reader, GraphState* builder,
                  WriterProperties* properties) {
  auto minimum_depth = xmlTextReaderDepth(reader);
  int ret            = xmlTextReaderRead(reader);

  bool finished_nodes = false;

  // will terminate when </graph> reached or an improper read
  while (ret == 1 && minimum_depth < xmlTextReaderDepth(reader)) {
    xmlChar* name;
    name = xmlTextReaderName(reader);
    if (name == NULL) {
      name = xmlStrdup(BAD_CAST "--");
    }
    // if elt is an xml node
    if (xmlTextReaderNodeType(reader) == 1) {
      // if elt is a "node" xml node read it in
      if (xmlStrEqual(name, BAD_CAST "node")) {
        if (ProcessNode(reader, builder, properties)) {
          builder->topology_builder.out_indices.push_back(0);
          builder->nodes++;

          if (builder->nodes % (properties->chunk_size * 100) == 0) {
            GALOIS_LOG_VERBOSE("Nodes Processed: {}", builder->nodes);
          }
        }
      } else if (xmlStrEqual(name, BAD_CAST "edge")) {
        if (!finished_nodes) {
          finished_nodes = true;
          std::cout << "Finished processing nodes" << std::endl;
        }
        // if elt is an "egde" xml node read it in
        if (ProcessEdge(reader, builder, properties)) {
          builder->edges++;

          if (builder->edges % (properties->chunk_size * 100) == 0) {
            GALOIS_LOG_VERBOSE("Edges Processed: {}", builder->edges);
          }
        }
      } else {
        GALOIS_LOG_ERROR("Found element: {}, which was ignored",
                         std::string((const char*)name));
      }
    }

    xmlFree(name);
    ret = xmlTextReaderRead(reader);
  }
  builder->node_properties.keys.clear();
  builder->node_labels.keys.clear();
  builder->edge_properties.keys.clear();
  builder->edge_types.keys.clear();
  std::cout << "Finished processing edges" << std::endl;

  // add buffered rows before exiting and even out columns
  EvenOutChunkBuilders(&builder->node_properties.builders,
                       &builder->node_properties.chunks, properties,
                       builder->nodes);
  EvenOutChunkBuilders(&builder->node_labels.builders,
                       &builder->node_labels.chunks, properties,
                       builder->nodes);
  EvenOutChunkBuilders(&builder->edge_properties.builders,
                       &builder->edge_properties.chunks, properties,
                       builder->edges);
  EvenOutChunkBuilders(&builder->edge_types.builders,
                       &builder->edge_types.chunks, properties, builder->edges);

  builder->topology_builder.out_dests.resize(
      builder->edges, std::numeric_limits<uint32_t>::max());
}

/***********************************/
/* Functions for importing MongoDB */
/***********************************/

std::string TypeName(ImportDataType type) {
  switch (type) {
  case ImportDataType::kString:
    return std::string("string");
  case ImportDataType::kDouble:
    return std::string("double");
  case ImportDataType::kInt64:
    return std::string("int64");
  case ImportDataType::kInt32:
    return std::string("int32");
  case ImportDataType::kBoolean:
    return std::string("bool");
  case ImportDataType::kTimestampMilli:
    return std::string("timestamp milli");
  case ImportDataType::kStruct:
    return std::string("struct");
  default:
    return std::string("unsupported");
  }
}

ImportDataType ParseType(const std::string& in) {
  auto type = boost::to_lower_copy<std::string>(in);
  if (type == std::string("string")) {
    return ImportDataType::kString;
  }
  if (type == std::string("double")) {
    return ImportDataType::kDouble;
  }
  if (type == std::string("float")) {
    return ImportDataType::kFloat;
  }
  if (type == std::string("int64")) {
    return ImportDataType::kInt64;
  }
  if (type == std::string("int32")) {
    return ImportDataType::kInt32;
  }
  if (type == std::string("bool")) {
    return ImportDataType::kBoolean;
  }
  if (type == std::string("timestamp")) {
    return ImportDataType::kTimestampMilli;
  }
  if (type == std::string("struct")) {
    return ImportDataType::kStruct;
  }
  return ImportDataType::kUnsupported;
}

// extract the type from a bson type
ImportDataType ExtractTypeMongoDB(bsoncxx::type value) {
  switch (value) {
  case bsoncxx::type::k_utf8:
    return ImportDataType::kString;
  // case bsoncxx::type::k_oid:
  //  return ImportDataType::kString;
  case bsoncxx::type::k_double:
    return ImportDataType::kDouble;
  case bsoncxx::type::k_int64:
    return ImportDataType::kInt64;
  case bsoncxx::type::k_int32:
    return ImportDataType::kInt32;
  case bsoncxx::type::k_bool:
    return ImportDataType::kBoolean;
  case bsoncxx::type::k_date:
    return ImportDataType::kTimestampMilli;
  case bsoncxx::type::k_document:
    return ImportDataType::kStruct;
  default:
    return ImportDataType::kUnsupported;
  }
}

PropertyKey ProcessElement(const bsoncxx::document::element& elt,
                           const std::string& name) {
  auto elt_type = elt.type();
  bool is_list  = elt_type == bsoncxx::type::k_array;
  if (is_list) {
    auto array = elt.get_array().value;
    if (array.length() <= 0) {
      return PropertyKey{
          name,
          ImportDataType::kUnsupported,
          is_list,
      };
    }
    elt_type = array[0].type();
    if (elt_type == bsoncxx::type::k_document) {
      return PropertyKey{
          name,
          ImportDataType::kUnsupported,
          is_list,
      };
    }
  }

  return PropertyKey{
      name,
      ExtractTypeMongoDB(elt_type),
      is_list,
  };
}

void HandleNodeDocumentMongoDB(GraphState* builder,
                               WriterProperties* properties,
                               const bsoncxx::document::view& doc,
                               const std::string& collection_name);

void AddEdge(GraphState* builder, WriterProperties* properties, uint32_t src,
             uint32_t dest, const std::string& type) {
  builder->topology_builder.sources_intermediate.push_back(std::string(""));
  builder->topology_builder.sources.push_back(src);
  builder->topology_builder.destinations_intermediate.push_back(
      std::string(""));
  builder->topology_builder.destinations.push_back(dest);
  builder->topology_builder.out_indices[src]++;

  // add type
  auto entry = builder->edge_types.keys.find(type);
  size_t index;
  // if type does not already exist, add a column
  if (entry == builder->edge_types.keys.end()) {
    index = AddFalseBuilder(std::pair<std::string, std::string>(type, type),
                            &builder->edge_types);
  } else {
    index = entry->second;
  }
  AddLabel(builder->edge_types.builders[index],
           &builder->edge_types.chunks[index], properties, builder->edges);

  builder->edges++;
}

void AddEdge(GraphState* builder, WriterProperties* properties, uint32_t src,
             const std::string& dest, const std::string& type) {
  // if dest is an edge, do not create a shallow edge to it
  if (builder->topology_builder.edge_ids.find(dest) !=
      builder->topology_builder.edge_ids.end()) {
    return;
  }

  builder->topology_builder.sources_intermediate.push_back(std::string(""));
  builder->topology_builder.sources.push_back(src);
  builder->topology_builder.destinations_intermediate.push_back(dest);
  builder->topology_builder.destinations.push_back(
      std::numeric_limits<uint32_t>::max());
  builder->topology_builder.out_indices[src]++;

  // add type
  auto entry = builder->edge_types.keys.find(type);
  size_t index;
  // if type does not already exist, add a column
  if (entry == builder->edge_types.keys.end()) {
    index = AddFalseBuilder(std::pair<std::string, std::string>(type, type),
                            &builder->edge_types);
  } else {
    index = entry->second;
  }
  AddLabel(builder->edge_types.builders[index],
           &builder->edge_types.chunks[index], properties, builder->edges);

  builder->edges++;
}

void HandleEmbeddedDocuments(
    GraphState* builder, WriterProperties* properties,
    const std::vector<bsoncxx::document::element>& docs,
    const std::string& parent_name, size_t parent_index) {
  for (bsoncxx::document::element elt : docs) {
    std::string name = elt.key().data();

    if (elt.type() == bsoncxx::type::k_document) {
      std::string edge_type = parent_name + "_" + name;
      AddEdge(builder, properties, (uint32_t)parent_index,
              (uint32_t)builder->topology_builder.node_indexes.size(),
              edge_type);

      HandleNodeDocumentMongoDB(builder, properties, elt.get_document(), name);
    } else {
      bsoncxx::array::view array = elt.get_array().value;
      std::string edge_type      = name;

      for (bsoncxx::array::element arr_elt : array) {
        AddEdge(builder, properties, (uint32_t)parent_index,
                (uint32_t)builder->topology_builder.node_indexes.size(),
                edge_type);
        HandleNodeDocumentMongoDB(builder, properties, arr_elt.get_document(),
                                  name);
      }
    }
  }
}

bool HandleNonPropertyNodeElement(GraphState* builder,
                                  WriterProperties* properties,
                                  std::vector<bsoncxx::document::element>* docs,
                                  const bsoncxx::document::element& elt,
                                  size_t node_index,
                                  const std::string& collection_name) {
  auto name     = elt.key().data();
  auto elt_type = elt.type();

  // initialize new node
  if (name == std::string("_id")) {
    builder->topology_builder.node_indexes.insert(
        std::pair<std::string, size_t>(elt.get_oid().value.to_string(),
                                       node_index));
    return true;
  }
  /* if an elt is a document type treat it as a struct
  // if elt is an embedded document defer adding it to later
  if (elt_type == bsoncxx::type::k_document) {
    docs->push_back(elt);
    return true;
  }*/
  // if elt is an ObjectID (foreign key), add a property-less edge
  if (elt_type == bsoncxx::type::k_oid) {
    std::string edge_type = collection_name + "_" + name;
    AddEdge(builder, properties, (uint32_t)node_index,
            elt.get_oid().value.to_string(), edge_type);
    return true;
  }
  // if elt is an array of embedded documents defer adding them to later
  if (elt_type == bsoncxx::type::k_array) {
    auto arr = elt.get_array().value;
    if (arr.length() > 0 && arr[0].type() == bsoncxx::type::k_document) {
      docs->push_back(elt);
      return true;
    }
    if (arr.length() > 0 && arr[0].type() == bsoncxx::type::k_oid) {
      for (auto arr_elt : arr) {
        std::string edge_type = name;
        AddEdge(builder, properties, (uint32_t)node_index,
                arr_elt.get_oid().value.to_string(), edge_type);
      }
      return true;
    }
  }
  return false;
}

void HandleEmbeddedNodeStruct(GraphState* builder, WriterProperties* properties,
                              std::vector<bsoncxx::document::element>* docs,
                              const bsoncxx::document::element& doc_elt,
                              const std::string& prefix, size_t parent_index) {
  const bsoncxx::document::view& doc = doc_elt.get_document();

  // handle document
  for (bsoncxx::document::element elt : doc) {
    if (HandleNonPropertyNodeElement(builder, properties, docs, elt,
                                     parent_index, doc_elt.key().data())) {
      continue;
    }
    auto elt_name = prefix + elt.key().data();

    // since all edge cases have been checked, we can add this property
    auto keyIter = builder->node_properties.keys.find(elt_name);
    size_t index;
    // if an entry for the key does not already exist, make an
    // entry for it
    if (keyIter == builder->node_properties.keys.end()) {
      auto key = ProcessElement(elt, elt_name);
      if (key.type == ImportDataType::kUnsupported) {
        std::cout << "elt not type not supported: " << ((uint8_t)elt.type())
                  << std::endl;
        continue;
      }
      index = AddBuilder(&builder->node_properties, std::move(key));
    } else {
      index = keyIter->second;
    }
    AddValue(elt, builder->node_properties.builders[index],
             &builder->node_properties.chunks[index], properties,
             builder->nodes);
    if (elt.type() == bsoncxx::type::k_document) {
      auto new_prefix = elt_name + ".";
      HandleEmbeddedNodeStruct(builder, properties, docs, elt, new_prefix,
                               parent_index);
    }
  }
}

// for now only handle arrays and data all of same type
void HandleNodeDocumentMongoDB(GraphState* builder,
                               WriterProperties* properties,
                               const bsoncxx::document::view& doc,
                               const std::string& collection_name) {
  auto node_index = builder->topology_builder.node_indexes.size();
  builder->topology_builder.out_indices.push_back(0);
  std::vector<bsoncxx::document::element> docs;

  // handle document
  for (bsoncxx::document::element elt : doc) {
    if (HandleNonPropertyNodeElement(builder, properties, &docs, elt,
                                     node_index, collection_name)) {
      continue;
    }

    // since all edge cases have been checked, we can add this property
    auto keyIter = builder->node_properties.keys.find(elt.key().data());
    size_t index;
    // if an entry for the key does not already exist, make an
    // entry for it
    if (keyIter == builder->node_properties.keys.end()) {
      auto key = ProcessElement(elt, elt.key().data());
      if (key.type == ImportDataType::kUnsupported) {
        std::cout << "elt not type not supported: " << ((uint8_t)elt.type())
                  << std::endl;
        continue;
      }
      index = AddBuilder(&builder->node_properties, std::move(key));
    } else {
      index = keyIter->second;
    }
    AddValue(elt, builder->node_properties.builders[index],
             &builder->node_properties.chunks[index], properties,
             builder->nodes);
    if (elt.type() == bsoncxx::type::k_document) {
      std::string prefix = elt.key().data() + std::string(".");
      HandleEmbeddedNodeStruct(builder, properties, &docs, elt, prefix,
                               node_index);
    }
  }

  // add label
  auto entry = builder->node_labels.keys.find(collection_name);
  size_t index;
  // if type does not already exist, add a column
  if (entry == builder->node_labels.keys.end()) {
    index = AddFalseBuilder(
        std::pair<std::string, std::string>(collection_name, collection_name),
        &builder->node_labels);
  } else {
    index = entry->second;
  }
  AddLabel(builder->node_labels.builders[index],
           &builder->node_labels.chunks[index], properties, builder->nodes);

  builder->nodes++;
  // deal with embedded documents
  HandleEmbeddedDocuments(builder, properties, docs, collection_name,
                          node_index);
}

void HandleEmbeddedEdgeStruct(GraphState* builder, WriterProperties* properties,
                              const bsoncxx::document::view& doc,
                              const std::string& prefix) {

  // handle document
  for (bsoncxx::document::element elt : doc) {
    auto elt_name = prefix + elt.key().data();

    // since all edge cases have been checked, we can add this property
    auto keyIter = builder->edge_properties.keys.find(elt_name);
    size_t index;
    // if an entry for the key does not already exist, make an
    // entry for it
    if (keyIter == builder->edge_properties.keys.end()) {
      auto key = ProcessElement(elt, elt_name);
      if (key.type == ImportDataType::kUnsupported) {
        std::cout << "elt not type not supported: " << ((uint8_t)elt.type())
                  << std::endl;
        continue;
      }
      index = AddBuilder(&builder->edge_properties, std::move(key));
    } else {
      index = keyIter->second;
    }
    AddValue(elt, builder->edge_properties.builders[index],
             &builder->edge_properties.chunks[index], properties,
             builder->edges);
    if (elt.type() == bsoncxx::type::k_document) {
      auto new_prefix = elt_name + ".";
      HandleEmbeddedEdgeStruct(builder, properties, elt.get_document(),
                               new_prefix);
    }
  }
}

// for now only handle arrays and data all of same type
void HandleEdgeDocumentMongoDB(GraphState* builder,
                               WriterProperties* properties,
                               const bsoncxx::document::view& doc,
                               const std::string& collection_name) {
  bool found_source = false;
  std::string src;
  std::string dest;

  // handle document
  for (bsoncxx::document::element elt : doc) {
    auto name = elt.key().data();

    // initialize new node
    if (name == std::string("_id")) {
      builder->topology_builder.edge_ids.insert(
          elt.get_oid().value.to_string());
      continue;
    }
    // handle src and destination node IDs
    if (elt.type() == bsoncxx::type::k_oid) {
      if (!found_source) {
        src          = elt.get_oid().value.to_string();
        found_source = true;
      } else {
        dest = elt.get_oid().value.to_string();
      }
      continue;
    }

    // since all edge cases have been checked, we can add this property
    auto keyIter = builder->edge_properties.keys.find(name);
    size_t index;
    // if an entry for the key does not already exist, make an
    // entry for it
    if (keyIter == builder->edge_properties.keys.end()) {
      auto key = ProcessElement(elt, name);
      if (key.type == ImportDataType::kUnsupported) {
        std::cout << "elt not type not supported: " << ((uint8_t)elt.type())
                  << std::endl;
        continue;
      }
      index = AddBuilder(&builder->edge_properties, std::move(key));
    } else {
      index = keyIter->second;
    }
    AddValue(elt, builder->edge_properties.builders[index],
             &builder->edge_properties.chunks[index], properties,
             builder->edges);
    if (elt.type() == bsoncxx::type::k_document) {
      std::string prefix = elt.key().data() + std::string(".");
      HandleEmbeddedEdgeStruct(builder, properties, elt.get_document(), prefix);
    }
  }

  // add type
  auto entry = builder->edge_types.keys.find(collection_name);
  size_t index;
  // if type does not already exist, add a column
  if (entry == builder->edge_types.keys.end()) {
    index = AddFalseBuilder(
        std::pair<std::string, std::string>(collection_name, collection_name),
        &builder->edge_types);
  } else {
    index = entry->second;
  }
  AddLabel(builder->edge_types.builders[index],
           &builder->edge_types.chunks[index], properties, builder->edges);

  // handle topology requirements
  builder->topology_builder.sources_intermediate.push_back(src);
  builder->topology_builder.sources.push_back(
      std::numeric_limits<uint32_t>::max());
  builder->topology_builder.destinations_intermediate.push_back(dest);
  builder->topology_builder.destinations.push_back(
      std::numeric_limits<uint32_t>::max());

  builder->edges++;
}
/* deprecated comment
 *    - it contains an embedded document
 */

/* A document is not an edge if:
 *    - it contains an array of ObjectIDs
 *    - it contains an array of Documents
 *    - it does not have exactly 2 ObjectIDs excluding its own ID
 */
bool CheckIfDocumentIsEdge(const bsoncxx::document::view& doc) {
  uint32_t oid_count = 0;

  // handle document
  for (bsoncxx::document::element elt : doc) {
    if (elt.key().data() == std::string("_id")) {
      continue;
    }

    switch (elt.type()) {
    case bsoncxx::type::k_oid: {
      oid_count++;
      if (oid_count > 2) {
        return false;
      }
      break;
    }
    case bsoncxx::type::k_array: {
      bsoncxx::array::view arr = elt.get_array().value;
      if (arr.length() > 0) {
        if (arr[0].type() == bsoncxx::type::k_document ||
            arr[0].type() == bsoncxx::type::k_oid) {
          return false;
        }
      }
      break;
    } /*
     case bsoncxx::type::k_document: {
       return false;
     }*/
    default: {
      break;
    }
    }
  }
  return oid_count == 2;
}

bool CheckIfCollectionIsEdge(mongocxx::collection* coll) {
  {
    auto doc = coll->find_one({});
    if (!CheckIfDocumentIsEdge(doc.value().view())) {
      return false;
    }
  }

  mongocxx::pipeline pipeline;
  auto docs = coll->aggregate(pipeline.sample(1000));
  for (auto doc : docs) {
    if (!CheckIfDocumentIsEdge(doc)) {
      return false;
    }
  }
  return true;
}

void ExtractDocumentFields(const bsoncxx::document::view& doc,
                           CollectionFields* fields, const std::string& prefix,
                           const std::string& parent_name) {
  for (auto elt : doc) {
    auto name = elt.key().data();
    if (name == std::string("_id")) {
      continue;
    }
    if (elt.type() == bsoncxx::type::k_oid) {
      fields->embedded_relations.insert(parent_name + "_" + name);
      continue;
    }
    auto elt_name = prefix + name;
    if (fields->property_fields.find(elt_name) ==
        fields->property_fields.end()) {
      auto elt_key = ProcessElement(elt, elt_name);
      if (elt_key.type != ImportDataType::kUnsupported) {
        fields->property_fields.insert(
            std::pair<std::string, PropertyKey>(elt_name, elt_key));
      } else {
        if (elt.type() == bsoncxx::type::k_array) {
          auto arr = elt.get_array().value;
          if (arr.length() > 0) {
            if (arr[0].type() == bsoncxx::type::k_oid) {
              fields->embedded_relations.insert(name);
            } else if (arr[0].type() == bsoncxx::type::k_document) {
              fields->embedded_nodes.insert(name);
              fields->embedded_relations.insert(name);
            }
          }
        }
      }
    }
    if (elt.type() == bsoncxx::type::k_document) {
      auto new_prefix = elt_name + ".";
      ExtractDocumentFields(elt.get_document(), fields, new_prefix,
                            elt.key().data());
    }
  }
}

void ExtractCollectionFields(mongocxx::collection* coll,
                             CollectionFields* fields,
                             const std::string& coll_name) {
  {
    auto doc = coll->find_one({});
    if (!doc) {
      // empty collection so skip it
      return;
    }
    ExtractDocumentFields(doc.value().view(), fields, std::string(""),
                          coll_name);
  }

  mongocxx::pipeline pipeline;
  auto docs = coll->aggregate(pipeline.sample(1000));
  for (auto doc : docs) {
    ExtractDocumentFields(doc, fields, std::string(""), coll_name);
  }
}

std::vector<std::string>
GetUserInputForEdges(const std::vector<std::string>& possible_edges,
                     std::vector<std::string>* nodes) {
  std::vector<std::string> edges;

  for (const std::string& coll_name : possible_edges) {
    bool done = false;
    while (!done) {
      std::cout << "Treat " << coll_name << " as an edge (y/n): ";
      std::string res;
      std::getline(std::cin, res);

      if (res.empty()) {
        std::cout << "Please enter yes or no\n";
      } else if (res[0] == 'y' || res[0] == 'Y') {
        edges.push_back(coll_name);
        done = true;
      } else if (res[0] == 'n' || res[0] == 'N') {
        nodes->push_back(coll_name);
        done = true;
      } else {
        std::cout << "Please enter yes or no\n";
      }
    }
  }
  return edges;
}

// TODO support multiple labels per collection
template <typename T>
void GetUserInputForLabels(LabelsState* state, const T& coll_names) {
  for (const std::string& coll_name : coll_names) {
    std::cout << "Choose label for " << coll_name << " (" << coll_name << "): ";
    std::string res;
    std::getline(std::cin, res);

    std::string existing_key;
    if (res.empty()) {
      for (auto iter : state->keys) {
        if (state->schema[iter.second]->name() == coll_name) {
          existing_key = iter.first;
          break;
        }
      }
      if (existing_key.empty()) {
        AddFalseBuilder(
            std::pair<std::string, std::string>(coll_name, coll_name), state);
      }
    } else {
      for (auto iter : state->keys) {
        if (state->schema[iter.second]->name() == res) {
          existing_key = iter.first;
          break;
        }
      }
      if (existing_key.empty()) {
        AddFalseBuilder(std::pair<std::string, std::string>(coll_name, res),
                        state);
      }
    }
    if (!existing_key.empty()) {
      auto index = state->keys.find(existing_key)->second;
      state->keys.insert(std::pair<std::string, size_t>(coll_name, index));
    }
  }
}

void GetUserInputForFields(GraphState* builder, CollectionFields doc_fields,
                           bool for_node) {
  auto fields = doc_fields.property_fields;
  if (for_node) {
    std::cout << "Node Fields:\n";
  } else {
    std::cout << "Edge Fields:\n";
  }
  std::cout << "Total Detected Fields: " << fields.size() << std::endl;
  for (auto [name, key] : fields) {
    std::cout << "Choose property name for field " << name << " (" << name
              << "): ";
    std::string res;
    std::getline(std::cin, res);

    if (!res.empty()) {
      key.name = res;
    }

    bool done      = false;
    auto type_name = TypeName(key.type);
    while (!done) {
      std::cout << "Choose type for field " << name << " (" << type_name;
      if (key.is_list) {
        std::cout << " array";
      }
      std::cout << "): ";
      std::getline(std::cin, res);
      if (!res.empty()) {
        std::istringstream iss(res);
        std::vector<std::string> tokens{std::istream_iterator<std::string>{iss},
                                        std::istream_iterator<std::string>{}};
        if (tokens.size() <= 2) {
          auto new_type = ParseType(tokens[0]);
          if (new_type != ImportDataType::kUnsupported) {
            if (tokens.size() == 2) {
              if (new_type == ImportDataType::kStruct) {
                std::cout << "Arrays of structs are not supported\n";
              } else if (boost::to_lower_copy<std::string>(tokens[1]) ==
                         "array") {
                key.type    = new_type;
                key.is_list = true;
                done        = true;
              } else {
                std::cout
                    << "Second argument could not be recognized, to specify an "
                       "array use the format: \"double array\"\n";
              }
            } else {
              key.type    = new_type;
              key.is_list = false;
              done        = true;
            }
          } else {
            std::cout << "Inputted datatype could not be recognized, valid "
                         "datatypes:\n";
            std::cout << "\"string\", \"string array\"\n";
            std::cout << "\"int64\", \"int64 array\"\n";
            std::cout << "\"int32\", \"int32 array\"\n";
            std::cout << "\"double\", \"double array\"\n";
            std::cout << "\"float\", \"float array\"\n";
            std::cout << "\"bool\", \"bool array\"\n";
            std::cout << "\"timestamp\", \"timestamp array\"\n";
            std::cout << "\"struct\"\n";
          }
        } else {
          std::cout << "Too many arguments\n";
        }
      } else {
        done = true;
      }
    }
    if (for_node) {
      AddBuilder(&builder->node_properties, std::move(key));
    } else {
      AddBuilder(&builder->edge_properties, std::move(key));
    }
  }
}

} // end of unnamed namespace

/// ConvertGraphML converts a GraphML file into katana form
///
/// \param infilename path to source graphml file
/// \returns arrow tables of node properties/labels, edge properties/types, and
/// csr topology
galois::GraphComponents galois::ConvertGraphML(const std::string& infilename,
                                               size_t chunk_size) {
  xmlTextReaderPtr reader;
  int ret = 0;

  GraphState builder{};
  WriterProperties properties{GetNullArrays(chunk_size),
                              GetFalseArray(chunk_size), chunk_size};

  galois::setActiveThreads(1000);
  bool finishedGraph = false;
  std::cout << "Start converting GraphML file: " << infilename << std::endl;

  reader = xmlNewTextReaderFilename(infilename.c_str());
  if (reader != NULL) {
    ret = xmlTextReaderRead(reader);

    // procedure:
    // read in "key" xml nodes and add them to nodeKeys and edgeKeys
    // once we reach the first "graph" xml node we parse it using the above keys
    // once we have parsed the first "graph" xml node we exit
    while (ret == 1 && !finishedGraph) {
      xmlChar* name;
      name = xmlTextReaderName(reader);
      if (name == NULL) {
        name = xmlStrdup(BAD_CAST "--");
      }
      // if elt is an xml node
      if (xmlTextReaderNodeType(reader) == 1) {
        // if elt is a "key" xml node read it in
        if (xmlStrEqual(name, BAD_CAST "key")) {
          PropertyKey key = ProcessKey(reader);
          if (key.id.size() > 0 && key.id != std::string("label") &&
              key.id != std::string("IGNORE")) {
            if (key.for_node) {
              AddBuilder(&builder.node_properties, std::move(key));
            } else if (key.for_edge) {
              AddBuilder(&builder.edge_properties, std::move(key));
            }
          }
        } else if (xmlStrEqual(name, BAD_CAST "graph")) {
          std::cout << "Finished processing property headers" << std::endl;
          std::cout << "Node Properties declared: "
                    << builder.node_properties.keys.size() << std::endl;
          std::cout << "Edge Properties declared: "
                    << builder.edge_properties.keys.size() << std::endl;
          ProcessGraph(reader, &builder, &properties);
          finishedGraph = true;
        }
      }

      xmlFree(name);
      ret = xmlTextReaderRead(reader);
    }
    xmlFreeTextReader(reader);
    if (ret < 0) {
      GALOIS_LOG_FATAL("Failed to parse {}", infilename);
    }
  } else {
    GALOIS_LOG_FATAL("Unable to open {}", infilename);
  }

  std::cout << "Node Properties:" << std::endl;
  WriteNullStats(builder.node_properties.chunks, &properties, builder.nodes);
  std::cout << "Node Labels:" << std::endl;
  WriteFalseStats(builder.node_labels.chunks, &properties, builder.nodes);
  std::cout << "Edge Properties Pre:" << std::endl;
  WriteNullStats(builder.edge_properties.chunks, &properties, builder.edges);
  std::cout << "Edge Types Pre:" << std::endl;
  WriteFalseStats(builder.edge_types.chunks, &properties, builder.edges);

  // build final nodes
  auto final_node_table = BuildTable(&builder.node_properties.chunks,
                                     &builder.node_properties.schema);
  auto final_label_table =
      BuildTable(&builder.node_labels.chunks, &builder.node_labels.schema);

  std::cout << "Finished building nodes" << std::endl;

  // rearrange edges to match implicit edge IDs
  auto edgesTables = BuildFinalEdges(&builder, &properties);
  std::shared_ptr<arrow::Table> final_edge_table = edgesTables.first;
  std::shared_ptr<arrow::Table> final_type_table = edgesTables.second;

  std::cout << "Finished topology and ordering edges" << std::endl;

  // build topology
  auto topology = std::make_shared<galois::graphs::GraphTopology>();
  arrow::Status st;
  std::shared_ptr<arrow::UInt64Builder> topology_indices_builder =
      std::make_shared<arrow::UInt64Builder>();
  st = topology_indices_builder->AppendValues(
      builder.topology_builder.out_indices);
  if (!st.ok()) {
    GALOIS_LOG_FATAL("Error building topology: {}", st.ToString());
  }
  std::shared_ptr<arrow::UInt32Builder> topology_dests_builder =
      std::make_shared<arrow::UInt32Builder>();
  st = topology_dests_builder->AppendValues(builder.topology_builder.out_dests);
  if (!st.ok()) {
    GALOIS_LOG_FATAL("Error building topology: {}", st.ToString());
  }

  st = topology_indices_builder->Finish(&topology->out_indices);
  if (!st.ok()) {
    GALOIS_LOG_FATAL("Error building arrow array for topology: {}",
                     st.ToString());
  }
  st = topology_dests_builder->Finish(&topology->out_dests);
  if (!st.ok()) {
    GALOIS_LOG_FATAL("Error building arrow array for topology: {}",
                     st.ToString());
  }

  std::cout << "Finished graphml conversion to arrow" << std::endl;
  std::cout << "Nodes: " << topology->out_indices->length() << std::endl;
  std::cout << "Node Properties: " << final_node_table->num_columns()
            << std::endl;
  std::cout << "Node Labels: " << final_label_table->num_columns() << std::endl;
  std::cout << "Edges: " << topology->out_dests->length() << std::endl;
  std::cout << "Edge Properties: " << final_edge_table->num_columns()
            << std::endl;
  std::cout << "Edge Types: " << final_type_table->num_columns() << std::endl;

  return GraphComponents{final_node_table, final_label_table, final_edge_table,
                         final_type_table, topology};
}

GraphComponents ConvertMongoDB(const std::string& db_name, size_t chunk_size) {
  // establich mongodb connection
  mongocxx::instance inst{};
  mongocxx::client conn{mongocxx::uri{}};

  mongocxx::database db               = conn[db_name];
  std::vector<std::string> coll_names = db.list_collection_names({});

  GraphState builder{};
  WriterProperties properties{GetNullArrays(chunk_size),
                              GetFalseArray(chunk_size), chunk_size};

  galois::setActiveThreads(1000);

  std::vector<std::string> possible_edges;
  std::vector<std::string> nodes;

  // iterate over all collections in db, find edge and node collections
  for (std::string coll_name : coll_names) {
    mongocxx::collection coll = db[coll_name];

    auto doc = coll.find_one({});
    if (!doc) {
      // empty collection so skip it
      continue;
    }
    if (CheckIfCollectionIsEdge(&coll)) {
      possible_edges.push_back(coll_name);
    } else {
      nodes.push_back(coll_name);
    }
  }
  auto edges = GetUserInputForEdges(possible_edges, &nodes);

  CollectionFields node_fields;
  CollectionFields edge_fields;

  // iterate over all collections in db, find as many fields as possible
  for (std::string coll_name : nodes) {
    mongocxx::collection coll = db[coll_name];
    ExtractCollectionFields(&coll, &node_fields, coll_name);
  }
  for (std::string coll_name : edges) {
    mongocxx::collection coll = db[coll_name];
    ExtractCollectionFields(&coll, &edge_fields, coll_name);
  }

  std::cout << "Nodes: " << nodes.size() << std::endl;
  GetUserInputForLabels(&builder.node_labels, nodes);
  std::cout << "Embedded Nodes: " << node_fields.embedded_nodes.size()
            << std::endl;
  GetUserInputForLabels(&builder.node_labels, node_fields.embedded_nodes);
  std::cout << "Edges: " << edges.size() << std::endl;
  GetUserInputForLabels(&builder.edge_types, edges);
  std::cout << "Embedded Edges: " << edge_fields.embedded_relations.size()
            << std::endl;
  GetUserInputForLabels(&builder.edge_types, node_fields.embedded_relations);

  GetUserInputForFields(&builder, std::move(node_fields), true);
  GetUserInputForFields(&builder, std::move(edge_fields), false);

  // add all edges first
  for (auto coll_name : edges) {
    mongocxx::collection coll = db[coll_name];

    mongocxx::cursor doc_cursor = coll.find({});
    // iterate over each document in collection
    for (bsoncxx::document::view doc : doc_cursor) {
      HandleEdgeDocumentMongoDB(&builder, &properties, doc, coll_name);
    }
  }
  // then add all nodes
  for (auto coll_name : nodes) {
    mongocxx::collection coll = db[coll_name];

    mongocxx::cursor doc_cursor = coll.find({});
    // iterate over each document in collection
    for (bsoncxx::document::view doc : doc_cursor) {
      HandleNodeDocumentMongoDB(&builder, &properties, doc, coll_name);
    }
  }
  builder.topology_builder.out_dests.resize(
      builder.edges, std::numeric_limits<uint32_t>::max());

  // fix intermediate destinations
  ResolveIntermediateIDs(&builder);

  // add buffered rows and even out columns
  EvenOutChunkBuilders(&builder.node_properties.builders,
                       &builder.node_properties.chunks, &properties,
                       builder.nodes);
  EvenOutChunkBuilders(&builder.node_labels.builders,
                       &builder.node_labels.chunks, &properties, builder.nodes);
  EvenOutChunkBuilders(&builder.edge_properties.builders,
                       &builder.edge_properties.chunks, &properties,
                       builder.edges);
  EvenOutChunkBuilders(&builder.edge_types.builders, &builder.edge_types.chunks,
                       &properties, builder.edges);

  // build final nodes
  auto final_node_table = BuildTable(&builder.node_properties.chunks,
                                     &builder.node_properties.schema);
  auto final_label_table =
      BuildTable(&builder.node_labels.chunks, &builder.node_labels.schema);

  std::cout << "Finished building nodes" << std::endl;

  // rearrange edges to match implicit edge IDs
  auto edges_tables = BuildFinalEdges(&builder, &properties);
  std::shared_ptr<arrow::Table> final_edge_table = edges_tables.first;
  std::shared_ptr<arrow::Table> final_type_table = edges_tables.second;

  std::cout << "Finished topology and ordering edges" << std::endl;

  // build topology
  auto topology = std::make_shared<galois::graphs::GraphTopology>();
  arrow::Status st;
  std::shared_ptr<arrow::UInt64Builder> topologyIndicesBuilder =
      std::make_shared<arrow::UInt64Builder>();
  st = topologyIndicesBuilder->AppendValues(
      builder.topology_builder.out_indices);
  if (!st.ok()) {
    GALOIS_LOG_FATAL("Error building topology");
  }
  std::shared_ptr<arrow::UInt32Builder> topologyDestsBuilder =
      std::make_shared<arrow::UInt32Builder>();
  st = topologyDestsBuilder->AppendValues(builder.topology_builder.out_dests);
  if (!st.ok()) {
    GALOIS_LOG_FATAL("Error building topology");
  }

  st = topologyIndicesBuilder->Finish(&topology->out_indices);
  if (!st.ok()) {
    GALOIS_LOG_FATAL("Error building arrow array for topology");
  }
  st = topologyDestsBuilder->Finish(&topology->out_dests);
  if (!st.ok()) {
    GALOIS_LOG_FATAL("Error building arrow array for topology");
  }

  std::cout << final_node_table->ToString() << std::endl;
  std::cout << final_label_table->ToString() << std::endl;
  std::cout << final_edge_table->ToString() << std::endl;
  std::cout << final_type_table->ToString() << std::endl;
  std::cout << topology->out_indices->ToString() << std::endl;
  std::cout << topology->out_dests->ToString() << std::endl;

  std::cout << "Finished mongodb conversion to arrow" << std::endl;
  std::cout << "Nodes: " << topology->out_indices->length() << std::endl;
  std::cout << "Node Properties: " << final_node_table->num_columns()
            << std::endl;
  std::cout << "Node Labels: " << final_label_table->num_columns() << std::endl;
  std::cout << "Edges: " << topology->out_dests->length() << std::endl;
  std::cout << "Edge Properties: " << final_edge_table->num_columns()
            << std::endl;
  std::cout << "Edge Types: " << final_type_table->num_columns() << std::endl;

  return GraphComponents{final_node_table, final_label_table, final_edge_table,
                         final_type_table, topology};
}

/// convertToPropertyGraphAndWrite formally builds katana form via
/// PropertyFileGraph from imported components and writes the result to target
/// directory
///
/// \param graph_comps imported components to convert into a PropertyFileGraph
/// \param dir local FS directory or s3 directory to write PropertyFileGraph to
void galois::WritePropertyGraph(const galois::GraphComponents& graph_comps,
                                const std::string& dir) {
  galois::graphs::PropertyFileGraph graph;

  auto result = graph.SetTopology(*graph_comps.topology);
  if (!result) {
    GALOIS_LOG_FATAL("Error adding topology: {}", result.error());
  }

  if (graph_comps.node_properties->num_columns() > 0) {
    result = graph.AddNodeProperties(graph_comps.node_properties);
    if (!result) {
      GALOIS_LOG_FATAL("Error adding node properties: {}", result.error());
    }
  }
  if (graph_comps.node_labels->num_columns() > 0) {
    result = graph.AddNodeProperties(graph_comps.node_labels);
    if (!result) {
      GALOIS_LOG_FATAL("Error adding node labels: {}", result.error());
    }
  }
  if (graph_comps.edge_properties->num_columns() > 0) {
    result = graph.AddEdgeProperties(graph_comps.edge_properties);
    if (!result) {
      GALOIS_LOG_FATAL("Error adding edge properties: {}", result.error());
    }
  }
  if (graph_comps.edge_types->num_columns() > 0) {
    result = graph.AddEdgeProperties(graph_comps.edge_types);
    if (!result) {
      GALOIS_LOG_FATAL("Error adding edge types: {}", result.error());
    }
  }

  WritePropertyGraph(std::move(graph), dir);
}

void galois::WritePropertyGraph(galois::graphs::PropertyFileGraph prop_graph,
                                const std::string& dir) {
  std::string meta_file = dir;
  if (meta_file[meta_file.length() - 1] == '/') {
    meta_file += "meta";
  } else {
    meta_file += "/meta";
  }
  auto result = prop_graph.Write(meta_file);
  if (!result) {
    GALOIS_LOG_FATAL("Error writing to fs: {}", result.error());
  }
}
