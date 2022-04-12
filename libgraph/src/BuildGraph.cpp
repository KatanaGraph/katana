#include "katana/BuildGraph.h"

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

#include <arrow/api.h>
#include <arrow/array.h>
#include <arrow/io/api.h>
#include <boost/algorithm/string.hpp>
#include <boost/foreach.hpp>
#include <boost/lexical_cast.hpp>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#include "katana/ArrowInterchange.h"
#include "katana/ErrorCode.h"
#include "katana/Galois.h"
#include "katana/Logging.h"
#include "katana/ParallelSTL.h"
#include "katana/PropertyGraph.h"
#include "katana/SharedMemSys.h"
#include "katana/Threads.h"

using ArrayBuilders = std::vector<std::shared_ptr<arrow::ArrayBuilder>>;
using BooleanBuilders = std::vector<std::shared_ptr<arrow::BooleanBuilder>>;
using ChunkedArrays = std::vector<std::shared_ptr<arrow::ChunkedArray>>;
using ArrowArrays = std::vector<std::shared_ptr<arrow::Array>>;
using ArrowFields = std::vector<std::shared_ptr<arrow::Field>>;
using NullMaps = std::pair<
    std::unordered_map<int, std::shared_ptr<arrow::Array>>,
    std::unordered_map<int, std::shared_ptr<arrow::Array>>>;

using katana::GraphComponents;
using katana::ImportData;
using katana::ImportDataType;
using katana::LabelRule;
using katana::LabelsState;
using katana::PropertiesState;
using katana::PropertyKey;
using katana::TopologyState;
using katana::WriterProperties;

namespace {

/************************************/
/* Basic Building Utility Functions */
/************************************/

template <typename T>
std::shared_ptr<arrow::Array>
BuildArray(std::shared_ptr<T> builder) {
  std::shared_ptr<arrow::Array> array;
  auto st = builder->Finish(&array);
  if (!st.ok()) {
    KATANA_LOG_FATAL("Error building arrow array: {}", st.ToString());
  }
  return array;
}

ChunkedArrays
BuildChunks(std::vector<ArrowArrays>* chunks) {
  ChunkedArrays chunked_arrays;
  chunked_arrays.resize(chunks->size());
  for (size_t n = 0; n < chunks->size(); n++) {
    chunked_arrays[n] = std::make_shared<arrow::ChunkedArray>(chunks->at(n));
  }
  return chunked_arrays;
}

std::shared_ptr<arrow::Table>
BuildTable(std::vector<ArrowArrays>* chunks, ArrowFields* schema_vector) {
  ChunkedArrays columns = BuildChunks(chunks);

  auto schema = std::make_shared<arrow::Schema>(*schema_vector);
  return arrow::Table::Make(schema, columns);
}

/********************************************************************/
/* Helper functions for building initial null arrow array constants */
/********************************************************************/

template <typename T>
void
AddNullArrays(
    std::unordered_map<int, std::shared_ptr<arrow::Array>>* null_map,
    std::unordered_map<int, std::shared_ptr<arrow::Array>>* lists_null_map,
    size_t elts) {
  auto* pool = arrow::default_memory_pool();

  // the builder types are still added for the list types since the list type is
  // extraneous info
  auto builder = std::make_shared<T>();
  auto st = builder->AppendNulls(elts);
  null_map->insert(std::pair<int, std::shared_ptr<arrow::Array>>(
      builder->type()->id(), BuildArray(builder)));

  auto list_builder =
      std::make_shared<arrow::ListBuilder>(pool, std::make_shared<T>());
  st = list_builder->AppendNulls(elts);
  lists_null_map->insert(std::pair<int, std::shared_ptr<arrow::Array>>(
      builder->type()->id(), BuildArray(list_builder)));

  if (!st.ok()) {
    KATANA_LOG_FATAL("Error creating null builders: {}", st.ToString());
  }
}

// for Timestamp Types
void
AddNullArrays(
    std::unordered_map<int, std::shared_ptr<arrow::Array>>* null_map,
    std::unordered_map<int, std::shared_ptr<arrow::Array>>* lists_null_map,
    size_t elts, std::shared_ptr<arrow::DataType> type) {
  auto* pool = arrow::default_memory_pool();

  // the builder types are still added for the list types since the list type is
  // extraneous info
  auto builder = std::make_shared<arrow::TimestampBuilder>(type, pool);
  auto st = builder->AppendNulls(elts);
  null_map->insert(std::pair<int, std::shared_ptr<arrow::Array>>(
      builder->type()->id(), BuildArray(builder)));

  auto list_builder = std::make_shared<arrow::ListBuilder>(
      pool, std::make_shared<arrow::TimestampBuilder>(type, pool));
  st = list_builder->AppendNulls(elts);
  lists_null_map->insert(std::pair<int, std::shared_ptr<arrow::Array>>(
      builder->type()->id(), BuildArray(list_builder)));

  if (!st.ok()) {
    KATANA_LOG_FATAL("Error creating null builders: {}", st.ToString());
  }
}

NullMaps
GetNullArrays(size_t elts) {
  std::unordered_map<int, std::shared_ptr<arrow::Array>> null_map;
  std::unordered_map<int, std::shared_ptr<arrow::Array>> lists_null_map;

  AddNullArrays<arrow::StringBuilder>(&null_map, &lists_null_map, elts);
  AddNullArrays<arrow::Int32Builder>(&null_map, &lists_null_map, elts);
  AddNullArrays<arrow::Int64Builder>(&null_map, &lists_null_map, elts);
  AddNullArrays<arrow::FloatBuilder>(&null_map, &lists_null_map, elts);
  AddNullArrays<arrow::DoubleBuilder>(&null_map, &lists_null_map, elts);
  AddNullArrays<arrow::BooleanBuilder>(&null_map, &lists_null_map, elts);
  AddNullArrays<arrow::UInt8Builder>(&null_map, &lists_null_map, elts);
  AddNullArrays(
      &null_map, &lists_null_map, elts,
      arrow::timestamp(arrow::TimeUnit::NANO, "UTC"));

  return NullMaps(std::move(null_map), std::move(lists_null_map));
}

std::shared_ptr<arrow::Array>
GetFalseArray(size_t elts) {
  auto builder = std::make_shared<arrow::BooleanBuilder>();
  arrow::Status st;
  for (size_t i = 0; i < elts; i++) {
    st = builder->Append(false);
  }
  if (!st.ok()) {
    KATANA_LOG_FATAL(
        "Error appending to an arrow array builder: {}", st.ToString());
  }
  return BuildArray(builder);
}

WriterProperties
GetWriterProperties(size_t chunk_size) {
  return WriterProperties{
      GetNullArrays(chunk_size), GetFalseArray(chunk_size), chunk_size};
}

/*************************************************************/
/* Utility functions for retrieving null arrays from the map */
/*************************************************************/

template <typename T>
std::shared_ptr<arrow::Array>
FindNullArray(std::shared_ptr<T> builder, WriterProperties* properties) {
  auto type = builder->type()->id();
  std::shared_ptr<arrow::Array> null_array;
  if (type != arrow::Type::LIST) {
    null_array = properties->null_arrays.first.find(type)->second;
  } else {
    auto list_builder = std::static_pointer_cast<arrow::ListBuilder>(builder);
    null_array = properties->null_arrays.second
                     .find(list_builder->value_builder()->type()->id())
                     ->second;
  }
  return null_array;
}

std::shared_ptr<arrow::Array>
FindNullArray(
    std::shared_ptr<arrow::Array> array, WriterProperties* properties) {
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

void
WriteNullStats(
    const std::vector<ArrowArrays>& table, WriterProperties* properties,
    size_t total) {
  if (table.size() == 0) {
    std::cout << "This table has no entries\n";
    return;
  }
  size_t null_constants = 0;
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
  std::cout << "Total non-null Values in Table: " << non_null_values << "\n";
  std::cout << "Total Values in Table: " << total * table.size() << "\n";
  std::cout << "Value Ratio: "
            << ((double)non_null_values) / (total * table.size()) << "\n";
  std::cout << "Total Null Chunks in table " << null_constants << "\n";
  std::cout << "Total Chunks in Table: " << table[0].size() * table.size()
            << "\n";
  std::cout << "Constant Ratio: "
            << ((double)null_constants) / (table[0].size() * table.size())
            << "\n";
  std::cout << "\n";
}

void
WriteFalseStats(
    const std::vector<ArrowArrays>& table, WriterProperties* properties,
    size_t total) {
  if (table.size() == 0) {
    std::cout << "This table has no entries\n";
    return;
  }
  size_t false_constants = 0;
  size_t true_values = 0;

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
  std::cout << "Total true Values in Table: " << true_values << "\n";
  std::cout << "Total Values in Table: " << total * table.size() << "\n";
  std::cout << "True Ratio: " << ((double)true_values) / (total * table.size())
            << "\n";
  std::cout << "Total False Chunks in table " << false_constants << "\n";
  std::cout << "Total Chunks in Table: " << table[0].size() * table.size()
            << "\n";
  std::cout << "Constant Ratio: "
            << ((double)false_constants) / (table[0].size() * table.size())
            << "\n";
  std::cout << "\n";
}

/************************************************/
/* Functions for adding values to arrow builder */
/************************************************/

// Append an array to a builder
void
AppendArray(
    std::shared_ptr<arrow::ListBuilder> list_builder,
    std::function<ImportData(ImportDataType, bool)> resolve_value) {
  arrow::Status st = arrow::Status::OK();
  bool is_list = true;

  switch (list_builder->value_builder()->type()->id()) {
  case arrow::Type::STRING: {
    auto sb = static_cast<arrow::StringBuilder*>(list_builder->value_builder());
    auto res = resolve_value(ImportDataType::kString, is_list);
    if (res.type != ImportDataType::kUnsupported) {
      st = list_builder->Append();
      st = sb->AppendValues(std::get<std::vector<std::string>>(res.value));
    }
    break;
  }
  case arrow::Type::INT64: {
    auto lb = static_cast<arrow::Int64Builder*>(list_builder->value_builder());
    auto res = resolve_value(ImportDataType::kInt64, is_list);
    if (res.type != ImportDataType::kUnsupported) {
      st = list_builder->Append();
      st = lb->AppendValues(std::get<std::vector<int64_t>>(res.value));
    }
    break;
  }
  case arrow::Type::INT32: {
    auto ib = static_cast<arrow::Int32Builder*>(list_builder->value_builder());
    auto res = resolve_value(ImportDataType::kInt32, is_list);
    if (res.type != ImportDataType::kUnsupported) {
      st = list_builder->Append();
      st = ib->AppendValues(std::get<std::vector<int32_t>>(res.value));
    }
    break;
  }
  case arrow::Type::DOUBLE: {
    auto db = static_cast<arrow::DoubleBuilder*>(list_builder->value_builder());
    auto res = resolve_value(ImportDataType::kDouble, is_list);
    if (res.type != ImportDataType::kUnsupported) {
      st = list_builder->Append();
      st = db->AppendValues(std::get<std::vector<double>>(res.value));
    }
    break;
  }
  case arrow::Type::FLOAT: {
    auto fb = static_cast<arrow::FloatBuilder*>(list_builder->value_builder());
    auto res = resolve_value(ImportDataType::kFloat, is_list);
    if (res.type != ImportDataType::kUnsupported) {
      st = list_builder->Append();
      st = fb->AppendValues(std::get<std::vector<float>>(res.value));
    }
    break;
  }
  case arrow::Type::BOOL: {
    auto bb =
        static_cast<arrow::BooleanBuilder*>(list_builder->value_builder());
    auto res = resolve_value(ImportDataType::kBoolean, is_list);
    if (res.type != ImportDataType::kUnsupported) {
      st = list_builder->Append();
      st = bb->AppendValues(std::get<std::vector<bool>>(res.value));
    }
    break;
  }
  case arrow::Type::TIMESTAMP: {
    auto tb =
        static_cast<arrow::TimestampBuilder*>(list_builder->value_builder());
    auto res = resolve_value(ImportDataType::kTimestampMilli, is_list);
    if (res.type != ImportDataType::kUnsupported) {
      st = list_builder->Append();
      st = tb->AppendValues(std::get<std::vector<int64_t>>(res.value));
    }
    break;
  }
  default: {
    break;
  }
  }
  if (!st.ok()) {
    KATANA_LOG_FATAL(
        "Error adding value to arrow list array builder: {}", st.ToString());
  }
}

// Append a non-null value to an array
void
AppendValue(
    std::shared_ptr<arrow::ArrayBuilder> array,
    std::function<ImportData(ImportDataType, bool)> resolve_value) {
  arrow::Status st = arrow::Status::OK();
  bool is_list = false;

  switch (array->type()->id()) {
  case arrow::Type::STRING: {
    auto sb = std::static_pointer_cast<arrow::StringBuilder>(array);
    auto res = resolve_value(ImportDataType::kString, is_list);
    if (res.type != ImportDataType::kUnsupported) {
      st = sb->Append(std::get<std::string>(res.value));
    }
    break;
  }
  case arrow::Type::INT64: {
    auto lb = std::static_pointer_cast<arrow::Int64Builder>(array);
    auto res = resolve_value(ImportDataType::kInt64, is_list);
    if (res.type != ImportDataType::kUnsupported) {
      st = lb->Append(std::get<int64_t>(res.value));
    }
    break;
  }
  case arrow::Type::INT32: {
    auto ib = std::static_pointer_cast<arrow::Int32Builder>(array);
    auto res = resolve_value(ImportDataType::kInt32, is_list);
    if (res.type != ImportDataType::kUnsupported) {
      st = ib->Append(std::get<int32_t>(res.value));
    }
    break;
  }
  case arrow::Type::DOUBLE: {
    auto db = std::static_pointer_cast<arrow::DoubleBuilder>(array);
    auto res = resolve_value(ImportDataType::kDouble, is_list);
    if (res.type != ImportDataType::kUnsupported) {
      st = db->Append(std::get<double>(res.value));
    }
    break;
  }
  case arrow::Type::FLOAT: {
    auto fb = std::static_pointer_cast<arrow::FloatBuilder>(array);
    auto res = resolve_value(ImportDataType::kFloat, is_list);
    if (res.type != ImportDataType::kUnsupported) {
      st = fb->Append(std::get<float>(res.value));
    }
    break;
  }
  case arrow::Type::BOOL: {
    auto bb = std::static_pointer_cast<arrow::BooleanBuilder>(array);
    auto res = resolve_value(ImportDataType::kBoolean, is_list);
    if (res.type != ImportDataType::kUnsupported) {
      st = bb->Append(std::get<bool>(res.value));
    }
    break;
  }
  case arrow::Type::TIMESTAMP: {
    auto tb = std::static_pointer_cast<arrow::TimestampBuilder>(array);
    auto res = resolve_value(ImportDataType::kTimestampMilli, is_list);
    if (res.type != ImportDataType::kUnsupported) {
      st = tb->Append(std::get<int64_t>(res.value));
    }
    break;
  }
  // for now uint8_t is an alias for a struct
  case arrow::Type::UINT8: {
    auto bb = std::static_pointer_cast<arrow::UInt8Builder>(array);
    auto res = resolve_value(ImportDataType::kStruct, is_list);
    if (res.type != ImportDataType::kUnsupported) {
      st = bb->Append(std::get<uint8_t>(res.value));
    }
    break;
  }
  case arrow::Type::LIST: {
    auto lb = std::static_pointer_cast<arrow::ListBuilder>(array);
    AppendArray(lb, resolve_value);
    break;
  }
  default: {
    break;
  }
  }
  if (!st.ok()) {
    KATANA_LOG_FATAL(
        "Error adding value to arrow array builder, parquet error: {}",
        st.ToString());
  }
}

// Adds nulls to an array being built until its length == total
template <typename T>
void
AddNulls(
    std::shared_ptr<T> builder, ArrowArrays* chunks,
    std::shared_ptr<arrow::Array> null_array, WriterProperties* properties,
    size_t total) {
  auto chunk_size = properties->chunk_size;
  auto nulls_needed = total - (chunks->size() * chunk_size) - builder->length();
  arrow::Status st;

  // simplest case, no falses needed
  if (nulls_needed == 0) {
    return;
  }

  // case where nulls needed but mid-array
  if (builder->length() != 0) {
    auto nulls_to_add = std::min(chunk_size - builder->length(), nulls_needed);
    st = builder->AppendNulls(nulls_to_add);
    nulls_needed -= nulls_to_add;

    // if we filled up a chunk, flush it
    if (static_cast<size_t>(builder->length()) == chunk_size) {
      chunks->emplace_back(BuildArray(builder));
    } else {
      // if we did not fill up the array we know it must need no more nulls
      return;
    }
  }

  // case where we are at the start of a new array and have null_arrays to add
  for (auto i = chunk_size; i <= nulls_needed; i += chunk_size) {
    chunks->emplace_back(null_array);
  }
  nulls_needed %= chunk_size;

  // case where we are at the start of a new array and have less than a
  // null_array to add
  st = builder->AppendNulls(nulls_needed);

  if (!st.ok()) {
    KATANA_LOG_FATAL(
        "Error appending to an arrow array builder: {}", st.ToString());
  }
}

// Adds nulls to an array being built until its length == total
template <typename T>
void
AddNulls(
    std::shared_ptr<T> builder, ArrowArrays* chunks,
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
void
AddFalses(
    std::shared_ptr<arrow::BooleanBuilder> builder, ArrowArrays* chunks,
    WriterProperties* properties, size_t total) {
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
    if (static_cast<size_t>(builder->length()) == chunk_size) {
      chunks->emplace_back(BuildArray(builder));
    } else {
      // if we did not fill up the array we know it must need no more falses
      return;
    }
  }

  // case where we are at the start of a new array and have false_arrays to add
  for (auto i = chunk_size; i <= falses_needed; i += chunk_size) {
    chunks->emplace_back(properties->false_array);
  }
  falses_needed %= chunk_size;

  // case where we are at the start of a new array and have less than a
  // false_array to add
  for (size_t i = 0; i < falses_needed; i++) {
    st = builder->Append(false);
  }

  if (!st.ok()) {
    KATANA_LOG_FATAL(
        "Error appending to an arrow array builder: {}", st.ToString());
  }
}

// Add nulls until the array is even and then append val so that length = total
// + 1 at the end
template <typename T, typename W>
void
AddTypedValue(
    const W& val, std::shared_ptr<T> builder, ArrowArrays* chunks,
    std::shared_ptr<arrow::Array> null_array, WriterProperties* properties,
    size_t total) {
  AddNulls(builder, chunks, null_array, properties, total);
  auto st = builder->Append(val);
  if (!st.ok()) {
    KATANA_LOG_FATAL(
        "Error appending to an arrow array builder: {}", st.ToString());
  }

  // if we filled up a chunk, flush it
  if (static_cast<size_t>(builder->length()) == properties->chunk_size) {
    chunks->emplace_back(BuildArray(builder));
  }
}

// Add nulls until the array is even and then append a list so that length =
// total + 1 at the end
template <typename T, typename W>
void
AddArray(
    const std::shared_ptr<arrow::ListArray>& list_vals,
    const std::shared_ptr<W>& vals, size_t index,
    std::shared_ptr<arrow::ListBuilder> list_builder, T* type_builder,
    ArrowArrays* chunks, std::shared_ptr<arrow::Array> null_array,
    WriterProperties* properties, size_t total) {
  AddNulls(list_builder, chunks, null_array, properties, total);

  int32_t start = list_vals->value_offset(index);
  int32_t end = list_vals->value_offset(index + 1);

  auto st = list_builder->Append();
  for (int32_t s = start; s < end; s++) {
    st = type_builder->Append(vals->Value(s));
    if (!st.ok()) {
      KATANA_LOG_FATAL(
          "Error appending value to an arrow array builder: {}", st.ToString());
    }
  }
  // if we filled up a chunk, flush it
  if (static_cast<size_t>(list_builder->length()) == properties->chunk_size) {
    chunks->emplace_back(BuildArray(list_builder));
  }
}

// Add nulls until the array is even and then append a list so that length =
// total + 1 at the end
void
AddArray(
    const std::shared_ptr<arrow::ListArray>& list_vals,
    const std::shared_ptr<arrow::StringArray>& vals, size_t index,
    std::shared_ptr<arrow::ListBuilder> list_builder,
    arrow::StringBuilder* type_builder, ArrowArrays* chunks,
    std::shared_ptr<arrow::Array> null_array, WriterProperties* properties,
    size_t total) {
  AddNulls(list_builder, chunks, null_array, properties, total);

  int32_t start = list_vals->value_offset(index);
  int32_t end = list_vals->value_offset(index + 1);

  auto st = list_builder->Append();
  for (int32_t s = start; s < end; s++) {
    st = type_builder->Append(vals->GetView(s));
    if (!st.ok()) {
      KATANA_LOG_FATAL(
          "Error appending value to an arrow array builder: {}", st.ToString());
    }
  }
  // if we filled up a chunk, flush it
  if (static_cast<size_t>(list_builder->length()) == properties->chunk_size) {
    chunks->emplace_back(BuildArray(list_builder));
  }
}

// Add nulls until the array is even and then append val so that length = total
// + 1 at the end
void
AddValueInternal(
    std::shared_ptr<arrow::ArrayBuilder> builder, ArrowArrays* chunks,
    WriterProperties* properties, size_t total,
    std::function<ImportData(ImportDataType, bool)> resolve_value) {
  AddNulls(builder, chunks, properties, total);
  AppendValue(builder, resolve_value);

  // if we filled up a chunk, flush it
  if (static_cast<size_t>(builder->length()) == properties->chunk_size) {
    chunks->emplace_back(BuildArray(builder));
  }
}

// Add falses until the array is even and then append true so that length =
// total + 1 at the end
void
AddLabelInternal(
    std::shared_ptr<arrow::BooleanBuilder> builder, ArrowArrays* chunks,
    WriterProperties* properties, size_t total) {
  AddFalses(builder, chunks, properties, total);
  auto st = builder->Append(true);
  if (!st.ok()) {
    KATANA_LOG_FATAL(
        "Error appending to an arrow array builder: {}", st.ToString());
  }

  // if we filled up a chunk, flush it
  if (static_cast<size_t>(builder->length()) == properties->chunk_size) {
    chunks->emplace_back(BuildArray(builder));
  }
}

/***********************************/
/* Functions for handling topology */
/***********************************/

// Used to build the out_dests component of the CSR representation
uint64_t
SetEdgeID(
    TopologyState* topology_builder, std::vector<uint64_t>* offsets,
    size_t index) {
  uint32_t src = topology_builder->sources[index];
  uint64_t base = src ? topology_builder->out_indices[src - 1] : 0;
  uint64_t i = base + offsets->at(src)++;

  topology_builder->out_dests[i] = topology_builder->destinations[index];
  return i;
}

/******************************************************************************/
/* Functions for ensuring all arrow arrays are of the right length in the end */
/******************************************************************************/

// Adds nulls to the array until its length == total
template <typename T>
void
EvenOutArray(
    ArrowArrays* chunks, std::shared_ptr<T> builder,
    std::shared_ptr<arrow::Array> null_array, WriterProperties* properties,
    size_t total) {
  AddNulls(builder, chunks, null_array, properties, total);

  if (total % properties->chunk_size != 0) {
    chunks->emplace_back(BuildArray(builder));
  }
}

// Adds falses to the array until its length == total
void
EvenOutArray(
    ArrowArrays* chunks, std::shared_ptr<arrow::BooleanBuilder> builder,
    WriterProperties* properties, size_t total) {
  AddFalses(builder, chunks, properties, total);

  if (total % properties->chunk_size != 0) {
    chunks->emplace_back(BuildArray(builder));
  }
}

// Adds nulls to the arrays until each length == total
void
EvenOutChunkBuilders(
    ArrayBuilders* builders, std::vector<ArrowArrays>* chunks,
    WriterProperties* properties, size_t total) {
  katana::do_all(
      katana::iterate(static_cast<size_t>(0), builders->size()),
      [&](const size_t& i) {
        AddNulls(builders->at(i), &chunks->at(i), properties, total);

        if (total % properties->chunk_size != 0) {
          chunks->at(i).emplace_back(BuildArray(builders->at(i)));
        }
      });
}

// Adds falses to the arrays until each length == total
void
EvenOutChunkBuilders(
    BooleanBuilders* builders, std::vector<ArrowArrays>* chunks,
    WriterProperties* properties, size_t total) {
  katana::do_all(
      katana::iterate(static_cast<size_t>(0), builders->size()),
      [&](const size_t& i) {
        AddFalses(builders->at(i), &chunks->at(i), properties, total);

        if (total % properties->chunk_size != 0) {
          chunks->at(i).emplace_back(BuildArray(builders->at(i)));
        }
      });
}

/**************************************************/
/* Functions for reordering edges into CSR format */
/**************************************************/

// Rearrange an array's entries to match up with those of mapping
template <typename T, typename W>
ArrowArrays
RearrangeArray(
    std::shared_ptr<T> builder,
    const std::shared_ptr<arrow::ChunkedArray>& chunked_array,
    const std::vector<size_t>& mapping, WriterProperties* properties) {
  auto chunk_size = properties->chunk_size;
  ArrowArrays chunks;
  auto st = builder->Reserve(chunk_size);
  if (!st.ok()) {
    KATANA_LOG_FATAL(
        "Error reserving space for arrow array: {}", st.ToString());
  }
  // cast and store array chunks for use in loop
  std::vector<std::shared_ptr<W>> arrays;
  for (auto chunk : chunked_array->chunks()) {
    arrays.emplace_back(std::static_pointer_cast<W>(chunk));
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
RearrangeArray(
    std::shared_ptr<arrow::StringBuilder> builder,
    const std::shared_ptr<arrow::ChunkedArray>& chunked_array,
    const std::vector<size_t>& mapping, WriterProperties* properties) {
  auto chunk_size = properties->chunk_size;
  ArrowArrays chunks;
  auto st = builder->Reserve(chunk_size);
  if (!st.ok()) {
    KATANA_LOG_FATAL(
        "Error reserving space for arrow array: {}", st.ToString());
  }
  // cast and store array chunks for use in loop
  std::vector<std::shared_ptr<arrow::StringArray>> arrays;
  for (auto chunk : chunked_array->chunks()) {
    arrays.emplace_back(std::static_pointer_cast<arrow::StringArray>(chunk));
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
RearrangeArray(
    std::shared_ptr<arrow::BooleanBuilder> builder,
    const std::shared_ptr<arrow::ChunkedArray>& chunked_array,
    const std::vector<size_t>& mapping, WriterProperties* properties) {
  auto chunk_size = properties->chunk_size;
  ArrowArrays chunks;
  auto st = builder->Reserve(chunk_size);
  if (!st.ok()) {
    KATANA_LOG_FATAL(
        "Error reserving space for arrow array: {}", st.ToString());
  }
  // cast and store array chunks for use in loop
  std::vector<std::shared_ptr<arrow::BooleanArray>> arrays;
  for (auto chunk : chunked_array->chunks()) {
    arrays.emplace_back(std::static_pointer_cast<arrow::BooleanArray>(chunk));
  }

  // add non-null values
  for (size_t i = 0; i < mapping.size(); i++) {
    auto val = arrays[mapping[i] / chunk_size]->Value(mapping[i] % chunk_size);
    if (val) {
      AddLabelInternal(builder, &chunks, properties, i);
    }
  }
  EvenOutArray(&chunks, builder, properties, mapping.size());
  return chunks;
}

// Rearrange a list array's entries to match up with those of mapping
template <typename T, typename W>
ArrowArrays
RearrangeArray(
    const std::shared_ptr<arrow::ListBuilder>& builder, T* type_builder,
    const std::shared_ptr<arrow::ChunkedArray>& chunked_array,
    const std::vector<size_t>& mapping, WriterProperties* properties) {
  auto chunk_size = properties->chunk_size;
  ArrowArrays chunks;
  auto st = builder->Reserve(chunk_size);
  if (!st.ok()) {
    KATANA_LOG_FATAL(
        "Error reserving space for arrow array: {}", st.ToString());
  }
  // cast and store array chunks for use in loop
  std::vector<std::shared_ptr<arrow::ListArray>> list_arrays;
  std::vector<std::shared_ptr<W>> sub_arrays;
  for (auto chunk : chunked_array->chunks()) {
    auto array_temp = std::static_pointer_cast<arrow::ListArray>(chunk);
    list_arrays.emplace_back(array_temp);
    sub_arrays.emplace_back(std::static_pointer_cast<W>(array_temp->values()));
  }
  std::shared_ptr<arrow::Array> null_array =
      properties->null_arrays.second.find(type_builder->type()->id())->second;

  // add values
  for (size_t i = 0; i < mapping.size(); i++) {
    auto list_array = list_arrays[mapping[i] / chunk_size];
    auto sub_array = sub_arrays[mapping[i] / chunk_size];
    auto index = mapping[i] % chunk_size;
    if (!list_array->IsNull(index)) {
      AddArray(
          list_array, sub_array, index, builder, type_builder, &chunks,
          null_array, properties, i);
    }
  }
  EvenOutArray(&chunks, builder, null_array, properties, mapping.size());
  return chunks;
}

// Rearrange a list array's entries to match up with those of mapping
ArrowArrays
RearrangeListArray(
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
    chunks = RearrangeArray<arrow::StringBuilder, arrow::StringArray>(
        builder, sb, list_chunked_array, mapping, properties);
    break;
  }
  case arrow::Type::INT64: {
    auto builder = std::make_shared<arrow::ListBuilder>(
        pool, std::make_shared<arrow::Int64Builder>());
    auto lb = static_cast<arrow::Int64Builder*>(builder->value_builder());
    chunks = RearrangeArray<arrow::Int64Builder, arrow::Int64Array>(
        builder, lb, list_chunked_array, mapping, properties);
    break;
  }
  case arrow::Type::INT32: {
    auto builder = std::make_shared<arrow::ListBuilder>(
        pool, std::make_shared<arrow::Int32Builder>());
    auto ib = static_cast<arrow::Int32Builder*>(builder->value_builder());
    chunks = RearrangeArray<arrow::Int32Builder, arrow::Int32Array>(
        builder, ib, list_chunked_array, mapping, properties);
    break;
  }
  case arrow::Type::DOUBLE: {
    auto builder = std::make_shared<arrow::ListBuilder>(
        pool, std::make_shared<arrow::DoubleBuilder>());
    auto db = static_cast<arrow::DoubleBuilder*>(builder->value_builder());
    chunks = RearrangeArray<arrow::DoubleBuilder, arrow::DoubleArray>(
        builder, db, list_chunked_array, mapping, properties);
    break;
  }
  case arrow::Type::FLOAT: {
    auto builder = std::make_shared<arrow::ListBuilder>(
        pool, std::make_shared<arrow::FloatBuilder>());
    auto fb = static_cast<arrow::FloatBuilder*>(builder->value_builder());
    chunks = RearrangeArray<arrow::FloatBuilder, arrow::FloatArray>(
        builder, fb, list_chunked_array, mapping, properties);
    break;
  }
  case arrow::Type::BOOL: {
    auto builder = std::make_shared<arrow::ListBuilder>(
        pool, std::make_shared<arrow::BooleanBuilder>());
    auto bb = static_cast<arrow::BooleanBuilder*>(builder->value_builder());
    chunks = RearrangeArray<arrow::BooleanBuilder, arrow::BooleanArray>(
        builder, bb, list_chunked_array, mapping, properties);
    break;
  }
  case arrow::Type::TIMESTAMP: {
    auto builder = std::make_shared<arrow::ListBuilder>(
        pool, std::make_shared<arrow::TimestampBuilder>(list_type, pool));
    auto tb = static_cast<arrow::TimestampBuilder*>(builder->value_builder());
    chunks = RearrangeArray<arrow::TimestampBuilder, arrow::TimestampArray>(
        builder, tb, list_chunked_array, mapping, properties);
    break;
  }
  case arrow::Type::UINT8: {
    auto builder = std::make_shared<arrow::ListBuilder>(
        pool, std::make_shared<arrow::UInt8Builder>());
    auto bb = static_cast<arrow::UInt8Builder*>(builder->value_builder());
    chunks = RearrangeArray<arrow::UInt8Builder, arrow::UInt8Array>(
        builder, bb, list_chunked_array, mapping, properties);
    break;
  }
  default: {
    KATANA_LOG_FATAL(
        "Unsupported arrow array type passed to RearrangeListArray: {}",
        list_type);
  }
  }
  return chunks;
}

// Rearrange each column in a table so that their entries match up with those of
// mapping
std::vector<ArrowArrays>
RearrangeTable(
    const ChunkedArrays& initial, const std::vector<size_t>& mapping,
    WriterProperties* properties) {
  std::vector<ArrowArrays> rearranged;
  rearranged.resize(initial.size());

  katana::do_all(
      katana::iterate(static_cast<size_t>(0), initial.size()),
      [&](const size_t& n) {
        auto array = initial[n];
        auto arrayType = array->type()->id();
        ArrowArrays ca;

        switch (arrayType) {
        case arrow::Type::STRING: {
          auto sb = std::make_shared<arrow::StringBuilder>();
          ca = RearrangeArray(sb, array, mapping, properties);
          break;
        }
        case arrow::Type::INT64: {
          auto lb = std::make_shared<arrow::Int64Builder>();
          ca = RearrangeArray<arrow::Int64Builder, arrow::Int64Array>(
              lb, array, mapping, properties);
          break;
        }
        case arrow::Type::INT32: {
          auto ib = std::make_shared<arrow::Int32Builder>();
          ca = RearrangeArray<arrow::Int32Builder, arrow::Int32Array>(
              ib, array, mapping, properties);
          break;
        }
        case arrow::Type::DOUBLE: {
          auto db = std::make_shared<arrow::DoubleBuilder>();
          ca = RearrangeArray<arrow::DoubleBuilder, arrow::DoubleArray>(
              db, array, mapping, properties);
          break;
        }
        case arrow::Type::FLOAT: {
          auto fb = std::make_shared<arrow::FloatBuilder>();
          ca = RearrangeArray<arrow::FloatBuilder, arrow::FloatArray>(
              fb, array, mapping, properties);
          break;
        }
        case arrow::Type::BOOL: {
          auto bb = std::make_shared<arrow::BooleanBuilder>();
          ca = RearrangeArray<arrow::BooleanBuilder, arrow::BooleanArray>(
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
          ca = RearrangeArray<arrow::UInt8Builder, arrow::UInt8Array>(
              bb, array, mapping, properties);
          break;
        }
        case arrow::Type::LIST: {
          ca = RearrangeListArray(array, mapping, properties);
          break;
        }
        default: {
          KATANA_LOG_FATAL(
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
std::vector<ArrowArrays>
RearrangeTypeTable(
    const ChunkedArrays& initial, const std::vector<size_t>& mapping,
    WriterProperties* properties) {
  std::vector<ArrowArrays> rearranged;
  rearranged.resize(initial.size());

  katana::do_all(
      katana::iterate(static_cast<size_t>(0), initial.size()),
      [&](const size_t& n) {
        auto array = initial[n];

        auto bb = std::make_shared<arrow::BooleanBuilder>();
        auto ca = RearrangeArray(bb, array, mapping, properties);
        rearranged[n] = ca;

        array.reset();
      });
  return rearranged;
}

template <typename BuilderType, typename ValueType>
std::shared_ptr<arrow::Array>
BuildImportVec(
    BuilderType&& builder, std::shared_ptr<arrow::Array> array,
    const std::vector<katana::ImportData>& imp) {
  if (auto st = builder.Resize(imp.size()); !st.ok()) {
    KATANA_LOG_DEBUG(
        "arrow builder failed resize: {} : {}", imp.size(), st.CodeAsString());
    return nullptr;
  }

  for (auto i : imp) {
    if (i.type == ImportDataType::kUnsupported) {
      if (auto res = builder.AppendNull(); !res.ok()) {
        KATANA_LOG_DEBUG(
            "arrow builder failed append null: {}", res.CodeAsString());
        return nullptr;
      }
    } else {
      if (auto res = builder.Append(std::get<ValueType>(i.value)); !res.ok()) {
        KATANA_LOG_DEBUG(
            "arrow builder failed append type: {} : {}", i.type,
            res.CodeAsString());
        return nullptr;
      }
    }
  }
  if (auto st = builder.Finish(&array); !st.ok()) {
    KATANA_LOG_DEBUG("arrow builder failed: {}", st.CodeAsString());
    return nullptr;
  }
  return array;
}

}  // end of unnamed namespace

katana::PropertyGraphBuilder::PropertyGraphBuilder(size_t chunk_size)
    : properties_(GetWriterProperties(chunk_size)),
      node_properties_(PropertiesState{}),
      edge_properties_(PropertiesState{}),
      node_labels_(LabelsState{}),
      edge_types_(LabelsState{}),
      topology_builder_(TopologyState{}),
      nodes_(0),
      edges_(0),
      building_node_(false),
      building_edge_(false) {}

/***************************/
/* Basic utility functions */
/***************************/

size_t
katana::PropertyGraphBuilder::GetNodeIndex() {
  if (building_node_) {
    return nodes_;
  }
  return std::numeric_limits<size_t>::max();
}

size_t
katana::PropertyGraphBuilder::GetNodes() {
  return nodes_;
}

size_t
katana::PropertyGraphBuilder::GetEdges() {
  return edges_;
}

/****************************************************************/
/* Functions for handling topology and logical flow of building */
/****************************************************************/

bool
katana::PropertyGraphBuilder::StartNode() {
  if (building_node_ || building_edge_) {
    return false;
  }
  building_node_ = true;
  topology_builder_.out_indices.emplace_back(0);

  return building_node_;
}

bool
katana::PropertyGraphBuilder::StartNode(const std::string& id) {
  if (this->StartNode()) {
    this->AddNodeID(id);
    return true;
  }
  return false;
}

void
katana::PropertyGraphBuilder::AddNodeID(const std::string& id) {
  topology_builder_.node_indexes.insert(
      std::pair<std::string, size_t>(id, nodes_));
}

void
katana::PropertyGraphBuilder::AddOutgoingEdge(
    const std::string& target, const std::string& label) {
  if (!building_node_) {
    return;
  }
  // if dest is an edge, do not create a shallow edge to it
  if (topology_builder_.edge_ids.find(target) !=
      topology_builder_.edge_ids.end()) {
    return;
  }
  building_node_ = false;
  building_edge_ = true;

  uint32_t src = static_cast<uint32_t>(nodes_);
  topology_builder_.sources.emplace_back(src);
  topology_builder_.out_indices[src]++;

  this->AddEdgeTarget(target);
  this->AddLabel(label);

  edges_++;
  building_node_ = true;
  building_edge_ = false;
}

void
katana::PropertyGraphBuilder::AddOutgoingEdge(
    uint32_t target, const std::string& label) {
  if (!building_node_) {
    return;
  }
  building_node_ = false;
  building_edge_ = true;

  uint32_t src = static_cast<uint32_t>(nodes_);
  topology_builder_.sources.emplace_back(src);
  topology_builder_.out_indices[src]++;
  topology_builder_.destinations.emplace_back(target);

  this->AddLabel(label);

  edges_++;
  building_node_ = true;
  building_edge_ = false;
}

bool
katana::PropertyGraphBuilder::FinishNode() {
  if (!building_node_) {
    return false;
  }
  nodes_++;
  building_node_ = false;

  return true;
}

bool
katana::PropertyGraphBuilder::AddNode(const std::string& id) {
  std::cout << "Adding placeholder node: " << id << std::endl;
  this->StartNode(id);
  return this->FinishNode();
}

bool
katana::PropertyGraphBuilder::StartEdge() {
  if (building_node_ || building_edge_) {
    return false;
  }
  building_edge_ = true;
  return building_edge_;
}

bool
katana::PropertyGraphBuilder::StartEdge(
    const std::string& source, const std::string& target) {
  if (building_node_ || building_edge_) {
    return false;
  }
  building_edge_ = true;

  this->AddEdgeSource(source);
  this->AddEdgeTarget(target);

  return building_edge_;
}

void
katana::PropertyGraphBuilder::AddEdgeID(const std::string& id) {
  topology_builder_.edge_ids.insert(id);
}

void
katana::PropertyGraphBuilder::AddEdgeSource(const std::string& source) {
  if (!building_edge_) {
    return;
  }
  auto src_entry = topology_builder_.node_indexes.find(source);
  if (src_entry != topology_builder_.node_indexes.end()) {
    topology_builder_.sources.emplace_back(
        static_cast<uint32_t>(src_entry->second));
    topology_builder_.out_indices[src_entry->second]++;
  } else {
    topology_builder_.sources_intermediate.insert(
        std::pair<size_t, std::string>(edges_, source));
    topology_builder_.sources.emplace_back(
        std::numeric_limits<uint32_t>::max());
  }
}

void
katana::PropertyGraphBuilder::AddEdgeTarget(const std::string& target) {
  if (!building_edge_) {
    return;
  }
  auto dest_entry = topology_builder_.node_indexes.find(target);
  if (dest_entry != topology_builder_.node_indexes.end()) {
    topology_builder_.destinations.emplace_back(
        static_cast<uint32_t>(dest_entry->second));
  } else {
    topology_builder_.destinations_intermediate.insert(
        std::pair<size_t, std::string>(edges_, target));
    topology_builder_.destinations.emplace_back(
        std::numeric_limits<uint32_t>::max());
  }
}

bool
katana::PropertyGraphBuilder::FinishEdge() {
  if (!building_edge_) {
    return false;
  }
  edges_++;
  building_edge_ = false;

  return true;
}

bool
katana::PropertyGraphBuilder::AddEdge(
    const std::string& source, const std::string& target) {
  this->StartEdge(source, target);
  return this->FinishEdge();
}

bool
katana::PropertyGraphBuilder::AddEdge(
    uint32_t source, const std::string& target, const std::string& label) {
  // if dest is an edge, do not create a shallow edge to it
  if (topology_builder_.edge_ids.find(target) !=
      topology_builder_.edge_ids.end()) {
    return false;
  }
  this->StartEdge();

  topology_builder_.sources.emplace_back(source);
  topology_builder_.out_indices[source]++;

  this->AddEdgeTarget(target);
  this->AddLabel(label);
  return this->FinishEdge();
}

bool
katana::PropertyGraphBuilder::AddEdge(
    uint32_t source, uint32_t target, const std::string& label) {
  this->StartEdge();
  topology_builder_.sources.emplace_back(source);
  topology_builder_.out_indices[source]++;
  topology_builder_.destinations.emplace_back(target);

  this->AddLabel(label);
  return this->FinishEdge();
}

/**************************************/
/* Functions for adding arrow columns */
/**************************************/

// Special case for building label builders where the empty value is false,
// not null
size_t
katana::PropertyGraphBuilder::AddLabelBuilder(const LabelRule& rule) {
  LabelsState* labels = rule.for_node ? &node_labels_ : &edge_types_;

  size_t index;
  auto reverse_iter = labels->reverse_schema.find(rule.label);
  // add entry to map if it is not already present
  if (reverse_iter == labels->reverse_schema.end()) {
    index = labels->keys.size();
    labels->keys.insert(std::pair<std::string, size_t>(rule.id, index));

    // add column to schema, builders, and chunks
    labels->schema.emplace_back(arrow::field(rule.label, arrow::boolean()));
    labels->builders.emplace_back(std::make_shared<arrow::BooleanBuilder>());
    labels->chunks.emplace_back(ArrowArrays{});
    labels->reverse_schema.insert(
        std::pair<std::string, std::string>(rule.label, rule.id));
  } else {
    // add a redirect entry to the existing label
    index = labels->keys.find(reverse_iter->second)->second;
    labels->keys.insert(std::pair<std::string, size_t>(rule.id, index));
  }
  return index;
}

// Case for adding properties for which we know their type
size_t
katana::PropertyGraphBuilder::AddBuilder(const PropertyKey& key) {
  PropertiesState* properties =
      key.for_node ? &node_properties_ : &edge_properties_;

  auto* pool = arrow::default_memory_pool();
  if (!key.is_list) {
    switch (key.type) {
    case ImportDataType::kString: {
      properties->schema.emplace_back(arrow::field(key.name, arrow::utf8()));
      properties->builders.emplace_back(
          std::make_shared<arrow::StringBuilder>());
      break;
    }
    case ImportDataType::kInt64: {
      properties->schema.emplace_back(arrow::field(key.name, arrow::int64()));
      properties->builders.emplace_back(
          std::make_shared<arrow::Int64Builder>());
      break;
    }
    case ImportDataType::kInt32: {
      properties->schema.emplace_back(arrow::field(key.name, arrow::int32()));
      properties->builders.emplace_back(
          std::make_shared<arrow::Int32Builder>());
      break;
    }
    case ImportDataType::kDouble: {
      properties->schema.emplace_back(arrow::field(key.name, arrow::float64()));
      properties->builders.emplace_back(
          std::make_shared<arrow::DoubleBuilder>());
      break;
    }
    case ImportDataType::kFloat: {
      properties->schema.emplace_back(arrow::field(key.name, arrow::float32()));
      properties->builders.emplace_back(
          std::make_shared<arrow::FloatBuilder>());
      break;
    }
    case ImportDataType::kBoolean: {
      properties->schema.emplace_back(arrow::field(key.name, arrow::boolean()));
      properties->builders.emplace_back(
          std::make_shared<arrow::BooleanBuilder>());
      break;
    }
    case ImportDataType::kTimestampMilli: {
      auto field = arrow::field(
          key.name, arrow::timestamp(arrow::TimeUnit::MILLI, "UTC"));
      properties->schema.emplace_back(field);
      properties->builders.emplace_back(
          std::make_shared<arrow::TimestampBuilder>(field->type(), pool));
      break;
    }
    case ImportDataType::kStruct: {
      properties->schema.emplace_back(arrow::field(key.name, arrow::uint8()));
      properties->builders.emplace_back(
          std::make_shared<arrow::UInt8Builder>());
      break;
    }
    default:
      // for now handle uncaught types as strings
      KATANA_LOG_WARN("treating unknown type {} as string", key.type);
      properties->schema.emplace_back(arrow::field(key.name, arrow::utf8()));
      properties->builders.emplace_back(
          std::make_shared<arrow::StringBuilder>());
      break;
    }
  } else {
    switch (key.type) {
    case ImportDataType::kString: {
      properties->schema.emplace_back(
          arrow::field(key.name, arrow::list(arrow::utf8())));
      properties->builders.emplace_back(std::make_shared<arrow::ListBuilder>(
          pool, std::make_shared<arrow::StringBuilder>()));
      break;
    }
    case ImportDataType::kInt64: {
      properties->schema.emplace_back(
          arrow::field(key.name, arrow::list(arrow::int64())));
      properties->builders.emplace_back(std::make_shared<arrow::ListBuilder>(
          pool, std::make_shared<arrow::Int64Builder>()));
      break;
    }
    case ImportDataType::kInt32: {
      properties->schema.emplace_back(
          arrow::field(key.name, arrow::list(arrow::int32())));
      properties->builders.emplace_back(std::make_shared<arrow::ListBuilder>(
          pool, std::make_shared<arrow::Int32Builder>()));
      break;
    }
    case ImportDataType::kDouble: {
      properties->schema.emplace_back(
          arrow::field(key.name, arrow::list(arrow::float64())));
      properties->builders.emplace_back(std::make_shared<arrow::ListBuilder>(
          pool, std::make_shared<arrow::DoubleBuilder>()));
      break;
    }
    case ImportDataType::kFloat: {
      properties->schema.emplace_back(
          arrow::field(key.name, arrow::list(arrow::float32())));
      properties->builders.emplace_back(std::make_shared<arrow::ListBuilder>(
          pool, std::make_shared<arrow::FloatBuilder>()));
      break;
    }
    case ImportDataType::kBoolean: {
      properties->schema.emplace_back(
          arrow::field(key.name, arrow::list(arrow::boolean())));
      properties->builders.emplace_back(std::make_shared<arrow::ListBuilder>(
          pool, std::make_shared<arrow::BooleanBuilder>()));
      break;
    }
    case ImportDataType::kTimestampMilli: {
      auto field = arrow::field(
          key.name, arrow::timestamp(arrow::TimeUnit::MILLI, "UTC"));

      properties->schema.emplace_back(
          arrow::field(key.name, arrow::list(field)));
      properties->builders.emplace_back(std::make_shared<arrow::ListBuilder>(
          pool,
          std::make_shared<arrow::TimestampBuilder>(field->type(), pool)));
      break;
    }
    default:
      // for now handle uncaught types as strings
      KATANA_LOG_WARN(
          "treating unknown array type {} as a string array", key.type);
      properties->schema.emplace_back(
          arrow::field(key.name, arrow::list(arrow::utf8())));
      properties->builders.emplace_back(std::make_shared<arrow::ListBuilder>(
          pool, std::make_shared<arrow::StringBuilder>()));
      break;
    }
  }
  auto index = properties->keys.size();
  properties->chunks.emplace_back(ArrowArrays{});
  properties->keys.insert(std::pair<std::string, size_t>(key.id, index));
  return index;
}

/*************************************************/
/* Functions for adding values to arrow builders */
/*************************************************/

// Add nulls until the array is even and then append val so that length = total
// + 1 at the end
void
katana::PropertyGraphBuilder::AddValue(
    const std::string& id, std::function<PropertyKey()> process_element,
    std::function<ImportData(ImportDataType, bool)> resolve_value) {
  if (!building_node_ && !building_edge_) {
    return;
  }
  auto property_builder =
      building_node_ ? &node_properties_ : &edge_properties_;
  auto total = building_node_ ? nodes_ : edges_;

  auto key_iter = property_builder->keys.find(id);
  size_t index;
  // if an entry for the key does not already exist, make an
  // entry for it
  if (key_iter == property_builder->keys.end()) {
    auto key = process_element();
    if (key.type == ImportDataType::kUnsupported) {
      std::cout << "elt not type not supported\n";
      return;
    }
    key.for_node = building_node_;
    key.for_edge = building_edge_;
    index = this->AddBuilder(std::move(key));
  } else {
    index = key_iter->second;
  }
  AddValueInternal(
      property_builder->builders[index], &property_builder->chunks[index],
      &properties_, total, resolve_value);
}

// Add falses until the array is even and then append true so that length =
// total + 1 at the end
void
katana::PropertyGraphBuilder::AddLabel(const std::string& name) {
  if (!building_node_ && !building_edge_) {
    return;
  }
  auto label_builder = building_node_ ? &node_labels_ : &edge_types_;
  auto total = building_node_ ? nodes_ : edges_;

  // add label
  auto entry = label_builder->keys.find(name);
  size_t index;
  // if type does not already exist, add a column
  if (entry == label_builder->keys.end()) {
    LabelRule rule{name, building_node_, building_edge_, name};
    index = this->AddLabelBuilder(std::move(rule));
  } else {
    index = entry->second;
  }
  AddLabelInternal(
      label_builder->builders[index], &label_builder->chunks[index],
      &properties_, total);
}

/*********************************/
/* Functions for building Graphs */
/*********************************/

// Resolve string node IDs to node indexes, if a node does not exist create an
// empty node
void
katana::PropertyGraphBuilder::ResolveIntermediateIDs() {
  TopologyState* topology = &topology_builder_;

  for (auto [index, str_id] : topology->destinations_intermediate) {
    auto dest_index = topology->node_indexes.find(str_id);
    uint32_t dest;
    // if node does not exist, create it
    if (dest_index == topology->node_indexes.end()) {
      dest = nodes_;
      this->AddNode(str_id);
    } else {
      dest = static_cast<uint32_t>(dest_index->second);
    }
    topology->destinations[index] = dest;
  }

  for (auto [index, str_id] : topology->sources_intermediate) {
    auto src_index = topology->node_indexes.find(str_id);
    uint32_t src;
    if (src_index == topology->node_indexes.end()) {
      src = nodes_;
      this->AddNode(str_id);
    } else {
      src = static_cast<uint32_t>(src_index->second);
    }
    topology->sources[index] = src;
    topology->out_indices[src]++;
  }
}

// Build CSR format and rearrange edge tables to correspond to the CSR
katana::GraphComponent
katana::PropertyGraphBuilder::BuildFinalEdges(bool verbose) {
  katana::ParallelSTL::partial_sum(
      topology_builder_.out_indices.begin(),
      topology_builder_.out_indices.end(),
      topology_builder_.out_indices.begin());

  std::vector<size_t> edge_mapping;
  edge_mapping.resize(edges_, std::numeric_limits<uint64_t>::max());

  std::vector<uint64_t> offsets;
  offsets.resize(nodes_, 0);

  // get edge indices
  for (size_t i = 0; i < topology_builder_.sources.size(); i++) {
    uint64_t edgeID = SetEdgeID(&topology_builder_, &offsets, i);
    edge_mapping[edgeID] = i;
  }

  auto initial_edges = BuildChunks(&edge_properties_.chunks);
  auto initial_types = BuildChunks(&edge_types_.chunks);

  auto final_edge_builders =
      RearrangeTable(initial_edges, edge_mapping, &properties_);
  auto final_type_builders =
      RearrangeTypeTable(initial_types, edge_mapping, &properties_);

  if (verbose) {
    std::cout << "Edge Properties Post:\n";
    WriteNullStats(final_edge_builders, &properties_, edges_);
    std::cout << "Edge Types Post:\n";
    WriteFalseStats(final_type_builders, &properties_, edges_);
  }

  return GraphComponent{
      BuildTable(&final_edge_builders, &edge_properties_.schema),
      BuildTable(&final_type_builders, &edge_types_.schema)};
}

katana::Result<GraphComponents>
katana::PropertyGraphBuilder::Finish(bool verbose) {
  topology_builder_.out_dests.resize(
      edges_, std::numeric_limits<uint32_t>::max());
  this->ResolveIntermediateIDs();

  // add buffered rows and even out columns
  EvenOutChunkBuilders(
      &node_properties_.builders, &node_properties_.chunks, &properties_,
      nodes_);
  EvenOutChunkBuilders(
      &node_labels_.builders, &node_labels_.chunks, &properties_, nodes_);
  EvenOutChunkBuilders(
      &edge_properties_.builders, &edge_properties_.chunks, &properties_,
      edges_);
  EvenOutChunkBuilders(
      &edge_types_.builders, &edge_types_.chunks, &properties_, edges_);

  if (verbose) {
    std::cout << "Node Properties:\n";
    WriteNullStats(node_properties_.chunks, &properties_, nodes_);
    std::cout << "Node Labels:\n";
    WriteFalseStats(node_labels_.chunks, &properties_, nodes_);
    std::cout << "Edge Properties Pre:\n";
    WriteNullStats(edge_properties_.chunks, &properties_, edges_);
    std::cout << "Edge Types Pre:\n";
    WriteFalseStats(edge_types_.chunks, &properties_, edges_);
  }

  // build final nodes
  auto final_node_table =
      BuildTable(&node_properties_.chunks, &node_properties_.schema);
  auto final_label_table =
      BuildTable(&node_labels_.chunks, &node_labels_.schema);
  GraphComponent nodes_tables{final_node_table, final_label_table};

  if (verbose) {
    std::cout << "Finished building nodes\n";
  }

  // rearrange edges to match implicit edge IDs
  auto edges_tables = this->BuildFinalEdges(verbose);

  if (verbose) {
    std::cout << "Finished topology and ordering edges\n";
  }

  // build topology
  katana::GraphTopology pg_topo(
      topology_builder_.out_indices.data(),
      topology_builder_.out_indices.size(), topology_builder_.out_dests.data(),
      topology_builder_.out_dests.size());

  if (verbose) {
    std::cout << "Finished mongodb conversion to arrow\n";
    std::cout << "Nodes: " << pg_topo.NumNodes() << "\n";
    std::cout << "Node Properties: " << nodes_tables.properties->num_columns()
              << "\n";
    std::cout << "Node Labels: " << nodes_tables.labels->num_columns() << "\n";
    std::cout << "Edges: " << pg_topo.NumEdges() << "\n";
    std::cout << "Edge Properties: " << edges_tables.properties->num_columns()
              << "\n";
    std::cout << "Edge Types: " << edges_tables.labels->num_columns() << "\n";
  }

  return katana::GraphComponents{
      nodes_tables, edges_tables, std::move(pg_topo)};
}

// NB: is_list is always initialized
void
ImportData::ValueFromArrowScalar(std::shared_ptr<arrow::Scalar> scalar) {
  switch (scalar->type->id()) {
  case arrow::Type::STRING: {
    type = ImportDataType::kString;
    value = std::static_pointer_cast<arrow::StringScalar>(scalar)->ToString();
    break;
  }
  case arrow::Type::INT64: {
    type = ImportDataType::kInt64;
    value = std::static_pointer_cast<arrow::Int64Scalar>(scalar)->value;
    break;
  }
  case arrow::Type::INT32: {
    type = ImportDataType::kInt32;
    value = std::static_pointer_cast<arrow::Int32Scalar>(scalar)->value;
    break;
  }
  case arrow::Type::UINT32: {
    type = ImportDataType::kUInt32;
    value = std::static_pointer_cast<arrow::UInt32Scalar>(scalar)->value;
    break;
  }
  case arrow::Type::DOUBLE: {
    type = ImportDataType::kDouble;
    value = std::static_pointer_cast<arrow::DoubleScalar>(scalar)->value;
    break;
  }
  case arrow::Type::FLOAT: {
    type = ImportDataType::kFloat;
    value = std::static_pointer_cast<arrow::FloatScalar>(scalar)->value;
    break;
  }
  case arrow::Type::BOOL: {
    type = ImportDataType::kBoolean;
    value = std::static_pointer_cast<arrow::BooleanScalar>(scalar)->value;
    break;
  }
  case arrow::Type::TIMESTAMP: {
    type = ImportDataType::kTimestampMilli;
    // TODO (witchel) Is this kosher?
    value = std::static_pointer_cast<arrow::Int64Scalar>(scalar)->value;
    break;
  }
  // for now uint8_t is an alias for a struct
  case arrow::Type::UINT8: {
    type = ImportDataType::kStruct;
    value = std::static_pointer_cast<arrow::UInt8Scalar>(scalar)->value;
    break;
  }
  case arrow::Type::LIST: {
    KATANA_LOG_FATAL("not yet supported");
    break;
  }
  default: {
    KATANA_LOG_FATAL("not yet supported");
    break;
  }
  }
}

katana::Result<std::unique_ptr<katana::PropertyGraph>>
katana::ConvertToPropertyGraph(
    katana::GraphComponents&& graph_comps, katana::TxnContext* txn_ctx) {
  auto pg_result = katana::PropertyGraph::Make(std::move(graph_comps.topology));
  if (!pg_result) {
    return pg_result.error().WithContext("adding topology");
  }
  std::unique_ptr<katana::PropertyGraph> graph = std::move(pg_result.value());

  if (graph_comps.nodes.properties->num_columns() > 0) {
    auto result =
        graph->AddNodeProperties(graph_comps.nodes.properties, txn_ctx);
    if (!result) {
      return result.error().WithContext("adding node properties");
    }
  }
  if (graph_comps.nodes.labels->num_columns() > 0) {
    auto result = graph->AddNodeProperties(graph_comps.nodes.labels, txn_ctx);
    if (!result) {
      return result.error().WithContext("adding node labels");
    }
  }
  if (graph_comps.edges.properties->num_columns() > 0) {
    auto result =
        graph->AddEdgeProperties(graph_comps.edges.properties, txn_ctx);
    if (!result) {
      return result.error().WithContext("adding edge properties");
    }
  }
  if (graph_comps.edges.labels->num_columns() > 0) {
    auto result = graph->AddEdgeProperties(graph_comps.edges.labels, txn_ctx);
    if (!result) {
      return result.error().WithContext("adding edge labels");
    }
  }
  return std::unique_ptr<katana::PropertyGraph>(std::move(graph));
}

/// WritePropertyGraph writes an RDG from the provided \param graph_comps
/// to the directory \param dir
katana::Result<void>
katana::WritePropertyGraph(
    katana::GraphComponents&& graph_comps, const katana::URI& dir,
    katana::TxnContext* txn_ctx) {
  auto graph_ptr = ConvertToPropertyGraph(std::move(graph_comps), txn_ctx);
  if (!graph_ptr) {
    return graph_ptr.error();
  }

  return WritePropertyGraph(*graph_ptr.value(), dir, txn_ctx);
}

katana::Result<void>
katana::WritePropertyGraph(
    katana::PropertyGraph& prop_graph, const katana::URI& dir,
    katana::TxnContext* txn_ctx) {
  for (const auto& field : prop_graph.loaded_node_schema()->fields()) {
    KATANA_LOG_VERBOSE(
        "node prop: ({}) {}", field->type()->ToString(), field->name());
  }
  for (const auto& field : prop_graph.loaded_edge_schema()->fields()) {
    KATANA_LOG_VERBOSE(
        "edge prop: ({}) {}", field->type()->ToString(), field->name());
  }

  KATANA_CHECKED(prop_graph.Write(dir, "libkatana_graph", txn_ctx));
  return ResultSuccess();
}

std::vector<katana::ImportData>
katana::ArrowToImport(const std::shared_ptr<arrow::ChunkedArray>& arr) {
  std::vector<katana::ImportData> ret(
      arr->length(),
      katana::ImportData(katana::ImportDataType::kUnsupported, false));
  int64_t index = 0;
  for (int32_t chnum = 0; chnum < arr->num_chunks(); ++chnum) {
    for (int64_t i = 0; i < arr->chunk(chnum)->length(); ++i) {
      if (!arr->chunk(chnum)->IsNull(i)) {
        auto res = arr->chunk(chnum)->GetScalar(i);
        if (!res.ok()) {
          KATANA_LOG_DEBUG("ArrowToImport problem scalar: {}", index);
        } else {
          ret[index].ValueFromArrowScalar(res.ValueOrDie());
        }
      }
      index++;
    }
  }
  return ret;
}

std::shared_ptr<arrow::Array>
ToArrowArray(
    arrow::Type::type arrow_type, std::shared_ptr<arrow::Array> arrow_dst,
    const std::vector<katana::ImportData>& import_src) {
  switch (arrow_type) {
  case arrow::Type::STRING: {
    arrow::StringBuilder builder;
    arrow_dst = BuildImportVec<arrow::StringBuilder, std::string>(
        std::move(builder), arrow_dst, import_src);
    break;
  }
  case arrow::Type::INT64: {
    arrow::Int64Builder builder;
    arrow_dst = BuildImportVec<arrow::Int64Builder, int64_t>(
        std::move(builder), arrow_dst, import_src);
    break;
  }
  case arrow::Type::INT32: {
    arrow::Int32Builder builder;
    arrow_dst = BuildImportVec<arrow::Int32Builder, int32_t>(
        std::move(builder), arrow_dst, import_src);
    break;
  }
  case arrow::Type::UINT32: {
    arrow::UInt32Builder builder;
    arrow_dst = BuildImportVec<arrow::UInt32Builder, uint32_t>(
        std::move(builder), arrow_dst, import_src);
    break;
  }
  case arrow::Type::DOUBLE: {
    arrow::DoubleBuilder builder;
    arrow_dst = BuildImportVec<arrow::DoubleBuilder, double>(
        std::move(builder), arrow_dst, import_src);
    break;
  }
  case arrow::Type::FLOAT: {
    arrow::FloatBuilder builder;
    arrow_dst = BuildImportVec<arrow::FloatBuilder, float>(
        std::move(builder), arrow_dst, import_src);
    break;
  }
  case arrow::Type::BOOL: {
    arrow::BooleanBuilder builder;
    arrow_dst = BuildImportVec<arrow::BooleanBuilder, bool>(
        std::move(builder), arrow_dst, import_src);
    break;
  }
  case arrow::Type::TIMESTAMP: {
    auto* pool = arrow::default_memory_pool();
    arrow::TimestampBuilder builder(
        arrow::timestamp(arrow::TimeUnit::NANO, "UTC"), pool);
    arrow_dst = BuildImportVec<arrow::TimestampBuilder, bool>(
        std::move(builder), arrow_dst, import_src);
    break;
  }
  // for now uint8_t is an alias for a struct
  case arrow::Type::UINT8: {
    arrow::UInt8Builder builder;
    arrow_dst = BuildImportVec<arrow::UInt8Builder, uint8_t>(
        std::move(builder), arrow_dst, import_src);
    break;
  }
  default: {
    KATANA_LOG_FATAL("not yet supported");
    break;
  }
  }
  return arrow_dst;
}

katana::Result<std::shared_ptr<arrow::ChunkedArray>>
katana::ImportToArrow(
    arrow::Type::type arrow_type,
    const std::vector<katana::ImportData>& import_src) {
  std::shared_ptr<arrow::Array> array = nullptr;

  array = ToArrowArray(arrow_type, array, import_src);

  std::vector<std::shared_ptr<arrow::Array>> chunks;
  chunks.push_back(std::move(array));
  return std::make_shared<arrow::ChunkedArray>(chunks);
}
