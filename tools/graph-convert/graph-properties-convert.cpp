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
#include <arrow/api.h>
#include <arrow/buffer.h>
#include <arrow/io/api.h>
#include <arrow/array/builder_binary.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#include "galois/ErrorCode.h"
#include "galois/Galois.h"
#include "galois/Logging.h"
#include "galois/graphs/PropertyFileGraph.h"
#include "galois/ParallelSTL.h"
#include "galois/SharedMemSys.h"
#include "galois/Threads.h"

using galois::GraphComponents;
using galois::GraphState;
using galois::ImportDataType;
using galois::LabelRule;
using galois::LabelsState;
using galois::PropertiesState;
using galois::PropertyKey;
using galois::TopologyState;
using galois::WriterProperties;

using ArrayBuilders   = std::vector<std::shared_ptr<arrow::ArrayBuilder>>;
using BooleanBuilders = std::vector<std::shared_ptr<arrow::BooleanBuilder>>;
using ChunkedArrays   = std::vector<std::shared_ptr<arrow::ChunkedArray>>;
using ArrowArrays     = std::vector<std::shared_ptr<arrow::Array>>;
using ArrowFields     = std::vector<std::shared_ptr<arrow::Field>>;
using NullMaps =
    std::pair<std::unordered_map<int, std::shared_ptr<arrow::Array>>,
              std::unordered_map<int, std::shared_ptr<arrow::Array>>>;

namespace {

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
    std::cout << "This table has no entries\n";
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

void WriteFalseStats(const std::vector<ArrowArrays>& table,
                     WriterProperties* properties, size_t total) {
  if (table.size() == 0) {
    std::cout << "This table has no entries\n";
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
    if (static_cast<size_t>(builder->length()) == chunk_size) {
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
    if (static_cast<size_t>(builder->length()) == chunk_size) {
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
  if (static_cast<size_t>(builder->length()) == properties->chunk_size) {
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
  if (static_cast<size_t>(list_builder->length()) == properties->chunk_size) {
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
  if (static_cast<size_t>(list_builder->length()) == properties->chunk_size) {
    chunks->push_back(BuildArray(list_builder));
  }
}

/***********************************/
/* Functions for handling topology */
/***********************************/

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
        dest = static_cast<uint32_t>(dest_index->second);
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
        src = static_cast<uint32_t>(src_index->second);
      }
      topology->sources[i] = src;
      topology->out_indices[src]++;
    }
  }
}

/******************************************************************************/
/* Functions for ensuring all arrow arrays are of the right length in the end */
/******************************************************************************/

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
  galois::do_all(galois::iterate(static_cast<size_t>(0), builders->size()),
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
  galois::do_all(galois::iterate(static_cast<size_t>(0), builders->size()),
                 [&](const size_t& i) {
                   AddFalses(builders->at(i), &chunks->at(i), properties,
                             total);

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
      galois::iterate(static_cast<size_t>(0), initial.size()),
      [&](const size_t& n) {
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

  galois::do_all(galois::iterate(static_cast<size_t>(0), initial.size()),
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

  std::cout << "Edge Properties Post:\n";
  WriteNullStats(finalEdgeBuilders, properties, builder->edges);
  std::cout << "Edge Types Post:\n";
  WriteFalseStats(finalTypeBuilders, properties, builder->edges);

  return std::pair<std::shared_ptr<arrow::Table>,
                   std::shared_ptr<arrow::Table>>(
      BuildTable(&finalEdgeBuilders, &builder->edge_properties.schema),
      BuildTable(&finalTypeBuilders, &builder->edge_types.schema));
}

} // end of unnamed namespace

/***************************************/
/* Functions for writing GraphML files */
/***************************************/

xmlTextWriterPtr galois::CreateGraphmlFile(const std::string& outfile) {
  xmlTextWriterPtr writer;
  writer = xmlNewTextWriterFilename(outfile.c_str(), 0);
  xmlTextWriterStartDocument(writer, "1.0", "UTF-8", NULL);
  xmlTextWriterSetIndentString(writer, BAD_CAST "");
  xmlTextWriterSetIndent(writer, 1);

  xmlTextWriterStartElement(writer, BAD_CAST "graphml");
  xmlTextWriterWriteAttribute(writer, BAD_CAST "xmlns",
                              BAD_CAST "http://graphml.graphdrawing.org/xmlns");
  xmlTextWriterWriteAttribute(writer, BAD_CAST "xmlns:xsi",
                              BAD_CAST
                              "http://www.w3.org/2001/XMLSchema-instance");
  xmlTextWriterWriteAttribute(
      writer, BAD_CAST "xmlns:schemaLocation",
      BAD_CAST "http://graphml.graphdrawing.org/xmlns "
               "http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd");

  return writer;
}

void galois::WriteGraphmlRule(xmlTextWriterPtr writer, const LabelRule& rule) {
  xmlTextWriterStartElement(writer, BAD_CAST "rule");
  xmlTextWriterWriteAttribute(writer, BAD_CAST "id", BAD_CAST rule.id.c_str());
  if (rule.for_node) {
    xmlTextWriterWriteAttribute(writer, BAD_CAST "for", BAD_CAST "node");
  } else if (rule.for_edge) {
    xmlTextWriterWriteAttribute(writer, BAD_CAST "for", BAD_CAST "edge");
  }
  xmlTextWriterWriteAttribute(writer, BAD_CAST "attr.label",
                              BAD_CAST rule.label.c_str());

  xmlTextWriterEndElement(writer);
}

void galois::WriteGraphmlKey(xmlTextWriterPtr writer, const PropertyKey& key) {
  xmlTextWriterStartElement(writer, BAD_CAST "key");
  xmlTextWriterWriteAttribute(writer, BAD_CAST "id", BAD_CAST key.id.c_str());
  if (key.for_node) {
    xmlTextWriterWriteAttribute(writer, BAD_CAST "for", BAD_CAST "node");
  } else if (key.for_edge) {
    xmlTextWriterWriteAttribute(writer, BAD_CAST "for", BAD_CAST "edge");
  }
  xmlTextWriterWriteAttribute(writer, BAD_CAST "attr.name",
                              BAD_CAST key.name.c_str());
  auto type = TypeName(key.type);
  xmlTextWriterWriteAttribute(writer, BAD_CAST "attr.type",
                              BAD_CAST type.c_str());
  if (key.is_list) {
    xmlTextWriterWriteAttribute(writer, BAD_CAST "attr.list",
                                BAD_CAST type.c_str());
  }

  xmlTextWriterEndElement(writer);
}

void galois::FinishGraphmlFile(xmlTextWriterPtr writer) {
  xmlTextWriterEndElement(writer); // end graphml
  xmlTextWriterEndDocument(writer);
  xmlFreeTextWriter(writer);
}
void galois::ExportSchemaMapping(const std::string& outfile,
                                 const std::vector<LabelRule>& rules,
                                 const std::vector<PropertyKey>& keys) {
  xmlTextWriterPtr writer = CreateGraphmlFile(outfile);

  for (const LabelRule& rule : rules) {
    WriteGraphmlRule(writer, rule);
  }
  for (const PropertyKey& key : keys) {
    WriteGraphmlKey(writer, key);
  }

  xmlTextWriterStartElement(writer, BAD_CAST "graph");
  xmlTextWriterEndElement(writer);

  FinishGraphmlFile(writer);
}

/***************************************/
/* Functions for parsing GraphML files */
/***************************************/

// extract the type from an attr.type or attr.list attribute from a key element
ImportDataType galois::ExtractTypeGraphML(xmlChar* value) {
  ImportDataType type = ImportDataType::kString;
  if (xmlStrEqual(value, BAD_CAST "string")) {
    type = ImportDataType::kString;
  } else if (xmlStrEqual(value, BAD_CAST "long") ||
             xmlStrEqual(value, BAD_CAST "int64")) {
    type = ImportDataType::kInt64;
  } else if (xmlStrEqual(value, BAD_CAST "int") ||
             xmlStrEqual(value, BAD_CAST "int32")) {
    type = ImportDataType::kInt32;
  } else if (xmlStrEqual(value, BAD_CAST "double")) {
    type = ImportDataType::kDouble;
  } else if (xmlStrEqual(value, BAD_CAST "float")) {
    type = ImportDataType::kFloat;
  } else if (xmlStrEqual(value, BAD_CAST "boolean")) {
    type = ImportDataType::kBoolean;
  } else if (xmlStrEqual(value, BAD_CAST "timestamp milli")) {
    type = ImportDataType::kTimestampMilli;
  } else if (xmlStrEqual(value, BAD_CAST "struct")) {
    type = ImportDataType::kStruct;
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
PropertyKey galois::ProcessKey(xmlTextReaderPtr reader) {
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
 * reader should be pointing to rule node elt before calling
 *
 * extracts key attribute information for use later
 */
LabelRule galois::ProcessRule(xmlTextReaderPtr reader) {
  int ret = xmlTextReaderMoveToNextAttribute(reader);
  xmlChar *name, *value;

  std::string id;
  bool for_node = false;
  bool for_edge = false;
  std::string attr_label;

  while (ret == 1) {
    name  = xmlTextReaderName(reader);
    value = xmlTextReaderValue(reader);
    if (name != NULL) {
      if (xmlStrEqual(name, BAD_CAST "id")) {
        id = std::string((const char*)value);
      } else if (xmlStrEqual(name, BAD_CAST "for")) {
        for_node = xmlStrEqual(value, BAD_CAST "node") == 1;
        for_edge = xmlStrEqual(value, BAD_CAST "edge") == 1;
      } else if (xmlStrEqual(name, BAD_CAST "attr.label")) {
        attr_label = std::string((const char*)value);
      } else {
        GALOIS_LOG_ERROR("Attribute on key: {}, was not recognized",
                         std::string((const char*)name));
      }
    }

    xmlFree(name);
    xmlFree(value);
    ret = xmlTextReaderMoveToNextAttribute(reader);
  }
  return LabelRule{
      id,
      for_node,
      for_edge,
      attr_label,
  };
}

/**************************************************/
/* Functions for reading schema mapping from file */
/**************************************************/

std::pair<std::vector<std::string>, std::vector<std::string>>
galois::ProcessSchemaMapping(GraphState* builder, const std::string& mapping,
                             const std::vector<std::string>& coll_names) {
  xmlTextReaderPtr reader;
  int ret              = 0;
  bool finished_header = false;
  std::vector<std::string> nodes;
  std::vector<std::string> edges;

  std::cout << "Start reading GraphML schema mapping file: " << mapping << "\n";

  reader = xmlNewTextReaderFilename(mapping.c_str());
  if (reader != NULL) {
    ret = xmlTextReaderRead(reader);

    // procedure:
    // read in "key" xml nodes and add them to nodeKeys and edgeKeys
    // once we reach the first "graph" xml node we parse it using the above keys
    // once we have parsed the first "graph" xml node we exit
    while (ret == 1 && !finished_header) {
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
              AddBuilder(&builder->node_properties, std::move(key));
            } else if (key.for_edge) {
              AddBuilder(&builder->edge_properties, std::move(key));
            }
          }
        } else if (xmlStrEqual(name, BAD_CAST "rule")) {
          LabelRule rule = ProcessRule(reader);
          if (rule.id.size() > 0) {
            if (rule.for_node) {
              if (std::find(coll_names.begin(), coll_names.end(), rule.id) !=
                  coll_names.end()) {
                nodes.push_back(rule.id);
              }
              AddLabelBuilder(&builder->node_labels, std::move(rule));
            } else if (rule.for_edge) {
              if (std::find(coll_names.begin(), coll_names.end(), rule.id) !=
                  coll_names.end()) {
                edges.push_back(rule.id);
              }
              AddLabelBuilder(&builder->edge_types, std::move(rule));
            }
          }
        } else if (xmlStrEqual(name, BAD_CAST "graph")) {
          std::cout << "Finished processing headers\n";
          finished_header = true;
        }
      }

      xmlFree(name);
      ret = xmlTextReaderRead(reader);
    }
    xmlFreeTextReader(reader);
    if (ret < 0) {
      GALOIS_LOG_FATAL("Failed to parse {}", mapping);
    }
  } else {
    GALOIS_LOG_FATAL("Unable to open {}", mapping);
  }
  return std::pair<std::vector<std::string>, std::vector<std::string>>(nodes,
                                                                       edges);
}

/**************************************************/
/* Functions for converting to/from datatype enum */
/**************************************************/

std::string galois::TypeName(ImportDataType type) {
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

ImportDataType galois::ParseType(const std::string& in) {
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

/**************************/
/* Basic helper functions */
/**************************/

WriterProperties galois::GetWriterProperties(size_t chunk_size) {
  return WriterProperties{GetNullArrays(chunk_size), GetFalseArray(chunk_size),
                          chunk_size};
}

/**************************************/
/* Functions for adding arrow columns */
/**************************************/

// Special case for building label builders where the empty value is false,
// not null
size_t galois::AddLabelBuilder(LabelsState* labels, LabelRule rule) {
  size_t index;
  auto reverse_iter = labels->reverse_schema.find(rule.label);
  // add entry to map if it is not already present
  if (reverse_iter == labels->reverse_schema.end()) {
    index = labels->keys.size();
    labels->keys.insert(std::pair<std::string, size_t>(rule.id, index));

    // add column to schema, builders, and chunks
    labels->schema.push_back(arrow::field(rule.label, arrow::boolean()));
    labels->builders.push_back(std::make_shared<arrow::BooleanBuilder>());
    labels->chunks.push_back(ArrowArrays{});
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
size_t galois::AddBuilder(PropertiesState* properties, PropertyKey key) {
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

/*************************************************/
/* Functions for adding values to arrow builders */
/*************************************************/

// Add nulls until the array is even and then append val so that length = total
// + 1 at the end
void galois::AddValue(std::shared_ptr<arrow::ArrayBuilder> builder,
                      ArrowArrays* chunks, WriterProperties* properties,
                      size_t total, std::function<void(void)> AppendValue) {
  AddNulls(builder, chunks, properties, total);
  AppendValue();

  // if we filled up a chunk, flush it
  if (static_cast<size_t>(builder->length()) == properties->chunk_size) {
    chunks->push_back(BuildArray(builder));
  }
}

// Add falses until the array is even and then append true so that length =
// total + 1 at the end
void galois::AddLabel(std::shared_ptr<arrow::BooleanBuilder> builder,
                      ArrowArrays* chunks, WriterProperties* properties,
                      size_t total) {
  AddFalses(builder, chunks, properties, total);
  auto st = builder->Append(true);
  if (!st.ok()) {
    GALOIS_LOG_FATAL("Error appending to an arrow array builder: {}",
                     st.ToString());
  }

  // if we filled up a chunk, flush it
  if (static_cast<size_t>(builder->length()) == properties->chunk_size) {
    chunks->push_back(BuildArray(builder));
  }
}

/**************************************/
/* Functions for building GraphStates */
/**************************************/

galois::GraphComponents
galois::BuildGraphComponents(GraphState builder, WriterProperties properties) {
  if (!builder.topology_builder.sources_intermediate.empty() ||
      !builder.topology_builder.destinations_intermediate.empty()) {
    ResolveIntermediateIDs(&builder);
  }

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

  std::cout << "Node Properties:\n";
  WriteNullStats(builder.node_properties.chunks, &properties, builder.nodes);
  std::cout << "Node Labels:\n";
  WriteFalseStats(builder.node_labels.chunks, &properties, builder.nodes);
  std::cout << "Edge Properties Pre:\n";
  WriteNullStats(builder.edge_properties.chunks, &properties, builder.edges);
  std::cout << "Edge Types Pre:\n";
  WriteFalseStats(builder.edge_types.chunks, &properties, builder.edges);

  // build final nodes
  auto final_node_table = BuildTable(&builder.node_properties.chunks,
                                     &builder.node_properties.schema);
  auto final_label_table =
      BuildTable(&builder.node_labels.chunks, &builder.node_labels.schema);

  std::cout << "Finished building nodes\n";

  // rearrange edges to match implicit edge IDs
  auto edges_tables = BuildFinalEdges(&builder, &properties);
  std::shared_ptr<arrow::Table> final_edge_table = edges_tables.first;
  std::shared_ptr<arrow::Table> final_type_table = edges_tables.second;

  std::cout << "Finished topology and ordering edges\n";

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

  std::cout << "Finished mongodb conversion to arrow\n";
  std::cout << "Nodes: " << topology->out_indices->length() << "\n";
  std::cout << "Node Properties: " << final_node_table->num_columns() << "\n";
  std::cout << "Node Labels: " << final_label_table->num_columns() << "\n";
  std::cout << "Edges: " << topology->out_dests->length() << "\n";
  std::cout << "Edge Properties: " << final_edge_table->num_columns() << "\n";
  std::cout << "Edge Types: " << final_type_table->num_columns() << "\n";

  return galois::GraphComponents{final_node_table, final_label_table,
                                 final_edge_table, final_type_table, topology};
}

/// convertToPropertyGraphAndWrite formally builds katana form via
/// PropertyFileGraph from imported components and writes the result to target
/// directory
///
/// \param graph_comps imported components to convert into a PropertyFileGraph
/// \param dir local FS directory or s3 directory to write PropertyFileGraph
/// to
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
