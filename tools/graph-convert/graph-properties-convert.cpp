#include "graph-properties-convert.h"

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <fstream>
#include <iostream>
#include <limits>
#include <random>
#include <sstream>
#include <unordered_map>
#include <vector>

#include <boost/algorithm/string.hpp>
#include <boost/foreach.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <csv.hpp>
#include <libxml/xmlreader.h>
#include <arrow/api.h>
#include <arrow/buffer.h>
#include <arrow/io/api.h>
#include <arrow/array/builder_binary.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#include "galois/ErrorCode.h"
#include "galois/Logging.h"
#include "galois/graphs/PropertyFileGraph.h"
#include "galois/SharedMemSys.h"

namespace {

using galois::GraphComponents;
using galois::ImportDataType;

typedef std::vector<std::shared_ptr<arrow::ArrayBuilder>> ArrayBuilders;
typedef std::vector<std::shared_ptr<arrow::StringBuilder>> StringBuilders;
typedef std::vector<std::shared_ptr<arrow::BooleanBuilder>> BooleanBuilders;
typedef std::vector<std::shared_ptr<arrow::ChunkedArray>> ChunkedArrays;
typedef std::vector<std::shared_ptr<arrow::Array>> ArrowArrays;
typedef std::vector<std::shared_ptr<arrow::StringArray>> StringArrays;
typedef std::vector<std::shared_ptr<arrow::BooleanArray>> BooleanArrays;
typedef std::vector<std::shared_ptr<arrow::Field>> ArrowFields;

struct KeyGraphML {
  std::string id;
  bool forNode;
  bool forEdge;
  std::string name;
  ImportDataType type;
  bool isList;
  size_t columnID;

  KeyGraphML(const std::string& id_, bool forNode_, bool forEdge_,
             const std::string& name_, ImportDataType type_, bool isList_,
             size_t columnID_)
      : id(id_), forNode(forNode_), forEdge(forEdge_), name(name_), type(type_),
        isList(isList_), columnID(columnID_) {}
  KeyGraphML(const std::string& id, bool forNode, size_t columnID)
      : KeyGraphML(id, forNode, !forNode, id, ImportDataType::STRING, false,
                   columnID) {}
};

/************************************/
/* Shared GraphML/CSV/JSON Function */
/************************************/

template <typename T>
void computePrefixSum(std::vector<T>* array) {
  auto prev = array->begin();
  for (auto ii = array->begin() + 1, ei = array->end(); ii != ei;
       ++ii, ++prev) {
    *ii += *prev;
  }
}

uint64_t setEdgeID(const std::vector<uint64_t>& out_indices_,
                   std::vector<uint32_t>* out_dests_,
                   std::vector<uint64_t>* offsets,
                   const std::vector<uint32_t>& sources,
                   const std::vector<uint32_t>& destinations, size_t index) {
  uint32_t src  = sources[index];
  uint64_t base = src ? out_indices_[src - 1] : 0;
  uint64_t i    = base + offsets->at(src)++;

  out_dests_->at(i) = destinations[index];
  return i;
}

/**********************/
/* CSV/JSON Functions */
/**********************/

BooleanArrays buildArrays(BooleanBuilders* columnBuilders) {
  BooleanArrays arrays;
  arrays.reserve(columnBuilders->size());
  for (size_t i = 0; i < columnBuilders->size(); i++) {
    std::shared_ptr<arrow::BooleanArray> array;
    auto st = columnBuilders->at(i)->Finish(&array);
    if (!st.ok()) {
      GALOIS_LOG_FATAL("Error building arrow array");
    }
    arrays.push_back(array);
  }
  return arrays;
}

template <typename T>
std::shared_ptr<arrow::Table>
buildTable(std::vector<std::shared_ptr<T>>* columnBuilders,
           ArrowFields* schemaVector) {
  ArrowArrays columns;
  columns.reserve(columnBuilders->size());
  for (size_t i = 0; i < columnBuilders->size(); i++) {
    std::shared_ptr<arrow::Array> array;
    auto st = columnBuilders->at(i)->Finish(&array);
    if (!st.ok()) {
      GALOIS_LOG_FATAL("Error building arrow array");
    }
    columns.push_back(array);
  }

  auto schema = std::make_shared<arrow::Schema>(*schemaVector);
  return arrow::Table::Make(schema, columns);
}

template <typename T, typename W>
void rearrangeArray(std::shared_ptr<T> builder, const std::shared_ptr<W>& array,
                    const std::vector<size_t>& mapping) {
  auto st = builder->Reserve(array->length());
  if (!st.ok()) {
    GALOIS_LOG_FATAL("Error reserving space for arrow array");
  }

  for (size_t k = 0; k < mapping.size(); k++) {
    size_t index = mapping[k];

    if (array->IsNull(index)) {
      st = builder->AppendNull();
      if (!st.ok()) {
        GALOIS_LOG_FATAL("Error appending null to an arrow array builder");
      }
      continue;
    }
    st = builder->Append(array->Value(index));
    if (!st.ok()) {
      GALOIS_LOG_FATAL("Error appending value to an arrow array builder");
    }
  }
}

void rearrangeArray(std::shared_ptr<arrow::StringBuilder> builder,
                    const std::shared_ptr<arrow::StringArray>& array,
                    const std::vector<size_t>& mapping) {
  auto st = builder->Reserve(array->length());
  if (!st.ok()) {
    GALOIS_LOG_FATAL("Error reserving space for arrow array");
  }

  for (size_t k = 0; k < mapping.size(); k++) {
    size_t index = mapping[k];

    if (array->IsNull(index)) {
      st = builder->AppendNull();
      if (!st.ok()) {
        GALOIS_LOG_FATAL("Error appending null to an arrow array builder");
      }
      continue;
    }
    st = builder->Append(array->GetView(index));
    if (!st.ok()) {
      GALOIS_LOG_FATAL("Error appending value to an arrow array builder");
    }
  }
}

BooleanBuilders rearrangeTable(const BooleanArrays& initial,
                               const std::vector<size_t>& mapping) {
  BooleanBuilders rearranged;
  rearranged.reserve(initial.size());
  for (size_t i = 0; i < initial.size(); i++) {
    auto bb    = std::make_shared<arrow::BooleanBuilder>();
    auto array = initial[i];

    rearrangeArray(bb, array, mapping);

    rearranged.push_back(bb);
  }
  return rearranged;
}

StringArrays buildArrays(StringBuilders* columnBuilders) {
  StringArrays arrays;
  arrays.reserve(columnBuilders->size());
  for (size_t i = 0; i < columnBuilders->size(); i++) {
    std::shared_ptr<arrow::StringArray> array;
    auto st = columnBuilders->at(i)->Finish(&array);
    if (!st.ok()) {
      GALOIS_LOG_FATAL("Error building arrow array");
    }
    arrays.push_back(array);
  }
  return arrays;
}

// special case for building boolean builders where the empty value is false,
// not null
void addFalseColumnBuilder(const std::string& column,
                           std::unordered_map<std::string, size_t>* map,
                           BooleanBuilders* columnBuilders,
                           ArrowFields* schemaVector,
                           std::shared_ptr<arrow::DataType> type,
                           std::vector<bool>* falseMap, size_t offset) {
  // add entry to map
  auto [entry, found] =
      map->insert(std::pair<std::string, size_t>(column, map->size()));
  if (!found) {
    GALOIS_LOG_ERROR(
        "An label name that already exists was attempted to be added");
  }

  // add column to schema and column builders, make table even by adding nulls
  schemaVector->push_back(arrow::field(column, type));
  columnBuilders->push_back(std::make_shared<arrow::BooleanBuilder>());
  for (size_t i = 0; i < offset; i++) {
    auto st = columnBuilders->at(entry->second)->Append(false);
    if (!st.ok()) {
      GALOIS_LOG_FATAL("Error appending to an arrow array builder");
    }
  }
  // add to null map
  falseMap->push_back(true);
}

template <typename T>
void addColumnBuilder(const std::string& column,
                      std::unordered_map<std::string, size_t>* map,
                      std::vector<std::shared_ptr<T>>* columnBuilders,
                      ArrowFields* schemaVector,
                      std::shared_ptr<arrow::DataType> type,
                      std::vector<bool>* nullMap, size_t offset) {
  // add entry to map
  auto [entry, found] =
      map->insert(std::pair<std::string, size_t>(column, map->size()));
  if (!found) {
    GALOIS_LOG_ERROR(
        "An column name that already exists was attempted to be added");
  }

  // add column to schema and column builders, make table even by adding nulls
  schemaVector->push_back(arrow::field(column, type));
  columnBuilders->push_back(std::make_shared<T>());
  auto st = columnBuilders->at(entry->second)->AppendNulls(offset);
  if (!st.ok()) {
    GALOIS_LOG_FATAL("Error appending nulls to an arrow array builder");
  }

  // add to null map
  nullMap->push_back(true);
}

template <typename T>
void addNullsToBuilderAndReset(std::vector<std::shared_ptr<T>>* columnBuilders,
                               std::vector<bool>* nullMap) {
  for (size_t i = 0; i < columnBuilders->size(); i++) {
    if (nullMap->at(i)) {
      auto st = columnBuilders->at(i)->AppendNull();
      if (!st.ok()) {
        GALOIS_LOG_FATAL("Error appending null to an arrow array builder");
      }
    } else {
      nullMap->at(i) = true;
    }
  }
}

void addFalsesToBuilderAndReset(BooleanBuilders* columnBuilders,
                                std::vector<bool>* falseMap) {
  for (size_t i = 0; i < columnBuilders->size(); i++) {
    if (falseMap->at(i)) {
      auto st = columnBuilders->at(i)->Append(false);
      if (!st.ok()) {
        GALOIS_LOG_FATAL("Error appending null to an arrow array builder");
      }
    } else {
      falseMap->at(i) = true;
    }
  }
}

StringBuilders rearrangeTable(const StringArrays& initial,
                              const std::vector<size_t>& mapping) {
  StringBuilders rearranged;
  rearranged.reserve(initial.size());
  for (size_t i = 0; i < initial.size(); i++) {
    auto sb    = std::make_shared<arrow::StringBuilder>();
    auto array = initial[i];
    rearrangeArray(sb, array, mapping);

    rearranged.push_back(sb);
  }
  return rearranged;
}

std::pair<std::shared_ptr<arrow::Table>, std::shared_ptr<arrow::Table>>
buildFinalEdges(StringBuilders* edgeColumnBuilders,
                ArrowFields* edgeSchemaVector,
                BooleanBuilders* edgeTypeColumnBuilders,
                ArrowFields* edgeTypeSchemaVector,
                std::vector<uint64_t>* out_indices_,
                std::vector<uint32_t>* out_dests_,
                const std::vector<uint32_t>& sources,
                const std::vector<uint32_t>& destinations) {
  computePrefixSum(out_indices_);

  std::vector<size_t> edgeMapping;
  edgeMapping.resize(sources.size(), std::numeric_limits<uint64_t>::max());

  std::vector<uint64_t> offsets;
  offsets.resize(out_indices_->size(), 0);

  // get edge indices
  for (size_t i = 0; i < sources.size(); i++) {
    uint64_t edgeID = setEdgeID(*out_indices_, out_dests_, &offsets, sources,
                                destinations, i);
    edgeMapping[edgeID] = i;
  }

  auto initialEdgeArrays = buildArrays(edgeColumnBuilders);
  auto initialTypeArrays = buildArrays(edgeTypeColumnBuilders);
  auto finalEdgeBuilders = rearrangeTable(initialEdgeArrays, edgeMapping);
  auto finalTypeBuilders = rearrangeTable(initialTypeArrays, edgeMapping);
  return std::pair<std::shared_ptr<arrow::Table>,
                   std::shared_ptr<arrow::Table>>(
      buildTable(&finalEdgeBuilders, edgeSchemaVector),
      buildTable(&finalTypeBuilders, edgeTypeSchemaVector));
}

std::pair<std::vector<std::string>, std::vector<size_t>>
extractHeaderCSV(csv::CSVReader* reader, ArrowFields* nodeSchemaVector,
                 StringBuilders* nodeColumnBuilders,
                 ArrowFields* edgeSchemaVector,
                 StringBuilders* edgeColumnBuilders,
                 std::unordered_map<size_t, size_t>* nodeKeys,
                 std::unordered_map<size_t, size_t>* edgeKeys) {

  std::vector<std::string> headers = reader->get_col_names();
  std::vector<size_t> positions;
  positions.resize(5, 0);

  bool nodeHeaders = true;
  for (size_t i = 0; i < headers.size(); i++) {
    if (headers[i] == std::string("_id")) {
      positions[0] = i;
    } else if (headers[i] == std::string("_labels")) {
      positions[1] = i;
    } else if (headers[i] == std::string("_start")) {
      positions[2] = i;
      nodeHeaders  = false;
    } else if (headers[i] == std::string("_end")) {
      positions[3] = i;
      nodeHeaders  = false;
    } else if (headers[i] == std::string("_type")) {
      positions[4] = i;
      nodeHeaders  = false;
    } else {
      if (nodeHeaders) {
        nodeSchemaVector->push_back(arrow::field(headers[i], arrow::utf8()));
        nodeColumnBuilders->push_back(std::make_shared<arrow::StringBuilder>());
        nodeKeys->insert(std::pair<size_t, size_t>(i, nodeKeys->size()));
      } else {
        edgeSchemaVector->push_back(arrow::field(headers[i], arrow::utf8()));
        edgeColumnBuilders->push_back(std::make_shared<arrow::StringBuilder>());
        edgeKeys->insert(std::pair<size_t, size_t>(i, edgeKeys->size()));
      }
    }
  }
  return std::pair(headers, positions);
}

/*********************/
/* GraphML Functions */
/*********************/

template <typename T>
std::shared_ptr<arrow::Array> buildArray(std::shared_ptr<T> columnBuilder) {
  std::shared_ptr<arrow::Array> array;
  auto st = columnBuilder->Finish(&array);
  if (!st.ok()) {
    GALOIS_LOG_FATAL("Error building arrow array: {}", st.ToString());
  }
  return array;
}

template <typename T>
ArrowArrays buildChunksOfArrays(T* columnBuilders) {
  ArrowArrays arrays;
  arrays.reserve(columnBuilders->size());
  for (size_t i = 0; i < columnBuilders->size(); i++) {
    arrays.push_back(buildArray(columnBuilders->at(i)));
  }
  return arrays;
}

template <typename T>
void chunkBuilders(std::vector<ArrowArrays>* arrayChunks, T* builders) {
  ArrowArrays arrays = buildChunksOfArrays(builders);

  for (size_t i = 0; i < arrayChunks->size(); i++) {
    arrayChunks->at(i).push_back(arrays[i]);
  }
}

ChunkedArrays buildChunks(std::vector<ArrowArrays>* arrayChunks) {
  ChunkedArrays chunkedArrays;
  chunkedArrays.reserve(arrayChunks->size());
  for (size_t i = 0; i < arrayChunks->size(); i++) {
    chunkedArrays.push_back(
        std::make_shared<arrow::ChunkedArray>(arrayChunks->at(i)));
  }
  return chunkedArrays;
}

std::shared_ptr<arrow::Table> buildTable(std::vector<ArrowArrays>* arrayChunks,
                                         ArrowFields* schemaVector) {
  ChunkedArrays columns = buildChunks(arrayChunks);

  auto schema = std::make_shared<arrow::Schema>(*schemaVector);
  return arrow::Table::Make(schema, columns);
}

// special case for building boolean builders where the empty value is false,
// not null
void addFalseColumnBuilder(const std::string& column,
                           std::unordered_map<std::string, size_t>* map,
                           BooleanBuilders* columnBuilders,
                           ArrowFields* schemaVector,
                           std::vector<ArrowArrays>* arrayChunks,
                           std::shared_ptr<arrow::DataType> type, size_t offset,
                           size_t chunkSize) {
  // add entry to map
  auto [entry, found] =
      map->insert(std::pair<std::string, size_t>(column, map->size()));
  if (!found) {
    GALOIS_LOG_ERROR(
        "An label name that already exists was attempted to be added");
  }

  // add column to schema and column builders
  schemaVector->push_back(arrow::field(column, type));
  columnBuilders->push_back(std::make_shared<arrow::BooleanBuilder>());

  arrow::Status st;
  // add column to arrayChunks and make even
  ArrowArrays chunks;
  for (auto i = chunkSize; i <= offset; i += chunkSize) {
    for (uint64_t i = 0; i < chunkSize; i++) {
      st = columnBuilders->at(entry->second)->Append(false);
    }
    chunks.push_back(buildArray(columnBuilders->at(entry->second)));
  }
  arrayChunks->push_back(chunks);

  // make even by adding final falses
  for (size_t i = 0; i < offset % chunkSize; i++) {
    st = columnBuilders->at(entry->second)->Append(false);
    if (!st.ok()) {
      GALOIS_LOG_FATAL("Error appending to an arrow array builder: {}",
                       st.ToString());
    }
  }
}

void addColumnBuilder(const std::string& column, bool forNode,
                      std::unordered_map<std::string, KeyGraphML>* map,
                      ArrayBuilders* columnBuilders, ArrowFields* schemaVector,
                      std::vector<ArrowArrays>* arrayChunks,
                      std::shared_ptr<arrow::DataType> type, size_t offset,
                      size_t chunkSize) {
  // add entry to map
  auto [entry, found] = map->insert(std::pair<std::string, KeyGraphML>(
      column, KeyGraphML(column, forNode, map->size())));
  if (!found) {
    GALOIS_LOG_ERROR(
        "An column name that already exists was attempted to be added");
  }

  // add column to schema and column builders
  schemaVector->push_back(arrow::field(column, type));
  columnBuilders->push_back(std::make_shared<arrow::StringBuilder>());

  arrow::Status st;
  // add column to arrayChunks and make even
  ArrowArrays chunks;
  for (auto i = chunkSize; i <= offset; i += chunkSize) {
    st = columnBuilders->at(entry->second.columnID)->AppendNulls(chunkSize);
    chunks.push_back(buildArray(columnBuilders->at(entry->second.columnID)));
  }
  arrayChunks->push_back(chunks);

  // make even by adding final nulls
  st = columnBuilders->at(entry->second.columnID)
           ->AppendNulls(offset % chunkSize);
  if (!st.ok()) {
    GALOIS_LOG_FATAL("Error appending nulls to an arrow array builder: {}",
                     st.ToString());
  }
}

void addBuilder(ArrayBuilders* columnBuilders, ArrowFields* schemaVector,
                KeyGraphML* key) {
  if (!key->isList) {
    switch (key->type) {
    case ImportDataType::STRING: {
      schemaVector->push_back(arrow::field(key->name, arrow::utf8()));
      columnBuilders->push_back(std::make_shared<arrow::StringBuilder>());
      break;
    }
    case ImportDataType::INT64: {
      schemaVector->push_back(arrow::field(key->name, arrow::int64()));
      columnBuilders->push_back(std::make_shared<arrow::Int64Builder>());
      break;
    }
    case ImportDataType::INT32: {
      schemaVector->push_back(arrow::field(key->name, arrow::int32()));
      columnBuilders->push_back(std::make_shared<arrow::Int32Builder>());
      break;
    }
    case ImportDataType::DOUBLE: {
      schemaVector->push_back(arrow::field(key->name, arrow::float64()));
      columnBuilders->push_back(std::make_shared<arrow::DoubleBuilder>());
      break;
    }
    case ImportDataType::FLOAT: {
      schemaVector->push_back(arrow::field(key->name, arrow::float32()));
      columnBuilders->push_back(std::make_shared<arrow::FloatBuilder>());
      break;
    }
    case ImportDataType::BOOLEAN: {
      schemaVector->push_back(arrow::field(key->name, arrow::boolean()));
      columnBuilders->push_back(std::make_shared<arrow::BooleanBuilder>());
      break;
    }
    default:
      // for now handle uncaught types as strings
      GALOIS_LOG_WARN("treating unknown type {} as string", key->type);
      schemaVector->push_back(arrow::field(key->name, arrow::utf8()));
      columnBuilders->push_back(std::make_shared<arrow::StringBuilder>());
      break;
    }
  } else {
    auto* pool = arrow::default_memory_pool();
    switch (key->type) {
    case ImportDataType::STRING: {
      schemaVector->push_back(
          arrow::field(key->name, arrow::list(arrow::utf8())));
      columnBuilders->push_back(std::make_shared<arrow::ListBuilder>(
          pool, std::make_shared<arrow::StringBuilder>()));
      break;
    }
    case ImportDataType::INT64: {
      schemaVector->push_back(
          arrow::field(key->name, arrow::list(arrow::int64())));
      columnBuilders->push_back(std::make_shared<arrow::ListBuilder>(
          pool, std::make_shared<arrow::Int64Builder>()));
      break;
    }
    case ImportDataType::INT32: {
      schemaVector->push_back(
          arrow::field(key->name, arrow::list(arrow::int32())));
      columnBuilders->push_back(std::make_shared<arrow::ListBuilder>(
          pool, std::make_shared<arrow::Int32Builder>()));
      break;
    }
    case ImportDataType::DOUBLE: {
      schemaVector->push_back(
          arrow::field(key->name, arrow::list(arrow::float64())));
      columnBuilders->push_back(std::make_shared<arrow::ListBuilder>(
          pool, std::make_shared<arrow::DoubleBuilder>()));
      break;
    }
    case ImportDataType::FLOAT: {
      schemaVector->push_back(
          arrow::field(key->name, arrow::list(arrow::float32())));
      columnBuilders->push_back(std::make_shared<arrow::ListBuilder>(
          pool, std::make_shared<arrow::FloatBuilder>()));
      break;
    }
    case ImportDataType::BOOLEAN: {
      schemaVector->push_back(
          arrow::field(key->name, arrow::list(arrow::boolean())));
      columnBuilders->push_back(std::make_shared<arrow::ListBuilder>(
          pool, std::make_shared<arrow::BooleanBuilder>()));
      break;
    }
    default:
      // for now handle uncaught types as strings
      GALOIS_LOG_WARN("treating unknown array type {} as a string array",
                      key->type);
      schemaVector->push_back(
          arrow::field(key->name, arrow::list(arrow::utf8())));
      columnBuilders->push_back(std::make_shared<arrow::ListBuilder>(
          pool, std::make_shared<arrow::StringBuilder>()));
      break;
    }
  }
}

template <typename T, typename W>
ArrowArrays
rearrangeArray(std::shared_ptr<T> builder,
               const std::shared_ptr<arrow::ChunkedArray>& chunkedArray,
               const std::vector<size_t>& mapping, size_t chunkSize) {
  ArrowArrays chunks;
  auto st = builder->Reserve(chunkSize);
  if (!st.ok()) {
    GALOIS_LOG_FATAL("Error reserving space for arrow array: {}",
                     st.ToString());
  }
  // cast and store array chunks for use in loop
  std::vector<std::shared_ptr<W>> arrays;
  for (auto chunk : chunkedArray->chunks()) {
    arrays.push_back(std::static_pointer_cast<W>(chunk));
  }

  for (size_t k = 0; k * chunkSize < mapping.size(); k++) {
    for (size_t i = 0; i < chunkSize && k * chunkSize + i < mapping.size();
         i++) {
      size_t index    = mapping[k * chunkSize + i];
      auto array      = arrays[index / chunkSize];
      size_t subIndex = index % chunkSize;

      if (array->IsNull(subIndex)) {
        st = builder->AppendNull();
        if (!st.ok()) {
          GALOIS_LOG_FATAL("Error appending null to an arrow array builder: {}",
                           st.ToString());
        }
        continue;
      }
      st = builder->Append(array->Value(subIndex));
      if (!st.ok()) {
        GALOIS_LOG_FATAL("Error appending value to an arrow array builder: {}",
                         st.ToString());
      }
    }
    chunks.push_back(buildArray(builder));
  }
  return chunks;
}

ArrowArrays
rearrangeArray(std::shared_ptr<arrow::StringBuilder> builder,
               const std::shared_ptr<arrow::ChunkedArray>& chunkedArray,
               const std::vector<size_t>& mapping, size_t chunkSize) {
  ArrowArrays chunks;
  auto st = builder->Reserve(chunkSize);
  if (!st.ok()) {
    GALOIS_LOG_FATAL("Error reserving space for arrow array: {}",
                     st.ToString());
  }
  // cast and store array chunks for use in loop
  std::vector<std::shared_ptr<arrow::StringArray>> arrays;
  for (auto chunk : chunkedArray->chunks()) {
    arrays.push_back(std::static_pointer_cast<arrow::StringArray>(chunk));
  }

  for (size_t k = 0; k * chunkSize < mapping.size(); k++) {
    for (size_t i = 0; i < chunkSize && k * chunkSize + i < mapping.size();
         i++) {
      size_t index    = mapping[k * chunkSize + i];
      auto array      = arrays[index / chunkSize];
      size_t subIndex = index % chunkSize;

      if (array->IsNull(subIndex)) {
        st = builder->AppendNull();
        if (!st.ok()) {
          GALOIS_LOG_FATAL("Error appending null to an arrow array builder: {}",
                           st.ToString());
        }
        continue;
      }
      st = builder->Append(array->GetView(subIndex));
      if (!st.ok()) {
        GALOIS_LOG_FATAL("Error appending value to an arrow array builder: {}",
                         st.ToString());
      }
    }
    chunks.push_back(buildArray(builder));
  }
  return chunks;
}

template <typename T, typename W>
ArrowArrays
rearrangeArray(const std::shared_ptr<arrow::ListBuilder>& builder, T* tBuilder,
               const std::shared_ptr<arrow::ChunkedArray>& chunkedArray,
               const std::vector<size_t>& mapping, size_t chunkSize) {
  ArrowArrays chunks;
  auto st = builder->Reserve(chunkSize);
  if (!st.ok()) {
    GALOIS_LOG_FATAL("Error reserving space for arrow array: {}",
                     st.ToString());
  }
  // cast and store array chunks for use in loop
  std::vector<std::shared_ptr<arrow::ListArray>> listArrays;
  std::vector<std::shared_ptr<W>> subArrays;
  for (auto chunk : chunkedArray->chunks()) {
    auto arrayTemp = std::static_pointer_cast<arrow::ListArray>(chunk);
    listArrays.push_back(arrayTemp);
    subArrays.push_back(std::static_pointer_cast<W>(arrayTemp->values()));
  }

  for (size_t k = 0; k * chunkSize < mapping.size(); k++) {
    for (size_t i = 0; i < chunkSize && k * chunkSize + i < mapping.size();
         i++) {
      size_t index    = mapping[k * chunkSize + i];
      auto array      = listArrays[index / chunkSize];
      auto sArray     = subArrays[index / chunkSize];
      size_t subIndex = index % chunkSize;

      if (array->IsNull(subIndex)) {
        st = builder->AppendNull();
        if (!st.ok()) {
          GALOIS_LOG_FATAL("Error appending null to an arrow array builder: {}",
                           st.ToString());
        }
        continue;
      }
      int32_t start = array->value_offset(subIndex);
      int32_t end   = array->value_offset(subIndex + 1);

      st = builder->Append();
      for (int32_t s = start; s < end; s++) {
        st = tBuilder->Append(sArray->Value(s));
        if (!st.ok()) {
          GALOIS_LOG_FATAL(
              "Error appending value to an arrow array builder: {}",
              st.ToString());
        }
      }
    }
    chunks.push_back(buildArray(builder));
  }
  return chunks;
}

ArrowArrays
rearrangeArray(std::shared_ptr<arrow::ListBuilder> builder,
               arrow::StringBuilder* sBuilder,
               const std::shared_ptr<arrow::ChunkedArray>& chunkedArray,
               const std::vector<size_t>& mapping, size_t chunkSize) {
  ArrowArrays chunks;
  auto st = builder->Reserve(chunkSize);
  if (!st.ok()) {
    GALOIS_LOG_FATAL("Error reserving space for arrow array: {}",
                     st.ToString());
  }
  // cast and store array chunks for use in loop
  std::vector<std::shared_ptr<arrow::ListArray>> listArrays;
  std::vector<std::shared_ptr<arrow::StringArray>> subArrays;
  for (auto chunk : chunkedArray->chunks()) {
    auto arrayTemp = std::static_pointer_cast<arrow::ListArray>(chunk);
    listArrays.push_back(arrayTemp);
    subArrays.push_back(
        std::static_pointer_cast<arrow::StringArray>(arrayTemp->values()));
  }

  for (size_t k = 0; k * chunkSize < mapping.size(); k++) {
    for (size_t i = 0; i < chunkSize && k * chunkSize + i < mapping.size();
         i++) {
      size_t index    = mapping[k * chunkSize + i];
      auto array      = listArrays[index / chunkSize];
      auto sArray     = subArrays[index / chunkSize];
      size_t subIndex = index % chunkSize;

      if (array->IsNull(subIndex)) {
        st = builder->AppendNull();
        if (!st.ok()) {
          GALOIS_LOG_FATAL("Error appending null to an arrow array builder: {}",
                           st.ToString());
        }
        continue;
      }
      int32_t start = array->value_offset(subIndex);
      int32_t end   = array->value_offset(subIndex + 1);

      st = builder->Append();
      for (int32_t s = start; s < end; s++) {
        st = sBuilder->Append(sArray->GetView(s));
        if (!st.ok()) {
          GALOIS_LOG_FATAL(
              "Error appending value to an arrow array builder: {}",
              st.ToString());
        }
      }
    }
    chunks.push_back(buildArray(builder));
  }
  return chunks;
}

ArrowArrays
rearrangeListArray(const std::shared_ptr<arrow::ChunkedArray>& listChunkedArray,
                   const std::vector<size_t>& mapping, size_t chunkSize) {
  auto* pool = arrow::default_memory_pool();
  ArrowArrays chunks;
  auto listType =
      std::static_pointer_cast<arrow::BaseListType>(listChunkedArray->type())
          ->value_type()
          ->id();

  switch (listType) {
  case arrow::Type::STRING: {
    auto builder = std::make_shared<arrow::ListBuilder>(
        pool, std::make_shared<arrow::StringBuilder>());
    auto sb = static_cast<arrow::StringBuilder*>(builder->value_builder());
    chunks  = rearrangeArray(builder, sb, listChunkedArray, mapping, chunkSize);
    break;
  }
  case arrow::Type::INT64: {
    auto builder = std::make_shared<arrow::ListBuilder>(
        pool, std::make_shared<arrow::Int64Builder>());
    auto lb = static_cast<arrow::Int64Builder*>(builder->value_builder());
    chunks  = rearrangeArray<arrow::Int64Builder, arrow::Int64Array>(
        builder, lb, listChunkedArray, mapping, chunkSize);
    break;
  }
  case arrow::Type::INT32: {
    auto builder = std::make_shared<arrow::ListBuilder>(
        pool, std::make_shared<arrow::Int32Builder>());
    auto ib = static_cast<arrow::Int32Builder*>(builder->value_builder());
    chunks  = rearrangeArray<arrow::Int32Builder, arrow::Int32Array>(
        builder, ib, listChunkedArray, mapping, chunkSize);
    break;
  }
  case arrow::Type::DOUBLE: {
    auto builder = std::make_shared<arrow::ListBuilder>(
        pool, std::make_shared<arrow::DoubleBuilder>());
    auto db = static_cast<arrow::DoubleBuilder*>(builder->value_builder());
    chunks  = rearrangeArray<arrow::DoubleBuilder, arrow::DoubleArray>(
        builder, db, listChunkedArray, mapping, chunkSize);
    break;
  }
  case arrow::Type::FLOAT: {
    auto builder = std::make_shared<arrow::ListBuilder>(
        pool, std::make_shared<arrow::FloatBuilder>());
    auto fb = static_cast<arrow::FloatBuilder*>(builder->value_builder());
    chunks  = rearrangeArray<arrow::FloatBuilder, arrow::FloatArray>(
        builder, fb, listChunkedArray, mapping, chunkSize);
    break;
  }
  case arrow::Type::BOOL: {
    auto builder = std::make_shared<arrow::ListBuilder>(
        pool, std::make_shared<arrow::BooleanBuilder>());
    auto bb = static_cast<arrow::BooleanBuilder*>(builder->value_builder());
    chunks  = rearrangeArray<arrow::BooleanBuilder, arrow::BooleanArray>(
        builder, bb, listChunkedArray, mapping, chunkSize);
    break;
  }
  default: {
    GALOIS_LOG_FATAL(
        "Unsupported arrow array type passed to rearrangeListArray: {}",
        listType);
  }
  }
  return chunks;
}

std::vector<ArrowArrays> rearrangeTable(const ChunkedArrays& initial,
                                        const std::vector<size_t>& mapping,
                                        size_t chunkSize) {
  std::vector<ArrowArrays> rearranged;
  rearranged.reserve(initial.size());

  for (size_t i = 0; i < initial.size(); i++) {
    auto array     = initial[i];
    auto arrayType = array->type()->id();

    switch (arrayType) {
    case arrow::Type::STRING: {
      auto sb = std::make_shared<arrow::StringBuilder>();
      auto ca = rearrangeArray(sb, array, mapping, chunkSize);
      rearranged.push_back(ca);
      break;
    }
    case arrow::Type::INT64: {
      auto lb = std::make_shared<arrow::Int64Builder>();
      auto ca = rearrangeArray<arrow::Int64Builder, arrow::Int64Array>(
          lb, array, mapping, chunkSize);
      rearranged.push_back(ca);
      break;
    }
    case arrow::Type::INT32: {
      auto ib = std::make_shared<arrow::Int32Builder>();
      auto ca = rearrangeArray<arrow::Int32Builder, arrow::Int32Array>(
          ib, array, mapping, chunkSize);
      rearranged.push_back(ca);
      break;
    }
    case arrow::Type::DOUBLE: {
      auto db = std::make_shared<arrow::DoubleBuilder>();
      auto ca = rearrangeArray<arrow::DoubleBuilder, arrow::DoubleArray>(
          db, array, mapping, chunkSize);
      rearranged.push_back(ca);
      break;
    }
    case arrow::Type::FLOAT: {
      auto fb = std::make_shared<arrow::FloatBuilder>();
      auto ca = rearrangeArray<arrow::FloatBuilder, arrow::FloatArray>(
          fb, array, mapping, chunkSize);
      rearranged.push_back(ca);
      break;
    }
    case arrow::Type::BOOL: {
      auto bb = std::make_shared<arrow::BooleanBuilder>();
      auto ca = rearrangeArray<arrow::BooleanBuilder, arrow::BooleanArray>(
          bb, array, mapping, chunkSize);
      rearranged.push_back(ca);
      break;
    }
    case arrow::Type::LIST: {
      // auto la = std::static_pointer_cast<arrow::ListArray>(array);
      auto ca = rearrangeListArray(array, mapping, chunkSize);
      rearranged.push_back(ca);
      break;
    }
    default: {
      GALOIS_LOG_FATAL(
          "Unsupported arrow array type passed to rearrangeTable: {}",
          arrayType);
    }
    }
    // array.reset();
  }
  return rearranged;
}

std::pair<std::shared_ptr<arrow::Table>, std::shared_ptr<arrow::Table>>
buildFinalEdges(const ChunkedArrays& initialEdges,
                ArrowFields* edgeSchemaVector,
                const ChunkedArrays& initialTypes,
                ArrowFields* edgeTypeSchemaVector,
                std::vector<uint64_t>* out_indices_,
                std::vector<uint32_t>* out_dests_,
                const std::vector<uint32_t>& sources,
                const std::vector<uint32_t>& destinations, size_t chunkSize) {
  computePrefixSum(out_indices_);

  std::vector<size_t> edgeMapping;
  edgeMapping.resize(sources.size(), std::numeric_limits<uint64_t>::max());

  std::vector<uint64_t> offsets;
  offsets.resize(out_indices_->size(), 0);

  // get edge indices
  for (size_t i = 0; i < sources.size(); i++) {
    uint64_t edgeID = setEdgeID(*out_indices_, out_dests_, &offsets, sources,
                                destinations, i);
    edgeMapping[edgeID] = i;
  }

  auto finalEdgeBuilders = rearrangeTable(initialEdges, edgeMapping, chunkSize);
  auto finalTypeBuilders = rearrangeTable(initialTypes, edgeMapping, chunkSize);
  return std::pair<std::shared_ptr<arrow::Table>,
                   std::shared_ptr<arrow::Table>>(
      buildTable(&finalEdgeBuilders, edgeSchemaVector),
      buildTable(&finalTypeBuilders, edgeTypeSchemaVector));
}

std::vector<std::string> parseStringList(std::string rawList) {
  std::vector<std::string> list;

  if (rawList.size() >= 2 && rawList.front() == '[' && rawList.back() == ']') {
    rawList.erase(0, 1);
    rawList.erase(rawList.length() - 1, 1);
  } else {
    GALOIS_LOG_ERROR(
        "The provided list was not formatted like neo4j, returning string");
    list.push_back(rawList);
    return list;
  }

  const char* charList = rawList.c_str();
  // parse the list
  for (size_t i = 0; i < rawList.size();) {
    bool firstQuoteFound   = false;
    bool foundEndOfElem    = false;
    size_t startOfElem     = i;
    int consecutiveSlashes = 0;

    // parse the field
    for (; !foundEndOfElem && i < rawList.size(); i++) {
      // if second quote not escaped then end of element reached
      if (charList[i] == '\"') {
        if (consecutiveSlashes % 2 == 0) {
          if (!firstQuoteFound) {
            firstQuoteFound = true;
            startOfElem     = i + 1;
          } else if (firstQuoteFound) {
            foundEndOfElem = true;
          }
        }
        consecutiveSlashes = 0;
      } else if (charList[i] == '\\') {
        consecutiveSlashes++;
      } else {
        consecutiveSlashes = 0;
      }
    }
    size_t endOfElem  = i - 1;
    size_t elemLength = endOfElem - startOfElem;

    if (endOfElem <= startOfElem) {
      list.push_back("");
    } else {

      std::string elemRough(&charList[startOfElem], elemLength);
      std::string elem("");
      elem.reserve(elemRough.size());
      size_t currIndex = 0;
      size_t nextSlash = elemRough.find_first_of('\\');

      while (nextSlash != std::string::npos) {
        elem.append(elemRough.begin() + currIndex,
                    elemRough.begin() + nextSlash);

        switch (elemRough[nextSlash + 1]) {
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
                          elemRough[nextSlash + 1]);
        }

        currIndex = nextSlash + 2;
        nextSlash = elemRough.find_first_of('\\', currIndex);
      }
      elem.append(elemRough.begin() + currIndex, elemRough.end());

      list.push_back(elem);
    }
  }

  return list;
}

template <typename T>
std::vector<T> parseNumberList(std::string rawList) {
  std::vector<T> list;

  if (rawList.front() == '[' && rawList.back() == ']') {
    rawList.erase(0, 1);
    rawList.erase(rawList.length() - 1, 1);
  } else {
    GALOIS_LOG_ERROR("The provided list was not formatted like neo4j, "
                     "returning empty vector");
    return list;
  }
  std::vector<std::string> elems;
  boost::split(elems, rawList, boost::is_any_of(","));

  for (std::string s : elems) {
    list.push_back(boost::lexical_cast<T>(s));
  }
  return list;
}

std::vector<bool> parseBooleanList(std::string rawList) {
  std::vector<bool> list;

  if (rawList.front() == '[' && rawList.back() == ']') {
    rawList.erase(0, 1);
    rawList.erase(rawList.length() - 1, 1);
  } else {
    GALOIS_LOG_ERROR("The provided list was not formatted like neo4j, "
                     "returning empty vector");
    return list;
  }
  std::vector<std::string> elems;
  boost::split(elems, rawList, boost::is_any_of(","));

  for (std::string s : elems) {
    bool boolVal = s[0] == 't' || s[0] == 'T';
    list.push_back(boolVal);
  }
  return list;
}

void appendArray(std::shared_ptr<arrow::ListBuilder> lBuilder,
                 const std::string& val) {
  arrow::Status st = arrow::Status::OK();

  switch (lBuilder->value_builder()->type()->id()) {
  case arrow::Type::STRING: {
    auto sb     = static_cast<arrow::StringBuilder*>(lBuilder->value_builder());
    st          = lBuilder->Append();
    auto sarray = parseStringList(val);
    st          = sb->AppendValues(sarray);
    break;
  }
  case arrow::Type::INT64: {
    auto lb     = static_cast<arrow::Int64Builder*>(lBuilder->value_builder());
    st          = lBuilder->Append();
    auto larray = parseNumberList<int64_t>(val);
    st          = lb->AppendValues(larray);
    break;
  }
  case arrow::Type::INT32: {
    auto ib     = static_cast<arrow::Int32Builder*>(lBuilder->value_builder());
    st          = lBuilder->Append();
    auto iarray = parseNumberList<int32_t>(val);
    st          = ib->AppendValues(iarray);
    break;
  }
  case arrow::Type::DOUBLE: {
    auto db     = static_cast<arrow::DoubleBuilder*>(lBuilder->value_builder());
    st          = lBuilder->Append();
    auto darray = parseNumberList<double>(val);
    st          = db->AppendValues(darray);
    break;
  }
  case arrow::Type::FLOAT: {
    auto fb     = static_cast<arrow::FloatBuilder*>(lBuilder->value_builder());
    st          = lBuilder->Append();
    auto farray = parseNumberList<float>(val);
    st          = fb->AppendValues(farray);
    break;
  }
  case arrow::Type::BOOL: {
    auto bb = static_cast<arrow::BooleanBuilder*>(lBuilder->value_builder());
    st      = lBuilder->Append();
    auto barray = parseBooleanList(val);
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

void appendValue(std::shared_ptr<arrow::ArrayBuilder> array,
                 const std::string& val, size_t currentElts) {
  arrow::Status st   = arrow::Status::OK();
  uint64_t nullCount = currentElts - array->length();
  st                 = array->AppendNulls(nullCount);

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
    auto bb      = std::static_pointer_cast<arrow::BooleanBuilder>(array);
    bool boolVal = val[0] == 't' || val[0] == 'T';
    st           = bb->Append(boolVal);
    break;
  }
  case arrow::Type::LIST: {
    auto lb = std::static_pointer_cast<arrow::ListBuilder>(array);
    appendArray(lb, val);
    break;
  }
  default: {
    break;
  }
  }
  if (!st.ok()) {
    GALOIS_LOG_FATAL("Error adding value to arrow array builder: {}, at depth "
                     "{}, parquet error: {}",
                     val, currentElts, st.ToString());
  }
}

void evenOutTable(ArrayBuilders& arrays, size_t currentElts) {
  arrow::Status st = arrow::Status::OK();
  for (auto array : arrays) {
    uint64_t nullCount = currentElts - array->length();
    st                 = array->AppendNulls(nullCount);
  }
  if (!st.ok()) {
    GALOIS_LOG_FATAL("Error adding value to arrow array builder: {}",
                     st.ToString());
  }
}
void evenOutTable(BooleanBuilders& arrays, size_t currentElts) {
  arrow::Status st = arrow::Status::OK();
  for (auto array : arrays) {
    uint64_t falseCount = currentElts - array->length();
    for (uint64_t i = 0; i < falseCount; i++) {
      st = array->Append(false);
    }
  }
  if (!st.ok()) {
    GALOIS_LOG_FATAL("Error adding value to arrow array builder: {}",
                     st.ToString());
  }
}

// extract the type from an attr.type or attr.list attribute from a key element
ImportDataType extractTypeGraphML(xmlChar* value) {
  ImportDataType type = ImportDataType::STRING;
  if (xmlStrEqual(value, BAD_CAST "string")) {
    type = ImportDataType::STRING;
  } else if (xmlStrEqual(value, BAD_CAST "long")) {
    type = ImportDataType::INT64;
  } else if (xmlStrEqual(value, BAD_CAST "int")) {
    type = ImportDataType::INT32;
  } else if (xmlStrEqual(value, BAD_CAST "double")) {
    type = ImportDataType::DOUBLE;
  } else if (xmlStrEqual(value, BAD_CAST "float")) {
    type = ImportDataType::FLOAT;
  } else if (xmlStrEqual(value, BAD_CAST "boolean")) {
    type = ImportDataType::BOOLEAN;
  } else {
    GALOIS_LOG_ERROR("Came across attr.type: {}, that is not supported",
                     std::string((const char*)value));
    type = ImportDataType::STRING;
  }
  return type;
}

/*
 * reader should be pointing to key node elt before calling
 *
 * extracts key attribute information for use later
 */
KeyGraphML processKey(xmlTextReaderPtr reader) {
  int ret = xmlTextReaderMoveToNextAttribute(reader);
  xmlChar *name, *value;

  std::string id;
  bool forNode = false;
  bool forEdge = false;
  std::string attrName;
  ImportDataType type = ImportDataType::STRING;
  bool isList         = false;

  while (ret == 1) {
    name  = xmlTextReaderName(reader);
    value = xmlTextReaderValue(reader);
    if (name != NULL) {
      if (xmlStrEqual(name, BAD_CAST "id")) {
        id = std::string((const char*)value);
      } else if (xmlStrEqual(name, BAD_CAST "for")) {
        forNode = xmlStrEqual(value, BAD_CAST "node") == 1;
        forEdge = xmlStrEqual(value, BAD_CAST "edge") == 1;
      } else if (xmlStrEqual(name, BAD_CAST "attr.name")) {
        attrName = std::string((const char*)value);
      } else if (xmlStrEqual(name, BAD_CAST "attr.type")) {
        // do this check for neo4j
        if (!isList) {
          type = extractTypeGraphML(value);
        }
      } else if (xmlStrEqual(name, BAD_CAST "attr.list")) {
        isList = true;
        type   = extractTypeGraphML(value);
      } else {
        GALOIS_LOG_ERROR("Attribute on key: {}, was not recognized",
                         std::string((const char*)name));
      }
    }

    xmlFree(name);
    xmlFree(value);
    ret = xmlTextReaderMoveToNextAttribute(reader);
  }
  size_t columnID = 0;
  return KeyGraphML{
      id, forNode, forEdge, attrName, type, isList, columnID,
  };
}

/*
 * reader should be pointing at the data element before calling
 *
 * parses data from a GraphML file into property: pair<string, string>
 */
std::pair<std::string, std::string> processData(xmlTextReaderPtr reader) {
  auto minimumDepth = xmlTextReaderDepth(reader);

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
  while (ret == 1 && minimumDepth < xmlTextReaderDepth(reader)) {
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
bool processNode(xmlTextReaderPtr reader,
                 std::unordered_map<std::string, KeyGraphML>* nodeKeys,
                 ArrowFields* nodeSchemaVector,
                 ArrayBuilders* nodeColumnBuilders,
                 std::vector<ArrowArrays>* nodeChunks,
                 std::unordered_map<std::string, size_t>* labelIndices,
                 ArrowFields* nodeLabelSchemaVector,
                 BooleanBuilders* nodeLabelColumnBuilders,
                 std::vector<ArrowArrays>* labelChunks, size_t nodes,
                 size_t chunkSize,
                 std::unordered_map<std::string, size_t>* nodeIndexes) {
  auto minimumDepth = xmlTextReaderDepth(reader);

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
    nodeIndexes->insert(
        std::pair<std::string, size_t>(id, nodeIndexes->size()));
  }

  // parse "data" xml nodes for properties
  ret = xmlTextReaderRead(reader);
  // will terminate when </node> reached or an improper read
  while (ret == 1 && minimumDepth < xmlTextReaderDepth(reader)) {
    name = xmlTextReaderName(reader);
    if (name == NULL) {
      name = xmlStrdup(BAD_CAST "--");
    }
    // if elt is an xml node (we do not parse text for nodes)
    if (xmlTextReaderNodeType(reader) == 1) {
      // if elt is a "data" xml node read it in
      if (xmlStrEqual(name, BAD_CAST "data")) {
        auto property = processData(reader);
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
              auto keyIter = nodeKeys->find(property.first);
              // if an entry for the key does not already exist, make a default
              // entry for it
              if (keyIter == nodeKeys->end()) {
                addColumnBuilder(property.first, true, nodeKeys,
                                 nodeColumnBuilders, nodeSchemaVector,
                                 nodeChunks, arrow::utf8(), nodes, chunkSize);
                keyIter = nodeKeys->find(property.first);
              }
              appendValue(nodeColumnBuilders->at(keyIter->second.columnID),
                          property.second, nodes % chunkSize);
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
      auto entry = labelIndices->find(label);

      // if label does not already exist, add a column
      if (entry == labelIndices->end()) {
        addFalseColumnBuilder(label, labelIndices, nodeLabelColumnBuilders,
                              nodeLabelSchemaVector, labelChunks,
                              arrow::boolean(), nodes, chunkSize);
        entry = labelIndices->find(label);
      }
      auto array = nodeLabelColumnBuilders->at(entry->second);
      auto st    = arrow::Status::OK();
      for (uint64_t i = array->length(); i < nodes % chunkSize; i++) {
        st = array->Append(false);
      }
      st = array->Append(true);
      if (!st.ok()) {
        GALOIS_LOG_FATAL("Error adding value to arrow array: {}",
                         st.ToString());
      }
    }
  }
  return validNode;
}

/*
 * reader should be pointing at the edge element before calling
 *
 * parses the edge from a GraphML file into readable form
 */
bool processEdge(xmlTextReaderPtr reader,
                 std::unordered_map<std::string, KeyGraphML>* edgeKeys,
                 ArrowFields* edgeSchemaVector,
                 ArrayBuilders* edgeColumnBuilders,
                 std::vector<ArrowArrays>* edgeChunks,
                 std::unordered_map<std::string, size_t>* typeIndices,
                 ArrowFields* edgeTypeSchemaVector,
                 BooleanBuilders* edgeTypeColumnBuilders,
                 std::vector<ArrowArrays>* typeChunks, size_t edges,
                 size_t chunkSize, std::vector<uint64_t>* out_indices_,
                 std::vector<uint32_t>* sources,
                 std::vector<uint32_t>* destinations,
                 std::unordered_map<std::string, size_t>* nodeIndexes) {
  auto minimumDepth = xmlTextReaderDepth(reader);

  int ret = xmlTextReaderMoveToNextAttribute(reader);
  xmlChar *name, *value;

  std::string source;
  std::string target;
  std::string type;
  bool extractedType = false; // neo4j includes these twice so only parse 1

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
        type          = std::string((const char*)value);
        extractedType = true;
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

  bool validEdge = !source.empty() && !target.empty();
  if (validEdge) {
    auto srcEntry  = nodeIndexes->find(source);
    auto destEntry = nodeIndexes->find(target);

    validEdge =
        srcEntry != nodeIndexes->end() && destEntry != nodeIndexes->end();
    if (validEdge) {
      sources->push_back(srcEntry->second);
      destinations->push_back((uint32_t)destEntry->second);
      out_indices_->at(srcEntry->second) =
          out_indices_->at(srcEntry->second) + 1;
    }
  }

  // parse "data" xml edges for properties
  ret = xmlTextReaderRead(reader);
  // will terminate when </edge> reached or an improper read
  while (ret == 1 && minimumDepth < xmlTextReaderDepth(reader)) {
    name = xmlTextReaderName(reader);
    if (name == NULL) {
      name = xmlStrdup(BAD_CAST "--");
    }
    // if elt is an xml node (we do not parse text for nodes)
    if (xmlTextReaderNodeType(reader) == 1) {
      // if elt is a "data" xml node read it in
      if (xmlStrEqual(name, BAD_CAST "data")) {
        auto property = processData(reader);
        if (property.first.size() > 0) {
          // we reserve the data fields label and labels for node/edge labels
          if (property.first == std::string("label") ||
              property.first == std::string("labels")) {
            if (!extractedType) {
              type          = property.second;
              extractedType = true;
            }
          } else if (property.first != std::string("IGNORE")) {
            if (validEdge) {
              auto keyIter = edgeKeys->find(property.first);
              // if an entry for the key does not already exist, make a default
              // entry for it
              if (keyIter == edgeKeys->end()) {
                addColumnBuilder(property.first, false, edgeKeys,
                                 edgeColumnBuilders, edgeSchemaVector,
                                 edgeChunks, arrow::utf8(), edges, chunkSize);
                keyIter = edgeKeys->find(property.first);
              }
              appendValue(edgeColumnBuilders->at(keyIter->second.columnID),
                          property.second, edges % chunkSize);
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
  if (validEdge && type.length() > 0) {
    auto entry = typeIndices->find(type);

    // if type does not already exist, add a column
    if (entry == typeIndices->end()) {
      addFalseColumnBuilder(type, typeIndices, edgeTypeColumnBuilders,
                            edgeTypeSchemaVector, typeChunks, arrow::boolean(),
                            edges, chunkSize);
      entry = typeIndices->find(type);
    }
    auto array = edgeTypeColumnBuilders->at(entry->second);
    auto st    = arrow::Status::OK();
    for (uint64_t i = array->length(); i < edges % chunkSize; i++) {
      st = array->Append(false);
    }
    st = array->Append(true);
    if (!st.ok()) {
      GALOIS_LOG_FATAL("Error adding value to arrow array: {}", st.ToString());
    }
  }
  return validEdge;
}

/*
 * reader should be pointing at the graph element before calling
 *
 * parses the graph structure from a GraphML file into Galois format
 */
void processGraphGraphML(
    xmlTextReaderPtr reader,
    std::unordered_map<std::string, KeyGraphML>* nodeKeys,
    std::unordered_map<std::string, KeyGraphML>* edgeKeys,
    ArrayBuilders* nodeColumnBuilders, ArrayBuilders* edgeColumnBuilders,
    ArrowFields* nodeSchemaVector, ArrowFields* edgeSchemaVector,
    std::vector<ArrowArrays>* nodeChunks, std::vector<ArrowArrays>* edgeChunks,
    std::unordered_map<std::string, size_t>* labelIndices,
    std::unordered_map<std::string, size_t>* typeIndices,
    BooleanBuilders* nodeLabelColumnBuilders,
    BooleanBuilders* edgeTypeColumnBuilders, ArrowFields* nodeLabelSchemaVector,
    ArrowFields* edgeTypeSchemaVector, std::vector<ArrowArrays>* labelChunks,
    std::vector<ArrowArrays>* typeChunks, std::vector<uint64_t>* out_indices_,
    std::vector<uint32_t>* out_dests_, std::vector<uint32_t>* sources,
    std::vector<uint32_t>* destinations, size_t chunkSize) {
  auto minimumDepth = xmlTextReaderDepth(reader);
  int ret           = xmlTextReaderRead(reader);

  // maps node IDs to node indexes
  std::unordered_map<std::string, size_t> nodeIndexes;

  size_t nodes = 0;
  size_t edges = 0;

  // will terminate when </graph> reached or an improper read
  while (ret == 1 && minimumDepth < xmlTextReaderDepth(reader)) {
    xmlChar* name;
    name = xmlTextReaderName(reader);
    if (name == NULL) {
      name = xmlStrdup(BAD_CAST "--");
    }
    // if elt is an xml node
    if (xmlTextReaderNodeType(reader) == 1) {
      // if elt is a "node" xml node read it in
      if (xmlStrEqual(name, BAD_CAST "node")) {
        if (processNode(reader, nodeKeys, nodeSchemaVector, nodeColumnBuilders,
                        nodeChunks, labelIndices, nodeLabelSchemaVector,
                        nodeLabelColumnBuilders, labelChunks, nodes, chunkSize,
                        &nodeIndexes)) {
          out_indices_->push_back(0);
          nodes++;
          if (nodes % chunkSize == 0) {
            evenOutTable(*nodeColumnBuilders, chunkSize);
            evenOutTable(*nodeLabelColumnBuilders, chunkSize);

            chunkBuilders(nodeChunks, nodeColumnBuilders);
            chunkBuilders(labelChunks, nodeLabelColumnBuilders);
          }
        }
      } else if (xmlStrEqual(name, BAD_CAST "edge")) {
        // if elt is an "egde" xml node read it in
        if (processEdge(reader, edgeKeys, edgeSchemaVector, edgeColumnBuilders,
                        edgeChunks, typeIndices, edgeTypeSchemaVector,
                        edgeTypeColumnBuilders, typeChunks, edges, chunkSize,
                        out_indices_, sources, destinations, &nodeIndexes)) {
          edges++;
          if (edges % chunkSize == 0) {
            evenOutTable(*edgeColumnBuilders, chunkSize);
            evenOutTable(*edgeTypeColumnBuilders, chunkSize);

            chunkBuilders(edgeChunks, edgeColumnBuilders);
            chunkBuilders(typeChunks, edgeTypeColumnBuilders);
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

  // add buffered rows before exiting
  if (nodes % chunkSize != 0) {
    evenOutTable(*nodeColumnBuilders, nodes % chunkSize);
    evenOutTable(*nodeLabelColumnBuilders, nodes % chunkSize);

    chunkBuilders(nodeChunks, nodeColumnBuilders);
    chunkBuilders(labelChunks, nodeLabelColumnBuilders);
  }
  if (edges % chunkSize != 0) {
    evenOutTable(*edgeColumnBuilders, edges % chunkSize);
    evenOutTable(*edgeTypeColumnBuilders, edges % chunkSize);

    chunkBuilders(edgeChunks, edgeColumnBuilders);
    chunkBuilders(typeChunks, edgeTypeColumnBuilders);
  }

  out_dests_->resize(edges, std::numeric_limits<uint32_t>::max());
}

} // end of unnamed namespace

namespace galois {

/// convertGraphML converts a GraphML file into katana form
///
/// \param infilename path to source graphml file
/// \returns arrow tables of node properties/labels, edge properties/types, and
/// csr topology
GraphComponents convertGraphML(const std::string& infilename,
                               const size_t chunkSize) {
  xmlTextReaderPtr reader;
  int ret = 0;

  std::unordered_map<std::string, KeyGraphML> nodeKeys;
  std::unordered_map<std::string, KeyGraphML> edgeKeys;

  ArrowFields nodeSchemaVector;
  ArrayBuilders nodeColumnBuilders;
  ArrowFields edgeSchemaVector;
  ArrayBuilders edgeColumnBuilders;

  std::vector<ArrowArrays> nodeChunks;
  std::vector<ArrowArrays> edgeChunks;

  std::unordered_map<std::string, size_t> labelIndices;
  std::unordered_map<std::string, size_t> typeIndices;

  ArrowFields nodeLabelSchemaVector;
  BooleanBuilders nodeLabelColumnBuilders;
  ArrowFields edgeTypeSchemaVector;
  BooleanBuilders edgeTypeColumnBuilders;

  std::vector<ArrowArrays> labelChunks;
  std::vector<ArrowArrays> typeChunks;

  // node's start of edge lists
  std::vector<uint64_t> out_indices_;
  // edge list of destinations
  std::vector<uint32_t> out_dests_;
  // list of sources of edges
  std::vector<uint32_t> sources;
  // list of destinations of edges
  std::vector<uint32_t> destinations;

  bool finishedGraph = false;

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
          KeyGraphML key = processKey(reader);
          if (key.id.size() > 0 && key.id != std::string("label") &&
              key.id != std::string("IGNORE")) {
            if (key.forNode) {
              key.columnID = nodeKeys.size();
              nodeKeys.insert(std::pair(key.id, key));
              addBuilder(&nodeColumnBuilders, &nodeSchemaVector, &key);
              nodeChunks.push_back(ArrowArrays{});
            } else if (key.forEdge) {
              key.columnID = edgeKeys.size();
              edgeKeys.insert(std::pair(key.id, key));
              addBuilder(&edgeColumnBuilders, &edgeSchemaVector, &key);
              edgeChunks.push_back(ArrowArrays{});
            }
          }
        } else if (xmlStrEqual(name, BAD_CAST "graph")) {
          processGraphGraphML(reader, &nodeKeys, &edgeKeys, &nodeColumnBuilders,
                              &edgeColumnBuilders, &nodeSchemaVector,
                              &edgeSchemaVector, &nodeChunks, &edgeChunks,
                              &labelIndices, &typeIndices,
                              &nodeLabelColumnBuilders, &edgeTypeColumnBuilders,
                              &nodeLabelSchemaVector, &edgeTypeSchemaVector,
                              &labelChunks, &typeChunks, &out_indices_,
                              &out_dests_, &sources, &destinations, chunkSize);
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

  // rearrange edges to match implicit edge IDs
  auto initialEdges = buildChunks(&edgeChunks);
  auto initialTypes = buildChunks(&typeChunks);
  auto edgesTables  = buildFinalEdges(
      initialEdges, &edgeSchemaVector, initialTypes, &edgeTypeSchemaVector,
      &out_indices_, &out_dests_, sources, destinations, chunkSize);
  std::shared_ptr<arrow::Table> finalEdgeTable = edgesTables.first;
  std::shared_ptr<arrow::Table> finalTypeTable = edgesTables.second;

  // build final nodes
  auto finalNodeTable  = buildTable(&nodeChunks, &nodeSchemaVector);
  auto finalLabelTable = buildTable(&labelChunks, &nodeLabelSchemaVector);

  // build topology
  auto topology = std::make_shared<galois::graphs::GraphTopology>();
  arrow::Status st;
  std::shared_ptr<arrow::UInt64Builder> topologyIndicesBuilder =
      std::make_shared<arrow::UInt64Builder>();
  st = topologyIndicesBuilder->AppendValues(out_indices_);
  if (!st.ok()) {
    GALOIS_LOG_FATAL("Error building topology: {}", st.ToString());
  }
  std::shared_ptr<arrow::UInt32Builder> topologyDestsBuilder =
      std::make_shared<arrow::UInt32Builder>();
  st = topologyDestsBuilder->AppendValues(out_dests_);
  if (!st.ok()) {
    GALOIS_LOG_FATAL("Error building topology: {}", st.ToString());
  }

  st = topologyIndicesBuilder->Finish(&topology->out_indices);
  if (!st.ok()) {
    GALOIS_LOG_FATAL("Error building arrow array for topology: {}",
                     st.ToString());
  }
  st = topologyDestsBuilder->Finish(&topology->out_dests);
  if (!st.ok()) {
    GALOIS_LOG_FATAL("Error building arrow array for topology: {}",
                     st.ToString());
  }

  return GraphComponents{finalNodeTable, finalLabelTable, finalEdgeTable,
                         finalTypeTable, topology};
}

/// convertNeo4jJSON converts a json file exported from neo4j into katana form
///
/// Example neo4j JSON:
/// {"type":"node","id":"2","labels":["Person"],"properties":{"born":1967,"name":"Carrie-Anne
/// Moss"}}
/// {"id":"0","type":"relationship","label":"ACTED_IN","properties":{"roles":["Neo"]},"start":{"id":"1","labels":["Person"]},"end":{"id":"0","labels":["Movie"]}}
///
/// Each node/edge of the graph is represented as a separate JSON object
/// with the fields: id, type, label/labels, properties(contains key value pairs
/// for the objects properties) Edges contain the addition fields: start, end
///
/// \param infilename path to source json file
/// \returns arrow tables of node properties/labels, edge properties/types, and
/// csr topology
GraphComponents convertNeo4jJSON(const std::string& infilename) {
  std::ifstream file(infilename);
  if (file) {
    std::unordered_map<std::string, size_t> nodeKeys;
    std::unordered_map<std::string, size_t> edgeKeys;

    ArrowFields nodeSchemaVector;
    StringBuilders nodeColumnBuilders;
    ArrowFields edgeSchemaVector;
    StringBuilders edgeColumnBuilders;

    std::unordered_map<std::string, size_t> labelIndices;
    std::unordered_map<std::string, size_t> typeIndices;

    ArrowFields nodeLabelSchemaVector;
    BooleanBuilders nodeLabelColumnBuilders;
    ArrowFields edgeTypeSchemaVector;
    BooleanBuilders edgeTypeColumnBuilders;

    std::vector<bool> nodePropertyNull;
    std::vector<bool> edgePropertyNull;
    std::vector<bool> nodeLabelNull;
    std::vector<bool> edgeTypeNull;

    std::unordered_map<std::string, size_t> nodeIndexes;

    // node's start of edge lists
    std::vector<uint64_t> out_indices_;
    // edge list of destinations
    std::vector<uint32_t> out_dests_;
    // list of sources of edges
    std::vector<uint32_t> sources;
    // list of destinations of edges
    std::vector<uint32_t> destinations;

    int64_t nodes = 0;
    int64_t edges = 0;

    std::string line;
    while (std::getline(file, line)) {
      std::stringstream ss(line);

      boost::property_tree::ptree root;
      boost::property_tree::read_json(ss, root);

      std::string type = root.get<std::string>("type", "");
      // verify child "properties" exists
      boost::optional<boost::property_tree::ptree&> child =
          root.get_child_optional("properties");
      if (type == std::string("node")) {
        boost::optional<boost::property_tree::ptree&> idChild =
            root.get_child_optional("id");
        bool validNode = false;
        if (idChild) {
          // add node's id
          std::string id = root.get<std::string>("id");
          validNode      = id.size() > 0;
          if (validNode) {
            nodeIndexes.insert(
                std::pair<std::string, size_t>(id, nodeIndexes.size()));
            out_indices_.push_back(0);
          }
        }
        if (validNode) {
          if (child) {
            // iterate over all node's properties
            for (boost::property_tree::ptree::value_type& prop :
                 root.get_child("properties")) {
              std::string propName  = prop.first;
              std::string propValue = prop.second.data();

              auto key = nodeKeys.find(propName);
              // if property does not already exist, add a column
              if (key == nodeKeys.end()) {
                addColumnBuilder(propName, &nodeKeys, &nodeColumnBuilders,
                                 &nodeSchemaVector, arrow::utf8(),
                                 &nodePropertyNull, nodes);
                key = nodeKeys.find(propName);
              }

              auto st = nodeColumnBuilders[key->second]->Append(
                  propValue.c_str(), propValue.length());
              if (!st.ok()) {
                GALOIS_LOG_FATAL("Error adding value to arrow array: {}",
                                 st.ToString());
              }
              nodePropertyNull[key->second] = false;
            }
          }
          boost::optional<boost::property_tree::ptree&> labels =
              root.get_child_optional("labels");
          if (labels) {
            // iterate over all node's labels
            for (boost::property_tree::ptree::value_type& label :
                 root.get_child("labels")) {
              std::string propValue = label.second.data();

              auto key = labelIndices.find(propValue);
              // if property does not already exist, add a column
              if (key == labelIndices.end()) {
                addFalseColumnBuilder(propValue, &labelIndices,
                                      &nodeLabelColumnBuilders,
                                      &nodeLabelSchemaVector, arrow::boolean(),
                                      &nodeLabelNull, nodes);
                key = labelIndices.find(propValue);
              }

              auto st = nodeLabelColumnBuilders[key->second]->Append(true);
              if (!st.ok()) {
                GALOIS_LOG_FATAL("Error adding value to arrow array: {}",
                                 st.ToString());
              }
              nodeLabelNull[key->second] = false;
            }
          }

          addNullsToBuilderAndReset(&nodeColumnBuilders, &nodePropertyNull);
          addFalsesToBuilderAndReset(&nodeLabelColumnBuilders, &nodeLabelNull);
          nodes++;
        }
      } else if (type == std::string("relationship")) {
        boost::optional<boost::property_tree::ptree&> srcChild =
            root.get_child_optional("start.id");
        boost::optional<boost::property_tree::ptree&> destChild =
            root.get_child_optional("end.id");
        bool validEdge = false;
        if (srcChild && destChild) {
          std::string source = root.get<std::string>("start.id");
          std::string target = root.get<std::string>("end.id");

          validEdge = source.size() > 0 && target.size() > 0;
          if (validEdge) {
            auto srcEntry  = nodeIndexes.find(source);
            auto destEntry = nodeIndexes.find(target);

            validEdge =
                srcEntry != nodeIndexes.end() && destEntry != nodeIndexes.end();
            if (validEdge) {
              sources.push_back(srcEntry->second);
              destinations.push_back(destEntry->second);
              out_indices_[srcEntry->second]++;
            }
          }
        }
        if (validEdge) {
          if (child) {
            // iterate over all relationship's properties
            for (boost::property_tree::ptree::value_type& prop :
                 root.get_child("properties")) {
              std::string propName  = prop.first;
              std::string propValue = prop.second.data();

              auto key = edgeKeys.find(propName);
              // if property does not already exist, add a column
              if (key == edgeKeys.end()) {
                addColumnBuilder(propName, &edgeKeys, &edgeColumnBuilders,
                                 &edgeSchemaVector, arrow::utf8(),
                                 &edgePropertyNull, edges);
                key = edgeKeys.find(propName);
              }

              auto st = edgeColumnBuilders[key->second]->Append(
                  propValue.c_str(), propValue.length());
              if (!st.ok()) {
                GALOIS_LOG_FATAL("Error adding value to arrow array: {}",
                                 st.ToString());
              }
              edgePropertyNull[key->second] = false;
            }
          }
          boost::optional<boost::property_tree::ptree&> label =
              root.get_child_optional("label");
          if (label) {
            // add edge's type
            std::string type = root.get<std::string>("label");

            auto key = typeIndices.find(type);
            // if property does not already exist, add a column
            if (key == typeIndices.end()) {
              addFalseColumnBuilder(type, &typeIndices, &edgeTypeColumnBuilders,
                                    &edgeTypeSchemaVector, arrow::boolean(),
                                    &edgeTypeNull, edges);
              key = typeIndices.find(type);
            }

            auto st = edgeTypeColumnBuilders[key->second]->Append(true);
            if (!st.ok()) {
              GALOIS_LOG_FATAL("Error adding value to arrow array: {}",
                               st.ToString());
            }
            edgeTypeNull[key->second] = false;
          }

          addNullsToBuilderAndReset(&edgeColumnBuilders, &edgePropertyNull);
          addFalsesToBuilderAndReset(&edgeTypeColumnBuilders, &edgeTypeNull);
          edges++;
        }
      }
    }
    out_dests_.resize(edges, std::numeric_limits<uint32_t>::max());

    auto edgesTables =
        buildFinalEdges(&edgeColumnBuilders, &edgeSchemaVector,
                        &edgeTypeColumnBuilders, &edgeTypeSchemaVector,
                        &out_indices_, &out_dests_, sources, destinations);
    auto finalEdgeTable = edgesTables.first;
    auto finalTypeTable = edgesTables.second;

    auto finalNodeTable = buildTable(&nodeColumnBuilders, &nodeSchemaVector);
    auto finalLabelTable =
        buildTable(&nodeLabelColumnBuilders, &nodeLabelSchemaVector);

    // build topology
    auto topology = std::make_shared<galois::graphs::GraphTopology>();
    arrow::Status st;
    std::shared_ptr<arrow::UInt64Builder> topologyIndicesBuilder =
        std::make_shared<arrow::UInt64Builder>();
    st = topologyIndicesBuilder->AppendValues(out_indices_);
    if (!st.ok()) {
      GALOIS_LOG_FATAL("Error building topology: {}", st.ToString());
    }
    std::shared_ptr<arrow::UInt32Builder> topologyDestsBuilder =
        std::make_shared<arrow::UInt32Builder>();
    st = topologyDestsBuilder->AppendValues(out_dests_);
    if (!st.ok()) {
      GALOIS_LOG_FATAL("Error building topology: {}", st.ToString());
    }

    st = topologyIndicesBuilder->Finish(&topology->out_indices);
    if (!st.ok()) {
      GALOIS_LOG_FATAL("Error building arrow array for topology: {}",
                       st.ToString());
    }
    st = topologyDestsBuilder->Finish(&topology->out_dests);
    if (!st.ok()) {
      GALOIS_LOG_FATAL("Error building arrow array for topology: {}",
                       st.ToString());
    }

    return GraphComponents{finalNodeTable, finalLabelTable, finalEdgeTable,
                           finalTypeTable, topology};
  }
  GALOIS_LOG_FATAL("Error opening file: {}", infilename);
}

/// convertNeo4jCSV converts a csv file exported from neo4j into katana form
///
/// Example neo4j CSV:
/// "_id","_labels","born","name","released","tagline","title","_start","_end","_type","roles","text"
/// "0",":Movie","","","1999","Welcome to the Real World","The Matrix",,,,,
/// "1",":Person","1964","Keanu Reeves","","","",,,,,
/// ,,,,,,,"1","0","ACTED_IN","[""Neo""]",""
///
/// The first line contains a csv header line
/// All data is quoted even null properties for that object type, i.e. Keanu
/// Reaves is a node so has a "" tagline value All data of the other type is
/// left empty, i.e. Keanu Reaves is a node so has null for roles and text
/// values The required fields are for nodes: _id, _labels; for edges: _start,
/// _end, _type
///
/// \param infilename path to source csv file
/// \returns arrow tables of node properties/labels, edge properties/types, and
/// csr topology
GraphComponents convertNeo4jCSV(const std::string& infilename) {
  ArrowFields nodeSchemaVector;
  StringBuilders nodeColumnBuilders;
  ArrowFields edgeSchemaVector;
  StringBuilders edgeColumnBuilders;

  std::unordered_map<std::string, size_t> labelIndices;
  std::unordered_map<std::string, size_t> typeIndices;

  ArrowFields nodeLabelSchemaVector;
  BooleanBuilders nodeLabelColumnBuilders;
  ArrowFields edgeTypeSchemaVector;
  BooleanBuilders edgeTypeColumnBuilders;

  std::vector<bool> nodeLabelNull;
  std::vector<bool> edgeTypeNull;

  std::unordered_map<std::string, size_t> nodeIndexes;

  // node's start of edge lists
  std::vector<uint64_t> out_indices_;
  // edge list of destinations
  std::vector<uint32_t> out_dests_;
  // list of sources of edges
  std::vector<uint32_t> sources;
  // list of destinations of edges
  std::vector<uint32_t> destinations;

  std::unordered_map<size_t, size_t> nodeKeys;
  std::unordered_map<size_t, size_t> edgeKeys;

  csv::CSVReader reader(infilename);

  auto headerInfo     = extractHeaderCSV(&reader, &nodeSchemaVector,
                                     &nodeColumnBuilders, &edgeSchemaVector,
                                     &edgeColumnBuilders, &nodeKeys, &edgeKeys);
  auto headers        = headerInfo.first;
  size_t id_index     = headerInfo.second[0];
  size_t labels_index = headerInfo.second[1];
  size_t start_index  = headerInfo.second[2];
  size_t end_index    = headerInfo.second[3];
  size_t type_index   = headerInfo.second[4];

  size_t nodes = 0;
  size_t edges = 0;

  std::vector<std::string> fields;
  for (csv::CSVRow& row : reader) {
    for (csv::CSVField& field : row) {
      std::string value = field.get<>();

      fields.push_back(value);
    }
    // deal with nodes
    if (fields[id_index].length() > 0) {
      nodeIndexes.insert(
          std::pair<std::string, size_t>(fields[id_index], nodeIndexes.size()));

      // extract labels
      if (fields[labels_index].length() > 0) {
        std::vector<std::string> labels;
        // erase prepended ':' if it exists
        std::string data = fields[labels_index];
        if (data.front() == ':') {
          data.erase(0, 1);
        }
        boost::split(labels, data, boost::is_any_of(":"));

        // add labels if they exists
        if (labels.size() > 0) {
          for (std::string label : labels) {
            auto entry = labelIndices.find(label);

            // if label does not already exist, add a column
            if (entry == labelIndices.end()) {
              addFalseColumnBuilder(label, &labelIndices,
                                    &nodeLabelColumnBuilders,
                                    &nodeLabelSchemaVector, arrow::boolean(),
                                    &nodeLabelNull, nodes);
              entry = labelIndices.find(label);
            }
            auto st = nodeLabelColumnBuilders[entry->second]->Append(true);
            if (!st.ok()) {
              GALOIS_LOG_FATAL("Error adding value to arrow array builder: {}",
                               st.ToString());
            }
            nodeLabelNull[entry->second] = false;
          }
        }
        addFalsesToBuilderAndReset(&nodeLabelColumnBuilders, &nodeLabelNull);
      }
      // extract properties
      for (auto i : nodeKeys) {
        if (fields[i.first].length() > 0) {
          auto st = nodeColumnBuilders[i.second]->Append(
              fields[i.first].c_str(), fields[i.first].length());
          if (!st.ok()) {
            GALOIS_LOG_FATAL("Error adding value to arrow array builder: {}",
                             st.ToString());
          }
        } else {
          auto st = nodeColumnBuilders[i.second]->AppendNull();
          if (!st.ok()) {
            GALOIS_LOG_FATAL("Error adding null to arrow array builder: {}",
                             st.ToString());
          }
        }
      }
      out_indices_.push_back(0);
      nodes++;
    }
    // deal with edges
    else if (fields[start_index].length() > 0 &&
             fields[end_index].length() > 0) {
      auto srcEntry  = nodeIndexes.find(fields[start_index]);
      auto destEntry = nodeIndexes.find(fields[end_index]);

      bool validEdge =
          srcEntry != nodeIndexes.end() && destEntry != nodeIndexes.end();
      if (validEdge) {
        sources.push_back(srcEntry->second);
        destinations.push_back(destEntry->second);
        out_indices_[srcEntry->second]++;
      }
      if (validEdge) {
        for (auto i : edgeKeys) {
          if (fields[i.first].length() > 0) {
            auto st = edgeColumnBuilders[i.second]->Append(
                fields[i.first].c_str(), fields[i.first].length());
            if (!st.ok()) {
              GALOIS_LOG_FATAL("Error adding value to arrow array builder: {}",
                               st.ToString());
            }
          } else {
            auto st = edgeColumnBuilders[i.second]->AppendNull();
            if (!st.ok()) {
              GALOIS_LOG_FATAL("Error adding null to arrow array builder: {}",
                               st.ToString());
            }
          }
        }

        // add edge's type
        std::string type = fields[type_index];
        if (type.length() > 0) {
          auto key = typeIndices.find(type);
          // if property does not already exist, add a column
          if (key == typeIndices.end()) {
            addFalseColumnBuilder(type, &typeIndices, &edgeTypeColumnBuilders,
                                  &edgeTypeSchemaVector, arrow::boolean(),
                                  &edgeTypeNull, edges);
            key = typeIndices.find(type);
          }

          auto st = edgeTypeColumnBuilders[key->second]->Append(true);
          if (!st.ok()) {
            GALOIS_LOG_FATAL("Error adding value to arrow array builder: {}",
                             st.ToString());
          }
          edgeTypeNull[key->second] = false;
        }
        addFalsesToBuilderAndReset(&edgeTypeColumnBuilders, &edgeTypeNull);
        edges++;
      }
    }
    fields.clear();
  }
  out_dests_.resize(edges, std::numeric_limits<uint32_t>::max());

  auto edgesTables = buildFinalEdges(
      &edgeColumnBuilders, &edgeSchemaVector, &edgeTypeColumnBuilders,
      &edgeTypeSchemaVector, &out_indices_, &out_dests_, sources, destinations);
  auto finalEdgeTable = edgesTables.first;
  auto finalTypeTable = edgesTables.second;

  auto finalNodeTable = buildTable(&nodeColumnBuilders, &nodeSchemaVector);
  auto finalLabelTable =
      buildTable(&nodeLabelColumnBuilders, &nodeLabelSchemaVector);

  // build topology
  auto topology = std::make_shared<galois::graphs::GraphTopology>();
  arrow::Status st;
  std::shared_ptr<arrow::UInt64Builder> topologyIndicesBuilder =
      std::make_shared<arrow::UInt64Builder>();
  st = topologyIndicesBuilder->AppendValues(out_indices_);
  if (!st.ok()) {
    GALOIS_LOG_FATAL("Error building topology arrow array: {}", st.ToString());
  }
  std::shared_ptr<arrow::UInt32Builder> topologyDestsBuilder =
      std::make_shared<arrow::UInt32Builder>();
  st = topologyDestsBuilder->AppendValues(out_dests_);
  if (!st.ok()) {
    GALOIS_LOG_FATAL("Error building topology arrow array: {}", st.ToString());
  }

  st = topologyIndicesBuilder->Finish(&topology->out_indices);
  if (!st.ok()) {
    GALOIS_LOG_FATAL("Error building arrow array for topology: {}",
                     st.ToString());
  }
  st = topologyDestsBuilder->Finish(&topology->out_dests);
  if (!st.ok()) {
    GALOIS_LOG_FATAL("Error building arrow array for topology: {}",
                     st.ToString());
  }

  return GraphComponents{finalNodeTable, finalLabelTable, finalEdgeTable,
                         finalTypeTable, topology};
}

/// convertToPropertyGraphAndWrite formally builds katana form via
/// PropertyFileGraph from imported components and writes the result to target
/// directory
///
/// \param graphComps imported components to convert into a PropertyFileGraph
/// \param dir local FS directory or s3 directory to write PropertyFileGraph to
void convertToPropertyGraphAndWrite(const GraphComponents& graphComps,
                                    const std::string& dir) {
  galois::graphs::PropertyFileGraph graph;

  auto result = graph.SetTopology(*graphComps.topology);
  if (!result) {
    GALOIS_LOG_FATAL("Error adding topology: {}", result.error());
  }

  if (graphComps.nodeProperties->num_columns() > 0) {
    result = graph.AddNodeProperties(graphComps.nodeProperties);
    if (!result) {
      GALOIS_LOG_FATAL("Error adding node properties: {}", result.error());
    }
  }
  if (graphComps.nodeLabels->num_columns() > 0) {
    result = graph.AddNodeProperties(graphComps.nodeLabels);
    if (!result) {
      GALOIS_LOG_FATAL("Error adding node labels: {}", result.error());
    }
  }
  if (graphComps.edgeProperties->num_columns() > 0) {
    result = graph.AddEdgeProperties(graphComps.edgeProperties);
    if (!result) {
      GALOIS_LOG_FATAL("Error adding edge properties: {}", result.error());
    }
  }
  if (graphComps.edgeTypes->num_columns() > 0) {
    result = graph.AddEdgeProperties(graphComps.edgeTypes);
    if (!result) {
      GALOIS_LOG_FATAL("Error adding edge types: {}", result.error());
    }
  }

  std::string metaFile = dir;
  if (metaFile[metaFile.length() - 1] == '/') {
    metaFile += "meta";
  } else {
    metaFile += "/meta";
  }

  result = graph.Write(metaFile);
  if (!result) {
    GALOIS_LOG_FATAL("Error writing to fs: {}", result.error());
  }
}

} // end of namespace galois
