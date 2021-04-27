#include "graph-properties-convert-mongodb.h"

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
#include <boost/lexical_cast.hpp>

#include "katana/ErrorCode.h"
#include "katana/Galois.h"
#include "katana/GraphMLSchema.h"
#include "katana/Logging.h"
#include "katana/PropertyGraph.h"
#include "katana/SharedMemSys.h"
#include "katana/Threads.h"

using katana::GraphComponents;
using katana::ImportData;
using katana::ImportDataType;
using katana::LabelRule;
using katana::PropertyKey;

namespace {

struct CollectionFields {
  std::map<std::string, PropertyKey> property_fields;
  std::set<std::string> embedded_nodes;
  std::set<std::string> embedded_relations;
};

struct MongoClient {
  mongoc_client_t* client;

  MongoClient(mongoc_client_t* client_) : client(client_) {}
  ~MongoClient() { mongoc_client_destroy(client); }
};

struct bson_value_t_wrapper {
  bson_value_t val;
};

/******************************/
/* Functions for parsing data */
/******************************/

template <typename T>
std::optional<T>
RetrievePrimitive(const bson_value_t* elt) {
  switch (elt->value_type) {
  case BSON_TYPE_INT64:
    return static_cast<T>(elt->value.v_int64);
  case BSON_TYPE_INT32:
    return static_cast<T>(elt->value.v_int32);
  case BSON_TYPE_DOUBLE:
    return static_cast<T>(elt->value.v_double);
  case BSON_TYPE_BOOL:
    return static_cast<T>(elt->value.v_bool);
  case BSON_TYPE_UTF8:
    try {
      return boost::lexical_cast<T>(
          std::string(elt->value.v_utf8.str, elt->value.v_utf8.len));
    } catch (const boost::bad_lexical_cast&) {
      // std::cout << "A primitive value was not retrieved: " <<
      // elt.get_utf8().value.data() << "\n";
      return std::nullopt;
    }
  default:
    // std::cout << "A primitive value was not retrieved\n";
    return std::nullopt;
  }
}

std::optional<std::string>
RetrieveString(const bson_value_t* elt) {
  switch (elt->value_type) {
  case BSON_TYPE_INT64:
    return boost::lexical_cast<std::string>(elt->value.v_int64);
  case BSON_TYPE_INT32:
    return boost::lexical_cast<std::string>(elt->value.v_int32);
  case BSON_TYPE_DOUBLE:
    return boost::lexical_cast<std::string>(elt->value.v_double);
  case BSON_TYPE_BOOL:
    return boost::lexical_cast<std::string>(elt->value.v_bool);
  case BSON_TYPE_UTF8:
    return std::string(elt->value.v_utf8.str, elt->value.v_utf8.len);
  default:
    return std::nullopt;
  }
}

std::optional<int64_t>
RetrieveDate(const bson_value_t* elt) {
  if (elt->value_type == BSON_TYPE_DATE_TIME) {
    return elt->value.v_datetime;
  }
  return std::nullopt;
}

std::optional<uint8_t>
RetrieveStruct(const bson_value_t* elt) {
  if (elt->value_type == BSON_TYPE_DOCUMENT) {
    return 1;
  }
  return std::nullopt;
}

template <typename T>
ImportData
RetrievePrimitiveList(ImportDataType type, bson_iter_t* val) {
  ImportData data{type, true};
  std::vector<T> list;

  while (bson_iter_next(val)) {
    auto elt = bson_iter_value(val);
    auto res = RetrievePrimitive<T>(elt);
    if (res) {
      list.emplace_back(res.value());
    }
  }
  data.value = list;
  return data;
}

ImportData
RetrieveStringList(ImportDataType type, bson_iter_t* val) {
  ImportData data{type, true};
  std::vector<std::string> list;

  while (bson_iter_next(val)) {
    auto elt = bson_iter_value(val);
    auto res = RetrieveString(elt);
    if (res) {
      list.emplace_back(res.value());
    }
  }
  data.value = list;
  return data;
}

ImportData
RetrieveDateList(ImportDataType type, bson_iter_t* val) {
  ImportData data{type, true};
  std::vector<int64_t> timestamps;

  while (bson_iter_next(val)) {
    auto elt = bson_iter_value(val);
    if (elt->value_type == BSON_TYPE_DATE_TIME) {
      timestamps.emplace_back(elt->value.v_datetime);
    }
  }
  data.value = timestamps;
  return data;
}

/************************************************/
/* Functions for adding values to arrow builder */
/************************************************/

template <typename Fn>
ImportData
ResolveOptional(
    ImportDataType type, bool is_list, const bson_value_t* val, Fn resolver) {
  ImportData data{type, is_list};
  auto res = resolver(val);
  if (!res) {
    data.type = ImportDataType::kUnsupported;
  } else {
    data.value = res.value();
  }
  return data;
}

ImportData
ResolveListValue(const bson_value_t* array_ptr, ImportDataType type) {
  bson_t array;
  bson_iter_t val;
  if (bson_init_static(
          &array, array_ptr->value.v_doc.data,
          array_ptr->value.v_doc.data_len)) {
    if (!bson_iter_init(&val, &array)) {
      return ImportData{ImportDataType::kUnsupported, true};
    }
  } else {
    return ImportData{ImportDataType::kUnsupported, true};
  }

  switch (type) {
  case ImportDataType::kString:
    return RetrieveStringList(type, &val);
  case ImportDataType::kInt64:
    return RetrievePrimitiveList<int64_t>(type, &val);
  case ImportDataType::kInt32:
    return RetrievePrimitiveList<int32_t>(type, &val);
  case ImportDataType::kDouble:
    return RetrievePrimitiveList<double>(type, &val);
  case ImportDataType::kFloat:
    return RetrievePrimitiveList<float>(type, &val);
  case ImportDataType::kBoolean:
    return RetrievePrimitiveList<bool>(type, &val);
  case ImportDataType::kTimestampMilli:
    return RetrieveDateList(type, &val);
  default:
    return ImportData{ImportDataType::kUnsupported, true};
  }
}

ImportData
ResolveValue(const bson_value_t* val, ImportDataType type, bool is_list) {
  if (is_list) {
    return ResolveListValue(val, type);
  }
  switch (type) {
  case ImportDataType::kString:
    return ResolveOptional(type, is_list, val, RetrieveString);
  case ImportDataType::kInt64:
    return ResolveOptional(type, is_list, val, RetrievePrimitive<int64_t>);
  case ImportDataType::kInt32:
    return ResolveOptional(type, is_list, val, RetrievePrimitive<int32_t>);
  case ImportDataType::kDouble:
    return ResolveOptional(type, is_list, val, RetrievePrimitive<double>);
  case ImportDataType::kFloat:
    return ResolveOptional(type, is_list, val, RetrievePrimitive<float>);
  case ImportDataType::kBoolean:
    return ResolveOptional(type, is_list, val, RetrievePrimitive<bool>);
  case ImportDataType::kTimestampMilli:
    return ResolveOptional(type, is_list, val, RetrieveDate);
  case ImportDataType::kStruct:
    return ResolveOptional(type, is_list, val, RetrieveStruct);
  default:
    return ImportData{ImportDataType::kUnsupported, is_list};
  }
}

/*****************************************/
/* Helper functions for MongoDB C Driver */
/*****************************************/

std::string
ExtractOid(const bson_value_t* elt) {
  const bson_oid_t* oid = &elt->value.v_oid;
  char str[25];
  bson_oid_to_string(oid, str);
  return std::string(str);
}

std::string
ExtractOid(const bson_iter_t& elt) {
  const bson_oid_t* oid = bson_iter_oid(&elt);
  char str[25];
  bson_oid_to_string(oid, str);
  return std::string(str);
}

bson_type_t
ExtractBsonArrayType(const bson_value_t* val) {
  bson_type_t type = BSON_TYPE_NULL;
  bson_t array;
  if (bson_init_static(
          &array, val->value.v_doc.data, val->value.v_doc.data_len)) {
    bson_iter_t arr_iter;
    if (bson_iter_init(&arr_iter, &array)) {
      if (bson_iter_next(&arr_iter)) {
        auto elt = bson_iter_value(&arr_iter);
        type = elt->value_type;
      }
    }
  }
  return type;
}

/***********************************/
/* Functions for importing MongoDB */
/***********************************/

// extract the type from a bson type
ImportDataType
ExtractTypeMongoDB(bson_type_t value) {
  switch (value) {
  case BSON_TYPE_UTF8:
    return ImportDataType::kString;
  case BSON_TYPE_DOUBLE:
    return ImportDataType::kDouble;
  case BSON_TYPE_INT64:
    return ImportDataType::kInt64;
  case BSON_TYPE_INT32:
    return ImportDataType::kInt32;
  case BSON_TYPE_BOOL:
    return ImportDataType::kBoolean;
  case BSON_TYPE_DATE_TIME:
    return ImportDataType::kTimestampMilli;
  case BSON_TYPE_DOCUMENT:
    return ImportDataType::kStruct;
  default:
    return ImportDataType::kUnsupported;
  }
}

PropertyKey
ProcessElement(const bson_value_t* elt, const std::string& name) {
  auto elt_type = elt->value_type;
  bool is_list = elt_type == BSON_TYPE_ARRAY;
  if (is_list) {
    elt_type = ExtractBsonArrayType(elt);
    if (elt_type == BSON_TYPE_DOCUMENT) {
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

/****************************************/
/* MongoDB functions for handling edges */
/****************************************/

void
HandleEmbeddedEdgeStruct(
    katana::PropertyGraphBuilder* builder, const bson_value_t* doc_ptr,
    const std::string& prefix) {
  bson_t doc;
  bson_iter_t iter;
  if (bson_init_static(
          &doc, doc_ptr->value.v_doc.data, doc_ptr->value.v_doc.data_len)) {
    if (bson_iter_init(&iter, &doc)) {
      // handle document
      while (bson_iter_next(&iter)) {
        const bson_value_t* elt = bson_iter_value(&iter);
        auto elt_name =
            prefix +
            std::string(bson_iter_key(&iter), bson_iter_key_len(&iter));

        // since all edge cases have been checked, we can add this property
        builder->AddValue(
            elt_name, [&]() { return ProcessElement(elt, elt_name); },
            [&elt](ImportDataType type, bool is_list) {
              return ResolveValue(elt, type, is_list);
            });

        if (elt->value_type == BSON_TYPE_DOCUMENT) {
          auto new_prefix = elt_name + ".";
          HandleEmbeddedEdgeStruct(builder, bson_iter_value(&iter), new_prefix);
        }
      }
    }
  }
}

/****************************************/
/* MongoDB functions for handling nodes */
/****************************************/

void
HandleEmbeddedDocuments(
    katana::PropertyGraphBuilder* builder,
    const std::vector<std::pair<std::string, bson_value_t_wrapper>>& docs,
    const std::string& parent_name, size_t parent_index) {
  for (auto [name, elt_wrapper] : docs) {
    auto elt = &elt_wrapper.val;
    if (elt->value_type == BSON_TYPE_DOCUMENT) {
      std::string edge_type = parent_name + "_" + name;

      bson_t doc;
      if (bson_init_static(
              &doc, elt->value.v_doc.data, elt->value.v_doc.data_len)) {
        builder->AddEdge(
            static_cast<uint32_t>(parent_index),
            static_cast<uint32_t>(builder->GetNodes()), edge_type);
        katana::HandleNodeDocumentMongoDB(builder, &doc, name);
      }
    } else {
      bson_t array;
      if (bson_init_static(
              &array, elt->value.v_doc.data, elt->value.v_doc.data_len)) {
        bson_iter_t arr_iter;
        if (bson_iter_init(&arr_iter, &array)) {
          while (bson_iter_next(&arr_iter)) {
            auto doc_ptr = bson_iter_value(&arr_iter);
            if (doc_ptr->value_type == BSON_TYPE_DOCUMENT) {
              bson_t doc;
              if (bson_init_static(
                      &doc, doc_ptr->value.v_doc.data,
                      doc_ptr->value.v_doc.data_len)) {
                builder->AddEdge(
                    static_cast<uint32_t>(parent_index),
                    static_cast<uint32_t>(builder->GetNodes()), name);
                katana::HandleNodeDocumentMongoDB(builder, &doc, name);
              }
            }
          }
        }
      }
    }
    bson_value_destroy(elt);
  }
}

bool
HandleNonPropertyNodeElement(
    katana::PropertyGraphBuilder* builder,
    std::vector<std::pair<std::string, bson_value_t_wrapper>>* docs,
    const std::string& name, const bson_value_t* elt,
    const std::string& collection_name) {
  auto elt_type = elt->value_type;

  // initialize new node
  if (name == std::string("_id")) {
    builder->AddNodeId(ExtractOid(elt));
    return true;
  }
  // if elt is an ObjectID (foreign key), add a property-less edge
  if (elt_type == BSON_TYPE_OID) {
    std::string edge_type = collection_name + "_" + name;
    auto oid = ExtractOid(elt);
    builder->AddOutgoingEdge(oid, edge_type);
    return true;
  }
  // if elt is an array of embedded documents defer adding them to later
  if (elt_type == BSON_TYPE_ARRAY) {
    auto array_type = ExtractBsonArrayType(elt);
    if (array_type == BSON_TYPE_DOCUMENT) {
      bson_value_t copy;
      bson_value_copy(elt, &copy);
      docs->emplace_back(std::pair<std::string, bson_value_t_wrapper>(
          name, bson_value_t_wrapper{std::move(copy)}));
      return true;
    }
    if (array_type == BSON_TYPE_OID) {
      bson_t array;
      if (bson_init_static(
              &array, elt->value.v_doc.data, elt->value.v_doc.data_len)) {
        bson_iter_t arr_iter;
        if (bson_iter_init(&arr_iter, &array)) {
          while (bson_iter_next(&arr_iter)) {
            auto val = bson_iter_value(&arr_iter);
            auto oid = ExtractOid(val);
            builder->AddOutgoingEdge(oid, name);
          }
        }
      }
      return true;
    }
  }
  return false;
}

void
HandleEmbeddedNodeStruct(
    katana::PropertyGraphBuilder* builder,
    std::vector<std::pair<std::string, bson_value_t_wrapper>>* docs,
    const std::string& name, const bson_value_t* doc_ptr,
    const std::string& prefix) {
  bson_t doc;
  bson_iter_t iter;
  if (bson_init_static(
          &doc, doc_ptr->value.v_doc.data, doc_ptr->value.v_doc.data_len)) {
    if (bson_iter_init(&iter, &doc)) {
      // handle document
      while (bson_iter_next(&iter)) {
        const bson_value_t* elt = bson_iter_value(&iter);
        std::string struct_name{bson_iter_key(&iter), bson_iter_key_len(&iter)};
        auto elt_name = prefix + struct_name;

        if (HandleNonPropertyNodeElement(
                builder, docs, struct_name, elt, name)) {
          continue;
        }

        // since all edge cases have been checked, we can add this property
        builder->AddValue(
            elt_name, [&]() { return ProcessElement(elt, elt_name); },
            [&elt](ImportDataType type, bool is_list) {
              return ResolveValue(elt, type, is_list);
            });

        if (elt->value_type == BSON_TYPE_DOCUMENT) {
          auto new_prefix = elt_name + ".";
          HandleEmbeddedNodeStruct(builder, docs, elt_name, elt, new_prefix);
        }
      }
    }
  }
}

/**********************************/
/* Functions for MongoDB querying */
/**********************************/

// mongoc_init() should be called before this function
mongoc_client_t*
GetMongoClient(const char* uri_string) {
  bson_error_t error;
  mongoc_uri_t* uri = mongoc_uri_new_with_error(uri_string, &error);
  if (!uri) {
    KATANA_LOG_FATAL(
        "Failed to parse URI: {}\n"
        "Error message: {}\n",
        uri_string, error.message);
  }
  mongoc_client_t* client = mongoc_client_new_from_uri(uri);
  if (!client) {
    KATANA_LOG_FATAL(
        "Could not connect to URI: {}\n"
        "Error message: {}\n",
        uri_string, error.message);
  }
  mongoc_client_set_appname(client, "graph-properties-convert");
  mongoc_uri_destroy(uri);

  return client;
}

std::vector<std::string>
GetCollectionNames(mongoc_database_t* database) {
  std::vector<std::string> coll_names;

  bson_error_t error;
  char** collection_names =
      mongoc_database_get_collection_names_with_opts(database, nullptr, &error);

  for (size_t i = 0; collection_names[i] != nullptr; i++) {
    coll_names.emplace_back(collection_names[i]);
  }
  bson_strfreev(collection_names);

  return coll_names;
}

template <typename T>
void
QueryEntireCollection(
    mongoc_database_t* database, const bson_t** document,
    const std::string& coll_name, T document_op) {
  bson_error_t error;
  auto collection = mongoc_database_get_collection(database, coll_name.c_str());
  bson_t filter;
  bson_init(&filter);
  auto cursor =
      mongoc_collection_find_with_opts(collection, &filter, nullptr, nullptr);

  while (mongoc_cursor_next(cursor, document)) {
    document_op();
  }
  if (mongoc_cursor_error(cursor, &error)) {
    KATANA_LOG_ERROR(
        "An error occurred with a mongodb cursor: {}", error.message);
  }

  bson_destroy(&filter);
  mongoc_cursor_destroy(cursor);
  mongoc_collection_destroy(collection);
}

/***************************************/
/* Functions for MongoDB preprocessing */
/***************************************/

/* A document is not an edge if:
 *    - it contains an array of ObjectIDs
 *    - it contains an array of Documents
 *    - it does not have exactly 2 ObjectIDs excluding its own ID
 */
bool
CheckIfDocumentIsEdge(const bson_t* doc) {
  uint32_t oid_count = 0;

  bson_iter_t iter;
  if (!bson_iter_init(&iter, doc)) {
    return false;
  }
  // handle document
  while (bson_iter_next(&iter)) {
    const bson_value_t* elt = bson_iter_value(&iter);
    std::string name{bson_iter_key(&iter), bson_iter_key_len(&iter)};

    if (name == std::string("_id")) {
      continue;
    }

    switch (elt->value_type) {
    case BSON_TYPE_OID: {
      oid_count++;
      if (oid_count > 2) {
        return false;
      }
      break;
    }
    case BSON_TYPE_ARRAY: {
      auto array_type = ExtractBsonArrayType(elt);
      if (array_type == BSON_TYPE_DOCUMENT || array_type == BSON_TYPE_OID) {
        return false;
      }
      break;
    }
    default: {
      break;
    }
    }
  }
  return oid_count == 2;
}

bool
CheckIfCollectionIsEdge(mongoc_collection_t* coll) {
  {
    // findOne and check it exists
    bson_t* opts;
    opts = BCON_NEW("limit", BCON_INT64(1));
    bson_t filter;
    bson_init(&filter);
    auto cursor =
        mongoc_collection_find_with_opts(coll, &filter, opts, nullptr);
    bson_destroy(opts);
    bson_destroy(&filter);

    const bson_t* doc;
    if (mongoc_cursor_next(cursor, &doc)) {
      if (!CheckIfDocumentIsEdge(doc)) {
        mongoc_cursor_destroy(cursor);
        return false;
      }
    } else {
      mongoc_cursor_destroy(cursor);
      return false;
    }
    mongoc_cursor_destroy(cursor);
  }

  // randomly sample 1000 documents from collection
  bson_t* pipeline = BCON_NEW(
      "pipeline", "[", "{", "$sample", "{", "size", BCON_INT32(1000), "}", "}",
      "]");
  auto docs = mongoc_collection_aggregate(
      coll, MONGOC_QUERY_NONE, pipeline, nullptr, nullptr);
  bson_destroy(pipeline);

  const bson_t* doc;
  while (mongoc_cursor_next(docs, &doc)) {
    if (!CheckIfDocumentIsEdge(doc)) {
      mongoc_cursor_destroy(docs);
      return false;
    }
  }
  mongoc_cursor_destroy(docs);
  return true;
}

void
ExtractDocumentFields(
    const bson_t* doc, CollectionFields* fields, const std::string& prefix,
    const std::string& parent_name) {
  bson_iter_t iter;
  if (!bson_iter_init(&iter, doc)) {
    return;
  }
  // handle document
  while (bson_iter_next(&iter)) {
    const bson_value_t* elt = bson_iter_value(&iter);
    std::string name{bson_iter_key(&iter), bson_iter_key_len(&iter)};

    if (name == std::string("_id")) {
      continue;
    }
    if (elt->value_type == BSON_TYPE_OID) {
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
        if (elt->value_type == BSON_TYPE_ARRAY) {
          auto array_type = ExtractBsonArrayType(elt);
          if (array_type == BSON_TYPE_OID) {
            fields->embedded_relations.insert(name);
          } else if (array_type == BSON_TYPE_DOCUMENT) {
            fields->embedded_nodes.insert(name);
            fields->embedded_relations.insert(name);
          }
        }
      }
    }
    if (elt->value_type == BSON_TYPE_DOCUMENT) {
      auto new_prefix = elt_name + ".";
      bson_t doc;
      if (bson_init_static(
              &doc, elt->value.v_doc.data, elt->value.v_doc.data_len)) {
        ExtractDocumentFields(&doc, fields, new_prefix, name);
      }
    }
  }
}

void
ExtractCollectionFields(
    mongoc_collection_t* coll, CollectionFields* fields,
    const std::string& coll_name) {
  {
    // findOne and check it exists
    bson_t* opts;
    opts = BCON_NEW("limit", BCON_INT64(1));
    bson_t filter;
    bson_init(&filter);
    auto cursor =
        mongoc_collection_find_with_opts(coll, &filter, opts, nullptr);
    bson_destroy(&filter);
    bson_destroy(opts);

    const bson_t* doc;
    if (mongoc_cursor_next(cursor, &doc)) {
      ExtractDocumentFields(doc, fields, std::string(""), coll_name);
    } else {
      // empty collection so skip it
      mongoc_cursor_destroy(cursor);
      return;
    }
    mongoc_cursor_destroy(cursor);
  }

  // randomly sample 1000 documents from collection
  bson_t* pipeline = BCON_NEW(
      "pipeline", "[", "{", "$sample", "{", "size", BCON_INT32(1000), "}", "}",
      "]");
  auto docs = mongoc_collection_aggregate(
      coll, MONGOC_QUERY_NONE, pipeline, nullptr, nullptr);
  bson_destroy(pipeline);

  const bson_t* doc;
  while (mongoc_cursor_next(docs, &doc)) {
    ExtractDocumentFields(doc, fields, std::string(""), coll_name);
  }
  mongoc_cursor_destroy(docs);
}

/**********************************************/
/* Functions to get user input for conversion */
/**********************************************/

bool
GetUserBool(const std::string& prompt) {
  while (true) {
    std::cout << prompt << " (y/n): ";
    std::string res;
    std::getline(std::cin, res);

    if (res.empty()) {
      std::cout << "Please enter yes or no\n";
    } else if (res[0] == 'y' || res[0] == 'Y') {
      return true;
    } else if (res[0] == 'n' || res[0] == 'N') {
      return false;
    } else {
      std::cout << "Please enter yes or no\n";
    }
  }
}

std::vector<std::string>
GetUserInputForEdges(
    const std::vector<std::string>& possible_edges,
    std::vector<std::string>* nodes) {
  std::vector<std::string> edges;

  for (const std::string& coll_name : possible_edges) {
    if (GetUserBool("Treat " + coll_name + " as an edge")) {
      edges.emplace_back(coll_name);
    } else {
      nodes->emplace_back(coll_name);
    }
  }
  return edges;
}

// TODO support multiple labels per collection
template <typename T>
void
GetUserInputForLabels(
    xmlTextWriterPtr writer, const T& coll_names, bool for_node) {
  for (const std::string& coll_name : coll_names) {
    std::cout << "Choose label for " << coll_name << " (" << coll_name << "): ";
    std::string res;
    std::getline(std::cin, res);

    std::string existing_key;
    if (res.empty()) {
      LabelRule rule{coll_name, for_node, !for_node, coll_name};
      katana::graphml::WriteGraphmlRule(writer, rule);
    } else {
      LabelRule rule{coll_name, for_node, !for_node, res};
      katana::graphml::WriteGraphmlRule(writer, rule);
    }
  }
}

void
GetUserInputForFields(
    xmlTextWriterPtr writer, CollectionFields doc_fields, bool for_node) {
  auto fields = doc_fields.property_fields;
  std::cout << "Total Detected Fields: " << fields.size() << "\n";
  for (auto& [name, key] : fields) {
    std::cout << "Choose property name for field " << name << " (" << name
              << "): ";
    std::string res;
    std::getline(std::cin, res);

    if (!res.empty()) {
      key.name = res;
    }

    bool done = false;
    auto type_name = katana::graphml::TypeName(key.type);
    while (!done) {
      std::cout << "Choose type for field " << name << " (" << type_name;
      if (key.is_list) {
        std::cout << " array";
      }
      std::cout << "): ";
      std::getline(std::cin, res);
      if (!res.empty()) {
        std::istringstream iss(res);
        std::vector<std::string> tokens{
            std::istream_iterator<std::string>{iss},
            std::istream_iterator<std::string>{}};
        if (tokens.size() <= 2) {
          auto new_type = katana::graphml::ParseType(tokens[0]);
          if (new_type != ImportDataType::kUnsupported) {
            if (tokens.size() == 2) {
              if (new_type == ImportDataType::kStruct) {
                std::cout << "Arrays of structs are not supported\n";
              } else if (
                  boost::to_lower_copy<std::string>(tokens[1]) == "array") {
                key.type = new_type;
                key.is_list = true;
                done = true;
              } else {
                std::cout
                    << "Second argument could not be recognized, to specify an "
                       "array use the format: \"double array\"\n";
              }
            } else {
              key.type = new_type;
              key.is_list = false;
              done = true;
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
    key.for_node = for_node;
    key.for_edge = !for_node;
    katana::graphml::WriteGraphmlKey(writer, key);
  }
}

void
GetMappingInput(
    mongoc_database_t* database, const std::vector<std::string>& coll_names,
    const std::string& outfile) {
  std::vector<std::string> nodes;
  std::vector<std::string> possible_edges;

  std::vector<LabelRule> rules;
  std::vector<PropertyKey> keys;

  // iterate over all collections in db, find edge and node collections
  for (std::string coll_name : coll_names) {
    auto collection =
        mongoc_database_get_collection(database, coll_name.c_str());

    if (CheckIfCollectionIsEdge(collection)) {
      possible_edges.emplace_back(coll_name);
    } else {
      nodes.emplace_back(coll_name);
    }
    mongoc_collection_destroy(collection);
  }
  // finalize node and edge collection mappings
  auto edges = GetUserInputForEdges(possible_edges, &nodes);

  CollectionFields node_fields;
  CollectionFields edge_fields;

  // iterate over all collections in db, find as many fields as possible
  for (std::string coll_name : nodes) {
    auto collection =
        mongoc_database_get_collection(database, coll_name.c_str());
    ExtractCollectionFields(collection, &node_fields, coll_name);
    mongoc_collection_destroy(collection);
    rules.emplace_back(coll_name, true, false, coll_name);
  }
  for (std::string coll_name : edges) {
    auto collection =
        mongoc_database_get_collection(database, coll_name.c_str());
    ExtractCollectionFields(collection, &edge_fields, coll_name);
    mongoc_collection_destroy(collection);
    rules.emplace_back(coll_name, false, true, coll_name);
  }

  for (const std::string& embedded_node : node_fields.embedded_nodes) {
    rules.emplace_back(embedded_node, true, false, embedded_node);
  }
  for (const std::string& embedded_relation : node_fields.embedded_relations) {
    rules.emplace_back(embedded_relation, false, true, embedded_relation);
  }
  for (auto field : node_fields.property_fields) {
    field.second.for_node = true;
    keys.emplace_back(field.second);
  }
  for (auto field : edge_fields.property_fields) {
    field.second.for_edge = true;
    keys.emplace_back(field.second);
  }

  if (GetUserBool("Generate default mapping now")) {
    katana::graphml::ExportSchemaMapping(outfile, rules, keys);
    return;
  }
  auto writer = katana::graphml::CreateGraphmlFile(outfile);

  // finalize labels for nodes and edges mappings
  std::cout << "Nodes: " << nodes.size() << "\n";
  GetUserInputForLabels(writer, nodes, true);
  std::cout << "Embedded Nodes: " << node_fields.embedded_nodes.size() << "\n";
  GetUserInputForLabels(writer, node_fields.embedded_nodes, true);
  std::cout << "Edges: " << edges.size() << "\n";
  GetUserInputForLabels(writer, edges, false);
  std::cout << "Embedded Edges: " << edge_fields.embedded_relations.size()
            << "\n";
  GetUserInputForLabels(writer, node_fields.embedded_relations, false);

  // finalize field names and types
  std::cout << "Node Fields:\n";
  GetUserInputForFields(writer, std::move(node_fields), true);
  std::cout << "Edge Fields:\n";
  GetUserInputForFields(writer, std::move(edge_fields), false);

  xmlTextWriterStartElement(writer, BAD_CAST "graph");
  xmlTextWriterEndElement(writer);

  katana::graphml::FinishGraphmlFile(writer);
}

std::pair<std::vector<std::string>, std::vector<std::string>>
GetUserInput(
    mongoc_database_t* database, const std::vector<std::string>& coll_names) {
  std::vector<std::string> nodes;
  std::vector<std::string> possible_edges;

  // iterate over all collections in db, find edge and node collections
  for (std::string coll_name : coll_names) {
    auto collection =
        mongoc_database_get_collection(database, coll_name.c_str());

    if (CheckIfCollectionIsEdge(collection)) {
      possible_edges.emplace_back(coll_name);
    } else {
      nodes.emplace_back(coll_name);
    }
    mongoc_collection_destroy(collection);
  }
  // finalize node and edge collection mappings
  auto edges = GetUserInputForEdges(possible_edges, &nodes);

  return std::pair<std::vector<std::string>, std::vector<std::string>>(
      std::move(nodes), std::move(edges));
}

}  // end of unnamed namespace

// for now only handle arrays and data all of same type
void
katana::HandleEdgeDocumentMongoDB(
    katana::PropertyGraphBuilder* builder, const bson_t* doc,
    const std::string& collection_name) {
  builder->StartEdge();

  bool found_source = false;
  bson_iter_t iter;
  if (bson_iter_init(&iter, doc)) {
    // handle document
    while (bson_iter_next(&iter)) {
      const bson_value_t* elt = bson_iter_value(&iter);
      std::string name{bson_iter_key(&iter), bson_iter_key_len(&iter)};

      // initialize new node
      if (name == std::string("_id")) {
        builder->AddEdgeId(ExtractOid(iter));
        continue;
      }
      // handle src and destination node IDs
      if (elt->value_type == BSON_TYPE_OID) {
        if (!found_source) {
          builder->AddEdgeSource(ExtractOid(iter));
          found_source = true;
        } else {
          builder->AddEdgeTarget(ExtractOid(iter));
        }
        continue;
      }

      // since all edge cases have been checked, we can add this property
      builder->AddValue(
          name, [&]() { return ProcessElement(elt, name); },
          [&elt](ImportDataType type, bool is_list) {
            return ResolveValue(elt, type, is_list);
          });

      if (elt->value_type == BSON_TYPE_DOCUMENT) {
        std::string prefix = name + std::string(".");
        HandleEmbeddedEdgeStruct(builder, bson_iter_value(&iter), prefix);
      }
    }
  }
  builder->AddLabel(collection_name);
  builder->FinishEdge();
}

// for now only handle arrays and data all of same type
void
katana::HandleNodeDocumentMongoDB(
    katana::PropertyGraphBuilder* builder, const bson_t* doc,
    const std::string& collection_name) {
  builder->StartNode();
  auto node_index = builder->GetNodeIndex();
  std::vector<std::pair<std::string, bson_value_t_wrapper>> docs;

  bson_iter_t iter;
  if (bson_iter_init(&iter, doc)) {
    // handle document
    while (bson_iter_next(&iter)) {
      const bson_value_t* elt = bson_iter_value(&iter);
      std::string name{bson_iter_key(&iter), bson_iter_key_len(&iter)};
      if (HandleNonPropertyNodeElement(
              builder, &docs, name, elt, collection_name)) {
        continue;
      }

      builder->AddValue(
          name, [&]() { return ProcessElement(elt, name); },
          [&elt](ImportDataType type, bool is_list) {
            return ResolveValue(elt, type, is_list);
          });

      if (elt->value_type == BSON_TYPE_DOCUMENT) {
        std::string prefix = name + std::string(".");
        HandleEmbeddedNodeStruct(builder, &docs, name, elt, prefix);
      }
    }
  }
  builder->AddLabel(collection_name);
  builder->FinishNode();

  // deal with embedded documents
  HandleEmbeddedDocuments(builder, docs, collection_name, node_index);
}

void
katana::GenerateMappingMongoDB(
    const std::string& db_name, const std::string& outfile) {
  const char* uri_string = "mongodb://localhost:27017";

  mongoc_init();
  MongoClient client_wrapper{GetMongoClient(uri_string)};
  mongoc_database_t* database =
      mongoc_client_get_database(client_wrapper.client, db_name.c_str());
  std::vector<std::string> coll_names = GetCollectionNames(database);

  // get user input on node/edge mappings, label names, property names and
  // values
  GetMappingInput(database, std::move(coll_names), outfile);

  mongoc_cleanup();
}

katana::GraphComponents
katana::ConvertMongoDB(
    const std::string& db_name, const std::string& mapping, size_t chunk_size) {
  const char* uri_string = "mongodb://localhost:27017";
  const bson_t* document = nullptr;

  katana::PropertyGraphBuilder builder{chunk_size};
  katana::setActiveThreads(1000);

  mongoc_init();
  MongoClient client_wrapper{GetMongoClient(uri_string)};
  mongoc_database_t* database =
      mongoc_client_get_database(client_wrapper.client, db_name.c_str());
  std::vector<std::string> coll_names = GetCollectionNames(database);

  // get input on node/edge mappings, label names, property names and
  // values
  std::vector<std::string> nodes;
  std::vector<std::string> edges;
  if (!mapping.empty()) {
    auto res =
        katana::graphml::ProcessSchemaMapping(&builder, mapping, coll_names);
    nodes = res.first;
    edges = res.second;
  } else {
    auto res = GetUserInput(database, coll_names);
    nodes = res.first;
    edges = res.second;
  }

  // add all edges first
  for (auto coll_name : edges) {
    QueryEntireCollection(database, &document, coll_name, [&]() {
      katana::HandleEdgeDocumentMongoDB(&builder, document, coll_name);
    });
  }
  // then add all nodes
  for (auto coll_name : nodes) {
    QueryEntireCollection(database, &document, coll_name, [&]() {
      katana::HandleNodeDocumentMongoDB(&builder, document, coll_name);
    });
  }

  mongoc_cleanup();
  if (auto r = builder.Finish(); !r) {
    KATANA_LOG_FATAL("Failed to construct graph: {}", r.error());
  } else {
    return r.value();
  }
}
