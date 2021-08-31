#include "graph-properties-convert-mysql.h"

#include <mysql.h>
#include <unistd.h>

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

struct MysqlRes {
  MYSQL_RES* res;

  MysqlRes(MYSQL_RES* res_) : res(res_) {}
  ~MysqlRes() { mysql_free_result(res); }
};

struct Relationship {
  std::string label;
  std::string source_table;
  std::string source_field;
  size_t source_index;
  std::string target_table;
  std::string target_field;

  Relationship(
      std::string source_table_, std::string source_field_,
      std::string target_table_, std::string target_field_)
      : label(source_table_ + "_" + target_table_ + "_" + source_field_),
        source_table(std::move(source_table_)),
        source_field(std::move(source_field_)),
        source_index(0),
        target_table(std::move(target_table_)),
        target_field(std::move(target_field_)) {}
};

struct TableData {
  std::string name;
  bool is_node;
  int64_t primary_key_index;
  std::vector<Relationship> out_references;
  std::vector<Relationship> in_references;
  std::vector<std::string> field_names;
  std::vector<size_t> field_indexes;
  std::unordered_set<std::string> ignore_list;

  TableData(std::string name_)
      : name(std::move(name_)),
        is_node(true),
        primary_key_index(-1),
        out_references(std::vector<Relationship>{}),
        in_references(std::vector<Relationship>{}),
        field_names(std::vector<std::string>{}),
        field_indexes(std::vector<size_t>{}),
        ignore_list(std::unordered_set<std::string>{}) {}

  void ResolveOutgoingKeys(const std::string& field, size_t field_index) {
    for (auto& relation : this->out_references) {
      if (relation.source_field == field) {
        relation.source_index = field_index;
      }
    }
  }

  bool IsValidEdge() {
    if (this->out_references.size() != 2) {
      return false;
    }
    if (!this->in_references.empty()) {
      return false;
    }
    return true;
  }
};

template <typename T>
ImportData
Resolve(ImportDataType type, bool is_list, T val) {
  ImportData data{type, is_list};
  data.value = val;
  return data;
}

ImportData
ResolveBool(const std::string& val) {
  if (val.empty()) {
    return ImportData{ImportDataType::kUnsupported, false};
  }
  ImportData data{ImportDataType::kBoolean, false};

  bool res = val[0] > '0' && val[0] <= '9';
  if (res) {
    data.value = res;
    return data;
  }
  res = val[0] == 't' || val[0] == 'T';
  if (res) {
    data.value = res;
    return data;
  }
  res = val[0] == 'y' || val[0] == 'Y';
  if (res) {
    data.value = res;
    return data;
  }
  data.value = false;
  return data;
}

ImportData
ResolveValue(const std::string& val, ImportDataType type, bool is_list) {
  if (is_list) {
    return ImportData{ImportDataType::kUnsupported, is_list};
  }
  try {
    switch (type) {
    case ImportDataType::kString:
      return Resolve(type, is_list, val);
    case ImportDataType::kInt64:
      return Resolve(type, is_list, boost::lexical_cast<int64_t>(val));
    case ImportDataType::kInt32:
      return Resolve(type, is_list, boost::lexical_cast<int32_t>(val));
    case ImportDataType::kDouble:
      return Resolve(type, is_list, boost::lexical_cast<double>(val));
    case ImportDataType::kFloat:
      return Resolve(type, is_list, boost::lexical_cast<float>(val));
    case ImportDataType::kBoolean:
      return ResolveBool(val);
    case ImportDataType::kTimestampMilli:
      return ImportData{ImportDataType::kUnsupported, false};
    default:
      return ImportData{ImportDataType::kUnsupported, false};
    }
  } catch (const boost::bad_lexical_cast&) {
    return ImportData{ImportDataType::kUnsupported, false};
  }
}

ImportDataType
ExtractTypeMysql(enum_field_types type) {
  switch (type) {
  case MYSQL_TYPE_TINY:
    return ImportDataType::kBoolean;
  case MYSQL_TYPE_SHORT:
    return ImportDataType::kInt32;
  case MYSQL_TYPE_INT24:
    return ImportDataType::kInt32;
  case MYSQL_TYPE_LONG:
    return ImportDataType::kInt32;
  case MYSQL_TYPE_LONGLONG:
    return ImportDataType::kInt64;
  case MYSQL_TYPE_FLOAT:
    return ImportDataType::kFloat;
  case MYSQL_TYPE_DOUBLE:
    return ImportDataType::kDouble;
  case MYSQL_TYPE_STRING:
    return ImportDataType::kString;
  case MYSQL_TYPE_VAR_STRING:
    return ImportDataType::kString;
  case MYSQL_TYPE_BLOB:
    return ImportDataType::kString;
  default:
    return ImportDataType::kString;
  }
}

std::string
GenerateFetchForeignKeyQuery(const std::string& table) {
  return std::string{
      "SELECT DISTINCT "
      "TABLE_NAME, "
      "COLUMN_NAME, "
      "CONSTRAINT_NAME, "
      "REFERENCED_TABLE_NAME, "
      "REFERENCED_COLUMN_NAME "
      "FROM "
      "INFORMATION_SCHEMA.KEY_COLUMN_USAGE "
      "WHERE "
      "REFERENCED_TABLE_NAME IS NOT NULL AND "
      "TABLE_NAME = '" +
      table + "';"};
}

std::string
GenerateFetchRowQuery(const std::string& table) {
  return std::string{"SELECT * FROM " + table + " LIMIT 1;"};
}

std::string
GenerateFetchTableQuery(const std::string& table) {
  return std::string{"SELECT * FROM " + table + ";"};
}

std::vector<std::string>
FetchTableNames(MYSQL* con) {
  std::vector<std::string> table_names;

  MysqlRes tables{mysql_list_tables(con, NULL)};
  auto num_fields = mysql_num_fields(tables.res);
  MYSQL_ROW row;

  while ((row = mysql_fetch_row(tables.res))) {
    auto lengths = mysql_fetch_lengths(tables.res);
    for (size_t i = 0; i < num_fields; i++) {
      table_names.emplace_back(std::string(row[i], lengths[i]));
    }
  }
  return table_names;
}

/*
std::vector<std::string> FetchFieldNames(MysqlRes* table) {
  std::vector<std::string> field_names;

  MYSQL_FIELD* field;
  while ((field = mysql_fetch_field(table->res))) {
    field_names.emplace_back(std::string(field->name, field->name_length));
  }
  return field_names;
}*/

MysqlRes
RunQuery(MYSQL* con, const std::string& query) {
  if (mysql_real_query(con, query.c_str(), query.size())) {
    KATANA_LOG_FATAL("Could not run query {}: {}", query, mysql_error(con));
  }
  return MysqlRes(mysql_use_result(con));
}

void
AddNodeTable(
    katana::PropertyGraphBuilder* builder, MYSQL* con,
    const TableData& table_data) {
  MysqlRes table = RunQuery(con, GenerateFetchTableQuery(table_data.name));
  MYSQL_ROW row;

  while ((row = mysql_fetch_row(table.res))) {
    auto lengths = mysql_fetch_lengths(table.res);

    builder->StartNode();
    builder->AddLabel(table_data.name);

    // if table has a primary key, add it as node's ID
    auto primary_index = table_data.primary_key_index;
    if (primary_index >= 0) {
      std::string primary_key{row[primary_index], lengths[primary_index]};
      builder->AddNodeID(table_data.name + primary_key);
    }

    // add data fields
    for (size_t i = 0; i < table_data.field_names.size(); i++) {
      auto index = table_data.field_indexes[i];
      // if the data is null then do not add it
      if (row[index] != NULL) {
        std::string value{row[index], lengths[index]};

        builder->AddValue(
            table_data.field_names[i],
            []() {
              return PropertyKey{
                  "invalid", ImportDataType::kUnsupported, false};
            },
            [&value](ImportDataType type, bool is_list) {
              return ResolveValue(value, type, is_list);
            });
      }
    }

    // if table has outgoing edges, add them
    for (auto relation : table_data.out_references) {
      auto foreign_index = relation.source_index;
      // if the target is null then do not add an edge
      if (row[foreign_index] != NULL) {
        std::string foreign_key{row[foreign_index], lengths[foreign_index]};
        std::string edge_id = relation.target_table + foreign_key;
        builder->AddOutgoingEdge(edge_id, relation.label);
      }
    }
    builder->FinishNode();
  }
}

void
AddEdgeTable(
    katana::PropertyGraphBuilder* builder, MYSQL* con,
    const TableData& table_data) {
  MysqlRes table = RunQuery(con, GenerateFetchTableQuery(table_data.name));
  MYSQL_ROW row;

  while ((row = mysql_fetch_row(table.res))) {
    auto lengths = mysql_fetch_lengths(table.res);

    builder->StartEdge();
    builder->AddLabel(table_data.name);

    bool adding_source = true;
    // if the source or target is null then add a placeholder node
    for (auto relation : table_data.out_references) {
      auto foreign_index = relation.source_index;
      std::string foreign_key{row[foreign_index], lengths[foreign_index]};
      std::string edge_id = relation.target_table + foreign_key;
      if (adding_source) {
        builder->AddEdgeSource(edge_id);
        adding_source = false;
      } else {
        builder->AddEdgeTarget(edge_id);
      }
    }

    // add data fields
    for (size_t i = 0; i < table_data.field_names.size(); i++) {
      auto index = table_data.field_indexes[i];
      // if the data is null then do not add it
      if (row[index] != NULL) {
        std::string value{row[index], lengths[index]};

        builder->AddValue(
            table_data.field_names[i],
            []() {
              return PropertyKey{
                  "invalid", ImportDataType::kUnsupported, false};
            },
            [&value](ImportDataType type, bool is_list) {
              return ResolveValue(value, type, is_list);
            });
      }
    }
    builder->FinishEdge();
  }
}

/************************************/
/* Functions for getting user input */
/************************************/

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

// TODO support multiple labels per collection
void
GetUserInputForLabels(
    xmlTextWriterPtr writer, const std::map<std::string, TableData>& table_data,
    bool for_node) {
  for (auto [name, data] : table_data) {
    if (for_node == data.is_node) {
      std::cout << "Choose label for " << name << " (" << name << "): ";
      std::string res;
      std::getline(std::cin, res);

      std::string existing_key;
      if (res.empty()) {
        LabelRule rule{name, for_node, !for_node, name};
        katana::graphml::WriteGraphmlRule(writer, rule);
      } else {
        LabelRule rule{name, for_node, !for_node, res};
        katana::graphml::WriteGraphmlRule(writer, rule);
      }
    }
  }
}

// TODO support multiple labels per collection
void
GetUserInputForLabels(
    xmlTextWriterPtr writer,
    const std::map<std::string, LabelRule>& foreign_labels) {
  for (auto [name, rule] : foreign_labels) {
    std::cout << "Choose label for " << name << " (" << name << "): ";
    std::string res;
    std::getline(std::cin, res);

    std::string existing_key;
    if (res.empty()) {
      katana::graphml::WriteGraphmlRule(writer, rule);
    } else {
      rule.label = res;
      katana::graphml::WriteGraphmlRule(writer, rule);
    }
  }
}

void
GetUserInputForFields(
    xmlTextWriterPtr writer, std::map<std::string, PropertyKey>* fields) {
  std::cout << "Total Detected Fields: " << fields->size() << "\n";
  for (auto& [name, key] : (*fields)) {
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
    katana::graphml::WriteGraphmlKey(writer, key);
  }
}

/***********************************************/
/* Functions for preprocessing MySQL databases */
/***********************************************/

void
ExhaustResultSet(MysqlRes* res) {
  while (mysql_fetch_row(res->res))
    ;
}

bool
ContainsRelation(
    const std::vector<LabelRule>& rules, const std::string& label) {
  for (auto rule : rules) {
    if (rule.for_edge && rule.id == label) {
      return true;
    }
  }
  return false;
}

bool
ContainsKey(
    const std::vector<PropertyKey>& keys, const std::string& id,
    bool for_node) {
  for (auto key : keys) {
    if (key.for_node == for_node && key.for_edge == !for_node && key.id == id) {
      return true;
    }
  }
  return false;
}

PropertyKey
ProcessField(MYSQL_FIELD* field) {
  std::string id{field->name, field->name_length};
  bool for_node = false;
  bool for_edge = false;
  std::string attr_name = id;
  ImportDataType type = ExtractTypeMysql(field->type);
  bool is_list = false;

  return PropertyKey{
      id, for_node, for_edge, attr_name, type, is_list,
  };
}

template <typename T>
void
PreprocessForeignKeys(
    MysqlRes* foreign_keys, T* table_data, const std::string& table_name) {
  TableData data{table_name};
  MYSQL_ROW row;

  // each row consists of:
  // Source Table, Source Column, Constraint Name, Target Table, Target Column
  while ((row = mysql_fetch_row(foreign_keys->res))) {
    auto lengths = mysql_fetch_lengths(foreign_keys->res);
    std::string source_table{row[0], lengths[0]};
    std::string source_field{row[1], lengths[1]};
    std::string target_table{row[3], lengths[3]};
    std::string target_field{row[4], lengths[4]};

    data.ignore_list.insert(source_field);

    Relationship relation{
        std::move(source_table),
        std::move(source_field),
        std::move(target_table),
        std::move(target_field),
    };
    data.out_references.emplace_back(std::move(relation));
  }
  table_data->insert(std::pair<std::string, TableData>(table_name, data));
}

template <typename T>
void
PreprocessForeignKeys(
    MysqlRes* foreign_keys, T* table_data, const std::vector<LabelRule>& rules,
    const std::string& table_name) {
  TableData data{table_name};
  data.is_node = !ContainsRelation(rules, table_name);
  MYSQL_ROW row;

  // each row consists of:
  // Source Table, Source Column, Constraint Name, Target Table, Target Column
  while ((row = mysql_fetch_row(foreign_keys->res))) {
    auto lengths = mysql_fetch_lengths(foreign_keys->res);
    std::string source_table{row[0], lengths[0]};
    std::string source_field{row[1], lengths[1]};
    std::string target_table{row[3], lengths[3]};
    std::string target_field{row[4], lengths[4]};

    data.ignore_list.insert(source_field);

    Relationship relation{
        std::move(source_table),
        std::move(source_field),
        std::move(target_table),
        std::move(target_field),
    };
    if (!data.is_node || ContainsRelation(rules, relation.label)) {
      data.out_references.emplace_back(std::move(relation));
    }
  }
  table_data->insert(std::pair<std::string, TableData>(table_name, data));
}

template <typename T>
void
FillForeignKeyRelations(T* table_data) {
  for (auto& iter : (*table_data)) {
    for (auto relation : iter.second.out_references) {
      auto& dest = table_data->find(relation.target_table)->second;
      dest.in_references.emplace_back(relation);
    }
  }
}

template <typename T>
void
SetEdges(T* table_data) {
  for (auto& iter : (*table_data)) {
    if (iter.second.IsValidEdge()) {
      iter.second.is_node = !GetUserBool("Treat " + iter.first + " as an edge");
    }
  }
}

template <typename T>
void
PreprocessFields(
    MysqlRes* table_row, T* table_data,
    std::map<std::string, PropertyKey>* property_fields,
    const std::string& table_name) {
  auto table_iter = table_data->find(table_name);
  MYSQL_FIELD* field;

  size_t index = 0;
  while ((field = mysql_fetch_field(table_row->res))) {
    auto key = ProcessField(field);

    // if this field is a primary key, do not add it for now
    if (IS_PRI_KEY(field->flags)) {
      table_iter->second.primary_key_index = static_cast<int64_t>(index);
    } else if (
        table_iter->second.ignore_list.find(key.id) ==
        table_iter->second.ignore_list.end()) {
      // if this field will be added to the database
      key.for_node = table_iter->second.is_node;
      key.for_edge = !table_iter->second.is_node;
      property_fields->insert(std::pair<std::string, PropertyKey>(key.id, key));

      table_iter->second.field_names.emplace_back(key.id);
      table_iter->second.field_indexes.emplace_back(index);
    } else {
      // if this field is a foreign key, resolve its local field indexes
      table_iter->second.ResolveOutgoingKeys(key.id, index);
    }
    index++;
  }
}

template <typename T>
void
PreprocessFields(
    MysqlRes* table_row, T* table_data, const std::vector<PropertyKey>& keys,
    const std::string& table_name) {
  auto table_iter = table_data->find(table_name);
  MYSQL_FIELD* field;

  size_t index = 0;
  while ((field = mysql_fetch_field(table_row->res))) {
    auto key = ProcessField(field);

    // if this field is a primary key, do not add it for now
    if (IS_PRI_KEY(field->flags)) {
      table_iter->second.primary_key_index = static_cast<int64_t>(index);
    } else if (
        table_iter->second.ignore_list.find(key.id) !=
        table_iter->second.ignore_list.end()) {
      // if this field is a foreign key, resolve its local field indexes
      table_iter->second.ResolveOutgoingKeys(key.id, index);
    }
    // if this field will be added to the database
    if (ContainsKey(keys, key.id, table_iter->second.is_node)) {
      table_iter->second.field_names.emplace_back(key.id);
      table_iter->second.field_indexes.emplace_back(index);
    }
    index++;
  }
}

std::unordered_map<std::string, TableData>
PreprocessTables(
    MYSQL* con, katana::PropertyGraphBuilder* builder,
    const std::vector<std::string>& table_names) {
  std::unordered_map<std::string, TableData> table_data;
  std::map<std::string, PropertyKey> node_fields;
  std::map<std::string, PropertyKey> edge_fields;

  // first process tables for primary and foreign keys
  for (auto table_name : table_names) {
    MysqlRes foreign_keys =
        RunQuery(con, GenerateFetchForeignKeyQuery(table_name));
    PreprocessForeignKeys(&foreign_keys, &table_data, table_name);
  }

  FillForeignKeyRelations(&table_data);
  SetEdges(&table_data);

  for (auto table_name : table_names) {
    MysqlRes table_row = RunQuery(con, GenerateFetchRowQuery(table_name));
    if (table_data.find(table_name)->second.is_node) {
      PreprocessFields(&table_row, &table_data, &node_fields, table_name);
    } else {
      PreprocessFields(&table_row, &table_data, &edge_fields, table_name);
    }
    ExhaustResultSet(&table_row);
  }
  for (auto [name, data] : table_data) {
    LabelRule rule{name, data.is_node, !data.is_node, name};
    builder->AddLabelBuilder(rule);
  }
  for (auto iter : node_fields) {
    builder->AddBuilder(iter.second);
  }
  for (auto iter : edge_fields) {
    builder->AddBuilder(iter.second);
  }
  return table_data;
}

std::unordered_map<std::string, TableData>
PreprocessTables(
    MYSQL* con, katana::PropertyGraphBuilder* builder,
    const std::vector<std::string>& table_names,
    const std::vector<LabelRule>& rules, const std::vector<PropertyKey>& keys) {
  std::unordered_map<std::string, TableData> table_data;

  // first process tables for primary and foreign keys
  for (auto table_name : table_names) {
    MysqlRes foreign_keys =
        RunQuery(con, GenerateFetchForeignKeyQuery(table_name));
    PreprocessForeignKeys(&foreign_keys, &table_data, rules, table_name);
  }

  FillForeignKeyRelations(&table_data);

  for (auto table_name : table_names) {
    MysqlRes table_row = RunQuery(con, GenerateFetchRowQuery(table_name));
    if (table_data.find(table_name)->second.is_node) {
      PreprocessFields(&table_row, &table_data, keys, table_name);
    } else {
      PreprocessFields(&table_row, &table_data, keys, table_name);
    }
    ExhaustResultSet(&table_row);
  }
  for (auto rule : rules) {
    builder->AddLabelBuilder(rule);
  }
  for (auto key : keys) {
    builder->AddBuilder(key);
  }
  return table_data;
}

void
GetMappingInput(
    MYSQL* con, const std::vector<std::string>& table_names,
    const std::string& outfile) {
  std::map<std::string, TableData> table_data;
  std::map<std::string, PropertyKey> node_fields;
  std::map<std::string, PropertyKey> edge_fields;
  std::map<std::string, LabelRule> foreign_rules;
  std::vector<PropertyKey> keys;
  std::vector<LabelRule> rules;
  size_t nodes = 0;
  size_t edges = 0;

  // first process tables for primary and foreign keys
  for (auto table_name : table_names) {
    MysqlRes foreign_keys =
        RunQuery(con, GenerateFetchForeignKeyQuery(table_name));
    PreprocessForeignKeys(&foreign_keys, &table_data, table_name);
  }

  FillForeignKeyRelations(&table_data);
  SetEdges(&table_data);

  for (auto table_name : table_names) {
    MysqlRes table_row = RunQuery(con, GenerateFetchRowQuery(table_name));
    if (table_data.find(table_name)->second.is_node) {
      PreprocessFields(&table_row, &table_data, &node_fields, table_name);
    } else {
      PreprocessFields(&table_row, &table_data, &edge_fields, table_name);
    }
    ExhaustResultSet(&table_row);
  }
  for (auto iter : node_fields) {
    keys.emplace_back(iter.second);
  }
  for (auto iter : edge_fields) {
    keys.emplace_back(iter.second);
  }
  // add tables that are nodes
  for (auto [name, data] : table_data) {
    if (data.is_node) {
      nodes++;
      rules.emplace_back(name, data.is_node, !data.is_node, name);

      // find foreign key edges
      for (auto relation : data.out_references) {
        LabelRule rule{relation.label, false, true, relation.label};
        foreign_rules.insert(
            std::pair<std::string, LabelRule>(relation.label, rule));
      }
    }
  }
  // add tables that are edges
  for (auto [name, data] : table_data) {
    if (!data.is_node) {
      edges++;
      rules.emplace_back(name, data.is_node, !data.is_node, name);
    }
  }
  // add edges that are foreign keys
  for (auto iter : foreign_rules) {
    rules.emplace_back(iter.second);
  }

  if (GetUserBool("Generate default mapping now")) {
    katana::graphml::ExportSchemaMapping(outfile, rules, keys);
    return;
  }
  auto writer = katana::graphml::CreateGraphmlFile(outfile);

  // finalize labels for nodes and edges mappings
  std::cout << "Nodes: " << nodes << "\n";
  GetUserInputForLabels(writer, table_data, true);
  std::cout << "Edges: " << edges << "\n";
  GetUserInputForLabels(writer, table_data, false);
  std::cout << "Edges: " << foreign_rules.size() << "\n";
  GetUserInputForLabels(writer, foreign_rules);

  // finalize field names and types
  std::cout << "Node Fields:\n";
  GetUserInputForFields(writer, &node_fields);
  std::cout << "Edge Fields:\n";
  GetUserInputForFields(writer, &edge_fields);

  xmlTextWriterStartElement(writer, BAD_CAST "graph");
  xmlTextWriterEndElement(writer);

  katana::graphml::FinishGraphmlFile(writer);
}

}  // end of unnamed namespace

GraphComponents
katana::ConvertMysql(
    const std::string& db_name, const std::string& mapping,
    const size_t chunk_size, const std::string& host, const std::string& user) {
  katana::PropertyGraphBuilder builder{chunk_size};
  std::string password{getpass("MySQL Password: ")};

  MYSQL* con = mysql_init(NULL);
  if (con == nullptr) {
    KATANA_LOG_FATAL("mysql_init() failed");
  }
  if (mysql_real_connect(
          con, host.c_str(), user.c_str(), password.c_str(), db_name.c_str(), 0,
          NULL, 0) == NULL) {
    KATANA_LOG_FATAL(
        "Could not establish mysql connection: {}", mysql_error(con));
  }
  std::vector<std::string> table_names = FetchTableNames(con);
  std::unordered_map<std::string, TableData> table_data;
  if (!mapping.empty()) {
    auto res = katana::graphml::ProcessSchemaMapping(mapping);
    std::vector<LabelRule> rules = res.first;
    std::vector<PropertyKey> keys = res.second;
    table_data = PreprocessTables(con, &builder, table_names, rules, keys);
  } else {
    table_data = PreprocessTables(con, &builder, table_names);
  }

  for (auto table : table_data) {
    if (table.second.is_node) {
      AddNodeTable(&builder, con, table.second);
    } else {
      AddEdgeTable(&builder, con, table.second);
    }
  }
  mysql_close(con);
  auto out_result = builder.Finish();
  if (!out_result) {
    KATANA_LOG_FATAL("Failed to construct graph: {}", out_result.error());
  }
  katana::GraphComponents out = std::move(out_result.value());
  out.Dump();
  return out;
}

void
katana::GenerateMappingMysql(
    const std::string& db_name, const std::string& outfile,
    const std::string& host, const std::string& user) {
  std::string password{getpass("MySQL Password: ")};

  MYSQL* con = mysql_init(NULL);
  if (con == nullptr) {
    KATANA_LOG_FATAL("mysql_init() failed");
  }
  if (mysql_real_connect(
          con, host.c_str(), user.c_str(), password.c_str(), db_name.c_str(), 0,
          NULL, 0) == NULL) {
    KATANA_LOG_FATAL(
        "Could not establish mysql connection: {}", mysql_error(con));
  }
  std::vector<std::string> table_names = FetchTableNames(con);

  // get user input on node/edge mappings, label names, property names and
  // values
  GetMappingInput(con, std::move(table_names), outfile);

  mysql_close(con);
}
