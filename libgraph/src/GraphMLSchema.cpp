#include "katana/GraphMLSchema.h"

#include <iostream>

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

#include "katana/RDG.h"

using katana::ImportDataType;
using katana::LabelRule;
using katana::PropertyKey;

struct GraphmlList {
  std::string list;
  int64_t elts;

  GraphmlList() {
    list = std::string{"["};
    elts = 0;
  }

  void AddStringElt(const std::string& elt) {
    if (elts > 0) {
      list += ",";
    }
    list += "\"" + elt + "\"";
    elts++;
  }

  template <typename T>
  void AddNumericElt(T elt) {
    if (elts > 0) {
      list += ",";
    }
    list += boost::lexical_cast<std::string>(elt);
    elts++;
  }

  void AddBoolElt(bool elt) {
    if (elts > 0) {
      list += ",";
    }
    if (elt) {
      list += "true";
    } else {
      list += "false";
    }
    elts++;
  }

  std::string Finish() {
    list += "]";
    return list;
  }
};

/***************************************/
/* Functions for writing GraphML files */
/***************************************/

xmlTextWriterPtr
katana::graphml::CreateGraphmlFile(const std::string& outfile) {
  xmlTextWriterPtr writer;
  writer = xmlNewTextWriterFilename(outfile.c_str(), 0);
  xmlTextWriterStartDocument(writer, "1.0", "UTF-8", NULL);
  xmlTextWriterSetIndentString(writer, BAD_CAST "");
  xmlTextWriterSetIndent(writer, 1);

  xmlTextWriterStartElement(writer, BAD_CAST "graphml");
  xmlTextWriterWriteAttribute(
      writer, BAD_CAST "xmlns",
      BAD_CAST "http://graphml.graphdrawing.org/xmlns");
  xmlTextWriterWriteAttribute(
      writer, BAD_CAST "xmlns:xsi",
      BAD_CAST "http://www.w3.org/2001/XMLSchema-instance");
  xmlTextWriterWriteAttribute(
      writer, BAD_CAST "xmlns:schemaLocation",
      BAD_CAST
      "http://graphml.graphdrawing.org/xmlns "
      "http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd");

  return writer;
}

void
katana::graphml::WriteGraphmlRule(
    xmlTextWriterPtr writer, const LabelRule& rule) {
  xmlTextWriterStartElement(writer, BAD_CAST "rule");
  xmlTextWriterWriteAttribute(writer, BAD_CAST "id", BAD_CAST rule.id.c_str());
  if (rule.for_node) {
    xmlTextWriterWriteAttribute(writer, BAD_CAST "for", BAD_CAST "node");
  } else if (rule.for_edge) {
    xmlTextWriterWriteAttribute(writer, BAD_CAST "for", BAD_CAST "edge");
  }
  xmlTextWriterWriteAttribute(
      writer, BAD_CAST "attr.label", BAD_CAST rule.label.c_str());

  xmlTextWriterEndElement(writer);
}

void
katana::graphml::WriteGraphmlKey(
    xmlTextWriterPtr writer, const PropertyKey& key) {
  xmlTextWriterStartElement(writer, BAD_CAST "key");
  xmlTextWriterWriteAttribute(writer, BAD_CAST "id", BAD_CAST key.id.c_str());
  if (key.for_node) {
    xmlTextWriterWriteAttribute(writer, BAD_CAST "for", BAD_CAST "node");
  } else if (key.for_edge) {
    xmlTextWriterWriteAttribute(writer, BAD_CAST "for", BAD_CAST "edge");
  }
  xmlTextWriterWriteAttribute(
      writer, BAD_CAST "attr.name", BAD_CAST key.name.c_str());
  auto type = TypeName(key.type);
  xmlTextWriterWriteAttribute(
      writer, BAD_CAST "attr.type", BAD_CAST type.c_str());
  if (key.is_list) {
    xmlTextWriterWriteAttribute(
        writer, BAD_CAST "attr.list", BAD_CAST type.c_str());
  }

  xmlTextWriterEndElement(writer);
}

void
katana::graphml::StartGraphmlNode(
    xmlTextWriterPtr writer, const std::string& node_id,
    const std::string& labels) {
  xmlTextWriterStartElement(writer, BAD_CAST "node");
  xmlTextWriterWriteAttribute(writer, BAD_CAST "id", BAD_CAST node_id.c_str());
  if (!labels.empty()) {
    xmlTextWriterWriteAttribute(
        writer, BAD_CAST "labels", BAD_CAST labels.c_str());
  }
}

void
katana::graphml::FinishGraphmlNode(xmlTextWriterPtr writer) {
  xmlTextWriterEndElement(writer);
}

void
katana::graphml::StartGraphmlEdge(
    xmlTextWriterPtr writer, const std::string& edge_id,
    const std::string& src_node, const std::string& dest_node,
    const std::string& labels) {
  xmlTextWriterStartElement(writer, BAD_CAST "edge");
  xmlTextWriterWriteAttribute(writer, BAD_CAST "id", BAD_CAST edge_id.c_str());
  xmlTextWriterWriteAttribute(
      writer, BAD_CAST "source", BAD_CAST src_node.c_str());
  xmlTextWriterWriteAttribute(
      writer, BAD_CAST "target", BAD_CAST dest_node.c_str());
  if (!labels.empty()) {
    xmlTextWriterWriteAttribute(
        writer, BAD_CAST "labels", BAD_CAST labels.c_str());
  }
}

void
katana::graphml::FinishGraphmlEdge(xmlTextWriterPtr writer) {
  xmlTextWriterEndElement(writer);
}

void
katana::graphml::AddGraphmlProperty(
    xmlTextWriterPtr writer, const std::string& property_name,
    const std::string& property_value) {
  xmlTextWriterStartElement(writer, BAD_CAST "data");
  xmlTextWriterWriteAttribute(
      writer, BAD_CAST "key", BAD_CAST property_name.c_str());

  // write value here
  xmlTextWriterWriteString(writer, BAD_CAST property_value.c_str());

  xmlTextWriterEndElement(writer);
}

void
katana::graphml::FinishGraphmlFile(xmlTextWriterPtr writer) {
  xmlTextWriterEndElement(writer);  // end graphml
  xmlTextWriterEndDocument(writer);
  xmlFreeTextWriter(writer);
}
void
katana::graphml::ExportSchemaMapping(
    const std::string& outfile, const std::vector<LabelRule>& rules,
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

namespace {
std::vector<PropertyKey>
ExtractSchema(
    std::shared_ptr<arrow::Schema> schema, bool is_node,
    std::vector<uint64_t>* property_indexes,
    std::vector<uint64_t>* label_indexes) {
  std::vector<PropertyKey> keys;
  int fields = schema->num_fields();

  for (auto i = 0; i < fields; i++) {
    std::shared_ptr<arrow::Field> field = schema->field(i);
    std::string name = field->name();
    std::shared_ptr<arrow::DataType> type = field->type();
    // ignore boolean fields since for now we assume they are labels
    if (type->id() != arrow::Type::UINT8) {
      bool is_list = type->id() == arrow::Type::LIST;
      ImportDataType import_type = katana::graphml::ParseType(type);
      // TODO(Patrick) clean this up properly
      if (import_type == ImportDataType::kUnsupported) {
        import_type = ImportDataType::kString;
      }
      // TODO(Patrick) when we add graphml parsing support for timestamps delete this
      if (import_type == ImportDataType::kTimestampMilli) {
        import_type = ImportDataType::kString;
      }

      PropertyKey key{name, is_node, !is_node, name, import_type, is_list};
      keys.emplace_back(key);
      property_indexes->emplace_back(i);
    } else {
      label_indexes->emplace_back(i);
    }
  }
  return keys;
}

std::string
ExtractList(std::shared_ptr<arrow::ListArray> array, int64_t index) {
  GraphmlList data{};
  int32_t start = array->value_offset(index);
  int32_t end = array->value_offset(index + 1);

  switch (array->value_type()->id()) {
  case arrow::Type::STRING: {
    auto sb = std::static_pointer_cast<arrow::StringArray>(array->values());

    for (int32_t s = start; s < end; s++) {
      data.AddStringElt(sb->GetString(s));
    }
    break;
  }
  case arrow::Type::INT64: {
    auto sb = std::static_pointer_cast<arrow::Int64Array>(array->values());

    for (int32_t s = start; s < end; s++) {
      data.AddNumericElt(sb->Value(s));
    }
    break;
  }
  case arrow::Type::INT32: {
    auto sb = std::static_pointer_cast<arrow::Int32Array>(array->values());

    for (int32_t s = start; s < end; s++) {
      data.AddNumericElt(sb->Value(s));
    }
    break;
  }
  case arrow::Type::DOUBLE: {
    auto sb = std::static_pointer_cast<arrow::DoubleArray>(array->values());

    for (int32_t s = start; s < end; s++) {
      data.AddNumericElt(sb->Value(s));
    }
    break;
  }
  case arrow::Type::FLOAT: {
    auto sb = std::static_pointer_cast<arrow::FloatArray>(array->values());

    for (int32_t s = start; s < end; s++) {
      data.AddNumericElt(sb->Value(s));
    }
    break;
  }
  case arrow::Type::BOOL: {
    auto sb = std::static_pointer_cast<arrow::BooleanArray>(array->values());

    for (int32_t s = start; s < end; s++) {
      data.AddBoolElt(sb->Value(s));
    }
    break;
  }
  case arrow::Type::TIMESTAMP: {
    auto sb = std::static_pointer_cast<arrow::TimestampArray>(array->values());

    for (int32_t s = start; s < end; s++) {
      data.AddNumericElt(sb->Value(s));
    }
    break;
  }
  case arrow::Type::UINT8: {
    auto sb = std::static_pointer_cast<arrow::UInt8Array>(array->values());

    for (int32_t s = start; s < end; s++) {
      data.AddNumericElt(sb->Value(s));
    }
    break;
  }
  default: {
    KATANA_LOG_ERROR(
        "Attempted to export a property type that this exporter does not yet "
        "support: {}",
        array->type()->ToString());
  }
  }
  return data.Finish();
}

std::string
ExtractData(std::shared_ptr<arrow::Array> array, int64_t index) {
  switch (array->type()->id()) {
  case arrow::Type::STRING: {
    auto sb = std::static_pointer_cast<arrow::StringArray>(array);
    return sb->GetString(index);
  }
  case arrow::Type::INT64: {
    auto lb = std::static_pointer_cast<arrow::Int64Array>(array);
    return boost::lexical_cast<std::string>(lb->Value(index));
  }
  case arrow::Type::INT32: {
    auto ib = std::static_pointer_cast<arrow::Int32Array>(array);
    return boost::lexical_cast<std::string>(ib->Value(index));
  }
  case arrow::Type::DOUBLE: {
    auto db = std::static_pointer_cast<arrow::DoubleArray>(array);
    return boost::lexical_cast<std::string>(db->Value(index));
  }
  case arrow::Type::FLOAT: {
    auto fb = std::static_pointer_cast<arrow::FloatArray>(array);
    return boost::lexical_cast<std::string>(fb->Value(index));
  }
  case arrow::Type::BOOL: {
    auto bb = std::static_pointer_cast<arrow::BooleanArray>(array);
    return boost::lexical_cast<std::string>(bb->Value(index));
  }
  case arrow::Type::TIMESTAMP: {
    auto tb = std::static_pointer_cast<arrow::TimestampArray>(array);
    return boost::lexical_cast<std::string>(tb->Value(index));
  }
  // for now uint8_t is an alias for a struct
  case arrow::Type::UINT8: {
    auto bb = std::static_pointer_cast<arrow::UInt8Array>(array);
    return boost::lexical_cast<std::string>(bb->Value(index));
  }
  case arrow::Type::LIST: {
    auto lb = std::static_pointer_cast<arrow::ListArray>(array);
    return ExtractList(lb, index);
  }
  default: {
    KATANA_LOG_ERROR(
        "Attempted to export a property type that this exporter does not yet "
        "support: {}",
        array->type()->ToString());
    return std::string{};
  }
  }
}

template <typename EdgeIterRange>
bool
InRange(uint64_t id, const EdgeIterRange& range) {
  return *range.begin() <= id && id < *range.end();
}
}  // namespace

void
katana::graphml::ExportGraph(
    const std::string& outfile, const katana::URI& rdg_dir,
    katana::TxnContext* txn_ctx) {
  auto result =
      katana::PropertyGraph::Make(rdg_dir, txn_ctx, katana::RDGLoadOptions());
  if (!result) {
    KATANA_LOG_FATAL("failed to load {}: {}", rdg_dir, result.error());
  }
  std::unique_ptr<katana::PropertyGraph> graph = std::move(result.value());

  xmlTextWriterPtr writer = CreateGraphmlFile(outfile);

  // export schema
  std::shared_ptr<arrow::Schema> node_schema = graph->loaded_node_schema();
  std::shared_ptr<arrow::Schema> edge_schema = graph->loaded_edge_schema();

  std::vector<uint64_t> node_property_indexes;
  std::vector<uint64_t> node_label_indexes;
  std::vector<uint64_t> edge_property_indexes;
  std::vector<uint64_t> edge_label_indexes;

  std::vector<PropertyKey> node_keys = ExtractSchema(
      node_schema, true, &node_property_indexes, &node_label_indexes);
  for (const PropertyKey& key : node_keys) {
    WriteGraphmlKey(writer, key);
  }
  std::vector<PropertyKey> edge_keys = ExtractSchema(
      edge_schema, false, &edge_property_indexes, &edge_label_indexes);
  for (const PropertyKey& key : edge_keys) {
    WriteGraphmlKey(writer, key);
  }

  xmlTextWriterStartElement(writer, BAD_CAST "graph");

  // export nodes and edges here

  std::vector<int64_t> chunk_indexes;
  std::vector<int64_t> sub_indexes;
  for (int i = 0; i < graph->GetNumNodeProperties(); i++) {
    chunk_indexes.emplace_back(0);
    sub_indexes.emplace_back(0);
  }

  for (uint64_t i = 0; i < graph->NumNodes(); i++) {
    // find labels
    std::string labels;
    for (auto j : node_label_indexes) {
      if (sub_indexes[j] >=
          graph->GetNodeProperty(j)->chunk(chunk_indexes[j])->length()) {
        sub_indexes[j] = 0;
        chunk_indexes[j]++;
      }
      if (!graph->GetNodeProperty(j)
               ->chunk(chunk_indexes[j])
               ->IsNull(sub_indexes[j])) {
        auto node_labels = std::static_pointer_cast<arrow::UInt8Array>(
            graph->GetNodeProperty(j)->chunk(chunk_indexes[j]));
        if (node_labels->Value(sub_indexes[j]) > 0) {
          labels += ":" + node_schema->field(j)->name();
        }
      }
      sub_indexes[j]++;
    }

    StartGraphmlNode(writer, boost::lexical_cast<std::string>(i), labels);

    // add properties
    for (auto j : node_property_indexes) {
      if (sub_indexes[j] >=
          graph->GetNodeProperty(j)->chunk(chunk_indexes[j])->length()) {
        sub_indexes[j] = 0;
        chunk_indexes[j]++;
      }
      if (!graph->GetNodeProperty(j)
               ->chunk(chunk_indexes[j])
               ->IsNull(sub_indexes[j])) {
        auto name = node_schema->field(j)->name();
        auto data = ExtractData(
            graph->GetNodeProperty(j)->chunk(chunk_indexes[j]), sub_indexes[j]);
        AddGraphmlProperty(writer, name, data);
      }
      sub_indexes[j]++;
    }
    FinishGraphmlNode(writer);
  }

  const katana::GraphTopology& topology = graph->topology();
  uint32_t src_node = 0;

  chunk_indexes.clear();
  sub_indexes.clear();
  for (int i = 0; i < graph->GetNumEdgeProperties(); i++) {
    chunk_indexes.emplace_back(0);
    sub_indexes.emplace_back(0);
  }
  for (uint64_t i = 0; i < graph->NumEdges(); i++) {
    // find labels
    std::string labels;
    for (auto j : edge_label_indexes) {
      if (sub_indexes[j] >=
          graph->GetEdgeProperty(0)->chunk(chunk_indexes[j])->length()) {
        sub_indexes[j] = 0;
        chunk_indexes[j]++;
      }
      if (!graph->GetEdgeProperty(j)
               ->chunk(chunk_indexes[j])
               ->IsNull(sub_indexes[j])) {
        auto edge_labels = std::static_pointer_cast<arrow::UInt8Array>(
            graph->GetEdgeProperty(j)->chunk(chunk_indexes[j]));
        if (edge_labels->Value(sub_indexes[j]) > 0) {
          // TODO(Patrick) when the parser is altered to handle multiple labels, use this line instead
          //labels += ":" + edge_schema->field(j)->name();
          labels = edge_schema->field(j)->name();
        }
      }
      sub_indexes[j]++;
    }

    while (!InRange(i, topology.OutEdges(src_node))) {
      src_node++;
    }
    std::string src = boost::lexical_cast<std::string>(src_node);
    std::string dest = boost::lexical_cast<std::string>(topology.OutEdgeDst(i));
    StartGraphmlEdge(
        writer, boost::lexical_cast<std::string>(i), src, dest, labels);

    // add properties
    for (auto j : edge_property_indexes) {
      if (sub_indexes[j] >=
          graph->GetEdgeProperty(j)->chunk(chunk_indexes[j])->length()) {
        sub_indexes[j] = 0;
        chunk_indexes[j]++;
      }
      if (!graph->GetEdgeProperty(j)
               ->chunk(chunk_indexes[j])
               ->IsNull(sub_indexes[j])) {
        std::string name = edge_schema->field(j)->name();
        std::string data = ExtractData(
            graph->GetEdgeProperty(j)->chunk(chunk_indexes[j]), sub_indexes[j]);
        AddGraphmlProperty(writer, name, data);
      }
      sub_indexes[j]++;
    }
    FinishGraphmlEdge(writer);
  }

  xmlTextWriterEndElement(writer);  // end graph

  FinishGraphmlFile(writer);
}

/***************************************/
/* Functions for parsing GraphML files */
/***************************************/

// extract the type from an attr.type or attr.list attribute from a key element
ImportDataType
katana::graphml::ExtractTypeGraphML(xmlChar* value) {
  ImportDataType type = ImportDataType::kString;
  if (xmlStrEqual(value, BAD_CAST "string")) {
    type = ImportDataType::kString;
  } else if (
      xmlStrEqual(value, BAD_CAST "long") ||
      xmlStrEqual(value, BAD_CAST "int64")) {
    type = ImportDataType::kInt64;
  } else if (
      xmlStrEqual(value, BAD_CAST "int") ||
      xmlStrEqual(value, BAD_CAST "int32")) {
    type = ImportDataType::kInt32;
  } else if (xmlStrEqual(value, BAD_CAST "double")) {
    type = ImportDataType::kDouble;
  } else if (xmlStrEqual(value, BAD_CAST "float")) {
    type = ImportDataType::kFloat;
  } else if (
      xmlStrEqual(value, BAD_CAST "boolean") ||
      xmlStrEqual(value, BAD_CAST "bool")) {
    type = ImportDataType::kBoolean;
  } else if (xmlStrEqual(value, BAD_CAST "timestamp milli")) {
    type = ImportDataType::kTimestampMilli;
  } else if (xmlStrEqual(value, BAD_CAST "struct")) {
    type = ImportDataType::kStruct;
  } else {
    KATANA_LOG_ERROR(
        "Came across attr.type: {}, that is not supported",
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
PropertyKey
katana::graphml::ProcessKey(xmlTextReaderPtr reader) {
  int ret = xmlTextReaderMoveToNextAttribute(reader);
  xmlChar *name, *value;

  std::string id;
  bool for_node = false;
  bool for_edge = false;
  std::string attr_name;
  ImportDataType type = ImportDataType::kString;
  bool is_list = false;

  while (ret == 1) {
    name = xmlTextReaderName(reader);
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
        type = ExtractTypeGraphML(value);
      } else {
        KATANA_LOG_ERROR(
            "Attribute on key: {}, was not recognized",
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
LabelRule
katana::graphml::ProcessRule(xmlTextReaderPtr reader) {
  int ret = xmlTextReaderMoveToNextAttribute(reader);
  xmlChar *name, *value;

  std::string id;
  bool for_node = false;
  bool for_edge = false;
  std::string attr_label;

  while (ret == 1) {
    name = xmlTextReaderName(reader);
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
        KATANA_LOG_ERROR(
            "Attribute on key: {}, was not recognized",
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
katana::graphml::ProcessSchemaMapping(
    katana::PropertyGraphBuilder* builder, const std::string& mapping,
    const std::vector<std::string>& coll_names) {
  xmlTextReaderPtr reader;
  int ret = 0;
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
              builder->AddBuilder(std::move(key));
            } else if (key.for_edge) {
              builder->AddBuilder(std::move(key));
            }
          }
        } else if (xmlStrEqual(name, BAD_CAST "rule")) {
          LabelRule rule = ProcessRule(reader);
          if (rule.id.size() > 0) {
            if (rule.for_node) {
              if (std::find(coll_names.begin(), coll_names.end(), rule.id) !=
                  coll_names.end()) {
                nodes.emplace_back(rule.id);
              }
              builder->AddLabelBuilder(std::move(rule));
            } else if (rule.for_edge) {
              if (std::find(coll_names.begin(), coll_names.end(), rule.id) !=
                  coll_names.end()) {
                edges.emplace_back(rule.id);
              }
              builder->AddLabelBuilder(std::move(rule));
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
      KATANA_LOG_FATAL("Failed to parse {}", mapping);
    }
  } else {
    KATANA_LOG_FATAL("Unable to open {}", mapping);
  }
  return std::pair<std::vector<std::string>, std::vector<std::string>>(
      nodes, edges);
}

std::pair<std::vector<LabelRule>, std::vector<PropertyKey>>
katana::graphml::ProcessSchemaMapping(const std::string& mapping) {
  xmlTextReaderPtr reader;
  int ret = 0;
  bool finished_header = false;
  std::vector<LabelRule> rules;
  std::vector<PropertyKey> keys;

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
            keys.emplace_back(key);
          }
        } else if (xmlStrEqual(name, BAD_CAST "rule")) {
          LabelRule rule = ProcessRule(reader);
          if (rule.id.size() > 0) {
            rules.emplace_back(rule);
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
      KATANA_LOG_FATAL("Failed to parse {}", mapping);
    }
  } else {
    KATANA_LOG_FATAL("Unable to open {}", mapping);
  }
  return std::pair<std::vector<LabelRule>, std::vector<PropertyKey>>(
      rules, keys);
}

/**************************************************/
/* Functions for converting to/from datatype enum */
/**************************************************/

std::string
katana::graphml::TypeName(ImportDataType type) {
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

ImportDataType
katana::graphml::ParseType(const std::string& in) {
  std::string type = boost::to_lower_copy<std::string>(in);
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

ImportDataType
katana::graphml::ParseType(const std::shared_ptr<arrow::DataType>& in) {
  arrow::Type::type type = in->id();
  if (type == arrow::Type::STRING) {
    return ImportDataType::kString;
  }
  if (type == arrow::Type::DOUBLE) {
    return ImportDataType::kDouble;
  }
  if (type == arrow::Type::FLOAT) {
    return ImportDataType::kFloat;
  }
  if (type == arrow::Type::INT64) {
    return ImportDataType::kInt64;
  }
  if (type == arrow::Type::INT32) {
    return ImportDataType::kInt32;
  }
  if (type == arrow::Type::BOOL || type == arrow::Type::UINT8) {
    return ImportDataType::kBoolean;
  }
  if (type == arrow::Type::TIMESTAMP) {
    return ImportDataType::kTimestampMilli;
  }
  //if (type == std::string("struct")) {
  //  return ImportDataType::kStruct;
  //}
  return ImportDataType::kUnsupported;
}
