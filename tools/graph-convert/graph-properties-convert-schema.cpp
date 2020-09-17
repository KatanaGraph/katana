#include "graph-properties-convert-schema.h"

#include <iostream>

#include <boost/algorithm/string.hpp>

using galois::ImportDataType;
using galois::LabelRule;
using galois::PropertyKey;

/***************************************/
/* Functions for writing GraphML files */
/***************************************/

xmlTextWriterPtr
galois::CreateGraphmlFile(const std::string& outfile) {
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
galois::WriteGraphmlRule(xmlTextWriterPtr writer, const LabelRule& rule) {
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
galois::WriteGraphmlKey(xmlTextWriterPtr writer, const PropertyKey& key) {
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
galois::FinishGraphmlFile(xmlTextWriterPtr writer) {
  xmlTextWriterEndElement(writer);  // end graphml
  xmlTextWriterEndDocument(writer);
  xmlFreeTextWriter(writer);
}
void
galois::ExportSchemaMapping(
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

/***************************************/
/* Functions for parsing GraphML files */
/***************************************/

// extract the type from an attr.type or attr.list attribute from a key element
ImportDataType
galois::ExtractTypeGraphML(xmlChar* value) {
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
    GALOIS_LOG_ERROR(
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
galois::ProcessKey(xmlTextReaderPtr reader) {
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
        GALOIS_LOG_ERROR(
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
galois::ProcessRule(xmlTextReaderPtr reader) {
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
        GALOIS_LOG_ERROR(
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
galois::ProcessSchemaMapping(
    galois::PropertyGraphBuilder* builder, const std::string& mapping,
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
      GALOIS_LOG_FATAL("Failed to parse {}", mapping);
    }
  } else {
    GALOIS_LOG_FATAL("Unable to open {}", mapping);
  }
  return std::pair<std::vector<std::string>, std::vector<std::string>>(
      nodes, edges);
}

std::pair<std::vector<LabelRule>, std::vector<PropertyKey>>
galois::ProcessSchemaMapping(const std::string& mapping) {
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
      GALOIS_LOG_FATAL("Failed to parse {}", mapping);
    }
  } else {
    GALOIS_LOG_FATAL("Unable to open {}", mapping);
  }
  return std::pair<std::vector<LabelRule>, std::vector<PropertyKey>>(
      rules, keys);
}

/**************************************************/
/* Functions for converting to/from datatype enum */
/**************************************************/

std::string
galois::TypeName(ImportDataType type) {
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
galois::ParseType(const std::string& in) {
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
