#include "graph-properties-convert-graphml.h"

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

namespace {

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

/************************************************/
/* Functions for adding values to arrow builder */
/************************************************/

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

/***************************************/
/* Functions for parsing GraphML files */
/***************************************/

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
                PropertyKey key{property.first, ImportDataType::kString, false};
                index = galois::AddBuilder(&builder->node_properties,
                                           std::move(key));
              } else {
                index = keyIter->second;
              }
              galois::AddValue(builder->node_properties.builders[index],
                               &builder->node_properties.chunks[index],
                               properties, builder->nodes, [&]() {
                                 AppendValue(
                                     builder->node_properties.builders[index],
                                     property.second);
                               });
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
        LabelRule rule{label};
        index = galois::AddLabelBuilder(&builder->node_labels, std::move(rule));
      } else {
        index = entry->second;
      }
      galois::AddLabel(builder->node_labels.builders[index],
                       &builder->node_labels.chunks[index], properties,
                       builder->nodes);
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
          static_cast<uint32_t>(dest_entry->second));
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
                PropertyKey key{property.first, ImportDataType::kString, false};
                index = galois::AddBuilder(&builder->edge_properties,
                                           std::move(key));
              } else {
                index = keyIter->second;
              }
              galois::AddValue(builder->edge_properties.builders[index],
                               &builder->edge_properties.chunks[index],
                               properties, builder->edges, [&]() {
                                 AppendValue(
                                     builder->edge_properties.builders[index],
                                     property.second);
                               });
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
      LabelRule rule{type};
      index = galois::AddLabelBuilder(&builder->edge_types, std::move(type));
    } else {
      index = entry->second;
    }
    galois::AddLabel(builder->edge_types.builders[index],
                     &builder->edge_types.chunks[index], properties,
                     builder->edges);
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
          std::cout << "Finished processing nodes\n";
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
  std::cout << "Finished processing edges;";

  builder->topology_builder.out_dests.resize(
      builder->edges, std::numeric_limits<uint32_t>::max());
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
  WriterProperties properties = galois::GetWriterProperties(chunk_size);

  galois::setActiveThreads(1000);
  bool finishedGraph = false;
  std::cout << "Start converting GraphML file: " << infilename << "\n";

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
          PropertyKey key = galois::ProcessKey(reader);
          if (key.id.size() > 0 && key.id != std::string("label") &&
              key.id != std::string("IGNORE")) {
            if (key.for_node) {
              galois::AddBuilder(&builder.node_properties, std::move(key));
            } else if (key.for_edge) {
              galois::AddBuilder(&builder.edge_properties, std::move(key));
            }
          }
        } else if (xmlStrEqual(name, BAD_CAST "graph")) {
          std::cout << "Finished processing property headers\n";
          std::cout << "Node Properties declared: "
                    << builder.node_properties.keys.size() << "\n";
          std::cout << "Edge Properties declared: "
                    << builder.edge_properties.keys.size() << "\n";
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
  return galois::BuildGraphComponents(std::move(builder),
                                      std::move(properties));
}
