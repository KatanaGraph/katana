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
#include "graph-properties-convert-schema.h"

using galois::ImportData;
using galois::ImportDataType;
using galois::LabelRule;
using galois::PropertyKey;

namespace {

/******************************/
/* Functions for parsing data */
/******************************/

std::optional<std::vector<std::string>> ParseStringList(std::string raw_list) {
  std::vector<std::string> list;

  if (raw_list.size() >= 2 && raw_list.front() == '[' &&
      raw_list.back() == ']') {
    raw_list.erase(0, 1);
    raw_list.erase(raw_list.length() - 1, 1);
  } else {
    GALOIS_LOG_ERROR(
        "The provided list was not formatted like neo4j, returning null");
    return std::nullopt;
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
      list.emplace_back("");
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

      list.emplace_back(elem);
    }
  }

  return list;
}

template <typename T>
std::optional<std::vector<T>> ParseNumberList(std::string raw_list) {
  std::vector<T> list;

  if (raw_list.front() == '[' && raw_list.back() == ']') {
    raw_list.erase(0, 1);
    raw_list.erase(raw_list.length() - 1, 1);
  } else {
    GALOIS_LOG_ERROR("The provided list was not formatted like neo4j, "
                     "returning empty vector");
    return std::nullopt;
  }
  std::vector<std::string> elems;
  boost::split(elems, raw_list, boost::is_any_of(","));

  for (std::string s : elems) {
    list.emplace_back(boost::lexical_cast<T>(s));
  }
  return list;
}

std::optional<std::vector<bool>> ParseBooleanList(std::string raw_list) {
  std::vector<bool> list;

  if (raw_list.front() == '[' && raw_list.back() == ']') {
    raw_list.erase(0, 1);
    raw_list.erase(raw_list.length() - 1, 1);
  } else {
    GALOIS_LOG_ERROR("The provided list was not formatted like neo4j, "
                     "returning empty vector");
    return std::nullopt;
  }
  std::vector<std::string> elems;
  boost::split(elems, raw_list, boost::is_any_of(","));

  for (std::string s : elems) {
    bool bool_val = s[0] == 't' || s[0] == 'T';
    list.emplace_back(bool_val);
  }
  return list;
}

/************************************************/
/* Functions for adding values to arrow builder */
/************************************************/

template <typename T>
ImportData Resolve(ImportDataType type, bool is_list, T val) {
  ImportData data{type, is_list};
  data.value = val;
  return data;
}

template <typename Fn>
ImportData ResolveOptionalList(ImportDataType type, const std::string& val,
                               Fn resolver) {
  ImportData data{type, true};

  auto res = resolver(val);
  if (!res) {
    data.type = ImportDataType::kUnsupported;
  } else {
    data.value = res.value();
  }
  return data;
}

ImportData ResolveListValue(const std::string& val, ImportDataType type) {
  switch (type) {
  case ImportDataType::kString:
    return ResolveOptionalList(type, val, ParseStringList);
  case ImportDataType::kInt64:
    return ResolveOptionalList(type, val, ParseNumberList<int64_t>);
  case ImportDataType::kInt32:
    return ResolveOptionalList(type, val, ParseNumberList<int32_t>);
  case ImportDataType::kDouble:
    return ResolveOptionalList(type, val, ParseNumberList<double>);
  case ImportDataType::kFloat:
    return ResolveOptionalList(type, val, ParseNumberList<float>);
  case ImportDataType::kBoolean:
    return ResolveOptionalList(type, val, ParseBooleanList);
  case ImportDataType::kTimestampMilli:
    return ImportData{ImportDataType::kUnsupported, true};
  default:
    return ImportData{ImportDataType::kUnsupported, true};
  }
}

ImportData ResolveValue(const std::string& val, ImportDataType type,
                        bool is_list) {
  if (is_list) {
    return ResolveListValue(val, type);
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
      return Resolve(type, is_list, val[0] == 't' || val[0] == 'T');
    case ImportDataType::kTimestampMilli:
      return ImportData{ImportDataType::kUnsupported, false};
    default:
      return ImportData{ImportDataType::kUnsupported, false};
    }
  } catch (const boost::bad_lexical_cast&) {
    return ImportData{ImportDataType::kUnsupported, false};
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
void ProcessNode(xmlTextReaderPtr reader,
                 galois::PropertyGraphBuilder* builder) {
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
    builder->StartNode(id);
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
        if (!property.first.empty()) {
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
              const std::string& value = property.second;
              builder->AddValue(
                  property.first,
                  [&]() {
                    return PropertyKey{property.first, ImportDataType::kString,
                                       false};
                  },
                  [&value](ImportDataType type, bool is_list) {
                    return ResolveValue(value, type, is_list);
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
  if (validNode) {
    if (!labels.empty()) {
      for (std::string label : labels) {
        builder->AddLabel(label);
      }
    }
    builder->FinishNode();
  }
}

/*
 * reader should be pointing at the edge element before calling
 *
 * parses the edge from a GraphML file into readable form
 */
void ProcessEdge(xmlTextReaderPtr reader,
                 galois::PropertyGraphBuilder* builder) {
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
    valid_edge = builder->StartEdge(source, target);
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
        if (!property.first.empty()) {
          // we reserve the data fields label and labels for node/edge labels
          if (property.first == std::string("label") ||
              property.first == std::string("labels")) {
            if (!extracted_type) {
              type           = property.second;
              extracted_type = true;
            }
          } else if (property.first != std::string("IGNORE")) {
            if (valid_edge) {
              const std::string& value = property.second;
              builder->AddValue(
                  property.first,
                  [&]() {
                    return PropertyKey{property.first, ImportDataType::kString,
                                       false};
                  },
                  [&value](ImportDataType type, bool is_list) {
                    return ResolveValue(value, type, is_list);
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
  if (valid_edge) {
    if (type.length() > 0) {
      builder->AddLabel(type);
    }
    builder->FinishEdge();
  }
}

/*
 * reader should be pointing at the graph element before calling
 *
 * parses the graph structure from a GraphML file into Galois format
 */
void ProcessGraph(xmlTextReaderPtr reader,
                  galois::PropertyGraphBuilder* builder) {
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
        ProcessNode(reader, builder);
      } else if (xmlStrEqual(name, BAD_CAST "edge")) {
        if (!finished_nodes) {
          finished_nodes = true;
          std::cout << "Finished processing nodes\n";
        }
        // if elt is an "egde" xml node read it in
        ProcessEdge(reader, builder);
      } else {
        GALOIS_LOG_ERROR("Found element: {}, which was ignored",
                         std::string((const char*)name));
      }
    }

    xmlFree(name);
    ret = xmlTextReaderRead(reader);
  }
  std::cout << "Finished processing edges\n";
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

  galois::PropertyGraphBuilder builder{chunk_size};

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
          if (!key.id.empty() && key.id != std::string("label") &&
              key.id != std::string("IGNORE")) {
            if (key.for_node) {
              builder.AddBuilder(std::move(key));
            } else if (key.for_edge) {
              builder.AddBuilder(std::move(key));
            }
          }
        } else if (xmlStrEqual(name, BAD_CAST "graph")) {
          std::cout << "Finished processing property headers\n";
          ProcessGraph(reader, &builder);
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
  return builder.Finish();
}
