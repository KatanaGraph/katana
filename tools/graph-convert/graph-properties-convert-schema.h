#ifndef KATANA_TOOLS_GRAPH_CONVERT_GRAPH_PROPERTIES_CONVERT_SCHEMA_H_
#define KATANA_TOOLS_GRAPH_CONVERT_GRAPH_PROPERTIES_CONVERT_SCHEMA_H_

#include <libxml/xmlreader.h>
#include <libxml/xmlwriter.h>

#include "katana/BuildGraph.h"

namespace katana {

xmlTextWriterPtr CreateGraphmlFile(const std::string& outfile);
void WriteGraphmlRule(xmlTextWriterPtr writer, const LabelRule& rule);
void WriteGraphmlKey(xmlTextWriterPtr writer, const PropertyKey& key);

void StartGraphmlNode(
    xmlTextWriterPtr writer, const std::string& node_id,
    const std::string& labels);
void FinishGraphmlNode(xmlTextWriterPtr writer);

void StartGraphmlEdge(
    xmlTextWriterPtr writer, const std::string& edge_id,
    const std::string& src_node, const std::string& dest_node,
    const std::string& labels);
void FinishGraphmlEdge(xmlTextWriterPtr writer);

void AddGraphmlProperty(
    xmlTextWriterPtr writer, const std::string& property_name,
    const std::string& property_value);

void FinishGraphmlFile(xmlTextWriterPtr writer);

void ExportSchemaMapping(
    const std::string& outfile, const std::vector<LabelRule>& rules,
    const std::vector<PropertyKey>& keys);
void ExportGraph(const std::string& outfile, const std::string& rdg_file);

ImportDataType ExtractTypeGraphML(xmlChar* value);
PropertyKey ProcessKey(xmlTextReaderPtr reader);
LabelRule ProcessRule(xmlTextReaderPtr reader);
std::pair<std::vector<std::string>, std::vector<std::string>>
ProcessSchemaMapping(
    PropertyGraphBuilder* builder, const std::string& mapping,
    const std::vector<std::string>& coll_names);
std::pair<std::vector<LabelRule>, std::vector<PropertyKey>>
ProcessSchemaMapping(const std::string& mapping);

std::string TypeName(ImportDataType type);
ImportDataType ParseType(const std::string& in);
ImportDataType ParseType(std::shared_ptr<arrow::DataType> in);

}  // namespace katana

#endif
