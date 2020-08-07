#ifndef GALOIS_TOOLS_GRAPH_CONVERT_GRAPH_PROPERTIES_CONVERT_SCHEMA_H_
#define GALOIS_TOOLS_GRAPH_CONVERT_GRAPH_PROPERTIES_CONVERT_SCHEMA_H_

#include <libxml/xmlreader.h>
#include <libxml/xmlwriter.h>

#include "graph-properties-convert.h"

namespace galois {

xmlTextWriterPtr CreateGraphmlFile(const std::string& outfile);
void WriteGraphmlRule(xmlTextWriterPtr writer, const LabelRule& rule);
void WriteGraphmlKey(xmlTextWriterPtr writer, const PropertyKey& key);
void FinishGraphmlFile(xmlTextWriterPtr writer);
void ExportSchemaMapping(const std::string& outfile,
                         const std::vector<LabelRule>& rules,
                         const std::vector<PropertyKey>& keys);

ImportDataType ExtractTypeGraphML(xmlChar* value);
PropertyKey ProcessKey(xmlTextReaderPtr reader);
LabelRule ProcessRule(xmlTextReaderPtr reader);
std::pair<std::vector<std::string>, std::vector<std::string>>
ProcessSchemaMapping(PropertyGraphBuilder* builder, const std::string& mapping,
                     const std::vector<std::string>& coll_names);

std::string TypeName(ImportDataType type);
ImportDataType ParseType(const std::string& in);

} // namespace galois

#endif
