#ifndef KATANA_LIBGRAPH_KATANA_GRAPHMLSCHEMA_H_
#define KATANA_LIBGRAPH_KATANA_GRAPHMLSCHEMA_H_

#include <libxml/xmlreader.h>
#include <libxml/xmlwriter.h>

#include "katana/BuildGraph.h"

namespace katana {
namespace graphml {

KATANA_EXPORT xmlTextWriterPtr CreateGraphmlFile(const std::string& outfile);
KATANA_EXPORT void WriteGraphmlRule(
    xmlTextWriterPtr writer, const LabelRule& rule);
KATANA_EXPORT void WriteGraphmlKey(
    xmlTextWriterPtr writer, const PropertyKey& key);

KATANA_EXPORT void StartGraphmlNode(
    xmlTextWriterPtr writer, const std::string& node_id,
    const std::string& labels);
KATANA_EXPORT void FinishGraphmlNode(xmlTextWriterPtr writer);

KATANA_EXPORT void StartGraphmlEdge(
    xmlTextWriterPtr writer, const std::string& edge_id,
    const std::string& src_node, const std::string& dest_node,
    const std::string& labels);
KATANA_EXPORT void FinishGraphmlEdge(xmlTextWriterPtr writer);

KATANA_EXPORT void AddGraphmlProperty(
    xmlTextWriterPtr writer, const std::string& property_name,
    const std::string& property_value);

KATANA_EXPORT void FinishGraphmlFile(xmlTextWriterPtr writer);

KATANA_EXPORT void ExportSchemaMapping(
    const std::string& outfile, const std::vector<LabelRule>& rules,
    const std::vector<PropertyKey>& keys);
KATANA_EXPORT void ExportGraph(
    const std::string& outfile, const katana::URI& rdg_dir,
    katana::TxnContext* txn_ctx);

KATANA_EXPORT ImportDataType ExtractTypeGraphML(xmlChar* value);
KATANA_EXPORT PropertyKey ProcessKey(xmlTextReaderPtr reader);
KATANA_EXPORT LabelRule ProcessRule(xmlTextReaderPtr reader);
KATANA_EXPORT std::pair<std::vector<std::string>, std::vector<std::string>>
ProcessSchemaMapping(
    PropertyGraphBuilder* builder, const std::string& mapping,
    const std::vector<std::string>& coll_names);
KATANA_EXPORT std::pair<std::vector<LabelRule>, std::vector<PropertyKey>>
ProcessSchemaMapping(const std::string& mapping);

KATANA_EXPORT std::string TypeName(ImportDataType type);
KATANA_EXPORT ImportDataType ParseType(const std::string& in);
KATANA_EXPORT ImportDataType
ParseType(const std::shared_ptr<arrow::DataType>& in);

}  // namespace graphml
}  // namespace katana

#endif
