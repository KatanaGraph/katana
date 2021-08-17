#include <boost/filesystem.hpp>

#include "TestTypedPropertyGraph.h"
#include "katana/Logging.h"
#include "katana/PropertyGraph.h"
#include "katana/SharedMemSys.h"
#include "katana/analytics/Utils.h"
#include "llvm/Support/CommandLine.h"
#include "stdio.h"
#include "tsuba/RDG.h"

namespace cll = llvm::cl;
namespace fs = boost::filesystem;

static cll::opt<std::string> ldbc_003InputFile(
    cll::Positional, cll::desc("<ldbc_003 input file>"), cll::Required);

void
TestLoadGraphWithoutExternalTypes() {
  // Load existing "old" graph.
  auto g_preconvert =
      katana::PropertyGraph::Make(ldbc_003InputFile, tsuba::RDGLoadOptions())
          .value();

  const auto g_node_type_manager = g_preconvert->GetNodeTypeManager();

  const auto g_edge_type_manager = g_preconvert->GetEdgeTypeManager();

  // PG
  KATANA_LOG_WARN("g_preconvert.size() = {}", g_preconvert->size());
  KATANA_LOG_WARN("g_preconvert.num_edges() = {}", g_preconvert->num_edges());
  KATANA_LOG_WARN("g_preconvert.num_nodes() = {}", g_preconvert->num_nodes());

  KATANA_LOG_WARN(
      "g_preconvert.node_entity_type_ids_size() = {}",
      g_preconvert->node_entity_type_ids_size());

  KATANA_LOG_WARN(
      "g_preconvert.edge_entity_type_ids_size() = {}",
      g_preconvert->edge_entity_type_ids_size());

  KATANA_ASSERT(
      g_preconvert->num_nodes() == g_preconvert->node_entity_type_ids_size());
  KATANA_ASSERT(
      g_preconvert->num_edges() == g_preconvert->edge_entity_type_ids_size());

  // type manager
  KATANA_LOG_WARN(
      "g->GetNodeTypeManager().GetEntityTypeIDToAtomicEntityTypeIDs().size() = "
      "{}",
      g_node_type_manager.GetEntityTypeIDToAtomicEntityTypeIDs().size());

  KATANA_LOG_WARN(
      "g->GetEdgeTypeManager().GetEntityTypeIDToAtomicEntityTypeIDs().size() = "
      "{}",
      g_edge_type_manager.GetEntityTypeIDToAtomicEntityTypeIDs().size());

  KATANA_LOG_WARN(
      "g->GetNodeTypeManager().GetEntityTypeIDToAtomicTypeNameMap().size() = "
      "{}",
      g_node_type_manager.GetEntityTypeIDToAtomicTypeNameMap().size());

  KATANA_LOG_WARN(
      "g->GetEdgeTypeManager().GetEntityTypeIDToAtomicTypeNameMap().size() = "
      "{}",
      g_edge_type_manager.GetEntityTypeIDToAtomicTypeNameMap().size());

  KATANA_LOG_WARN(
      "g->GetNodeTypeManager().GetNumAtomicTypes() = "
      "{}",
      g_node_type_manager.GetNumAtomicTypes());

  KATANA_LOG_WARN(
      "g->GetEdgeTypeManager().GetNumAtomicTypes() = "
      "{}",
      g_edge_type_manager.GetNumAtomicTypes());


  auto uri_res = katana::Uri::MakeRand("/tmp/propertyfilegraph");
  KATANA_LOG_ASSERT(uri_res);
  std::string tmp_rdg_dir(uri_res.value().path());  // path() because local
  std::string command_line;

  // Store new converted graph
  auto write_result = g_preconvert->Write(tmp_rdg_dir, command_line);
  KATANA_LOG_WARN(
      "writing converted graph, creating temp file {}", tmp_rdg_dir);

  if (!write_result) {
    fs::remove_all(tmp_rdg_dir);
    KATANA_LOG_FATAL("writing result: {}", write_result.error());
  }

  auto g_postconvert = katana::PropertyGraph::Make(tmp_rdg_dir, tsuba::RDGLoadOptions()).value();

  KATANA_LOG_WARN("g_postconvert.size() = {}", g_postconvert->size());
  KATANA_LOG_WARN("g_postconvert.num_edges() = {}", g_postconvert->num_edges());
  KATANA_LOG_WARN("g_postconvert.num_nodes() = {}", g_postconvert->num_nodes());
  // TODO: (emcginnis) do some validation?
}

int
main(int argc, char** argv) {
  katana::SharedMemSys sys;
  cll::ParseCommandLineOptions(argc, argv);

  TestLoadGraphWithoutExternalTypes();

  return 0;
}


