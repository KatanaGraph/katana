#include "katana/RDGPythonInterface.h"

#include <cassert>
#include <cstddef>
#include <string>
#include <vector>

#include "RDGPartHeader.h"
#include "katana/EntityTypeManager.h"
#include "katana/RDG.h"
#include "katana/RDGManifest.h"
#include "katana/Result.h"
#include "katana/URI.h"
#include "katana/tsuba.h"

namespace katana {

katana::Result<void>
WriteRDGPartHeader(
    std::vector<katana::RDGPropInfo> node_properties,
    std::vector<katana::RDGPropInfo> edge_properties,
    katana::EntityTypeManager node_entity_type_manager,
    katana::EntityTypeManager edge_entity_type_manager,
    const std::string& topology_path, const std::string& rdg_dir) {
  // We need to do a bunch of stuff here
  auto manifest = RDGManifest();
  manifest = manifest.NextVersion(1, 0, false, RDGLineage());
  manifest.set_dir(KATANA_CHECKED(katana::Uri::Make(rdg_dir)));
  katana::Uri part_header_uri_path = manifest.PartitionFileName(0);
  auto part_header =
      KATANA_CHECKED(katana::RDGPartHeader::Make(part_header_uri_path));

  // Create vector that is needed by part_header for prop_info, do this for both node and edges
  std::vector<katana::PropStorageInfo> node_props;
  node_props.reserve(node_properties.size());
  for (auto rdg_prop_info : node_properties) {
    node_props.emplace_back(katana::PropStorageInfo(
        rdg_prop_info.property_name, rdg_prop_info.property_path));
  }

  std::vector<katana::PropStorageInfo> edge_props;
  edge_props.reserve(edge_properties.size());
  for (auto rdg_prop_info : edge_properties) {
    edge_props.emplace_back(katana::PropStorageInfo(
        rdg_prop_info.property_name, rdg_prop_info.property_path));
  }

  // Set the node and edge prop info lists
  part_header.set_node_prop_info_list(std::move(node_props));
  part_header.set_edge_prop_info_list(std::move(edge_props));

  // Set the entity type managers for nodes and edges
  part_header.StoreNodeEntityTypeManager(node_entity_type_manager);
  part_header.StoreEdgeEntityTypeManager(edge_entity_type_manager);

  // Set the topology metadata
  part_header.MakePartitionTopologyMetadataEntry(topology_path);

  // Write out the part_header
  std::unique_ptr<WriteGroup> write_group = KATANA_CHECKED(WriteGroup::Make());
  auto policy = katana::RDG::RDGVersioningPolicy::RetainVersion;
  katana::RDGHandle handle =
      KATANA_CHECKED(katana::Open(std::move(manifest), katana::kReadWrite));
  KATANA_CHECKED(part_header.Write(handle, write_group.get(), policy));

  return katana::ResultSuccess();
}

}  // namespace katana
