#ifndef KATANA_LIBTSUBA_RDGPYTHONINTERFACE_H_
#define KATANA_LIBTSUBA_RDGPYTHONINTERFACE_H_

#include <cassert>
#include <cstddef>
#include <string>
#include <vector>

#include "katana/EntityTypeManager.h"
#include "katana/RDG.h"
#include "katana/Result.h"
#include "katana/tsuba.h"

namespace katana {

struct RDGPropInfo {
  std::string property_name;
  std::string property_path;
};

/// RDGPythonInterface exposes some inner details of RDGs that are needed
/// by the Python out-of-core import tool
class RDGPythonInterface {
public:
  katana::Result<void> WriteRDGPartHeader(
      std::vector<RDGPropInfo> node_properties,
      std::vector<RDGPropInfo> edge_properties,
      katana::EntityTypeManager node_entity_type_manager,
      katana::EntityTypeManager edge_entity_type_manager,
      const std::string& topology_path, const std::string& rdg_dir);
};

}  // namespace katana

#endif
