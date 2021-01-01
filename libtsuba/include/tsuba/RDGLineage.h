#ifndef KATANA_LIBTSUBA_TSUBA_RDGLINEAGE_H_
#define KATANA_LIBTSUBA_TSUBA_RDGLINEAGE_H_

#include <string>

#include "katana/JSON.h"

namespace tsuba {

class RDGLineage {
  std::string command_line_{};

public:
  const std::string& command_line() { return command_line_; }
  void AddCommandLine(const std::string& cmd);
  void ClearLineage();

  friend void to_json(nlohmann::json& j, const RDGLineage& lineage);
  friend void from_json(const nlohmann::json& j, RDGLineage& lineage);
};

void to_json(nlohmann::json& j, const RDGLineage& lineage);
void from_json(const nlohmann::json& j, RDGLineage& lineage);

}  // namespace tsuba

#endif
