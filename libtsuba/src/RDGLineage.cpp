#include "tsuba/RDGLineage.h"

#include "katana/Logging.h"

using json = nlohmann::json;

namespace tsuba {

void
RDGLineage::AddCommandLine(const std::string& cmd) {
  if (!command_line_.empty()) {
    KATANA_LOG_DEBUG(
        "Add command line to lineage was: {} is: {}", command_line_, cmd);
  }
  command_line_ = cmd;
}

void
RDGLineage::ClearLineage() {
  command_line_.clear();
}

}  // namespace tsuba

void
tsuba::to_json(json& j, const tsuba::RDGLineage& lineage) {
  j = json{{"command_line", lineage.command_line_}};
}

void
tsuba::from_json(const json& j, tsuba::RDGLineage& lineage) {
  j.at("command_line").get_to(lineage.command_line_);
}
