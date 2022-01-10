#include "katana/RDGLineage.h"

#include "katana/Logging.h"

using json = nlohmann::json;

void
katana::RDGLineage::AddCommandLine(const std::string& cmd) {
  if (!command_line_.empty()) {
    KATANA_LOG_DEBUG(
        "Add command line to lineage was: {} is: {}", command_line_, cmd);
  }
  command_line_ = cmd;
}

void
katana::RDGLineage::ClearLineage() {
  command_line_.clear();
}

void
katana::to_json(json& j, const katana::RDGLineage& lineage) {
  j = json{{"command_line", lineage.command_line_}};
}

void
katana::from_json(const json& j, katana::RDGLineage& lineage) {
  j.at("command_line").get_to(lineage.command_line_);
}
