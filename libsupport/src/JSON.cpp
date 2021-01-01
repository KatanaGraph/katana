#include "katana/JSON.h"

katana::Result<std::string>
katana::JsonDump(const nlohmann::json& obj) {
  try {
    return obj.dump();
  } catch (const std::exception& exp) {
    KATANA_LOG_DEBUG("nlohmann::json::dump exception: {}", exp.what());
  }
  return katana::ErrorCode::JsonDumpFailed;
}
