#include "galois/JSON.h"

galois::Result<std::string>
galois::JsonDump(const nlohmann::json& obj) {
  try {
    return obj.dump();
  } catch (const std::exception& exp) {
    GALOIS_LOG_DEBUG("nlohmann::json::dump exception: {}", exp.what());
  }
  return galois::ErrorCode::JsonDumpFailed;
}
