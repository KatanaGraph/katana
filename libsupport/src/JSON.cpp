#include "katana/JSON.h"

/// ,_._._._._._._._._|__________________________________________________________,
/// |_|_|_|_|_|_|_|_|_|_________________________________________________________/
///                   !

///  __-----_________________{]__________________________________________________
/// {&&&&&&&#%%&#%&%&%&%&%#%&|]__________________________________________________\
///                          {]

katana::Result<std::string>
katana::JsonDump(const nlohmann::json& obj) {
  try {
    return obj.dump();
  } catch (const std::exception& exp) {
    return KATANA_ERROR(
        katana::ErrorCode::JSONDumpFailed, "nlohmann::json::dump exception: {}",
        exp.what());
  }
}

katana::Result<std::string>
katana::JsonDump(const nlohmann::ordered_json& obj) {
  try {
    return obj.dump();
  } catch (const std::exception& exp) {
    return KATANA_ERROR(
        katana::ErrorCode::JSONDumpFailed,
        "nlohmann::ordered_json::dump exception: {}", exp.what());
  }
}
