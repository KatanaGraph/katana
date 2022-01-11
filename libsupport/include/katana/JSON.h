#ifndef KATANA_LIBSUPPORT_KATANA_JSON_H_
#define KATANA_LIBSUPPORT_KATANA_JSON_H_

#include <nlohmann/json.hpp>

#include "katana/ErrorCode.h"
#include "katana/Logging.h"
#include "katana/Result.h"

namespace katana {

/// call parse and turn exceptions into the results we know and love
template <typename T, typename U>
katana::Result<T>
JsonParse(U& obj) {
  try {
    auto j = nlohmann::json::parse(obj.begin(), obj.end());
    return j.template get<T>();
  } catch (const std::exception& exp) {
    return KATANA_ERROR(
        katana::ErrorCode::JSONParseFailed, "parsing json: {}", exp.what());
  }
  return katana::ErrorCode::JSONParseFailed;
}

template <typename T, typename U>
katana::Result<void>
JsonParse(U& obj, T* val) {
  try {
    auto j = nlohmann::json::parse(obj.begin(), obj.end());
    j.get_to(*val);
    return katana::ResultSuccess();
  } catch (const std::exception& exp) {
    return KATANA_ERROR(
        katana::ErrorCode::JSONParseFailed, "parsing json: {}", exp.what());
  }
  return katana::Result<void>(katana::ErrorCode::JSONParseFailed);
}

/// Dump to string, but catch errors
KATANA_EXPORT katana::Result<std::string> JsonDump(const nlohmann::json& obj);
KATANA_EXPORT katana::Result<std::string> JsonDump(
    const nlohmann::ordered_json& obj);

template <typename T>
katana::Result<std::string>
JsonDump(const T& obj) {
  return JsonDump(nlohmann::json(obj));
}

}  // namespace katana

#endif
