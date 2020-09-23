#ifndef GALOIS_LIBSUPPORT_GALOIS_JSON_H_
#define GALOIS_LIBSUPPORT_GALOIS_JSON_H_

#include <nlohmann/json.hpp>

#include "galois/ErrorCode.h"
#include "galois/Logging.h"
#include "galois/Result.h"

namespace galois {

/// call parse and turn exceptions into the results we know and love
template <typename T, typename U>
galois::Result<T>
JsonParse(U& obj) {
  try {
    auto j = nlohmann::json::parse(obj.begin(), obj.end());
    return j.template get<T>();
  } catch (const std::exception& exp) {
    GALOIS_LOG_DEBUG("nlohmann::json::parse exception: {}", exp.what());
  }
  return galois::ErrorCode::JsonParseFailed;
}

template <typename T, typename U>
galois::Result<void>
JsonParse(U& obj, T* val) {
  try {
    auto j = nlohmann::json::parse(obj.begin(), obj.end());
    j.get_to(*val);
    return galois::ResultSuccess();
  } catch (const std::exception& exp) {
    GALOIS_LOG_DEBUG("nlohmann::json::parse exception: {}", exp.what());
  }
  return galois::ErrorCode::JsonParseFailed;
}

/// Dump to string, but catch errors
galois::Result<std::string> GALOIS_EXPORT JsonDump(const nlohmann::json& obj);

template <typename T>
galois::Result<std::string>
JsonDump(const T& obj) {
  return JsonDump(nlohmann::json(obj));
}

}  // namespace galois

#endif
