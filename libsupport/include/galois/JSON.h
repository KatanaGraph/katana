#ifndef GALOIS_LIBSUPPORT_GALOIS_JSON_H_
#define GALOIS_LIBSUPPORT_GALOIS_JSON_H_

#include <nlohmann/json.hpp>

#include "galois/Result.h"
#include "galois/Logging.h"
#include "galois/ErrorCode.h"

namespace {

/// call parse and turn exceptions into the results we know and love
template <typename T, typename O>
galois::Result<T> JsonParse(O& obj) {
  try {
    auto j = nlohmann::json::parse(obj.begin(), obj.end());
    return j.template get<T>();
  } catch (const std::exception& exp) {
    GALOIS_LOG_DEBUG("nlohmann::json exception: {}", exp.what());
  }
  return galois::ErrorCode::JsonParseFailed;
}

template <typename T, typename O>
galois::Result<void> JsonParse(O& obj, T* val) {
  try {
    auto j = nlohmann::json::parse(obj.begin(), obj.end());
    j.get_to(*val);
    return galois::ResultSuccess();
  } catch (const std::exception& exp) {
    GALOIS_LOG_DEBUG("nlohmann::json exception: {}", exp.what());
  }
  return galois::ErrorCode::JsonParseFailed;
}

template <typename T>
galois::Result<std::string> JsonDump(const T& obj) {
  try {
    return obj.dump();
  } catch (const std::exception& exp) {
    GALOIS_LOG_DEBUG("nlohmann::json exception: {}", exp.what());
  }
  return galois::ErrorCode::JsonDumpFailed;
}

} // namespace

#endif
