#ifndef GALOIS_LIBSUPPORT_GALOIS_HTTP_H_
#define GALOIS_LIBSUPPORT_GALOIS_HTTP_H_

#include "galois/JSON.h"
#include "galois/Result.h"

namespace galois {

GALOIS_EXPORT Result<void> HttpInit();

/// Perform an HTTP get request on url and fill buffer with the result on success
GALOIS_EXPORT Result<void> HttpGet(
    const std::string& url, std::vector<char>* buffer);

/// Perform an HTTP post request on url and send the contents of buffer
GALOIS_EXPORT Result<void> HttpPost(
    const std::string& url, const std::string& data);

/// Perform an HTTP put request on url and send the contents of buffer
GALOIS_EXPORT Result<void> HttpPut(
    const std::string& url, const std::string& data);

template <typename T>
Result<T>
HttpGetJson(const std::string& url) {
  std::vector<char> buffer;
  if (auto res = HttpGet(url, &buffer); !res) {
    return res.error();
  }
  return JsonParse<T>(buffer);
}

template <typename T>
Result<void>
HttpPostJson(const std::string& url, const T& obj) {
  auto json_res = JsonDump(obj);
  if (!json_res) {
    return json_res.error();
  }
  return HttpPost(url, std::move(json_res.value()));
}

template <typename T>
Result<void>
HttpPutJson(const std::string& url, const T& obj) {
  auto json_res = JsonDump(nlohmann::json(obj));
  if (!json_res) {
    return json_res.error();
  }
  return HttpPut(url, std::move(json_res.value()));
}

}  // namespace galois

#endif
