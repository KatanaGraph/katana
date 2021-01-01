#ifndef KATANA_LIBSUPPORT_KATANA_HTTP_H_
#define KATANA_LIBSUPPORT_KATANA_HTTP_H_

#include "katana/JSON.h"
#include "katana/Result.h"

namespace katana {

KATANA_EXPORT Result<void> HttpInit();

/// Perform an HTTP get request on url and fill buffer with the result on success
KATANA_EXPORT Result<void> HttpGet(
    const std::string& url, std::vector<char>* response);

/// Perform an HTTP post request on url and send the contents of buffer
KATANA_EXPORT Result<void> HttpPost(
    const std::string& url, const std::string& data,
    std::vector<char>* response);

/// Perform an HTTP put request on url and send the contents of buffer
KATANA_EXPORT Result<void> HttpPut(
    const std::string& url, const std::string& data,
    std::vector<char>* response);

/// Perform an HTTP delete request on url and send the contents of buffer
KATANA_EXPORT Result<void> HttpDelete(
    const std::string& url, std::vector<char>* response);

template <typename T, typename Callable, typename... Args>
Result<T>
HttpOpJson(Callable func, Args&&... args) {
  std::vector<char> response;
  auto res = func(std::forward<Args>(args)..., &response);
  if (!res) {
    return res.error();
  }
  return JsonParse<T>(response);
}

// Use these if the server speaks JSON
template <typename T>
Result<T>
HttpGetJson(const std::string& url) {
  return HttpOpJson<T>(HttpGet, url);
}

template <typename T>
Result<T>
HttpDeleteJson(const std::string& url) {
  return HttpOpJson<T>(HttpDelete, url);
}

template <typename T, typename U>
Result<U>
HttpPostJson(const std::string& url, const T& obj) {
  auto json_res = JsonDump(obj);
  if (!json_res) {
    return json_res.error();
  }
  return HttpOpJson<U>(HttpPost, url, std::move(json_res.value()));
}

template <typename T, typename U>
Result<U>
HttpPutJson(const std::string& url, const T& obj) {
  auto json_res = JsonDump(obj);
  if (!json_res) {
    return json_res.error();
  }
  return HttpOpJson<U>(HttpPut, url, std::move(json_res.value()));
}

}  // namespace katana

#endif
