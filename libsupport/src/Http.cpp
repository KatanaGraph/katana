#include "galois/Http.h"

#include <curl/curl.h>

#include "galois/ErrorCode.h"
#include "galois/Logging.h"
#include "galois/Result.h"

namespace {

class CurlHandle {
  CURL* handle_{};
  struct curl_slist* headers_{};

  CurlHandle(CURL* handle) : handle_(handle) {}

  static size_t WriteDataToVectorCB(
      char* ptr, size_t size, size_t nmemb, void* user_data) {
    size_t real_size = size * nmemb;
    auto buffer = static_cast<std::vector<char>*>(user_data);
    buffer->insert(
        buffer->end(), ptr, ptr + real_size);  // NOLINT (c interface)
    return real_size;
  };

public:
  CurlHandle(const CurlHandle& no_copy) = delete;
  CurlHandle& operator=(const CurlHandle& no_copy) = delete;
  CurlHandle(CurlHandle&& other) noexcept {
    std::swap(handle_, other.handle_);
    std::swap(headers_, other.headers_);
  }
  CurlHandle& operator=(CurlHandle&& other) noexcept {
    std::swap(handle_, other.handle_);
    std::swap(headers_, other.headers_);
    return *this;
  }

  static galois::Result<CurlHandle> Make(
      const std::string& url, std::vector<char>* response) {
    CURL* curl = curl_easy_init();
    if (!curl) {
      return galois::ErrorCode::HttpError;
    }
    CurlHandle handle(curl);
    if (auto res = handle.SetOpt(CURLOPT_URL, url.c_str()); !res) {
      return res.error();
    }
    if (auto res = handle.SetOpt(CURLOPT_WRITEDATA, response); !res) {
      return res.error();
    }
    if (auto res = handle.SetOpt(CURLOPT_WRITEFUNCTION, WriteDataToVectorCB);
        !res) {
      return res.error();
    }
    return CurlHandle(std::move(handle));
  }

  CURL* handle() { return handle_; }
  ~CurlHandle() {
    if (headers_ != nullptr) {
      curl_slist_free_all(headers_);
    }
    if (handle_ != nullptr) {
      curl_easy_cleanup(handle_);
    }
  }

  void SetHeader(const std::string& header) {
    headers_ = curl_slist_append(headers_, header.c_str());
  }

  template <typename T>
  galois::Result<void> SetOpt(CURLoption option, T param) {
    if (auto err = curl_easy_setopt(handle_, option, param); err != CURLE_OK) {
      GALOIS_LOG_DEBUG("CURL error: {}", curl_easy_strerror(err));
      return galois::ErrorCode::InvalidArgument;
    }
    return galois::ResultSuccess();
  }

  galois::Result<void> Perform() {
    if (headers_ != nullptr) {
      if (auto res = SetOpt(CURLOPT_HTTPHEADER, headers_); !res) {
        return res.error();
      }
    }
    CURLcode request_res = curl_easy_perform(handle_);
    if (request_res != CURLE_OK) {
      GALOIS_LOG_ERROR("CURL error: {}", curl_easy_strerror(request_res));
      return galois::ErrorCode::HttpError;
    }

    int64_t response_code;
    curl_easy_getinfo(handle_, CURLINFO_RESPONSE_CODE, &response_code);
    switch (response_code) {
    case 200:
      return galois::ResultSuccess();
    case 404:
      return galois::ErrorCode::NotFound;
    case 400:
      return galois::ErrorCode::InvalidArgument;
    default:
      GALOIS_LOG_DEBUG(
          "HTTP request returned unhandled code: {}", response_code);
      return galois::ErrorCode::HttpError;
    }
  }
};

galois::Result<void>
HttpUploadCommon(CurlHandle&& holder, const std::string& data) {
  if (auto res = holder.SetOpt(CURLOPT_POSTFIELDS, data.c_str()); !res) {
    return res.error();
  }
  if (auto res = holder.SetOpt(CURLOPT_POSTFIELDSIZE, data.size()); !res) {
    return res.error();
  }
  holder.SetHeader("Content-Type: application/json");
  holder.SetHeader("Accept: application/json");
  return holder.Perform();
}

}  // namespace

galois::Result<void>
galois::HttpGet(const std::string& url, std::vector<char>* response) {
  auto curl_res = CurlHandle::Make(url, response);
  if (!curl_res) {
    return curl_res.error();
  }
  CurlHandle curl(std::move(curl_res.value()));

  if (auto res = curl.SetOpt(CURLOPT_HTTPGET, 1L); !res) {
    return res.error();
  }
  if (auto res = curl.Perform(); !res) {
    GALOIS_LOG_DEBUG("GET failed for url: {}", url);
    return res;
  }
  return galois::ResultSuccess();
}

galois::Result<void>
galois::HttpPost(
    const std::string& url, const std::string& data,
    std::vector<char>* response) {
  auto handle_res = CurlHandle::Make(url, response);
  if (!handle_res) {
    GALOIS_LOG_ERROR("POST failed for url: {}", url);
    return handle_res.error();
  }

  if (auto res = HttpUploadCommon(std::move(handle_res.value()), data); !res) {
    GALOIS_LOG_DEBUG("POST failed for url: {}", url);
    return res.error();
  }
  return galois::ResultSuccess();
}

galois::Result<void>
galois::HttpDelete(const std::string& url, std::vector<char>* response) {
  auto curl_res = CurlHandle::Make(url, response);
  if (!curl_res) {
    return curl_res.error();
  }
  CurlHandle curl = std::move(curl_res.value());

  if (auto res = curl.SetOpt(CURLOPT_CUSTOMREQUEST, "DELETE"); !res) {
    return res.error();
  }
  if (auto res = curl.Perform(); !res) {
    GALOIS_LOG_DEBUG("DELETE failed for url: {}", url);
    return res;
  }
  return galois::ResultSuccess();
}

galois::Result<void>
galois::HttpPut(
    const std::string& url, const std::string& data,
    std::vector<char>* response) {
  auto curl_res = CurlHandle::Make(url, response);
  if (!curl_res) {
    return curl_res.error();
  }
  CurlHandle curl = std::move(curl_res.value());

  if (auto res = curl.SetOpt(CURLOPT_CUSTOMREQUEST, "PUT"); !res) {
    return res.error();
  }

  if (auto res = HttpUploadCommon(std::move(curl), data); !res) {
    GALOIS_LOG_DEBUG("PUT failed for url: {}", url);
    return res.error();
  }
  return galois::ResultSuccess();
}

galois::Result<void>
galois::HttpInit() {
  auto init_ret = curl_global_init(CURL_GLOBAL_ALL);
  if (init_ret != CURLE_OK) {
    GALOIS_LOG_ERROR(
        "libcurl initialization failed: {}", curl_easy_strerror(init_ret));
    return ErrorCode::HttpError;
  }
  return galois::ResultSuccess();
}
