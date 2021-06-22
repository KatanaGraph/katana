#include "katana/Http.h"

#include <curl/curl.h>

#include "katana/ErrorCode.h"
#include "katana/Logging.h"
#include "katana/Result.h"

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

  static katana::Result<CurlHandle> Make(
      const std::string& url, std::vector<char>* response) {
    CURL* curl = curl_easy_init();
    if (!curl) {
      return katana::ErrorCode::HttpError;
    }
    CurlHandle handle(curl);
    KATANA_CHECK(handle.SetOpt(CURLOPT_URL, url.c_str()));
    KATANA_CHECK(handle.SetOpt(CURLOPT_WRITEDATA, response));
    KATANA_CHECK(handle.SetOpt(CURLOPT_WRITEFUNCTION, WriteDataToVectorCB));
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
  katana::Result<void> SetOpt(CURLoption option, T param) {
    if (auto err = curl_easy_setopt(handle_, option, param); err != CURLE_OK) {
      KATANA_LOG_ERROR("CURL error: {}", curl_easy_strerror(err));
      return katana::ErrorCode::InvalidArgument;
    }
    return katana::ResultSuccess();
  }

  katana::Result<void> Perform() {
    if (headers_ != nullptr) {
      KATANA_CHECK(SetOpt(CURLOPT_HTTPHEADER, headers_));
    }
    CURLcode request_res = curl_easy_perform(handle_);
    if (request_res != CURLE_OK) {
      KATANA_LOG_ERROR("CURL error: {}", curl_easy_strerror(request_res));
      return katana::ErrorCode::HttpError;
    }

    int64_t response_code;
    curl_easy_getinfo(handle_, CURLINFO_RESPONSE_CODE, &response_code);
    switch (response_code) {
    case 200:
      return katana::ResultSuccess();
    case 404:
      return katana::ErrorCode::NotFound;
    case 400:
      return katana::ErrorCode::HttpError;
    case 409:
      return katana::ErrorCode::AlreadyExists;
    default:
      KATANA_LOG_ERROR(
          "HTTP request returned unhandled code: {}", response_code);
      return katana::ErrorCode::HttpError;
    }
  }
};

katana::Result<void>
HttpUploadCommon(CurlHandle&& holder, const std::string& data) {
  KATANA_CHECK(holder.SetOpt(CURLOPT_POSTFIELDS, data.c_str()));
  KATANA_CHECK(holder.SetOpt(CURLOPT_POSTFIELDSIZE, data.size()));
  holder.SetHeader("Content-Type: application/json");
  holder.SetHeader("Accept: application/json");
  return holder.Perform();
}

}  // namespace

katana::Result<void>
katana::HttpGet(const std::string& url, std::vector<char>* response) {
  CurlHandle curl = KATANA_CHECK(CurlHandle::Make(url, response));
  KATANA_CHECK(curl.SetOpt(CURLOPT_HTTPGET, 1L));
  if (auto res = curl.Perform(); !res) {
    KATANA_LOG_DEBUG("GET failed for url: {}", url);
    return res;
  }
  return katana::ResultSuccess();
}

katana::Result<void>
katana::HttpPost(
    const std::string& url, const std::string& data,
    std::vector<char>* response) {
  auto handle_res = CurlHandle::Make(url, response);
  if (!handle_res) {
    KATANA_LOG_ERROR("POST failed for url: {}", url);
    return handle_res.error();
  }

  if (auto res = HttpUploadCommon(std::move(handle_res.value()), data); !res) {
    KATANA_LOG_DEBUG("POST failed for url: {}", url);
    return res.error();
  }
  return katana::ResultSuccess();
}

katana::Result<void>
katana::HttpDelete(const std::string& url, std::vector<char>* response) {
  CurlHandle curl = KATANA_CHECK(CurlHandle::Make(url, response));
  KATANA_CHECK(curl.SetOpt(CURLOPT_CUSTOMREQUEST, "DELETE"));

  if (auto res = curl.Perform(); !res) {
    KATANA_LOG_DEBUG("DELETE failed for url: {}", url);
    return res;
  }
  return katana::ResultSuccess();
}

katana::Result<void>
katana::HttpPut(
    const std::string& url, const std::string& data,
    std::vector<char>* response) {
  CurlHandle curl = KATANA_CHECK(CurlHandle::Make(url, response));
  KATANA_CHECK(curl.SetOpt(CURLOPT_CUSTOMREQUEST, "PUT"));

  if (auto res = HttpUploadCommon(std::move(curl), data); !res) {
    KATANA_LOG_DEBUG("PUT failed for url: {}", url);
    return res.error();
  }
  return katana::ResultSuccess();
}

katana::Result<void>
katana::HttpInit() {
  auto init_ret = curl_global_init(CURL_GLOBAL_ALL);
  if (init_ret != CURLE_OK) {
    KATANA_LOG_ERROR(
        "libcurl initialization failed: {}", curl_easy_strerror(init_ret));
    return ErrorCode::HttpError;
  }
  return katana::ResultSuccess();
}
