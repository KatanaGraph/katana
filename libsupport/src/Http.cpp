#include "galois/Http.h"

#include <curl/curl.h>

#include "galois/Logging.h"
#include "galois/Result.h"

namespace {

class CurlHandleHolder {
  CURL* handle_;
  struct curl_slist* headers_;

public:
  CurlHandleHolder(CURL* handle) : handle_(handle), headers_(nullptr) {}
  CurlHandleHolder(const CurlHandleHolder& no_copy) = delete;
  CurlHandleHolder& operator=(const CurlHandleHolder& no_copy) = delete;
  CurlHandleHolder(CurlHandleHolder&& other)
      : handle_(nullptr), headers_(nullptr) {
    std::swap(handle_, other.handle_);
    std::swap(headers_, other.headers_);
  }
  CurlHandleHolder& operator=(CurlHandleHolder&& other) {
    std::swap(handle_, other.handle_);
    std::swap(headers_, other.headers_);
    return *this;
  }
  CURL* handle() { return handle_; }
  ~CurlHandleHolder() {
    if (headers_ != nullptr) {
      curl_slist_free_all(headers_);
    }
    if (handle_ != nullptr) {
      curl_easy_cleanup(handle_);
    }
  }
  void SetHeadersToFree(struct curl_slist* headers) {
    if (headers_ != nullptr) {
      curl_slist_free_all(headers_);
    }
    headers_ = headers;
  }
};

size_t
WriteDataToVectorCB(char* ptr, size_t size, size_t nmemb, void* user_data) {
  size_t real_size = size * nmemb;
  auto buffer = static_cast<std::vector<char>*>(user_data);
  buffer->insert(buffer->end(), ptr, ptr + real_size);  // NOLINT (c interface)
  return real_size;
};

template <typename T>
galois::Result<void>
CurlSetopt(CURL* handle, CURLoption option, T param) {
  if (auto err = curl_easy_setopt(handle, option, param); err != CURLE_OK) {
    GALOIS_LOG_DEBUG("CURL error: {}", curl_easy_strerror(err));
    return galois::ErrorCode::InvalidArgument;
  }
  return galois::ResultSuccess();
}

galois::Result<void>
HttpUploadCommon(
    CurlHandleHolder&& holder, const std::string& url,
    const std::string& data) {
  if (auto res = CurlSetopt(holder.handle(), CURLOPT_POSTFIELDS, data.c_str());
      !res) {
    return res.error();
  }
  if (auto res =
          CurlSetopt(holder.handle(), CURLOPT_POSTFIELDSIZE, data.size());
      !res) {
    return res.error();
  }

  struct curl_slist* headers = NULL;
  headers = curl_slist_append(headers, "Content-Type: application/json");
  headers = curl_slist_append(headers, "Accept: application/json");

  holder.SetHeadersToFree(headers);

  if (auto res = CurlSetopt(holder.handle(), CURLOPT_HTTPHEADER, headers);
      !res) {
    return res.error();
  }
  if (auto res = CurlSetopt(holder.handle(), CURLOPT_URL, url.c_str()); !res) {
    return res.error();
  }

  CURLcode request_res = curl_easy_perform(holder.handle());
  if (request_res != CURLE_OK) {
    GALOIS_LOG_ERROR("CURL error: {}", curl_easy_strerror(request_res));
    return galois::ErrorCode::HttpError;
  }

  long response_code;
  curl_easy_getinfo(holder.handle(), CURLINFO_RESPONSE_CODE, &response_code);
  if (response_code != 200) {
    return galois::ErrorCode::HttpError;
  }
  return galois::ResultSuccess();
}

}  // namespace

galois::Result<void>
galois::HttpGet(const std::string& url, std::vector<char>* buffer) {
  CURL* curl = curl_easy_init();
  if (!curl) {
    return ResultErrno();
  }
  CurlHandleHolder holder(curl);
  if (auto res = CurlSetopt(holder.handle(), CURLOPT_HTTPGET, 1L); !res) {
    return res.error();
  }
  if (auto res = CurlSetopt(holder.handle(), CURLOPT_URL, url.c_str()); !res) {
    return res.error();
  }
  if (auto res = CurlSetopt(holder.handle(), CURLOPT_WRITEDATA, buffer); !res) {
    return res.error();
  }
  if (auto res = CurlSetopt(
          holder.handle(), CURLOPT_WRITEFUNCTION, WriteDataToVectorCB);
      !res) {
    return res.error();
  }
  CURLcode request_res = curl_easy_perform(holder.handle());
  if (request_res != CURLE_OK) {
    GALOIS_LOG_ERROR("CURL error: {}", curl_easy_strerror(request_res));
    return ErrorCode::HttpError;
  }
  long response_code;
  curl_easy_getinfo(holder.handle(), CURLINFO_RESPONSE_CODE, &response_code);
  if (response_code != 200) {
    return ErrorCode::HttpError;
  }
  return ResultSuccess();
}

galois::Result<void>
galois::HttpPost(const std::string& url, const std::string& data) {
  CURL* curl = curl_easy_init();
  if (!curl) {
    return ResultErrno();
  }
  return HttpUploadCommon(CurlHandleHolder(curl), url, data);
}

galois::Result<void>
galois::HttpPut(const std::string& url, const std::string& data) {
  CURL* curl = curl_easy_init();
  if (!curl) {
    return ResultErrno();
  }
  CurlHandleHolder holder(curl);
  if (auto res = CurlSetopt(holder.handle(), CURLOPT_CUSTOMREQUEST, "PUT");
      !res) {
    return res.error();
  }
  return HttpUploadCommon(std::move(holder), url, data);
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
