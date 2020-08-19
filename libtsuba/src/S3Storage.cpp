#include "S3Storage.h"

#include <regex>

#include "galois/Result.h"
#include "tsuba/file.h"
#include "tsuba/Errors.h"
#include "s3.h"

namespace {

const std::regex kS3UriRegex("s3://([-a-z0-9.]+)/(.+)");

} // namespace

namespace tsuba {

galois::Result<std::pair<std::string, std::string>>
S3Storage::CleanURI(const std::string& uri) {
  std::smatch sub_match;
  if (!std::regex_match(uri, sub_match, kS3UriRegex)) {
    return ErrorCode::InvalidArgument;
  }
  return std::make_pair(sub_match[1], sub_match[2]);
}

galois::Result<void> S3Storage::Init() { return S3Init(); }

galois::Result<void> S3Storage::Fini() { return S3Fini(); }

galois::Result<void> S3Storage::Stat(const std::string& uri, StatBuf* s_buf) {
  auto uri_res = CleanURI(std::string(uri));
  if (!uri_res) {
    return uri_res.error();
  }
  auto [bucket, object] = std::move(uri_res.value());
  return S3GetSize(bucket, object, &s_buf->size);
}

galois::Result<void> S3Storage::Create(const std::string& uri, bool overwrite) {
  auto uri_res = CleanURI(std::string(uri));
  if (!uri_res) {
    return uri_res.error();
  }
  auto [bucket, object] = std::move(uri_res.value());
  auto exists_res       = tsuba::S3Exists(bucket, object);
  if (!exists_res) {
    return exists_res.error();
  }

  if (!overwrite && exists_res.value()) {
    return tsuba::ErrorCode::Exists;
  }
  // S3 has atomic puts, so create on write.
  return galois::ResultSuccess();
}

galois::Result<void> S3Storage::GetMultiSync(const std::string& uri,
                                             uint64_t start, uint64_t size,
                                             uint8_t* result_buf) {
  auto uri_res = CleanURI(std::string(uri));
  if (!uri_res) {
    return uri_res.error();
  }
  auto [bucket, object] = std::move(uri_res.value());
  return tsuba::S3DownloadRange(bucket, object, start, size, result_buf);
}

galois::Result<void> S3Storage::PutMultiSync(const std::string& uri,
                                             const uint8_t* data,
                                             uint64_t size) {
  auto uri_res = CleanURI(std::string(uri));
  if (!uri_res) {
    return uri_res.error();
  }
  auto [bucket, object] = std::move(uri_res.value());
  return tsuba::S3UploadOverwrite(bucket, object, data, size);
}

galois::Result<std::unique_ptr<FileAsyncWork>>
S3Storage::PutAsync(const std::string& uri, const uint8_t* data,
                    uint64_t size) {
  auto uri_res = CleanURI(std::string(uri));
  if (!uri_res) {
    return uri_res.error();
  }
  auto [bucket, object] = std::move(uri_res.value());
  return tsuba::S3PutAsync(bucket, object, data, size);
}

galois::Result<std::unique_ptr<FileAsyncWork>>
S3Storage::GetAsync(const std::string& uri, uint64_t start, uint64_t size,
                    uint8_t* result_buf) {
  auto uri_res = CleanURI(std::string(uri));
  if (!uri_res) {
    return uri_res.error();
  }
  auto [bucket, object] = std::move(uri_res.value());
  return tsuba::S3GetAsync(bucket, object, start, size, result_buf);
}

galois::Result<std::unique_ptr<FileAsyncWork>>
S3Storage::ListAsync(const std::string& uri) {
  auto uri_res = CleanURI(std::string(uri));
  if (!uri_res) {
    return uri_res.error();
  }
  auto [bucket, object] = std::move(uri_res.value());
  return tsuba::S3ListAsync(bucket, object);
}

} // namespace tsuba
