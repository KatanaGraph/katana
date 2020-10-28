#include "S3Storage.h"

#include <regex>

#include "GlobalState.h"
#include "galois/Result.h"
#include "s3.h"
#include "tsuba/Errors.h"
#include "tsuba/file.h"

namespace {

const std::regex kS3UriRegex("s3://([-a-z0-9.]+)/(.+)");
const std::regex kS3BucketRegex("s3://([-a-z0-9.]+)");

}  // namespace

namespace tsuba {

tsuba::GlobalFileStorageAllocator s3_storage_allocator([]() {
  return std::unique_ptr<tsuba::FileStorage>(new tsuba::S3Storage());
});

galois::Result<std::pair<std::string, std::string>>
S3Storage::CleanUri(const std::string& uri) {
  std::smatch sub_match;
  if (!std::regex_match(uri, sub_match, kS3UriRegex)) {
    if (std::regex_match(uri, sub_match, kS3BucketRegex)) {
      // This can happen with FileDelete
      return std::make_pair(sub_match[1], "");
    }
    return ErrorCode::InvalidArgument;
  }
  return std::make_pair(sub_match[1], sub_match[2]);
}

galois::Result<void>
S3Storage::Init() {
  auto res = internal::S3Init();
  if (!res) {
    GALOIS_LOG_WARN("failed to initailize S3: {}", res.error());
    return ErrorCode::InvalidArgument;
  }
  s3_client = res.value();
  return galois::ResultSuccess();
}

galois::Result<void>
S3Storage::Fini() {
  return internal::S3Fini(s3_client);
}

galois::Result<void>
S3Storage::Stat(const std::string& uri, StatBuf* s_buf) {
  auto uri_res = CleanUri(std::string(uri));
  if (!uri_res) {
    return uri_res.error();
  }
  auto [bucket, object] = std::move(uri_res.value());
  return S3GetSize(s3_client, bucket, object, &s_buf->size);
}

galois::Result<void>
S3Storage::GetMultiSync(
    const std::string& uri, uint64_t start, uint64_t size,
    uint8_t* result_buf) {
  auto uri_res = CleanUri(std::string(uri));
  if (!uri_res) {
    return uri_res.error();
  }
  auto [bucket, object] = std::move(uri_res.value());
  return tsuba::S3DownloadRange(
      s3_client, bucket, object, start, size, result_buf);
}

galois::Result<void>
S3Storage::PutMultiSync(
    const std::string& uri, const uint8_t* data, uint64_t size) {
  auto uri_res = CleanUri(std::string(uri));
  if (!uri_res) {
    return uri_res.error();
  }
  auto [bucket, object] = std::move(uri_res.value());
  return tsuba::S3UploadOverwrite(s3_client, bucket, object, data, size);
}

std::future<galois::Result<void>>
S3Storage::PutAsync(
    const std::string& uri, const uint8_t* data, uint64_t size) {
  auto uri_res = CleanUri(std::string(uri));
  if (!uri_res) {
    return std::async(
        [=]() -> galois::Result<void> { return uri_res.error(); });
  }
  auto [bucket, object] = std::move(uri_res.value());
  return tsuba::S3PutAsync(s3_client, bucket, object, data, size);
}

std::future<galois::Result<void>>
S3Storage::GetAsync(
    const std::string& uri, uint64_t start, uint64_t size,
    uint8_t* result_buf) {
  auto uri_res = CleanUri(std::string(uri));
  if (!uri_res) {
    return std::async(
        [=]() -> galois::Result<void> { return uri_res.error(); });
  }
  auto [bucket, object] = std::move(uri_res.value());
  return tsuba::S3GetAsync(s3_client, bucket, object, start, size, result_buf);
}

std::future<galois::Result<void>>
S3Storage::ListAsync(
    const std::string& uri, std::vector<std::string>* list,
    std::vector<uint64_t>* size) {
  auto uri_res = CleanUri(std::string(uri));
  if (!uri_res) {
    return std::async(
        [=]() -> galois::Result<void> { return uri_res.error(); });
  }
  auto [bucket, object] = std::move(uri_res.value());
  return tsuba::S3ListAsync(s3_client, bucket, object, list, size);
}

galois::Result<void>
S3Storage::Delete(
    const std::string& uri, const std::unordered_set<std::string>& files) {
  auto uri_res = CleanUri(std::string(uri));
  if (!uri_res) {
    return uri_res.error();
  }
  auto [bucket, object] = std::move(uri_res.value());
  return tsuba::S3Delete(s3_client, bucket, object, files);
}

}  // namespace tsuba
