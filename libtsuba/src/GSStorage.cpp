#include "GSStorage.h"

#include <regex>
#include <unordered_set>

#include "GlobalState.h"
#include "galois/Result.h"
#include "galois/Uri.h"
#include "gs.h"
#include "tsuba/Errors.h"
#include "tsuba/file.h"

namespace {

const std::regex kGSUriRegex("gs://([-a-z0-9.]+)/(.+)");
const std::regex kGSBucketRegex("gs://([-a-z0-9.]+)");

}  // namespace

namespace tsuba {

GlobalFileStorageAllocator gs_storage_allocator([]() {
  return std::unique_ptr<FileStorage>(new GSStorage());
});

galois::Result<std::pair<std::string, std::string>>
GSStorage::CleanUri(const std::string& uri) {
  std::smatch sub_match;
  if (!std::regex_match(uri, sub_match, kGSUriRegex)) {
    if (std::regex_match(uri, sub_match, kGSBucketRegex)) {
      // This can happen with FileDelete
      return std::make_pair(sub_match[1], "");
    }
    return ErrorCode::InvalidArgument;
  }
  return std::make_pair(sub_match[1], sub_match[2]);
}

galois::Result<void>
GSStorage::Init() {
  auto res = GSInit();
  if (!res) {
    GALOIS_LOG_WARN("failed to initailize GS: {}", res.error());
    return ErrorCode::InvalidArgument;
  }
  s3_client = res.value();
  return galois::ResultSuccess();
}

galois::Result<void>
GSStorage::Fini() {
  return GSFini(s3_client);
}

galois::Result<void>
GSStorage::Stat(const std::string& uri, StatBuf* s_buf) {
  auto uri_res = CleanUri(std::string(uri));
  if (!uri_res) {
    return uri_res.error();
  }
  auto [bucket, object] = std::move(uri_res.value());
  return GSGetSize(s3_client, bucket, object, &s_buf->size);
}

galois::Result<void>
GSStorage::GetMultiSync(
    const std::string& uri, uint64_t start, uint64_t size,
    uint8_t* result_buf) {
  auto uri_res = CleanUri(std::string(uri));
  if (!uri_res) {
    return uri_res.error();
  }
  auto [bucket, object] = std::move(uri_res.value());
  return GSGetSync(s3_client, bucket, object, start, size, result_buf);
}

galois::Result<void>
GSStorage::PutMultiSync(
    const std::string& uri, const uint8_t* data, uint64_t size) {
  auto uri_res = CleanUri(std::string(uri));
  if (!uri_res) {
    return uri_res.error();
  }
  auto [bucket, object] = std::move(uri_res.value());
  return GSPutSync(s3_client, bucket, object, data, size);
}

std::future<galois::Result<void>>
GSStorage::PutAsync(
    const std::string& uri, const uint8_t* data, uint64_t size) {
  auto uri_res = CleanUri(std::string(uri));
  if (!uri_res) {
    return std::async(
        [=]() -> galois::Result<void> { return uri_res.error(); });
  }
  auto [bucket, object] = std::move(uri_res.value());
  return GSPutAsync(s3_client, bucket, object, data, size);
}

std::future<galois::Result<void>>
GSStorage::GetAsync(
    const std::string& uri, uint64_t start, uint64_t size,
    uint8_t* result_buf) {
  auto uri_res = CleanUri(std::string(uri));
  if (!uri_res) {
    return std::async(
        [=]() -> galois::Result<void> { return uri_res.error(); });
  }
  auto [bucket, object] = std::move(uri_res.value());
  return GSGetAsync(s3_client, bucket, object, start, size, result_buf);
}

std::future<galois::Result<void>>
GSStorage::ListAsync(
    const std::string& directory, std::vector<std::string>* list,
    std::vector<uint64_t>* size) {
  auto uri_res = CleanUri(std::string(directory));
  if (!uri_res) {
    return std::async(
        [=]() -> galois::Result<void> { return uri_res.error(); });
  }
  auto [bucket, object] = std::move(uri_res.value());
  // TODO (witchel) gs requires prefix to end in /.
  if (!object.empty()) {
    if (object[object.size() - 1] != '/') {
      object += '/';
    }
  }
  return GSListAsync(s3_client, bucket, object, list, size);
}

galois::Result<void>
GSStorage::Delete(
    const std::string& directory,
    const std::unordered_set<std::string>& files) {
  auto uri_res = CleanUri(std::string(directory));
  if (!uri_res) {
    return uri_res.error();
  }
  auto [bucket, object] = std::move(uri_res.value());
  return GSDelete(s3_client, bucket, object, files);
}

}  // namespace tsuba
