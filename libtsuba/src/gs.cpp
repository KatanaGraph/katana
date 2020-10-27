#include "gs.h"

#include "galois/GetEnv.h"
#include "galois/Logging.h"
#include "galois/Random.h"
#include "galois/Result.h"
#include "galois/Uri.h"
#include "s3.h"
#include "tsuba/Errors.h"
#include "tsuba/s3_internal.h"

galois::Result<void>
tsuba::GSInit() {
  galois::SetEnv(
      "GALOIS_AWS_TEST_ENDPOINT", "https://storage.googleapis.com",
      /*overwrite*/ true);
  return S3Init();
}

galois::Result<void>
tsuba::GSFini() {
  return S3Fini();
}

galois::Result<void>
tsuba::GSGetSize(
    const std::string& bucket, const std::string& object, uint64_t* size) {
  return S3GetSize(bucket, object, size);
}

galois::Result<bool>
tsuba::GSExists(const std::string& bucket, const std::string& object) {
  return S3Exists(bucket, object);
}

galois::Result<void>
tsuba::GSGetSync(
    const std::string& bucket, const std::string& object, uint64_t start,
    uint64_t size, uint8_t* result_buf) {
  // Multi-part
  return tsuba::S3DownloadRange(bucket, object, start, size, result_buf);
}

galois::Result<void>
tsuba::GSPutSync(
    const std::string& bucket, const std::string& object, const uint8_t* data,
    uint64_t size) {
  return tsuba::internal::S3PutSingleSync(bucket, object, data, size);
}

std::future<galois::Result<void>>
tsuba::GSGetAsync(
    const std::string& bucket, const std::string& object, uint64_t start,
    uint64_t size, uint8_t* result_buf) {
  return tsuba::S3GetAsync(bucket, object, start, size, result_buf);
}

std::future<galois::Result<void>>
tsuba::GSPutAsync(
    const std::string& bucket, const std::string& object, const uint8_t* data,
    uint64_t size) {
  auto future = std::async([=]() -> galois::Result<void> {
    tsuba::internal::CountingSemaphore sema;
    if (auto res = tsuba::internal::S3PutSingleAsync(
            bucket, object, data, size, &sema);
        !res) {
      GALOIS_LOG_ERROR("GSPutSingleAsync return {}", res.error());
    }
    // Only 1 outstanding store at a time
    tsuba::internal::S3PutSingleAsyncFinish(&sema);
    return galois::ResultSuccess();
  });
  return future;
}

std::future<galois::Result<void>>
tsuba::GSListAsync(
    const std::string& bucket, const std::string& object,
    std::vector<std::string>* list, std::vector<uint64_t>* size) {
  return tsuba::internal::S3ListAsyncV1(bucket, object, list, size);
}

galois::Result<void>
tsuba::GSDelete(
    const std::string& bucket, const std::string& object,
    const std::unordered_set<std::string>& files) {
  galois::Result<void> ret = galois::ResultSuccess();
  for (const auto& file : files) {
    auto res = tsuba::internal::S3SingleDelete(
        bucket, galois::Uri::JoinPath(object, file));
    if (!res && ret == galois::ResultSuccess()) {
      ret = res.error();
    }
  }
  return ret;
}
