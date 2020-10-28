#include "gs.h"

#include "galois/GetEnv.h"
#include "galois/Logging.h"
#include "galois/Random.h"
#include "galois/Result.h"
#include "galois/Uri.h"
#include "s3.h"
#include "tsuba/Errors.h"
#include "tsuba/s3_internal.h"

galois::Result<tsuba::internal::S3Client>
tsuba::GSInit() {
  return tsuba::internal::S3Init("https://storage.googleapis.com");
}

galois::Result<void>
tsuba::GSFini(tsuba::internal::S3Client s3_client) {
  return tsuba::internal::S3Fini(s3_client);
}

galois::Result<void>
tsuba::GSGetSize(
    tsuba::internal::S3Client s3_client, const std::string& bucket,
    const std::string& object, uint64_t* size) {
  return S3GetSize(s3_client, bucket, object, size);
}

galois::Result<void>
tsuba::GSGetSync(
    tsuba::internal::S3Client s3_client, const std::string& bucket,
    const std::string& object, uint64_t start, uint64_t size,
    uint8_t* result_buf) {
  // Multi-part
  return tsuba::S3DownloadRange(
      s3_client, bucket, object, start, size, result_buf);
}

galois::Result<void>
tsuba::GSPutSync(
    tsuba::internal::S3Client s3_client, const std::string& bucket,
    const std::string& object, const uint8_t* data, uint64_t size) {
  return tsuba::internal::S3PutSingleSync(
      s3_client, bucket, object, data, size);
}

std::future<galois::Result<void>>
tsuba::GSGetAsync(
    tsuba::internal::S3Client s3_client, const std::string& bucket,
    const std::string& object, uint64_t start, uint64_t size,
    uint8_t* result_buf) {
  return tsuba::S3GetAsync(s3_client, bucket, object, start, size, result_buf);
}

std::future<galois::Result<void>>
tsuba::GSPutAsync(
    tsuba::internal::S3Client s3_client, const std::string& bucket,
    const std::string& object, const uint8_t* data, uint64_t size) {
  auto future = std::async([=]() -> galois::Result<void> {
    tsuba::internal::CountingSemaphore sema;
    if (auto res = tsuba::internal::S3PutSingleAsync(
            s3_client, bucket, object, data, size, &sema);
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
    tsuba::internal::S3Client s3_client, const std::string& bucket,
    const std::string& object, std::vector<std::string>* list,
    std::vector<uint64_t>* size) {
  return tsuba::internal::S3ListAsyncV1(s3_client, bucket, object, list, size);
}

galois::Result<void>
tsuba::GSDelete(
    tsuba::internal::S3Client s3_client, const std::string& bucket,
    const std::string& object, const std::unordered_set<std::string>& files) {
  galois::Result<void> ret = galois::ResultSuccess();
  for (const auto& file : files) {
    auto res = tsuba::internal::S3SingleDelete(
        s3_client, bucket, galois::Uri::JoinPath(object, file));
    if (!res && ret == galois::ResultSuccess()) {
      ret = res.error();
    }
  }
  return ret;
}
