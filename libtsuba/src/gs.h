#ifndef GALOIS_LIBTSUBA_GS_H_
#define GALOIS_LIBTSUBA_GS_H_

#include <cstdint>
#include <future>
#include <string>
#include <unordered_set>

#include "galois/Result.h"
#include "tsuba/s3_internal.h"

namespace tsuba {

galois::Result<internal::S3Client> GSInit();
galois::Result<void> GSFini(internal::S3Client s3_client);
galois::Result<void> GSGetSize(
    internal::S3Client s3_client, const std::string& bucket,
    const std::string& object, uint64_t* size);

galois::Result<void> GSGetSync(
    internal::S3Client s3_client, const std::string& bucket,
    const std::string& object, uint64_t start, uint64_t size,
    uint8_t* result_buf);

galois::Result<void> GSPutSync(
    internal::S3Client s3_client, const std::string& bucket,
    const std::string& object, const uint8_t* data, uint64_t size);

std::future<galois::Result<void>> GSGetAsync(
    internal::S3Client s3_client, const std::string& bucket,
    const std::string& object, uint64_t start, uint64_t size,
    uint8_t* result_buf);

std::future<galois::Result<void>> GSPutAsync(
    internal::S3Client s3_client, const std::string& bucket,
    const std::string& object, const uint8_t* data, uint64_t size);

std::future<galois::Result<void>> GSListAsync(
    internal::S3Client s3_client, const std::string& bucket,
    const std::string& object, std::vector<std::string>* list,
    std::vector<uint64_t>* size);

galois::Result<void> GSDelete(
    internal::S3Client s3_client, const std::string& bucket,
    const std::string& object, const std::unordered_set<std::string>& files);

} /* namespace tsuba */

#endif
