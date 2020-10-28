#ifndef GALOIS_LIBTSUBA_S3_H_
#define GALOIS_LIBTSUBA_S3_H_

#include <cstdint>
#include <future>
#include <string>
#include <string_view>
#include <unordered_set>

#include <aws/core/utils/memory/stl/AWSString.h>

#include "galois/Result.h"
#include "tsuba/s3_internal.h"

namespace tsuba {

// NB: S3Init and S3Fini are in s3_internal.h

galois::Result<void> S3GetSize(
    tsuba::internal::S3Client s3_client, const std::string& bucket,
    const std::string& object, uint64_t* size);

galois::Result<void> S3DownloadRange(
    tsuba::internal::S3Client s3_client, const std::string& bucket,
    const std::string& object, uint64_t start, uint64_t size,
    uint8_t* result_buf);

galois::Result<void> S3UploadOverwrite(
    tsuba::internal::S3Client s3_client, const std::string& bucket,
    const std::string& object, const uint8_t* data, uint64_t size);

std::future<galois::Result<void>> S3GetAsync(
    tsuba::internal::S3Client s3_client, const std::string& bucket,
    const std::string& object, uint64_t start, uint64_t size,
    uint8_t* result_buf);

// Call this function to do an async multipart put
// All but the first call can block, making this a bulk synchronous parallel
// interface
std::future<galois::Result<void>> S3PutAsync(
    tsuba::internal::S3Client s3_client, const std::string& bucket,
    const std::string& object, const uint8_t* data, uint64_t size);

// Listing relative to the full path of the provided directory
std::future<galois::Result<void>> S3ListAsync(
    tsuba::internal::S3Client s3_client, const std::string& bucket,
    const std::string& object, std::vector<std::string>* list,
    std::vector<uint64_t>* size);

galois::Result<void> S3Delete(
    tsuba::internal::S3Client s3_client, const std::string& bucket,
    const std::string& object, const std::unordered_set<std::string>& files);

/* Utility functions for converting between Aws::String and std::string */
inline std::string_view
FromAwsString(const Aws::String& s) {
  return {s.data(), s.size()};
}
inline Aws::String
ToAwsString(std::string_view s) {
  return Aws::String(s.data(), s.size());
}

} /* namespace tsuba */

#endif

// https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html
//  Bucket names must be between 3 and 63 characters long.
//  Bucket names can consist only of lowercase letters, numbers,
//    dots (.), and hyphens (-).
//  Bucket names must begin and end with a letter or number.
//  Bucket names must not be formatted as an IP address
//    (for example, 192.168.5.4).
//  Bucket names can't begin with xn-- (for buckets created after
//    February 2020).
//  Bucket names must be unique within a partition. A partition is a
//    grouping of Regions. AWS currently has three partitions: aws
//    (Standard Regions), aws-cn (China Regions), and aws-us-gov
//    (AWS GovCloud [US] Regions).
//  Buckets used with Amazon S3 Transfer Acceleration can't have dots
//    (.) in their names.
