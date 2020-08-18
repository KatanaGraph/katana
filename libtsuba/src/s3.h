#ifndef GALOIS_LIBTSUBA_S3_H_
#define GALOIS_LIBTSUBA_S3_H_

#include <string>
#include <string_view>
#include <cstdint>

#include <aws/core/utils/memory/stl/AWSString.h>

#include "galois/Result.h"
#include "tsuba/FileAsyncWork.h"

namespace tsuba {

galois::Result<void> S3Init();
galois::Result<void> S3Fini();
galois::Result<void> S3GetSize(const std::string& bucket,
                               const std::string& object, uint64_t* size);
galois::Result<bool> S3Exists(const std::string& bucket,
                              const std::string& object);

galois::Result<void> S3DownloadRange(const std::string& bucket,
                                     const std::string& object, uint64_t start,
                                     uint64_t size, uint8_t* result_buf);

galois::Result<void> S3UploadOverwrite(const std::string& bucket,
                                       const std::string& object,
                                       const uint8_t* data, uint64_t size);

galois::Result<std::unique_ptr<FileAsyncWork>>
S3GetAsync(const std::string& bucket, const std::string& object, uint64_t start,
           uint64_t size, uint8_t* result_buf);

// Call this function to do an async multipart put
// All but the first call can block, making this a bulk synchronous parallel
// interface
galois::Result<std::unique_ptr<FileAsyncWork>>
S3PutAsync(const std::string& bucket, const std::string& object,
           const uint8_t* data, uint64_t size);

galois::Result<std::unique_ptr<FileAsyncWork>>
S3ListAsync(const std::string& bucket, const std::string& object,
            std::vector<std::string>& list_out);

/* Utility functions for converting between Aws::String and std::string */
inline std::string_view FromAwsString(const Aws::String& s) {
  return {s.data(), s.size()};
}
inline Aws::String ToAwsString(const std::string& s) {
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
