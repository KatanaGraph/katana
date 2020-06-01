#ifndef GALOIS_LIBTSUBA_S3_H_
#define GALOIS_LIBTSUBA_S3_H_

#include <string>
#include <cstdint>

namespace tsuba {

int S3Init();
void S3Fini();
int S3Open(const std::string& bucket, const std::string& object);
uint64_t S3GetSize(const std::string& bucket, const std::string& object,
                   uint64_t* size);

std::pair<std::string, std::string> S3SplitUri(const std::string& uri);

int S3DownloadRange(const std::string& bucket, const std::string& object,
                    uint64_t start, uint64_t size, uint8_t* result_buf);

int S3UploadOverwrite(const std::string& bucket, const std::string& object,
                      const uint8_t* data, uint64_t size);

int S3UploadOverwriteSync(const std::string& bucket, const std::string& object,
                          const uint8_t* data, uint64_t size);
int S3UploadOverwriteAsync(const std::string& bucket, const std::string& object,
                           const uint8_t* data, uint64_t size);
int S3UploadOverwriteAsyncFinish(const std::string& bucket,
                                 const std::string& object);
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
