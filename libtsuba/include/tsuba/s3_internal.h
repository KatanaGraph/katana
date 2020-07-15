#ifndef GALOIS_LIBTSUBA_S3_INTERNAL_H_
#define GALOIS_LIBTSUBA_S3_INTERNAL_H_

#include "galois/Result.h"
#include <string>

// Don't call these directly.  They are intended for use only in s3.cpp and
// testing code

namespace tsuba::internal {

// Private functions
galois::Result<void> S3PutSingleSync(const std::string& bucket,
                                     const std::string& object,
                                     const uint8_t* data, uint64_t size);
galois::Result<void> S3PutMultiAsync1(const std::string& bucket,
                                      const std::string& object,
                                      const uint8_t* data, uint64_t size);
galois::Result<void> S3PutMultiAsync2(const std::string& bucket,
                                      const std::string& object);
galois::Result<void> S3PutMultiAsync3(const std::string& bucket,
                                      const std::string& object);
galois::Result<void> S3PutMultiAsyncFinish(const std::string& bucket,
                                           const std::string& object);
galois::Result<void> S3PutSingleAsync(const std::string& bucket,
                                      const std::string& object,
                                      const uint8_t* data, uint64_t size);
galois::Result<void> S3PutSingleAsyncFinish(const std::string& bucket,
                                            const std::string& object);
} // namespace tsuba::internal

#endif // GALOIS_LIBTSUBA_S3_INTERNAL_H_
