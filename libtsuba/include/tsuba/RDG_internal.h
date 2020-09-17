#ifndef GALOIS_LIBTSUBA_TSUBA_RDGINTERNAL_H_
#define GALOIS_LIBTSUBA_TSUBA_RDGINTERNAL_H_

#include <cstdint>
#include <string>

#include <arrow/api.h>

#include "galois/Result.h"
#include "galois/config.h"

namespace tsuba::internal {

// Helper function for loading Arrow Tables from Parquet files,
// exported here only for testing
GALOIS_EXPORT galois::Result<std::shared_ptr<arrow::Table>> LoadPartialTable(
    const std::string& expected_name, const std::string& file_path,
    int64_t offset, int64_t length);

}  // namespace tsuba::internal

#endif
