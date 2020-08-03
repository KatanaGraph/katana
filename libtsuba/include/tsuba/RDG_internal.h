#ifndef GALOIS_LIBTSUBA_TSUBA_RDGINTERNAL_H_
#define GALOIS_LIBTSUBA_TSUBA_RDGINTERNAL_H_

#include <string>
#include <cstdint>

#include <arrow/api.h>

#include "galois/Result.h"

namespace tsuba::internal {

// Helper function for loading Arrow Tables from Parquet files,
// exported here only for testing
galois::Result<std::shared_ptr<arrow::Table>>
LoadPartialTable(const std::string& expected_name, const std::string& file_path,
                 int64_t offset, int64_t length);

} // namespace tsuba::internal

#endif
