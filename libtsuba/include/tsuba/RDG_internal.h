#ifndef GALOIS_LIBTSUBA_TSUBA_RDGINTERNAL_H_
#define GALOIS_LIBTSUBA_TSUBA_RDGINTERNAL_H_

#include <cstdint>
#include <string>

#include <arrow/api.h>

#include "galois/Result.h"
#include "galois/Uri.h"
#include "galois/config.h"

namespace tsuba::internal {

// Helper function for loading Arrow Tables from Parquet files,
// exported here only for testing
GALOIS_EXPORT galois::Result<std::shared_ptr<arrow::Table>> LoadPartialTable(
    const std::string& expected_name, const galois::Uri& file_path,
    int64_t offset, int64_t length);

// Used for garbage collection
// Return all file names that store data for this handle
GALOIS_EXPORT galois::Result<std::unordered_set<std::string>> FileNames(
    const std::string& dir, uint64_t version);
GALOIS_EXPORT galois::Result<uint64_t> ParseVersion(const std::string& file);

}  // namespace tsuba::internal

#endif
