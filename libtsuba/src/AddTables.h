#ifndef GALOIS_LIBTSUBA_ADDTABLES_H_
#define GALOIS_LIBTSUBA_ADDTABLES_H_

#include <arrow/api.h>

#include "RDGPartHeader.h"
#include "galois/Result.h"
#include "galois/Uri.h"

namespace tsuba {

GALOIS_EXPORT galois::Result<std::shared_ptr<arrow::Table>> LoadTable(
    const std::string& expected_name, const galois::Uri& file_path);

GALOIS_EXPORT galois::Result<std::shared_ptr<arrow::Table>> LoadTableSlice(
    const std::string& expected_name, const galois::Uri& file_path,
    int64_t offset, int64_t length);

template <typename AddFn>
galois::Result<void>
AddTables(
    const galois::Uri& uri,
    const std::vector<tsuba::PropStorageInfo>& properties, AddFn add_fn) {
  for (const tsuba::PropStorageInfo& properties : properties) {
    auto p_path = uri.Join(properties.path);

    auto load_result = LoadTable(properties.name, p_path);
    if (!load_result) {
      return load_result.error();
    }

    std::shared_ptr<arrow::Table> table = load_result.value();

    auto add_result = add_fn(table);
    if (!add_result) {
      return add_result.error();
    }
  }

  return galois::ResultSuccess();
}

template <typename AddFn>
galois::Result<void>
AddTablesSlice(
    const galois::Uri& dir,
    const std::vector<tsuba::PropStorageInfo>& properties,
    std::pair<uint64_t, uint64_t> range, AddFn add_fn) {
  for (const tsuba::PropStorageInfo& properties : properties) {
    galois::Uri p_path = dir.Join(properties.path);

    auto load_result = LoadTableSlice(
        properties.name, p_path, range.first, range.second - range.first);
    if (!load_result) {
      return load_result.error();
    }

    std::shared_ptr<arrow::Table> table = load_result.value();

    auto add_result = add_fn(table);
    if (!add_result) {
      return add_result.error();
    }
  }

  return galois::ResultSuccess();
}

}  // namespace tsuba

#endif
