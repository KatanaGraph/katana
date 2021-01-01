#ifndef KATANA_LIBTSUBA_ADDTABLES_H_
#define KATANA_LIBTSUBA_ADDTABLES_H_

#include <arrow/api.h>

#include "RDGPartHeader.h"
#include "katana/Result.h"
#include "katana/Uri.h"

namespace tsuba {

KATANA_EXPORT katana::Result<std::shared_ptr<arrow::Table>> LoadTable(
    const std::string& expected_name, const katana::Uri& file_path);

KATANA_EXPORT katana::Result<std::shared_ptr<arrow::Table>> LoadTableSlice(
    const std::string& expected_name, const katana::Uri& file_path,
    int64_t offset, int64_t length);

template <typename AddFn>
katana::Result<void>
AddTables(
    const katana::Uri& uri,
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

  return katana::ResultSuccess();
}

template <typename AddFn>
katana::Result<void>
AddTablesSlice(
    const katana::Uri& dir,
    const std::vector<tsuba::PropStorageInfo>& properties,
    std::pair<uint64_t, uint64_t> range, AddFn add_fn) {
  for (const tsuba::PropStorageInfo& properties : properties) {
    katana::Uri p_path = dir.Join(properties.path);

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

  return katana::ResultSuccess();
}

}  // namespace tsuba

#endif
