#ifndef KATANA_LIBTSUBA_ADDPROPERTIES_H_
#define KATANA_LIBTSUBA_ADDPROPERTIES_H_

#include <arrow/api.h>

#include "RDGPartHeader.h"
#include "katana/ReadGroup.h"
#include "katana/Result.h"
#include "katana/URI.h"

namespace katana {

KATANA_EXPORT katana::Result<std::shared_ptr<arrow::Table>> LoadProperties(
    const std::string& expected_name, const katana::Uri& file_path);

KATANA_EXPORT katana::Result<std::shared_ptr<arrow::Table>> LoadPropertySlice(
    const std::string& expected_name, const katana::Uri& file_path,
    int64_t offset, int64_t length);

KATANA_EXPORT katana::Result<void> AddProperties(
    const katana::Uri& uri, katana::PropertyCache* cache, katana::RDG* rdg,
    const std::vector<katana::PropStorageInfo*>& properties, ReadGroup* grp,
    const std::function<katana::Result<void>(std::shared_ptr<arrow::Table>)>&
        add_fn);

KATANA_EXPORT katana::Result<void> AddPropertySlice(
    const katana::Uri& dir,
    const std::vector<katana::PropStorageInfo*>& properties,
    std::pair<uint64_t, uint64_t> range, ReadGroup* grp,
    const std::function<katana::Result<void>(std::shared_ptr<arrow::Table>)>&
        add_fn);

}  // namespace katana

#endif
