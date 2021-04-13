#ifndef KATANA_LIBTSUBA_TSUBA_PARQUETWRITER_H_
#define KATANA_LIBTSUBA_TSUBA_PARQUETWRITER_H_

#include <arrow/api.h>

#include "katana/Result.h"
#include "katana/Uri.h"
#include "tsuba/WriteGroup.h"

namespace tsuba {

class KATANA_EXPORT ParquetWriter {
public:
  /// \returns a Writer that will write a table consisting of a single column
  /// \param array named \param name to a storage location
  static katana::Result<std::unique_ptr<ParquetWriter>> Make(
      std::shared_ptr<arrow::ChunkedArray> array, const std::string& name);

  /// \returns a Writer that will write \param table to a storage location
  static katana::Result<std::unique_ptr<ParquetWriter>> Make(
      std::shared_ptr<arrow::Table> table);

  /// write table out to a storage location \param uri If \param group is null,
  /// the write is synchronous, if not an asynchronous write is started to be
  /// managed by group
  katana::Result<void> WriteToUri(
      const katana::Uri& uri, WriteGroup* group = nullptr);

private:
  ParquetWriter(std::shared_ptr<arrow::Table> table)
      : table_(std::move(table)) {}

  std::shared_ptr<arrow::Table> table_;
};

}  // namespace tsuba

#endif
