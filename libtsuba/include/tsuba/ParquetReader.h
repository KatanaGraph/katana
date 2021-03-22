#ifndef KATANA_LIBTSUBA_TSUBA_PARQUETREADER_H_
#define KATANA_LIBTSUBA_TSUBA_PARQUETREADER_H_

#include <optional>

#include <arrow/api.h>

#include "katana/Result.h"
#include "katana/Uri.h"

namespace tsuba {

class ParquetReader {
public:
  struct Slice {
    int64_t offset;
    int64_t length;
  };

  /// \returns a Reader that will read a table from storage location optionally
  /// reading only part of the table defined by \param slice
  static katana::Result<std::unique_ptr<ParquetReader>> Make(
      std::optional<Slice> slice = std::nullopt);

  /// read table from \param uri
  katana::Result<std::shared_ptr<arrow::Table>> ReadFromUri(
      const katana::Uri& uri);

private:
  ParquetReader(std::optional<Slice> slice) : slice_(slice) {}

  katana::Result<std::shared_ptr<arrow::Table>> ReadFromUriSliced(
      const katana::Uri& uri);

  std::optional<Slice> slice_;
};

}  // namespace tsuba

#endif
