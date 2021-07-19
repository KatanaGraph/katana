#include "AddProperties.h"

#include <memory>
#include <optional>

#include <arrow/chunked_array.h>

#include "katana/Result.h"
#include "tsuba/Errors.h"
#include "tsuba/FileView.h"
#include "tsuba/ParquetReader.h"

namespace {

katana::Result<std::shared_ptr<arrow::Table>>
DoLoadProperties(
    const std::string& expected_name, const katana::Uri& file_path,
    std::optional<tsuba::ParquetReader::Slice> slice = std::nullopt) {
  auto read_opts = tsuba::ParquetReader::ReadOpts::Defaults();
  read_opts.slice = slice;
  auto reader_res = tsuba::ParquetReader::Make(read_opts);
  if (!reader_res) {
    return reader_res.error().WithContext("loading property");
  }
  std::unique_ptr<tsuba::ParquetReader> reader = std::move(reader_res.value());

  auto out_res = reader->ReadTable(file_path);
  if (!out_res) {
    return out_res.error().WithContext("loading property");
  }

  std::shared_ptr<arrow::Table> out = std::move(out_res.value());

  std::shared_ptr<arrow::Schema> schema = out->schema();
  if (schema->num_fields() != 1) {
    return KATANA_ERROR(
        tsuba::ErrorCode::InvalidArgument, "expected 1 field found {} instead",
        schema->num_fields());
  }

  if (schema->field(0)->name() != expected_name) {
    return KATANA_ERROR(
        tsuba::ErrorCode::InvalidArgument, "expected {} found {} instead",
        expected_name, schema->field(0)->name());
  }
  return out;
}

}  // namespace

katana::Result<std::shared_ptr<arrow::Table>>
tsuba::LoadProperties(
    const std::string& expected_name, const katana::Uri& file_path) {
  try {
    return DoLoadProperties(expected_name, file_path);
  } catch (const std::exception& exp) {
    return KATANA_ERROR(
        tsuba::ErrorCode::ArrowError, "arrow exception: {}", exp.what());
  }
}

katana::Result<std::shared_ptr<arrow::Table>>
tsuba::LoadPropertySlice(
    const std::string& expected_name, const katana::Uri& file_path,
    int64_t offset, int64_t length) {
  try {
    return DoLoadProperties(
        expected_name, file_path,
        tsuba::ParquetReader::Slice{.offset = offset, .length = length});
  } catch (const std::exception& exp) {
    return KATANA_ERROR(
        tsuba::ErrorCode::ArrowError, "arrow exception: {}", exp.what());
  }
}

katana::Result<void>
tsuba::AddProperties(
    const katana::Uri& uri,
    const std::vector<tsuba::PropStorageInfo*>& properties, ReadGroup* grp,
    const std::function<katana::Result<void>(std::shared_ptr<arrow::Table>)>&
        add_fn) {
  for (tsuba::PropStorageInfo* prop : properties) {
    if (!prop->IsAbsent()) {
      return KATANA_ERROR(
          ErrorCode::Exists, "property {} must be absent to be added",
          std::quoted(prop->name()));
    }
    const std::string& name = prop->name();
    const katana::Uri& path = uri.Join(prop->path());

    prop->NoteLoad();

    std::future<katana::CopyableResult<std::shared_ptr<arrow::Table>>> future =
        std::async(
            std::launch::async,
            [name,
             path]() -> katana::CopyableResult<std::shared_ptr<arrow::Table>> {
              auto load_result = LoadProperties(name, path);
              if (!load_result) {
                return load_result.error().WithContext(
                    "error loading {}", path);
              }
              return load_result.value();
            });
    auto on_complete = [add_fn,
                        name](const std::shared_ptr<arrow::Table>& props)
        -> katana::CopyableResult<void> {
      auto add_result = add_fn(props);
      if (!add_result) {
        return add_result.error().WithContext("adding {}", std::quoted(name));
      }
      return katana::CopyableResultSuccess();
    };
    if (grp) {
      grp->AddReturnsOp<std::shared_ptr<arrow::Table>>(
          std::move(future), path.string(), on_complete);
      continue;
    }
    auto read_res = future.get();
    if (!read_res) {
      return read_res.error();
    }
    auto on_complete_res = on_complete(read_res.value());
    if (!on_complete_res) {
      return on_complete_res.error();
    }
  }

  return katana::ResultSuccess();
}

katana::Result<void>
tsuba::AddPropertySlice(
    const katana::Uri& dir,
    const std::vector<tsuba::PropStorageInfo*>& properties,
    std::pair<uint64_t, uint64_t> range, ReadGroup* grp,
    const std::function<katana::Result<void>(std::shared_ptr<arrow::Table>)>&
        add_fn) {
  uint64_t begin = range.first;
  uint64_t size = range.second - range.first;
  for (tsuba::PropStorageInfo* prop : properties) {
    if (!prop->IsAbsent()) {
      return KATANA_ERROR(
          ErrorCode::Exists, "property {} must be absent to be added",
          std::quoted(prop->name()));
    }
    const std::string& name = prop->name();
    const katana::Uri& path = dir.Join(prop->path());

    prop->NoteLoad();

    // since this property is going to be sliced it has no on disk form, so
    // immediately mark it dirty
    prop->NoteModify();

    std::future<katana::CopyableResult<std::shared_ptr<arrow::Table>>> future =
        std::async(
            std::launch::async,
            [name, path, begin,
             size]() -> katana::CopyableResult<std::shared_ptr<arrow::Table>> {
              auto load_result = LoadPropertySlice(name, path, begin, size);
              if (!load_result) {
                return load_result.error().WithContext(
                    "error loading {}", path);
              }
              return load_result.value();
            });
    auto on_complete = [add_fn,
                        name](const std::shared_ptr<arrow::Table>& props)
        -> katana::CopyableResult<void> {
      auto add_result = add_fn(props);
      if (!add_result) {
        return add_result.error().WithContext("adding {}", std::quoted(name));
      }
      return katana::CopyableResultSuccess();
    };
    if (grp) {
      grp->AddReturnsOp<std::shared_ptr<arrow::Table>>(
          std::move(future), path.string(), on_complete);
      continue;
    }
    auto read_res = future.get();
    if (!read_res) {
      return read_res.error();
    }
    auto on_complete_res = on_complete(read_res.value());
    if (!on_complete_res) {
      return on_complete_res.error();
    }
  }

  return katana::ResultSuccess();
}
