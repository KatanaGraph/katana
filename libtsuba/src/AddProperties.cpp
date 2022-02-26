#include "AddProperties.h"

#include <memory>
#include <optional>

#include <arrow/chunked_array.h>
#include <arrow/type_fwd.h>

#include "katana/ArrowInterchange.h"
#include "katana/ErrorCode.h"
#include "katana/FileView.h"
#include "katana/Logging.h"
#include "katana/MemorySupervisor.h"
#include "katana/ParquetReader.h"
#include "katana/ProgressTracer.h"
#include "katana/PropertyManager.h"
#include "katana/Result.h"
#include "katana/Time.h"

namespace {

katana::Result<std::shared_ptr<arrow::Table>>
DoLoadProperties(
    const std::string& expected_name, const katana::Uri& file_path,
    std::optional<katana::ParquetReader::Slice> slice = std::nullopt) {
  std::unique_ptr<katana::ParquetReader> reader =
      KATANA_CHECKED(katana::ParquetReader::Make());

  std::shared_ptr<arrow::Table> out =
      KATANA_CHECKED(reader->ReadTable(file_path, slice));

  std::shared_ptr<arrow::Schema> schema = out->schema();
  if (schema->num_fields() != 1) {
    return KATANA_ERROR(
        katana::ErrorCode::InvalidArgument, "expected 1 field found {} instead",
        schema->num_fields());
  }

  if (schema->field(0)->name() != expected_name) {
    return KATANA_ERROR(
        katana::ErrorCode::InvalidArgument, "expected {} found {} instead",
        expected_name, schema->field(0)->name());
  }
  return out;
}

}  // namespace

katana::Result<std::shared_ptr<arrow::Table>>
katana::LoadProperties(
    const std::string& expected_name, const katana::Uri& file_path) {
  try {
    return DoLoadProperties(expected_name, file_path);
  } catch (const std::exception& exp) {
    return KATANA_ERROR(
        katana::ErrorCode::ArrowError, "arrow exception: {}", exp.what());
  }
}

katana::Result<std::shared_ptr<arrow::Table>>
katana::LoadPropertySlice(
    const std::string& expected_name, const katana::Uri& file_path,
    int64_t offset, int64_t length) {
  try {
    return DoLoadProperties(
        expected_name, file_path,
        katana::ParquetReader::Slice{.offset = offset, .length = length});
  } catch (const std::exception& exp) {
    return KATANA_ERROR(
        katana::ErrorCode::ArrowError, "arrow exception: {}", exp.what());
  }
}

katana::Result<void>
katana::AddProperties(
    const katana::Uri& uri, bool is_property,
    const std::vector<katana::PropStorageInfo*>& properties, ReadGroup* grp,
    const std::function<katana::Result<void>(std::shared_ptr<arrow::Table>)>&
        add_fn) {
  for (katana::PropStorageInfo* prop : properties) {
    if (!prop->IsAbsent()) {
      return KATANA_ERROR(
          ErrorCode::AlreadyExists, "property {} must be absent to be added",
          std::quoted(prop->name()));
    }
    if (is_property) {
      PropertyManager* pm =
          katana::MemorySupervisor::Get().GetPropertyManager();
      KATANA_LOG_DEBUG_ASSERT(pm);
      KATANA_LOG_DEBUG_ASSERT(!uri.empty());
      const katana::Uri& cache_key = uri.Join(prop->path());
      std::shared_ptr<arrow::Table> props = pm->GetProperty(cache_key);
      if (props) {
        KATANA_CHECKED_CONTEXT(
            add_fn(props), "adding {}", std::quoted(prop->name()));
        prop->WasLoaded(props->field(0)->type());
        auto cache_stats = pm->GetPropertyCacheStats();
        katana::GetTracer().GetActiveSpan().Log(
            "addproperties property cache hit",
            {
                {"name", prop->name()},
                {"path", cache_key.string()},
                {"counts", fmt::format(
                               "get {} insert {}", cache_stats.get_count,
                               cache_stats.insert_count)},
                {"hit_rate", fmt::format(
                                 "total: {:.1f}% get: {:.1f}% insert: {:.1f}%",
                                 cache_stats.total_hit_percentage(),
                                 cache_stats.get_hit_percentage(),
                                 cache_stats.insert_hit_percentage())},
            });
        return katana::ResultSuccess();
      }
    }
    katana::GetTracer().GetActiveSpan().Log(
        "addproperties property cache miss", {
                                                 {"name", prop->name()},
                                             });
    const katana::Uri& path = uri.Join(prop->path());

    std::future<katana::CopyableResult<std::shared_ptr<arrow::Table>>> future =
        std::async(
            std::launch::async,
            [prop,
             path]() -> katana::CopyableResult<std::shared_ptr<arrow::Table>> {
              return KATANA_CHECKED_CONTEXT(
                  LoadProperties(prop->name(), path), "error loading {}", path);
            });
    auto on_complete = [add_fn, is_property,
                        prop](const std::shared_ptr<arrow::Table>& props)
        -> katana::CopyableResult<void> {
      KATANA_CHECKED_CONTEXT(
          add_fn(props), "adding {}", std::quoted(prop->name()));
      prop->WasLoaded(props->field(0)->type());
      PropertyManager* pm =
          katana::MemorySupervisor::Get().GetPropertyManager();
      if (is_property) {
        pm->PropertyLoadedActive(props);
      } else {
        katana::GetTracer().GetActiveSpan().Log(
            "addproperties property cache callback non-property",
            {
                {"name", prop->name()},
                {"file_name", prop->path()},
            });
      }
      return katana::CopyableResultSuccess();
    };
    if (grp) {
      grp->AddReturnsOp<std::shared_ptr<arrow::Table>>(
          std::move(future), path.string(), on_complete);
      continue;
    }
    auto read_res = KATANA_CHECKED(future.get());

    KATANA_CHECKED(on_complete(read_res));
  }

  return katana::ResultSuccess();
}

katana::Result<void>
katana::AddPropertySlice(
    const katana::Uri& dir,
    const std::vector<katana::PropStorageInfo*>& properties,
    std::pair<uint64_t, uint64_t> range, ReadGroup* grp,
    const std::function<katana::Result<void>(std::shared_ptr<arrow::Table>)>&
        add_fn) {
  uint64_t begin = range.first;
  uint64_t size = range.second > range.first ? range.second - range.first : 0;
  for (katana::PropStorageInfo* prop : properties) {
    if (!prop->IsAbsent()) {
      return KATANA_ERROR(
          ErrorCode::AlreadyExists, "property {} must be absent to be added",
          std::quoted(prop->name()));
    }
    const katana::Uri& path = dir.Join(prop->path());

    std::future<katana::CopyableResult<std::shared_ptr<arrow::Table>>> future =
        std::async(
            std::launch::async,
            [path, prop, begin,
             size]() -> katana::CopyableResult<std::shared_ptr<arrow::Table>> {
              std::shared_ptr<arrow::Table> load_result =
                  KATANA_CHECKED_CONTEXT(
                      LoadPropertySlice(prop->name(), path, begin, size),
                      "error loading {}", path);
              return load_result;
            });
    auto on_complete = [add_fn,
                        prop](const std::shared_ptr<arrow::Table>& props)
        -> katana::CopyableResult<void> {
      KATANA_CHECKED_CONTEXT(
          add_fn(props), "adding: {}", std::quoted(prop->name()));
      // NB: Sliced properties don't fit super cleanly into the PropStorageInfo
      // model. This property is dirty in the sense that there is no file on
      // storage that exactly matches it but it is clean in the sense that it
      // has not been modified. Leave it as clean to simplify loading/unloading
      // logic in RDGSlice.
      prop->WasLoaded(props->field(0)->type());

      return katana::CopyableResultSuccess();
    };
    if (grp) {
      grp->AddReturnsOp<std::shared_ptr<arrow::Table>>(
          std::move(future), path.string(), on_complete);
      continue;
    }
    std::shared_ptr<arrow::Table> props = KATANA_CHECKED(future.get());
    KATANA_CHECKED(on_complete(props));
  }

  return katana::ResultSuccess();
}
