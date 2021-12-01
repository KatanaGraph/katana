#include "AddProperties.h"

#include <memory>
#include <optional>

#include <arrow/chunked_array.h>
#include <arrow/type_fwd.h>

#include "katana/ArrowInterchange.h"
#include "katana/Logging.h"
#include "katana/ProgressTracer.h"
#include "katana/Result.h"
#include "katana/Time.h"
#include "tsuba/Errors.h"
#include "tsuba/FileView.h"
#include "tsuba/ParquetReader.h"
#include "tsuba/PropertyCache.h"

namespace {

katana::Result<std::shared_ptr<arrow::Table>>
DoLoadProperties(
    const std::string& expected_name, const katana::Uri& file_path,
    std::optional<tsuba::ParquetReader::Slice> slice = std::nullopt) {
  std::unique_ptr<tsuba::ParquetReader> reader =
      KATANA_CHECKED(tsuba::ParquetReader::Make());

  std::shared_ptr<arrow::Table> out =
      KATANA_CHECKED(reader->ReadTable(file_path, slice));

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
    const katana::Uri& uri, tsuba::NodeEdge node_edge,
    tsuba::PropertyCache* cache, tsuba::RDG* rdg,
    const std::vector<tsuba::PropStorageInfo*>& properties, ReadGroup* grp,
    const std::function<katana::Result<void>(std::shared_ptr<arrow::Table>)>&
        add_fn) {
  for (tsuba::PropStorageInfo* prop : properties) {
    if (!prop->IsAbsent()) {
      return KATANA_ERROR(
          ErrorCode::Exists, "property {} must be absent to be added",
          std::quoted(prop->name()));
    }
    if (cache != nullptr) {
      tsuba::PropertyCacheKey cache_key(
          node_edge, rdg->rdg_dir().string(), prop->name());
      auto column_table = cache->Get(cache_key);
      if (column_table) {
        auto props = column_table.value();
        KATANA_CHECKED_CONTEXT(
            add_fn(props), "adding {}", std::quoted(prop->name()));
        prop->WasLoaded(props->field(0)->type());
        auto& tracer = katana::GetTracer();
        tracer.GetActiveSpan().Log(
            "property loaded from cache",
            {{"type", cache_key.TypeAsConstChar()},
             {"name", prop->name()},
             {"approx_size", katana::ApproxTableMemUse(props)},
             {"approx_size_human",
              katana::BytesToStr(
                  "{:.2f}{}", katana::ApproxTableMemUse(props))}});
        return katana::ResultSuccess();
      }
    }
    const katana::Uri& path = uri.Join(prop->path());

    std::future<katana::CopyableResult<std::shared_ptr<arrow::Table>>> future =
        std::async(
            std::launch::async,
            [prop,
             path]() -> katana::CopyableResult<std::shared_ptr<arrow::Table>> {
              return KATANA_CHECKED_CONTEXT(
                  LoadProperties(prop->name(), path), "error loading {}", path);
            });
    auto on_complete = [add_fn, prop, node_edge, cache,
                        rdg](const std::shared_ptr<arrow::Table>& props)
        -> katana::CopyableResult<void> {
      if (cache != nullptr) {
        auto& tracer = katana::GetTracer();
        // Do not put uint8 types in property cache.  Users cannot create uint8
        // properties via Cypher, but they can create them via parquet import.
        if (props->column(0)->type()->Equals(arrow::uint8())) {
          KATANA_WARN_ONCE(
              "deprecated graph format; type is uint8: {}",
              props->field(0)->name());
          KATANA_LOG_VASSERT(
              !rdg->IsEntityTypeIDsOutsideProperties(),
              "storage_format_version >= 2 RDG may not have uint8 type "
              "properties");
        } else {
          tracer.GetActiveSpan().Log(
              "property inserted into cache",
              {{"type",
                (node_edge == tsuba::NodeEdge::kNode) ? "node" : "edge"},
               {"name", prop->name()},
               {"approx_size", katana::ApproxTableMemUse(props)},
               {"approx_size_human",
                katana::BytesToStr(
                    "{:.2f}{}", katana::ApproxTableMemUse(props))}});
          // Only match properties from the same RDG prefix
          tsuba::PropertyCacheKey cache_key(
              node_edge, rdg->rdg_dir().string(), prop->name());
          cache->Insert(cache_key, props, rdg);
        }
      }
      KATANA_CHECKED_CONTEXT(
          add_fn(props), "adding {}", std::quoted(prop->name()));
      prop->WasLoaded(props->field(0)->type());
      if (prop->type()->Equals(arrow::uint8())) {
        KATANA_LOG_VASSERT(
            !rdg->IsEntityTypeIDsOutsideProperties(),
            "storage_format_version >= 2 RDG may not have uint8 type "
            "properties");
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
tsuba::AddPropertySlice(
    const katana::Uri& dir,
    const std::vector<tsuba::PropStorageInfo*>& properties,
    std::pair<uint64_t, uint64_t> range, ReadGroup* grp,
    const std::function<katana::Result<void>(std::shared_ptr<arrow::Table>)>&
        add_fn) {
  uint64_t begin = range.first;
  uint64_t size = range.second > range.first ? range.second - range.first : 0;
  for (tsuba::PropStorageInfo* prop : properties) {
    if (!prop->IsAbsent()) {
      return KATANA_ERROR(
          ErrorCode::Exists, "property {} must be absent to be added",
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
      prop->WasLoaded(props->field(0)->type());

      // since this property is going to be sliced it has no on disk form, so
      // immediately mark it dirty
      prop->WasModified(props->field(0)->type());

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
