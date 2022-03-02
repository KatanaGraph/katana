#include "katana/StorageBackedArrowTable.h"

#include <cstring>
#include <functional>
#include <future>

#include <boost/iterator/transform_iterator.hpp>

#include "katana/FileView.h"
#include "katana/StorageHelpers.h"
#include "storage_operations_generated.h"

template <typename T>
using Result = katana::Result<T>;

template <typename T>
using CopyableResult = katana::CopyableResult<T>;

Result<std::shared_ptr<katana::StorageBackedArrowTable>>
katana::StorageBackedArrowTable::Make(
    const URI& storage_location, int64_t rows) {
  if (rows < 0) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "number of rows must be non-negative");
  }
  auto new_table = MakeShared(storage_location, rows);
  KATANA_CHECKED(new_table->ResetSchema());
  return new_table;
}

Result<std::shared_ptr<katana::StorageBackedArrowTable>>
katana::StorageBackedArrowTable::Make(
    const URI& storage_location, const std::vector<std::string>& names,
    const std::vector<std::shared_ptr<StorageBackedArrowArray>>& cols) {
  if (names.size() != cols.size()) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument,
        "must provide the same number of names a columns");
  }

  if (cols.empty()) {
    return Make(storage_location, 0);
  }

  auto new_table = MakeShared(storage_location, cols.front()->length());
  for (size_t i = 0; i < cols.size(); ++i) {
    if (cols[i]->length() != new_table->num_rows()) {
      return KATANA_ERROR(
          ErrorCode::InvalidArgument, "columns must have the same length");
    }
    auto [it, was_emplaced] = new_table->columns_.emplace(names[i], cols[i]);
    if (!was_emplaced) {
      return KATANA_ERROR(
          ErrorCode::InvalidArgument,
          "column names must be unique (found multiple named {})",
          std::quoted(names[i]));
    }
  }

  KATANA_CHECKED(new_table->ResetSchema());
  return new_table;
}

std::future<CopyableResult<std::shared_ptr<katana::StorageBackedArrowTable>>>
katana::StorageBackedArrowTable::FromStorageAsync(const URI& uri) {
  return std::async(
      std::launch::async,
      [=]() -> CopyableResult<std::shared_ptr<StorageBackedArrowTable>> {
        FileView fv;
        KATANA_CHECKED(fv.Bind(uri.string(), /*resolve=*/true));

        auto storage_location = uri.DirName();

        flatbuffers::Verifier verifier(fv.ptr<uint8_t>(), fv.size());
        if (!verifier.VerifyBuffer<fbs::StorageBackedArrowTable>()) {
          return KATANA_ERROR(
              ErrorCode::InvalidArgument,
              "file does not appear to contain an array (failed validation)");
        }

        auto fb_sbat = std::make_unique<fbs::StorageBackedArrowTableT>();
        flatbuffers::GetRoot<fbs::StorageBackedArrowTable>(fv.ptr<uint8_t>())
            ->UnPackTo(fb_sbat.get());

        ReadGroup rg;
        auto new_table = MakeShared(storage_location, fb_sbat->num_rows);
        for (const auto& col : fb_sbat->columns) {
          auto col_uri = KATANA_CHECKED(UriFromFB(storage_location, *col->uri));

          std::function<CopyableResult<void>(
              std::shared_ptr<StorageBackedArrowArray>)>
              on_complete =
                  [col = col.get(), new_table = new_table.get()](
                      std::shared_ptr<StorageBackedArrowArray> new_col)
              -> CopyableResult<void> {
            new_table->columns_.emplace(col->name, new_col);
            return CopyableResultSuccess();
          };

          rg.AddReturnsOp(
              StorageBackedArrowArray::FromStorageAsync(col_uri),
              col_uri.string(), on_complete);
        }
        KATANA_CHECKED(rg.Finish());

        KATANA_CHECKED(new_table->ResetSchema());
        return new_table;
      });
}

Result<std::shared_ptr<katana::StorageBackedArrowTable>>
katana::StorageBackedArrowTable::Append(
    const std::shared_ptr<arrow::Table>& to_append,
    const std::shared_ptr<arrow::Array>& take_indexes) {
  return AppendCommon(to_append, take_indexes);
}

Result<std::shared_ptr<katana::StorageBackedArrowTable>>
katana::StorageBackedArrowTable::Append(
    const std::shared_ptr<StorageBackedArrowTable>& to_append,
    const std::shared_ptr<arrow::Array>& take_indexes) {
  return AppendCommon(to_append, take_indexes);
}

Result<std::shared_ptr<katana::StorageBackedArrowTable>>
katana::StorageBackedArrowTable::AppendNulls(int64_t num_nulls) {
  if (num_nulls < 0) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument,
        "number of nulls to append must be non-negative");
  }

  auto new_table = MakeShared(storage_location_, num_rows() + num_nulls);
  KATANA_CHECKED(FillOtherColumns(new_table.get()));
  KATANA_CHECKED(new_table->ResetSchema());
  return new_table;
}

Result<void>
katana::StorageBackedArrowTable::Unload(WriteGroup* wg) {
  return CreateOrJoinAsyncGroup(wg, [&](WriteGroup* new_wg) -> Result<void> {
    for (const auto& [name, col] : columns_) {
      KATANA_CHECKED(col->Unload(new_wg));
    }
    return ResultSuccess();
  });
}

Result<katana::URI>
katana::StorageBackedArrowTable::Persist(WriteGroup* wg) {
  return CreateOrJoinAsyncGroup(wg, [&](WriteGroup* new_wg) -> Result<URI> {
    fbs::StorageBackedArrowTableT table_fb;
    table_fb.num_rows = num_rows();
    for (auto& [name, array] : columns_) {
      auto new_col = std::make_unique<fbs::StorageBackedArrowColumnT>();
      new_col->name = name;
      new_col->uri =
          UriToFB(storage_location_, KATANA_CHECKED(array->Persist(new_wg)));
      table_fb.columns.emplace_back(std::move(new_col));
    }
    auto table_file = storage_location_.RandFile("property-table");
    KATANA_CHECKED(PersistFB(&table_fb, table_file, new_wg));

    return table_file;
  });
}

Result<std::shared_ptr<katana::StorageBackedArrowTable>>
katana::StorageBackedArrowTable::AppendNewData(
    const std::shared_ptr<arrow::Table>& to_append) {
  auto new_table =
      MakeShared(storage_location_, to_append->num_rows() + num_rows());
  for (int col_idx = 0; col_idx < to_append->num_columns(); ++col_idx) {
    const auto& col_to_append = to_append->column(col_idx);
    std::string name = to_append->field(col_idx)->name();
    if (name.empty()) {
      return KATANA_ERROR(
          ErrorCode::InvalidArgument, "column names cannot be empty");
    }

    std::shared_ptr<StorageBackedArrowArray> new_col;
    if (auto it = columns_.find(name); it != columns_.end()) {
      new_col = it->second;
    } else {
      new_col = KATANA_CHECKED(StorageBackedArrowArray::Make(
          storage_location_.Join(name), col_to_append->type(), num_rows()));
    }

    new_col = KATANA_CHECKED_CONTEXT(
        katana::Append(new_col, col_to_append), "column name: {}", name);

    auto [it, was_emplaced] = new_table->columns_.emplace(name, new_col);
    if (!was_emplaced) {
      return KATANA_ERROR(
          ErrorCode::InvalidArgument,
          "column names must be unique (found multiple named {})",
          std::quoted(name));
    }
  }
  return new_table;
}

Result<std::shared_ptr<katana::StorageBackedArrowTable>>
katana::StorageBackedArrowTable::AppendNewData(
    const std::shared_ptr<StorageBackedArrowTable>& to_append) {
  auto new_table =
      MakeShared(storage_location_, to_append->num_rows() + num_rows());
  for (const auto& [name, col_to_append] : to_append->columns_) {
    std::shared_ptr<StorageBackedArrowArray> new_col;
    if (auto it = columns_.find(name); it != columns_.end()) {
      new_col = it->second;
    } else {
      new_col = KATANA_CHECKED(StorageBackedArrowArray::Make(
          storage_location_.Join(name), col_to_append->type(), num_rows()));
    }

    new_col = KATANA_CHECKED_CONTEXT(
        katana::Append(new_col, col_to_append), "column name: {}",
        std::quoted(name));

    auto [it, was_emplaced] = new_table->columns_.emplace(name, new_col);
    if (!was_emplaced) {
      return KATANA_ERROR(
          ErrorCode::InvalidArgument,
          "column names must be unique (found multiple named {})",
          std::quoted(name));
    }
  }
  return new_table;
}

template <typename TableType>
Result<std::shared_ptr<katana::StorageBackedArrowTable>>
katana::StorageBackedArrowTable::AppendCommon(
    const std::shared_ptr<TableType>& to_append,
    const std::shared_ptr<arrow::Array>& take_indexes) {
  if (take_indexes && to_append &&
      to_append->num_rows() != take_indexes->length()) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument,
        "number of indexes taken must match the number of rows in the table");
  }

  std::shared_ptr<katana::StorageBackedArrowTable> new_table;
  if (to_append) {
    new_table = KATANA_CHECKED(AppendNewData(to_append));
  } else {
    new_table = MakeShared(
        storage_location_,
        num_rows() + (take_indexes ? take_indexes->length() : 0));
  }

  KATANA_CHECKED(FillOtherColumns(new_table.get(), take_indexes));
  KATANA_CHECKED(new_table->ResetSchema());
  return new_table;
}

Result<void>
katana::StorageBackedArrowTable::FillOtherColumns(
    StorageBackedArrowTable* new_table,
    const std::shared_ptr<arrow::Array>& take_indexes) {
  for (const auto& [name, col] : columns_) {
    if (new_table->columns_.find(name) == new_table->columns_.end()) {
      if (take_indexes) {
        new_table->columns_.emplace(
            name, KATANA_CHECKED(katana::TakeAppend(col, take_indexes)));
      } else {
        new_table->columns_.emplace(
            name, KATANA_CHECKED(katana::AppendNulls(
                      col, new_table->num_rows() - num_rows())));
      }
    }
  }
  return ResultSuccess();
}

Result<void>
katana::StorageBackedArrowTable::ResetSchema() {
  arrow::SchemaBuilder builder(
      arrow::SchemaBuilder::ConflictPolicy::CONFLICT_ERROR);
  auto get_field = [](const auto& kv) {
    return arrow::field(kv.first, kv.second->type());
  };
  std::vector fields(
      boost::make_transform_iterator(columns_.begin(), get_field),
      boost::make_transform_iterator(columns_.end(), get_field));
  // need schema to be the same across hosts
  std::sort(fields.begin(), fields.end(), [](const auto& f1, const auto& f2) {
    return f1->name() < f2->name();
  });
  KATANA_CHECKED(builder.AddFields(fields));
  schema_ = KATANA_CHECKED(builder.Finish());
  return ResultSuccess();
}
